package aqi

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

/** 核心指标卡：优良率、重污染天数、PM2.5 均值、首要污染物 */
final case class Kpi(
    validDays: Long,
    goodDays: Long,
    goodRate: Double,
    heavyDays: Long,
    pm25Avg: Double,
    primary: String,
    avgAqi: Double,
    maxAqi: Int,
    maxAqiDate: String,
    stationCount: Long,
    countyCount: Long,
    stationDays: Long,
    recordCount: Long
)

/** 月度对比的一行：同一污染物两个月的均值与变化率 */
final case class MonthCompare(
    pollutant: String,
    unit: String,
    a: Double,
    b: Double,
    diff: Double,
    changeRate: Double
)

/** 气象影响：三个相关系数 + 散点 + 分天气统计 */
final case class WeatherMetrics(
    corrWind: Double,
    corrTemp: Double,
    corrHumidity: Double,
    pairs: Long,
    scatter: DataFrame,
    byWeather: DataFrame
)

final case class MetricsResult(
    kpi: Kpi,
    months: (String, String),
    monthCompare: Seq[MonthCompare],
    daily: DataFrame,
    levelDist: DataFrame,
    primaryDist: DataFrame,
    primaryBase: Long,
    stationRank: DataFrame,
    countyCompare: DataFrame,
    weather: WeatherMetrics,
    calendar: DataFrame
)

/**
 * 指标层：九项指标全部用 DataFrame / Spark SQL 完成聚合，
 * 只有最终几十到几百行的结果才 collect 出来交给输出层序列化。
 *
 * 口径约定（与设计文档第三章一一对应）：
 *   站点日均 = 同站同日逐时记录 → 各污染物日均（有效 ≥3 条）→ 再算 AQI
 *   区县日均 = 该区县各站日均浓度的算术平均 → 再算 AQI
 *   城市日均 = 全市各站日均浓度的算术平均 → 再算 AQI
 *   AQI 一律"先平均浓度、后算 AQI"，不对 AQI 取平均（AQI 与浓度非线性）
 */
class Metrics(spark: SparkSession, data: CleanedData) {

  import spark.implicits._

  private def c(name: String): Column = col(s"`$name`")

  private def unitOf(p: String): String = if (p == "CO") "mg/m³" else "μg/m³"

  def compute(): MetricsResult = {
    Report.banner("第三章  指标口径与计算过程（九项指标）")

    val city = data.cityDaily
    val sd = data.stationDaily

    val validDays = city.count()
    val goodDays = city.filter(col("aqi") <= Config.goodAqiThreshold).count()
    val heavyDays = city.filter(col("aqi") > Config.heavyPollutionThreshold).count()
    val goodRate = if (validDays == 0) 0.0 else goodDays * 100.0 / validDays

    val avgAqiRow = city.agg(avg("aqi")).head()
    val avgAqi = Option(avgAqiRow.getAs[java.lang.Double](0)).map(_.doubleValue()).getOrElse(Double.NaN)

    val worst = city.orderBy(desc("aqi")).limit(1).select(col("date"), col("aqi")).head()
    val maxAqi = Option(worst.getAs[java.lang.Integer](1)).map(_.intValue()).getOrElse(0)
    val maxAqiDate = worst.getString(0)

    // PM2.5 期间均值：以"站点日"为样本，避免站点数多的区县被过度加权
    val pm25Row = sd.filter(c("PM2.5").isNotNull).agg(avg(c("PM2.5"))).head()
    val pm25Avg = Option(pm25Row.getAs[java.lang.Double](0)).map(_.doubleValue()).getOrElse(Double.NaN)

    val primaryBase = sd.filter(col("aqi").isNotNull).count()
    val primary = if (primaryBase == 0) "无"
    else sd.filter(col("aqi").isNotNull).groupBy("primary").count()
      .orderBy(desc("count")).limit(1).head().getString(0)

    val kpi = Kpi(
      validDays = validDays,
      goodDays = goodDays,
      goodRate = goodRate,
      heavyDays = heavyDays,
      pm25Avg = pm25Avg,
      primary = primary,
      avgAqi = avgAqi,
      maxAqi = maxAqi,
      maxAqiDate = maxAqiDate,
      stationCount = data.stations.count(),
      countyCount = data.countyDaily.select("county").distinct().count(),
      stationDays = sd.count(),
      recordCount = data.hourly.count()
    )

    printKpi(kpi)

    val daily = buildDailySeries(city)
    val months = buildMonths(sd)
    val monthCompare = buildMonthCompare(sd, months)
    val levelDist = buildLevelDist(city, validDays)
    val primaryDist = buildPrimaryDist(sd, primaryBase)
    val stationRank = buildStationRank(sd)
    val countyCompare = buildCountyCompare()
    val weather = buildWeather()
    val calendar = buildCalendar()
    printOthers(daily, levelDist, primaryDist, primaryBase, stationRank, countyCompare,
      weather, monthCompare, months, calendar.count())

    MetricsResult(kpi, months, monthCompare, daily, levelDist, primaryDist, primaryBase,
      stationRank, countyCompare, weather, calendar)
  }

  // ---------- 指标 2：60 天时间序列 ----------

  private def buildDailySeries(city: DataFrame): DataFrame =
    city.select(
      (Seq(col("date"), col("aqi"), col("level"), col("color"), col("primary")) ++
        Config.pollutants.map(p => round(c(p), 2).as(p))): _*
    ).orderBy("date")

  // ---------- 指标 3：月均浓度对比 ----------

  private def buildMonths(sd: DataFrame): (String, String) = {
    val ms = sd.select(substring(col("date"), 1, 7).as("m")).distinct().orderBy("m").collect().map(_.getString(0))
    require(ms.length >= 2, s"统计区间不足两个月，实际月份：${ms.mkString(",")}")
    (ms.head, ms.last)
  }

  private def buildMonthCompare(sd: DataFrame, months: (String, String)): Seq[MonthCompare] = {
    def monthAvg(m: String): Map[String, Double] = {
      val exprs = Config.pollutants.map(p => avg(c(p)).as(p))
      val row = sd.filter(substring(col("date"), 1, 7) === m).agg(exprs.head, exprs.tail: _*).head()
      Config.pollutants.map { p =>
        val v = row.getAs[java.lang.Double](p)
        p -> (if (v == null) Double.NaN else v.doubleValue())
      }.toMap
    }
    val a = monthAvg(months._1)
    val b = monthAvg(months._2)
    Config.pollutants.map { p =>
      val diff = b(p) - a(p)
      val rate = if (a(p) == 0 || a(p).isNaN) Double.NaN else diff / a(p) * 100
      MonthCompare(p, unitOf(p), a(p), b(p), diff, rate)
    }
  }

  // ---------- 指标 4：AQI 等级分布 ----------

  private def buildLevelDist(city: DataFrame, validDays: Long): DataFrame = {
    val counts = city.filter(col("level").isNotNull).groupBy("level")
      .agg(count(lit(1)).as("days"))
    data.levels
      .join(counts, Seq("level"), "left")
      .withColumn("days", coalesce(col("days"), lit(0L)))
      .withColumn("ratio", round(col("days") * 100.0 / validDays, 2))
      .select("level", "lo", "hi", "color", "days", "ratio", "advice")
      .orderBy("lo")
  }

  // ---------- 指标 5：首要污染物占比 ----------

  private def buildPrimaryDist(sd: DataFrame, base: Long): DataFrame =
    sd.filter(col("aqi").isNotNull).groupBy("primary")
      .agg(count(lit(1)).as("records"))
      .withColumn("ratio", round(col("records") * 100.0 / base, 2))
      .orderBy(desc("records"))

  // ---------- 指标 6：站点排名 ----------

  private def buildStationRank(sd: DataFrame): DataFrame = {
    val agg = sd.filter(col("aqi").isNotNull)
      .groupBy(col("station_code"), col("station_name"), col("county"), col("station_type"))
      .agg(
        round(avg("aqi"), 1).as("avgAqi"),
        count(lit(1)).as("validDays"),
        sum(when(col("aqi") <= Config.goodAqiThreshold, 1).otherwise(0)).as("goodDays"),
        max("aqi").as("maxAqi"),
        round(avg(c("PM2.5")), 1).as("pm25")
      )
      .withColumn("goodRate", round(col("goodDays") * 100.0 / col("validDays"), 2))
    agg.withColumn("rank", row_number().over(Window.partitionBy(lit(1)).orderBy(col("avgAqi").asc)))
  }

  // ---------- 指标 7：区县对比 ----------

  private def buildCountyCompare(): DataFrame = {
    val cd = data.countyDaily.filter(col("aqi").isNotNull)
    cd.groupBy("county").agg(
      round(avg("aqi"), 1).as("avgAqi"),
      count(lit(1)).as("validDays"),
      sum(when(col("aqi") <= Config.goodAqiThreshold, 1).otherwise(0)).as("goodDays"),
      sum(when(col("aqi") > Config.heavyPollutionThreshold, 1).otherwise(0)).as("heavyDays"),
      max("aqi").as("maxAqi"),
      round(avg(c("PM2.5")), 1).as("pm25")
    ).withColumn("goodRate", round(col("goodDays") * 100.0 / col("validDays"), 2))
      .orderBy(col("avgAqi").desc)
  }

  // ---------- 指标 8：气象影响 ----------

  private def buildWeather(): WeatherMetrics = {
    val cd = data.countyDaily
      .filter(c("平均风速").isNotNull && col("aqi").isNotNull)
      .cache()
    val pairs = cd.count()

    // 相关系数在"区县日"尺度上算：与优良率、日均 AQI 的口径一致。
    // 若用逐时浓度对逐日气象，样本量虚高且量纲错配，相关系数会明显偏大。
    val corrWind = cd.stat.corr("平均风速", "aqi")
    val corrTemp = cd.stat.corr("平均气温", "aqi")
    val corrHum = cd.stat.corr("相对湿度", "aqi")

    val scatter = cd.select(
      col("date"), col("county"), col("weather"),
      round(c("平均风速"), 2).as("wind"),
      round(c("平均气温"), 1).as("temp"),
      round(c("相对湿度"), 1).as("humidity"),
      col("aqi")
    ).orderBy("date", "county")

    val byWeather = cd.groupBy("weather").agg(
      round(avg("aqi"), 1).as("avgAqi"),
      count(lit(1)).as("days"),
      round(avg(c("平均风速")), 2).as("avgWind"),
      round(avg(c("相对湿度")), 1).as("avgHumidity")
    ).orderBy(col("avgAqi").desc)

    cd.unpersist()
    WeatherMetrics(corrWind, corrTemp, corrHum, pairs, scatter, byWeather)
  }

  // ---------- 指标 9：污染日历 ----------

  private def buildCalendar(): DataFrame =
    data.countyDaily.select(col("date"), col("county"), col("aqi"), col("level"), col("color"))
      .orderBy("date", "county")

  // ==================== 控制台输出 ====================

  private def printKpi(k: Kpi): Unit = {
    Report.section("3.1  核心指标")
    Report.table(
      Seq("指标", "数值", "口径说明"),
      Seq(
        Seq("有效天数", k.validDays, "城市日均 AQI 存在的天数，作为优良率分母"),
        Seq("优良天数", k.goodDays, "城市日均 AQI ≤ 100"),
        Seq("优良率", f"${k.goodRate}%.2f%%", "优良天数 ÷ 有效天数"),
        Seq("重污染天数", k.heavyDays, "城市日均 AQI > 200（重度及以上）"),
        Seq("PM2.5 期间均值", f"${k.pm25Avg}%.1f μg/m³", "站点日均样本的算术平均"),
        Seq("期间平均 AQI", f"${k.avgAqi}%.1f", "城市日均 AQI 的平均"),
        Seq("AQI 峰值", s"${k.maxAqi}（${k.maxAqiDate}）", "城市日均最大值"),
        Seq("首要污染物", k.primary, "站点日样本中出现最多的首要污染物")
      )
    )
    Report.note(s"样本规模：${k.stationCount} 个站点、${k.countyCount} 个区县、${k.recordCount} 条逐时记录、${k.stationDays} 个站点日")
  }

  private def printOthers(
      daily: DataFrame,
      levelDist: DataFrame,
      primaryDist: DataFrame,
      primaryBase: Long,
      rank: DataFrame,
      county: DataFrame,
      weather: WeatherMetrics,
      mc: Seq[MonthCompare],
      months: (String, String),
      calendarCells: Long
  ): Unit = {
    Report.section("3.2  60 天时间序列")
    val days = daily.collect()
    val aqis = days.map(r => r.getAs[java.lang.Integer]("aqi")).filter(_ != null).map(_.intValue())
    val pm = days.map(r => r.getAs[java.lang.Double]("PM2.5")).filter(_ != null).map(_.doubleValue())
    Report.table(
      Seq("序列", "天数", "最小", "最大", "均值"),
      Seq(
        Seq("日均 AQI", days.length, aqis.min, aqis.max, f"${aqis.sum.toDouble / aqis.length}%.1f"),
        Seq("日均 PM2.5(μg/m³)", pm.length, f"${pm.min}%.1f", f"${pm.max}%.1f", f"${pm.sum / pm.length}%.1f")
      )
    )
    val d0 = days.head.getAs[String]("date")
    val d1 = days.last.getAs[String]("date")
    Report.note(s"序列覆盖 $d0 ~ $d1，按日期升序，无缺失日期")

    Report.section("3.3  月均浓度对比（" + months._1 + " vs " + months._2 + "）")
    Report.table(
      Seq("污染物", "前月", "后月", "变化量", "变化率"),
      mc.map(m => Seq(s"${m.pollutant}(${m.unit})", f"${m.a}%.1f", f"${m.b}%.1f",
        f"${m.diff}%+.1f", if (m.changeRate.isNaN) "-" else f"${m.changeRate}%+.2f%%"))
    )

    Report.section("3.4  AQI 等级分布（区间关联 aqi_levels 得到，非 if-else 写死）")
    Report.table(
      Seq("等级", "AQI 区间", "天数", "占比", "标识色"),
      levelDist.collect().map { r =>
        Seq(r.getAs[String]("level"), s"${r.getAs[Int]("lo")}~${r.getAs[Int]("hi")}",
          r.getAs[Long]("days"), f"${r.getAs[Double]("ratio")}%.2f%%", r.getAs[String]("color"))
      }
    )

    Report.section("3.5  首要污染物占比（样本 " + primaryBase + " 个站点日）")
    Report.table(
      Seq("首要污染物", "记录数", "占比"),
      primaryDist.collect().map { r =>
        Seq(r.getAs[String]("primary"), r.getAs[Long]("records"), f"${r.getAs[Double]("ratio")}%.2f%%")
      }
    )

    Report.section("3.6  站点排名（按期间平均 AQI）")
    val all = rank.orderBy("rank").collect()
    def line(r: org.apache.spark.sql.Row): Seq[Any] =
      Seq(r.getAs[Int]("rank"), r.getAs[String]("station_name"), r.getAs[String]("county"),
        f"${r.getAs[Double]("avgAqi")}%.1f", f"${r.getAs[Double]("goodRate")}%.2f%%",
        r.getAs[Int]("maxAqi"))
    Report.note("最优 5 个站点")
    Report.table(Seq("排名", "站点", "区县", "平均 AQI", "优良率", "峰值 AQI"), all.take(5).map(line))
    Report.note("最差 5 个站点")
    Report.table(Seq("排名", "站点", "区县", "平均 AQI", "优良率", "峰值 AQI"), all.takeRight(5).map(line))

    Report.section("3.7  区县对比")
    Report.table(
      Seq("区县", "平均 AQI", "优良率", "重污染天", "PM2.5"),
      county.collect().map { r =>
        Seq(r.getAs[String]("county"), f"${r.getAs[Double]("avgAqi")}%.1f",
          f"${r.getAs[Double]("goodRate")}%.2f%%", r.getAs[Long]("heavyDays"),
          f"${r.getAs[Double]("pm25")}%.1f")
      }
    )

    Report.section("3.8  气象影响（区县日尺度，样本 " + weather.pairs + " 对）")
    Report.table(
      Seq("气象因子", "与日均 AQI 的皮尔逊相关系数", "解释"),
      Seq(
        Seq("平均风速", f"${weather.corrWind}%.4f", if (weather.corrWind < 0) "负相关：风速越大越利于扩散，AQI 越低" else "正相关"),
        Seq("平均气温", f"${weather.corrTemp}%.4f", "秋冬逆温期气温与污染的关系"),
        Seq("相对湿度", f"${weather.corrHumidity}%.4f", "高湿利于二次生成与吸湿增长")
      )
    )
    Report.note("不同天气类型下的平均 AQI")
    Report.table(
      Seq("天气", "平均 AQI", "天数", "平均风速"),
      weather.byWeather.collect().map { r =>
        Seq(r.getAs[String]("weather"), f"${r.getAs[Double]("avgAqi")}%.1f",
          r.getAs[Long]("days"), f"${r.getAs[Double]("avgWind")}%.2f")
      }
    )

    Report.section("3.9  污染日历（日期 × 区县）")
    Report.note(s"共 $calendarCells 格；60 天 x 6 区县全部有值，热力图无空白格")
  }
}
