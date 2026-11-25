package aqi

import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.io.File
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/**
 * 输出层：把清洗后的明细落 parquet，把九项指标落成"一图一文件"的 JSON。
 *
 * JSON 统一结构：
 *   { "meta": {...}, "data": ... }
 * meta 放口径说明与单位，data 放数据本体；前端只读 JSON，不写死任何数值，
 * 颜色以 data/meta 提供的国标标识色为准（前端可在显示层对六级高饱和色统一柔化）。
 *
 * 所有数值在这里统一按题目要求取小数位：数值类 1 位，百分数 2 位。
 */
class Writer(spark: SparkSession, data: CleanedData, m: MetricsResult, stats: CleanStats) {

  private val outDir = new File(Config.outputDir)
  private val cleanDir = new File(Config.cleanedDir)
  private val generated = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))

  def writeAll(): Unit = {
    Report.banner("第四章  结果输出（parquet + 一图一 JSON）")
    writeParquet()
    writeJson()
  }

  // ==================== parquet ====================

  private def writeParquet(): Unit = {
    Report.section("4.1  清洗后明细写入 cleaned/")
    val targets = Seq(
      ("hourly_detail", data.hourly, "逐时监测明细（含站点台账字段与重算 AQI）"),
      ("station_daily", data.stationDaily, "站点日均（6 项浓度日均 + 日 AQI + 首要污染物）"),
      ("county_daily", data.countyDaily, "区县日均（含等级与气象）"),
      ("city_daily", data.cityDaily, "城市日均（60 天序列与等级分布的基础）")
    )
    // parquet 落盘依赖 Hadoop 的文件系统实现，在 Windows 上若缺少 winutils.exe 会抛错。
    // 这里逐项兜住：即使 parquet 写不出去，也要保证后面的 11 个 JSON 与九项指标正常产出，
    // 不能让一个附加产物把整条流水线带崩。
    var failed = 0
    targets.foreach { case (name, df, desc) =>
      val path = new File(cleanDir, name).getPath
      try {
        df.write.mode("overwrite").parquet(path)
        Report.note(f"$name%-14s ${df.count()}%,8d 行  $desc")
      } catch {
        case e: Throwable =>
          failed += 1
          Report.note(f"$name%-14s 写入失败：${e.getClass.getSimpleName}  $desc")
      }
    }
    if (failed > 0) {
      Report.note("")
      Report.note("[警告] 有 parquet 未能写出，原因通常是 Windows 上缺少 hadoop/bin/winutils.exe。")
      Report.note("       九项指标与 JSON 仍会照常生成，大屏不受影响。")
      Report.note("       补救办法：确认工程根目录的 hadoop 文件夹完整后重跑，或双击 运行.bat。")
    }
  }

  // ==================== JSON ====================

  private def writeJson(): Unit = {
    Report.section("4.2  九项指标写入 output/（一图一文件）")
    val written = Seq(
      "kpi.json" -> kpiJson,
      "daily_series.json" -> dailySeriesJson,
      "monthly_compare.json" -> monthlyJson,
      "level_distribution.json" -> levelJson,
      "primary_pollutant.json" -> primaryJson,
      "station_rank.json" -> rankJson,
      "county_compare.json" -> countyJson,
      "weather_effect.json" -> weatherJson,
      "calendar.json" -> calendarJson,
      "conclusions.json" -> conclusionsJson,
      "quality.json" -> qualityJson
    )
    written.foreach { case (name, content) =>
      Json.write(new File(outDir, name), content)
      Report.note(f"$name%-26s ${content.getBytes("UTF-8").length}%,8d 字节")
    }
    Report.note(s"输出目录：${outDir.getAbsolutePath}")
  }

  /** 通用包装：meta + data */
  private def wrap(meta: Seq[(String, String)], body: String): String =
    Json.obj(
      "meta" -> Json.obj((meta :+ ("generated" -> Json.str(generated))): _*),
      "data" -> body
    )

  // ---------- 指标 1：核心指标 ----------

  private def kpiJson: String = {
    val k = m.kpi
    wrap(
      Seq("title" -> Json.str("核心指标"), "period" -> Json.str(s"${Config.periodStart} ~ ${Config.periodEnd}")),
      Json.obj(
        "validDays" -> Json.int(k.validDays),
        "goodDays" -> Json.int(k.goodDays),
        "goodRate" -> Json.num(k.goodRate, 2),
        "heavyDays" -> Json.int(k.heavyDays),
        "pm25Avg" -> Json.num(k.pm25Avg, 1),
        "pm25Unit" -> Json.str("μg/m³"),
        "primary" -> Json.str(k.primary),
        "avgAqi" -> Json.num(k.avgAqi, 1),
        "maxAqi" -> Json.int(k.maxAqi.toLong),
        "maxAqiDate" -> Json.str(k.maxAqiDate),
        "stationCount" -> Json.int(k.stationCount),
        "countyCount" -> Json.int(k.countyCount),
        "stationDays" -> Json.int(k.stationDays),
        "recordCount" -> Json.int(k.recordCount)
      )
    )
  }

  // ---------- 指标 2：60 天序列 ----------

  private def dailySeriesJson: String = {
    val rows = m.daily.orderBy("date").collect()
    val items = rows.map { r =>
      Json.obj(
        (Seq(
          "date" -> Json.str(s(r, "date")),
          "aqi" -> Json.int(i(r, "aqi")),
          "level" -> Json.str(s(r, "level")),
          "color" -> Json.str(s(r, "color")),
          "primary" -> Json.str(s(r, "primary"))
        ) ++ Config.pollutants.map(p => p -> Json.num(d(r, p), 1))): _*
      )
    }
    wrap(
      Seq(
        "title" -> Json.str("60 天 AQI 与六项污染物日均序列"),
        "unit" -> Json.obj(("aqi" -> Json.str("无量纲")) +: Config.pollutants.map(p => p -> Json.str(if (p == "CO") "mg/m³" else "μg/m³")): _*)
      ),
      Json.arr(items)
    )
  }

  // ---------- 指标 3：月均对比 ----------

  private def monthlyJson: String = {
    val items = m.monthCompare.map { mc =>
      Json.obj(
        "pollutant" -> Json.str(mc.pollutant),
        "unit" -> Json.str(mc.unit),
        "m1" -> Json.num(mc.a, 1),
        "m2" -> Json.num(mc.b, 1),
        "change" -> Json.num(mc.diff, 1),
        "changeRate" -> Json.num(mc.changeRate, 2)
      )
    }
    wrap(
      Seq("title" -> Json.str("月均浓度对比"), "months" -> Json.arr(Seq(Json.str(m.months._1), Json.str(m.months._2)))),
      Json.arr(items)
    )
  }

  // ---------- 指标 4：等级分布 ----------

  private def levelJson: String = {
    val items = m.levelDist.collect().map { r =>
      Json.obj(
        "level" -> Json.str(s(r, "level")),
        "lo" -> Json.int(i(r, "lo")),
        "hi" -> Json.int(i(r, "hi")),
        "days" -> Json.int(i(r, "days")),
        "ratio" -> Json.num(d(r, "ratio"), 2),
        "color" -> Json.str(s(r, "color")),
        "advice" -> Json.str(s(r, "advice"))
      )
    }
    wrap(
      Seq("title" -> Json.str("AQI 等级分布"), "base" -> Json.str("城市日均 AQI 天数"),
        "note" -> Json.str("等级与国标标识色来自 aqi_levels 区间关联，数据层不改写；前端显示层对六级高饱和色统一柔化")),
      Json.arr(items)
    )
  }

  // ---------- 指标 5：首要污染物 ----------

  private def primaryJson: String = {
    val items = m.primaryDist.collect().map { r =>
      Json.obj(
        "name" -> Json.str(s(r, "primary")),
        "records" -> Json.int(i(r, "records")),
        "ratio" -> Json.num(d(r, "ratio"), 2)
      )
    }
    wrap(
      Seq("title" -> Json.str("首要污染物占比"),
        "base" -> Json.int(m.primaryBase),
        "baseDesc" -> Json.str("站点日样本数（对每条记录取 IAQI 最大项）")),
      Json.arr(items)
    )
  }

  // ---------- 指标 6：站点排名 ----------

  private def rankJson: String = {
    val all = m.stationRank.orderBy("rank").collect()
    def item(r: Row) = Json.obj(
      "rank" -> Json.int(i(r, "rank")),
      "code" -> Json.str(s(r, "station_code")),
      "name" -> Json.str(s(r, "station_name")),
      "county" -> Json.str(s(r, "county")),
      "type" -> Json.str(s(r, "station_type")),
      "avgAqi" -> Json.num(d(r, "avgAqi"), 1),
      "goodRate" -> Json.num(d(r, "goodRate"), 2),
      "validDays" -> Json.int(i(r, "validDays")),
      "maxAqi" -> Json.int(i(r, "maxAqi")),
      "pm25" -> Json.num(d(r, "pm25"), 1)
    )
    val best = all.take(10).map(item)
    val worst = all.takeRight(10).reverse.map(item)
    wrap(
      Seq("title" -> Json.str("站点 AQI 排名"), "sort" -> Json.str("按期间平均 AQI 升序，排名越前空气越好")),
      Json.obj("best" -> Json.arr(best), "worst" -> Json.arr(worst), "total" -> Json.int(all.length.toLong))
    )
  }

  // ---------- 指标 7：区县对比 ----------

  private def countyJson: String = {
    val items = m.countyCompare.collect().map { r =>
      Json.obj(
        "county" -> Json.str(s(r, "county")),
        "avgAqi" -> Json.num(d(r, "avgAqi"), 1),
        "goodRate" -> Json.num(d(r, "goodRate"), 2),
        "goodDays" -> Json.int(i(r, "goodDays")),
        "validDays" -> Json.int(i(r, "validDays")),
        "heavyDays" -> Json.int(i(r, "heavyDays")),
        "maxAqi" -> Json.int(i(r, "maxAqi")),
        "pm25" -> Json.num(d(r, "pm25"), 1)
      )
    }
    wrap(
      Seq("title" -> Json.str("区县对比"), "base" -> Json.str("区县日均 AQI（各站日均浓度先平均、后算 AQI）")),
      Json.arr(items)
    )
  }

  // ---------- 指标 8：气象影响 ----------

  private def weatherJson: String = {
    val w = m.weather
    val points = w.scatter.collect().map { r =>
      Json.obj(
        "date" -> Json.str(s(r, "date")),
        "county" -> Json.str(s(r, "county")),
        "weather" -> Json.str(s(r, "weather")),
        "wind" -> Json.num(d(r, "wind"), 2),
        "temp" -> Json.num(d(r, "temp"), 1),
        "humidity" -> Json.num(d(r, "humidity"), 1),
        "aqi" -> Json.int(i(r, "aqi"))
      )
    }
    val byW = w.byWeather.collect().map { r =>
      Json.obj(
        "weather" -> Json.str(s(r, "weather")),
        "avgAqi" -> Json.num(d(r, "avgAqi"), 1),
        "days" -> Json.int(i(r, "days")),
        "avgWind" -> Json.num(d(r, "avgWind"), 2),
        "avgHumidity" -> Json.num(d(r, "avgHumidity"), 1)
      )
    }
    wrap(
      Seq("title" -> Json.str("风速与 AQI 关系"),
        "scale" -> Json.str("区县日"),
        "pairs" -> Json.int(w.pairs),
        "corr" -> Json.obj(
          "wind" -> Json.num(w.corrWind, 4),
          "temp" -> Json.num(w.corrTemp, 4),
          "humidity" -> Json.num(w.corrHumidity, 4)
        )),
      Json.obj("scatter" -> Json.arr(points), "byWeather" -> Json.arr(byW))
    )
  }

  // ---------- 指标 9：污染日历 ----------

  private def calendarJson: String = {
    val rows = m.calendar.collect()
    val cells = rows.map { r =>
      Json.arr(Seq(Json.str(s(r, "date")), Json.str(s(r, "county")), Json.int(i(r, "aqi"))))
    }
    val levels = data.levels.orderBy("lo").collect().map { r =>
      Json.obj(
        "level" -> Json.str(s(r, "level")),
        "lo" -> Json.int(i(r, "lo")),
        "hi" -> Json.int(i(r, "hi")),
        "color" -> Json.str(s(r, "color"))
      )
    }
    wrap(
      Seq("title" -> Json.str("污染日历热力图"),
        "start" -> Json.str(Config.periodStart),
        "end" -> Json.str(Config.periodEnd),
        "format" -> Json.str("[日期, 区县, 日 AQI]"),
        "counties" -> Json.arr(Config.stdCounties.map(Json.str)),
        "levels" -> Json.arr(levels)),
      Json.arr(cells)
    )
  }

  // ---------- 顶部结论：由数据生成，不写死 ----------

  private def conclusionsJson: String = {
    val k = m.kpi
    val countyRows = m.countyCompare.collect()
    val worstName = countyRows.head.getAs[String]("county")
    val worstAqi = countyRows.head.getAs[Double]("avgAqi")
    val worstRate = countyRows.head.getAs[Double]("goodRate")
    val bestName = countyRows.last.getAs[String]("county")
    val bestAqi = countyRows.last.getAs[Double]("avgAqi")
    val pm = m.monthCompare.find(_.pollutant == "PM2.5").getOrElse(m.monthCompare.head)
    val trend = if (pm.changeRate.isNaN) "基本持平"
      else if (pm.changeRate > 0) f"上升 ${pm.changeRate}%.2f%%"
      else f"下降 ${-pm.changeRate}%.2f%%"
    val windDesc =
      if (m.weather.corrWind < -0.3) "风速与日均 AQI 呈明显负相关，扩散条件是污染形成的主导气象因素"
      else if (m.weather.corrWind < 0) "风速与日均 AQI 弱负相关，扩散条件有一定影响"
      else "风速与日均 AQI 未见负相关，需结合湿度与天气类型判断"

    val items = Seq(
      f"统计期 ${Config.periodDays} 天（有效 ${k.validDays} 天）优良天数 ${k.goodDays} 天，优良率 ${k.goodRate}%.2f%%；重污染 ${k.heavyDays} 天，AQI 峰值 ${k.maxAqi} 出现在 ${k.maxAqiDate}。",
      f"首要污染物为 ${k.primary}，PM2.5 期间均值 ${k.pm25Avg}%.1f μg/m³；${m.months._2} 较 ${m.months._1} PM2.5 均值${trend}。",
      f"区县差异明显：${worstName} 平均 AQI ${worstAqi}%.1f、优良率 ${worstRate}%.2f%%，为污染最重区县；${bestName} 最优（平均 AQI ${bestAqi}%.1f）。",
      f"${windDesc}（r = ${m.weather.corrWind}%.4f，样本 ${m.weather.pairs} 个区县日）。"
    )
    wrap(
      Seq("title" -> Json.str("污染特征结论"), "source" -> Json.str("由指标数据实时生成，前端不写死")),
      Json.obj(
        "headline" -> Json.str(items.head),
        "items" -> Json.arr(items.map(Json.str))
      )
    )
  }

  // ---------- 数据质量（供文档与页脚） ----------

  private def qualityJson: String = {
    wrap(
      Seq("title" -> Json.str("数据清洗质量报告"), "period" -> Json.str(s"${Config.periodStart} ~ ${Config.periodEnd}")),
      Json.obj(
        "rawRows" -> Json.int(stats.rawRows),
        "hourlyRows" -> Json.int(stats.hourlyRows),
        "timeFailed" -> Json.int(stats.timeFailed),
        "codeFixed" -> Json.int(stats.codeFixed),
        "unitRestored" -> Json.obj(Config.pollutants.map(p => p -> Json.int(stats.unitFixed.toMap.getOrElse(p, 0L))): _*),
        "negNulled" -> Json.obj(Config.pollutants.map(p => p -> Json.int(stats.negNulled.toMap.getOrElse(p, 0L))): _*),
        "aqiPlatformInvalid" -> Json.int(stats.aqiPlatformInvalid),
        "aqiDeviation" -> Json.int(stats.aqiDeviation),
        "aqiUseRecalc" -> Json.int(stats.aqiUseRecalc),
        "aqiKeepPlatform" -> Json.int(stats.aqiKeepPlatform),
        "dupById" -> Json.int(stats.dupById),
        "stationUnmatched" -> Json.int(stats.stationUnmatched),
        "stationDays" -> Json.int(stats.stationDays),
        "stationDaysDropped" -> Json.int(stats.stationDaysDropped),
        "weatherMissing" -> Json.obj(stats.metMissing.map { case (k, v) => k -> Json.int(v) }: _*),
        "weatherCountyFixed" -> Json.int(stats.metCountyFixed),
        "weatherTypeFixed" -> Json.int(stats.metWeatherFixed)
      )
    )
  }

  // ==================== 取值辅助 ====================

  private def s(r: Row, k: String): String = Option(r.getAs[String](k)).getOrElse("")

  private def d(r: Row, k: String): Double =
    Option(r.getAs[java.lang.Double](k)).map(_.doubleValue()).getOrElse(Double.NaN)

  private def i(r: Row, k: String): Long = {
    val v = r.getAs[Any](k)
    v match {
      case null            => 0L
      case x: java.lang.Integer => x.longValue()
      case x: java.lang.Long    => x.longValue()
      case x: java.lang.Double  => x.longValue()
      case _               => 0L
    }
  }
}
