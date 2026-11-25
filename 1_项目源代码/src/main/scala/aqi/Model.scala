package aqi

/**
 * 领域模型与 AQI 计算核心。
 *
 * AQI 采用国标分指数（IAQI）方法：把某污染物浓度落到分段表所在区间，
 * 在该区间内做线性插值得到该污染物的 IAQI，六项取最大值即为 AQI。
 *
 *   插值公式：IAQI = (IAQI高 − IAQI低) ÷ (浓度高 − 浓度低) × (浓度 − 浓度低) + IAQI低
 *
 * 分段表直接取自《数据集说明.txt》，不做任何改动。
 */
object Model {

  /** IAQI 分档节点，与浓度分段表一一对应 */
  val IAQI_LEVELS: Array[Double] = Array(0, 50, 100, 150, 200, 300, 400, 500)

  /**
   * 六项污染物浓度限值分段表（单位：CO 为 mg/m³，其余为 μg/m³）。
   * 注意：CO 的分段值数量级与颗粒物完全不同，重算前必须先完成单位统一。
   */
  val BREAKPOINTS: Map[String, Array[Double]] = Map(
    "PM2.5" -> Array(0, 35, 75, 115, 150, 250, 350, 500),
    "PM10" -> Array(0, 50, 150, 250, 350, 420, 500, 600),
    "SO2" -> Array(0, 50, 150, 475, 800, 1600, 2100, 2620),
    "NO2" -> Array(0, 40, 80, 180, 280, 565, 750, 940),
    "CO" -> Array(0, 2, 4, 14, 24, 36, 48, 60),
    "O3" -> Array(0, 100, 160, 215, 265, 800, 1000, 1200)
  )

  /** 首要污染物名称（浓度字段名 → 展示名） */
  val DISPLAY_NAME: Map[String, String] = Map(
    "PM2.5" -> "PM2.5",
    "PM10" -> "PM10",
    "SO2" -> "SO2",
    "NO2" -> "NO2",
    "CO" -> "CO",
    "O3" -> "O3"
  )

  /**
   * 计算单项污染物分指数 IAQI。
   * 浓度低于首档返回 0，高于末档按 500 封顶（国标做法）。
   * 返回 Option，污染物未知时给 None，避免脏输入直接崩。
   */
  def iaqi(pollutant: String, concentration: Double): Option[Double] =
    BREAKPOINTS.get(pollutant).map { bp =>
      if (concentration <= bp.head) 0.0
      else if (concentration >= bp.last) IAQI_LEVELS.last
      else {
        // 找到浓度所落的区间下标 i，满足 bp(i-1) < c <= bp(i)
        val i = (1 until bp.length).find(idx => concentration <= bp(idx)).getOrElse(bp.length - 1)
        val cLo = bp(i - 1)
        val cHi = bp(i)
        val aLo = IAQI_LEVELS(i - 1)
        val aHi = IAQI_LEVELS(i)
        (aHi - aLo) / (cHi - cLo) * (concentration - cLo) + aLo
      }
    }

  /** AQI 计算结果：重算 AQI、首要污染物、各污染物 IAQI 明细 */
  final case class AqiResult(aqi: Option[Double], primary: String, detail: Map[String, Double])

  /**
   * 由六项浓度计算 AQI 与首要污染物。
   * 缺测（None/null）的污染物不参与计算；全部缺测则 AQI 为空。
   * 首要污染物取 IAQI 最大项；AQI 不超过 50 时记"无"。
   *
   * 注意：这里按 Config.pollutants 的固定顺序遍历，而不是遍历 Map。
   * Map 的遍历顺序不稳定，两项 IAQI 恰好相等时会导致首要污染物在不同运行间漂移；
   * 固定顺序后，并列时统一取靠前的一项（PM2.5 优先），结果可复现。
   */
  def compute(concentrations: Map[String, Double]): AqiResult = {
    val valid = Config.pollutants.flatMap { p =>
      concentrations.get(p).flatMap(c => iaqi(p, c).map(p -> _))
    }
    if (valid.isEmpty) AqiResult(None, "无", Map.empty)
    else {
      val (maxPollutant, maxIaqi) = valid.maxBy(_._2)
      val primary = if (maxIaqi <= 50) "无" else maxPollutant
      AqiResult(Some(maxIaqi), primary, valid.toMap)
    }
  }

  /** AQI 取整：区间表为整数闭区间，浮点 AQI 必须先取整再关联，否则 50.4 之类会落空 */
  def roundAqi(v: Double): Int = math.round(v).toInt
}
