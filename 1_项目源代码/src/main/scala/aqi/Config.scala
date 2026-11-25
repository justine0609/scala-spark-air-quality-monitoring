package aqi

/**
 * 全局配置。
 * 所有路径、口径阈值、维度标准值集中在此，避免散落在业务代码里。
 */
object Config {

  /** 应用名，会显示在 Spark UI 与日志中 */
  val appName: String = "AQI-Project4"

  /** 运行模式：题目要求本地模式 local[*] */
  val master: String = "local[*]"

  /** 数据目录，可用 -Daqi.data.dir=xxx 覆盖 */
  val dataDir: String = sys.props.getOrElse("aqi.data.dir", "data")

  /** 清洗后明细输出目录 */
  val cleanedDir: String = "cleaned"

  /** 指标 JSON 输出目录（一图一文件） */
  val outputDir: String = "output"

  /** 六项污染物，顺序固定：PM2.5、PM10、SO2、NO2、CO、O3 */
  val pollutants: Seq[String] = Seq("PM2.5", "PM10", "SO2", "NO2", "CO", "O3")

  /** 日尺度聚合门槛：同站同日有效记录数不少于 3 条才计算日均 */
  val minValidRecordsForDaily: Int = 3

  /** AQI 重算与原值偏差阈值：超过则采用重算值 */
  val aqiDeviationThreshold: Int = 10

  /** 优良判定阈值：日 AQI <= 100 记为优良天 */
  val goodAqiThreshold: Int = 100

  /** 重污染判定阈值：AQI > 200 */
  val heavyPollutionThreshold: Int = 200

  /** 六个标准区县（与站点台账一致） */
  val stdCounties: Seq[String] = Seq("魏都区", "建安区", "禹州市", "长葛市", "襄城县", "鄢陵县")

  /** 三类标准站点类型 */
  val stdStationTypes: Seq[String] = Seq("国控", "省控", "市控")

  /** 统计区间（用于日志与文档说明） */
  val periodStart: String = "2024-10-01"
  val periodEnd: String = "2024-11-29"
  val periodDays: Int = 60

  /** 拼接数据文件路径 */
  def csv(file: String): String = s"$dataDir/$file"

  /** 四份原始数据文件名 */
  val fileMeasurements: String = "hourly_measurements.csv"
  val fileStations: String = "stations.csv"
  val fileMeteorology: String = "meteorology.csv"
  val fileAqiLevels: String = "aqi_levels.csv"

  // ==================== 运行环境自愈 ====================

  /**
   * Windows 下 Spark 依赖 Hadoop 的本地可执行文件 winutils.exe 与动态库 hadoop.dll。
   * Hadoop 的 Shell 类在启动时会检查 HADOOP_HOME 环境变量或 hadoop.home.dir 系统属性，
   * 两者都未设置时直接抛 FileNotFoundException，SparkSession 根本创建不起来。
   *
   * 为让本工程在任意一台 Windows 机器上（不预先配置环境变量）都能直接运行，
   * 这里把两个文件随工程分发在 hadoop/bin 下，启动时按"当前目录 → 上级目录 → 环境变量"
   * 的顺序探测，找到即写入 hadoop.home.dir 系统属性。
   *
   * 注意：环境变量无法在 JVM 启动后再设置，但 hadoop.home.dir 系统属性与 HADOOP_HOME
   * 在 Hadoop 源码中是等价的（Shell.checkHadoopHomeInner 二者取其一）。
   */
  def prepareHadoopEnv(): Unit = {
    val os = System.getProperty("os.name", "").toLowerCase
    if (!os.contains("win")) {
      println("[环境] 非 Windows 系统，Hadoop 原生库无需处理")
      return
    }

    val key = "hadoop.home.dir"
    val already = Option(System.getProperty(key)).orElse(Option(System.getenv("HADOOP_HOME")))
    if (already.exists(h => new java.io.File(h, "bin/winutils.exe").isFile)) {
      println(s"[环境] Hadoop 原生库已就绪：${already.get}")
      return
    }

    val homes = (Seq(new java.io.File(".", "hadoop"), new java.io.File("..", "hadoop"))
      ++ already.map(new java.io.File(_)))
      .map(_.getAbsoluteFile)
      .distinct

    homes.find(d => new java.io.File(d, "bin/winutils.exe").isFile) match {
      case Some(dir) =>
        System.setProperty(key, dir.getAbsolutePath)
        val bin = new java.io.File(dir, "bin").getAbsolutePath
        val lp = System.getProperty("java.library.path", "")
        if (!lp.split(java.io.File.pathSeparator).contains(bin)) {
          System.setProperty("java.library.path", bin + java.io.File.pathSeparator + lp)
        }
        println(s"[环境] 已自动定位 Hadoop 原生库：$bin")
      case None =>
        println("[环境] 警告：未找到 hadoop/bin/winutils.exe，Windows 下写入 parquet 可能失败")
        println("[环境]       请确认工程根目录下的 hadoop 文件夹完整，或双击 运行.bat 启动")
    }
  }
}
