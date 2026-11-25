package aqi

/**
 * 监测时间解析工具。
 *
 * 原始数据存在四种写法（数据集说明已给出），必须全部解析成功，
 * 且解析失败的记录要能被统计出来，不能静默丢弃。
 *
 *   1. 2024-11-14 09:00:00   短横线
 *   2. 2024/11/14 09:00      斜杠
 *   3. 2024年11月14日 09时    中文
 *   4. 202411140900          纯数字（yyyyMMddHHmm）
 *
 * 这里用正则 + 模式匹配实现，解析结果统一成 yyyy-MM-dd HH:mm:ss。
 * 返回 Option 而不是直接返回 null，是本次用到的 Scala 语言特性之一。
 */
object TimeUtil {

  private val P1 = """(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})""".r
  private val P2 = """(\d{4})/(\d{2})/(\d{2}) (\d{2}):(\d{2})""".r
  private val P3 = """(\d{4})年(\d{2})月(\d{2})日 (\d{2})时""".r
  private val P4 = """(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})""".r

  private val StdPattern = """(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})""".r

  /** 解析为 yyyy-MM-dd HH:mm:ss；无法识别返回 None */
  def parse(raw: String): Option[String] =
    Option(raw).map(_.trim).flatMap {
      case P1(y, mo, d, h, mi, se) => Some(s"$y-$mo-$d $h:$mi:$se")
      case P2(y, mo, d, h, mi)     => Some(s"$y-$mo-$d $h:$mi:00")
      case P3(y, mo, d, h)         => Some(s"$y-$mo-$d $h:00:00")
      case P4(y, mo, d, h, mi)     => Some(s"$y-$mo-$d $h:$mi:00")
      case _                       => None
    }

  /** 归类原始写法，用于统计"时间格式分布" */
  def kind(raw: String): String =
    Option(raw).map(_.trim).map {
      case P1(_, _, _, _, _, _) => "A 短横线"
      case P2(_, _, _, _, _)    => "B 斜杠"
      case P3(_, _, _, _)       => "C 中文"
      case P4(_, _, _, _, _)    => "D 纯数字"
      case other                => s"X 无法识别:$other"
    }.getOrElse("X 空值")

  /** 是否为标准格式（清洗后自检用） */
  def isStd(s: String): Boolean = Option(s).exists(v => StdPattern.pattern.matcher(v).matches())

  /** 从标准格式中取日期部分 yyyy-MM-dd */
  def dateOf(stdTime: String): String = stdTime.take(10)

  /** 从标准格式中取小时 0-23 */
  def hourOf(stdTime: String): Int = stdTime.substring(11, 13).toInt
}
