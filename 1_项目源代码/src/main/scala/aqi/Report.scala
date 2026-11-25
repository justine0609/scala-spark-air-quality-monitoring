package aqi

/**
 * 控制台报告输出工具（纯文本，无第三方依赖）。
 * 按显示宽度对齐，中文按 2 个字符宽度计算，保证终端里表格不歪。
 */
object Report {

  def banner(title: String): Unit = {
    println()
    println("=" * 78)
    println("  " + title)
    println("=" * 78)
  }

  def section(title: String): Unit = println(s"\n【$title】")

  def note(text: String): Unit = println("  · " + text)

  private def dispLen(s: String): Int = s.map(ch => if (ch >= 0x2E80) 2 else 1).sum

  private def padRight(s: String, w: Int): String = s + " " * math.max(0, w - dispLen(s))

  private def padLeft(s: String, w: Int): String = " " * math.max(0, w - dispLen(s)) + s

  /** 打印对齐表格：首列左对齐，其余列右对齐 */
  def table(headers: Seq[String], rows: Seq[Seq[Any]]): Unit = {
    val strRows = rows.map(_.map(_.toString))
    val all = headers +: strRows
    val widths = headers.indices.map(i => all.map(r => dispLen(r(i))).max)
    def line(r: Seq[String]): String =
      r.indices.map(i => if (i == 0) padRight(r(i), widths(i)) else padLeft(r(i), widths(i))).mkString("  ")
    println("  " + line(headers))
    println("  " + widths.map("-" * _).mkString("  "))
    strRows.foreach(r => println("  " + line(r)))
  }

  /** 千分位格式化整数 */
  def num(v: Long): String = String.format(java.util.Locale.ROOT, "%,d", java.lang.Long.valueOf(v))

  /** 数值格式化，保留指定小数位；null 显示为 "-" */
  def f(v: java.lang.Double, digits: Int = 1): String =
    if (v == null) "-" else String.format(s"%.${digits}f", v)
}
