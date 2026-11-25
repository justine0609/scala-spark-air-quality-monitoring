package aqi

import java.io.{File, PrintWriter}

/**
 * 极简 JSON 构造工具：不引入任何第三方依赖，避免为了写 JSON 再拉一个库。
 * 只负责转义、拼装与 UTF-8 落盘，业务格式由 Writer 决定。
 */
object Json {

  /** 字符串转义，覆盖引号、反斜杠、换行、制表符与控制字符 */
  def esc(s: String): String = {
    val sb = new StringBuilder
    s.foreach {
      case '"'  => sb.append("\\\"")
      case '\\' => sb.append("\\\\")
      case '\n' => sb.append("\\n")
      case '\r' => sb.append("\\r")
      case '\t' => sb.append("\\t")
      case ch if ch < ' ' => sb.append(f"\\u${ch.toInt}%04x")
      case ch   => sb.append(ch)
    }
    sb.toString()
  }

  def str(v: String): String = if (v == null) "null" else "\"" + esc(v) + "\""

  /** 数值：NaN/Infinity 输出 null（JSON 不支持），小数位可按指标要求指定 */
  def num(v: Double, digits: Int = 1): String =
    if (v.isNaN || v.isInfinite) "null"
    else String.format(java.util.Locale.ROOT, s"%.${digits}f", java.lang.Double.valueOf(v))

  def int(v: Long): String = v.toString

  def obj(fields: (String, String)*): String =
    fields.map { case (k, v) => str(k) + ":" + v }.mkString("{", ",", "}")

  def arr(items: Seq[String]): String = items.mkString("[", ",", "]")

  /** 写出文件，统一 UTF-8，不写 BOM */
  def write(file: File, content: String): Unit = {
    val parent = file.getParentFile
    if (parent != null && !parent.exists()) parent.mkdirs()
    val w = new PrintWriter(file, "UTF-8")
    try w.write(content) finally w.close()
  }
}
