package utils

object ParseHelper {

  private def intOrZero(s: String): Int = if(s.isEmpty) 0 else s.toInt

  // TODO: test
  def splitLine(line: String): (Int, Int) = {
    val separator = {
      if(line.contains(",")) ","
      else if(line.contains("\t")) "\t"
      else throw new IllegalArgumentException("values must be separated by comma or tab")
    }
    val values = line.split(separator)
    values match {
      case Array(first, second) => (intOrZero(first), intOrZero(second))
      case Array(first) => (intOrZero(first), 0)
      case Array() => (0,0)
    }
  }

}
