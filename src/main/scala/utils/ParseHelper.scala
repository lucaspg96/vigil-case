package utils

/***
 * This class contains the parse logic used by the RDD solution.
 * It is responsible for handling both csv and tsv file and to
 * replace the empty strings by 0
 */
object ParseHelper {

  /***
   * This method takes an string casts it into an Int.
   * If the string is empty, the value will be 0
   * @param s the string to be parsed
   * @return the integer value of the string content or 0 if empty
   */
  def intOrZero(s: String): Int = if(s.strip().isEmpty) 0 else s.strip().toInt

  /***
   * This method takes an csv or tsv line and parse its values,
   * replacing the empty fields by 0
   * @param line the line to be parsed
   * @return the tuple with the values
   * @throws IllegalArgumentException if the values are not separated by comma or tab
   * @throws NumberFormatException if at least one value is not a parseable integer
   */
  def splitLine(line: String): (Int, Int) = {
    // identifying the separator to use
    val separator = {
      if(line.contains(",")) ","
      else if(line.contains("\t")) "\t"
      // if it is not a csv or tsv line, throws an exception
      else throw new IllegalArgumentException("values must be separated by comma or tab")
    }

    val values = line.split(separator)

    values match {
      // if the line contains 2 values or just the second one
      case Array(first, second) => (intOrZero(first), intOrZero(second))
      // if the line contains just the first value
      case Array(first) => (intOrZero(first), 0)
      // if the line contains no values, just the separator
      case Array() => (0,0)
    }
  }

}
