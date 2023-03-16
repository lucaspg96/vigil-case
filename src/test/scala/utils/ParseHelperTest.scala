package utils

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should
import utils.ParseHelper

class ParseHelperTest extends AnyFlatSpec with should.Matchers {

  "The line parser method" should "parse a csv line" in {
    val line = "10,15"
    ParseHelper.splitLine(line) should be (10,15)
  }

  it should "parse a tsv line" in {
    val line = "10\t15"
    ParseHelper.splitLine(line) should be (10,15)
  }

  it should "replace an left blank space for 0 in a csv line" in {
    val line = ",15"
    ParseHelper.splitLine(line) should be (0,15)
  }

  it should "replace an right blank space for 0 in a csv line" in {
    val line = "10,"
    ParseHelper.splitLine(line) should be (10,0)
  }

  it should "replace an left blank space for 0 in a tsv line" in {
    val line = "\t15"
    ParseHelper.splitLine(line) should be (0,15)
  }

  it should "replace an right blank space for 0 in a tsv line" in {
    val line = "10\t"
    ParseHelper.splitLine(line) should be (10,0)
  }

  it should "throw an error if it is not a csv or tsv line" in {
    val line = "10;15"
    a [IllegalArgumentException] should be thrownBy (ParseHelper.splitLine(line))
  }

  it should "throw an error if the one of the values is not number" in {
    a [NumberFormatException] should be thrownBy (ParseHelper.splitLine("10,abc"))
    a [NumberFormatException] should be thrownBy (ParseHelper.splitLine("abc,abc"))
    a [NumberFormatException] should be thrownBy (ParseHelper.splitLine("10\tabc"))
    a [NumberFormatException] should be thrownBy (ParseHelper.splitLine("abc\t10"))
  }

}
