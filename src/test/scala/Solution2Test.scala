import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should
import spark.SparkHelper

class Solution2Test extends AnyFlatSpec
  with should.Matchers
  with SolutionTest {

  override def solutionName: String = "DataFrame solution"

  override def runSolution(input: Iterable[String]): Seq[(Int, Int)] = {
    val inputDf = createDfFromIterable(input)
    dfToIntTuples(Solution2.solution(inputDf))
  }

  import SparkHelper.session.implicits._

  private def createDfFromIterable(it: Iterable[String]): DataFrame = {
    it.toList.toDF()
  }

  private def dfToStrings(df: DataFrame): Seq[String] = {
    df.collect().map(_.getAs[String](0))
  }

  private def dfToIntTuples(df: DataFrame): Seq[(Int, Int)] = {
    df.collect().map(r => (
      r.getAs[Int]("key"), r.getAs[Int]("value")
    ))
  }

  it should "add a zero to an empty field on the left of csv lines" in {
    val df = createDfFromIterable(csvFileWithMissing)
    val replacedDf = df.withColumn("value", Solution2.replaceLeftEmptyValues)
    val result = dfToStrings(replacedDf)

    result should be (Array("X,Y", "0,0", "0,", "0,0", "1,", "1,", "3,"))
  }

  it should "add a zero to an empty field on the right of csv lines" in {
    val df = createDfFromIterable(csvFileWithMissing)
    val replacedDf = df.withColumn("value", Solution2.replaceRightEmptyValues)
    val result = dfToStrings(replacedDf)

    result should be (Array("X,Y", ",0", "0,0", "0,0", "1,0", "1,0", "3,0"))
  }

  it should "add a zero to an empty field on the left of tsv lines" in {
    val df = createDfFromIterable(tsvFileWithMissing)
    val replacedDf = df.withColumn("value", Solution2.replaceLeftEmptyValues)
    val result = dfToStrings(replacedDf)

    result should be (Array("apples\toranges", "1\t", "0,0", "0\t", "4\t5", "3\t"))
  }

  it should "add a zero to an empty field on the right of tsv lines" in {
    val df = createDfFromIterable(tsvFileWithMissing)
    val replacedDf = df.withColumn("value", Solution2.replaceRightEmptyValues)
    val result = dfToStrings(replacedDf)

    result should be (Array("apples\toranges", "1,0", "\t0", "0,0", "4\t5", "3,0"))
  }

  it should "add a zero to the empty fields on csv and tsv files" in {
    val df = createDfFromIterable(csvFileWithMissing ++ tsvFileWithMissing)
    val replacedDf = df
      .withColumn("value", Solution2.replaceRightEmptyValues)
      .withColumn("value", Solution2.replaceLeftEmptyValues)
    val result = dfToStrings(replacedDf)

    result should be (Array(
      "X,Y", "0,0", "0,0", "0,0", "1,0", "1,0", "3,0",
      "apples\toranges", "1,0", "0,0", "0,0", "4\t5", "3,0"
    ))
  }

}