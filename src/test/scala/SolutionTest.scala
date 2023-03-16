import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

trait SolutionTest extends AnyFlatSpec
  with should.Matchers
  with TestFiles{

  def solutionName: String
  def runSolution(input: Iterable[String]): Seq[(Int,Int)]

  solutionName should "work properly with 1 csv file as input" in {
    val result = runSolution(csvFile)

    result.length should be (3)
    result should contain (1,2)
    result should contain (2,3)
    result should contain (4,5)

  }

  it should "work properly with 1 tsv file as input" in {
    val result = runSolution(tsvFile)

    result.length should be (2)
    result should contain (1,2)
    result should contain (3,0)

  }

  it should "work properly with csv and tsv files as input" in {
    val result = runSolution(csvFile ++ tsvFile)

    result.length should be (3)
    result should contain (2,3)
    result should contain (4,5)
    result should contain (3,0)
  }

  it should "work properly with csv file with missing values" in {
    val result = runSolution(csvFileWithMissing)
    println(result.mkString("\n"))
    result.length should be (2)
    result should contain (0,0)
    result should contain (3,0)
  }

  it should "work properly with tsv file with missing values" in {
    val result = runSolution(tsvFileWithMissing)

    result.length should be (3)
    result should contain (1,0)
    result should contain (4,5)
    result should contain (3,0)
  }

  it should "work properly with csv and tsv files, with or without missing values" in {
    val result = runSolution(allFiles)

    result.length should be (4)
    result should contain (2,3)
    result should contain (3,0)
    result should contain (0,0)
    result should contain (1,0)
  }
}
