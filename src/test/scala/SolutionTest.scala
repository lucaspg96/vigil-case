import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

/***
 * This trait is responsible for defining the test cases to validate the problem solution,
 * regardless of the implementation that is going to be tested (RDD or DataFrame).
 *
 * To test a solution, we just need to inherit this trait and provide two implementations:
 *  - the "solutionName", so we can identify which solution is being tested
 *  - the way that the solution will be executed, given the lines as an input
 *
 *  If it is necessary, each solution test suite can add more tests to particular functions
 */
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
