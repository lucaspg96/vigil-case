import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

/***
 * This test suite is testing the RDD solution. It only tests the pre-defined solution tests.
 * There is a class to test the lines parsing at [[utils.ParseHelperTest]]
 */
class Solution1Test extends AnyFlatSpec
  with should.Matchers
  with SolutionTest {

  override def solutionName: String = "RDD Solution"

  override def runSolution(input: Iterable[String]): Seq[(Int, Int)] = {
    Solution1.solution(input).collect()
  }

}
