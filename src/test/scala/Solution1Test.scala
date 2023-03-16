import org.mockito.MockitoSugar._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class Solution1Test extends AnyFlatSpec
  with should.Matchers
  with SolutionTest {

  override def solutionName: String = "RDD Solution"

  override def runSolution(input: Iterable[String]): Seq[(Int, Int)] = {
    Solution1.solution(input).collect()
  }

//  "RDD solution" should "work properly with 1 csv file as input" in {
//    val result = Solution1.solution(csvFile).collect()
//
//    result.length should be (3)
//    result should contain (1,2)
//    result should contain (2,3)
//    result should contain (4,5)
//
//  }
//
//  it should "work properly with 1 tsv file as input" in {
//    val result = Solution1.solution(tsvFile).collect()
//
//    result.length should be (2)
//    result should contain (1,2)
//    result should contain (3,0)
//
//  }
//
//  it should "work properly with csv and tsv files as input" in {
//    val result = Solution1.solution(csvFile ++ tsvFile).collect()
//
//    result.length should be (3)
//    result should contain (2,3)
//    result should contain (4,5)
//    result should contain (3,0)
//  }
//
//  it should "work properly with csv file with missing values" in {
//    val result = Solution1.solution(csvFileWithMissing).collect()
//
//    result.length should be (2)
//    result should contain (0,0)
//    result should contain (3,0)
//  }
//
//  it should "work properly with tsv file with missing values" in {
//    val result = Solution1.solution(tsvFileWithMissing).collect()
//
//    result.length should be (3)
//    result should contain (1,0)
//    result should contain (4,5)
//    result should contain (3,0)
//  }
//
//  it should "work properly with csv and tsv files, with or without missing values" in {
//    val result = Solution1.solution(allFiles).collect()
//
//    result.length should be (4)
//    result should contain (2,3)
//    result should contain (3,0)
//    result should contain (0,0)
//    result should contain (1,0)
//  }

}
