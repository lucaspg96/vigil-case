import org.apache.spark.rdd.RDD
import s3.S3ClientHelper
import spark.SparkHelper
import utils.ParseHelper

/***
 * This solution was developed using the RDD API from Spark.
I started with this one because the way that we manipulate the data is more
similar to day-to-day Scala data structures, so a first solution could be developed
faster. The data is processed using some basic scala functions and the grouping and
counting are made using Map-Reduce.

However, it has a drawback: this implementation gets the amazon S3 objects input streams
and persists the content in memory. That is not a good approach for big data scenarios and
could be resolved by using Spark (non-structured) stream API or even by persisting the files
into the local storage and loading them into the RDD. But, since the goal here is the data processing algorithm, I abstracted that problem.
 */
object Solution1 extends App {

  // Transforming the arguments to a map turns easier to handle default values
  val argsMap = args.zipWithIndex.map(_.swap).toMap

  // the arguments are parsed as demanded at the document
  val inputPath = argsMap(0)
  val outputPath = argsMap.getOrElse(1, "output-file.tsv")
  val credentialsPath = argsMap.getOrElse(2, "~/.aws/credentials")

  // here I create an object to help on S3 calls, hiding some overhead
  // NOTE: the endpoint provided here is pointing to a local MinIO instance. if none is provided,
  // it will point to the default s3 URL
  val s3Client: S3ClientHelper = new S3ClientHelper(credentialsPath, endpointURL = Some("http://localhost:9000"))
  // once the helper is constructed, it is used to configure spark's S3 connection
  SparkHelper.configureAwsCredentials(s3Client)

  // for this test, I'm using a test bucket.
  val bucketName = "test"
  s3Client.createBucketIfNotExists(bucketName)

  // here we load the files content on memory
  val fileContent = s3Client.getObjectsContentAsIterable(bucketName, inputPath)

  /***
   * The solution is implemented here. It receives the files content and process them
   * to identify the keys that have a value occurring an odd number of times
   * @param content the lines of the files
   * @return the keys that have a value occurring an odd number of times and the respective value
   */
  def solution(content: Iterable[String]): RDD[(Int, Int)] = {
    // to create the RDD, we need to persist the lines on memory
    val lines = SparkHelper.context.parallelize(content.toSeq)

    // now we want to filter the lines that are headers.
    // Here, I assume that headers are written with words, not numbers
    val linesWithoutHeaders = lines.filter(l => l.head match {
      case ',' => true //first value is and empty string
      case '\t' => true //first value is and empty string
      case c: Char if c.isDigit => true //first value is a number
      case _ => false //line is empty or starts with a letter
    })

    // parsing the lines numbers, replacing empty strings by 0
    val pipeline = linesWithoutHeaders.map(ParseHelper.splitLine)
      // grouping each line by the key (first column)
      .groupByKey()
      .mapValues(values => {
        // Counting the occurrences of each value for each key
        values.groupBy(identity).view.mapValues(_.size).toMap
          // looking for the first one (if exists) that appears an odd number of times
          // if there is more the one, only the first will be returned (the challenge said that it would not happen)
          .find(_._2 % 2 == 1)
      })
      .collect{
        // taking only the keys with an odd value repetition
        case (key, Some((value,_))) => (key,value)
      }

    pipeline
  }

  // now we call the solution function
  val result = solution(fileContent)
  // I print it, just to debug
  result.collect().foreach(println)

  // the result is then saved as a TSV file on S3
  SparkHelper.saveRddAsTSVOnS3(result, bucketName)

  SparkHelper.session.stop()

}
