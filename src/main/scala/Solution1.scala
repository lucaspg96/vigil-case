import org.apache.spark.rdd.RDD
import s3.S3ClientHelper
import spark.SparkHelper
import utils.ParseHelper

object Solution1 extends App {

  println(args.mkString(","))
  val argsMap = args.zipWithIndex.map(_.swap).toMap

  val inputPath = argsMap(0)
  val outputPath = argsMap.getOrElse(1, "output-file.tsv")
  val credentialsPath = argsMap.getOrElse(2, "~/.aws/credentials")

  val s3Client: S3ClientHelper = new S3ClientHelper(credentialsPath, endpointURL = Some("http://localhost:9000"))
  SparkHelper.configureAwsCredentials(s3Client)

  val bucketName = "test"
  s3Client.createBucketIfNotExists(bucketName)

  val fileContent = s3Client.getObjectsAsIterable(bucketName, inputPath)

  def solution(content: Iterable[String]): RDD[(Int, Int)] = {
    val lines = SparkHelper.context.parallelize(content.toSeq)
    val linesWithoutHeaders = lines.filter(l => l.head match {
      case ',' => true //first value is and empty string
      case '\t' => true //first value is and empty string
      case c: Char if c.isDigit => true //first value is a number
      case _ => false //line is empty or starts with a letter
    })

    // parsing the lines
    val pipeline = linesWithoutHeaders.map(ParseHelper.splitLine)
      // grouping each line by the key (first column)
      .groupByKey()
      .mapValues(values => {
        // Counting the occurrences of each value for each key
        values.groupBy(v => v).view.mapValues(_.size).toMap
          // taking the first one (if exists) that appears an odd number of times
          .find(_._2 % 2 == 1)
      })
      // taking only the keys with an odd value repetition
      .filter(_._2.isDefined)
      .mapValues(_.get._1)

    pipeline
  }

  val pipeline = solution(fileContent)
  pipeline.collect().foreach(println)

  SparkHelper.saveRddAsTSV(pipeline, bucketName)

  SparkHelper.session.stop()

}
