import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import s3.S3ClientHelper
import spark.SparkHelper

object Solution2 {

  val replaceRightEmptyValues: Column = regexp_replace(
    col("value"),
    lit("\\s$|,$"),
    lit(",0")
  )

  val replaceLeftEmptyValues: Column = regexp_replace(
    col("value"),
    lit("^\\s|^,"),
    lit("0,")
  )

  def solution(df: DataFrame): DataFrame = {

    val fixDf = df
      .withColumn("value", replaceRightEmptyValues)
      .withColumn("value", replaceLeftEmptyValues)

    val valuesDf = fixDf
      .filter(col("value").rlike("^[0-9]"))
      .withColumn("values", split(fixDf("value"), "\\s|,"))
      .drop(col("value"))
      .withColumn("key", element_at(col("values"), 1).cast("int"))
      .withColumn("value", element_at(col("values"), 2).cast("int"))
      .select("key", "value")

    val processedDf = valuesDf.groupBy("key", "value")
      .count()
      .filter((col("count") % 2) === 1)
      .drop("count")

    processedDf
  }

  def main(args: Array[String]): Unit = {

    println(args.mkString(","))
    val argsMap = args.zipWithIndex.map(_.swap).toMap

    val inputPath = argsMap.getOrElse(0, "file")
    val outputPath = argsMap.getOrElse(1, "output")
    val credentialsPath = argsMap.getOrElse(2, "~/.aws/credentials")

    val s3Client: S3ClientHelper = new S3ClientHelper(credentialsPath, endpointURL = Some("http://localhost:9000"))
    SparkHelper.configureAwsCredentials(s3Client)

    val bucketName = "test"
    s3Client.createBucketIfNotExists(bucketName)

    val processedDf = solution(SparkHelper.readFilesAsDataFrame(bucketName, inputPath))
    processedDf.show()
    SparkHelper.saveDfAsTSV(processedDf, bucketName, outputPath)

    SparkHelper.session.stop()
  }


}
