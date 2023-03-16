package spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import s3.S3ClientHelper

object SparkHelper {

  val appName = "Vigil Case"

  lazy val session: SparkSession = SparkSession.builder.appName(appName).master("local[1]").getOrCreate()

  lazy val context: SparkContext = session.sparkContext

  def configureAwsCredentials(s3ClientHelper: S3ClientHelper): Unit = {
    context
      .hadoopConfiguration.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    context
      .hadoopConfiguration.set("fs.s3a.access.key", s3ClientHelper.credentials.getAWSAccessKeyId)
    context
      .hadoopConfiguration.set("fs.s3a.secret.key", s3ClientHelper.credentials.getAWSSecretKey)
    context
      .hadoopConfiguration.set("fs.s3a.endpoint", s3ClientHelper.endpoint)
    context
      .hadoopConfiguration.set("fs.s3a.path.style.access", s3ClientHelper.enablePathStyle.toString)
    context
      .hadoopConfiguration.set("fs.s3a.connection.establish.timeout", "5")
    context
      .hadoopConfiguration.set("fs.s3a.attempts.maximum", "1")


  }

  def readFilesAsDataFrame(bucketName: String, inputPath: String): DataFrame = {
    val path = s"s3a://$bucketName/$inputPath*"
    session.read
      .text(path)
  }

  def saveRddAsTSV(rdd: RDD[(Int,Int)], bucketName: String, outputFolder: String = "output"): Unit = {
    saveDfAsTSV(
      session.createDataFrame(rdd)
        .withColumnRenamed("_1","key")
        .withColumnRenamed("_2","value"),
      bucketName, outputFolder)
  }

  def saveDfAsTSV(df: DataFrame, bucketName: String, outputFolder: String = "output"): Unit = {
    df.repartition(1)
      .write
      .option("sep","\t")
      .option("header", "true")
      .mode("overwrite")
      .csv(s"s3a://$bucketName/$outputFolder/")
  }

}
