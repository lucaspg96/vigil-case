package spark

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import s3.S3ClientHelper

/***
 * This class is just a helper to hide some overhead from Spark
 */
object SparkHelper {

  // Application name
  val appName = "Vigil Case"

  // Spark session, to use DataFrames
  lazy val session: SparkSession = SparkSession.builder.appName(appName).master("local[1]").getOrCreate()

  // Spark context, to use RDDs
  lazy val context: SparkContext = session.sparkContext

  /***
   * This method sets the configurations needed to Spark so it can use S3 as a filesystem
   * @param s3ClientHelper an instance containing the s3 configuration to read and write the files
   */
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

  /***
   * This method read all the objects from a path and return their content as a DataFrame
   * @param bucketName the name of the bucket
   * @param inputPath the path to retrieve the file(s)
   * @return
   */
  def readFilesAsDataFrame(bucketName: String, inputPath: String): DataFrame = {
    val path = s"s3a://$bucketName/$inputPath*"
    session.read.text(path)
  }

  /***
   * This method saves the RDD into a S3 bucket by converting it into a DataFrame
   * @param rdd the RDD to be saved
   * @param bucketName the name of the bucket
   * @param outputFolder the path where the files are going to be saved
   */
  def saveRddAsTSVOnS3(rdd: RDD[(Int,Int)], bucketName: String, outputFolder: String = "output"): Unit = {
    saveDfAsTSVOnS3(
      session.createDataFrame(rdd)
        // here I rename the columns to keep a readable
        .withColumnRenamed("_1","key")
        .withColumnRenamed("_2","value"),
      bucketName, outputFolder)
  }

  /***
   * This method saves a DataFrame into a S3 bucket.
   * @param df the DataFrame to be saved
   * @param bucketName the name of the bucket
   * @param outputFolder the path where the files are going to be saved
   */
  def saveDfAsTSVOnS3(df: DataFrame, bucketName: String, outputFolder: String = "output"): Unit = {
    // I opted to set the partitions to 1 to save just 1 file at the bucket
    df.repartition(1)
      .write
      .option("sep","\t") // defining the separator as a tab to save a tsv file
      .option("header", "true") // keep the headers
      .mode("overwrite") // overwrite if already exists
      .csv(s"s3a://$bucketName/$outputFolder/")
  }

}
