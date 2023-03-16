import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import s3.S3ClientHelper
import spark.SparkHelper

/***
 * This solution was developed using the DataFrame api from Spark.
 * Since I got some time left from the first solution, I wanted to try the DataFrame api.
 * It has been a while since I used this, then I am kinda rusty. However, this API is
 * more efficient and I thought that I needed to present a solution using it.
 *
 * For this solution I needed to use the main method instead of inheriting from App.
 * That is because I defined some operations over the dataframe using regex and
 * I needed to test them separately.
 */
object Solution2 {

  /***
   * To replace the empty strings by 0, we need to handle the cases where it appears on
   * the key column or the value column, and also if it is a csv line or a tsv line.
   *
   * I divided the solution into 2 steps:
   * - First I resolve the empty values at the right side (value column). This is when we have the
   *    string ending with a comma (,$) or a tab (\t). On that case, I replace the ending of the string
   *    with a ",0", turning it into a csv line;
   *
   * - Then I do the same to the left side (key column): string starting with coma (^,) or a tab (^\t).
   *    On that case, I replace the beginning of the string with a "0,", also turning it into a csv line.
   *
   * Now, the lines as a mix of csv and tsv without missing values
   */
  val replaceRightEmptyValues: Column = regexp_replace(
    col("value"),
    lit("\t$|,$"),
    lit(",0")
  )

  val replaceLeftEmptyValues: Column = regexp_replace(
    col("value"),
    lit("^\t|^,"),
    lit("0,")
  )

  /***
   * The solution is implemented here. It receives the files content as a dataframe and process them
   * to identify the keys that have a value occurring an odd number of times
   * @param df the lines of the files as a dataframe, where the column is named "value" and the lines are the file lines
   * @return the keys that have a value occurring an odd number of times and the respective value
   */
  def solution(df: DataFrame): DataFrame = {

    // First, we fix the missing values
    val fixDf = df
      .withColumn("value", replaceRightEmptyValues)
      .withColumn("value", replaceLeftEmptyValues)

    // Then, we discard the header ones (assuming that headers are written with letters)
    val valuesDf = fixDf
      .filter(col("value").rlike("^[0-9]"))
      // after that, we split the values by comma or tab and separe them into 2 columns:
      // key (1) and values (2), both with int values
      .withColumn("values", split(col("value"), "\t|,"))
      .withColumn("key", element_at(col("values"), 1).cast("int"))
      .withColumn("value", element_at(col("values"), 2).cast("int"))
      // then we project just the needed columns
      .select("key", "value")

    // Now, we need to find the desired pairs of key-value.
    // First, we group the pairs and count their occurrences
    val processedDf = valuesDf.groupBy("key", "value")
      .count()
      // then, we filter by the ones with an odd count
      .filter((col("count") % 2) === 1)
      // at last, we drop the temporary count column
      .drop("count")

    processedDf
  }

  def main(args: Array[String]): Unit = {

    // Transforming the arguments to a map turns easier to handle default values
    val argsMap = args.zipWithIndex.map(_.swap).toMap

    // the arguments are parsed as demanded at the document
    val inputPath = argsMap.getOrElse(0, "file")
    val outputPath = argsMap.getOrElse(1, "output")
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

    // now, we retrieve the files content from S3
    val filesDf = SparkHelper.readFilesAsDataFrame(bucketName, inputPath)
    val processedDf = solution(filesDf)
    // I print it, just to debug
    processedDf.show()

    // the result is then saved as a TSV file on S3
    SparkHelper.saveDfAsTSVOnS3(processedDf, bucketName, outputPath)

    SparkHelper.session.stop()
  }


}
