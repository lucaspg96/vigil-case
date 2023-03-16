package s3

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.auth.{AWSCredentials, AWSCredentialsProvider}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.s3.model.{Bucket, ListObjectsRequest, S3Object}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}

import java.io.File
import scala.io.Source
import scala.jdk.CollectionConverters._

/***
 * This class is used as a helper to hide some S3 overhead and
 * provide easy access to AWS credentials
 *
 * @param credentialsPath path to credentials file
 * @param profileName name of the profile that is going to be used
 * @param region region name
 * @param endpointURL S3 endpoint, used when pointing to MinIO
 * @param enablePathStyle path style access flag
 */
class S3ClientHelper(
                      credentialsPath: String,
                      profileName: String = "default",
                      region: String = "us-east-2",
                      endpointURL: Option[String] = None,
                      val enablePathStyle: Boolean = true,
                    ) {

  // The AWS profile with the credentials
  private val profile: AWSCredentialsProvider = new ProfileCredentialsProvider(credentialsPath, profileName)

  // If no endpoint is provided, we set the default S3 one
  val endpoint: String = endpointURL.getOrElse(s"s3.$region.amazonaws.com")

  // The S3 client
  private val s3Client: AmazonS3 = AmazonS3ClientBuilder.standard()
      .withCredentials(profile)
      .withPathStyleAccessEnabled(enablePathStyle)
      .withEndpointConfiguration(new EndpointConfiguration(endpoint, region))
      .build()

  /***
   * This method return the credentials used by the client
   * @return AWS credentials
   */
  def credentials: AWSCredentials = profile.getCredentials

  /***
   * This method list all the existing buckets that the credentials have access
   * @return list of existing bucket
   */
  def buckets: Seq[Bucket] = s3Client.listBuckets().asScala.toSeq

  /***
   * This method check if a bucket exists. If it doesn't, it will be created
   * @param bucketName the name of the bucket
   */
  def createBucketIfNotExists(bucketName: String): Unit = {
    if(!buckets.exists(_.getName == bucketName)) {
      createBucket(bucketName)
    }
  }

  /***
   * This method creates a bucket
   * @param bucketName the name of the bucket
   * @return the created Bucket object
   */
  def createBucket(bucketName: String): Bucket = s3Client.createBucket(bucketName)

  /***
   * This method lists all the objects that are inside a bucked within a path
   * @param bucketName the name of the bucket
   * @param prefix key prefix
   * @return all object names found
   */
  def listObjectsByPrefix(bucketName: String, prefix: String): Seq[String] = {
    val req = new ListObjectsRequest()
    req.setPrefix(prefix)
    req.setBucketName(bucketName)
    s3Client.listObjects(req).getObjectSummaries.asScala.map(_.getKey).toSeq
  }

  /***
   * This method retrieve an object inside a bucket using its key
   * @param bucketName the name of the bucket
   * @param path object key
   * @return the object
   */
  def getObject(bucketName: String, path: String): S3Object = s3Client.getObject(bucketName,path)

  /***
   * This method gets the content of all objects on a bucket within a path
   * @param bucketName the name of the bucket
   * @param prefix key prefix
   * @return the lines of all the found files
   */
  def getObjectsContentAsIterable(bucketName: String, prefix: String): Iterable[String] = {
    val objects = listObjectsByPrefix(bucketName, prefix)
    objects.map(getObject(bucketName,_).getObjectContent)
      .flatMap(Source.fromInputStream(_).getLines())
  }

  /***
   * This method uploads a file to a S3 bucket
   * @param bucketName the bucket name
   * @param filePath the path to the file that will be uploaded
   * @param outputPath the path which the file will be saved at the bucket
   */
  def uploadFile(bucketName: String, filePath: String, outputPath: Option[String] = None): Unit = {
    val fileName = filePath.split("/").last
    s3Client.putObject(
      bucketName,
      outputPath.fold(fileName)(o => s"${o.stripSuffix("/")}/$fileName"),
      new File(filePath))
  }

}
