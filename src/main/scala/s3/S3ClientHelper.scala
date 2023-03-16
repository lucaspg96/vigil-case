package s3

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.auth.{AWSCredentials, AWSCredentialsProvider, DefaultAWSCredentialsProviderChain}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.s3.model.{Bucket, ListObjectsRequest, S3Object}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}

import java.io.File
import scala.io.Source
import scala.jdk.CollectionConverters._

class S3ClientHelper(
                      credentialsPath: String,
                      profileName: String = "default",
                      region: String = "us-east-2",
                      endpointURL: Option[String] = None,
                      val enablePathStyle: Boolean = true,
                    ) {

  private val profile: AWSCredentialsProvider = new ProfileCredentialsProvider(credentialsPath, profileName)

  val endpoint: String = endpointURL.getOrElse(s"s3.$region.amazonaws.com")

  private val s3Client: AmazonS3 = AmazonS3ClientBuilder.standard()
      .withCredentials(profile)
      .withPathStyleAccessEnabled(enablePathStyle)
      .withEndpointConfiguration(new EndpointConfiguration(endpoint, region))
      .build()

  def credentials: AWSCredentials = profile.getCredentials

  def buckets: Seq[Bucket] = s3Client.listBuckets().asScala.toSeq

  def createBucketIfNotExists(bucketName: String): Unit = {
    if(!buckets.exists(_.getName == bucketName)) {
      createBucket(bucketName)
    }
  }

  def createBucket(bucketName: String): Bucket = s3Client.createBucket(bucketName)

  def listObjectsByPrefix(bucketName: String, prefix: String): Seq[String] = {
    val req = new ListObjectsRequest()
    req.setPrefix(prefix)
    req.setBucketName(bucketName)
    s3Client.listObjects(req).getObjectSummaries.asScala.map(_.getKey).toSeq
  }

  def getObject(bucketName: String, path: String): S3Object = s3Client.getObject(bucketName,path)

  def getObjectsAsIterable(bucketName: String, path: String): Iterable[String] = {
    val objects = listObjectsByPrefix(bucketName, path)
    objects.map(getObject(bucketName,_).getObjectContent)
      .flatMap(Source.fromInputStream(_).getLines())
  }

  def uploadFile(bucketName: String, path: String): Unit = {
    s3Client.putObject(bucketName, path.split("/").last, new File(path))
  }

}
