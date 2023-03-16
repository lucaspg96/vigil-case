import s3.S3ClientHelper

import java.io.File

object LoadFilesToS3 extends App {

  val s3Client: S3ClientHelper = new S3ClientHelper("src/main/resources/aws-credentials", endpointURL = Some("http://localhost:9000"))
  val bucketName = "test"
  s3Client.createBucketIfNotExists(bucketName)

  val filesDir = "src/main/resources/sample-files"
  new File(filesDir).list().foreach(f => s3Client.uploadFile(bucketName, s"$filesDir/$f"))

}
