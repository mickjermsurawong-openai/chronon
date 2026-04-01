package ai.chronon.integrations.aws

import ai.chronon.spark.submission.StorageClient
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{
  GetObjectRequest,
  HeadObjectRequest,
  ListObjectsV2Request,
  PutObjectRequest
}

import scala.jdk.CollectionConverters._

class S3StorageClient(s3Client: S3Client) extends StorageClient {

  private def downloadObjectToMemory(bucketName: String, objectName: String): Array[Byte] = {
    val request = GetObjectRequest.builder().bucket(bucketName).key(objectName).build()
    s3Client.getObjectAsBytes(request).asByteArray()
  }

  override def downloadObjectToMemory(objectPath: String): Array[Byte] = {
    val (bucket, key) = parsePath(objectPath)
    downloadObjectToMemory(bucket, key)
  }

  private def listFiles(bucketName: String, prefix: String): Iterator[String] = {
    val request = ListObjectsV2Request.builder().bucket(bucketName).prefix(prefix).build()
    s3Client
      .listObjectsV2Paginator(request)
      .contents()
      .iterator()
      .asScala
      .map(_.key())
  }

  override def listFiles(bucketPath: String): Iterator[String] = {
    val (bucket, prefix) = parsePath(bucketPath)
    listFiles(bucket, prefix)
  }

  private def fileExists(bucketName: String, objectName: String): Boolean = {
    try {
      val request = HeadObjectRequest.builder().bucket(bucketName).key(objectName).build()
      s3Client.headObject(request)
      true
    } catch {
      case _: software.amazon.awssdk.services.s3.model.NoSuchKeyException => false
    }
  }

  override def fileExists(objectPath: String): Boolean = {
    val (bucket, key) = parsePath(objectPath)
    fileExists(bucket, key)
  }

  override def upload(objectPath: String, content: Array[Byte]): Unit = {
    val (bucket, key) = parsePath(objectPath)
    val request = PutObjectRequest.builder().bucket(bucket).key(key).build()
    s3Client.putObject(request, RequestBody.fromBytes(content))
  }

  /** Parses "s3://bucket/key/path" into ("bucket", "key/path"). */
  private def parsePath(path: String): (String, String) = {
    val stripped = path.stripPrefix("s3://")
    val slashIdx = stripped.indexOf('/')
    if (slashIdx < 0) (stripped, "")
    else (stripped.substring(0, slashIdx), stripped.substring(slashIdx + 1))
  }
}

private[aws] object S3StorageClient {

  /** Lists checkpoints under `{flinkStateUri}/checkpoints/{flinkInternalJobId}/chk-*` and
    * returns the path to the highest-numbered checkpoint, or None if none exist.
    */
  def resolveLatestCheckpointPath(s3Client: S3Client,
                                  flinkInternalJobId: String,
                                  flinkStateUri: String): Option[String] =
    resolveLatestCheckpointPath(new S3StorageClient(s3Client), flinkInternalJobId, flinkStateUri)

  def resolveLatestCheckpointPath(storageClient: StorageClient,
                                  flinkInternalJobId: String,
                                  flinkStateUri: String): Option[String] = {
    val jobCheckpointPath = s"$flinkStateUri/checkpoints/$flinkInternalJobId"
    val latestCheckpoint = storageClient
      .listFiles(jobCheckpointPath)
      .filter(_.split("/").exists(_.startsWith("chk-")))
      .map(_.split("/").find(_.startsWith("chk-")).get)
      .toList
      .distinct
      .sortBy(_.substring(4).toInt)(Ordering.Int.reverse)
      .headOption
    latestCheckpoint.map(chk => s"$jobCheckpointPath/$chk")
  }
}
