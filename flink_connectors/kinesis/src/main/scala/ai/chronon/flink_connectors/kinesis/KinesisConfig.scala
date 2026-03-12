package ai.chronon.flink_connectors.kinesis

import ai.chronon.flink.FlinkUtils
import ai.chronon.online.TopicInfo
import org.apache.flink.kinesis.shaded.com.amazonaws.services.kinesis.model.DescribeStreamSummaryRequest
import org.apache.flink.kinesis.shaded.org.apache.flink.connector.aws.config.AWSConfigConstants
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants
import org.apache.flink.streaming.connectors.kinesis.util.AWSUtil

import java.util.Properties

object KinesisConfig {
  case class ConsumerConfig(properties: Properties, explicitParallelism: Option[Int])

  object Keys {
    val AwsRegion = "AWS_REGION"
    val AwsDefaultRegion = "AWS_DEFAULT_REGION"
    val AwsAccessKeyId = "AWS_ACCESS_KEY_ID"
    val AwsSecretAccessKey = "AWS_SECRET_ACCESS_KEY"
    val KinesisEndpoint = "KINESIS_ENDPOINT"
    val AssumeRoleArn = "KINESIS_ASSUME_ROLE_ARN"
    val TaskParallelism = "tasks"
    val InitialPosition = "initial_position"
    val EnableEfo = "enable_efo"
    val EfoConsumerName = "efo_consumer_name"
  }

  object Defaults {
    val InitialPosition: String = ConsumerConfigConstants.InitialPosition.LATEST.toString
  }

  def buildConsumerConfig(props: Map[String, String], topicInfo: TopicInfo): ConsumerConfig = {
    val lookup = new PropertyLookup(props, topicInfo)
    val properties = new Properties()

    val region = lookup.requiredOneOf(Keys.AwsRegion, Keys.AwsDefaultRegion)
    val maybeAccessKeyId = lookup.optional(Keys.AwsAccessKeyId)
    val maybeSecretAccessKey = lookup.optional(Keys.AwsSecretAccessKey)
    val maybeAssumeRoleArn = lookup.optional(Keys.AssumeRoleArn)

    properties.setProperty(AWSConfigConstants.AWS_REGION, region)

    // Credential provider selection:
    // - ASSUME_ROLE: When a cross-account role ARN is provided. Uses the default credential chain
    //                to assume the specified role (e.g., for reading Kinesis in another account)
    // - BASIC: When explicit credentials provided via -Z flags
    // - AUTO: When no -Z credentials provided.
    //         Uses AWS credential chain (env vars → system props → web identity → IAM roles)
    maybeAssumeRoleArn match {
      case Some(roleArn) =>
        properties.setProperty(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER, "ASSUME_ROLE")
        properties.setProperty(AWSConfigConstants.AWS_ROLE_ARN, roleArn)
        properties.setProperty(AWSConfigConstants.AWS_ROLE_SESSION_NAME, "chronon-kinesis-session")
      case None =>
        (maybeAccessKeyId, maybeSecretAccessKey) match {
          case (Some(accessKeyId), Some(secretAccessKey)) =>
            properties.setProperty(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER, "BASIC")
            properties.setProperty(AWSConfigConstants.AWS_ACCESS_KEY_ID, accessKeyId)
            properties.setProperty(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, secretAccessKey)
          case (None, None) =>
            properties.setProperty(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER, "AUTO")
          case _ =>
            throw new IllegalArgumentException(
              s"Both ${Keys.AwsAccessKeyId} and ${Keys.AwsSecretAccessKey} must be provided together, or neither for IAM role-based auth"
            )
        }
    }

    val initialPosition = lookup.optional(Keys.InitialPosition).getOrElse(Defaults.InitialPosition)
    properties.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, initialPosition)

    val endpoint = lookup.optional(Keys.KinesisEndpoint)
    val publisherType = lookup.optional(Keys.EnableEfo).map { enabledFlag =>
      if (enabledFlag.toBoolean) ConsumerConfigConstants.RecordPublisherType.EFO.toString
      else ConsumerConfigConstants.RecordPublisherType.POLLING.toString
    }
    val efoConsumerName = lookup.optional(Keys.EfoConsumerName)
    val explicitParallelism = lookup.optional(Keys.TaskParallelism).map(_.toInt)

    endpoint.foreach(properties.setProperty(AWSConfigConstants.AWS_ENDPOINT, _))
    publisherType.foreach(properties.setProperty(ConsumerConfigConstants.RECORD_PUBLISHER_TYPE, _))
    efoConsumerName.foreach(properties.setProperty(ConsumerConfigConstants.EFO_CONSUMER_NAME, _))

    ConsumerConfig(properties, explicitParallelism)
  }

  def getOpenShardCount(streamName: String, properties: Properties): Int = {
    val client = AWSUtil.createKinesisClient(properties)
    try {
      val request = new DescribeStreamSummaryRequest().withStreamName(streamName)
      val result = client.describeStreamSummary(request)
      result.getStreamDescriptionSummary.getOpenShardCount
    } finally {
      client.shutdown()
    }
  }

  private final class PropertyLookup(props: Map[String, String], topicInfo: TopicInfo) {
    def optional(key: String): Option[String] =
      FlinkUtils.getProperty(key, props, topicInfo)

    def required(key: String): String =
      optional(key).getOrElse(missing(key))

    def requiredOneOf(primary: String, fallback: String): String =
      optional(primary).orElse(optional(fallback)).getOrElse(missing(s"$primary or $fallback"))

    private def missing(name: String): Nothing =
      throw new IllegalArgumentException(s"Missing required property: $name")
  }
}
