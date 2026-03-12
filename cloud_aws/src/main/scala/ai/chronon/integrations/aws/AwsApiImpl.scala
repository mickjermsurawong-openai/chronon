package ai.chronon.integrations.aws

import ai.chronon.online._
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import ai.chronon.online.serde._
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient

import java.net.URI
import java.time.Duration
import java.util

/** Implementation of Chronon's API interface for AWS. This is a work in progress and currently just covers the
  * DynamoDB based KV store implementation.
  */
class AwsApiImpl(conf: Map[String, String]) extends Api(conf) {

  import AwsApiImpl._

  // For now similar to GcpApiImpl, we have a flag store that relies on some hardcoded values.
  val tilingEnabledFlagStore: FlagStore = (flagName: String, _: util.Map[String, String]) => {
    if (flagName == FlagStoreConstants.TILING_ENABLED) {
      true
    } else {
      false
    }
  }

  // We set the flag store to always return true for tiling enabled
  setFlagStore(tilingEnabledFlagStore)

  @transient lazy val ddbClient: DynamoDbAsyncClient = {
    val maxConcurrency = getOptional(DynamoMaxConcurrency, conf)
      .map(_.toInt)
      .getOrElse(DefaultMaxConcurrency)

    val connectionTimeout = getOptional(DynamoConnectionTimeout, conf)
      .map(Duration.parse)
      .getOrElse(DefaultConnectionTimeout)

    val apiCallTimeout = getOptional(DynamoApiCallTimeout, conf)
      .map(Duration.parse)
      .getOrElse(DefaultTotalTimeout)

    val apiCallAttemptTimeout = getOptional(DynamoApiCallAttemptTimeout, conf)
      .map(Duration.parse)
      .getOrElse(DefaultApiTimeout)

    val maybeRegion = getOptional("AWS_DEFAULT_REGION", conf)
    val maybeEndpoint = getOptional("DYNAMO_ENDPOINT", conf)

    logger.info(
      s"Creating DynamoDB client. " +
        s"Params: region: ${maybeRegion.getOrElse("not set")}, endpoint: ${maybeEndpoint.getOrElse("default")}, maxConcurrency: $maxConcurrency, " +
        s"connectionTimeout: $connectionTimeout, apiCallTimeout: $apiCallTimeout, " +
        s"apiCallAttemptTimeout: $apiCallAttemptTimeout")

    val httpClient = NettyNioAsyncHttpClient
      .builder()
      .maxConcurrency(maxConcurrency)
      .connectionTimeout(connectionTimeout)
      .build()

    val clientConfig = ClientOverrideConfiguration
      .builder()
      .apiCallTimeout(apiCallTimeout)
      .apiCallAttemptTimeout(apiCallAttemptTimeout)
      .build()

    var builder = DynamoDbAsyncClient
      .builder()
      .httpClient(httpClient)
      .overrideConfiguration(clientConfig)

    maybeRegion.foreach { region =>
      try {
        builder.region(Region.of(region))
      } catch {
        case e: IllegalArgumentException =>
          throw new IllegalArgumentException(s"Invalid AWS region format: $region", e)
      }
    }
    maybeEndpoint.foreach { endpoint =>
      try {
        builder = builder.endpointOverride(URI.create(endpoint))
      } catch {
        case e: IllegalArgumentException =>
          throw new IllegalArgumentException(s"Invalid DynamoDB endpoint URI: $endpoint", e)
      }
    }
    builder.build()

  }

  override def genKvStore: KVStore = {
    new DynamoDBKVStoreImpl(ddbClient, conf)
  }

  /** The stream decoder method in the AwsApi is currently unimplemented. This needs to be implemented before
    * we can spin up the Aws streaming Chronon stack
    */
  override def streamDecoder(groupByServingInfoParsed: GroupByServingInfoParsed): SerDe = ???

  @transient lazy val registry: ExternalSourceRegistry = new ExternalSourceRegistry()

  override def externalRegistry: ExternalSourceRegistry = registry

  /** The logResponse method is currently unimplemented. We'll need to implement this prior to bringing up the
    * fully functional serving stack in Aws which includes logging feature responses to a stream for OOC
    */
  override def logResponse(resp: LoggableResponse): Unit = ()

  override def genMetricsKvStore(tableBaseName: String): KVStore = null

  override def genEnhancedStatsKvStore(tableBaseName: String): KVStore = null
}

object AwsApiImpl {
  private val DefaultConnectionTimeout = Duration.ofMillis(1000L)
  private val DefaultApiTimeout = Duration.ofMillis(500L)
  private val DefaultTotalTimeout = Duration.ofMillis(3000L)
  private val DefaultMaxConcurrency = 100

  private[aws] val DynamoMaxConcurrency = "DYNAMO_MAX_CONCURRENCY"
  private[aws] val DynamoConnectionTimeout = "DYNAMO_CONNECTION_TIMEOUT"
  private[aws] val DynamoApiCallTimeout = "DYNAMO_API_CALL_TIMEOUT"
  private[aws] val DynamoApiCallAttemptTimeout = "DYNAMO_API_CALL_ATTEMPT_TIMEOUT"

  private[aws] def getOptional(key: String, conf: Map[String, String]): Option[String] =
    sys.env
      .get(key)
      .orElse(conf.get(key))
}
