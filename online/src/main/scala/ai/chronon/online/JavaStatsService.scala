/*
 *    Copyright (C) 2023 The Chronon Authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package ai.chronon.online

import ai.chronon.aggregator.row.{RowAggregator, StatsGenerator}
import ai.chronon.api.{Constants, SerdeUtils, StructType}
import ai.chronon.online.KVStore.GetRequest
import ai.chronon.online.serde.{AvroCodec, AvroConversions}
import org.apache.avro.generic
import org.slf4j.{Logger, LoggerFactory}

import java.nio.charset.Charset
import java.util.concurrent.{CompletableFuture, TimeUnit}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

/** Java-friendly service for fetching enhanced statistics from KV Store.
  * This is a lightweight version that doesn't require Spark dependencies.
  *
  * @param api The API instance for accessing KV stores
  * @param tableBaseName The base table name for the BigTable enhanced stats table (default: "ENHANCED_STATS")
  * @param datasetName The dataset name within the KV store (default: Constants.EnhancedStatsDataset)
  */
class JavaStatsService(api: Api,
                       tableBaseName: String = "ENHANCED_STATS",
                       datasetName: String = Constants.EnhancedStatsDataset)(implicit ec: ExecutionContext) {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
  private val charset = Charset.forName("UTF-8")

  // Use the specialized enhanced stats KV store for efficient BigTable time-series operations
  @transient lazy val kvStore: KVStore = api.genEnhancedStatsKvStore(tableBaseName)

  /** Helper to retrieve both key and value schemas from KV Store in a single batch call */
  private def getSchemasFromKVStore(keySchemaKey: String,
                                    valueSchemaKey: String): (Option[AvroCodec], Option[AvroCodec]) = {
    try {
      val requests = Seq(
        GetRequest(keySchemaKey.getBytes(charset), datasetName, None, None),
        GetRequest(valueSchemaKey.getBytes(charset), datasetName, None, None)
      )

      val responses = Await.result(
        kvStore.multiGet(requests),
        Duration(10L, TimeUnit.SECONDS)
      )

      val keyCodecOpt = responses(0).values match {
        case Success(values) if values.nonEmpty =>
          // Use the most recent schema (last in the list)
          val schemaString = new String(values.last.bytes, charset)
          logger.info(s"Successfully found schema: $keySchemaKey (version: latest of ${values.size})")
          Some(new AvroCodec(schemaString))
        case _ =>
          logger.warn(s"Schema not found for key: $keySchemaKey")
          None
      }

      val valueCodecOpt = responses(1).values match {
        case Success(values) if values.nonEmpty =>
          // Use the most recent schema (last in the list)
          val schemaString = new String(values.last.bytes, charset)
          logger.info(s"Successfully found schema: $valueSchemaKey (version: latest of ${values.size})")
          Some(new AvroCodec(schemaString))
        case _ =>
          logger.warn(s"Schema not found for key: $valueSchemaKey")
          None
      }

      (keyCodecOpt, valueCodecOpt)
    } catch {
      case e: Exception =>
        logger.error(s"Failed to retrieve schemas for $keySchemaKey and $valueSchemaKey: ${e.getMessage}")
        (None, None)
    }
  }

  /** Fetch and merge statistics for a given table and time range.
    *
    * @param tableName The name of the table (used as key in KV store)
    * @param startTimeMillis Start of time range (inclusive)
    * @param endTimeMillis End of time range (inclusive)
    * @return CompletableFuture of JavaStatsResponse with statistics or error
    */
  def fetchStats(tableName: String,
                 startTimeMillis: Long,
                 endTimeMillis: Long): CompletableFuture[JavaStatsResponse] = {

    val scalaFuture: Future[JavaStatsResponse] = Future {
      Try {
        // Retrieve schemas from KV Store in a single batch call
        val keySchemaKey = s"$tableName${Constants.TimedKvRDDKeySchemaKey}"
        val valueSchemaKey = s"$tableName${Constants.TimedKvRDDValueSchemaKey}"

        val (keyCodecOpt, valueCodecOpt) = getSchemasFromKVStore(keySchemaKey, valueSchemaKey)

        if (keyCodecOpt.isEmpty || valueCodecOpt.isEmpty) {
          throw new RuntimeException(s"Failed to retrieve schemas for $tableName. Has it been uploaded?")
        }

        val keyCodec = keyCodecOpt.get
        val valueCodec = valueCodecOpt.get

        // Encode key using the stored key codec
        val chrononRow = Array[Any](tableName)
        val record = AvroConversions
          .fromChrononRow(chrononRow, keyCodec.chrononSchema, keyCodec.schema)
          .asInstanceOf[generic.GenericData.Record]
        val keyBytes = keyCodec.encodeBinary(record)

        val getRequest = GetRequest(
          keyBytes,
          datasetName,
          Some(startTimeMillis),
          Some(endTimeMillis)
        )

        // Fetch all IRs in the time range
        val responseFuture = kvStore.get(getRequest)
        val response = Await.result(responseFuture, Duration(30L, TimeUnit.SECONDS))

        response.values match {
          case Failure(exception) =>
            throw new RuntimeException(s"Failed to fetch stats for $tableName: ${exception.getMessage}", exception)

          case Success(timedValues) =>
            if (timedValues == null || timedValues.isEmpty) {
              throw new RuntimeException(s"No data found for $tableName in range [$startTimeMillis, $endTimeMillis]")
            } else {
              logger.info(s"Fetched ${timedValues.size} tiles for $tableName")

              // Fetch schemas (these are static metadata)
              val selectedSchemaOpt = getMetadataValue(s"$tableName/selectedSchema", None)
              val noKeysSchemaOpt = getMetadataValue(s"$tableName/noKeysSchema", None)

              if (selectedSchemaOpt.isEmpty || noKeysSchemaOpt.isEmpty) {
                throw new RuntimeException(s"Failed to retrieve schemas for $tableName. Metadata may be missing.")
              }

              // Parse schemas
              val selectedSchemaCodec = new AvroCodec(selectedSchemaOpt.get)
              val selectedSchema = selectedSchemaCodec.chrononSchema.asInstanceOf[StructType]
              val noKeysSchemaCodec = new AvroCodec(noKeysSchemaOpt.get)
              val noKeysSchema = noKeysSchemaCodec.chrononSchema.asInstanceOf[StructType]

              // Fallback: try to get from metadata row (backward compatibility)
              val cardinalityMapOpt = getMetadataValue(s"$tableName/cardinalityMap", Some(endTimeMillis))
              val mergedCardinalityMap = if (cardinalityMapOpt.isDefined) {
                val cardinalityJson = cardinalityMapOpt.get
                cardinalityJson
                  .stripPrefix("{")
                  .stripSuffix("}")
                  .split(",")
                  .map { pair =>
                    val parts = pair.split(":")
                    val key = parts(0).stripPrefix("\"").stripSuffix("\"")
                    val value = parts(1).toDouble
                    key -> value
                  }
                  .toMap
              } else {
                Map.empty[String, Double]
              }

              logger.info(s"Merged cardinality map for $tableName with ${mergedCardinalityMap.size} columns")

              // Build aggregator with merged cardinality map
              val noKeysFields = noKeysSchema.fields.map(f => (f.name, f.fieldType)).toSeq
              val enhancedMetrics = StatsGenerator.buildEnhancedMetrics(
                noKeysFields,
                mergedCardinalityMap,
                cardinalityThreshold = 0.01
              )
              val aggregator = StatsGenerator.buildAggregator(enhancedMetrics, selectedSchema)

              // Merge all IRs after denormalizing (converts bytes back to sketch objects)
              // Skip tiles that fail to decode due to schema evolution
              var successfulTiles = 0
              var skippedTiles = 0
              val mergedIr = timedValues.foldLeft(aggregator.init) { (acc, timedValue) =>
                val result = Try {
                  val irBytes = timedValue.bytes
                  val normalizedIr = valueCodec.decodeRow(irBytes)
                  val denormalizedIr = aggregator.denormalize(normalizedIr)
                  aggregator.merge(acc, denormalizedIr)
                }

                result match {
                  case Success(mergedResult) =>
                    successfulTiles += 1
                    mergedResult
                  case Failure(e) =>
                    skippedTiles += 1
                    if (skippedTiles <= 3) {
                      // Log first few failures in detail to help debug
                      logger.warn(s"Skipping tile due to schema mismatch: ${e.getClass.getSimpleName}: ${e.getMessage}")
                    }
                    acc
                }
              }

              if (successfulTiles == 0) {
                throw new RuntimeException(
                  s"All $skippedTiles tiles failed to decode for $tableName. " +
                    "This may indicate a complete schema incompatibility."
                )
              }

              if (skippedTiles > 0) {
                logger.info(s"Successfully decoded $successfulTiles tiles, skipped $skippedTiles incompatible tiles")
              }

              // Finalize to get final statistics
              val normalized = aggregator.finalize(mergedIr)

              // Convert to Map for easy access
              val fieldNames = aggregator.outputSchema.map(_._1)
              val statsMap = fieldNames.zip(normalized).toMap

              logger.info(s"Merged $successfulTiles tiles into final statistics for $tableName")

              // Add derived features
              val enhancedStatsMap = addDerivedFeatures(statsMap)

              JavaStatsResponse.success(tableName, enhancedStatsMap.asJava, successfulTiles, skippedTiles)
            }
        }
      } match {
        case Success(response) => response
        case Failure(exception) =>
          logger.error("Exception found during response construction", exception)
          JavaStatsResponse.failure(exception.getMessage)
      }
    }

    // Convert Scala Future to Java CompletableFuture
    val promise = new CompletableFuture[JavaStatsResponse]()
    scalaFuture.onComplete {
      case Success(response)  => promise.complete(response)
      case Failure(exception) => promise.completeExceptionally(exception)
    }
    promise
  }

  /** Fetch metadata (cardinalityMap, selectedSchema, noKeysSchema) from KV Store
    *
    * @param tableName The table name
    * @param endTimeMillis Optional end time to get the latest cardinalityMap up to this time
    */
  private def fetchMetadata(
      tableName: String,
      endTimeMillis: Option[Long] = None): Option[(Map[String, Double], StructType, StructType)] = {
    val cardinalityMapKey = s"$tableName/cardinalityMap"
    val selectedSchemaKey = s"$tableName/selectedSchema"
    val noKeysSchemaKey = s"$tableName/noKeysSchema"

    try {
      // Fetch all three metadata values
      // cardinalityMap is time-dependent, fetch within time range to get the latest
      val cardinalityMapOpt = getMetadataValue(cardinalityMapKey, endTimeMillis)
      val selectedSchemaOpt = getMetadataValue(selectedSchemaKey, None)
      val noKeysSchemaOpt = getMetadataValue(noKeysSchemaKey, None)

      if (cardinalityMapOpt.isEmpty || selectedSchemaOpt.isEmpty || noKeysSchemaOpt.isEmpty) {
        logger.warn(s"Missing metadata for $tableName")
        return None
      }

      // Deserialize from JSON/Avro formats
      // Parse cardinality map from simple JSON format: {"col1":0.001,"col2":0.85}
      // Values are normalized ratios (distinct / total_rows).
      val cardinalityMapJson = cardinalityMapOpt.get
      val cardinalityMap = cardinalityMapJson
        .stripPrefix("{")
        .stripSuffix("}")
        .split(",")
        .map { pair =>
          val parts = pair.split(":")
          val key = parts(0).stripPrefix("\"").stripSuffix("\"")
          val value = parts(1).toDouble
          key -> value
        }
        .toMap

      // Parse schemas from Avro JSON format
      val selectedSchemaCodec = new AvroCodec(selectedSchemaOpt.get)
      val selectedSchema = selectedSchemaCodec.chrononSchema.asInstanceOf[StructType]

      val noKeysSchemaCodec = new AvroCodec(noKeysSchemaOpt.get)
      val noKeysSchema = noKeysSchemaCodec.chrononSchema.asInstanceOf[StructType]

      Some((cardinalityMap, selectedSchema, noKeysSchema))
    } catch {
      case e: Exception =>
        logger.error(s"Failed to fetch metadata for $tableName: ${e.getMessage}")
        None
    }
  }

  /** Helper to fetch a single metadata value from KV Store
    *
    * @param key The metadata key
    * @param endTimeMillis Optional end time for time-dependent metadata (gets latest value up to this time)
    */
  private def getMetadataValue(key: String, endTimeMillis: Option[Long]): Option[String] = {
    try {
      val response = Await.result(
        kvStore.get(GetRequest(key.getBytes(charset), datasetName, None, endTimeMillis)),
        Duration(10L, TimeUnit.SECONDS)
      )
      response.values match {
        case Success(values) if values.nonEmpty =>
          // Get the latest value (last one in the list since they're sorted by time)
          Some(new String(values.last.bytes, charset))
        case _ =>
          logger.warn(s"Metadata not found for key: $key")
          None
      }
    } catch {
      case e: Exception =>
        logger.error(s"Failed to retrieve metadata for $key: ${e.getMessage}")
        None
    }
  }

  /** Build a RowAggregator from stored metadata.
    * Reconstructs the original aggregator using cardinalityMap, selectedSchema, and noKeysSchema.
    *
    * @param tableName The table name
    * @param endTimeMillis Optional end time to get the latest cardinalityMap
    */
  private def buildAggregatorFromMetadata(tableName: String,
                                          endTimeMillis: Option[Long] = None): Option[RowAggregator] = {
    fetchMetadata(tableName, endTimeMillis).map { case (cardinalityMap, selectedSchema, noKeysSchema) =>
      logger.info(s"Reconstructing aggregator for $tableName with ${cardinalityMap.size} columns")

      // Rebuild the enhanced metrics using the stored metadata
      // buildEnhancedMetrics expects Seq[(String, DataType)]
      val noKeysFields = noKeysSchema.fields.map(f => (f.name, f.fieldType)).toSeq
      val enhancedMetrics = StatsGenerator.buildEnhancedMetrics(
        noKeysFields,
        cardinalityMap,
        cardinalityThreshold = 0.01
      )

      // Build the aggregator with the reconstructed metrics
      StatsGenerator.buildAggregator(enhancedMetrics, selectedSchema)
    }
  }

  /** Add derived features to the statistics map.
    * Computes additional metrics based on existing statistics:
    * - std_dev: Standard deviation from variance (sqrt of variance)
    * - false_sum: Count of false values (total_count - true_sum - null_sum)
    *
    * @param statsMap The original statistics map
    * @return Enhanced statistics map with derived features
    */
  private def addDerivedFeatures(statsMap: Map[String, Any]): Map[String, Any] = {
    var enhanced = statsMap

    // Add standard deviation for variance fields
    statsMap.foreach { case (key, value) =>
      if (key.endsWith("_variance")) {
        val baseKey = key.stripSuffix("_variance")
        val stdDevKey = s"${baseKey}_std_dev"
        value match {
          case v: Double => enhanced = enhanced + (stdDevKey -> Math.sqrt(v))
          case v: Float  => enhanced = enhanced + (stdDevKey -> Math.sqrt(v.toDouble))
          case _         => // Skip non-numeric variance
        }
      }
    }

    // Add false_sum for true_sum fields
    statsMap.foreach { case (key, value) =>
      if (key.endsWith("_true_sum")) {
        val baseKey = key.stripSuffix("_true_sum")
        val falseSumKey = s"${baseKey}_false_sum"
        val nullSumKey = s"${baseKey}__null_sum"

        (statsMap.get("total_count"), value, statsMap.get(nullSumKey)) match {
          case (Some(total: Long), trueSum: Long, Some(nullSum: Long)) =>
            enhanced = enhanced + (falseSumKey -> (total - trueSum - nullSum))
          case _ => // Skip if required fields are missing
        }
      }
    }

    enhanced
  }
}

/** Java-friendly response object for stats queries
  */
case class JavaStatsResponse(
    success: Boolean,
    tableName: String,
    statistics: java.util.Map[String, Any],
    tilesCount: Int,
    skippedTilesCount: Int,
    errorMessage: String
) {
  def isSuccess: Boolean = success
  def getStatistics: java.util.Map[String, Any] = statistics
  def getTilesCount: Int = tilesCount
  def getSkippedTilesCount: Int = skippedTilesCount
  def getErrorMessage: String = errorMessage
  def getTableName: String = tableName
}

object JavaStatsResponse {
  def success(tableName: String,
              stats: java.util.Map[String, Any],
              tilesCount: Int,
              skippedTilesCount: Int = 0): JavaStatsResponse =
    JavaStatsResponse(true, tableName, stats, tilesCount, skippedTilesCount, null)

  def failure(errorMessage: String): JavaStatsResponse =
    JavaStatsResponse(false, null, java.util.Collections.emptyMap(), 0, 0, errorMessage)
}
