package ai.chronon.flink

import ai.chronon.api.Extensions.GroupByOps
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.api.{StructType => ChrononStructType}
import ai.chronon.flink.types.{AvroCodecOutput, TimestampedTile}
import ai.chronon.online.GroupByServingInfoParsed
import ai.chronon.online.serde.AvroConversions
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.metrics.Counter
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

/** Converts giga tile output (TimestampedTile with finalized vector bytes) to KV PutRequests.
  * Key: plain entity key bytes (no TileKey wrapper, no day suffix).
  * Value: finalized feature vector bytes (passthrough).
  */
case class GigaTileAvroCodecFn(groupByServingInfoParsed: GroupByServingInfoParsed, enableDebug: Boolean = false)
    extends RichFlatMapFunction[TimestampedTile, AvroCodecOutput] {

  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
  @transient private var avroConversionErrorCounter: Counter = _
  @transient private var eventProcessingErrorCounter: Counter = _

  private lazy val streamingDataset: String = groupByServingInfoParsed.groupBy.streamingDataset
  private lazy val keyColumns: Array[String] =
    SparkExpressionEval.buildKeyValueEventTimeColumns(groupByServingInfoParsed.groupBy)._1
  private lazy val keyToBytes: Any => Array[Byte] = {
    val keyZSchema: ChrononStructType = groupByServingInfoParsed.keyChrononSchema
    AvroConversions.encodeBytes(keyZSchema,
                                {
                                  case x: Map[_, _] if x.keys.forall(_.isInstanceOf[String]) =>
                                    x.toArray.flatMap { case (key, value) => Array(key, value) }
                                })
  }

  override def open(configuration: Configuration): Unit = {
    super.open(configuration)
    val metricsGroup = getRuntimeContext.getMetricGroup
      .addGroup("chronon")
      .addGroup("feature_group", groupByServingInfoParsed.groupBy.getMetaData.getName)
    avroConversionErrorCounter = metricsGroup.counter("avro_conversion_errors")
    eventProcessingErrorCounter = metricsGroup.counter("event_processing_error")
  }

  override def flatMap(value: TimestampedTile, out: Collector[AvroCodecOutput]): Unit =
    try {
      val tsMills = value.latestTsMillis
      val entityKeyBytes = keyToBytes(value.keys.toArray)

      if (enableDebug) {
        logger.info(
          s"Giga tile PutRequest: groupBy=${groupByServingInfoParsed.groupBy.getMetaData.getName} " +
            s"tsMills=$tsMills valueBytes=${value.tileBytes.length} bytes")
      }

      // Plain entity key — no TileKey wrapper. Single entry per entity, overwritten on every emit.
      out.collect(new AvroCodecOutput(entityKeyBytes, value.tileBytes, streamingDataset, tsMills,
        value.startProcessingTime))
    } catch {
      case e: Exception =>
        logger.error("Error converting giga tile to PutRequest", e)
        eventProcessingErrorCounter.inc()
        avroConversionErrorCounter.inc()
    }
}
