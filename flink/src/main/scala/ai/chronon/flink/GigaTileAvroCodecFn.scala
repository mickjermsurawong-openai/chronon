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
  * Dataset: pushDataset (*_PUSH) — simple key→value, no time-series sort key.
  */
case class GigaTileAvroCodecFn(groupByServingInfoParsed: GroupByServingInfoParsed, enableDebug: Boolean = false)
    extends RichFlatMapFunction[TimestampedTile, AvroCodecOutput] {

  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
  @transient private var avroConversionErrorCounter: Counter = _
  @transient private var eventProcessingErrorCounter: Counter = _

  // Use pushDataset (*_PUSH) — not streamingDataset (*_STREAMING).
  // _STREAMING tables in DynamoDB have a sort key (timestamp) causing GetItem failures
  // on plain entity keys. _PUSH tables are simple key→value stores.
  private lazy val dataset: String = groupByServingInfoParsed.groupByOps.pushDataset
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
      val entityKeyBytes = keyToBytes(value.keys.toArray)

      if (enableDebug) {
        logger.info(
          s"Push PutRequest: groupBy=${groupByServingInfoParsed.groupBy.getMetaData.getName} " +
            s"valueBytes=${value.tileBytes.length} bytes")
      }

      // Plain entity key, simple key→value dataset, current time as write timestamp.
      // _PUSH tables have no sort key, so each write overwrites the previous value.
      out.collect(new AvroCodecOutput(entityKeyBytes, value.tileBytes, dataset,
        value.latestTsMillis, value.startProcessingTime))
    } catch {
      case e: Exception =>
        logger.error("Error converting push tile to PutRequest", e)
        eventProcessingErrorCounter.inc()
        avroConversionErrorCounter.inc()
    }
}
