package ai.chronon.flink.source

import ai.chronon.api.Extensions.{GroupByOps, MetadataOps}
import ai.chronon.flink.types.BatchIrRow
import ai.chronon.online.GroupByServingInfoParsed
import ai.chronon.online.serde.AvroConversions
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.data.RowData
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

import java.time.Duration

/** Builds a DataStream[BatchIrRow] from the GroupBy's upload Iceberg table.
  *
  * The Iceberg source operates in streaming monitor mode: it scans the latest partition
  * on startup, then monitors for new snapshots every `monitorInterval`.
  *
  * Configuration via props:
  *   - `iceberg.catalog.warehouse`: warehouse path (required for Iceberg source)
  *   - `iceberg.catalog.uri`: metastore URI (for hive catalog)
  *   - `iceberg.monitor.interval.minutes`: snapshot monitor interval (default: 30)
  *
  * Falls back to an idle (empty) stream if Iceberg is not configured.
  */
object BatchIrSourceBuilder {

  private val logger = LoggerFactory.getLogger(getClass)

  def build(env: StreamExecutionEnvironment,
            servingInfo: GroupByServingInfoParsed,
            props: Map[String, String]): DataStream[BatchIrRow] = {

    val warehouse = props.getOrElse("iceberg.catalog.warehouse", "")
    val monitorIntervalMin = props.getOrElse("iceberg.monitor.interval.minutes", "30").toLong

    if (warehouse.isEmpty) {
      logger.warn(s"No iceberg.catalog.warehouse configured for ${servingInfo.groupByOps.metaData.getName}. " +
        s"Returning idle batch IR stream (batch loading disabled).")
      return buildIdleStream(env, servingInfo.groupByOps.metaData.getName)
    }

    try {
      buildIcebergStream(env, servingInfo, props, warehouse, monitorIntervalMin)
    } catch {
      case e: Exception =>
        logger.error(s"Failed to create Iceberg source. Falling back to idle stream.", e)
        buildIdleStream(env, servingInfo.groupByOps.metaData.getName)
    }
  }

  private def buildIcebergStream(env: StreamExecutionEnvironment,
                                  servingInfo: GroupByServingInfoParsed,
                                  props: Map[String, String],
                                  warehouse: String,
                                  monitorIntervalMin: Long): DataStream[BatchIrRow] = {
    import org.apache.iceberg.catalog.TableIdentifier
    import org.apache.iceberg.flink.{CatalogLoader, TableLoader}
    import org.apache.iceberg.flink.source.IcebergSource

    val uploadTable = servingInfo.groupByOps.metaData.uploadTable
    val groupByName = servingInfo.groupByOps.metaData.getName
    val catalogProps = new java.util.HashMap[String, String]()
    catalogProps.put("type", props.getOrElse("iceberg.catalog.type", "hadoop"))
    catalogProps.put("warehouse", warehouse)
    props.get("iceberg.catalog.uri").foreach(catalogProps.put("uri", _))

    val catalogLoader = CatalogLoader.hadoop("chronon_catalog",
      new org.apache.hadoop.conf.Configuration(), catalogProps)
    val tableId = TableIdentifier.parse(uploadTable)
    val tableLoader = TableLoader.fromCatalog(catalogLoader, tableId)

    val icebergSource = IcebergSource
      .forRowData()
      .tableLoader(tableLoader)
      .streaming(true)
      .monitorInterval(Duration.ofMinutes(monitorIntervalMin))
      .build()

    // Iceberg source with idleness — prevents stalling the global watermark
    val batchWatermark = WatermarkStrategy
      .noWatermarks[RowData]()
      .withIdleness(Duration.ofMinutes(monitorIntervalMin + 5))

    val rawStream = env
      .fromSource(icebergSource, batchWatermark, s"Iceberg batch source for $uploadTable")
      .uid(s"iceberg-batch-source-$groupByName")
      .setParallelism(1)

    rawStream
      .flatMap(new BatchIrRowDecoder(servingInfo))
      .uid(s"batch-ir-decode-$groupByName")
      .name(s"Decode batch IR for $groupByName")
      .setParallelism(1)
  }

  /** Returns an empty stream that completes immediately. Used as fallback when Iceberg is not configured. */
  private def buildIdleStream(env: StreamExecutionEnvironment, groupByName: String): DataStream[BatchIrRow] = {
    env
      .fromElements(new BatchIrRow())
      .filter(_ => false)
      .uid(s"idle-batch-ir-source-$groupByName")
      .name(s"Idle batch IR source for $groupByName")
      .setParallelism(1)
      .returns(classOf[BatchIrRow])
  }
}

/** Decodes Iceberg RowData (key_bytes, value_bytes, key_json, value_json, ds) into BatchIrRow.
  * batchEnd is derived from the ds partition column — NOT from the static servingInfo.batchEndTsMillis.
  * Each new partition (ds=2026-03-29) produces a fresh batchEnd, ensuring onBatchUpdate accepts it.
  */
class BatchIrRowDecoder(servingInfo: GroupByServingInfoParsed)
    extends RichFlatMapFunction[RowData, BatchIrRow] {

  @transient private lazy val logger = LoggerFactory.getLogger(getClass)

  @transient private lazy val keyDecoder: Array[Byte] => java.util.List[Any] = {
    val keySchema = servingInfo.keyChrononSchema
    val converter = AvroConversions.genericRecordToChrononRowConverter(keySchema)
    val codec = servingInfo.keyCodec
    bytes: Array[Byte] => {
      val record = codec.decode(bytes)
      val decoded = converter(record)
      val keys = new java.util.ArrayList[Any](decoded.length)
      decoded.foreach(keys.add)
      keys
    }
  }

  @transient private lazy val dsParser: String => Long = {
    val fmt = new java.text.SimpleDateFormat(servingInfo.groupByServingInfo.getDateFormat)
    fmt.setTimeZone(java.util.TimeZone.getTimeZone("UTC"))
    ds: String => fmt.parse(ds).getTime
  }

  // Upload table columns: key_bytes(0), value_bytes(1), key_json(2), value_json(3), ds(4)
  private val DsColumnIndex = 4

  override def flatMap(row: RowData, out: Collector[BatchIrRow]): Unit = {
    try {
      val keyBytes = row.getBinary(0)
      val valueBytes = row.getBinary(1)
      if (keyBytes == null || valueBytes == null) return

      val entityKeys = keyDecoder(keyBytes)

      // Derive batchEnd from the ds partition column. Chronon convention:
      // batchEnd = after(ds) = ds + 1 day. Batch for ds=2026-03-28 covers
      // [Mar 28 00:00, Mar 29 00:00), so batchEnd = Mar 29 00:00.
      // See GroupByUpload: batchEndDate = partitionSpec.after(endDs).
      val DayMillis = 24 * 3600 * 1000L
      val batchEnd = if (row.getArity > DsColumnIndex && !row.isNullAt(DsColumnIndex)) {
        dsParser(row.getString(DsColumnIndex).toString) + DayMillis
      } else {
        servingInfo.batchEndTsMillis
      }

      out.collect(new BatchIrRow(entityKeys, valueBytes, batchEnd))
    } catch {
      case e: Exception =>
        logger.error("Error decoding batch IR row from Iceberg", e)
    }
  }
}
