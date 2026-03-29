package ai.chronon.online

import ai.chronon.aggregator.windowing.{FinalBatchIr, MegaTileAggregator}
import ai.chronon.api.Extensions.MetadataOps
import ai.chronon.api.{DataType, GroupBy, StructType}
import ai.chronon.api.ScalaJavaConversions.IteratorOps
import ai.chronon.online.serde.{AvroCodec, AvroConversions}
import org.apache.avro.generic.GenericData

import java.util

/** Codec for the GigaTile pipeline. Three responsibilities:
  *
  * 1. Encode finalized feature vectors (output schema) — for KV writes.
  * 2. Decode FinalBatchIr from upload table bytes — for Iceberg ingestion.
  * 3. Encode windowed IR to bytes — for batch update mismatch detection.
  */
class GigaTileCodec(groupBy: GroupBy, inputSchema: Seq[(String, DataType)]) {

  private val megaTileAgg = new MegaTileAggregator(groupBy.getAggregations.iterator().toScala.toSeq, inputSchema)
  private val windowedAgg = megaTileAgg.windowedAggregator
  private val baseAgg = megaTileAgg.baseAggregator

  // --- Output encoding (finalized feature vector) ---

  val outputSchema: StructType =
    StructType.from(s"${groupBy.getMetaData.cleanName}_OUTPUT", windowedAgg.outputSchema)
  private val outputEncodeFn: Any => Array[Byte] = AvroConversions.encodeBytes(outputSchema, null)

  def encodeOutput(finalizedVector: Array[Any]): Array[Byte] = outputEncodeFn(finalizedVector)

  // --- Windowed IR encoding (for mismatch comparison) ---

  private val megaTileCodec = new MegaTileCodec(groupBy, inputSchema)

  def encodeWindowedIr(ir: Array[Any]): Array[Byte] = megaTileCodec.encode(ir)

  // --- Batch IR decoding (from upload table value_bytes) ---

  private val irSchema: StructType =
    StructType.from(s"${groupBy.getMetaData.cleanName}_IR", megaTileAgg.batchIrSchema)
  private val irAvroSchema: String = AvroConversions.fromChrononSchema(irSchema).toString()

  @transient private lazy val irCodec: AvroCodec = AvroCodec.of(irAvroSchema)
  @transient private lazy val irRowConverter: Any => Array[Any] =
    AvroConversions.genericRecordToChrononRowConverter(irSchema)

  def decodeBatchIr(bytes: Array[Byte]): FinalBatchIr = {
    if (bytes == null) return null
    val record = irCodec.decode(bytes)
    val batchRecord = irRowConverter(record)
    val collapsed = windowedAgg.denormalize(batchRecord(0).asInstanceOf[Array[Any]])
    val tailHops = batchRecord(1)
      .asInstanceOf[util.ArrayList[Any]]
      .iterator()
      .toScala
      .map(
        _.asInstanceOf[util.ArrayList[Any]]
          .iterator()
          .toScala
          .map(hop => baseAgg.denormalizeInPlace(hop.asInstanceOf[Array[Any]]))
          .toArray)
      .toArray
    FinalBatchIr(collapsed, tailHops)
  }

  // --- Batch IR encoding (for Flink state persistence) ---

  private val irEncodeFn: Any => Array[Byte] = AvroConversions.encodeBytes(irSchema, null)

  def encodeBatchIr(batchIr: FinalBatchIr): Array[Byte] = {
    if (batchIr == null) return null
    val normalizedCollapsed = windowedAgg.normalize(batchIr.collapsed)
    val normalizedHops: Any = if (batchIr.tailHops != null) {
      val hopList = new util.ArrayList[Any](batchIr.tailHops.length)
      for (hopArray <- batchIr.tailHops) {
        val innerList = new util.ArrayList[Any](hopArray.length)
        for (hop <- hopArray) {
          innerList.add(baseAgg.normalizeInPlace(hop.clone().asInstanceOf[Array[Any]]))
        }
        hopList.add(innerList)
      }
      hopList
    } else {
      null
    }
    irEncodeFn(Array[Any](normalizedCollapsed, normalizedHops))
  }
}
