package ai.chronon.flink.window

import ai.chronon.aggregator.windowing.{FinalBatchIr, GigaTileStore, GigaTileStreamProcessor, MegaTileAggregator}
import ai.chronon.api.{Constants, DataType, GroupBy, TsUtils}
import ai.chronon.api.ScalaJavaConversions.IteratorOps
import ai.chronon.flink.SparkExpressionEval
import ai.chronon.flink.deser.ProjectedEvent
import ai.chronon.flink.types.TimestampedTile
import ai.chronon.online.{GigaTileCodec, MegaTileCodec}
import ai.chronon.online.serde.ArrayRow
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.metrics.Counter
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}

/** Flink KeyedProcessFunction for the GigaTile pipeline.
  *
  * Delegates all aggregation logic to GigaTileStreamProcessor.
  * Emits finalized feature vectors (not windowed IRs) to the KV store.
  *
  * For batch IR loading: call loadBatchIr() externally (e.g., from an Iceberg source
  * via a connected stream or broadcast). Currently handles events only — batch IR
  * integration requires upgrading to a CoProcessFunction (follow-up PR).
  */
class GigaTileProcessFunction(
    groupBy: GroupBy,
    inputSchema: Seq[(String, DataType)],
    enableDebug: Boolean = false
) extends KeyedProcessFunction[java.util.List[Any], ProjectedEvent, TimestampedTile] {

  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  @transient private var processor: GigaTileStreamProcessor = _
  @transient private var gigaTileCodec: GigaTileCodec = _
  @transient private var megaTileCodec: MegaTileCodec = _
  @transient private var flinkStore: FlinkGigaTileStore = _

  @transient private var eventProcessingErrorCounter: Counter = _
  @transient private var lastKey: java.util.List[Any] = _

  private val valueColumns: Array[String] = inputSchema.map(_._1).toArray
  private val timeColumnAlias: String = Constants.TimeColumn

  // Flink managed state (mega tile state)
  private var tileState: MapState[String, Array[Byte]] = _
  private var megaTileIrState: ValueState[Array[Byte]] = _
  private var largeTodayIrState: ValueState[Array[Byte]] = _
  private var largeYesterdayIrState: ValueState[Array[Byte]] = _
  private var currentDayStartState: ValueState[java.lang.Long] = _
  private var earliestTileStartState: ValueState[java.lang.Long] = _

  // Giga tile additional state
  private var batchIrState: ValueState[Array[Byte]] = _
  private var batchEndTsState: ValueState[java.lang.Long] = _
  private var runningLargeIrState: ValueState[Array[Byte]] = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    val metricsGroup = getRuntimeContext.getMetricGroup
      .addGroup("chronon")
      .addGroup("feature_group", groupBy.getMetaData.getName)
    eventProcessingErrorCounter = metricsGroup.counter("event_processing_error")

    tileState = getRuntimeContext.getMapState(
      new MapStateDescriptor[String, Array[Byte]]("giga-tile-tiles", classOf[String], classOf[Array[Byte]]))
    megaTileIrState =
      getRuntimeContext.getState(new ValueStateDescriptor[Array[Byte]]("giga-tile-ir", classOf[Array[Byte]]))
    largeTodayIrState =
      getRuntimeContext.getState(new ValueStateDescriptor[Array[Byte]]("giga-tile-large-today", classOf[Array[Byte]]))
    largeYesterdayIrState = getRuntimeContext.getState(
      new ValueStateDescriptor[Array[Byte]]("giga-tile-large-yesterday", classOf[Array[Byte]]))
    currentDayStartState = getRuntimeContext.getState(
      new ValueStateDescriptor[java.lang.Long]("giga-tile-day-start", classOf[java.lang.Long]))
    earliestTileStartState = getRuntimeContext.getState(
      new ValueStateDescriptor[java.lang.Long]("giga-tile-earliest-tile", classOf[java.lang.Long]))

    batchIrState =
      getRuntimeContext.getState(new ValueStateDescriptor[Array[Byte]]("giga-tile-batch-ir", classOf[Array[Byte]]))
    batchEndTsState = getRuntimeContext.getState(
      new ValueStateDescriptor[java.lang.Long]("giga-tile-batch-end", classOf[java.lang.Long]))
    runningLargeIrState = getRuntimeContext.getState(
      new ValueStateDescriptor[Array[Byte]]("giga-tile-running-large", classOf[Array[Byte]]))

    initializeTransients()
  }

  private def initializeTransients(): Unit = {
    val inputCols = inputSchema.map { case (name, dt) => (name, dt) }
    val megaTileAgg = new MegaTileAggregator(groupBy.getAggregations.iterator().toScala.toSeq, inputCols)
    gigaTileCodec = new GigaTileCodec(groupBy, inputCols)
    megaTileCodec = new MegaTileCodec(groupBy, inputCols)
    flinkStore = new FlinkGigaTileStore(megaTileAgg, megaTileCodec, gigaTileCodec)

    // Mismatch check: serialize-and-compare-bytes
    val irEqual: (Array[Any], Array[Any]) => Boolean = { (a, b) =>
      java.util.Arrays.equals(gigaTileCodec.encodeWindowedIr(a), gigaTileCodec.encodeWindowedIr(b))
    }

    processor = new GigaTileStreamProcessor(megaTileAgg, flinkStore, irEqual)
  }

  override def processElement(
      event: ProjectedEvent,
      ctx: KeyedProcessFunction[java.util.List[Any], ProjectedEvent, TimestampedTile]#Context,
      out: Collector[TimestampedTile]
  ): Unit = {
    try {
      if (processor == null) initializeTransients()

      val element = event.fields
      val tsMills = Try(element(timeColumnAlias).asInstanceOf[Long])
        .getOrElse(element(timeColumnAlias).asInstanceOf[Double].toLong)
      val values: Array[Any] = valueColumns.map(element(_))
      val row = new ArrayRow(values, tsMills)

      val currentKey = ctx.getCurrentKey
      if (lastKey == null || !lastKey.equals(currentKey)) {
        lastKey = currentKey
        flinkStore.bindFlinkState(
          tileState,
          megaTileIrState,
          largeTodayIrState,
          largeYesterdayIrState,
          currentDayStartState,
          earliestTileStartState,
          batchIrState,
          batchEndTsState,
          runningLargeIrState
        )
      }

      processor.advanceWatermark(ctx.timerService().currentWatermark())
      val result = processor.onEvent(row, tsMills)

      if (result.finalizedVector != null) {
        out.collect(
          new TimestampedTile(ctx.getCurrentKey,
                              gigaTileCodec.encodeOutput(result.finalizedVector),
                              tsMills,
                              event.startProcessingTimeMillis))
      }

      // Register eviction timer
      val nextEviction = TsUtils.round(tsMills, processor.minEvictionInterval) + processor.minEvictionInterval
      ctx.timerService().registerEventTimeTimer(nextEviction)
    } catch {
      case e: Exception =>
        logger.error(s"Error processing giga tile event for groupBy=${groupBy.getMetaData.getName}", e)
        eventProcessingErrorCounter.inc()
    }
  }

  override def onTimer(
      timestamp: Long,
      ctx: KeyedProcessFunction[java.util.List[Any], ProjectedEvent, TimestampedTile]#OnTimerContext,
      out: Collector[TimestampedTile]
  ): Unit = {
    try {
      if (processor == null) initializeTransients()

      val currentKey = ctx.getCurrentKey
      if (lastKey == null || !lastKey.equals(currentKey)) {
        lastKey = currentKey
        flinkStore.bindFlinkState(
          tileState,
          megaTileIrState,
          largeTodayIrState,
          largeYesterdayIrState,
          currentDayStartState,
          earliestTileStartState,
          batchIrState,
          batchEndTsState,
          runningLargeIrState
        )
      }

      processor.advanceWatermark(ctx.timerService().currentWatermark())
      val result = processor.onEviction(timestamp)

      if (result.finalizedVector != null) {
        out.collect(
          new TimestampedTile(ctx.getCurrentKey,
                              gigaTileCodec.encodeOutput(result.finalizedVector),
                              timestamp,
                              System.currentTimeMillis()))
      }

      // Re-register eviction timer
      ctx.timerService().registerEventTimeTimer(timestamp + processor.minEvictionInterval)
    } catch {
      case e: Exception =>
        logger.error(s"Error in giga tile eviction for groupBy=${groupBy.getMetaData.getName}", e)
        eventProcessingErrorCounter.inc()
    }
  }
}

/** TileStore backed by Flink state for GigaTile. Extends the mega tile state with batch IR storage. */
class FlinkGigaTileStore(megaTileAgg: MegaTileAggregator, codec: MegaTileCodec, gigaCodec: GigaTileCodec)
    extends GigaTileStore {
  private val windowedAgg = megaTileAgg.windowedAggregator

  private var tileState: MapState[String, Array[Byte]] = _
  private var megaTileIrState: ValueState[Array[Byte]] = _
  private var largeTodayIrState: ValueState[Array[Byte]] = _
  private var largeYesterdayIrState: ValueState[Array[Byte]] = _
  private var currentDayStartState: ValueState[java.lang.Long] = _
  private var earliestTileStartState: ValueState[java.lang.Long] = _
  private var batchIrState: ValueState[Array[Byte]] = _
  private var batchEndTsState: ValueState[java.lang.Long] = _
  private var runningLargeIrState: ValueState[Array[Byte]] = _

  // Decode cache (invalidated on key switch)
  private var cachedSmallDecoded: Array[Any] = _
  private var cachedSmallValid: Boolean = false
  private var largeTodayDecoded: Array[Any] = _
  private var largeTodayValid: Boolean = false
  private var largeYesterdayDecoded: Array[Any] = _
  private var largeYesterdayValid: Boolean = false
  private var batchIrDecoded: FinalBatchIr = _
  private var batchIrValid: Boolean = false
  private var runningLargeDecoded: Array[Any] = _
  private var runningLargeValid: Boolean = false

  def bindFlinkState(tiles: MapState[String, Array[Byte]],
                     megaTileIr: ValueState[Array[Byte]],
                     largeToday: ValueState[Array[Byte]],
                     largeYesterday: ValueState[Array[Byte]],
                     dayStart: ValueState[java.lang.Long],
                     earliest: ValueState[java.lang.Long],
                     batchIr: ValueState[Array[Byte]],
                     batchEndTs: ValueState[java.lang.Long],
                     runningLarge: ValueState[Array[Byte]]): Unit = {
    tileState = tiles
    megaTileIrState = megaTileIr
    largeTodayIrState = largeToday
    largeYesterdayIrState = largeYesterday
    currentDayStartState = dayStart
    earliestTileStartState = earliest
    batchIrState = batchIr
    batchEndTsState = batchEndTs
    runningLargeIrState = runningLarge
    // Invalidate all caches
    cachedSmallValid = false
    largeTodayValid = false
    largeYesterdayValid = false
    batchIrValid = false
    runningLargeValid = false
  }

  private def tileKey(hopSize: Long, tileStart: Long): String = s"$hopSize:$tileStart"

  override def getTile(hopSize: Long, tileStart: Long): Array[Any] = {
    val bytes = tileState.get(tileKey(hopSize, tileStart))
    if (bytes != null) codec.decodeBaseIr(bytes) else null
  }
  override def putTile(hopSize: Long, tileStart: Long, ir: Array[Any]): Unit =
    tileState.put(tileKey(hopSize, tileStart), codec.encodeBaseIr(ir))
  override def removeTile(hopSize: Long, tileStart: Long): Unit =
    tileState.remove(tileKey(hopSize, tileStart))
  override def tileIterator: Iterator[(Long, Long, Array[Any])] = {
    val iter = tileState.iterator()
    new Iterator[(Long, Long, Array[Any])] {
      override def hasNext: Boolean = iter.hasNext
      override def next(): (Long, Long, Array[Any]) = {
        val entry = iter.next()
        val parts = entry.getKey.split(":")
        (parts(0).toLong, parts(1).toLong, codec.decodeBaseIr(entry.getValue))
      }
    }
  }

  private def decodeWindowedIr(state: ValueState[Array[Byte]]): Array[Any] = {
    val bytes = state.value()
    if (bytes != null) codec.decode(bytes) else windowedAgg.init
  }

  override def getCachedSmallWindowIr: Array[Any] = {
    if (!cachedSmallValid) { cachedSmallDecoded = decodeWindowedIr(megaTileIrState); cachedSmallValid = true }
    cachedSmallDecoded
  }
  override def putCachedSmallWindowIr(ir: Array[Any]): Unit = {
    megaTileIrState.update(codec.encode(ir))
    cachedSmallDecoded = ir; cachedSmallValid = true
  }

  override def getLargeTodayIr: Array[Any] = {
    if (!largeTodayValid) { largeTodayDecoded = decodeWindowedIr(largeTodayIrState); largeTodayValid = true }
    largeTodayDecoded
  }
  override def putLargeTodayIr(ir: Array[Any]): Unit = {
    largeTodayIrState.update(codec.encode(ir))
    largeTodayDecoded = ir; largeTodayValid = true
  }

  override def getLargeYesterdayIr: Array[Any] = {
    if (!largeYesterdayValid) {
      largeYesterdayDecoded = decodeWindowedIr(largeYesterdayIrState); largeYesterdayValid = true
    }
    largeYesterdayDecoded
  }
  override def putLargeYesterdayIr(ir: Array[Any]): Unit = {
    largeYesterdayIrState.update(codec.encode(ir))
    largeYesterdayDecoded = ir; largeYesterdayValid = true
  }

  override def getCurrentDayStart: Long = Option(currentDayStartState.value()).map(_.longValue()).getOrElse(-1L)
  override def putCurrentDayStart(ts: Long): Unit = currentDayStartState.update(ts)

  override def getEarliestTileStart: Long =
    Option(earliestTileStartState.value()).map(_.longValue()).getOrElse(Long.MaxValue)
  override def putEarliestTileStart(ts: Long): Unit = earliestTileStartState.update(ts)

  // --- GigaTileStore batch state ---

  override def getBatchIr: FinalBatchIr = {
    if (!batchIrValid) {
      val bytes = batchIrState.value()
      batchIrDecoded = if (bytes != null) gigaCodec.decodeBatchIr(bytes) else null
      batchIrValid = true
    }
    batchIrDecoded
  }
  override def putBatchIr(ir: FinalBatchIr): Unit = {
    batchIrState.update(gigaCodec.encodeBatchIr(ir))
    batchIrDecoded = ir; batchIrValid = true
  }

  override def getBatchEndTs: Long = Option(batchEndTsState.value()).map(_.longValue()).getOrElse(-1L)
  override def putBatchEndTs(ts: Long): Unit = batchEndTsState.update(ts)

  override def getRunningLargeIr: Array[Any] = {
    if (!runningLargeValid) {
      runningLargeDecoded = decodeWindowedIr(runningLargeIrState)
      runningLargeValid = true
    }
    runningLargeDecoded
  }
  override def putRunningLargeIr(ir: Array[Any]): Unit = {
    runningLargeIrState.update(codec.encode(ir))
    runningLargeDecoded = ir; runningLargeValid = true
  }
}
