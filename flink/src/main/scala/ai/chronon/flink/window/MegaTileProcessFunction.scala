package ai.chronon.flink.window

import ai.chronon.aggregator.windowing.{MegaTileAggregator, MegaTileStreamProcessor, TileStore}
import ai.chronon.api.ScalaJavaConversions.ListOps
import ai.chronon.api.{Constants, DataType, GroupBy, TsUtils}
import ai.chronon.flink.FlinkJob
import ai.chronon.flink.deser.ProjectedEvent
import ai.chronon.flink.types.TimestampedTile
import ai.chronon.online.MegaTileCodec
import ai.chronon.online.serde.ArrayRow
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.metrics.Counter
import org.apache.flink.streaming.api.{TimeDomain, TimerService}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import scala.util.Try

sealed trait MegaTileEmissionPolicy extends Serializable

object MegaTileEmissionPolicy {

  case object DirtyBufferWithJitter extends MegaTileEmissionPolicy

  case object WallClockCadence extends MegaTileEmissionPolicy

  val DirtyBufferWithJitterName = "dirty_buffer_with_jitter"
  val WallClockCadenceName = "wall_clock_cadence"
  val Default: MegaTileEmissionPolicy = DirtyBufferWithJitter
  val DefaultName: String = DirtyBufferWithJitterName

  def fromString(raw: String): MegaTileEmissionPolicy =
    Option(raw).map(_.trim.toLowerCase) match {
      case Some(DirtyBufferWithJitterName) => DirtyBufferWithJitter
      case Some(WallClockCadenceName)      => WallClockCadence
      case _ =>
        throw new IllegalArgumentException(
          s"Unsupported MegaTile emission policy '$raw'. Expected one of: " +
            s"$DirtyBufferWithJitterName, $WallClockCadenceName")
    }
}

/** Flink KeyedProcessFunction that maintains per-entity mega tile state.
  * Delegates aggregation logic to MegaTileStreamProcessor and uses FlinkTileStore for keyed state.
  * See docs/source/megatile.md for the high-level clock/mode mental model.
  *
  * Step-by-step mental model in code order:
  *
  * Event path (`processElement`)
  * 1. Bind the current Flink key to its private MegaTile state box, then parse the projected event
  *    into `(eventTs, row)`.
  *    - Example: `List("user_123")` and `List("user_456")` each have separate tiles, day state,
  *      dirty bits, and timers. An event with `ts = 11:03:17` becomes one row applied only to that
  *      key's state.
  *
  * 2. Roll day state using the Flink watermark, not this event's timestamp or processing time (PT).
  *    If the watermark crosses midnight while today's row is still buffered, emit or buffer the
  *    old-day row first.
  *    - Why watermark: if one bad/future event arrives with `eventTs = tomorrow 00:01` and we
  *      rotated day state from that event timestamp, then later valid events from `today 23:50`
  *      would get misclassified into yesterday or dropped. The watermark means the stream has
  *      progressed past this event-time point, so day rollover only happens when Flink believes the
  *      whole stream is safely past midnight.
  *    - Where the watermark comes from: `FlinkJob.watermarkStrategy` assigns event timestamps and
  *      builds a bounded-out-of-orderness watermark. Each operator subtask sees the minimum
  *      watermark over its active partitions/input channels, so one input that is still behind can
  *      hold the watermark back.
  *
  * 3. Drop events older than yesterday relative to `currentDayStart`; otherwise call
  *    `processor.onEvent(row, eventTs, smallWindowAsOfTs)`. The processor uses `eventTs` for the
  *    base tile and today/yesterday bucket; `smallWindowAsOfTs` is the as-of timestamp for the
  *    cached small-window IR.
  *    - Example: with `currentDayStart = Apr 2 00:00`, `Apr 2 11:03` updates today's 11:00-11:05
  *      tile, `Apr 1 23:59` updates yesterday's large-window bucket, and `Mar 31 23:59` is dropped.
  *
  * 4. Mark `todayDirty` / `yesterdayDirty` from the processor result.
  *    - Why: state mutation and downstream writes are decoupled. Dirty bits remember which day rows
  *      need to be published later.
  *
  * 5. Keep one PT eviction timer per key at the next hop boundary.
  *    - Eviction's job is correctness of the in-memory state: drop expired 5-minute tiles, rebuild
  *      the small-window sawtooth IR, and mark today's row dirty if the rebuilt row changed.
  *    - Example: an 11:03 event lands in the 11:00-11:05 tile; the 11:05 eviction timer rebuilds
  *      the 1h value from retained 5-minute tiles so old tiles eventually fall out and values decay.
  *
  * 6. Emit immediately if buffering is disabled; otherwise keep one PT emit timer per key.
  *    - Emission's job is write coalescing only: if today/yesterday is dirty, serialize the current
  *      row and write it downstream. The first dirty update arms the timer; later updates only set
  *      dirty bits so hot keys do not emit once per event.
  *    - `DirtyBufferWithJitter` schedules relative to the first dirty write. `WallClockCadence`
  *      uses a stable key phase in live-like modes, with dirty-buffered fallback before the first
  *      watermark and during active catchup.
  *
  * Timer path (`onTimer`)
  * 7. Dispatch each PT callback into exactly one branch: evict+emit collision, evict-only, or
  *    emit-only.
  *
  * 8. On an eviction timer, choose the eviction as-of timestamp, maybe emit or buffer today's
  *    pre-rollover row, roll day state, drop expired small-window tiles, rebuild today's
  *    small-window state, mark dirty state, and re-arm the eviction heartbeat while small-window
  *    tiles still exist.
  *    - During replay lag, eviction uses the next watermark hop while this key is actively receiving
  *      events; otherwise eviction uses PT.
  *    - Example: if PT is Apr 2 11:00 but replayed events are from Mar 31 11:00, eviction should
  *      use Mar 31 watermark time while replay is active; otherwise PT eviction would jump
  *      `currentDayStart` to Apr 2 and make valid Mar 31 rows look older than yesterday.
  *    - In a normal live stream, watermark is already close to PT, so the otherwise branch is the
  *      expected path. A small slack above one hop prevents normal watermark jitter from looking
  *      like replay. If upstream stops for a key, that same branch keeps PT eviction running so
  *      values still decay with no new events.
  *
  * 9. On an emit timer, live cadence may roll day from PT first. Then emit each pending or dirty
  *    day row once and clear its dirty bit or pending buffer.
  */
class MegaTileProcessFunction(
    groupBy: GroupBy,
    inputSchema: Seq[(String, DataType)],
    enableDebug: Boolean = false,
    bufferingOutputTimeMillis: Long = 0L,
    bufferingOutputJitterMillis: Long = 0L,
    emissionPolicy: MegaTileEmissionPolicy = MegaTileEmissionPolicy.Default
) extends KeyedProcessFunction[java.util.List[Any], ProjectedEvent, TimestampedTile] {

  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  @transient private var processor: MegaTileStreamProcessor = _
  @transient private var megaTileCodec: MegaTileCodec = _
  @transient private var flinkStore: FlinkTileStore = _

  @transient private var eventProcessingErrorCounter: Counter = _
  @transient private var lastKey: java.util.List[Any] = _

  private val valueColumns: Array[String] = inputSchema.map(_._1).toArray
  private val timeColumnAlias: String = Constants.TimeColumn

  // Flink managed state
  private var tileState: MapState[String, Array[Byte]] = _
  private var megaTileIrState: ValueState[Array[Byte]] = _
  private var largeTodayIrState: ValueState[Array[Byte]] = _
  private var largeYesterdayIrState: ValueState[Array[Byte]] = _
  private var currentDayStartState: ValueState[java.lang.Long] = _
  private var earliestTileStartState: ValueState[java.lang.Long] = _
  private var pendingDayRollEmitTileBytesState: MapState[java.lang.Long, Array[Byte]] = _

  private var nextEvictPtTimerState: ValueState[java.lang.Long] = _
  private var nextEmitPtTimerState: ValueState[java.lang.Long] = _
  private var lastEventProcessingTsState: ValueState[java.lang.Long] = _
  private var todayDirtyState: ValueState[java.lang.Boolean] = _
  private var yesterdayDirtyState: ValueState[java.lang.Boolean] = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    val metricsGroup = getRuntimeContext.getMetricGroup
      .addGroup("chronon")
      .addGroup("feature_group", groupBy.getMetaData.getName)
    eventProcessingErrorCounter = metricsGroup.counter("event_processing_error")

    if (emissionPolicy == MegaTileEmissionPolicy.WallClockCadence &&
        bufferingOutputJitterMillis > 0L) {
      logger.warn(
        s"MegaTile emission policy ${MegaTileEmissionPolicy.WallClockCadenceName} ignores " +
          s"bufferingOutputJitterMillis=$bufferingOutputJitterMillis for " +
          s"groupBy=${groupBy.getMetaData.getName}; cadence spread comes from wall-clock phase")
    }

    tileState = getRuntimeContext.getMapState(
      new MapStateDescriptor[String, Array[Byte]]("mega-tile-tiles", classOf[String], classOf[Array[Byte]]))
    megaTileIrState =
      getRuntimeContext.getState(new ValueStateDescriptor[Array[Byte]]("mega-tile-ir", classOf[Array[Byte]]))
    largeTodayIrState =
      getRuntimeContext.getState(new ValueStateDescriptor[Array[Byte]]("mega-tile-large-today", classOf[Array[Byte]]))
    largeYesterdayIrState = getRuntimeContext.getState(
      new ValueStateDescriptor[Array[Byte]]("mega-tile-large-yesterday", classOf[Array[Byte]]))
    currentDayStartState = getRuntimeContext.getState(
      new ValueStateDescriptor[java.lang.Long]("mega-tile-day-start", classOf[java.lang.Long]))
    earliestTileStartState = getRuntimeContext.getState(
      new ValueStateDescriptor[java.lang.Long]("mega-tile-earliest-tile", classOf[java.lang.Long]))
    pendingDayRollEmitTileBytesState = getRuntimeContext.getMapState(
      new MapStateDescriptor[java.lang.Long, Array[Byte]](
        "mega-tile-pending-emit-tile-bytes",
        classOf[java.lang.Long],
        classOf[Array[Byte]]))
    nextEvictPtTimerState = getRuntimeContext.getState(
      new ValueStateDescriptor[java.lang.Long]("mega-tile-next-evict-pt-timer", classOf[java.lang.Long]))
    nextEmitPtTimerState = getRuntimeContext.getState(
      new ValueStateDescriptor[java.lang.Long]("mega-tile-next-emit-pt-timer", classOf[java.lang.Long]))
    lastEventProcessingTsState = getRuntimeContext.getState(
      new ValueStateDescriptor[java.lang.Long]("mega-tile-last-event-processing-ts", classOf[java.lang.Long]))
    todayDirtyState = getRuntimeContext.getState(
      new ValueStateDescriptor[java.lang.Boolean]("mega-tile-today-dirty", classOf[java.lang.Boolean]))
    yesterdayDirtyState = getRuntimeContext.getState(
      new ValueStateDescriptor[java.lang.Boolean]("mega-tile-yesterday-dirty", classOf[java.lang.Boolean]))

    initializeTransients()
  }

  private def initializeTransients(): Unit = {
    val inputCols = inputSchema.map { case (name, dt) => (name, dt) }
    val megaTileAgg = new MegaTileAggregator(groupBy.getAggregations.toScala.toSeq, inputCols)
    megaTileCodec = new MegaTileCodec(groupBy, inputCols)
    flinkStore = new FlinkTileStore(megaTileAgg, megaTileCodec)
    processor = new MegaTileStreamProcessor(megaTileAgg, flinkStore)
  }

  private def bindCurrentKey(currentKey: java.util.List[Any]): Unit = {
    if (lastKey == null || !lastKey.equals(currentKey)) {
      lastKey = currentKey
      flinkStore.bindFlinkState(tileState,
                                megaTileIrState,
                                largeTodayIrState,
                                largeYesterdayIrState,
                                currentDayStartState,
                                earliestTileStartState)
    }
  }

  override def processElement(
      event: ProjectedEvent,
      ctx: KeyedProcessFunction[java.util.List[Any], ProjectedEvent, TimestampedTile]#Context,
      out: Collector[TimestampedTile]
  ): Unit = {
    try {
      if (processor == null) initializeTransients()

      bindCurrentKey(ctx.getCurrentKey)

      val element = event.fields
      val tsMills = Try(element(timeColumnAlias).asInstanceOf[Long])
        .getOrElse(element(timeColumnAlias).asInstanceOf[Double].toLong)
      val values: Array[Any] = valueColumns.map(element(_))
      val row = new ArrayRow(values, tsMills)

      val timerService = ctx.timerService()
      val watermark = timerService.currentWatermark()
      val processingTs = timerService.currentProcessingTime()
      lastEventProcessingTsState.update(processingTs)

      // Step 2. Event ingestion rolls day state from watermark, not processing time, so a brief
      // source lag near midnight does not move `currentDayStart` too early.
      emitOrBufferTodayThenDayRoll(watermark, ctx.getCurrentKey, event.startProcessingTimeMillis, out)
      if (isOlderThanYesterdayForCurrentDay(tsMills)) {
        scheduleEvictTimerIfNeeded(timerService, processingTs)
        emitDirtyOrSchedule(event.startProcessingTimeMillis, processingTs, watermark, timerService, out)
        return
      }

      val mode = currentMode(processingTs, watermark)
      val smallWindowAsOfTs = smallWindowAsOfTsForEvent(mode, tsMills, processingTs, watermark)
      // Step 3.
      val result = processor.onEvent(row, tsMills, smallWindowAsOfTs)
      // Step 4.
      markDirtyState(result.todayEntry != null, result.yesterdayEntry != null)

      // Step 5.
      scheduleEvictTimerIfNeeded(timerService, processingTs)
      // Step 6.
      emitDirtyOrSchedule(event.startProcessingTimeMillis, processingTs, watermark, timerService, out)
    } catch {
      case e: Exception =>
        logger.error(s"Error processing mega tile event for groupBy=${groupBy.getMetaData.getName}", e)
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

      bindCurrentKey(ctx.getCurrentKey)
      if (ctx.timeDomain() != TimeDomain.PROCESSING_TIME) {
        return
      }

      val timerService = ctx.timerService()
      val processingTs = timerService.currentProcessingTime()
      val watermark = timerService.currentWatermark()
      val isEmitTimer = isCurrentEmitTimer(timestamp)
      val isEvictTimer = isCurrentEvictTimer(timestamp)
      if (isEmitTimer) {
        nextEmitPtTimerState.clear()
      }
      if (isEvictTimer) {
        nextEvictPtTimerState.clear()
      }

      if (isEvictTimer && isEmitTimer) {
        // Step 7 and Step 8.
        runEvictionTimer(ctx.getCurrentKey, timestamp, processingTs, watermark, timerService, out)
        // Step 9: this timestamp is also the buffered emit timer, so emit once after Step 8 rebuilt state.
        runEmitTimer(ctx.getCurrentKey, timerService, processingTs, watermark, out)
      } else if (isEvictTimer) {
        // Step 7 and Step 8.
        runEvictionTimer(ctx.getCurrentKey, timestamp, processingTs, watermark, timerService, out)
        // Step 9: eviction changed state; if buffering is enabled, arm one emit timer.
        emitDirtyOrSchedule(processingTs, processingTs, watermark, timerService, out)
      } else if (isEmitTimer) {
        // Step 7 and Step 9.
        runEmitTimer(ctx.getCurrentKey, timerService, processingTs, watermark, out)
      }
    } catch {
      case e: Exception =>
        logger.error(s"Error in mega tile eviction for groupBy=${groupBy.getMetaData.getName}", e)
        eventProcessingErrorCounter.inc()
    }
  }

  private def hasActiveSmallWindowState: Boolean =
    processor.hasSmallWindows &&
      Option(earliestTileStartState.value()).exists(_.longValue() != Long.MaxValue)

  private def isOlderThanYesterdayForCurrentDay(eventTs: Long): Boolean = {
    val currentDayStart = flinkStore.getCurrentDayStart
    currentDayStart != -1L && eventTs < currentDayStart - processor.DayMillis
  }

  private def bufferingEnabled: Boolean =
    bufferingOutputTimeMillis > 0L

  private def isTodayDirty: Boolean =
    Option(todayDirtyState.value()).exists(_.booleanValue())

  private def isYesterdayDirty: Boolean =
    Option(yesterdayDirtyState.value()).exists(_.booleanValue())

  private def hasPendingDayRollEmit: Boolean =
    !pendingDayRollEmitTileBytesState.isEmpty

  private def hasDirtyEmitState: Boolean =
    isTodayDirty || isYesterdayDirty || hasPendingDayRollEmit

  private def shouldKeepLiveCadenceTimer: Boolean =
    hasDirtyEmitState || hasActiveSmallWindowState

  private def markDirtyState(hasTodayUpdate: Boolean, hasYesterdayUpdate: Boolean): Unit = {
    if (hasTodayUpdate) todayDirtyState.update(java.lang.Boolean.TRUE)
    if (hasYesterdayUpdate) yesterdayDirtyState.update(java.lang.Boolean.TRUE)
  }

  private def runEvictionTimer(currentKey: java.util.List[Any],
                               timestamp: Long,
                               processingTs: Long,
                               watermark: Long,
                               timerService: TimerService,
                               out: Collector[TimestampedTile]): Unit = {
    val mode = currentMode(processingTs, watermark)
    val evictionTime = evictionTimeForProcessingTimer(mode, processingTs, watermark)
    emitOrBufferTodayThenDayRoll(evictionTime, currentKey, processingTs, out)

    val result = processor.onEviction(evictionTime)
    markDirtyState(result.todayEntry != null, hasYesterdayUpdate = false)
    scheduleEvictTimerIfNeeded(timerService, processingTs)

    if (enableDebug) {
      logger.info(
        s"MegaTile eviction groupBy=${groupBy.getMetaData.getName}, key=$currentKey, " +
          s"timerTs=$timestamp, evictionTime=$evictionTime, mode=$mode, " +
          s"watermark=$watermark, processingTs=$processingTs, dayStart=${flinkStore.getCurrentDayStart}, " +
          s"todayDirty=$isTodayDirty, yesterdayDirty=$isYesterdayDirty")
    }
  }

  /** Advance day state at `dayTransitionTs` without losing a buffered pre-rollover today row.
    *
    * Both input events and processing-time eviction timers can cross a day boundary:
    *   - `processElement` uses the current Flink watermark before applying the event.
    *   - `runEvictionTimer` uses either a watermark-aligned replay timestamp or wall-clock PT.
    *
    * If that timestamp moves `currentDayStart` forward by exactly one day and today's row is still
    * dirty, emit `packTodayEntry()` under the previous day key first. Then advance the processor's
    * day state. This keeps the final small-window row for the old day from being lost when
    * `advanceWatermark(...)` rotates `largeTodayIr` into yesterday and resets today.
    *
    * Emitting under `previousDayStart` preserves the logical daily key across rollover; `MegaTileAvroCodecFn`
    * uses that day-start timestamp when constructing the external `TileKey`.
    */
  private def emitOrBufferTodayThenDayRoll(dayTransitionTs: Long,
                                           keys: java.util.List[Any],
                                           processingTsMillis: Long,
                                           out: Collector[TimestampedTile]): Unit = {
    val previousDayStart = flinkStore.getCurrentDayStart
    val nextDayStart = previousDayStart + processor.DayMillis
    val shouldEmitPreviousTodayRow =
      previousDayStart != -1L &&
        isTodayDirty &&
        TsUtils.round(dayTransitionTs, processor.DayMillis) == nextDayStart
    if (shouldEmitPreviousTodayRow) {
      emitOrBufferDayRollMegaTile(keys, processor.packTodayEntry(), previousDayStart, processingTsMillis, out)
      todayDirtyState.clear()
    }

    processor.advanceWatermark(dayTransitionTs)
  }

  sealed private trait Mode

  private case object NoWatermark extends Mode

  private case object ActiveCatchup extends Mode

  private case object SparseKeyLag extends Mode

  private case object Live extends Mode

  sealed private trait EmitTimerScheduling

  private case class DirtyBufferedEmitTimer(maxJitterMillis: Long) extends EmitTimerScheduling

  private case object LiveCadenceEmitTimer extends EmitTimerScheduling

  private def currentMode(processingTs: Long, watermark: Long): Mode = {
    val lastEventProcessingTs =
      Option(lastEventProcessingTsState.value()).map(_.longValue()).getOrElse(Long.MinValue)
    val watermarkLaggingProcessingTime =
      watermark > Long.MinValue &&
        processingTs - watermark > processor.minSmallWindowTileSize + FlinkJob.CatchupWatermarkLagSlackMillis
    val keyRecentlySeenEvent =
      lastEventProcessingTs > Long.MinValue &&
        processingTs - lastEventProcessingTs <= processor.minSmallWindowTileSize

    if (watermark == Long.MinValue) {
      NoWatermark
    } else if (watermarkLaggingProcessingTime && keyRecentlySeenEvent) {
      ActiveCatchup
    } else if (watermarkLaggingProcessingTime && !keyRecentlySeenEvent) {
      SparseKeyLag
    } else {
      Live
    }
  }

  private def evictionTimeForProcessingTimer(mode: Mode, processingTs: Long, watermark: Long): Long =
    mode match {
      case ActiveCatchup =>
        nextSmallWindowHop(watermark)
      case NoWatermark | SparseKeyLag | Live =>
        processingTs
    }

  private def smallWindowAsOfTsForEvent(mode: Mode, eventTs: Long, processingTs: Long, watermark: Long): Long =
    mode match {
      case ActiveCatchup =>
        nextSmallWindowHop(watermark)
      case NoWatermark =>
        nextSmallWindowHop(eventTs)
      case SparseKeyLag | Live =>
        nextSmallWindowHop(processingTs)
    }

  private def nextSmallWindowHop(ts: Long): Long =
    TsUtils.round(ts, processor.minSmallWindowTileSize) + processor.minSmallWindowTileSize

  private def scheduleEvictTimerIfNeeded(timerService: TimerService, processingTs: Long): Unit = {
    if (!hasActiveSmallWindowState) {
      cancelEvictTimerIfPresent(timerService)
      return
    }

    val current = nextEvictPtTimerState.value()
    if (current != null) return

    val timestamp =
      TsUtils.round(processingTs, processor.minSmallWindowTileSize) + processor.minSmallWindowTileSize
    timerService.registerProcessingTimeTimer(timestamp)
    nextEvictPtTimerState.update(timestamp)
  }

  private def emitDirtyOrSchedule(outputProcessingTsMillis: Long,
                                  processingTs: Long,
                                  watermark: Long,
                                  timerService: TimerService,
                                  out: Collector[TimestampedTile]): Unit = {
    if (!bufferingEnabled) {
      emitDirtyMegaTiles(lastKey, outputProcessingTsMillis, out)
      return
    }
    val scheduling = emitTimerSchedulingFor(currentMode(processingTs, watermark))
    scheduleEmitTimerIfNeeded(timerService, processingTs, scheduling)
  }

  private def scheduleEmitTimerIfNeeded(timerService: TimerService,
                                        processingTs: Long,
                                        scheduling: EmitTimerScheduling): Unit =
    scheduling match {
      case DirtyBufferedEmitTimer(maxJitterMillis) =>
        scheduleDirtyBufferedEmitTimerIfNeeded(timerService, processingTs, maxJitterMillis)
      case LiveCadenceEmitTimer =>
        if (shouldKeepLiveCadenceTimer) {
          scheduleLiveCadenceTimer(timerService, processingTs)
        }
    }

  private def emitTimerSchedulingFor(mode: Mode): EmitTimerScheduling =
    emissionPolicy match {
      case MegaTileEmissionPolicy.DirtyBufferWithJitter =>
        DirtyBufferedEmitTimer(bufferingOutputJitterMillis)
      case MegaTileEmissionPolicy.WallClockCadence =>
        mode match {
          case NoWatermark | ActiveCatchup =>
            DirtyBufferedEmitTimer(maxJitterMillis = 0L)
          case SparseKeyLag | Live =>
            LiveCadenceEmitTimer
        }
    }

  private def scheduleLiveCadenceTimer(timerService: TimerService, processingTs: Long): Unit = {
    val current = nextEmitPtTimerState.value()
    if (current == null) {
      val timestamp = nextKeyWallClockEmitTimer(lastKey, processingTs)
      timerService.registerProcessingTimeTimer(timestamp)
      nextEmitPtTimerState.update(timestamp)
    }
  }

  private def scheduleDirtyBufferedEmitTimerIfNeeded(timerService: TimerService,
                                                     processingTs: Long,
                                                     maxJitterMillis: Long): Unit = {
    if (!hasDirtyEmitState) return

    val current = nextEmitPtTimerState.value()
    if (current == null) {
      val timestamp =
        processingTs + bufferingOutputTimeMillis + stableJitterMillis(lastKey, maxJitterMillis)
      timerService.registerProcessingTimeTimer(timestamp)
      nextEmitPtTimerState.update(timestamp)
    }
  }

  private def nextKeyWallClockEmitTimer(key: java.util.List[Any], processingTs: Long): Long = {
    val phase = stablePhaseMillis(key, bufferingOutputTimeMillis)
    val elapsedSincePhase = Math.floorMod(processingTs - phase, bufferingOutputTimeMillis)
    val delay =
      if (elapsedSincePhase == 0L) bufferingOutputTimeMillis
      else bufferingOutputTimeMillis - elapsedSincePhase
    processingTs + delay
  }

  private def stablePhaseMillis(key: java.util.List[Any], cadenceMillis: Long): Long =
    Math.floorMod((groupBy.getMetaData.getName :: key.toScala).hashCode().toLong, cadenceMillis)

  private def stableJitterMillis(key: java.util.List[Any], maxJitterMillis: Long): Long =
    if (maxJitterMillis <= 0L) 0L
    else Math.floorMod(key.hashCode().toLong, maxJitterMillis + 1L)

  private def cancelEvictTimerIfPresent(timerService: TimerService): Unit = {
    val current = nextEvictPtTimerState.value()
    if (current != null) {
      timerService.deleteProcessingTimeTimer(current.longValue())
      nextEvictPtTimerState.clear()
    }
  }

  private def isCurrentEmitTimer(timestamp: Long): Boolean = {
    val current = nextEmitPtTimerState.value()
    current != null && current.longValue() == timestamp
  }

  private def isCurrentEvictTimer(timestamp: Long): Boolean = {
    val current = nextEvictPtTimerState.value()
    current != null && current.longValue() == timestamp
  }

  private def runEmitTimer(keys: java.util.List[Any],
                           timerService: TimerService,
                           processingTsMillis: Long,
                           watermark: Long,
                           out: Collector[TimestampedTile]): Unit = {
    val mode = currentMode(processingTsMillis, watermark)
    val scheduling = emitTimerSchedulingFor(mode)
    scheduling match {
      case LiveCadenceEmitTimer =>
        // Large-window-only groupBys have no eviction timer; live cadence supplies their PT day roll.
        emitOrBufferTodayThenDayRoll(processingTsMillis, keys, processingTsMillis, out)
      case DirtyBufferedEmitTimer(_) =>
    }
    emitDirtyMegaTiles(keys, processingTsMillis, out)
    scheduleEmitTimerIfNeeded(timerService, processingTsMillis, scheduling)
  }

  private def emitDirtyMegaTiles(keys: java.util.List[Any],
                                 processingTsMillis: Long,
                                 out: Collector[TimestampedTile]): Unit = {
    val currentDayStart = flinkStore.getCurrentDayStart
    emitPendingDayRollMegaTile(keys, currentDayStart, processingTsMillis, out)
    if (isTodayDirty) {
      emitMegaTile(keys, processor.packTodayEntry(), currentDayStart, processingTsMillis, out)
      todayDirtyState.clear()
    }
    if (isYesterdayDirty) {
      emitMegaTile(keys,
                   processor.packYesterdayEntry(),
                   currentDayStart - processor.DayMillis,
                   processingTsMillis,
                   out)
      yesterdayDirtyState.clear()
    }
  }

  private def emitPendingDayRollMegaTile(keys: java.util.List[Any],
                                         currentDayStart: Long,
                                         processingTsMillis: Long,
                                         out: Collector[TimestampedTile]): Unit = {
    val pendingDayRollEmit =
      if (pendingDayRollEmitTileBytesState.isEmpty) {
        None
      } else {
        val iterator = pendingDayRollEmitTileBytesState.keys().iterator()
        val dayStartMillis = iterator.next().longValue()
        if (iterator.hasNext) {
          logger.warn(
            s"MegaTile expected at most one pending day-roll emit row for " +
              s"groupBy=${groupBy.getMetaData.getName}, key=$lastKey; dropping pending rows")
          None
        } else {
          Some(dayStartMillis -> pendingDayRollEmitTileBytesState.get(dayStartMillis))
        }
      }

    pendingDayRollEmit match {
      case Some((pendingDayStart, tileBytes)) =>
        val todayStart = currentDayStart
        val yesterdayStart = currentDayStart - processor.DayMillis
        val pendingIsRetainedDay =
          pendingDayStart == todayStart || pendingDayStart == yesterdayStart
        val pendingHasFresherDirtyRow =
          (pendingDayStart == todayStart && isTodayDirty) ||
            (pendingDayStart == yesterdayStart && isYesterdayDirty)

        if (pendingIsRetainedDay && !pendingHasFresherDirtyRow) {
          emitMegaTileBytes(keys, tileBytes, pendingDayStart, processingTsMillis, out)
        }
      case None =>
    }
    pendingDayRollEmitTileBytesState.clear()
  }

  private def emitOrBufferDayRollMegaTile(keys: java.util.List[Any],
                                          entry: Array[Any],
                                          dayStartMillis: Long,
                                          processingTsMillis: Long,
                                          out: Collector[TimestampedTile]): Unit = {
    if (!bufferingEnabled || emissionPolicy == MegaTileEmissionPolicy.DirtyBufferWithJitter) {
      emitMegaTile(keys, entry, dayStartMillis, processingTsMillis, out)
      return
    }

    if (hasPendingDayRollEmit) {
      emitPendingDayRollMegaTile(keys, dayStartMillis, processingTsMillis, out)
    }
    if (entry != null) {
      pendingDayRollEmitTileBytesState.put(dayStartMillis, megaTileCodec.encode(entry))
    }
  }

  private def emitMegaTile(keys: java.util.List[Any],
                           entry: Array[Any],
                           dayStartMillis: Long,
                           processingTsMillis: Long,
                           out: Collector[TimestampedTile]): Unit = {
    if (entry == null) return
    emitMegaTileBytes(keys, megaTileCodec.encode(entry), dayStartMillis, processingTsMillis, out)
  }

  private def emitMegaTileBytes(keys: java.util.List[Any],
                                tileBytes: Array[Byte],
                                dayStartMillis: Long,
                                processingTsMillis: Long,
                                out: Collector[TimestampedTile]): Unit =
    out.collect(new TimestampedTile(keys, tileBytes, dayStartMillis, processingTsMillis))
}

/** TileStore backed by Flink's keyed MapState/ValueState.
  * Encodes/decodes only the entries actually accessed - no bulk restore/persist.
  * Windowed IR get/put is memoized to avoid redundant decodes within the same event
  * (e.g., onEvent reads cachedSmallWindowIr, then packTodayEntry reads it again).
  * Cache is invalidated on key switch via bindFlinkState.
  */
class FlinkTileStore(megaTileAgg: MegaTileAggregator, codec: MegaTileCodec) extends TileStore {
  private val windowedAgg = megaTileAgg.windowedAggregator

  private var tileState: MapState[String, Array[Byte]] = _
  private var megaTileIrState: ValueState[Array[Byte]] = _
  private var largeTodayIrState: ValueState[Array[Byte]] = _
  private var largeYesterdayIrState: ValueState[Array[Byte]] = _
  private var currentDayStartState: ValueState[java.lang.Long] = _
  private var earliestTileStartState: ValueState[java.lang.Long] = _

  // Per-access decode cache for windowed IRs. Avoids redundant Avro decodes
  // when the same IR is read multiple times within one event (get -> update -> pack).
  // Invalidated on key switch (bindFlinkState) and updated on put.
  private var cachedSmallDecoded: Array[Any] = _
  private var cachedSmallValid: Boolean = false
  private var largeTodayDecoded: Array[Any] = _
  private var largeTodayValid: Boolean = false
  private var largeYesterdayDecoded: Array[Any] = _
  private var largeYesterdayValid: Boolean = false

  def bindFlinkState(tiles: MapState[String, Array[Byte]],
                     megaTileIr: ValueState[Array[Byte]],
                     largeToday: ValueState[Array[Byte]],
                     largeYesterday: ValueState[Array[Byte]],
                     dayStart: ValueState[java.lang.Long],
                     earliest: ValueState[java.lang.Long]): Unit = {
    tileState = tiles
    megaTileIrState = megaTileIr
    largeTodayIrState = largeToday
    largeYesterdayIrState = largeYesterday
    currentDayStartState = dayStart
    earliestTileStartState = earliest
    // Invalidate decode cache on key switch
    cachedSmallValid = false
    largeTodayValid = false
    largeYesterdayValid = false
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
}
