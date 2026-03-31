package ai.chronon.aggregator.windowing

import ai.chronon.api.Row
import ai.chronon.api.TsUtils

import scala.collection.mutable

/** Pure Scala state manager for the GigaTile streaming pipeline.
  *
  * Extends the MegaTile approach: Flink holds batch IR in state (loaded from Iceberg)
  * and maintains a runningLargeIr that's the fully merged value (batch + streaming).
  * Emits finalized feature vectors instead of windowed IRs.
  *
  * State layout (extends MegaTile):
  *   - tiles, cachedSmallWindowIr: unchanged (small window sawtooth)
  *   - largeTodayIr, largeYesterdayIr: unchanged (daily accumulators)
  *   - batchIr: FinalBatchIr from Iceberg (collapsed + tail hops)
  *   - batchEndTs: when batch was last computed
  *   - runningLargeIr: fully merged large window IR (batch + streaming)
  */
class GigaTileStreamProcessor(
    val megaTileAgg: MegaTileAggregator,
    val store: GigaTileStore,
    // Returns true if two windowed IRs are equal (for batch update mismatch detection).
    // Default: always report mismatch (always emit on batch update).
    val irEqual: (Array[Any], Array[Any]) => Boolean = (_, _) => false
) {

  val DayMillis: Long = 24 * 3600 * 1000L

  private val windowedAgg = megaTileAgg.windowedAggregator
  private val baseAgg = megaTileAgg.baseAggregator
  private val isNoBatch = megaTileAgg.isNoBatch
  private val columnHopSize = megaTileAgg.columnHopSize

  val smallWindowTiers: Set[Long] = {
    val tiers = mutable.Set.empty[Long]
    var col = 0
    while (col < windowedAgg.length) {
      if (isNoBatch(col)) tiers += columnHopSize(col)
      col += 1
    }
    tiers.toSet
  }

  val hasSmallWindows: Boolean = smallWindowTiers.nonEmpty

  // Eviction cadence: smallest hop across ALL windows (not just small).
  // Large-window-only GroupBys need eviction for tail hop correction.
  val minEvictionInterval: Long = megaTileAgg.activeTiers.min

  // Tracks the packed IR from the last eviction emit. Used to suppress redundant KV writes
  // when nothing changed between evictions (idle entities, no tail hop shift).
  private var lastEvictionPackedIr: Array[Any] = _

  // Hop indices only used by small (NO BATCH) windows — stripped from batch IR on load.
  // 5-min tail hops for ≤12h windows are never used by mergeTailHopsForBatchColumns.
  private[windowing] val smallWindowOnlyHopIndices: Set[Int] = {
    val usedByBatch = mutable.Set.empty[Int]
    var col = 0
    while (col < windowedAgg.length) {
      val window = megaTileAgg.windowMappings(col).aggregationPart.window
      // Only windowed BATCH columns actually use tail hops
      if (!isNoBatch(col) && window != null) {
        usedByBatch += megaTileAgg.tailHopIndicesArray(col)
      }
      col += 1
    }
    (0 until megaTileAgg.hopSizesArray.length).filterNot(usedByBatch.contains(_)).toSet
  }

  def onEvent(row: Row, eventTs: Long): GigaEmitResult = {
    var currentDayStart = store.getCurrentDayStart
    if (currentDayStart == -1L) {
      currentDayStart = TsUtils.round(eventTs, DayMillis)
      store.putCurrentDayStart(currentDayStart)
    }

    var dirty = false

    // --- Small windows: update tiles + cachedSmallWindowIr ---
    if (hasSmallWindows) {
      var tilesUpdated = false
      val tileStarts = megaTileAgg.tileStartsForEvent(eventTs)
      for ((hopSize, tileStart) <- tileStarts) {
        if (smallWindowTiers.contains(hopSize)) {
          val floor = megaTileAgg.retentionFloor(hopSize, eventTs, currentDayStart)
          val ceiling = currentDayStart + 2 * DayMillis
          if (tileStart >= floor && tileStart < ceiling) {
            val existing = store.getTile(hopSize, tileStart)
            val ir = if (existing != null) existing else baseAgg.init
            baseAgg.update(ir, row)
            store.putTile(hopSize, tileStart, ir)
            val earliest = store.getEarliestTileStart
            if (tileStart < earliest) store.putEarliestTileStart(tileStart)
            tilesUpdated = true
          }
        }
      }

      if (tilesUpdated) {
        val cachedIr = store.getCachedSmallWindowIr
        var col = 0
        while (col < windowedAgg.length) {
          if (isNoBatch(col)) {
            windowedAgg.columnAggregators(col).update(cachedIr, row)
          }
          col += 1
        }
        store.putCachedSmallWindowIr(cachedIr)
        dirty = true
      }
    }

    // --- Large windows: update daily accumulator AND running sum ---
    val todayStart = currentDayStart
    val nextDayStart = todayStart + DayMillis
    val yesterdayStart = todayStart - DayMillis

    if (eventTs >= nextDayStart) {
      // Future event — clamp to today. Day transitions are watermark-driven.
      val todayIr = store.getLargeTodayIr
      updateLargeWindowColumns(todayIr, row)
      store.putLargeTodayIr(todayIr)
      val runningIr = store.getRunningLargeIr
      updateLargeWindowColumns(runningIr, row)
      store.putRunningLargeIr(runningIr)
      dirty = true
    } else if (eventTs >= todayStart) {
      val todayIr = store.getLargeTodayIr
      updateLargeWindowColumns(todayIr, row)
      store.putLargeTodayIr(todayIr)
      val runningIr = store.getRunningLargeIr
      updateLargeWindowColumns(runningIr, row)
      store.putRunningLargeIr(runningIr)
      dirty = true
    } else if (eventTs >= yesterdayStart) {
      // Late event from yesterday — within 2d tolerance
      val yesterdayIr = store.getLargeYesterdayIr
      updateLargeWindowColumns(yesterdayIr, row)
      store.putLargeYesterdayIr(yesterdayIr)
      val runningIr = store.getRunningLargeIr
      updateLargeWindowColumns(runningIr, row)
      store.putRunningLargeIr(runningIr)
      dirty = true
    }
    // Events < yesterdayStart: dropped for large windows (> 2d late)

    if (dirty) GigaEmitResult(packAndFinalize()) else GigaEmitResult(null)
  }

  /** Watermark-driven day transition. runningLargeIr is NOT reset — it's cumulative.
    * Eviction corrects tail hop selection at the next timer firing.
    */
  def advanceWatermark(watermarkTs: Long): Unit = {
    val currentDayStart = store.getCurrentDayStart
    if (currentDayStart == -1L) return
    val wmDay = TsUtils.round(watermarkTs, DayMillis)
    if (wmDay > currentDayStart) {
      val newYesterday =
        if (wmDay == currentDayStart + DayMillis) store.getLargeTodayIr
        else windowedAgg.init
      store.putLargeYesterdayIr(newYesterday)
      store.putLargeTodayIr(windowedAgg.init)
      store.putCurrentDayStart(wmDay)
    }
  }

  /** Periodic eviction: corrects small window sawtooth and large window tail hop selection. */
  def onEviction(timerTs: Long): GigaEmitResult = {
    val currentDayStart = store.getCurrentDayStart
    if (currentDayStart == -1L) return GigaEmitResult(null)

    // --- Small windows: evict stale tiles, rebuild cachedSmallWindowIr ---
    if (hasSmallWindows) {
      val earliest = store.getEarliestTileStart
      if (earliest != Long.MaxValue) {
        val staleEntries = mutable.ArrayBuffer.empty[(Long, Long)]
        val iter = store.tileIterator
        while (iter.hasNext) {
          val (hopSize, tileStart, _) = iter.next()
          if (smallWindowTiers.contains(hopSize)) {
            val floor = megaTileAgg.retentionFloor(hopSize, timerTs, currentDayStart)
            if (tileStart < floor) staleEntries += ((hopSize, tileStart))
          }
        }
        staleEntries.foreach { case (h, t) => store.removeTile(h, t) }

        val tiles: Map[Long, mutable.Map[Long, Array[Any]]] =
          smallWindowTiers.map(hop => hop -> mutable.Map.empty[Long, Array[Any]]).toMap
        var newEarliest = Long.MaxValue
        val rebuildIter = store.tileIterator
        while (rebuildIter.hasNext) {
          val (hopSize, tileStart, ir) = rebuildIter.next()
          tiles.get(hopSize).foreach(_(tileStart) = ir)
          if (tileStart < newEarliest) newEarliest = tileStart
        }
        store.putEarliestTileStart(newEarliest)

        val rebuiltIr = megaTileAgg.buildMegaTileIr(tiles, now = timerTs, batchEnd = currentDayStart)
        store.putCachedSmallWindowIr(rebuiltIr)
      }
    }

    // --- Large windows: recompute runningLargeIr from batch + streaming ---
    recomputeRunningLargeIr(timerTs, currentDayStart)

    // Suppress redundant emits: skip if the packed IR hasn't changed since last eviction.
    // For idle entities with no events and no tail hop shift, this avoids O(entities) KV writes per interval.
    val packed = pack()
    if (lastEvictionPackedIr != null && irEqual(lastEvictionPackedIr, packed)) {
      GigaEmitResult(null)
    } else {
      lastEvictionPackedIr = windowedAgg.clone(packed)
      GigaEmitResult(windowedAgg.finalize(packed))
    }
  }

  /** Process a new batch IR from the Iceberg connected stream.
    *
    * @param newBatchIr  decoded FinalBatchIr from the Iceberg upload table
    * @param newBatchEnd batch upload boundary timestamp (midnight-aligned)
    * @param currentWatermark Flink's current watermark (for tail hop selection as queryTs)
    */
  def onBatchUpdate(newBatchIr: FinalBatchIr, newBatchEnd: Long, currentWatermark: Long): GigaEmitResult = {
    val oldBatchEnd = store.getBatchEndTs
    if (newBatchEnd <= oldBatchEnd) return GigaEmitResult(null)

    val strippedBatchIr = stripSmallWindowHops(newBatchIr)
    store.putBatchIr(strippedBatchIr)
    store.putBatchEndTs(newBatchEnd)

    var currentDayStart = store.getCurrentDayStart

    if (newBatchEnd > currentDayStart) {
      if (currentDayStart < 0) {
        // Uninitialized (no events yet). Safe to set from batch — largeTodayIr is init,
        // no overlap concern. Without this, all startup batch loads would defer.
        currentDayStart = newBatchEnd
        store.putCurrentDayStart(currentDayStart)
      } else {
        // Defer: watermark hasn't caught up. largeTodayIr has events that overlap
        // with batch. Wait for advanceWatermark to rotate, then eviction recomputes.
        return GigaEmitResult(null, needsEvictionTimer = true)
      }
    }

    // Save old running for comparison
    val oldRunningIr = store.getRunningLargeIr

    // Clear yesterday if batch covers it
    if (newBatchEnd >= currentDayStart) {
      store.putLargeYesterdayIr(windowedAgg.init)
    }

    // Recompute running sum from batch + streaming
    recomputeRunningLargeIr(currentWatermark, currentDayStart)

    val newRunningIr = store.getRunningLargeIr

    // Emit only on mismatch
    if (!irEqual(oldRunningIr, newRunningIr)) {
      GigaEmitResult(packAndFinalize(), needsEvictionTimer = true)
    } else {
      GigaEmitResult(null, needsEvictionTimer = true)
    }
  }

  // --- Private helpers ---

  /** Recompute runningLargeIr from: batch (collapsed + tail hops) + streaming (today + yesterday).
    * For columns where the entire window has moved past batchEndTs, the collapsed value
    * is stale — zero it out so stale batch data doesn't persist for idle entities.
    */
  private def recomputeRunningLargeIr(queryTs: Long, currentDayStart: Long): Unit = {
    val batchIr = store.getBatchIr
    val batchEndTs = store.getBatchEndTs
    val runningIr = if (batchIr != null) {
      val ir = windowedAgg.clone(batchIr.collapsed)
      megaTileAgg.mergeTailHopsForBatchColumns(ir, queryTs, batchEndTs, batchIr)
      // Zero out columns where the window has moved entirely past batch data.
      // collapsed covers [alignedCollapsedBoundary, batchEnd) — if the window start
      // (queryTs - windowMillis) is past batchEnd, collapsed is outside the window.
      var col = 0
      while (col < windowedAgg.length) {
        val window = megaTileAgg.windowMappings(col).aggregationPart.window
        if (!isNoBatch(col) && window != null && queryTs - megaTileAgg.windowMappings(col).millis >= batchEndTs) {
          ir(col) = null
        }
        col += 1
      }
      ir
    } else {
      windowedAgg.init
    }

    // Merge streaming daily accumulators
    val todayIr = store.getLargeTodayIr
    val yesterdayIr = store.getLargeYesterdayIr
    var col = 0
    while (col < windowedAgg.length) {
      if (!isNoBatch(col)) {
        if (todayIr(col) != null)
          runningIr(col) = windowedAgg.columnAggregators(col).merge(runningIr(col), todayIr(col))
        if (batchEndTs < currentDayStart && yesterdayIr(col) != null)
          runningIr(col) = windowedAgg.columnAggregators(col).merge(runningIr(col), yesterdayIr(col))
      }
      col += 1
    }

    store.putRunningLargeIr(runningIr)
  }

  private def updateLargeWindowColumns(ir: Array[Any], row: Row): Unit = {
    var col = 0
    while (col < windowedAgg.length) {
      if (!isNoBatch(col)) {
        windowedAgg.columnAggregators(col).update(ir, row)
      }
      col += 1
    }
  }

  /** Pack small + large window columns into a single windowed IR (before finalization). */
  private def pack(): Array[Any] = {
    val cachedIr = store.getCachedSmallWindowIr
    val runningIr = store.getRunningLargeIr
    val packed = new Array[Any](windowedAgg.length)
    var col = 0
    while (col < windowedAgg.length) {
      packed(col) = if (isNoBatch(col)) cachedIr(col) else runningIr(col)
      col += 1
    }
    packed
  }

  /** Pack small + large window columns and finalize to output values. */
  private[windowing] def packAndFinalize(): Array[Any] = windowedAgg.finalize(pack())

  /** Strip tail hops that are only used by small windows to reduce state size.
    * 5-min hops for ≤12h windows are never consumed by mergeTailHopsForBatchColumns.
    */
  private[windowing] def stripSmallWindowHops(batchIr: FinalBatchIr): FinalBatchIr = {
    if (smallWindowOnlyHopIndices.isEmpty || batchIr.tailHops == null) return batchIr
    val strippedHops = batchIr.tailHops.clone()
    for (idx <- smallWindowOnlyHopIndices) {
      if (idx < strippedHops.length) {
        strippedHops(idx) = Array.empty
      }
    }
    FinalBatchIr(batchIr.collapsed, strippedHops)
  }
}

case class GigaEmitResult(
    finalizedVector: Array[Any],
    needsEvictionTimer: Boolean = false
)
