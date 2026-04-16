package ai.chronon.aggregator.windowing

import ai.chronon.api.TsUtils

/** Fetcher-side merge logic for the per-day MegaTile KV scheme.
  *
  * Flink writes one entry per entity/day key and may also update yesterday's key for late events.
  * The fetcher reads the day keys needed to bridge batchEnd -> queryTs plus one fallback key for
  * no-batch windows, then combines them with batch IR into a finalized result.
  *
  * Per-column merge semantics:
  *   - Small windows (≤ tailBuffer): daily entry, plus optional batch tail when the request path knows
  *     the streaming entries start at batchEnd.
  *   - Large windows (> tailBuffer): batch collapsed + streaming daily aggregates + tail hops.
  *   - Unwindowed: batch collapsed + streaming daily aggregates (no tail hops).
  */
class MegaTileMerger(megaTileAgg: MegaTileAggregator) {

  private val windowedAggregator = megaTileAgg.windowedAggregator
  private val isNoBatch = megaTileAgg.isNoBatch

  val DayMillis: Long = 24 * 3600 * 1000L

  /** Returns (todayStart, yesterdayStart) aligned to day boundaries. */
  def streamingDayKeys(now: Long): (Long, Long) = {
    val todayStart = TsUtils.round(now, DayMillis)
    (todayStart, todayStart - DayMillis)
  }

  /** Returns the day keys that must be fetched to merge stream and batch exactly.
    *
    * Batch-backed columns need every daily entry from the batch day through today.
    * No-batch columns still need one extra previous-day fallback key when today's row is missing.
    * Keys are returned newest-first so callers preserve fallback order for no-batch columns.
    */
  def streamingDayKeys(queryTs: Long, batchEnd: Long): Seq[Long] = {
    val todayStart = TsUtils.round(queryTs, DayMillis)
    val batchDayStart = TsUtils.round(batchEnd, DayMillis)
    val oldestDayStart = Math.min(batchDayStart, todayStart - DayMillis)

    Iterator
      .iterate(todayStart)(_ - DayMillis)
      .takeWhile(_ >= oldestDayStart)
      .toSeq
  }

  /** Merge batch IR + today's and yesterday's daily streaming entries into a finalized result.
    *
    * @param batchIr     Finalized batch IR (collapsed + tail hops). May be null.
    * @param todayIr     Today's daily entry from stream KV. May be null.
    * @param yesterdayIr Yesterday's daily entry from stream KV. May be null.
    * @param todayStart  Midnight timestamp for today (the KV key timestamp).
    * @param queryTs     The query timestamp (wall-clock now).
    * @param batchEnd    The batch upload boundary timestamp.
    * @return Finalized feature values.
    */
  def merge(batchIr: FinalBatchIr,
            todayIr: Array[Any],
            yesterdayIr: Array[Any],
            todayStart: Long,
            queryTs: Long,
            batchEnd: Long): Array[Any] =
    merge(batchIr, todayIr, yesterdayIr, todayStart, queryTs, batchEnd, mergeNoBatchTailFromBatch = false)

  def merge(batchIr: FinalBatchIr,
            todayIr: Array[Any],
            yesterdayIr: Array[Any],
            todayStart: Long,
            queryTs: Long,
            batchEnd: Long,
            mergeNoBatchTailFromBatch: Boolean): Array[Any] = {
    merge(
      batchIr,
      Seq(todayStart -> todayIr, (todayStart - DayMillis) -> yesterdayIr),
      queryTs,
      batchEnd,
      mergeNoBatchTailFromBatch
    )
  }

  /** Merge batch IR + N daily streaming entries into a finalized result.
    *
    * @param batchIr      Finalized batch IR (collapsed + tail hops). May be null.
    * @param dailyTileIrs Daily streaming entries keyed by day start, newest-first preferred. Null IRs are allowed.
    * @param queryTs      The query timestamp (wall-clock now).
    * @param batchEnd     The batch upload boundary timestamp.
    * @return Finalized feature values.
    */
  def merge(batchIr: FinalBatchIr, dailyTileIrs: Seq[(Long, Array[Any])], queryTs: Long, batchEnd: Long): Array[Any] =
    merge(batchIr, dailyTileIrs, queryTs, batchEnd, mergeNoBatchTailFromBatch = false)

  def merge(batchIr: FinalBatchIr,
            dailyTileIrs: Seq[(Long, Array[Any])],
            queryTs: Long,
            batchEnd: Long,
            mergeNoBatchTailFromBatch: Boolean): Array[Any] = {

    val resultIr =
      if (batchIr != null) windowedAggregator.clone(batchIr.collapsed)
      else windowedAggregator.init
    val oldestNoBatchDayStart = TsUtils.round(queryTs, DayMillis) - DayMillis
    val batchDayStart = TsUtils.round(batchEnd, DayMillis)
    val nonNullDailyTileIrs = dailyTileIrs.filter(_._2 != null).sortBy(_._1).reverse

    var col = 0
    while (col < windowedAggregator.length) {
      if (isNoBatch(col)) {
        // Fall back to an older day only if the newer entry is missing entirely,
        // not if a newer entry exists but this column value is null (no events in window).
        val newestAvailableIr = nonNullDailyTileIrs.collectFirst {
          case (dayStart, dayIr) if dayStart >= oldestNoBatchDayStart => dayIr
        }.orNull
        resultIr(col) = if (newestAvailableIr != null) newestAvailableIr(col) else null
      } else {
        // Large window / unwindowed: batch collapsed + streaming daily aggregates
        // resultIr(col) already has batchIr.collapsed(col) from clone
        nonNullDailyTileIrs.reverseIterator.foreach { case (dayStart, dayIr) =>
          if (dayStart >= batchDayStart && dayIr(col) != null) {
            resultIr(col) = windowedAggregator.columnAggregators(col).merge(resultIr(col), dayIr(col))
          }
        }
      }
      col += 1
    }

    // Tail hops for large windowed columns, plus optional small-window batch tails for post-batch entries.
    if (batchIr != null) {
      if (mergeNoBatchTailFromBatch) {
        megaTileAgg.mergeTailHopsForNoBatchColumnsCrossingBatchEnd(resultIr, queryTs, batchEnd, batchIr)
      }
      megaTileAgg.mergeTailHopsForBatchColumns(resultIr, queryTs, batchEnd, batchIr)
    }

    windowedAggregator.finalize(resultIr)
  }
}
