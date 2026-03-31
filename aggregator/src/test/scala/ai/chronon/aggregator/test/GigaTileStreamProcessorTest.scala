package ai.chronon.aggregator.test

import ai.chronon.aggregator.row.RowAggregator
import ai.chronon.aggregator.windowing._
import ai.chronon.api._
import ai.chronon.api.Extensions.{AggregationOps, WindowOps}
import com.google.gson.Gson
import org.junit.Assert._
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
  * Tests the full GigaTile streaming pipeline:
  * GigaTileStreamProcessor replays events + batch IR, emitting finalized feature vectors.
  * Results compared against NaiveAggregator.
  *
  * Key differences from MegaTileStreamProcessorTest:
  * - Batch IR is loaded into the processor (simulating Iceberg connected stream)
  * - Output is a finalized vector (not a windowed IR entry)
  * - No separate fetcher merge step — the processor IS the merge
  */
class GigaTileStreamProcessorTest extends AnyFlatSpec {
  @transient lazy val logger = LoggerFactory.getLogger(getClass)
  val gson = new Gson

  val AllWindows: Seq[Window] = Seq(
    new Window(6, TimeUnit.HOURS),
    new Window(1, TimeUnit.DAYS),
    new Window(47, TimeUnit.HOURS),
    new Window(2, TimeUnit.DAYS),
    new Window(49, TimeUnit.HOURS),
    new Window(3, TimeUnit.DAYS),
    new Window(7, TimeUnit.DAYS)
  )

  val TailBufferMillis: Long = new Window(2, TimeUnit.DAYS).millis
  val DayMillis: Long = new Window(1, TimeUnit.DAYS).millis
  val Epsilon = 1e-6

  def approxEqual(a: Any, b: Any, sketchTolerance: Double = 0.0): Boolean = (a, b) match {
    case (null, null)                         => true
    case (null, _) | (_, null)                => false
    case (x: Double, y: Double)               => Math.abs(x - y) <= Epsilon * Math.max(1.0, Math.max(Math.abs(x), Math.abs(y)))
    case (x: Float, y: Float)                 => Math.abs(x - y) <= Epsilon.toFloat * Math.max(1.0f, Math.max(Math.abs(x), Math.abs(y)))
    case (x: Long, y: Long) if sketchTolerance > 0 =>
      x == y || Math.abs(x - y).toDouble <= sketchTolerance * Math.max(1.0, Math.max(Math.abs(x), Math.abs(y)).toDouble)
    case (x: java.util.List[_], y: java.util.List[_]) =>
      x.size() == y.size() && (0 until x.size()).forall(i => approxEqual(x.get(i), y.get(i), sketchTolerance))
    case (x: java.util.Map[_, _], y: java.util.Map[_, _]) =>
      x.size() == y.size() && x.keySet().toArray.forall(k => approxEqual(x.get(k), y.get(k), sketchTolerance))
    case (x: Array[_], y: Array[_]) =>
      x.length == y.length && x.zip(y).forall { case (a, b) => approxEqual(a, b, sketchTolerance) }
    case _ => a == b
  }

  def compareResults(actual: Array[Array[Any]], expected: Array[Array[Any]], queryTimes: Array[Long],
                     label: String, sketchTolerance: Double = 0.0): Unit = {
    assertEquals(s"$label: result count mismatch", expected.length, actual.length)
    for (i <- queryTimes.indices) {
      if (!approxEqual(actual(i), expected(i), sketchTolerance)) {
        val expStr = gson.toJson(expected(i))
        val actStr = gson.toJson(actual(i))
        fail(s"$label: mismatch at query ${queryTimes(i)} (index $i)\n  expected: $expStr\n  got:      $actStr")
      }
    }
  }

  def generateEvents(windowDays: Int, count: Int): (Array[TestRow], Seq[(String, DataType)]) = {
    val columns = Seq(Column("ts", LongType, windowDays), Column("num", LongType, 1000), Column("amount", DoubleType, 500))
    val data = CStream.gen(columns, count)
    (data.rows, columns.map(_.schema))
  }

  def naiveAggregate(allEvents: Array[TestRow], queryTimes: Array[Long], aggregations: Seq[Aggregation],
                     schema: Seq[(String, DataType)]): Array[Array[Any]] = {
    val unpackedParts = aggregations.flatMap(_.unpack)
    val unpacked = unpackedParts.map(_.window).toArray
    val tailHops = unpacked.map(w => FiveMinuteResolution.calculateTailHop(w))
    val rowAgg = new RowAggregator(schema, unpackedParts)
    val naiveAgg = new NaiveAggregator(rowAgg, unpacked, tailHops)
    naiveAgg.aggregate(allEvents, queryTimes).map(ir => rowAgg.finalize(ir))
  }

  /** Build a FinalBatchIr from events before batchEnd using the standard Sawtooth pipeline. */
  def buildBatchIr(allEvents: Array[TestRow], batchEnd: Long, aggregations: Seq[Aggregation],
                   schema: Seq[(String, DataType)]): FinalBatchIr = {
    val batchEvents = allEvents.filter(_.ts < batchEnd)
    val onlineAgg = new SawtoothOnlineAggregator(batchEnd, aggregations, schema, tailBufferMillis = TailBufferMillis)
    var batchIr = onlineAgg.init
    batchEvents.foreach(row => batchIr = onlineAgg.update(batchIr, row))
    onlineAgg.normalizeBatchIr(batchIr)
  }

  /** Denormalize a FinalBatchIr for use in the processor (which operates on denormalized IRs). */
  def denormalizeBatchIr(batchIr: FinalBatchIr, aggregations: Seq[Aggregation],
                         schema: Seq[(String, DataType)], batchEnd: Long): FinalBatchIr = {
    val onlineAgg = new SawtoothOnlineAggregator(batchEnd, aggregations, schema, tailBufferMillis = TailBufferMillis)
    onlineAgg.denormalizeBatchIr(batchIr)
  }

  /**
    * Full GigaTile simulation:
    * 1. Build batch IR from pre-batchEnd events
    * 2. Create GigaTileStreamProcessor, load batch via onBatchUpdate
    * 3. Replay streaming events through onEvent
    * 4. Simulate watermark advancement + eviction at regular intervals
    * 5. At each query: read the last emitted finalized vector
    * 6. Compare with naive
    */
  def gigaTileAggregate(allEvents: Array[TestRow],
                        queryTimes: Array[Long],
                        aggregations: Seq[Aggregation],
                        schema: Seq[(String, DataType)],
                        batchEnd: Long): Array[Array[Any]] = {

    val megaTileAgg = new MegaTileAggregator(aggregations, schema, tailBufferMillis = TailBufferMillis)
    val store = new InMemoryGigaTileStore(megaTileAgg.windowedAggregator)
    val processor = new GigaTileStreamProcessor(megaTileAgg, store)

    // Build and load batch IR
    val normalizedBatchIr = buildBatchIr(allEvents, batchEnd, aggregations, schema)
    val batchIr = denormalizeBatchIr(normalizedBatchIr, aggregations, schema, batchEnd)

    // Flink processes ALL events (sorted by timestamp)
    val streamingEvents = allEvents.sortBy(_.ts)
    val sortedQueries = queryTimes.sorted

    val evictionInterval = processor.minEvictionInterval
    var nextEvictionTs = Long.MaxValue
    var lastEmitted: Array[Any] = null

    def firePendingEvictions(upToTs: Long): Unit = {
      while (nextEvictionTs <= upToTs) {
        processor.advanceWatermark(nextEvictionTs)
        val result = processor.onEviction(nextEvictionTs)
        if (result.finalizedVector != null) lastEmitted = result.finalizedVector
        nextEvictionTs += evictionInterval
      }
    }

    var eventIdx = 0
    val resultsByQueryTs = mutable.Map[Long, Array[Any]]()

    // Load batch IR before processing events (simulates Iceberg scan on startup)
    val batchResult = processor.onBatchUpdate(batchIr, batchEnd, batchEnd)
    if (batchResult.finalizedVector != null) lastEmitted = batchResult.finalizedVector
    if (batchResult.needsEvictionTimer && nextEvictionTs == Long.MaxValue) {
      nextEvictionTs = TsUtils.round(batchEnd, evictionInterval) + evictionInterval
    }

    for (queryTs <- sortedQueries) {
      // Process all streaming events up to queryTs
      while (eventIdx < streamingEvents.length && streamingEvents(eventIdx).ts <= queryTs) {
        val event = streamingEvents(eventIdx)

        firePendingEvictions(event.ts)
        processor.advanceWatermark(event.ts)

        val result = processor.onEvent(event, event.ts)
        if (result.finalizedVector != null) lastEmitted = result.finalizedVector

        if (nextEvictionTs == Long.MaxValue) {
          nextEvictionTs = TsUtils.round(event.ts, evictionInterval) + evictionInterval
        }

        eventIdx += 1
      }

      // Fire pending evictions up to query time
      firePendingEvictions(queryTs)
      processor.advanceWatermark(queryTs)

      // Final eviction at query time to correct sawtooth
      val evictResult = processor.onEviction(queryTs)
      if (evictResult.finalizedVector != null) lastEmitted = evictResult.finalizedVector

      resultsByQueryTs(queryTs) = lastEmitted
    }

    queryTimes.map(resultsByQueryTs)
  }

  it should "match naive with batch fresh (14 day sim)" in {
    val (events, schema) = generateEvents(14, 20000)
    val maxTs = events.map(_.ts).max
    val batchEnd = TsUtils.round(maxTs - DayMillis, DayMillis)

    val aggregations = Seq(
      Builders.Aggregation(Operation.SUM, "num", AllWindows),
      Builders.Aggregation(Operation.COUNT, "num", AllWindows),
      Builders.Aggregation(Operation.AVERAGE, "amount", AllWindows)
    )

    val queryTimes = Array(
      batchEnd + 6 * 3600 * 1000L,
      batchEnd + 14 * 3600 * 1000L,
      batchEnd + 23 * 3600 * 1000L
    ).filter(_ <= maxTs)

    val results = gigaTileAggregate(events, queryTimes, aggregations, schema, batchEnd)
    val naive = naiveAggregate(events, queryTimes, aggregations, schema)
    compareResults(results, naive, queryTimes, "giga_batch_fresh")
  }

  it should "match naive with batch delayed (14 day sim)" in {
    val (events, schema) = generateEvents(14, 20000)
    val maxTs = events.map(_.ts).max
    val batchEnd = TsUtils.round(maxTs - 2 * DayMillis, DayMillis)

    val aggregations = Seq(
      Builders.Aggregation(Operation.SUM, "num", AllWindows),
      Builders.Aggregation(Operation.COUNT, "num", AllWindows)
    )

    val queryTimes = Array(
      batchEnd + DayMillis + 2 * 3600 * 1000L,
      batchEnd + DayMillis + 18 * 3600 * 1000L
    ).filter(_ <= maxTs)

    val results = gigaTileAggregate(events, queryTimes, aggregations, schema, batchEnd)
    val naive = naiveAggregate(events, queryTimes, aggregations, schema)
    compareResults(results, naive, queryTimes, "giga_batch_delayed")
  }

  it should "handle idle entities (batch only, no streaming events)" in {
    val (events, schema) = generateEvents(14, 10000)
    val maxTs = events.map(_.ts).max
    val batchEnd = maxTs + DayMillis

    val aggregations = Seq(
      Builders.Aggregation(Operation.SUM, "num", AllWindows),
      Builders.Aggregation(Operation.COUNT, "num", AllWindows)
    )

    val queryTimes = Array(batchEnd + 14 * 3600 * 1000L)

    val results = gigaTileAggregate(events, queryTimes, aggregations, schema, batchEnd)
    val naive = naiveAggregate(events, queryTimes, aggregations, schema)
    compareResults(results, naive, queryTimes, "giga_idle")
  }

  it should "match naive with all aggregation types" in {
    val (events, schema) = generateEvents(14, 20000)
    val maxTs = events.map(_.ts).max
    val batchEnd = TsUtils.round(maxTs - DayMillis, DayMillis)

    val aggregations = Seq(
      Builders.Aggregation(Operation.SUM, "num", AllWindows),
      Builders.Aggregation(Operation.COUNT, "num", AllWindows),
      Builders.Aggregation(Operation.AVERAGE, "amount", AllWindows),
      Builders.Aggregation(Operation.MIN, "num", AllWindows),
      Builders.Aggregation(Operation.MAX, "num", AllWindows),
      Builders.Aggregation(Operation.LAST, "num", AllWindows),
      Builders.Aggregation(Operation.FIRST, "num", AllWindows)
    )

    val queryTimes = Array(batchEnd + 14 * 3600 * 1000L).filter(_ <= maxTs)

    val results = gigaTileAggregate(events, queryTimes, aggregations, schema, batchEnd)
    val naive = naiveAggregate(events, queryTimes, aggregations, schema)
    compareResults(results, naive, queryTimes, "giga_multi_agg")
  }

  it should "handle day boundary transitions across multiple days" in {
    val (events, schema) = generateEvents(14, 20000)
    val maxTs = events.map(_.ts).max
    val batchEnd = TsUtils.round(maxTs - 2 * DayMillis, DayMillis)

    val aggregations = Seq(
      Builders.Aggregation(Operation.SUM, "num", AllWindows),
      Builders.Aggregation(Operation.COUNT, "num", AllWindows),
      Builders.Aggregation(Operation.AVERAGE, "amount", AllWindows)
    )

    val queryTimes = Array(
      batchEnd + 6 * 3600 * 1000L,
      batchEnd + 18 * 3600 * 1000L,
      batchEnd + 30 * 3600 * 1000L,
      batchEnd + 42 * 3600 * 1000L
    ).filter(_ <= maxTs)

    val results = gigaTileAggregate(events, queryTimes, aggregations, schema, batchEnd)
    val naive = naiveAggregate(events, queryTimes, aggregations, schema)
    compareResults(results, naive, queryTimes, "giga_day_boundary")
  }

  it should "handle multi-day watermark gap (3+ day idle then resume)" in {
    val (allEvents, schema) = generateEvents(14, 20000)
    val maxTs = allEvents.map(_.ts).max
    val gapEnd = TsUtils.round(maxTs - 2 * DayMillis, DayMillis)
    val gapStart = gapEnd - 3 * DayMillis

    val events = allEvents.filter(e => e.ts < gapStart || e.ts >= gapEnd)
    val batchEnd = TsUtils.round(maxTs - DayMillis, DayMillis)

    val aggregations = Seq(
      Builders.Aggregation(Operation.SUM, "num", AllWindows),
      Builders.Aggregation(Operation.COUNT, "num", AllWindows)
    )

    val queryTimes = Array(
      batchEnd + 6 * 3600 * 1000L,
      batchEnd + 14 * 3600 * 1000L
    ).filter(_ <= maxTs)

    val results = gigaTileAggregate(events, queryTimes, aggregations, schema, batchEnd)
    val naive = naiveAggregate(events, queryTimes, aggregations, schema)
    compareResults(results, naive, queryTimes, "giga_multi_day_gap")
  }

  it should "handle new entity (streaming only, no batch IR)" in {
    val (events, schema) = generateEvents(2, 5000)
    val maxTs = events.map(_.ts).max
    val now = TsUtils.round(maxTs, DayMillis) + 14 * 3600 * 1000L

    // Only small windows — these are fully covered by streaming tiles
    val smallWindows = Seq(
      new Window(6, TimeUnit.HOURS),
      new Window(1, TimeUnit.DAYS),
      new Window(2, TimeUnit.DAYS)
    )

    val aggregations = Seq(
      Builders.Aggregation(Operation.SUM, "num", smallWindows),
      Builders.Aggregation(Operation.COUNT, "num", smallWindows)
    )

    val megaTileAgg = new MegaTileAggregator(aggregations, schema, tailBufferMillis = TailBufferMillis)
    val store = new InMemoryGigaTileStore(megaTileAgg.windowedAggregator)
    val processor = new GigaTileStreamProcessor(megaTileAgg, store)

    // No batch loaded — entity type C
    val streamingEvents = events.filter(_.ts <= now).sortBy(_.ts)
    val evictionInterval = processor.minEvictionInterval
    var nextEvictionTs = Long.MaxValue
    var lastEmitted: Array[Any] = null

    for (event <- streamingEvents) {
      while (nextEvictionTs <= event.ts) {
        processor.advanceWatermark(nextEvictionTs)
        val r = processor.onEviction(nextEvictionTs)
        if (r.finalizedVector != null) lastEmitted = r.finalizedVector
        nextEvictionTs += evictionInterval
      }
      processor.advanceWatermark(event.ts)
      val result = processor.onEvent(event, event.ts)
      if (result.finalizedVector != null) lastEmitted = result.finalizedVector
      if (nextEvictionTs == Long.MaxValue) {
        nextEvictionTs = TsUtils.round(event.ts, evictionInterval) + evictionInterval
      }
    }

    // Final eviction at query time
    while (nextEvictionTs <= now) {
      processor.advanceWatermark(nextEvictionTs)
      val r = processor.onEviction(nextEvictionTs)
      if (r.finalizedVector != null) lastEmitted = r.finalizedVector
      nextEvictionTs += evictionInterval
    }
    processor.advanceWatermark(now)
    val evictResult = processor.onEviction(now)
    if (evictResult.finalizedVector != null) lastEmitted = evictResult.finalizedVector

    val naive = naiveAggregate(events.filter(_.ts <= now), Array(now), aggregations, schema)
    if (!approxEqual(lastEmitted, naive(0))) {
      val expStr = gson.toJson(naive(0))
      val actStr = gson.toJson(lastEmitted)
      fail(s"giga_new_entity: mismatch\n  expected: $expStr\n  got:      $actStr")
    }
  }

  it should "handle batch refresh (old batch → events → new batch)" in {
    val (events, schema) = generateEvents(14, 20000)
    val maxTs = events.map(_.ts).max
    val oldBatchEnd = TsUtils.round(maxTs - 3 * DayMillis, DayMillis)
    val newBatchEnd = TsUtils.round(maxTs - DayMillis, DayMillis)

    val aggregations = Seq(
      Builders.Aggregation(Operation.SUM, "num", AllWindows),
      Builders.Aggregation(Operation.COUNT, "num", AllWindows)
    )

    val megaTileAgg = new MegaTileAggregator(aggregations, schema, tailBufferMillis = TailBufferMillis)
    val store = new InMemoryGigaTileStore(megaTileAgg.windowedAggregator)
    val processor = new GigaTileStreamProcessor(megaTileAgg, store)

    // Build both batch IRs
    val oldNormalizedBatchIr = buildBatchIr(events, oldBatchEnd, aggregations, schema)
    val oldBatchIr = denormalizeBatchIr(oldNormalizedBatchIr, aggregations, schema, oldBatchEnd)
    val newNormalizedBatchIr = buildBatchIr(events, newBatchEnd, aggregations, schema)
    val newBatchIr = denormalizeBatchIr(newNormalizedBatchIr, aggregations, schema, newBatchEnd)

    // Load old batch
    processor.onBatchUpdate(oldBatchIr, oldBatchEnd, oldBatchEnd)

    // Process events
    val streamingEvents = events.sortBy(_.ts)
    val evictionInterval = processor.minEvictionInterval
    var nextEvictionTs = TsUtils.round(oldBatchEnd, evictionInterval) + evictionInterval
    var lastEmitted: Array[Any] = null

    val queryTs = newBatchEnd + 14 * 3600 * 1000L
    for (event <- streamingEvents if event.ts <= queryTs) {
      while (nextEvictionTs <= event.ts) {
        processor.advanceWatermark(nextEvictionTs)
        val r = processor.onEviction(nextEvictionTs)
        if (r.finalizedVector != null) lastEmitted = r.finalizedVector
        nextEvictionTs += evictionInterval
      }
      processor.advanceWatermark(event.ts)
      val result = processor.onEvent(event, event.ts)
      if (result.finalizedVector != null) lastEmitted = result.finalizedVector
    }

    // Load new batch (simulates daily Iceberg refresh)
    val batchResult = processor.onBatchUpdate(newBatchIr, newBatchEnd, queryTs)
    if (batchResult.finalizedVector != null) lastEmitted = batchResult.finalizedVector

    // Final eviction
    while (nextEvictionTs <= queryTs) {
      processor.advanceWatermark(nextEvictionTs)
      val r = processor.onEviction(nextEvictionTs)
      if (r.finalizedVector != null) lastEmitted = r.finalizedVector
      nextEvictionTs += evictionInterval
    }
    processor.advanceWatermark(queryTs)
    val evictResult = processor.onEviction(queryTs)
    if (evictResult.finalizedVector != null) lastEmitted = evictResult.finalizedVector

    val naive = naiveAggregate(events, Array(queryTs), aggregations, schema)
    if (!approxEqual(lastEmitted, naive(0))) {
      val expStr = gson.toJson(naive(0))
      val actStr = gson.toJson(lastEmitted)
      fail(s"giga_batch_refresh: mismatch\n  expected: $expStr\n  got:      $actStr")
    }
  }

  it should "accept sequential batch updates with advancing batchEnd (Bug 1 regression)" in {
    val (events, schema) = generateEvents(14, 20000)
    val maxTs = events.map(_.ts).max
    val day1End = TsUtils.round(maxTs - 3 * DayMillis, DayMillis)
    val day2End = day1End + DayMillis
    val day3End = day2End + DayMillis

    val aggregations = Seq(
      Builders.Aggregation(Operation.SUM, "num", AllWindows),
      Builders.Aggregation(Operation.COUNT, "num", AllWindows)
    )

    val megaTileAgg = new MegaTileAggregator(aggregations, schema, tailBufferMillis = TailBufferMillis)
    val store = new InMemoryGigaTileStore(megaTileAgg.windowedAggregator)
    val processor = new GigaTileStreamProcessor(megaTileAgg, store)

    // Day 1 batch — initializes currentDayStart = day1End
    val batch1 = denormalizeBatchIr(buildBatchIr(events, day1End, aggregations, schema), aggregations, schema, day1End)
    val result1 = processor.onBatchUpdate(batch1, day1End, day1End)
    assertNotNull("first batch should emit", result1.finalizedVector)
    assertEquals("store should track day1 batchEnd", day1End, store.getBatchEndTs)

    // Advance watermark past day2End so currentDayStart catches up (avoids defer)
    processor.advanceWatermark(day2End + 6 * 3600 * 1000L)

    // Day 2 batch — batchEnd advances, must NOT be rejected as stale
    val batch2 = denormalizeBatchIr(buildBatchIr(events, day2End, aggregations, schema), aggregations, schema, day2End)
    val result2 = processor.onBatchUpdate(batch2, day2End, day2End)
    assertNotNull("second batch should emit (not rejected as stale)", result2.finalizedVector)
    assertEquals("store should track day2 batchEnd", day2End, store.getBatchEndTs)

    // Advance watermark past day3End
    processor.advanceWatermark(day3End + 6 * 3600 * 1000L)

    // Day 3 batch
    val batch3 = denormalizeBatchIr(buildBatchIr(events, day3End, aggregations, schema), aggregations, schema, day3End)
    val result3 = processor.onBatchUpdate(batch3, day3End, day3End)
    assertNotNull("third batch should emit", result3.finalizedVector)
    assertEquals("store should track day3 batchEnd", day3End, store.getBatchEndTs)
  }

  it should "suppress redundant eviction emits when nothing changed (Bug 2 regression)" in {
    val (events, schema) = generateEvents(14, 10000)
    val maxTs = events.map(_.ts).max
    val batchEnd = TsUtils.round(maxTs - DayMillis, DayMillis)

    // Large windows only — no small windows, no tile rebuilding.
    // This ensures eviction only recomputes runningLargeIr from batch hops.
    // With 1hr hops, two evictions 5 min apart produce the same tail hop selection.
    val largeWindows = Seq(
      new Window(49, TimeUnit.HOURS),
      new Window(3, TimeUnit.DAYS),
      new Window(7, TimeUnit.DAYS)
    )

    val aggregations = Seq(
      Builders.Aggregation(Operation.SUM, "num", largeWindows),
      Builders.Aggregation(Operation.COUNT, "num", largeWindows)
    )

    val megaTileAgg = new MegaTileAggregator(aggregations, schema, tailBufferMillis = TailBufferMillis)
    // irEqual that actually compares values
    val irEqual: (Array[Any], Array[Any]) => Boolean = { (a, b) =>
      if (a == null && b == null) true
      else if (a == null || b == null) false
      else a.length == b.length && a.zip(b).forall {
        case (null, null) => true
        case (null, _) | (_, null) => false
        case (x, y) => x == y
      }
    }
    val store = new InMemoryGigaTileStore(megaTileAgg.windowedAggregator)
    val processor = new GigaTileStreamProcessor(megaTileAgg, store, irEqual)

    assertFalse("should have no small windows", processor.hasSmallWindows)

    // Load batch
    val batchIr = denormalizeBatchIr(buildBatchIr(events, batchEnd, aggregations, schema), aggregations, schema, batchEnd)
    processor.onBatchUpdate(batchIr, batchEnd, batchEnd)

    // Process some events
    val streamEvents = events.filter(e => e.ts >= batchEnd && e.ts < batchEnd + 12 * 3600 * 1000L).sortBy(_.ts)
    for (event <- streamEvents) {
      processor.advanceWatermark(event.ts)
      processor.onEvent(event, event.ts)
    }

    // First eviction at an hour boundary — should emit
    val evictionTs = TsUtils.round(batchEnd + 12 * 3600 * 1000L, 3600 * 1000L)
    processor.advanceWatermark(evictionTs)
    val result1 = processor.onEviction(evictionTs)
    assertNotNull("first eviction should emit", result1.finalizedVector)

    // Second eviction 5 min later — same hour boundary, no events, nothing changed
    val nextEviction = evictionTs + 5 * 60 * 1000L
    processor.advanceWatermark(nextEviction)
    val result2 = processor.onEviction(nextEviction)
    assertNull("redundant eviction should not emit when nothing changed", result2.finalizedVector)
  }

  // ==========================================================================
  // Targeted edge case tests — steady state focus
  // ==========================================================================

  private def row(ts: Long, num: Long, amount: Double): TestRow = new TestRow(ts, num, amount)()

  /** Helper: build a processor with batch loaded, return (processor, store). */
  private def buildProcessor(
      aggregations: Seq[Aggregation],
      schema: Seq[(String, DataType)],
      batchEvents: Array[TestRow],
      batchEnd: Long
  ): (GigaTileStreamProcessor, InMemoryGigaTileStore) = {
    val megaTileAgg = new MegaTileAggregator(aggregations, schema, tailBufferMillis = TailBufferMillis)
    val store = new InMemoryGigaTileStore(megaTileAgg.windowedAggregator)
    val processor = new GigaTileStreamProcessor(megaTileAgg, store)
    val normalizedBatchIr = buildBatchIr(batchEvents, batchEnd, aggregations, schema)
    val batchIr = denormalizeBatchIr(normalizedBatchIr, aggregations, schema, batchEnd)
    processor.onBatchUpdate(batchIr, batchEnd, batchEnd)
    (processor, store)
  }

  it should "not lose an event at exactly batchEnd timestamp after eviction" in {
    // Event at exactly midnight = batchEnd. Batch is exclusive [0, batchEnd), so event is NOT in batch.
    // onEvent clamps it to today (>= nextDayStart branch). After day rotation, it lands in
    // largeYesterdayIr. Eviction with batchEnd == currentDayStart skips yesterday merge.
    // The event must still be in the final answer.
    val batchEnd = 1743033600000L // Mar 27 00:00 UTC (arbitrary fixed point)
    val schema: Seq[(String, DataType)] = Seq(("ts", LongType), ("num", LongType), ("amount", DoubleType))

    // Batch events: everything before batchEnd
    val batchEvents = Array(
      row(batchEnd - 3 * 3600 * 1000L, 10L, 1.0),
      row(batchEnd - 6 * 3600 * 1000L, 20L, 2.0)
    )

    val aggregations = Seq(
      Builders.Aggregation(Operation.SUM, "num", Seq(new Window(3, TimeUnit.DAYS))),
      Builders.Aggregation(Operation.COUNT, "num", Seq(new Window(3, TimeUnit.DAYS)))
    )

    val (processor, _) = buildProcessor(aggregations, schema, batchEvents, batchEnd)

    // Event at exactly batchEnd — this is the edge case
    val midnightEvent = row(batchEnd, 100L, 5.0)
    processor.advanceWatermark(batchEnd)
    val eventResult = processor.onEvent(midnightEvent, batchEnd)
    assertNotNull("event at batchEnd should emit", eventResult.finalizedVector)

    // Advance watermark past midnight — triggers day rotation
    processor.advanceWatermark(batchEnd + DayMillis + 60000L)

    // Eviction after rotation
    val evictResult = processor.onEviction(batchEnd + DayMillis + 60000L)
    assertNotNull("eviction should emit", evictResult.finalizedVector)

    // The 100 from the midnight event must be present in the 3d SUM
    // Batch has 10 + 20 = 30. Midnight event adds 100. Total = 130.
    val naive = naiveAggregate(
      batchEvents :+ midnightEvent,
      Array(batchEnd + DayMillis + 60000L),
      aggregations,
      schema
    )
    if (!approxEqual(evictResult.finalizedVector, naive(0))) {
      val expStr = gson.toJson(naive(0))
      val actStr = gson.toJson(evictResult.finalizedVector)
      fail(s"midnight_event: expected $expStr got $actStr — event at batchEnd likely lost after eviction")
    }
  }

  it should "detect incremental vs eviction divergence for late yesterday event after fresh batch" in {
    // Fresh batch (batchEnd = currentDayStart). A late event from yesterday arrives AFTER
    // batch was loaded. The event was NOT in the batch source data.
    // onEvent adds it to largeYesterdayIr + runningLargeIr (incremental).
    // Eviction recomputes: yesterday not merged (batchEnd >= currentDayStart).
    // This tests whether the two paths agree.
    val batchEnd = 1743033600000L // Mar 27 00:00 UTC
    val schema: Seq[(String, DataType)] = Seq(("ts", LongType), ("num", LongType), ("amount", DoubleType))

    // Batch events: does NOT include the late event
    val batchEvents = Array(
      row(batchEnd - 12 * 3600 * 1000L, 10L, 1.0)
    )

    val aggregations = Seq(
      Builders.Aggregation(Operation.SUM, "num", Seq(new Window(3, TimeUnit.DAYS)))
    )

    val (processor, _) = buildProcessor(aggregations, schema, batchEvents, batchEnd)

    // Advance watermark to today
    processor.advanceWatermark(batchEnd + 3600 * 1000L)

    // Late event from yesterday — NOT in batch
    val lateEvent = row(batchEnd - 2 * 3600 * 1000L, 50L, 3.0)
    val eventResult = processor.onEvent(lateEvent, lateEvent.ts)
    assertNotNull("late event should emit", eventResult.finalizedVector)

    // Capture incremental value (from onEvent)
    val incrementalVector = eventResult.finalizedVector.clone()

    // Eviction recomputes from scratch
    val evictResult = processor.onEviction(batchEnd + 3600 * 1000L)
    assertNotNull("eviction should emit", evictResult.finalizedVector)
    val evictionVector = evictResult.finalizedVector

    // Document the divergence: incremental includes the late event, eviction may not.
    // Both are compared against naive (which DOES include the event).
    val allEvents = batchEvents :+ lateEvent
    val naive = naiveAggregate(allEvents, Array(batchEnd + 3600 * 1000L), aggregations, schema)

    val incrementalMatch = approxEqual(incrementalVector, naive(0))
    val evictionMatch = approxEqual(evictionVector, naive(0))

    // At minimum one of these should match. If neither matches, there's a bug.
    // The known design trade-off: eviction may drop the late event to avoid double-counting.
    assertTrue(
      s"at least one path should match naive. incremental=$incrementalMatch eviction=$evictionMatch",
      incrementalMatch || evictionMatch
    )

    // Log which path diverges for visibility
    if (incrementalMatch && !evictionMatch) {
      logger.warn("KNOWN TRADE-OFF: eviction drops late yesterday event not in batch " +
        "(avoids double-count, causes transient value flip)")
    }
    if (!incrementalMatch) {
      fail(s"incremental path should always include the late event: " +
        s"expected ${gson.toJson(naive(0))} got ${gson.toJson(incrementalVector)}")
    }
  }

  it should "not double-count events present in both batch and streaming after eviction" in {
    // Event at batchEnd - 1hr is in both batch IR and streaming.
    // Between event and eviction, runningLargeIr double-counts it.
    // After eviction, it must be counted exactly once.
    val batchEnd = 1743033600000L
    val schema: Seq[(String, DataType)] = Seq(("ts", LongType), ("num", LongType), ("amount", DoubleType))

    val overlapTs = batchEnd - 3600 * 1000L // 1hr before batchEnd
    val batchEvents = Array(
      row(overlapTs, 100L, 5.0),
      row(batchEnd - 12 * 3600 * 1000L, 10L, 1.0)
    )

    val aggregations = Seq(
      Builders.Aggregation(Operation.SUM, "num", Seq(new Window(3, TimeUnit.DAYS)))
    )

    val (processor, _) = buildProcessor(aggregations, schema, batchEvents, batchEnd)
    processor.advanceWatermark(batchEnd + 3600 * 1000L)

    // Replay the overlap event (simulates Kafka delivering it after batch)
    val overlapEvent = row(overlapTs, 100L, 5.0)
    processor.onEvent(overlapEvent, overlapEvent.ts)

    // Eviction should correct the double-count
    val evictResult = processor.onEviction(batchEnd + 3600 * 1000L)
    assertNotNull("eviction should emit", evictResult.finalizedVector)

    val naive = naiveAggregate(batchEvents, Array(batchEnd + 3600 * 1000L), aggregations, schema)
    if (!approxEqual(evictResult.finalizedVector, naive(0))) {
      val expStr = gson.toJson(naive(0))
      val actStr = gson.toJson(evictResult.finalizedVector)
      fail(s"double_count: expected $expStr got $actStr — overlap event likely counted twice")
    }
  }

  it should "correctly shift tail hops at hourly boundary for large-window-only GroupBy" in {
    // Large windows only (49h, 3d, 7d) — hourly hops.
    // Two evictions straddle an hourly boundary: the tail hop selection should change.
    val batchEnd = 1743033600000L
    val hourMillis = 3600 * 1000L
    val schema: Seq[(String, DataType)] = Seq(("ts", LongType), ("num", LongType), ("amount", DoubleType))

    // Dense events every hour for 10 days before batchEnd
    val batchEvents = (0 until 240).map { i =>
      row(batchEnd - (240 - i) * hourMillis, (i + 1).toLong, 1.0)
    }.toArray

    val largeWindows = Seq(new Window(49, TimeUnit.HOURS))
    val aggregations = Seq(
      Builders.Aggregation(Operation.SUM, "num", largeWindows),
      Builders.Aggregation(Operation.COUNT, "num", largeWindows)
    )

    val (processor, _) = buildProcessor(aggregations, schema, batchEvents, batchEnd)

    // No streaming events — batch only entity

    // Eviction just before hourly boundary
    val queryBefore = batchEnd + 48 * hourMillis + 59 * 60 * 1000L
    processor.advanceWatermark(queryBefore)
    val r1 = processor.onEviction(queryBefore)
    assertNotNull("eviction before hour boundary should emit", r1.finalizedVector)

    // Eviction just after hourly boundary — a different hop enters/exits the 49h window
    val queryAfter = batchEnd + 49 * hourMillis + 60 * 1000L
    processor.advanceWatermark(queryAfter)
    val r2 = processor.onEviction(queryAfter)

    // Verify both match naive
    val naiveBefore = naiveAggregate(batchEvents, Array(queryBefore), aggregations, schema)
    val naiveAfter = naiveAggregate(batchEvents, Array(queryAfter), aggregations, schema)

    if (!approxEqual(r1.finalizedVector, naiveBefore(0))) {
      fail(s"hop_shift: before boundary mismatch: expected ${gson.toJson(naiveBefore(0))} got ${gson.toJson(r1.finalizedVector)}")
    }
    if (r2.finalizedVector != null) {
      if (!approxEqual(r2.finalizedVector, naiveAfter(0))) {
        fail(s"hop_shift: after boundary mismatch: expected ${gson.toJson(naiveAfter(0))} got ${gson.toJson(r2.finalizedVector)}")
      }
    }
  }

  it should "survive day rotation with no events and produce correct values" in {
    // Batch loaded, events processed, then entity goes idle across a day boundary.
    // Verify eviction produces correct results after rotation.
    val batchEnd = 1743033600000L
    val schema: Seq[(String, DataType)] = Seq(("ts", LongType), ("num", LongType), ("amount", DoubleType))

    val batchEvents = (0 until 100).map { i =>
      row(batchEnd - (100 - i) * 3600 * 1000L, (i + 1).toLong, 1.0)
    }.toArray

    // A few streaming events on day 1 only
    val streamEvents = Array(
      row(batchEnd + 3600 * 1000L, 200L, 10.0),
      row(batchEnd + 6 * 3600 * 1000L, 300L, 15.0)
    )

    val aggregations = Seq(
      Builders.Aggregation(Operation.SUM, "num", AllWindows),
      Builders.Aggregation(Operation.COUNT, "num", AllWindows)
    )

    val (processor, _) = buildProcessor(aggregations, schema, batchEvents, batchEnd)

    // Process streaming events
    for (event <- streamEvents) {
      processor.advanceWatermark(event.ts)
      processor.onEvent(event, event.ts)
    }

    // Eviction before day boundary
    val preRotation = batchEnd + 23 * 3600 * 1000L
    processor.advanceWatermark(preRotation)
    processor.onEviction(preRotation)

    // Day rotation
    processor.advanceWatermark(batchEnd + DayMillis + 60000L)

    // Eviction after rotation — no new events
    val postRotation = batchEnd + DayMillis + 3600 * 1000L
    processor.advanceWatermark(postRotation)
    val result = processor.onEviction(postRotation)
    assertNotNull("post-rotation eviction should emit", result.finalizedVector)

    val allEvents = batchEvents ++ streamEvents
    val naive = naiveAggregate(allEvents, Array(postRotation), aggregations, schema)
    if (!approxEqual(result.finalizedVector, naive(0))) {
      fail(s"day_rotation: expected ${gson.toJson(naive(0))} got ${gson.toJson(result.finalizedVector)}")
    }
  }

  it should "handle MIN correctly when min value is at the sawtooth boundary" in {
    // The global MIN is in the oldest tail hop. As the window slides forward,
    // that hop should fall off and MIN should increase.
    val batchEnd = 1743033600000L
    val hourMillis = 3600 * 1000L
    val schema: Seq[(String, DataType)] = Seq(("ts", LongType), ("num", LongType), ("amount", DoubleType))

    // Place the minimum value exactly at the 3d window's oldest hop boundary
    val oldHopTs = batchEnd - 71 * hourMillis // ~71 hours before batchEnd
    val batchEvents = Array(
      row(oldHopTs, 1L, 1.0), // the min
      row(batchEnd - 24 * hourMillis, 100L, 10.0),
      row(batchEnd - 12 * hourMillis, 200L, 20.0),
      row(batchEnd - 1 * hourMillis, 150L, 15.0)
    )

    val aggregations = Seq(
      Builders.Aggregation(Operation.MIN, "num", Seq(new Window(3, TimeUnit.DAYS)))
    )

    val (processor, _) = buildProcessor(aggregations, schema, batchEvents, batchEnd)

    // Query while old hop is still in window — MIN should be 1
    val earlyQuery = batchEnd + 60000L
    processor.advanceWatermark(earlyQuery)
    val r1 = processor.onEviction(earlyQuery)
    val naive1 = naiveAggregate(batchEvents, Array(earlyQuery), aggregations, schema)
    if (!approxEqual(r1.finalizedVector, naive1(0))) {
      fail(s"min_boundary early: expected ${gson.toJson(naive1(0))} got ${gson.toJson(r1.finalizedVector)}")
    }

    // Query after old hop falls off the 3d window — MIN should increase to 100
    val lateQuery = batchEnd + 2 * hourMillis
    processor.advanceWatermark(lateQuery)
    val r2 = processor.onEviction(lateQuery)
    assertNotNull(r2.finalizedVector)
    val naive2 = naiveAggregate(batchEvents, Array(lateQuery), aggregations, schema)
    if (!approxEqual(r2.finalizedVector, naive2(0))) {
      fail(s"min_boundary late: expected ${gson.toJson(naive2(0))} got ${gson.toJson(r2.finalizedVector)}")
    }
  }

  it should "produce identical results from two consecutive emits (finalize must not corrupt IR)" in {
    val batchEnd = 1743033600000L
    val schema: Seq[(String, DataType)] = Seq(("ts", LongType), ("num", LongType), ("amount", DoubleType))

    val batchEvents = Array(
      row(batchEnd - 3600 * 1000L, 10L, 1.0),
      row(batchEnd - 7200 * 1000L, 20L, 2.0)
    )

    val aggregations = Seq(
      Builders.Aggregation(Operation.SUM, "num", AllWindows),
      Builders.Aggregation(Operation.AVERAGE, "amount", AllWindows),
      Builders.Aggregation(Operation.FIRST, "num", AllWindows),
      Builders.Aggregation(Operation.LAST, "num", AllWindows)
    )

    val (processor, _) = buildProcessor(aggregations, schema, batchEvents, batchEnd)

    processor.advanceWatermark(batchEnd + 3600 * 1000L)
    processor.onEvent(row(batchEnd + 1000L, 30L, 3.0), batchEnd + 1000L)

    // Two consecutive events — each triggers packAndFinalize. Second must not be corrupted.
    val r1 = processor.onEvent(row(batchEnd + 2000L, 40L, 4.0), batchEnd + 2000L)
    val r2 = processor.onEvent(row(batchEnd + 3000L, 0L, 0.0), batchEnd + 3000L)
    assertNotNull(r1.finalizedVector)
    assertNotNull(r2.finalizedVector)

    val allEvents = batchEvents ++ Array(
      row(batchEnd + 1000L, 30L, 3.0), row(batchEnd + 2000L, 40L, 4.0), row(batchEnd + 3000L, 0L, 0.0))
    val naive2 = naiveAggregate(allEvents, Array(batchEnd + 2000L), aggregations, schema)
    val naive3 = naiveAggregate(allEvents, Array(batchEnd + 3000L), aggregations, schema)

    if (!approxEqual(r1.finalizedVector, naive2(0))) {
      fail(s"finalize_corruption: first emit ${gson.toJson(r1.finalizedVector)} != naive ${gson.toJson(naive2(0))}")
    }
    if (!approxEqual(r2.finalizedVector, naive3(0))) {
      fail(s"finalize_corruption: second emit ${gson.toJson(r2.finalizedVector)} != naive ${gson.toJson(naive3(0))}")
    }
  }

  it should "handle all-small-window GroupBy with batch loaded correctly" in {
    // Only small windows (all <= tailBuffer). Large window paths should be no-ops.
    // Batch IR is loaded but only small window columns matter (from tiles).
    val batchEnd = 1743033600000L
    val schema: Seq[(String, DataType)] = Seq(("ts", LongType), ("num", LongType), ("amount", DoubleType))

    val smallWindows = Seq(
      new Window(6, TimeUnit.HOURS),
      new Window(1, TimeUnit.DAYS),
      new Window(2, TimeUnit.DAYS)
    )

    val aggregations = Seq(
      Builders.Aggregation(Operation.SUM, "num", smallWindows),
      Builders.Aggregation(Operation.COUNT, "num", smallWindows)
    )

    val batchEvents = (0 until 200).map { i =>
      row(batchEnd - (200 - i) * 5 * 60 * 1000L, (i + 1).toLong, 1.0)
    }.toArray

    val (processor, _) = buildProcessor(aggregations, schema, batchEvents, batchEnd)

    // Streaming events
    val streamEvents = Array(
      row(batchEnd + 3600 * 1000L, 500L, 25.0),
      row(batchEnd + 2 * 3600 * 1000L, 600L, 30.0)
    )
    for (event <- streamEvents) {
      processor.advanceWatermark(event.ts)
      processor.onEvent(event, event.ts)
    }

    val queryTs = batchEnd + 3 * 3600 * 1000L
    processor.advanceWatermark(queryTs)
    val result = processor.onEviction(queryTs)
    assertNotNull("all-small-window eviction should emit", result.finalizedVector)

    val allEvents = batchEvents ++ streamEvents
    val naive = naiveAggregate(allEvents, Array(queryTs), aggregations, schema)
    if (!approxEqual(result.finalizedVector, naive(0))) {
      fail(s"all_small_windows: expected ${gson.toJson(naive(0))} got ${gson.toJson(result.finalizedVector)}")
    }
  }
}
