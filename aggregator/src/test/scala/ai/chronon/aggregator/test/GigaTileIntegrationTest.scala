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

/** Integration test: verifies that the push-based (giga tile) path produces
  * identical results to the pull-based (mega tile + MegaTileMerger) path.
  *
  * Both paths are compared against NaiveAggregator as ground truth.
  * This catches any divergence between the two merge strategies.
  */
class GigaTileIntegrationTest extends AnyFlatSpec {
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

  def approxEqual(a: Any, b: Any): Boolean = (a, b) match {
    case (null, null)                         => true
    case (null, _) | (_, null)                => false
    case (x: Double, y: Double)               => Math.abs(x - y) <= Epsilon * Math.max(1.0, Math.max(Math.abs(x), Math.abs(y)))
    case (x: Float, y: Float)                 => Math.abs(x - y) <= Epsilon.toFloat * Math.max(1.0f, Math.max(Math.abs(x), Math.abs(y)))
    case (x: java.util.List[_], y: java.util.List[_]) =>
      x.size() == y.size() && (0 until x.size()).forall(i => approxEqual(x.get(i), y.get(i)))
    case (x: java.util.Map[_, _], y: java.util.Map[_, _]) =>
      x.size() == y.size() && x.keySet().toArray.forall(k => approxEqual(x.get(k), y.get(k)))
    case (x: Array[_], y: Array[_]) =>
      x.length == y.length && x.zip(y).forall { case (a, b) => approxEqual(a, b) }
    case _ => a == b
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

  /** Pull-based: MegaTileStreamProcessor → MegaTileMerger.merge (what the fetcher does today). */
  def megaTileAggregate(allEvents: Array[TestRow], queryTimes: Array[Long], aggregations: Seq[Aggregation],
                        schema: Seq[(String, DataType)], batchEnd: Long): Array[Array[Any]] = {
    val megaTileAgg = new MegaTileAggregator(aggregations, schema, tailBufferMillis = TailBufferMillis)
    val store = new InMemoryTileStore(megaTileAgg.windowedAggregator)
    val processor = new MegaTileStreamProcessor(megaTileAgg, store)
    val merger = new MegaTileMerger(megaTileAgg)

    val batchEvents = allEvents.filter(_.ts < batchEnd)
    val onlineAgg = new SawtoothOnlineAggregator(batchEnd, aggregations, schema, tailBufferMillis = TailBufferMillis)
    var batchIr = onlineAgg.init
    batchEvents.foreach(row => batchIr = onlineAgg.update(batchIr, row))
    val finalBatchIr = onlineAgg.finalizeSnapshot(batchIr)

    val streamingEvents = allEvents.sortBy(_.ts)
    val sortedQueries = queryTimes.sorted
    val evictionInterval = processor.minSmallWindowTileSize
    var nextEvictionTs = Long.MaxValue
    val kvStore = mutable.Map[Long, Array[Any]]()
    var eventIdx = 0
    val resultsByQueryTs = mutable.Map[Long, Array[Any]]()

    def firePendingEvictions(upToTs: Long): Unit = {
      if (!processor.hasSmallWindows) return
      while (nextEvictionTs <= upToTs) {
        processor.advanceWatermark(nextEvictionTs)
        val r = processor.onEviction(nextEvictionTs)
        if (r.todayEntry != null) kvStore(r.todayStart) = r.todayEntry
        nextEvictionTs += evictionInterval
      }
    }

    for (queryTs <- sortedQueries) {
      while (eventIdx < streamingEvents.length && streamingEvents(eventIdx).ts <= queryTs) {
        val event = streamingEvents(eventIdx)
        firePendingEvictions(event.ts)
        processor.advanceWatermark(event.ts)
        val result = processor.onEvent(event, event.ts)
        if (result.todayEntry != null) kvStore(result.todayStart) = result.todayEntry
        if (result.yesterdayEntry != null) kvStore(result.yesterdayStart) = result.yesterdayEntry
        if (nextEvictionTs == Long.MaxValue && processor.hasSmallWindows) {
          nextEvictionTs = TsUtils.round(event.ts, evictionInterval) + evictionInterval
        }
        eventIdx += 1
      }
      firePendingEvictions(queryTs)
      processor.advanceWatermark(queryTs)

      val (todayStart, yesterdayStart) = merger.streamingDayKeys(queryTs)
      val todayIr = processor.packTodayEntry()
      val yesterdayIr = kvStore.getOrElse(yesterdayStart, null)
      resultsByQueryTs(queryTs) = merger.merge(finalBatchIr, todayIr, yesterdayIr, todayStart, queryTs, batchEnd)
    }

    queryTimes.map(resultsByQueryTs)
  }

  /** Push-based: GigaTileStreamProcessor with batch IR loaded → finalized vectors. */
  def gigaTileAggregate(allEvents: Array[TestRow], queryTimes: Array[Long], aggregations: Seq[Aggregation],
                        schema: Seq[(String, DataType)], batchEnd: Long): Array[Array[Any]] = {
    val megaTileAgg = new MegaTileAggregator(aggregations, schema, tailBufferMillis = TailBufferMillis)
    val store = new InMemoryGigaTileStore(megaTileAgg.windowedAggregator)
    val processor = new GigaTileStreamProcessor(megaTileAgg, store)

    val batchEvents = allEvents.filter(_.ts < batchEnd)
    val onlineAgg = new SawtoothOnlineAggregator(batchEnd, aggregations, schema, tailBufferMillis = TailBufferMillis)
    var batchIr = onlineAgg.init
    batchEvents.foreach(row => batchIr = onlineAgg.update(batchIr, row))
    val normalizedBatchIr = onlineAgg.normalizeBatchIr(batchIr)
    val denormalizedBatchIr = onlineAgg.denormalizeBatchIr(normalizedBatchIr)

    val streamingEvents = allEvents.sortBy(_.ts)
    val sortedQueries = queryTimes.sorted
    val evictionInterval = processor.minEvictionInterval
    var nextEvictionTs = Long.MaxValue
    var lastEmitted: Array[Any] = null
    var eventIdx = 0
    val resultsByQueryTs = mutable.Map[Long, Array[Any]]()

    val batchResult = processor.onBatchUpdate(denormalizedBatchIr, batchEnd, batchEnd)
    if (batchResult.finalizedVector != null) lastEmitted = batchResult.finalizedVector
    if (batchResult.needsEvictionTimer && nextEvictionTs == Long.MaxValue) {
      nextEvictionTs = TsUtils.round(batchEnd, evictionInterval) + evictionInterval
    }

    def firePendingEvictions(upToTs: Long): Unit = {
      while (nextEvictionTs <= upToTs) {
        processor.advanceWatermark(nextEvictionTs)
        val r = processor.onEviction(nextEvictionTs)
        if (r.finalizedVector != null) lastEmitted = r.finalizedVector
        nextEvictionTs += evictionInterval
      }
    }

    for (queryTs <- sortedQueries) {
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
      firePendingEvictions(queryTs)
      processor.advanceWatermark(queryTs)
      val evictResult = processor.onEviction(queryTs)
      if (evictResult.finalizedVector != null) lastEmitted = evictResult.finalizedVector
      resultsByQueryTs(queryTs) = lastEmitted
    }

    queryTimes.map(resultsByQueryTs)
  }

  it should "push and pull produce identical results (batch fresh, SUM/COUNT/AVG)" in {
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

    val naive = naiveAggregate(events, queryTimes, aggregations, schema)
    val mega = megaTileAggregate(events, queryTimes, aggregations, schema, batchEnd)
    val giga = gigaTileAggregate(events, queryTimes, aggregations, schema, batchEnd)

    for (i <- queryTimes.indices) {
      assertTrue(s"push_vs_naive at ${queryTimes(i)}\n  naive: ${gson.toJson(naive(i))}\n  giga:  ${gson.toJson(giga(i))}",
                 approxEqual(giga(i), naive(i)))
      assertTrue(s"push_vs_pull at ${queryTimes(i)}\n  mega: ${gson.toJson(mega(i))}\n  giga: ${gson.toJson(giga(i))}",
                 approxEqual(giga(i), mega(i)))
    }
  }

  it should "push and pull produce identical results (batch delayed)" in {
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

    val naive = naiveAggregate(events, queryTimes, aggregations, schema)
    val mega = megaTileAggregate(events, queryTimes, aggregations, schema, batchEnd)
    val giga = gigaTileAggregate(events, queryTimes, aggregations, schema, batchEnd)

    for (i <- queryTimes.indices) {
      assertTrue(s"push_vs_naive_delayed at ${queryTimes(i)}\n  naive: ${gson.toJson(naive(i))}\n  giga:  ${gson.toJson(giga(i))}",
                 approxEqual(giga(i), naive(i)))
      assertTrue(s"push_vs_pull_delayed at ${queryTimes(i)}\n  mega: ${gson.toJson(mega(i))}\n  giga: ${gson.toJson(giga(i))}",
                 approxEqual(giga(i), mega(i)))
    }
  }

  it should "push and pull produce identical results (all agg types)" in {
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

    val naive = naiveAggregate(events, queryTimes, aggregations, schema)
    val mega = megaTileAggregate(events, queryTimes, aggregations, schema, batchEnd)
    val giga = gigaTileAggregate(events, queryTimes, aggregations, schema, batchEnd)

    for (i <- queryTimes.indices) {
      assertTrue(s"push_vs_naive_all_agg at ${queryTimes(i)}\n  naive: ${gson.toJson(naive(i))}\n  giga:  ${gson.toJson(giga(i))}",
                 approxEqual(giga(i), naive(i)))
      assertTrue(s"push_vs_pull_all_agg at ${queryTimes(i)}\n  mega: ${gson.toJson(mega(i))}\n  giga: ${gson.toJson(giga(i))}",
                 approxEqual(giga(i), mega(i)))
    }
  }

  it should "push and pull produce identical results (day boundaries)" in {
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

    val naive = naiveAggregate(events, queryTimes, aggregations, schema)
    val mega = megaTileAggregate(events, queryTimes, aggregations, schema, batchEnd)
    val giga = gigaTileAggregate(events, queryTimes, aggregations, schema, batchEnd)

    for (i <- queryTimes.indices) {
      assertTrue(s"push_vs_naive_daybound at ${queryTimes(i)}\n  naive: ${gson.toJson(naive(i))}\n  giga:  ${gson.toJson(giga(i))}",
                 approxEqual(giga(i), naive(i)))
      assertTrue(s"push_vs_pull_daybound at ${queryTimes(i)}\n  mega: ${gson.toJson(mega(i))}\n  giga: ${gson.toJson(giga(i))}",
                 approxEqual(giga(i), mega(i)))
    }
  }
}
