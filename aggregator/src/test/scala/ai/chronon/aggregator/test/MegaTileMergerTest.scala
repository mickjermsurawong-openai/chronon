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

class MegaTileMergerTest extends AnyFlatSpec {
  @transient lazy val logger = LoggerFactory.getLogger(getClass)
  val gson = new Gson

  val AllWindows: Seq[Window] = Seq(
    new Window(6, TimeUnit.HOURS),   // 6h  - NO BATCH, 5min tier
    new Window(1, TimeUnit.DAYS),    // 1d  - NO BATCH, 1hr tier
    new Window(47, TimeUnit.HOURS),  // 47h - NO BATCH, 1hr tier, just under tailBuffer
    new Window(2, TimeUnit.DAYS),    // 2d  - NO BATCH, 1hr tier, exactly tailBuffer
    new Window(49, TimeUnit.HOURS),  // 49h - BATCH, 1hr tier, just over tailBuffer
    new Window(3, TimeUnit.DAYS),    // 3d  - BATCH, 1hr tier
    new Window(7, TimeUnit.DAYS)     // 7d  - BATCH, 1hr tier
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
    case (x: Array[_], y: Array[_])           =>
      x.length == y.length && x.zip(y).forall { case (a, b) => approxEqual(a, b) }
    case _                                    => a == b
  }

  def compareResults(mergerResults: Array[Array[Any]],
                     naiveResults: Array[Array[Any]],
                     queryTimes: Array[Long],
                     label: String): Unit = {
    assertEquals(s"$label: result count mismatch", naiveResults.length, mergerResults.length)
    for (i <- queryTimes.indices) {
      if (!approxEqual(mergerResults(i), naiveResults(i))) {
        val naiveStr = gson.toJson(naiveResults(i))
        val mergerStr = gson.toJson(mergerResults(i))
        fail(s"$label: mismatch at query ${queryTimes(i)} (index $i)\n  expected: $naiveStr\n  got:      $mergerStr")
      }
    }
  }

  def generateEvents(windowDays: Int, count: Int): (Array[TestRow], Seq[(String, DataType)]) = {
    val columns = Seq(
      Column("ts", LongType, windowDays),
      Column("num", LongType, 1000),
      Column("amount", DoubleType, 500)
    )
    val data = CStream.gen(columns, count)
    (data.rows, columns.map(_.schema))
  }

  it should "fetch day keys from batch day through today plus one no-batch fallback day" in {
    val aggregations: Seq[Aggregation] = Seq(
      Builders.Aggregation(Operation.SUM, "num", AllWindows)
    )
    val schema: Seq[(String, DataType)] = Seq("ts" -> LongType, "num" -> LongType)
    val merger = new MegaTileMerger(new MegaTileAggregator(aggregations, schema, tailBufferMillis = TailBufferMillis))

    val todayStart = 1712275200000L // 2024-04-05T00:00:00Z
    val queryTs = todayStart + 90 * 60 * 1000L
    val batchEnd = 1711929600000L // 2024-04-01T00:00:00Z

    assertEquals(
      Seq(
        todayStart,
        1712188800000L,
        1712102400000L,
        1712016000000L,
        1711929600000L
      ),
      merger.streamingDayKeys(queryTs, batchEnd)
    )

    assertEquals(
      Seq(
        todayStart,
        1712188800000L
      ),
      merger.streamingDayKeys(queryTs, todayStart)
    )
  }

  it should "clear no-batch columns when no daily rows exist" in {
    val aggregations: Seq[Aggregation] = Seq(
      Builders.Aggregation(Operation.SUM, "num", Seq(new Window(6, TimeUnit.HOURS))),
      Builders.Aggregation(Operation.SUM, "num", Seq(new Window(7, TimeUnit.DAYS)))
    )
    val schema: Seq[(String, DataType)] = Seq("ts" -> LongType, "num" -> LongType)
    val batchEnd = 1712275200000L // 2024-04-05T00:00:00Z
    val queryTs = batchEnd + 6 * 3600 * 1000L

    val megaTileAgg = new MegaTileAggregator(aggregations, schema, tailBufferMillis = TailBufferMillis)
    val merger = new MegaTileMerger(megaTileAgg)
    val onlineAgg = new SawtoothOnlineAggregator(batchEnd, aggregations, schema, tailBufferMillis = TailBufferMillis)

    var batchIr = onlineAgg.init
    batchIr = onlineAgg.update(batchIr, new TestRow(batchEnd - 3600 * 1000L, 5L)(0))
    val finalBatchIr = onlineAgg.finalizeSnapshot(batchIr)

    val merged = merger.merge(
      finalBatchIr,
      Seq(
        TsUtils.round(queryTs, DayMillis) -> null,
        (TsUtils.round(queryTs, DayMillis) - DayMillis) -> null
      ),
      queryTs,
      batchEnd
    )

    assertNull(merged(0))
    assertEquals(5L, merged(1))
  }

  it should "include batch tail for same-day 1d no-batch windows when streaming only covers after batchEnd" in {
    val schema: Seq[(String, DataType)] = Seq("ts" -> LongType, "num" -> LongType)
    val batchEnd = 1776297600000L // 2026-04-16T00:00:00Z
    val queryTs = batchEnd + 16 * 3600 * 1000L // 2026-04-16T16:00:00Z
    val todayStart = TsUtils.round(queryTs, DayMillis)
    val oneHourMillis = new Window(1, TimeUnit.HOURS).millis
    val aggregations: Seq[Aggregation] = Seq(
      Builders.Aggregation(Operation.SUM, "num", Seq(new Window(1, TimeUnit.DAYS))),
      Builders.Aggregation(Operation.SUM, "num", Seq(new Window(7, TimeUnit.DAYS)))
    )
    val batchRows = Array(
      TestRow(batchEnd - 6 * 3600 * 1000L, 100L) // 2026-04-15T18:00:00Z, inside the query 1d window
    )
    val streamingRows = Array(
      TestRow(batchEnd + 1 * 3600 * 1000L, 10L),
      TestRow(batchEnd + 15 * 3600 * 1000L, 1L)
    )
    val allRows = batchRows ++ streamingRows

    val megaTileAgg = new MegaTileAggregator(aggregations, schema, tailBufferMillis = TailBufferMillis)
    val merger = new MegaTileMerger(megaTileAgg)
    val onlineAgg = new SawtoothOnlineAggregator(batchEnd, aggregations, schema, tailBufferMillis = TailBufferMillis)

    var batchIr = onlineAgg.init
    batchRows.foreach(row => batchIr = onlineAgg.update(batchIr, row))
    val finalBatchIr = onlineAgg.finalizeSnapshot(batchIr)

    val tiles: Map[Long, mutable.Map[Long, Array[Any]]] =
      megaTileAgg.activeTiers.map(hop => hop -> mutable.Map.empty[Long, Array[Any]]).toMap
    streamingRows.foreach { row =>
      megaTileAgg.tileStartsForEvent(row.ts).foreach {
        case (hopSize, tileStart) =>
          val ir = tiles(hopSize).getOrElseUpdate(tileStart, megaTileAgg.baseAggregator.init)
          megaTileAgg.baseAggregator.update(ir, row)
      }
    }

    val todayIr = megaTileAgg.buildMegaTileIr(tiles, now = queryTs, batchEnd = todayStart)
    val dailyTileIrs: Seq[(Long, Array[Any])] = Seq(
      todayStart -> todayIr,
      (todayStart - DayMillis) -> null
    )

    val mega = merger.merge(finalBatchIr, dailyTileIrs, queryTs, batchEnd, mergeNoBatchTailFromBatch = true)
    val megaWithoutBatch = merger.merge(null, dailyTileIrs, queryTs, batchEnd)
    val naive = naiveAggregate(allRows, Array(queryTs), aggregations, schema).head
    val vanillaTiles = mutable.Map.empty[Long, Array[Any]]
    streamingRows.foreach { row =>
      val tileStart = TsUtils.round(row.ts, oneHourMillis)
      val ir = vanillaTiles.getOrElseUpdate(tileStart, megaTileAgg.windowedAggregator.init)
      megaTileAgg.windowedAggregator.update(ir, row)
    }
    val vanilla = onlineAgg.lambdaAggregateFinalizedTiled(
      finalBatchIr,
      vanillaTiles.toSeq.map { case (tileStart, ir) => TiledIr(tileStart, ir) }.iterator,
      queryTs
    )

    assertEquals("sanity: last 1d should include pre-batch tail plus streaming rows", 111L, naive(0))
    assertEquals("sanity: vanilla tiled path includes the batch tail", naive(0), vanilla(0))
    assertEquals("sanity: without batch, the mega daily entry is only the post-batch streaming portion", 11L, megaWithoutBatch(0))
    assertEquals("1d no-batch path should include the pre-batch tail when the window crosses batchEnd", naive(0), mega(0))
    assertEquals("control: batch-backed 7d still merges the batch tail", naive(1), mega(1))
  }

  def naiveAggregate(allEvents: Array[TestRow],
                     queryTimes: Array[Long],
                     aggregations: Seq[Aggregation],
                     schema: Seq[(String, DataType)]): Array[Array[Any]] = {
    import scala.collection.JavaConverters._
    val unpackedParts = aggregations.flatMap(_.unpack)
    val unpacked = unpackedParts.map(_.window).toArray
    val tailHops = unpacked.map(w => FiveMinuteResolution.calculateTailHop(w))
    val rowAgg = new RowAggregator(schema, unpackedParts)
    val naiveAgg = new NaiveAggregator(rowAgg, unpacked, tailHops)
    val rawResults = naiveAgg.aggregate(allEvents, queryTimes)
    rawResults.map(ir => rowAgg.finalize(ir))
  }

  /**
    * Simulate the full Flink -> KV -> Fetcher pipeline using per-day entries.
    *
    * 1. Build batch IR from pre-batchEnd events (SawtoothOnlineAggregator)
    * 2. Maintain tiles in memory (same as MegaTileAggregatorTest)
    * 3. At each query time:
    *    - Ingest events up to queryTs into tiles
    *    - On day transition: freeze yesterday via buildMegaTileIr(tiles, todayStart, yesterdayStart)
    *    - Build today's live entry via buildMegaTileIr(tiles, queryTs, todayStart)
    *    - Call merger.merge(batchIr, todayIr, yesterdayIr, todayStart, queryTs, batchEnd)
    * 4. Compare with naive
    */
  def megaTileMergerAggregate(allEvents: Array[TestRow],
                              queryTimes: Array[Long],
                              aggregations: Seq[Aggregation],
                              schema: Seq[(String, DataType)],
                              batchEnd: Long): Array[Array[Any]] = {

    val megaTileAgg = new MegaTileAggregator(aggregations, schema, tailBufferMillis = TailBufferMillis)
    val merger = new MegaTileMerger(megaTileAgg)

    // 1. Build batch IR from pre-batchEnd events
    val batchEvents = allEvents.filter(_.ts < batchEnd)
    val onlineAgg =
      new SawtoothOnlineAggregator(batchEnd, aggregations, schema, tailBufferMillis = TailBufferMillis)
    var batchIr = onlineAgg.init
    batchEvents.foreach(row => batchIr = onlineAgg.update(batchIr, row))
    val finalBatchIr = onlineAgg.finalizeSnapshot(batchIr)

    // 2. Simulate Flink: bucket ALL events into small tiles per tier
    val tiles: Map[Long, mutable.Map[Long, Array[Any]]] =
      megaTileAgg.activeTiers.map(hop => hop -> mutable.Map.empty[Long, Array[Any]]).toMap

    val sortedEvents = allEvents.sortBy(_.ts)
    val sortedQueries = queryTimes.sorted
    var eventIdx = 0
    val resultsByQueryTs = mutable.Map[Long, Array[Any]]()

    for (queryTs <- sortedQueries) {
      val (todayStart, yesterdayStart) = merger.streamingDayKeys(queryTs)

      // Ingest events that have arrived by queryTs
      while (eventIdx < sortedEvents.length && sortedEvents(eventIdx).ts <= queryTs) {
        val event = sortedEvents(eventIdx)
        val tileStarts = megaTileAgg.tileStartsForEvent(event.ts)
        for ((hopSize, tileStart) <- tileStarts) {
          val tierTiles = tiles(hopSize)
          val ir = tierTiles.getOrElseUpdate(tileStart, megaTileAgg.baseAggregator.init)
          megaTileAgg.baseAggregator.update(ir, event)
        }
        eventIdx += 1
      }

      // Build today's live entry: small windows scope to [queryTs-window, queryTs),
      // large windows scope to [todayStart, queryTs)
      val todayIr = megaTileAgg.buildMegaTileIr(tiles, now = queryTs, batchEnd = todayStart)

      // Build yesterday's entry: small windows scope to [todayStart-window, todayStart),
      // large windows scope to [yesterdayStart, todayStart).
      // Safe to build on-the-fly because buildMegaTileIr only considers tiles with
      // tileStart < now (=todayStart), so today's events don't leak into yesterday.
      val yesterdayIr = megaTileAgg.buildMegaTileIr(tiles, now = todayStart, batchEnd = yesterdayStart)

      // Merge using the fetcher logic
      resultsByQueryTs(queryTs) = merger.merge(finalBatchIr, todayIr, yesterdayIr, todayStart, queryTs, batchEnd)
    }

    // Return results in original query order
    queryTimes.map(resultsByQueryTs)
  }

  it should "match naive aggregation with batch fresh" in {
    val (events, schema) = generateEvents(10, 10000)
    val maxTs = events.map(_.ts).max
    val batchEnd = TsUtils.round(maxTs - DayMillis, DayMillis)

    val aggregations: Seq[Aggregation] = Seq(
      Builders.Aggregation(Operation.SUM, "num", AllWindows),
      Builders.Aggregation(Operation.COUNT, "num", AllWindows),
      Builders.Aggregation(Operation.AVERAGE, "amount", AllWindows)
    )

    val queryTimes = Array(
      batchEnd + 6 * 3600 * 1000L,
      batchEnd + 14 * 3600 * 1000L,
      batchEnd + 23 * 3600 * 1000L
    ).filter(_ <= maxTs)

    val mergerResults = megaTileMergerAggregate(events, queryTimes, aggregations, schema, batchEnd)
    val naiveResults = naiveAggregate(events, queryTimes, aggregations, schema)
    compareResults(mergerResults, naiveResults, queryTimes, "batch_fresh")
  }

  it should "match naive aggregation with batch delayed" in {
    val (events, schema) = generateEvents(10, 10000)
    val maxTs = events.map(_.ts).max
    val batchEnd = TsUtils.round(maxTs - 2 * DayMillis, DayMillis)

    val aggregations: Seq[Aggregation] = Seq(
      Builders.Aggregation(Operation.SUM, "num", AllWindows),
      Builders.Aggregation(Operation.COUNT, "num", AllWindows)
    )

    val queryTimes = Array(
      batchEnd + DayMillis + 2 * 3600 * 1000L,
      batchEnd + DayMillis + 5 * 3600 * 1000L
    ).filter(_ <= maxTs)

    val mergerResults = megaTileMergerAggregate(events, queryTimes, aggregations, schema, batchEnd)
    val naiveResults = naiveAggregate(events, queryTimes, aggregations, schema)
    compareResults(mergerResults, naiveResults, queryTimes, "batch_delayed")
  }

  it should "handle idle entities with no streaming events" in {
    val (events, schema) = generateEvents(10, 5000)
    val maxTs = events.map(_.ts).max
    val batchEnd = maxTs + DayMillis

    val aggregations: Seq[Aggregation] = Seq(
      Builders.Aggregation(Operation.SUM, "num", AllWindows),
      Builders.Aggregation(Operation.COUNT, "num", AllWindows)
    )

    val queryTimes = Array(batchEnd + 14 * 3600 * 1000L)

    val mergerResults = megaTileMergerAggregate(events, queryTimes, aggregations, schema, batchEnd)
    val naiveResults = naiveAggregate(events, queryTimes, aggregations, schema)
    compareResults(mergerResults, naiveResults, queryTimes, "idle_entity")
  }

  it should "match naive with multiple aggregation types" in {
    val (events, schema) = generateEvents(10, 10000)
    val maxTs = events.map(_.ts).max
    val batchEnd = TsUtils.round(maxTs - DayMillis, DayMillis)

    val aggregations: Seq[Aggregation] = Seq(
      Builders.Aggregation(Operation.SUM, "num", AllWindows),
      Builders.Aggregation(Operation.COUNT, "num", AllWindows),
      Builders.Aggregation(Operation.AVERAGE, "amount", AllWindows),
      Builders.Aggregation(Operation.MIN, "num", AllWindows),
      Builders.Aggregation(Operation.MAX, "num", AllWindows),
      Builders.Aggregation(Operation.LAST, "num", AllWindows),
      Builders.Aggregation(Operation.FIRST, "num", AllWindows)
    )

    val queryTimes = Array(batchEnd + 14 * 3600 * 1000L).filter(_ <= maxTs)

    val mergerResults = megaTileMergerAggregate(events, queryTimes, aggregations, schema, batchEnd)
    val naiveResults = naiveAggregate(events, queryTimes, aggregations, schema)
    compareResults(mergerResults, naiveResults, queryTimes, "multi_agg_types")
  }

  it should "handle day boundary transitions" in {
    val (events, schema) = generateEvents(10, 10000)
    val maxTs = events.map(_.ts).max
    val batchEnd = TsUtils.round(maxTs - 2 * DayMillis, DayMillis)

    val aggregations: Seq[Aggregation] = Seq(
      Builders.Aggregation(Operation.SUM, "num", AllWindows),
      Builders.Aggregation(Operation.COUNT, "num", AllWindows),
      Builders.Aggregation(Operation.AVERAGE, "amount", AllWindows)
    )

    // Queries spanning 2 days after batchEnd
    val queryTimes = Array(
      batchEnd + 6 * 3600 * 1000L,   // day 1 morning
      batchEnd + 18 * 3600 * 1000L,  // day 1 evening
      batchEnd + 30 * 3600 * 1000L,  // day 2 morning
      batchEnd + 42 * 3600 * 1000L   // day 2 evening
    ).filter(_ <= maxTs)

    val mergerResults = megaTileMergerAggregate(events, queryTimes, aggregations, schema, batchEnd)
    val naiveResults = naiveAggregate(events, queryTimes, aggregations, schema)
    compareResults(mergerResults, naiveResults, queryTimes, "day_boundary")
  }
}
