package ai.chronon.flink.test.window

import ai.chronon.api._
import ai.chronon.api.Extensions.WindowOps
import ai.chronon.flink.FlinkJob
import ai.chronon.flink.deser.ProjectedEvent
import ai.chronon.flink.types.TimestampedTile
import ai.chronon.flink.window.{MegaTileEmissionPolicy, MegaTileProcessFunction}
import ai.chronon.online.MegaTileCodec
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.operators.KeyedProcessOperator
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.Instant
import java.util
import scala.collection.JavaConverters._

class MegaTileProcessFunctionTest extends AnyFlatSpec with Matchers {
  import MegaTileProcessFunctionTest._

  /*
   * Output values use the order from the test GroupBy:
   *   windowValues(oneHour, oneDay, threeDay)
   *
   * Each value is the count of view_by rows for that window. null means that window has no
   * aggregate value after eviction.
   */

  "MegaTileProcessFunction" should "watermark constants stay aligned with MegaTile eviction lifecycle" in {
    // With a 5-minute hop, a 5-minute bounded-out-of-orderness watermark keeps late events near the
    // hop boundary available until the processor has a chance to rebuild cached small-window state.
    FlinkJob.AllowedOutOfOrderness.toMillis shouldEqual 5 * 60 * 1000L
    // Idleness keeps watermarks moving when an upstream partition or low-volume key goes quiet.
    FlinkJob.IdlenessTimeout.toMillis shouldEqual 30 * 1000L
    // The extra slack prevents normal one-hop watermark jitter from being treated as active catchup.
    FlinkJob.CatchupWatermarkLagSlackMillis shouldEqual 30 * 1000L
  }

  it should "Live vs ActiveCatchup boundary: one-hop watermark lag plus slack still uses PT eviction" in {
    withDriver(bufferingOutputTimeMillis = 0L) { driver =>
      seedOldTileBeforeLiveCatchupBoundary(driver, "gen_live_slack")

      // The watermark is 5m15s behind PT, which is still inside one hop plus the 30s slack. The
      // current event is therefore treated as live and immediately sees the old tile in the cache.
      driver.processWatermark("2025-07-21T11:29:45Z")
      driver.setProcessingTime("2025-07-21T11:34:00Z")
      driver.processEvent("gen_live_slack", "2025-07-21T11:34:00Z", "user_current")
      driver.drainNewOutputs().last.values shouldEqual windowValues(3L, 3L, 3L)

      // The 11:35 PT eviction rebuilds 1h over [10:35, 11:35), so the old 10:30 tile falls out.
      driver.setProcessingTime("2025-07-21T11:35:00Z")
      assertSingleOutput(
        driver.drainNewOutputs(),
        expectedKey = "gen_live_slack",
        expectedDayStart = dayStart("2025-07-21T00:00:00Z"),
        expectedValues = windowValues(1L, 3L, 3L))
    }
  }

  it should "Live vs ActiveCatchup boundary: active catchup beyond slack uses watermark-time eviction" in {
    withDriver(bufferingOutputTimeMillis = 0L) { driver =>
      seedOldTileBeforeLiveCatchupBoundary(driver, "gen_catchup")

      // This watermark is 5m31s behind PT, just beyond one hop plus slack. A recently active key
      // stays in ActiveCatchup, so eviction follows the next watermark hop instead of wall-clock PT.
      driver.processWatermark("2025-07-21T11:29:29Z")
      driver.setProcessingTime("2025-07-21T11:34:00Z")
      driver.processEvent("gen_catchup", "2025-07-21T11:34:00Z", "user_current")
      driver.drainNewOutputs().last.values shouldEqual windowValues(3L, 3L, 3L)

      // The watermark-aligned rebuild is as of 11:30, so the old 10:30 tile is still inside 1h.
      driver.setProcessingTime("2025-07-21T11:35:00Z")
      assertSingleOutput(
        driver.drainNewOutputs(),
        expectedKey = "gen_catchup",
        expectedDayStart = dayStart("2025-07-21T00:00:00Z"),
        expectedValues = windowValues(2L, 3L, 3L))
    }
  }

  it should "Live mode lifecycle: continuous events before, during, and after day transition" in {
    withDriver(bufferingOutputTimeMillis = 0L) { driver =>
      // Seed the final Jul21 tile before either PT or watermark day rollover occurs.
      driver.setProcessingTime("2025-07-21T23:59:00Z")
      driver.processEvent("gen_steady", "2025-07-21T23:59:00Z", "user_before_roll")
      assertSingleOutput(
        driver.drainNewOutputs(),
        expectedKey = "gen_steady",
        expectedDayStart = dayStart("2025-07-21T00:00:00Z"),
        expectedValues = windowValues(1L, 1L, 1L))

      // PT eviction can fire first; it should not rely on the watermark crossing midnight.
      driver.setProcessingTime("2025-07-22T00:00:01Z")
      driver.drainNewOutputs() should not be empty

      // Once the watermark crosses midnight, the processor emits the old day before applying Jul22.
      driver.processWatermark("2025-07-22T00:00:00Z")
      driver.processEvent("gen_steady", "2025-07-22T00:00:00Z", "user_after_roll")
      val rolloverOutputs = driver.drainNewOutputs()
      assertSingleOutput(
        rolloverOutputs,
        expectedKey = "gen_steady",
        expectedDayStart = dayStart("2025-07-22T00:00:00Z"),
        expectedValues = windowValues(2L, 2L, 1L))

      // After the first Jul22 hop boundary, an exact-hop Jul22 event updates both no-batch windows.
      driver.setProcessingTime("2025-07-22T00:05:01Z")
      driver.drainNewOutputs() should not be empty

      driver.processEvent("gen_steady", "2025-07-22T00:05:00Z", "user_exact_hop")
      val todayOutputs = driver.drainNewOutputs()
      todayOutputs.last.dayStartMillis shouldEqual dayStart("2025-07-22T00:00:00Z")
      todayOutputs.last.values shouldEqual windowValues(3L, 3L, 2L)
    }
  }

  it should "Live mode lifecycle: PT midnight roll before watermark routes late previous-day events to both day rows" in {
    withDriver(bufferingOutputTimeMillis = 1000L) { driver =>
      // Keep watermark just before midnight but close enough to PT that this key remains Live.
      driver.processWatermark("2025-07-21T23:54:55.999Z")
      driver.setProcessingTime("2025-07-21T23:59:56.900Z")
      driver.processEvent("gen_midnight_live", "2025-07-21T23:59:56Z", "user_before_midnight")
      driver.drainNewOutputs() shouldBe empty

      // Buffered emit publishes the Jul21 row once the 1s delay expires.
      driver.setProcessingTime("2025-07-21T23:59:57.900Z")
      assertSingleOutput(
        driver.drainNewOutputs(),
        expectedKey = "gen_midnight_live",
        expectedDayStart = dayStart("2025-07-21T00:00:00Z"),
        expectedValues = windowValues(1L, 1L, 1L))

      // PT reaches midnight before the watermark. The buffered new-day row should not emit yet.
      driver.setProcessingTime("2025-07-22T00:00:00.001Z")
      driver.drainNewOutputs() shouldBe empty

      // A pre-midnight event arriving after the PT roll is still admissible. It belongs to Jul22
      // small windows because it is inside [23:05, 00:05), and to Jul21 large-window yesterday IR.
      driver.setProcessingTime("2025-07-22T00:00:00.006Z")
      driver.processEvent("gen_midnight_live", "2025-07-21T23:59:58Z", "user_previous_day_after_roll")
      driver.drainNewOutputs() shouldBe empty

      // The delayed callback emits both affected day rows: current small windows for Jul22 and
      // a complete previous-day row for Jul21.
      driver.setProcessingTime("2025-07-22T00:00:01.001Z")
      val boundaryOutputs = driver.drainNewOutputs()
      boundaryOutputs should have size 2
      boundaryOutputs.find(_.dayStartMillis == dayStart("2025-07-22T00:00:00Z")).map(_.values) shouldEqual
        Some(windowValues(2L, 2L, null))
      boundaryOutputs.find(_.dayStartMillis == dayStart("2025-07-21T00:00:00Z")).map(_.values) shouldEqual
        Some(windowValues(1L, 1L, 2L))
    }
  }

  it should "Live mode: retained yesterdayStart updates 1d without incrementing stale 1h" in {
    withDriver(bufferingOutputTimeMillis = 0L) { driver =>
      // Anchor the current-day cache at Jul22 start.
      driver.setProcessingTime("2025-07-22T00:10:00Z")
      driver.processEvent("gen_boundary", "2025-07-22T00:00:00Z", "user_today_start")
      driver.drainNewOutputs() should have size 1

      // The exact Jul21 boundary is retained as yesterday, but as of Jul22 00:15 the 1h horizon is
      // [Jul21 23:15, Jul22 00:15), so only 1d and 3d may include it.
      driver.processWatermark("2025-07-22T00:05:00Z")
      driver.processEvent("gen_boundary", "2025-07-21T00:00:00Z", "user_yesterday_start")
      val outputs = driver.drainNewOutputs()
      outputs should have size 2
      // Current-day packed row keeps 1h at one event while 1d includes the retained boundary event.
      outputs.find(_.dayStartMillis == dayStart("2025-07-22T00:00:00Z")).map(_.values) shouldEqual
        Some(windowValues(1L, 2L, 1L))
      // Without a prior rollover, there is no frozen no-batch snapshot for the previous day.
      outputs.find(_.dayStartMillis == dayStart("2025-07-21T00:00:00Z")).map(_.values) shouldEqual
        Some(windowValues(null, null, 1L))
    }
  }

  it should "Live mode lifecycle: PT eviction decays during a short stop and recovers on resume" in {
    withDriver(bufferingOutputTimeMillis = 0L) { driver =>
      // Initial event starts all three windows in Live mode.
      driver.processWatermark("2025-07-21T10:25:00Z")
      driver.setProcessingTime("2025-07-21T10:30:00Z")
      driver.processEvent("gen_stop_short", "2025-07-21T10:30:00Z", "user_initial")
      driver.drainNewOutputs().last.values shouldEqual windowValues(1L, 1L, 1L)

      // Keep the watermark close enough that the next two PT timers remain Live.
      driver.processWatermark("2025-07-21T11:25:00Z")
      // At 11:30 the 10:30 tile is still inside the 1h window.
      driver.setProcessingTime("2025-07-21T11:30:00Z")
      driver.drainNewOutputs().last.values shouldEqual windowValues(1L, 1L, 1L)

      driver.processWatermark("2025-07-21T11:29:30Z")
      // At 11:35 the 1h window starts at 10:35, so only the stopped key's 1h value decays.
      driver.setProcessingTime("2025-07-21T11:35:00Z")
      driver.drainNewOutputs().last.values shouldEqual windowValues(null, 1L, 1L)

      // A live resume event restarts 1h and increments the longer windows.
      driver.processEvent("gen_stop_short", "2025-07-21T11:36:00Z", "user_resume")
      driver.drainNewOutputs().last.values shouldEqual windowValues(1L, 2L, 2L)
    }
  }

  it should "Live mode lifecycle: late retained tile outside smallWindowAsOfTs does not re-inflate 1h" in {
    withDriver(bufferingOutputTimeMillis = 0L) { driver =>
      // 10:30 lands in the 10:30-10:35 tile in Live mode and starts every window.
      driver.processWatermark("2025-07-21T10:25:00Z")
      driver.setProcessingTime("2025-07-21T10:30:00Z")
      driver.processEvent("gen_late_same_day", "2025-07-21T10:30:00Z", "user_on_time")
      driver.drainNewOutputs().last.values shouldEqual windowValues(1L, 1L, 1L)

      // PT eviction at 11:35 rebuilds 1h over [10:35, 11:35), so the 10:30 tile falls out.
      driver.processWatermark("2025-07-21T11:29:30Z")
      driver.setProcessingTime("2025-07-21T11:35:00Z")
      driver.drainNewOutputs().last.values shouldEqual windowValues(null, 1L, 1L)

      // A late 10:32 event still updates the retained base tile and longer windows, but its tile is
      // outside the current 1h as-of horizon and must not re-inflate cached 1h.
      driver.setProcessingTime("2025-07-21T11:36:00Z")
      driver.processWatermark("2025-07-21T11:30:00Z")
      driver.processEvent("gen_late_same_day", "2025-07-21T10:32:00Z", "user_late_expired_hop")
      driver.drainNewOutputs().last.values shouldEqual windowValues(null, 2L, 2L)
    }
  }

  it should "SparseKeyLag lifecycle: long idle stop expires 1d, then Live resume restarts all windows" in {
    withDriver(bufferingOutputTimeMillis = 0L) { driver =>
      // Seed the key, then let PT move more than one day ahead with no new events.
      driver.processWatermark("2025-07-21T10:25:00Z")
      driver.setProcessingTime("2025-07-21T10:30:00Z")
      driver.processEvent("gen_stop_long", "2025-07-21T10:30:00Z", "user_initial")
      driver.drainNewOutputs() should have size 1

      // The key is idle while the watermark is stale, so SparseKeyLag uses PT for eviction. The
      // timer rolls to Jul22 and rebuilds an empty current-day row for no-batch windows.
      driver.setProcessingTime("2025-07-22T11:35:00Z")
      driver.drainNewOutputs().last.values shouldEqual windowValues(null, null, null)

      // A fresh Jul22 event restarts every current serving window once the watermark catches up
      // enough for Live mode.
      driver.processWatermark("2025-07-22T11:31:00Z")
      driver.processEvent("gen_stop_long", "2025-07-22T11:36:00Z", "user_resume")
      driver.drainNewOutputs().last.values shouldEqual windowValues(1L, 1L, 1L)
    }
  }

  it should "Live mode after idle eviction: retained yesterdayStart updates 1d without incrementing stale 1h" in {
    withDriver(bufferingOutputTimeMillis = 0L) { driver =>
      // Anchor current-day state, then let one PT hop eviction run while the key is otherwise idle.
      driver.processWatermark("2025-07-22T00:05:00Z")
      driver.setProcessingTime("2025-07-22T00:10:00Z")
      driver.processEvent("gen_stop_boundary", "2025-07-22T00:10:00Z", "user_anchor")
      driver.drainNewOutputs() should have size 1

      driver.processWatermark("2025-07-22T00:09:30Z")
      driver.setProcessingTime("2025-07-22T00:15:00Z")
      driver.drainNewOutputs() should not be empty

      // The retained Jul21 boundary updates Jul22 1d, but remains outside Jul22 1h after the idle
      // PT eviction has already advanced the processor.
      driver.processEvent("gen_stop_boundary", "2025-07-21T00:00:00Z", "user_yesterday_start")
      val outputs = driver.drainNewOutputs()
      outputs should have size 2
      // Current day sees the retained boundary in 1d only.
      outputs.find(_.dayStartMillis == dayStart("2025-07-22T00:00:00Z")).map(_.values) shouldEqual
        Some(windowValues(1L, 2L, 1L))
      // Yesterday row carries only the batch-backed 3d contribution.
      outputs.find(_.dayStartMillis == dayStart("2025-07-21T00:00:00Z")).map(_.values) shouldEqual
        Some(windowValues(null, null, 1L))

      // Just older than retained yesterday is dropped before it can mutate tile or large-window state.
      driver.processEvent("gen_stop_boundary", "2025-07-20T23:59:59.999Z", "user_too_old")
      driver.drainNewOutputs() shouldBe empty
    }
  }

  it should "ActiveCatchup lifecycle: active backlog uses watermark-time eviction, then SparseKeyLag fallback uses PT" in {
    withDriver(bufferingOutputTimeMillis = 0L) { driver =>
      // Replay starts two days behind PT; active per-key traffic keeps eviction on watermark time.
      driver.setProcessingTime("2025-07-23T10:30:00Z")
      driver.processWatermark("2025-07-21T10:30:00Z")
      driver.processEvent("gen_replay", "2025-07-21T10:30:00Z", "user_replay_1")
      driver.drainNewOutputs().last.dayStartMillis shouldEqual dayStart("2025-07-21T00:00:00Z")

      // The second replayed event stays in the Jul21 day because the key is still active.
      driver.setProcessingTime("2025-07-23T10:35:00Z")
      driver.processEvent("gen_replay", "2025-07-21T10:31:00Z", "user_replay_2")
      driver.drainNewOutputs().last.values shouldEqual windowValues(2L, 2L, 2L)

      // Once the key is idle for more than one hop, sparse-key fallback rolls to the PT day in
      // state. The rebuild does not need to publish a row when the packed value is unchanged.
      driver.setProcessingTime("2025-07-23T10:50:00Z")
      driver.drainNewOutputs() shouldBe empty
    }
  }

  it should "ActiveCatchup lifecycle: event ahead of stale watermark is retained, then appears after rebuild catches up" in {
    withDriver(bufferingOutputTimeMillis = 0L) { driver =>
      driver.processWatermark("2025-07-21T22:31:23.999Z")
      driver.setProcessingTime("2025-07-21T23:40:01Z")
      // ActiveCatchup evaluates no-batch cache as of the next watermark hop, so the current 23:40
      // event is retained but not immediately visible in 1h/1d.
      driver.processEvent("gen_resume_stale_watermark", "2025-07-21T23:40:00Z", "user_resume")
      driver.drainNewOutputs().last.values shouldEqual windowValues(null, null, 1L)

      // Once the watermark reaches the event-time frontier, the scheduled eviction rebuild includes
      // the retained 23:40 tile.
      driver.processWatermark("2025-07-21T23:40:00Z")
      driver.setProcessingTime("2025-07-21T23:45:00Z")
      driver.drainNewOutputs().last.values shouldEqual windowValues(1L, 1L, 1L)
    }
  }

  it should "catchup UTC day-boundary scenario matrix documents the four branch-driving axes" in {
    /*
     * These scenarios cover four independent axes that drive admission and rebuild behavior:
     *
     * 1. Resume timing: PT crosses UTC midnight before, near, or long after the stream watermark.
     * 2. Watermark state: watermark may be close enough for Live mode or stale enough for catchup.
     * 3. Key activity: an active key uses ActiveCatchup, while an idle key falls back to SparseKeyLag.
     * 4. Event age: exact yesterday-start rows are still retained, while one millisecond older rows
     *    are rejected before they can mutate state.
     */
    val scenarios = Seq(
      activeKeyResumesAfterUtcMidnightWithStaleWatermarkThenCatchesUp _,
      sparseKeyResumesAfterUtcMidnightWithStaleWatermark _,
      yesterdayBoundaryEventAfterResumeIsAdmitted _,
      olderThanYesterdayEventAfterResumeIsDropped _
    )

    scenarios.foreach { scenario =>
      withDriver(bufferingOutputTimeMillis = 0L)(scenario)
    }
  }

  it should "catchup UTC day-boundary: multi-day downtime jump does not emit old dirty day as adjacent rollover" in {
    withDriver(bufferingOutputTimeMillis = 3L * DayMillis) { driver =>
      // Buffer an Apr11 dirty row, but do not let its long delayed emit fire yet.
      driver.setProcessingTime("2026-04-11T23:50:00Z")
      driver.processEvent("axis_multi_day_jump", "2026-04-11T23:50:00Z", "user_seed")
      driver.drainNewOutputs() shouldBe empty

      // A PT jump to Apr13 is not an adjacent Apr11->Apr12 rollover. The guarded rollover emit
      // must not publish the old Apr11 row at the Apr13 boundary.
      driver.setProcessingTime("2026-04-13T00:10:00Z")
      driver.drainNewOutputs() shouldBe empty

      // After the PT day roll, Apr11 backlog is older than the retained yesterday boundary and is
      // dropped before mutating state.
      driver.processEvent("axis_multi_day_jump", "2026-04-11T23:59:00Z", "user_backlog_too_old_after_jump")
      driver.drainNewOutputs() shouldBe empty
    }
  }

  it should "ActiveCatchup lifecycle: replayed yesterdayStart updates 1d but not stale 1h" in {
    withDriver(bufferingOutputTimeMillis = 0L) { driver =>
      // Start replay on Jul22 event time while PT is already Jul24.
      driver.setProcessingTime("2025-07-24T10:00:00Z")
      driver.processWatermark("2025-07-22T00:00:00Z")
      driver.processEvent("gen_replay_boundary", "2025-07-22T00:00:00Z", "user_today_start")
      assertSingleOutput(
        driver.drainNewOutputs(),
        expectedKey = "gen_replay_boundary",
        expectedDayStart = dayStart("2025-07-22T00:00:00Z"),
        expectedValues = windowValues(1L, 1L, 1L))

      // Watermark rollover makes the duplicate Jul22 row a retained yesterday row for Jul23 output.
      driver.processWatermark("2025-07-23T00:00:00Z")
      driver.processEvent("gen_replay_boundary", "2025-07-22T00:00:00Z", "user_yesterday_start")
      val outputs = driver.drainNewOutputs()
      outputs should have size 2
      // In replay mode, the small-window as-of timestamp follows the watermark hop: 1d can include
      // the duplicate boundary row, while 1h must not double-count after the Jul23 day roll.
      outputs.find(_.dayStartMillis == dayStart("2025-07-23T00:00:00Z")).map(_.values) shouldEqual
        Some(windowValues(null, 2L, null))
      outputs.find(_.dayStartMillis == dayStart("2025-07-22T00:00:00Z")).map(_.values) shouldEqual
        Some(windowValues(1L, 1L, 2L))

      // Older-than-yesterday replay data is dropped relative to the post-roll currentDayStart.
      driver.processEvent("gen_replay_boundary", "2025-07-21T23:59:59.999Z", "user_too_old")
      driver.drainNewOutputs() shouldBe empty
    }
  }

  it should "SparseKeyLag lifecycle: sparse-key PT fallback can permanently drop backlog events older than yesterday after day roll" in {
    withDriver(bufferingOutputTimeMillis = 0L) { driver =>
      // Seed a sparse key during replay; the key then goes idle while the global watermark remains behind.
      driver.setProcessingTime("2025-07-23T10:30:00Z")
      driver.processWatermark("2025-07-21T10:30:00Z")
      driver.processEvent("gen_sparse_replay_drop", "2025-07-21T10:30:00Z", "user_seed")
      assertSingleOutput(
        driver.drainNewOutputs(),
        expectedKey = "gen_sparse_replay_drop",
        expectedDayStart = dayStart("2025-07-21T00:00:00Z"),
        expectedValues = windowValues(1L, 1L, 1L))

      // After one idle hop, SparseKeyLag uses PT eviction and rolls currentDayStart to Jul23. The
      // rebuild can be in-memory-only when the packed value is unchanged.
      driver.setProcessingTime("2025-07-23T10:40:00.001Z")
      driver.drainNewOutputs().lastOption.foreach { output =>
        output.keys shouldEqual List("gen_sparse_replay_drop")
        output.dayStartMillis shouldEqual dayStart("2025-07-23T00:00:00Z")
        output.values shouldEqual windowValues(null, null, null)
      }

      // A later Jul21 backlog event would be valid under active replay, but after the PT roll it is
      // older than currentDayStart - 1d and must be dropped permanently.
      driver.setProcessingTime("2025-07-23T10:41:00Z")
      driver.processEvent("gen_sparse_replay_drop", "2025-07-21T23:59:00Z", "user_backlog_too_late")
      driver.drainNewOutputs() shouldBe empty
    }
  }

  it should "Cold start backlog lifecycle: stream starts 2d behind and rolls old-day rows without dropping valid events" in {
    withDriver(bufferingOutputTimeMillis = 120000L) { driver =>
      // Cold start begins with PT on Jul23 but watermark/event time on Jul21.
      driver.setProcessingTime("2025-07-23T10:30:00Z")
      driver.processWatermark("2025-07-21T23:59:00Z")
      driver.processEvent("gen_cold_start", "2025-07-21T23:59:00Z", "user_day_1")
      driver.drainNewOutputs() shouldBe empty

      // Crossing the Jul22 watermark emits the buffered Jul21 row before applying the Jul22 event.
      driver.processWatermark("2025-07-22T00:00:01Z")
      driver.processEvent("gen_cold_start", "2025-07-22T00:00:00Z", "user_day_2")
      assertSingleOutput(
        driver.drainNewOutputs(),
        expectedKey = "gen_cold_start",
        expectedDayStart = dayStart("2025-07-21T00:00:00Z"),
        expectedValues = windowValues(1L, 1L, 1L))

      // Add rows around the Jul22 00:05 hop, plus one retained previous-day row and one too-old row.
      driver.processEvent("gen_cold_start", "2025-07-22T00:05:00Z", "user_exact_hop")
      driver.processEvent("gen_cold_start", "2025-07-22T00:05:01Z", "user_after_hop")
      driver.processEvent("gen_cold_start", "2025-07-21T00:00:00Z", "user_prev_day")
      driver.processEvent("gen_cold_start", "2025-07-20T23:59:59.999Z", "user_too_old")
      driver.drainNewOutputs() shouldBe empty

      // Buffered emit fires before the first watermark-aligned eviction rebuild.
      driver.setProcessingTime("2025-07-23T10:32:00Z")
      val preEvictionOutputs = driver.drainNewOutputs()
      preEvictionOutputs should have size 2
      // The retained Jul21 daily row keeps its frozen no-batch snapshot when later yesterday
      // updates republish the same day key.
      preEvictionOutputs.find(_.dayStartMillis == dayStart("2025-07-21T00:00:00Z")).map(_.values) shouldEqual
        Some(windowValues(1L, 1L, 2L))
      // Before the rebuild, cached 1h is bounded by watermark-hop smallWindowAsOfTs: Jul21 23:59
      // and Jul22 00:00 count for 1h; Jul21 00:00 is retained only for 1d/3d.
      preEvictionOutputs.find(_.dayStartMillis == dayStart("2025-07-22T00:00:00Z")).map(_.values) shouldEqual
        Some(windowValues(2L, 5L, 3L))

      // The 10:35 PT callback is the eviction timer; output remains buffered until 10:37.
      driver.setProcessingTime("2025-07-23T10:35:00Z")
      driver.drainNewOutputs() shouldBe empty

      // Buffered output after eviction reflects the watermark-aligned rebuild.
      driver.setProcessingTime("2025-07-23T10:37:00Z")
      assertSingleOutput(
        driver.drainNewOutputs(),
        expectedKey = "gen_cold_start",
        expectedDayStart = dayStart("2025-07-22T00:00:00Z"),
        expectedValues = windowValues(2L, 5L, 3L))
    }
  }

  it should "Stale event lifecycle: events older than yesterday are dropped before mutating state" in {
    withDriver(bufferingOutputTimeMillis = 0L) { driver =>
      // Anchor currentDayStart at Apr12.
      driver.processWatermark("2026-04-12T00:05:00Z")
      driver.setProcessingTime("2026-04-12T00:10:00Z")
      driver.processEvent("gen_stale", "2026-04-12T00:10:00Z", "user_anchor")
      driver.drainNewOutputs() should have size 1

      // Apr10 23:59:59.999 is one millisecond older than retained yesterday and should not dirty state.
      driver.processEvent("gen_stale", "2026-04-10T23:59:59.999Z", "user_too_old")
      driver.drainNewOutputs() shouldBe empty
    }
  }

  it should "Failure/recovery lifecycle: checkpoint restore preserves pending timers, dirty bits, and key isolation" in {
    val originalHarness = harness(bufferingOutputTimeMillis = 10 * 60 * 1000L)
    originalHarness.open()
    val baseProcessingTs = toMillis("2025-07-21T10:30:00Z")
    // Buffer two dirty keys, then snapshot before either key's emit timer fires.
    originalHarness.setProcessingTime(baseProcessingTs)
    originalHarness.processElement(event("gen_restore_a", "2025-07-21T10:30:00Z", "user_a_1", baseProcessingTs), 0L)
    originalHarness.processElement(event("gen_restore_b", "2025-07-21T10:31:00Z", "user_b_1", baseProcessingTs), 0L)
    originalHarness.extractOutputValues().asScala.toList shouldBe empty

    // Snapshot must capture dirty bits, pending timers, and per-key MegaTile state.
    val snapshot = originalHarness.snapshot(7L, baseProcessingTs + 1000L)
    originalHarness.close()

    val restoredHarness = harness(bufferingOutputTimeMillis = 10 * 60 * 1000L)
    restoredHarness.setup()
    restoredHarness.initializeState(snapshot)
    restoredHarness.open()

    // Pending timers survive restore but are not due yet.
    restoredHarness.setProcessingTime(toMillis("2025-07-21T10:35:00Z"))
    restoredHarness.extractOutputValues().asScala.toList shouldBe empty

    // The original dirty rows emit once their restored timers become due.
    restoredHarness.setProcessingTime(toMillis("2025-07-21T10:40:00Z"))
    val outputs = restoredHarness.extractOutputValues().asScala.toList.map(decodeOutput)
    outputs should have size 2
    outputs.find(_.keys == List("gen_restore_a")).map(_.values) shouldEqual Some(windowValues(1L, 1L, 1L))
    outputs.find(_.keys == List("gen_restore_b")).map(_.values) shouldEqual Some(windowValues(1L, 1L, 1L))

    // A post-restore event for key A must not mutate key B's restored state.
    restoredHarness.processElement(
      event("gen_restore_a", "2025-07-21T10:41:00Z", "user_a_2", toMillis("2025-07-21T10:40:00Z")),
      0L)
    restoredHarness.setProcessingTime(toMillis("2025-07-21T10:51:00Z"))
    val postRestore = restoredHarness.extractOutputValues().asScala.toList.map(decodeOutput)
    postRestore.filter(_.keys == List("gen_restore_a")).last.values shouldEqual windowValues(2L, 2L, 2L)
    postRestore.filter(_.keys == List("gen_restore_b")).last.values shouldEqual windowValues(1L, 1L, 1L)
    restoredHarness.close()
  }

  it should "Failure/recovery lifecycle: malformed timestamp rows are skipped without corrupting later valid rows" in {
    withDriver(bufferingOutputTimeMillis = 0L) { driver =>
      // Bad timestamp rows should increment error handling and leave keyed state untouched.
      driver.setProcessingTime("2025-07-21T10:30:00Z")
      driver.processMalformedTimestamp("gen_bad", "bad_ts", "user_bad")
      driver.drainNewOutputs() shouldBe empty

      // A later valid row for the same key should behave like the first accepted event.
      driver.processEvent("gen_bad", "2025-07-21T10:31:00Z", "user_good")
      driver.drainNewOutputs().last.values shouldEqual windowValues(1L, 1L, 1L)
    }
  }

  it should "Failure/recovery lifecycle: emit and eviction timers at the same PT instant coalesce into one row" in {
    withDriver(bufferingOutputTimeMillis = 5 * 60 * 1000L) { driver =>
      // The event arms both an eviction timer and a buffered emit timer for 10:35.
      driver.setProcessingTime("2025-07-21T10:30:00Z")
      driver.processEvent("gen_collision", "2025-07-21T10:30:00Z", "user_1")
      driver.drainNewOutputs() shouldBe empty

      // A shared callback timestamp should rebuild and emit once, not duplicate the dirty row.
      driver.setProcessingTime("2025-07-21T10:35:00Z")
      assertSingleOutput(
        driver.drainNewOutputs(),
        expectedKey = "gen_collision",
        expectedDayStart = dayStart("2025-07-21T00:00:00Z"),
        expectedValues = windowValues(1L, 1L, 1L))
    }
  }

  it should "buffered emissions add stable per-key jitter" in {
    val key = "gen_jitter"
    val baseProcessingTs = toMillis("2025-07-21T10:30:00Z")
    val bufferMillis = 1000L
    val maxJitterMillis = 1000L
    val expectedJitter = Math.floorMod(keyList(key).hashCode().toLong, maxJitterMillis + 1L)
    val expectedEmitTs = baseProcessingTs + bufferMillis + expectedJitter

    withDriver(bufferingOutputTimeMillis = bufferMillis, bufferingOutputJitterMillis = maxJitterMillis) { driver =>
      // The first dirty row schedules an emit at base delay plus stable key jitter.
      driver.setProcessingTimeMillis(baseProcessingTs)
      driver.processEvent(key, "2025-07-21T10:30:00Z", "user_1")
      driver.drainNewOutputs() shouldBe empty

      // Nothing should emit before the jittered timestamp.
      if (expectedEmitTs > baseProcessingTs) {
        driver.setProcessingTimeMillis(expectedEmitTs - 1L)
        driver.drainNewOutputs() shouldBe empty
      }

      // At the jittered timestamp exactly one buffered output is emitted.
      driver.setProcessingTimeMillis(expectedEmitTs)
      val outputs = driver.drainNewOutputs()
      assertSingleOutput(
        outputs,
        expectedKey = key,
        expectedDayStart = dayStart("2025-07-21T00:00:00Z"),
        expectedValues = windowValues(1L, 1L, 1L))
      outputs.head.processingTsMillis shouldEqual expectedEmitTs
    }
  }

  it should "wall-clock cadence emissions use stable per-key phase and emit only when dirty" in {
    val key = "gen_cadence"
    val baseProcessingTs = toMillis("2025-07-21T10:30:00Z")
    val cadenceMillis = 60 * 1000L
    val firstTick = nextCadenceEmitTick(key, baseProcessingTs, cadenceMillis)
    val secondTick = firstTick + cadenceMillis

    withDriver(bufferingOutputTimeMillis = cadenceMillis,
               emissionPolicy = MegaTileEmissionPolicy.WallClockCadence) { driver =>
      driver.processWatermark("2025-07-21T10:25:30Z")
      driver.setProcessingTimeMillis(baseProcessingTs)
      driver.processEvent(key, "2025-07-21T10:30:00Z", "user_1")
      driver.drainNewOutputs() shouldBe empty

      driver.setProcessingTimeMillis(firstTick - 1L)
      driver.drainNewOutputs() shouldBe empty

      driver.setProcessingTimeMillis(firstTick)
      val firstOutputs = driver.drainNewOutputs()
      assertSingleOutput(
        firstOutputs,
        expectedKey = key,
        expectedDayStart = dayStart("2025-07-21T00:00:00Z"),
        expectedValues = windowValues(1L, 1L, 1L))
      firstOutputs.head.processingTsMillis shouldEqual firstTick

      driver.setProcessingTimeMillis(secondTick)
      driver.drainNewOutputs() shouldBe empty

      driver.processWatermark("2025-07-21T10:32:00Z")
      driver.setProcessingTimeMillis(secondTick + 1L)
      driver.processEvent(key, "2025-07-21T10:31:00Z", "user_2")
      driver.drainNewOutputs() shouldBe empty

      driver.setProcessingTimeMillis(secondTick + cadenceMillis)
      val secondOutputs = driver.drainNewOutputs()
      assertSingleOutput(
        secondOutputs,
        expectedKey = key,
        expectedDayStart = dayStart("2025-07-21T00:00:00Z"),
        expectedValues = windowValues(2L, 2L, 2L))
      secondOutputs.head.processingTsMillis shouldEqual secondTick + cadenceMillis
    }
  }

  it should "wall-clock cadence uses dirty-buffered fallback before first watermark" in {
    val cadenceMillis = 60 * 1000L
    val beforeMidnightTs = toMillis("2025-07-21T23:59:30Z")
    val midnight = toMillis("2025-07-22T00:00:00Z")
    val dirtyBufferedTick = beforeMidnightTs + cadenceMillis
    val key = keyWithNextCadenceTickBetween(
      prefix = "gen_cadence_no_watermark",
      processingTs = beforeMidnightTs,
      cadenceMillis = cadenceMillis,
      lowerBound = midnight,
      upperBound = dirtyBufferedTick,
      cadenceGroupBy = largeOnlyGroupBy)
    val cadenceTick = nextCadenceEmitTick(key, beforeMidnightTs, cadenceMillis, largeOnlyGroupBy)

    withDriver(bufferingOutputTimeMillis = cadenceMillis,
               emissionPolicy = MegaTileEmissionPolicy.WallClockCadence,
               testGroupBy = largeOnlyGroupBy) { driver =>
      driver.setProcessingTimeMillis(beforeMidnightTs)
      driver.processEvent(key, "2025-07-21T23:59:30Z", "user_before_watermark")
      driver.drainNewOutputs() shouldBe empty

      cadenceTick should be > midnight
      cadenceTick should be < dirtyBufferedTick
      driver.setProcessingTimeMillis(cadenceTick)
      driver.drainNewOutputs() shouldBe empty

      driver.setProcessingTimeMillis(dirtyBufferedTick)
      val outputs = driver.drainNewOutputs()
      assertSingleOutput(
        outputs,
        expectedKey = key,
        expectedDayStart = dayStart("2025-07-21T00:00:00Z"),
        expectedValues = Seq(1L))
      outputs.head.processingTsMillis shouldEqual dirtyBufferedTick
    }
  }

  it should "wall-clock cadence allows one bounded off-phase emit after catchup becomes live" in {
    val cadenceMillis = 60 * 1000L
    val catchupProcessingTs = toMillis("2025-07-21T10:30:00Z")
    val liveTransitionTs = toMillis("2025-07-21T10:30:30Z")
    val dirtyBufferedTick = catchupProcessingTs + cadenceMillis
    val key = keyWithNextCadenceTickAfter(
      prefix = "gen_cadence_catchup_live",
      processingTs = liveTransitionTs,
      cadenceMillis = cadenceMillis,
      lowerBound = dirtyBufferedTick + 1L)
    val cadenceTick = nextCadenceEmitTick(key, liveTransitionTs, cadenceMillis)

    withDriver(bufferingOutputTimeMillis = cadenceMillis,
               emissionPolicy = MegaTileEmissionPolicy.WallClockCadence) { driver =>
      driver.processWatermark("2025-07-21T10:24:00Z")
      driver.setProcessingTimeMillis(catchupProcessingTs)
      driver.processEvent(key, "2025-07-21T10:30:00Z", "user_1")
      driver.drainNewOutputs() shouldBe empty

      driver.processWatermark("2025-07-21T10:25:30Z")
      driver.setProcessingTimeMillis(liveTransitionTs)
      driver.processEvent(key, "2025-07-21T10:30:30Z", "user_2")
      driver.drainNewOutputs() shouldBe empty

      driver.setProcessingTimeMillis(dirtyBufferedTick)
      val offPhaseOutputs = driver.drainNewOutputs()
      offPhaseOutputs should have size 1
      offPhaseOutputs.head.keys shouldEqual List(key)
      offPhaseOutputs.head.processingTsMillis shouldEqual dirtyBufferedTick

      driver.setProcessingTimeMillis(dirtyBufferedTick + 1L)
      driver.processEvent(key, "2025-07-21T10:31:00Z", "user_3")
      driver.drainNewOutputs() shouldBe empty

      driver.setProcessingTimeMillis(cadenceTick)
      val cadenceOutputs = driver.drainNewOutputs()
      cadenceOutputs should have size 1
      cadenceOutputs.head.keys shouldEqual List(key)
      cadenceOutputs.head.processingTsMillis shouldEqual cadenceTick
    }
  }

  it should "wall-clock cadence emit timer rolls large-window-only keys across UTC day" in {
    val cadenceMillis = 60 * 1000L
    val midnight = toMillis("2025-07-22T00:00:00Z")
    val beforeMidnightTs = toMillis("2025-07-21T23:59:30Z")
    val key = keyWithNextCadenceTickAfter(
      prefix = "gen_cadence_large_day_roll",
      processingTs = beforeMidnightTs,
      cadenceMillis = cadenceMillis,
      lowerBound = midnight,
      cadenceGroupBy = largeOnlyGroupBy)
    val firstTick = nextCadenceEmitTick(key, beforeMidnightTs, cadenceMillis, largeOnlyGroupBy)
    val secondTick = firstTick + cadenceMillis

    withDriver(bufferingOutputTimeMillis = cadenceMillis,
               emissionPolicy = MegaTileEmissionPolicy.WallClockCadence,
               testGroupBy = largeOnlyGroupBy) { driver =>
      driver.processWatermark("2025-07-21T23:59:30Z")
      driver.setProcessingTimeMillis(beforeMidnightTs)
      driver.processEvent(key, "2025-07-21T23:59:30Z", "user_before_midnight")
      driver.drainNewOutputs() shouldBe empty

      firstTick should be > midnight
      driver.setProcessingTimeMillis(firstTick - 1L)
      driver.drainNewOutputs() shouldBe empty

      driver.setProcessingTimeMillis(firstTick)
      val dayRollOutputs = driver.drainNewOutputs()
      assertSingleOutput(
        dayRollOutputs,
        expectedKey = key,
        expectedDayStart = dayStart("2025-07-21T00:00:00Z"),
        expectedValues = Seq(1L))
      dayRollOutputs.head.processingTsMillis shouldEqual firstTick

      driver.setProcessingTimeMillis(firstTick + 1L)
      driver.processEvent(key, "2025-07-22T00:00:30Z", "user_after_midnight")
      driver.drainNewOutputs() shouldBe empty

      driver.setProcessingTimeMillis(secondTick)
      val afterMidnightOutputs = driver.drainNewOutputs()
      assertSingleOutput(
        afterMidnightOutputs,
        expectedKey = key,
        expectedDayStart = dayStart("2025-07-22T00:00:00Z"),
        expectedValues = Seq(1L))
      afterMidnightOutputs.head.processingTsMillis shouldEqual secondTick
    }
  }

  it should "wall-clock cadence preserves pending day-roll output when watermark outruns PT emit" in {
    val cadenceMillis = 5 * 60 * 1000L
    val processingTs = toMillis("2025-07-23T10:00:00Z")
    val key = "gen_cadence_pending_day_roll"

    withDriver(bufferingOutputTimeMillis = cadenceMillis,
               emissionPolicy = MegaTileEmissionPolicy.WallClockCadence,
               testGroupBy = largeOnlyGroupBy) { driver =>
      driver.processWatermark("2025-07-21T23:50:00Z")
      driver.setProcessingTimeMillis(processingTs)
      driver.processEvent(key, "2025-07-21T23:50:00Z", "user_day_1")
      driver.drainNewOutputs() shouldBe empty

      driver.processWatermark("2025-07-22T00:00:01Z")
      driver.processEvent(key, "2025-07-22T00:00:00Z", "user_day_2")
      driver.drainNewOutputs() shouldBe empty

      driver.processWatermark("2025-07-23T00:00:01Z")
      driver.processEvent(key, "2025-07-23T00:00:00Z", "user_day_3")
      assertSingleOutput(
        driver.drainNewOutputs(),
        expectedKey = key,
        expectedDayStart = dayStart("2025-07-21T00:00:00Z"),
        expectedValues = Seq(1L))
    }
  }
}

object MegaTileProcessFunctionTest extends Matchers {
  private val DayMillis = new Window(1, TimeUnit.DAYS).millis

  private val inputSchema: Seq[(String, DataType)] =
    Seq("view_by" -> StringType, Constants.TimeColumn -> LongType)

  private val groupBy: GroupBy = {
    val gb = Builders.GroupBy(
      sources = Seq(
        Builders.Source.events(
          table = "events.test_stream",
          topic = "events.test_stream",
          query = Builders.Query(
            selects = Map("id" -> "id", "view_by" -> "view_by"),
            timeColumn = Constants.TimeColumn,
            startPartition = "20250101"
          )
        )
      ),
      keyColumns = Seq("id"),
      aggregations = Seq(
        Builders.Aggregation(
          operation = Operation.COUNT,
          inputColumn = "view_by",
          windows = Seq(new Window(1, TimeUnit.HOURS), new Window(1, TimeUnit.DAYS), new Window(3, TimeUnit.DAYS))
        )
      ),
      metaData = Builders.MetaData(name = "mega_tile_process_function_test"),
      accuracy = Accuracy.TEMPORAL
    )
    gb.setOnlineStrategy(OnlineStrategy.STREAMING_MEGATILES)
    gb
  }

  private val largeOnlyGroupBy: GroupBy = {
    val gb = Builders.GroupBy(
      sources = Seq(
        Builders.Source.events(
          table = "events.test_stream",
          topic = "events.test_stream",
          query = Builders.Query(
            selects = Map("id" -> "id", "view_by" -> "view_by"),
            timeColumn = Constants.TimeColumn,
            startPartition = "20250101"
          )
        )
      ),
      keyColumns = Seq("id"),
      aggregations = Seq(
        Builders.Aggregation(
          operation = Operation.COUNT,
          inputColumn = "view_by",
          windows = Seq(new Window(3, TimeUnit.DAYS))
        )
      ),
      metaData = Builders.MetaData(name = "mega_tile_process_function_large_only_test"),
      accuracy = Accuracy.TEMPORAL
    )
    gb.setOnlineStrategy(OnlineStrategy.STREAMING_MEGATILES)
    gb
  }

  private val megaTileCodec = new MegaTileCodec(groupBy, inputSchema)

  private def windowValues(oneHour: Any, oneDay: Any, threeDay: Any): Seq[Any] =
    Seq(oneHour, oneDay, threeDay)

  // Shared setup for the live/catchup boundary tests: create an old 10:30-10:35 tile, then
  // advance PT so the 11:35 callback decides whether that tile is still in 1h.
  private def seedOldTileBeforeLiveCatchupBoundary(driver: Driver, key: String): Unit = {
    driver.setProcessingTime("2025-07-21T10:30:00Z")
    driver.processEvent(key, "2025-07-21T10:30:00Z", "user_old_1")
    driver.processEvent(key, "2025-07-21T10:31:00Z", "user_old_2")
    driver.drainNewOutputs().last.values shouldEqual windowValues(2L, 2L, 2L)

    driver.setProcessingTime("2025-07-21T11:30:00Z")
    driver.drainNewOutputs().last.values shouldEqual windowValues(2L, 2L, 2L)
  }

  private def activeKeyResumesAfterUtcMidnightWithStaleWatermarkThenCatchesUp(driver: Driver): Unit = {
    val key = "axis_active_after_midnight"
    driver.processWatermark("2026-04-11T23:54:00Z")
    driver.setProcessingTime("2026-04-12T00:01:00Z")

    // The key is active and watermark is more than one hop plus slack behind PT, so the 00:00:30
    // tile is retained but not merged into no-batch windows as of the 23:55 watermark hop.
    driver.processEvent(key, "2026-04-12T00:00:30Z", "user_after_midnight")
    assertSingleOutput(
      driver.drainNewOutputs(),
      expectedKey = key,
      expectedDayStart = dayStart("2026-04-12T00:00:00Z"),
      expectedValues = windowValues(null, null, 1L))

    // Once watermark crosses midnight and is close enough to PT, the next PT eviction rebuilds the
    // small-window cache as live time and the retained 00:00 tile appears.
    driver.processWatermark("2026-04-12T00:00:30Z")
    driver.setProcessingTime("2026-04-12T00:05:00Z")
    assertSingleOutput(
      driver.drainNewOutputs(),
      expectedKey = key,
      expectedDayStart = dayStart("2026-04-12T00:00:00Z"),
      expectedValues = windowValues(1L, 1L, 1L))
  }

  private def sparseKeyResumesAfterUtcMidnightWithStaleWatermark(driver: Driver): Unit = {
    val key = "axis_sparse_after_midnight"
    driver.processWatermark("2026-04-11T23:49:00Z")
    driver.setProcessingTime("2026-04-11T23:50:00Z")
    driver.processEvent(key, "2026-04-11T23:50:00Z", "user_seed")
    assertSingleOutput(
      driver.drainNewOutputs(),
      expectedKey = key,
      expectedDayStart = dayStart("2026-04-11T00:00:00Z"),
      expectedValues = windowValues(1L, 1L, 1L))

    // At 00:10 the key has been idle for more than one hop, so stale-watermark mode is SparseKeyLag.
    // The overdue PT eviction rolls currentDayStart to Apr12 and rebuilds small windows as of PT.
    driver.processWatermark("2026-04-11T23:54:00Z")
    driver.setProcessingTime("2026-04-12T00:10:00Z")
    assertSingleOutput(
      driver.drainNewOutputs(),
      expectedKey = key,
      expectedDayStart = dayStart("2026-04-12T00:00:00Z"),
      expectedValues = windowValues(1L, 1L, null))
  }

  private def yesterdayBoundaryEventAfterResumeIsAdmitted(driver: Driver): Unit = {
    val key = "axis_yesterday_boundary"
    driver.processWatermark("2026-04-12T00:05:00Z")
    driver.setProcessingTime("2026-04-12T00:10:00Z")
    driver.processEvent(key, "2026-04-12T00:10:00Z", "user_anchor")
    assertSingleOutput(
      driver.drainNewOutputs(),
      expectedKey = key,
      expectedDayStart = dayStart("2026-04-12T00:00:00Z"),
      expectedValues = windowValues(1L, 1L, 1L))

    // Exact yesterday-start is admissible. It updates current 1d/3d state, but not current 1h.
    driver.processEvent(key, "2026-04-11T00:00:00Z", "user_yesterday_start")
    val outputs = driver.drainNewOutputs()
    outputs should have size 2
    outputs.find(_.dayStartMillis == dayStart("2026-04-12T00:00:00Z")).map(_.values) shouldEqual
      Some(windowValues(1L, 2L, 1L))
    outputs.find(_.dayStartMillis == dayStart("2026-04-11T00:00:00Z")).map(_.values) shouldEqual
      Some(windowValues(null, null, 1L))
  }

  private def olderThanYesterdayEventAfterResumeIsDropped(driver: Driver): Unit = {
    val key = "axis_older_than_yesterday"
    driver.processWatermark("2026-04-12T00:05:00Z")
    driver.setProcessingTime("2026-04-12T00:10:00Z")
    driver.processEvent(key, "2026-04-12T00:10:00Z", "user_anchor")
    assertSingleOutput(
      driver.drainNewOutputs(),
      expectedKey = key,
      expectedDayStart = dayStart("2026-04-12T00:00:00Z"),
      expectedValues = windowValues(1L, 1L, 1L))

    // One millisecond older than yesterday is rejected before processor.onEvent can mutate state.
    driver.processEvent(key, "2026-04-10T23:59:59.999Z", "user_too_old")
    driver.drainNewOutputs() shouldBe empty
  }

  private case class DecodedOutput(
      keys: List[Any],
      dayStartMillis: Long,
      processingTsMillis: Long,
      values: Seq[Any])

  final private class Driver(bufferingOutputTimeMillis: Long,
                             bufferingOutputJitterMillis: Long,
                             emissionPolicy: MegaTileEmissionPolicy,
                             testGroupBy: GroupBy) {
    private val outputCodec = new MegaTileCodec(testGroupBy, inputSchema)
    private val testHarness = harness(bufferingOutputTimeMillis,
                                      bufferingOutputJitterMillis,
                                      emissionPolicy,
                                      testGroupBy)
    private var emittedCount = 0
    private var currentProcessingTs = 0L

    testHarness.open()

    def setProcessingTime(iso: String): Unit =
      setProcessingTimeMillis(toMillis(iso))

    def setProcessingTimeMillis(ts: Long): Unit = {
      currentProcessingTs = ts
      testHarness.setProcessingTime(ts)
    }

    def processWatermark(iso: String): Unit =
      testHarness.processWatermark(toMillis(iso))

    def processEvent(id: String, eventIso: String, viewBy: String): Unit =
      testHarness.processElement(event(id, eventIso, viewBy, currentProcessingTs), 0L)

    def processMalformedTimestamp(id: String, malformedTs: String, viewBy: String): Unit =
      testHarness.processElement(
        ProjectedEvent(Map("id" -> id, "view_by" -> viewBy, Constants.TimeColumn -> malformedTs), currentProcessingTs),
        0L)

    def drainNewOutputs(): List[DecodedOutput] = {
      val allOutputs = testHarness.extractOutputValues().asScala.toList.map(decodeOutput(_, outputCodec))
      val newOutputs = allOutputs.drop(emittedCount)
      emittedCount = allOutputs.size
      newOutputs
    }

    def close(): Unit =
      testHarness.close()
  }

  private def withDriver(bufferingOutputTimeMillis: Long,
                         bufferingOutputJitterMillis: Long = 0L,
                         emissionPolicy: MegaTileEmissionPolicy = MegaTileEmissionPolicy.Default,
                         testGroupBy: GroupBy = groupBy)(
      fn: Driver => Unit): Unit = {
    val driver = new Driver(bufferingOutputTimeMillis, bufferingOutputJitterMillis, emissionPolicy, testGroupBy)
    try {
      fn(driver)
    } finally {
      driver.close()
    }
  }

  private def harness(bufferingOutputTimeMillis: Long,
                      bufferingOutputJitterMillis: Long = 0L,
                      emissionPolicy: MegaTileEmissionPolicy = MegaTileEmissionPolicy.Default,
                      testGroupBy: GroupBy = groupBy)
      : KeyedOneInputStreamOperatorTestHarness[java.util.List[Any], ProjectedEvent, TimestampedTile] = {
    new KeyedOneInputStreamOperatorTestHarness[java.util.List[Any], ProjectedEvent, TimestampedTile](
      new KeyedProcessOperator[java.util.List[Any], ProjectedEvent, TimestampedTile](
        new MegaTileProcessFunction(
          testGroupBy,
          inputSchema,
          bufferingOutputTimeMillis = bufferingOutputTimeMillis,
          bufferingOutputJitterMillis = bufferingOutputJitterMillis,
          emissionPolicy = emissionPolicy
        )),
      new KeySelector[ProjectedEvent, java.util.List[Any]] {
        override def getKey(value: ProjectedEvent): java.util.List[Any] =
          keyList(value.fields("id"))
      },
      TypeInformation.of(classOf[java.util.List[_]]).asInstanceOf[TypeInformation[java.util.List[Any]]]
    )
  }

  private def event(id: String, eventIso: String, viewBy: String, processingTs: Long): ProjectedEvent =
    ProjectedEvent(
      Map("id" -> id, "view_by" -> viewBy, Constants.TimeColumn -> toMillis(eventIso)),
      processingTs
    )

  private def keyList(key: Any): java.util.List[Any] = {
    val result = new util.ArrayList[Any](1)
    result.add(key)
    result
  }

  private def assertSingleOutput(
      outputs: List[DecodedOutput],
      expectedKey: String,
      expectedDayStart: Long,
      expectedValues: Seq[Any]): Unit = {
    outputs should have size 1
    outputs.head.keys shouldEqual List(expectedKey)
    outputs.head.dayStartMillis shouldEqual expectedDayStart
    outputs.head.values shouldEqual expectedValues
  }

  private def decodeOutput(tile: TimestampedTile): DecodedOutput =
    decodeOutput(tile, megaTileCodec)

  private def decodeOutput(tile: TimestampedTile, codec: MegaTileCodec): DecodedOutput =
    DecodedOutput(
      keys = tile.keys.asScala.toList,
      dayStartMillis = tile.latestTsMillis,
      processingTsMillis = tile.startProcessingTime,
      values = codec.rowAggregator
        .finalize(codec.decode(tile.tileBytes))
        .toSeq
    )

  private def dayStart(iso: String): Long =
    TsUtils.round(toMillis(iso), DayMillis)

  private def toMillis(iso: String): Long =
    Instant.parse(iso).toEpochMilli

  private def nextCadenceEmitTick(key: String,
                                  processingTs: Long,
                                  cadenceMillis: Long,
                                  cadenceGroupBy: GroupBy = groupBy): Long = {
    val phase = Math.floorMod(
      (cadenceGroupBy.getMetaData.getName :: List(key)).hashCode().toLong,
      cadenceMillis)
    val elapsedSincePhase = Math.floorMod(processingTs - phase, cadenceMillis)
    val delay = if (elapsedSincePhase == 0L) cadenceMillis else cadenceMillis - elapsedSincePhase
    processingTs + delay
  }

  private def keyWithNextCadenceTickAfter(prefix: String,
                                          processingTs: Long,
                                          cadenceMillis: Long,
                                          lowerBound: Long,
                                          cadenceGroupBy: GroupBy = groupBy): String =
    Iterator
      .from(0)
      .map(index => s"${prefix}_$index")
      .find(key => nextCadenceEmitTick(key, processingTs, cadenceMillis, cadenceGroupBy) > lowerBound)
      .getOrElse(throw new IllegalStateException(s"No cadence key found for prefix=$prefix"))

  private def keyWithNextCadenceTickBetween(prefix: String,
                                            processingTs: Long,
                                            cadenceMillis: Long,
                                            lowerBound: Long,
                                            upperBound: Long,
                                            cadenceGroupBy: GroupBy = groupBy): String =
    Iterator
      .from(0)
      .map(index => s"${prefix}_$index")
      .find { key =>
        val tick = nextCadenceEmitTick(key, processingTs, cadenceMillis, cadenceGroupBy)
        tick > lowerBound && tick < upperBound
      }
      .getOrElse(throw new IllegalStateException(
        s"No cadence key found for prefix=$prefix between $lowerBound and $upperBound"))
}
