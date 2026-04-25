# Mega Tile Design

## Problem

Current tiling writes many small hop-aligned tiles per entity (up to 288/day at 5-min resolution).
The serving side fetches all tiles via prefix scan and merges them per-window. This causes read
amplification and serving latency.

## Solution

Write **one mega tile per entity per day** to the KV store. The mega tile contains a **windowed IR**
where each column has the correct aggregate for its specific window. This reduces KV store reads from
O(N tiles) to one batch point get plus one stream point get per daily key from query day back to
the batch day, with one extra previous-day fallback key for no-batch columns.

## Opt-in

Enabled per GroupBy via the `OnlineStrategy` thrift enum:

```thrift
enum OnlineStrategy { DEFAULT = 0, STREAMING_MEGATILES = 1 }
struct GroupBy { ..., 8: optional OnlineStrategy onlineStrategy }
```

Both Flink and fetcher check `groupByOps.isMegaTilingEnabled`. The field is excluded from
`semanticHash` — changing the online strategy doesn't trigger batch recomputation.

## Window Categories

```
tailBuffer = 2d (default)

Small windows (≤ tailBuffer):
  effectiveStart = now - window.millis
  Flink covers full window via tiles + sawtooth running IR.
  Fetcher uses the newest available daily mega tile entry at or after yesterdayStart
  (self-contained).

Large windows (> tailBuffer) + unwindowed:
  effectiveStart = batchEnd (on fetcher side) or dayStart (on Flink side, per-day)
  Flink accumulates a daily running IR per day (today + yesterday).
  Fetcher merges batch collapsed + tail hops + every available daily streaming aggregate from
  the batch day through query day.
```

## Tier Assignment

Each column's hop size comes from `FiveMinuteResolution.calculateTailHop(window)`:

| window range | hop size |
|-------------|----------|
| < 12h | 5min (300,000 ms) |
| >= 12h, < 12d | 1hr (3,600,000 ms) |
| >= 12d | 1day (86,400,000 ms) |

Tiles are only maintained for **small-window tiers** — tiers that have at least one column
with `isNoBatch=true`. Large window columns don't use tiles; they use daily accumulators directly.

## Flink State Layout (MegaTileStreamProcessor + TileStore)

All state access goes through a `TileStore` abstraction. Flink backs it with `MapState`/`ValueState`
+ codec (serde on access, with decode memoization). Tests back it with `InMemoryTileStore`.

```
Small window state:
  tiles: MapState<"hopSize:tileStart", bytes>    // base (unwindowed) IRs, small-window tiers only
  cachedSmallWindowIr: ValueState<bytes>          // sawtooth running sum (windowed IR)

Large window state:
  largeTodayIr: ValueState<bytes>                 // daily accumulator for [todayStart, now)
  largeYesterdayIr: ValueState<bytes>             // daily accumulator for [yesterdayStart, todayStart)

Bookkeeping:
  currentDayStart: ValueState<Long>
  earliestTileStart: ValueState<Long>
```

## Flink ProcessFunction Mental Model

There are two clocks in this operator:

- **event time/watermark**: where Flink believes the stream has progressed in event time.
- **processing time (PT)**: wall clock in the Flink task.

There are two primary operating modes:

**Live**: watermark is close enough to PT. The small-window cache and eviction use PT as the as-of
time, matching vanilla tiled serving where reads enumerate tiles for wall-clock query time.

**ActiveCatchup**: watermark is far behind PT for an active key, often because replay is lagged or
allowed out-of-orderness is intentionally high. Event time is then decoupled from wall clock, so
event-path cache updates and timer-path eviction both use `nextSmallWindowHop(watermark)` instead
of PT. This avoids aging out replay tiles that are still current relative to the event-time
frontier.

Important details that are easy to mix up:

- `processor.onEvent(row, eventTs, smallWindowAsOfTs)` receives `eventTs` separately so the
  processor can choose the retained tile/day that the input row mutates. Mode does not choose that
  tile; it chooses the as-of timestamp for cache and eviction behavior.
- `smallWindowAsOfTs` is the event-path timestamp for deciding whether the touched tile contributes
  to the cached small-window IR. The small-window cache is the Flink implementation of Chronon's
  no-batch windows.
- `evictionTime` is the timer-path timestamp for making state accurate as of that point: it can
  roll day state, permanently drop expired retained tiles, and rebuild cached small-window IR.
- `NoWatermark` is startup/bootstrap behavior: the event path uses the row's event hop, while timer
  callbacks use PT because there is no watermark clock to trust yet.
- `SparseKeyLag` is the idle-key fallback: the global watermark may lag because of other input, but
  this key is not actively replaying, so timer callbacks use PT and values decay or roll forward
  with wall clock.
- Output buffering is write coalescing only. It uses PT timers to avoid emitting on every event or
  eviction; it does not change event-time mutation or as-of selection.
- MegaTile buffered emission defaults to `buffering_output_policy=dirty_buffer_with_jitter`, where
  the first dirty row schedules `processingTs + buffering_output_time_millis` plus optional
  `buffering_output_jitter_millis`. `wall_clock_cadence` instead uses a stable per-key wall-clock
  phase in `Live`/`SparseKeyLag`, while `NoWatermark` and `ActiveCatchup` keep the dirty-buffered
  fallback so startup and replay are bounded by the configured buffer.

The code-order step-by-step walkthrough lives in `MegaTileProcessFunction.scala`.

### On Event

1. **Update tiles** (small-window tiers only):
   - Compute `tileStartsForEvent(eventTs)` → one tile per active tier
   - Guard: only create tiles within `[retentionFloor, currentDayStart + 2*DayMillis)`
     (rejects both too-old and too-future timestamps)
   - Read tile from store, update with `baseAggregator.update(ir, row)`, write back
   - ~2 tile codec ops per event (one per small-window tier)

2. **Update cachedSmallWindowIr** (sawtooth):
   - `MegaTileProcessFunction` passes `smallWindowAsOfTs` separately from `eventTs`
   - For each no-batch column, merge the event only when its accepted tile falls inside
     `[effectiveStart(col, smallWindowAsOfTs, currentDayStart), smallWindowAsOfTs)`
   - The base tile still uses `eventTs`, so retained late events can update wider current windows
     or future rebuilds without re-inflating a stale small window that is outside the current as-of
     horizon
   - Up to 1 windowed IR codec op when a cached column is actually updated

3. **Update large window daily IR** — route by event day:
   - `eventTs >= nextDayStart` → clamp to today (future event; day transitions are not event-driven)
   - `eventTs >= todayStart` → update `largeTodayIr`
   - `eventTs >= yesterdayStart` → update `largeYesterdayIr` (retained late event)
   - `eventTs < yesterdayStart` → no large-window update; Flink drops this before processor mutation
     once `currentDayStart` is initialized
   - 1 windowed IR codec op

4. **Emit** to KV store (only dirty targets):
   - Today's entry: pack `cachedSmallWindowIr` + `largeTodayIr` → `(entity, todayStart)`
   - Yesterday's entry (only if late event touched it): pack `null` + `largeYesterdayIr` → `(entity, yesterdayStart)`

**Total hot-path cost per event: up to ~6 codec ops** (vs ~102 with bulk restore/persist).

### Day Transitions (advanceWatermark)

Day transitions are never triggered directly by event timestamps. This prevents future-timestamped
events from prematurely rotating state.

- Event ingestion calls `advanceWatermark` with the current Flink watermark before applying the row.
- Eviction timers call `advanceWatermark` with the selected eviction time: watermark-hop time in
  `ActiveCatchup`, and PT in `NoWatermark`, `SparseKeyLag`, and `Live`.
- Before an adjacent one-day roll, `MegaTileProcessFunction` emits or buffers any dirty today row
  under the previous day key so buffered small-window state is not lost. With
  `wall_clock_cadence`, a pending pre-rollover row is preserved until the next cadence emit timer.

```
transitionDay = round(dayTransitionTs, DayMillis)
if transitionDay > currentDayStart:
  // Single-day hop: carry today's aggregate to yesterday
  // Multi-day hop (e.g., after long idle): clear yesterday (stale beyond 2d tolerance)
  largeYesterdayIr = if (transitionDay == currentDayStart + DayMillis) largeTodayIr else init
  largeTodayIr = init
  currentDayStart = transitionDay
```

### Eviction (onEviction, triggered by PT timer)

`MegaTileProcessFunction` keeps one PT eviction timer per key at the next min-hop boundary. The
timer passes a mode-specific eviction time into `processor.onEviction`: watermark-hop time during
active catchup, otherwise PT.

1. Compute `retentionFloor` per small-window tier using the eviction time
2. Remove tiles below floor
3. **Rebuild `cachedSmallWindowIr`** from remaining tiles via
   `buildMegaTileIr(tiles, evictionTime, todayStart)`
   — corrects the sawtooth tail by scoping each column to its `effectiveStart`
4. Emit updated today entry

Eviction is the only O(tiles) operation and runs at timer cadence (every 5min or 1hr), not per event.

## Mega Tile Construction (buildMegaTileIr)

Used by eviction rebuild and by the MegaTileMergerTest simulation:

```
megaTileIr = windowedAggregator.init

for col in 0 until windowedAggregator.length:
  hopSize = calculateTailHop(windowMappings(col).window)
  effStart = effectiveStart(col, now, batchEnd)
  bucketIdx = baseIrIndices(col)

  for (tileStart, tileIr) in tiles[hopSize]:
    if tileStart >= effStart and tileStart < now:
      megaTileIr(col) = merge(megaTileIr(col), tileIr(bucketIdx))
```

Multiple windowed columns (e.g., `sum_6h`, `sum_2d`) that share the same bucket (`sum`)
read from different tiers and different time ranges but the same bucket index in the tile IR.

## Fetcher Merge (MegaTileMerger)

Fetcher reads one stream entry per daily key from query day back to the batch day, plus one extra
previous-day fallback key for no-batch columns, and one batch entry `(entity)` from batch KV.

```
def merge(batchIr, dailyTileIrs, queryTs, batchEnd):
  resultIr = clone(batchIr.collapsed) or init
  batchDayStart = round(batchEnd, 1d)
  noBatchFallbackDayStart = round(queryTs, 1d) - 1d

  for col in 0 until windowedAggregator.length:
    window = windowMappings(col).window

    if window != null and window.millis <= tailBufferMillis:   // SMALL WINDOW
      // Self-contained in daily entry. Use the newest non-null column value from
      // query day or fallback day; clear stale batch values when all daily rows are absent.
      resultIr(col) = newest dailyTileIr(col) where dayStart >= noBatchFallbackDayStart

    else:                                                       // LARGE WINDOW / UNWINDOWED
      // Batch collapsed + historical streaming daily aggregates, merged oldest -> newest.
      for (dayStart, dailyIr) in dailyTileIrs if dayStart >= batchDayStart:
        if dailyIr != null and dailyIr(col) != null:
          resultIr(col) = merge(resultIr(col), dailyIr(col))

  // Tail hops for large windowed columns only (not small, not unwindowed)
  mergeTailHopsForBatchColumns(resultIr, queryTs, batchEnd, batchIr)

  return windowedAggregator.finalize(resultIr)
```

## Daily KV Key Scheme

KV key: `TileKey(streamingDataset, entityKeyBytes, DayMillis, dayStart)` — reuses existing TileKey
with `tileSizeMs = DayMillis`. Each day is a separate point-get key.

- Flink emits `todayStart` (not raw `eventTs`) as the tile timestamp, so the codec always writes
  to the correct daily key even for future-timestamped events.
- Fetcher constructs explicit `GetRequest`s for
  `MegaTileMerger.streamingDayKeys(queryTs, batchEnd)`. Query time is resolved once and
  propagated to avoid midnight-boundary inconsistency.

## Codec (MegaTileCodec)

Windowed IR (mega tile entries): `encode(ir)` / `decode(bytes)` using the windowed aggregator schema
(one IR slot per (agg, window) pair).

Base IR (individual tiles in Flink state): `encodeBaseIr(ir)` / `decodeBaseIr(bytes)` using the
unwindowed base aggregator schema (one IR slot per aggregation bucket).

`AvroCodec.of` internally uses `ThreadLocal`, so `MegaTileCodec` resolves codec instances through
`def` accessors instead of sharing a single lazy instance across concurrent fetcher requests.

## Constraints

- **Batch staleness**: Fetcher covers every daily stream key from the batch day through query day,
  so large-window coverage remains continuous even when batch lags by multiple days.
- **Sawtooth approximation**: Accepted for all aggregation types. Between evictions, cached
  small-window columns can be over-inclusive by up to one tile interval at the tail. Retained late
  events outside the current `smallWindowAsOfTs` horizon do not update stale cached columns.
- **Late events**: Flink admits events with `eventTs >= currentDayStart - 1d`. A retained late
  event always uses `eventTs` for base tile and large-window day routing, but it updates cached
  small-window columns only when the touched tile is inside that column's current as-of horizon.
  Events older than yesterday relative to `currentDayStart` are dropped before mutating state.
- **Sparse-key lag**: If a key is idle while another input keeps the global watermark stale,
  `SparseKeyLag` uses PT for eviction. That can roll `currentDayStart` forward and later cause old
  backlog events to be dropped as older-than-yesterday for that key.
- **Future events**: Clamped to today for large windows. Day transitions are driven by watermark or
  timer eviction mode, not directly by event timestamps, so future timestamps cannot corrupt state.

## Scenario Tables

All windows, tailBuffer = 2d, batchEnd = Mar 25 00:00.

### Scenario 1: Batch fresh (now = Mar 25 14:00, batchEnd = Mar 25 00:00)

| window | tier | category | effectiveStart | today entry range | batch tail hops | batch collapsed | fetcher uses |
|--------|------|----------|---------------|-------------------|-----------------|-----------------|-------------|
| 6h | 5min | SMALL | Mar 25 08:00 | [08:00, 14:00) = 6h | — | — | today only |
| 1d | 1hr | SMALL | Mar 24 14:00 | [Mar 24 14:00, Mar 25 14:00) = 24h | — | — | today only |
| 47h | 1hr | SMALL | Mar 23 15:00 | [Mar 23 15:00, Mar 25 14:00) = 47h | — | — | today only |
| 2d | 1hr | SMALL | Mar 23 14:00 | [Mar 23 14:00, Mar 25 14:00) = 48h | — | — | today only |
| 49h | 1hr | LARGE | Mar 25 00:00 | [Mar 25 00:00, Mar 25 14:00) = 14h | [Mar 22 23:00, Mar 24 23:00) = 2d | [Mar 24 23:00, Mar 25 00:00) = 1h | today + collapsed + tail |
| 3d | 1hr | LARGE | Mar 25 00:00 | [Mar 25 00:00, Mar 25 14:00) = 14h | [Mar 22 00:00, Mar 24 00:00) = 2d | [Mar 24 00:00, Mar 25 00:00) = 1d | today + collapsed + tail |
| 7d | 1hr | LARGE | Mar 25 00:00 | [Mar 25 00:00, Mar 25 14:00) = 14h | [Mar 18 00:00, Mar 20 00:00) = 2d | [Mar 20 00:00, Mar 25 00:00) = 5d | today + collapsed + tail |

### Scenario 2: Batch delayed (now = Mar 25 02:00, batchEnd = Mar 24 00:00)

| window | tier | category | effectiveStart | today entry range | yesterday entry range | fetcher uses |
|--------|------|----------|---------------|-------------------|-----------------------|-------------|
| 6h | 5min | SMALL | Mar 24 20:00 | [20:00, 02:00) = 6h | (ignored) | today only |
| 1d | 1hr | SMALL | Mar 24 02:00 | [Mar 24 02:00, Mar 25 02:00) = 24h | (ignored) | today only |
| 49h | 1hr | LARGE | — | [Mar 25 00:00, Mar 25 02:00) = 2h | [Mar 24 00:00, Mar 25 00:00) = 24h | yesterday + today + collapsed + tail |
| 3d | 1hr | LARGE | — | [Mar 25 00:00, Mar 25 02:00) = 2h | [Mar 24 00:00, Mar 25 00:00) = 24h | yesterday + today + collapsed + tail |

For large windows with stale batch (`batchEnd < todayStart`): fetcher sums all daily stream entries
from `batchDayStart` through `todayStart` to cover `[batchEnd, now)`, then merges with batch
collapsed + tail hops.

### Flink state size (max tiles per tier)

| tier | driven by | max tiles |
|------|-----------|-----------|
| 5min | 6h window (if present) | 72 |
| 1hr | 2d window (max small) | 48 |

The 2d SMALL column always drives hourly tier state since `now - 2d` extends further back
than `todayStart`.

## Architecture

```
Flink write path:
  source → spark eval → watermarks → keyBy
    → MegaTileProcessFunction (delegates to MegaTileStreamProcessor via TileStore)
    → MegaTileAvroCodecFn (TileKey with DayMillis + dayStart)
    → AsyncKVStoreWriter

Fetcher read path:
  GroupByFetcher: daily stream point gets from query day back to batch day (+ fallback day)
    → GroupByResponseHandler.mergeMegaTilesFromStreaming
    → MegaTileCodec.decode (daily stream entries)
    → MegaTileMerger.merge (batch + daily stream entries → finalized result)

Shared pipeline tails (BaseFlinkJob):
  buildTiledTail: keyBy → window aggregate → TiledAvroCodecFn → KV write
  buildMegaTiledTail: keyBy → MegaTileProcessFunction → MegaTileAvroCodecFn → KV write
  Both FlinkGroupByStreamingJob and ChainedGroupByJob use these.
```

## Test Coverage

Tests at five layers, with aggregator/codec tests comparing against NaiveAggregator:

1. **MegaTileAggregatorTest** — tile building + serveMegaTile merge (7 tests)
2. **MegaTileMergerTest** — per-day entry split + MegaTileMerger.merge (7 tests)
3. **MegaTileStreamProcessorTest** — full processor simulation with sawtooth, as-of bounded late
   events, and eviction (9 tests)
4. **MegaTileCodecRoundTripTest** — serde round-trips via SerdeTileStore + key switch (5 tests)
5. **MegaTileProcessFunctionTest** — Flink keyed process-function lifecycle coverage for Live,
   ActiveCatchup, SparseKeyLag, buffered emit jitter, wall-clock cadence, timer restore, malformed
   rows, and UTC day boundaries

Windows tested: 6h, 1d, 47h, 2d, 49h, 3d, 7d.
Aggregation types: SUM, COUNT, AVERAGE, MIN, MAX, LAST, FIRST.
