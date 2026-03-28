# Giga Tile Design

## Problem

The mega tile architecture (see [megatile.md](megatile.md)) reduces KV reads from O(N tiles) to
3 point gets, but the fetcher still does work on every read: decode batch IR + decode today/yesterday
streaming entries + merge collapsed + scan tail hops + merge streaming + finalize. This compute
happens on every feature request.

## Goal

Push the merge to the write path. Flink produces a **fully finalized feature vector** per entity.
The fetcher does a single point get with zero compute.

```
Current (mega tile, pull-based):
  Fetcher: 3 KV gets → decode → merge batch + streaming → finalize → return

Proposed (giga tile, push-based):
  Flink: on event → merge batch + streaming → finalize → KV write
  Fetcher: 1 KV get → return
```

## Key Insight: Hop-Based Pruning

The `FinalBatchIr` structure stores tail hops as individual hop-aligned partial aggregates:
```
collapsed: aggregate of [alignedCollapsedBoundary, batchEnd)
tailHops[hopIndex]: time-sorted array of [baseIr_0, ..., hopStartTs]
```

`mergeTailHops` selects relevant hops using `queryTail = round(queryTs - windowMillis, hopSize)`.
As `queryTs` advances, `queryTail` advances, and older hops are **excluded** (not subtracted).
This gives us "invertibility for free" — even for non-invertible aggregations like MIN, MAX, FIRST,
LAST. We simply re-run `mergeTailHops` with the current `queryTs` and get the correct answer.

## Architecture

```
Iceberg (batch IR, written daily by GroupByUpload)
    │
    │  Flink reads periodically (connected keyed stream)
    ▼
Flink state per entity:
    batchIr: FinalBatchIr           // latest batch IR (collapsed + tail hops)
    batchEndTs: Long                // when batch was last computed
    runningLargeIr: Array[Any]      // fully merged: batch + streaming for large window cols
    largeTodayIr: Array[Any]        // today's streaming delta (for rotation tracking)
    largeYesterdayIr: Array[Any]    // yesterday's streaming delta
    tiles + cachedSmallWindowIr     // (existing mega tile state for small windows)
    │
    │  On event: update streaming state + running sum → emit finalized vector
    ▼
KV Store: (entity) → finalized feature vector
    │
    │  Single point get
    ▼
Fetcher: read → return
```

## Batch IR Ingestion

Flink reads the batch IR from the **existing GroupByUpload Iceberg table** — the same table
that Spark already writes to. No new tables, no new Spark changes.

```
GroupByUpload (Spark)
  │  Writes partition ds=YYYY-MM-DD to Iceberg upload table
  │  Columns: key_bytes, value_bytes, key_json, value_json, ds
  │  Iceberg commits a new snapshot on write
  │
  ▼
Flink-Iceberg Source (streaming monitor mode)
  │  monitorInterval = 30 min
  │  On startup: full table scan (latest ds partition) → all entities
  │  On new snapshot: incremental read → only new/changed files
  │
  │  Decode key_bytes → entityKey
  │  keyBy(entityKey) → network shuffle to correct task slot
  │
  ▼
CoProcessFunction (same task slot as Kafka events for this entity)
  │  processElement1: Kafka event → onEvent
  │  processElement2: batch IR row → onBatchUpdate
```

```
Stream 1: real events (Kafka)                            → keyBy(entityKey) ──┐
                                                                               ├→ CoProcessFunction
Stream 2: batch IR (Iceberg upload table, monitor mode)  → keyBy(entityKey) ──┘
```

**Why this works without any Spark changes:**
- GroupByUpload already writes `(key_bytes, value_bytes)` to an Iceberg table partitioned by `ds`
- Each write commits an Iceberg snapshot (standard Iceberg behavior)
- Flink's Iceberg source detects new snapshots and reads the new data
- `value_bytes` contains Avro-encoded `FinalBatchIr` — same format the fetcher reads today
- `key_bytes` contains Avro-encoded entity keys — same encoding as the Kafka event keys

**Key superset:** The Iceberg source emits ALL entities from the batch table. Entities that
only exist in batch (no streaming events) get a Flink key slot via the batch stream.
This solves the key superset problem without requiring Kafka events for every entity.

## State Layout

Extends the existing mega tile `TileStore` with batch-side state:

```
// Existing (mega tile, small windows):
tiles: MapState                    // per-tier base IRs for small window columns
cachedSmallWindowIr: ValueState    // sawtooth running sum, corrected on eviction

// Existing (mega tile, large window streaming delta):
largeTodayIr: ValueState           // daily accumulator [todayStart, now)
largeYesterdayIr: ValueState       // daily accumulator [yesterdayStart, todayStart)

// New (giga tile, batch + merged):
batchIr: ValueState                // FinalBatchIr (collapsed + tail hops)
batchEndTs: ValueState             // Long
runningLargeIr: ValueState         // fully merged large window IR (batch + streaming)

// Bookkeeping:
currentDayStart, earliestTileStart // (existing)
```

## Processing Logic

### On Event (hot path)

Same cost as mega tile — O(1) per event for large windows, O(tiers) for small windows.

```
def onEvent(row, eventTs):
  // Small windows: unchanged from mega tile
  updateTiles(row, eventTs)
  updateCachedSmallWindowIr(row)

  // Large windows: update daily accumulator AND running sum
  if eventTs >= todayStart:
    updateLargeWindowColumns(largeTodayIr, row)
    updateLargeWindowColumns(runningLargeIr, row)    // running sum stays current
  else if eventTs >= yesterdayStart:
    updateLargeWindowColumns(largeYesterdayIr, row)
    updateLargeWindowColumns(runningLargeIr, row)    // late event also merged into running sum

  // Emit finalized feature vector
  emit(finalize(pack(cachedSmallWindowIr, runningLargeIr)))
```

The `runningLargeIr` is the fully merged value: batch collapsed + relevant tail hops + all
streaming events. Updated incrementally on each event. Sawtooth at the tail — corrected on eviction.

### On Eviction (periodic, every minTileSize)

Corrects both small window sawtooth and large window tail hop selection.

```
def onEviction(timerTs):
  // Small windows: rebuild from tiles (existing mega tile logic)
  evictStaleTiles(timerTs)
  cachedSmallWindowIr = buildMegaTileIr(tiles, timerTs, todayStart)

  // Large windows: recompute running sum from batch + streaming.
  // queryTs advanced → tail hops may have shifted (oldest hop excluded).
  runningLargeIr = clone(batchIr.collapsed)
  mergeTailHops(runningLargeIr, queryTs=timerTs, batchEndTs=batchEndTs, batchIr)
  for col where !isNoBatch(col):
    runningLargeIr(col) = merge(runningLargeIr(col), largeTodayIr(col))
    // Include yesterday if batchEnd < todayStart (batch hasn't caught up)
    if batchEndTs < todayStart:
      runningLargeIr(col) = merge(runningLargeIr(col), largeYesterdayIr(col))

  emit(finalize(pack(cachedSmallWindowIr, runningLargeIr)))
```

### On Batch IR Update (daily, from Iceberg stream)

Stores the new batch IR immediately. Recomputation of `runningLargeIr` is deferred if the
watermark hasn't caught up to the new `batchEnd` (prevents double-counting from overlap
between batch and `largeTodayIr`). The next eviction picks it up.

```
def onBatchUpdate(newBatchIr, newBatchEnd):
  oldBatchEnd = batchEndTs
  if newBatchEnd <= oldBatchEnd: return   // same or older batch, skip

  // Always store the new batch IR — eviction and future events use it.
  batchIr = newBatchIr
  batchEndTs = newBatchEnd

  // Register eviction timer for batch-only entities (no streaming events to trigger it).
  if hasSmallWindows:
    registerEvictionTimer(now + minSmallWindowTileSize)

  if newBatchEnd > currentDayStart:
    // Batch is ahead of watermark (race: Iceberg delivered before watermark caught up).
    // largeTodayIr covers [currentDayStart, now) which overlaps with batch [.., newBatchEnd).
    // Cannot recompute without double-counting for non-invertible aggregations.
    // Defer — the next eviction will recompute with correct boundaries after
    // advanceWatermark moves currentDayStart past newBatchEnd.
    return

  // Safe: newBatchEnd <= currentDayStart — no overlap between batch and largeTodayIr.

  // Save old running sum for comparison
  oldRunningLargeIr = clone(runningLargeIr)

  // Clear yesterday if batch now covers it
  if newBatchEnd >= currentDayStart:
    largeYesterdayIr = init

  // Recompute running sum: new batch + tail hops + streaming
  runningLargeIr = clone(newBatchIr.collapsed)
  mergeTailHops(runningLargeIr, queryTs=watermark, batchEndTs=newBatchEnd, newBatchIr)
  for col where !isNoBatch(col):
    runningLargeIr(col) = merge(runningLargeIr(col), largeTodayIr(col))
    // Include yesterday if batch doesn't cover it
    if newBatchEnd < currentDayStart and largeYesterdayIr(col) != null:
      runningLargeIr(col) = merge(runningLargeIr(col), largeYesterdayIr(col))

  // Emit only on mismatch — most entities won't change materially
  if !equal(oldRunningLargeIr, runningLargeIr):
    emit(finalize(pack(cachedSmallWindowIr, runningLargeIr)))
```

**Why defer when `newBatchEnd > currentDayStart`?**

`largeTodayIr` covers `[currentDayStart, now)`. If `newBatchEnd > currentDayStart`, batch covers
`[..., newBatchEnd)` which overlaps with `[currentDayStart, newBatchEnd)` in `largeTodayIr`.
We can't subtract the overlap (non-invertible aggregations). We can't clear `largeTodayIr`
(loses post-`batchEnd` events). So we wait for the watermark to advance past `newBatchEnd`,
which rotates `largeTodayIr` via `advanceWatermark`. The next eviction then recomputes cleanly.

In normal operation, this defer never triggers: batch lands at ~6 AM, watermark passed midnight
hours ago, `newBatchEnd = currentDayStart`. The defer is a safety net for the narrow race window
when batch arrives just before the watermark crosses midnight.

### Day Transition (advanceWatermark)

Same as mega tile, plus `runningLargeIr` gets yesterday cleared.

```
def advanceWatermark(watermarkTs):
  wmDay = round(watermarkTs, DayMillis)
  if wmDay > currentDayStart:
    // Rotate yesterday → discard (or keep if single-day hop, clear if multi-day)
    largeYesterdayIr = if (wmDay == currentDayStart + DayMillis) largeTodayIr else init
    largeTodayIr = init
    currentDayStart = wmDay

    // runningLargeIr is NOT reset here — it's a cumulative sum.
    // Yesterday's data is still valid in the running sum.
    // The eviction timer will re-run mergeTailHops with the new queryTs
    // to prune any tail hops that fell off. Between the day transition
    // and the next eviction, the running sum is slightly over-inclusive
    // at the tail (sawtooth).
```

## Emit and KV Key

The emitted value is a **finalized feature vector** — the same format that the fetcher would
return to the ML model. No further processing needed.

```
KV key:   entityKeyBytes (plain entity key, no TileKey wrapper, no day suffix)
KV value: finalized feature vector (Avro encoded output schema)
```

Single entry per entity. Overwritten on every emit. The fetcher reads one key, decodes, returns.

## Scenario Tables

All windows, tailBuffer = 2d, now = Mar 26 14:00.

### Scenario 1: Batch fresh (batchEnd = Mar 26 00:00, 14h stale)

Streaming covers [batchEnd, now) = [Mar 26 00:00, Mar 26 14:00) = 14h.
`largeTodayIr` covers [Mar 26 00:00, Mar 26 14:00). `largeYesterdayIr` is empty (batch is fresh).

| window | category | runningLargeIr composition | tail hops selected | streaming added |
|--------|----------|----------------------------|--------------------|-----------------|
| 6h | SMALL | — (uses cachedSmallWindowIr) | — | — |
| 1d | SMALL | — | — | — |
| 2d | SMALL | — | — | — |
| 49h | LARGE | collapsed [Mar 24 23:00, Mar 26 00:00) + 1 hop [Mar 24 01:00, Mar 24 23:00) | hops where hopStart >= round(Mar 26 14:00 - 49h) = Mar 24 13:00 | + largeTodayIr [Mar 26 00:00, 14:00) |
| 3d | LARGE | collapsed [Mar 24 00:00, Mar 26 00:00) + 24 hops [Mar 23 00:00, Mar 24 00:00) | hops where hopStart >= round(Mar 26 14:00 - 3d) = Mar 23 14:00 | + largeTodayIr |
| 7d | LARGE | collapsed [Mar 20 00:00, Mar 26 00:00) + 48 hops [Mar 19 00:00, Mar 20 00:00) | hops where hopStart >= round(Mar 26 14:00 - 7d) = Mar 19 14:00 | + largeTodayIr |

**On event at Mar 26 14:00:**
- `runningLargeIr(49h)` = batch portion (collapsed + selected hops) + `largeTodayIr` contribution + new event
- Emit: `finalize(pack(cachedSmallWindowIr, runningLargeIr))` → single KV write

**On eviction at Mar 26 14:05:**
- `queryTs` advanced by 5min → `queryTail` for 49h advances → no hop falls off (1hr hops)
- `queryTail` for 3d advances → no hop falls off
- `runningLargeIr` recomputed from batch hops + streaming. Same value (no tail shift). No-op.

### Scenario 2: Batch stale (batchEnd = Mar 25 00:00, 38h stale)

Streaming covers [batchEnd, now) = [Mar 25 00:00, Mar 26 14:00) = 38h.
`largeYesterdayIr` covers [Mar 25 00:00, Mar 26 00:00). `largeTodayIr` covers [Mar 26 00:00, Mar 26 14:00).
Both are included because `batchEnd < todayStart`.

| window | category | runningLargeIr composition | tail hops selected | streaming added |
|--------|----------|----------------------------|--------------------|-----------------|
| 6h | SMALL | — | — | — |
| 1d | SMALL | — | — | — |
| 2d | SMALL | — | — | — |
| 49h | LARGE | collapsed [Mar 22 23:00, Mar 25 00:00) + hops | hops where hopStart >= Mar 24 13:00 | + largeYesterdayIr + largeTodayIr |
| 3d | LARGE | collapsed [Mar 23 00:00, Mar 25 00:00) + hops | hops where hopStart >= Mar 23 14:00 | + largeYesterdayIr + largeTodayIr |
| 7d | LARGE | collapsed [Mar 19 00:00, Mar 25 00:00) + hops | hops where hopStart >= Mar 19 14:00 | + largeYesterdayIr + largeTodayIr |

**On batch refresh (batchEnd advances Mar 25 → Mar 26 00:00):**
1. Store new batchIr (collapsed now covers [Mar 20 00:00, Mar 26 00:00) for 7d window)
2. Clear `largeYesterdayIr` — batch now covers [Mar 25 00:00, Mar 26 00:00)
3. Recompute `runningLargeIr`:
   - 49h: new collapsed + selected hops + largeTodayIr only (yesterday cleared)
   - 3d: new collapsed + selected hops + largeTodayIr only
4. Compare with old `runningLargeIr`
5. Emit only if mismatch (likely: batch incorporated Mar 25's full data, replacing streaming's accumulation)

### State transitions through a day

```
Time        Event                   runningLargeIr state
─────────── ─────────────────────── ──────────────────────────────────────────────
Mar 26 00:00  advanceWatermark       yesterday→today rotation. Running sum unchanged
              (day transition)       (sawtooth: slightly over-inclusive at tail)

Mar 26 00:05  eviction timer         Recompute from batch hops + streaming.
                                     Tail hops re-selected with queryTs=00:05.
                                     Running sum corrected.

Mar 26 00:05  event arrives          Merge into largeTodayIr + runningLargeIr.
  to 06:00    (continuous)           Emit finalized vector on each event.

Mar 26 06:00  batch refresh          New batchIr loaded from Iceberg.
              (Iceberg snapshot)     Clear largeYesterdayIr.
                                     Recompute running sum.
                                     Emit only if value changed.

Mar 26 06:00  events continue        runningLargeIr updated incrementally.
  to 24:00                           Eviction corrects tail every minTileSize.
```

## Comparison with Mega Tile

| | Mega tile (current) | Giga tile (proposed) |
|---|---|---|
| **KV reads per query** | 3 (today + yesterday + batch) | 1 |
| **Fetcher compute** | decode + merge + finalize | decode only |
| **Flink state** | ~17 KB/entity | ~19 KB/entity (+batchIr ~1-2 KB) |
| **KV writes per event** | 1 (streaming entry) | 1 (finalized vector) |
| **Batch data in Flink** | No | Yes (via Iceberg connected stream) |
| **Cold entity serving** | Fetcher reads batch KV | Flink emits on batch load |
| **Batch correction** | Implicit (fetcher merges latest) | Flink re-merges, emits on mismatch |

## Cost Model

**Per event (hot path):**
- Small windows: ~6 codec ops (unchanged from mega tile)
- Large windows: 1 additional `updateLargeWindowColumns(runningLargeIr, row)` — same column aggregator update as largeTodayIr. Negligible.
- Emit: encode finalized vector instead of windowed IR. Similar cost.

**Per eviction (every minTileSize):**
- Small windows: rebuild from tiles (unchanged)
- Large windows: `clone(collapsed) + mergeTailHops + merge(streaming)` — O(tailHops × columns). For 48 hops × 7 columns = 336 merge ops per eviction. At 5min cadence, that's ~1.1 merges/sec. Negligible.

**Per batch refresh (daily):**
- Recompute `runningLargeIr` for all entities. O(entities × tailHops × columns).
- Mismatch check + conditional emit. Most entities won't emit.
- Spread over the Iceberg scan duration (~minutes). Not bursty.

## Bootstrapping and Cold Start Recovery

### The problem

In both mega tile and giga tile, a stateless restart (no checkpoint/savepoint) creates a gap:
Kafka events from `[last_emit, restart_time)` are consumed and gone. Flink has no state for them.

- **Mega tile**: fetcher re-reads batch KV on every query, so large windows recover immediately.
  Small windows (streaming-only) lose data until events refill the window. The first new event
  overwrites the old streaming KV entry, causing a sudden drop.
- **Giga tile**: the fetcher serves whatever's in KV (last-emitted vector). ALL windows are stale
  until Flink catches up. No fallback.

Giga tile makes this worse because the fetcher has no independent data path to compensate.

### Fix: Kafka replay from batchEnd on cold start

On cold start (no checkpoint to restore), Flink must replay Kafka events to reconstruct
streaming state before emitting to KV.

```
Cold start sequence:
1. Flink starts with no state
2. Iceberg source loads batch IRs for all entities
   → batchEndTs known per entity (or globally if same GroupBy)
3. Kafka consumer start offset = timestamp(batchEndTs)
   → replays events from [batchEndTs, now)
4. Flink processes replayed events, building tiles + accumulators
   → does NOT emit to KV during replay (suppress until caught up)
5. Watermark reaches near-realtime (within eviction interval of wall clock)
   → Flink transitions to normal mode: emit on every event
```

**Suppress-until-caught-up** is critical. Without it, the KV store sees progressively-building
vectors during replay — the fetcher would serve fluctuating values (e.g., `sum_7d` climbing
from 0 to 5000 over 30 seconds of replay). With suppression, the KV entry stays at the
last-emitted value from the previous run until Flink is fully caught up.

Detection: Flink is "caught up" when `currentWatermark >= System.currentTimeMillis() - maxLag`
where `maxLag` is configurable (e.g., 1 minute). This is a standard Flink pattern for
distinguishing replay from live processing.

### Kafka retention requirement

Kafka topic retention must be ≥ max batch staleness (typically 2 days). This ensures that
on cold start, events from `[batchEnd, now)` are available for replay. If retention is shorter,
the gap between Kafka's oldest available offset and `batchEnd` creates a data hole.

For GroupBys with only small windows (≤ 2d), Kafka retention must cover the max window size.
These windows are self-contained in streaming — batch doesn't help.

### Checkpoint restore (normal case)

When restoring from a checkpoint/savepoint, Flink resumes from the checkpointed Kafka offsets.
No gap. No replay needed. The Iceberg source re-scans for any batch updates that landed
during downtime.

### First-ever startup (no prior state, no prior KV entries)

This is the greenfield case: the giga tile Flink job starts for the first time. The KV store
has no giga tile entries. The batch upload table has historical batch IRs.

```
First startup sequence:

1. Iceberg source scans the upload table (latest ds partition)
   → emits (key_bytes, value_bytes) for ALL entities
   → includes the GroupByServingInfo metadata row (filtered out by key)
   → N entities × ~1KB = ~1GB for 1M entities. One-time bounded scan.

2. keyBy(entityKey) routes each batch IR to the correct task slot
   → CoProcessFunction.processElement2 fires for each entity
   → onBatchUpdate stores batchIr, computes runningLargeIr
   → entities with no streaming events: emit finalized vector immediately
   → KV store now has entries for ALL batch entities (batch-only answer)

3. Kafka consumer starts from batchEndTs offset
   → replays events from [batchEnd, now)
   → KV writes suppressed during replay

4. Watermark catches up to wall clock
   → suppress lifted
   → entities that had streaming events now emit full (batch + streaming) vectors
   → KV store has correct entries for all entities
```

**Key question: will all keys be in the KV store?**

Yes, because:
- The Iceberg source emits ALL entities from the batch table (step 1)
- Each entity triggers `onBatchUpdate` which emits to KV (step 2)
- Batch-only entities get their vector from step 2 and never need streaming
- Streaming entities get an initial batch-only vector (step 2), then a corrected
  batch+streaming vector after replay (step 4)

**What about entities in Kafka but NOT in batch?**

These are brand-new entities that have streaming events but no batch history.
- Their first Kafka event creates Flink state
- `batchIr` is null → `runningLargeIr` = streaming only
- The emitted vector is streaming-only (correct for new entities with no history)
- When the next batch run picks them up, `onBatchUpdate` adds the batch component

**Timing between Iceberg scan and Kafka replay:**

The Iceberg scan and Kafka replay happen concurrently (two input streams).
For any given entity, events might arrive before the batch IR:
- Event arrives first → `batchIr` is null → streaming-only answer emitted
  (but KV writes are suppressed during replay, so this isn't served)
- Batch IR arrives during replay → `onBatchUpdate` stores it, but defers
  recomputation if watermark hasn't caught up
- After watermark catches up: eviction triggers full recomputation with
  both batch and streaming → correct vector emitted

The suppress-until-caught-up mechanism ensures the fetcher never sees
intermediate/incomplete vectors during the startup window.

**KV store population timeline (first startup):**

```
Time          KV state
──────────    ──────────────────────────────────────────────────
T+0           Empty (no prior entries)
T+1 min       Batch-only entities start appearing (Iceberg scan in progress)
              Streaming entities: suppressed (Kafka replay in progress)
T+5 min       Iceberg scan complete. All batch entities have KV entries.
              These are batch-only answers (no streaming component).
T+10 min      Kafka replay complete. Watermark caught up.
              Streaming entities now emit full batch+streaming vectors.
              All entities have correct KV entries.
```

### State transitions

```
                    ┌──────────────────────────────────────────────┐
                    │        First startup (greenfield)             │
                    │                                              │
                    │  1. Iceberg scan: load all batch IRs         │
                    │  2. Batch-only entities: emit immediately    │
                    │  3. Kafka replay from batchEndTs             │
                    │  4. Suppress streaming KV writes             │
                    │                                              │
                    │  ── watermark catches up to wall clock ──    │
                    │                                              │
                    │  5. Emit batch+streaming vectors             │
                    │  6. All entities now in KV store             │
                    └──────────────────────────────────────────────┘

                    ┌──────────────────────────────────────────────┐
                    │          Cold restart (no checkpoint)         │
                    │                                              │
                    │  Same as first startup, but KV store has     │
                    │  stale entries from previous run.             │
                    │  Batch-only entities: overwrite stale entry  │
                    │  Streaming entities: stale until replay done │
                    └──────────────────────────────────────────────┘

                    ┌──────────────────────────────────────────────┐
                    │       Checkpoint restore (normal case)        │
                    │                                              │
                    │  1. Restore Flink state from checkpoint      │
                    │  2. Resume Kafka from checkpointed offsets   │
                    │  3. Iceberg re-scan for batch updates        │
                    │  4. Immediately emit — no gap                │
                    └──────────────────────────────────────────────┘
```

## Implementation Path

Building on the existing mega tile infrastructure:

1. **Batch tail cumulation algorithm** (aggregator package, issue #1377 Task 1)
   - `cumulateTails(FinalBatchIr, SawtoothAggregator) → Array[(Long, Array[Any])]`
   - Produces cumulated entries indexed by query_ts
   - Used by GroupByUpload to write cumulated batch to Iceberg

2. **Extend TileStore with batch state**
   - Add `getBatchIr/putBatchIr`, `getBatchEndTs/putBatchEndTs`, `getRunningLargeIr/putRunningLargeIr`
   - InMemoryTileStore + FlinkTileStore implementations

3. **Extend MegaTileStreamProcessor → GigaTileStreamProcessor**
   - Add `onBatchUpdate(newBatchIr, newBatchEnd)` method
   - Modify eviction to recompute `runningLargeIr` from batch hops
   - Emit finalized vectors instead of windowed IRs

4. **Add Iceberg connected stream to Flink job**
   - `CoProcessFunction` handling both event stream and batch IR stream
   - Periodic Iceberg snapshot monitoring (every 30 min)

5. **Simplify fetcher path**
   - Single point get on entity key
   - Decode finalized vector → return
   - No merge logic needed

6. **Integration test**
   - Traffic replay: events + batch uploads + queries, time-ordered
   - Compare push-based results with backfilled results via `.diff`

Steps 1-3 are pure aggregator/online changes (testable without Flink/Spark).
Step 4 is Flink wiring. Steps 5-6 are cleanup and validation.
