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
  if batchIr != null:
    runningLargeIr = clone(batchIr.collapsed)
    mergeTailHops(runningLargeIr, queryTs=timerTs, batchEndTs=batchEndTs, batchIr)
  else:
    // No batch yet (new entity, batch hasn't run). Streaming-only.
    runningLargeIr = windowedAgg.init

  for col where !isNoBatch(col):
    runningLargeIr(col) = merge(runningLargeIr(col), largeTodayIr(col))
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

  // Register eviction timer unconditionally — needed for:
  // - batch-only entities (no streaming events to trigger it)
  // - large-windows-only GroupBys (no small window tiles to drive eviction)
  // minEvictionInterval = min(activeTiers) — the smallest hop size across ALL windows.
  // Matches hop granularity so tail hop corrections fire at the right cadence.
  registerEvictionTimer(now + minEvictionInterval)

  if newBatchEnd > currentDayStart:
    if currentDayStart < 0:
      // Uninitialized (no events yet). Safe to set from batch — largeTodayIr is init,
      // no overlap concern. Without this, all startup batch loads would defer.
      currentDayStart = newBatchEnd
      // fall through to recomputation
    else:
      // Real defer: watermark hasn't caught up. largeTodayIr has events that overlap
      // with batch. Wait for advanceWatermark to rotate, then eviction recomputes.
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

## Bootstrapping and Startup

### Emit strategy

No suppress logic inside Flink. Flink always emits when it has something to emit.
The serving layer doesn't route traffic until the orchestrator signals "ready."

**Emit triggers:**
- **Event arrival**: emit on every event (existing mega tile behavior).
- **Eviction timer**: emit on periodic eviction (existing).
- **Batch IR update**: emit if `runningLargeIr` changed. Single rule covers
  null→value (first batch load), batch correction, tail shift, no-change skip.

```
def onBatchUpdate(newBatchIr, newBatchEnd):
  // ... store batchIr, register timer, defer if overlap (see detailed pseudocode above) ...

  oldRunningLargeIr = clone(runningLargeIr)
  // ... recompute runningLargeIr from batch + hops + streaming ...

  if !equal(oldRunningLargeIr, runningLargeIr):
    emit(finalize(pack(cachedSmallWindowIr, runningLargeIr)))
```

### Readiness signal

The Flink job exposes a readiness condition: **initial Iceberg scan complete AND watermark
caught up** (within configurable `maxLag` of wall clock). The orchestrator checks this
before routing model traffic to the giga tile KV dataset.

```
isReady = icebergInitialScanComplete
          AND currentWatermark >= System.currentTimeMillis() - maxLag
```

This is a metric/health endpoint — not part of the emit logic. Flink doesn't gate emits
on readiness. It writes to KV freely. The orchestrator decides when to trust the output.

### Kafka replay on cold start

On cold start (no checkpoint), Flink replays Kafka from `batchEndTs` to reconstruct
streaming state. During replay, Flink emits progressively-building vectors. This is fine
because the orchestrator hasn't signaled "ready" yet — no traffic is routed.

```
Cold start / first startup:
1. Flink starts with no state
2. Iceberg source scans upload table (latest ds partition)
   → all entities get batch IR via onBatchUpdate
   → null→value trigger: each entity emits batch-only vector
3. Kafka consumer starts from batchEndTs offset
   → replays events from [batchEnd, now)
   → each event emits updated vector (progressively improving)
4. Watermark catches up + Iceberg scan done → isReady = true
5. Orchestrator routes traffic → fetcher reads correct vectors
```

During replay (steps 2-3), the KV store has intermediate vectors. Nobody reads them
because readiness hasn't been signaled.

### Kafka retention requirement

Kafka topic retention must be ≥ max batch staleness (typically 2 days). This ensures
events from `[batchEnd, now)` are available for replay. For GroupBys with only small
windows (≤ 2d), retention must cover the max window size.

### Key completeness

**Will all keys be in the KV store after startup?**

Yes, for all temporal entities:
- **Inactive entities** (in batch table, no recent streaming events): Iceberg source
  emits their batch IR → null→value trigger → batch-only vector emitted to KV.
- **Active entities**: batch-only vector first (Iceberg), then corrected with streaming
  during Kafka replay.
- **New entities (in Kafka, not in batch)**: first event creates Flink state. `batchIr`
  is null → streaming-only vector emitted. Correct for entities with no history.
  Next batch run picks them up via onBatchUpdate (null→value trigger).

**Note:** Giga tile only applies to `Accuracy.TEMPORAL` GroupBys (with a streaming topic).
`SNAPSHOT` GroupBys bypass Flink — they use the traditional `bulkPut` from Spark to KV.

### Startup timeline

```
Time          KV state                              isReady
──────────    ──────────────────────────────────     ───────
T+0           Empty                                  false
T+1 min       Batch-only vectors appearing           false
              (Iceberg scan in progress)
T+5 min       All batch entities in KV               false
              (Iceberg scan complete)
              Streaming entities: partial
              (Kafka replay in progress)
T+10 min      All entities fully correct             true
              (watermark caught up)
              Orchestrator routes traffic
```

### State transitions

```
┌──────────────────────────────────────────────┐
│   First startup / cold restart                │
│                                              │
│   1. Iceberg scan: batch IRs → KV           │
│   2. Kafka replay: streaming → KV           │
│   3. Both complete → isReady = true          │
│   4. Orchestrator routes traffic             │
└──────────────────────────────────────────────┘

┌──────────────────────────────────────────────┐
│   Checkpoint restore (normal)                 │
│                                              │
│   1. Restore state from checkpoint           │
│   2. Resume Kafka from checkpointed offsets  │
│   3. Iceberg re-scan for batch updates       │
│   4. isReady = true immediately              │
└──────────────────────────────────────────────┘
```

## Entity Scenario Matrix

Each entity falls into one of these categories. The matrix traces through startup, steady state,
and edge cases for each, showing which code paths fire and what gets emitted.

### Entity types

| Type | Description | Has batch IR? | Has Kafka events? |
|------|-------------|---------------|-------------------|
| **A: Active** | In batch table AND streaming topic | Yes | Yes |
| **B: Inactive** | In batch table, no recent events | Yes | No |
| **C: New** | Not in batch yet, has streaming events | No | Yes |

### Startup (first deploy or cold restart)

| Entity | Iceberg scan | Kafka replay | First emit | Fully correct at |
|--------|-------------|-------------|------------|------------------|
| **A** | batch IR loaded → `currentDayStart` init'd from `batchEnd` → `runningLargeIr` computed → emit batch-only vector | Events replay from `batchEnd` → `onEvent` merges into `runningLargeIr` → emit on each event | On batch IR load (Iceberg) | Watermark caught up (~minutes) |
| **B** | batch IR loaded → same as A → emit batch-only vector | No events → no Kafka processing | On batch IR load (Iceberg) | Immediately (batch is the full answer) |
| **C** | No row in Iceberg → no `onBatchUpdate` | Events arrive → `batchIr = null` → `onEvent` emits streaming-only vector | On first Kafka event | Next batch run picks it up |

### Steady state (job running, daily batch refresh)

| Entity | On event | On eviction | On batch refresh |
|--------|----------|-------------|------------------|
| **A** | Update tiles + `cachedSmallWindowIr` + `largeTodayIr` + `runningLargeIr` → emit | Rebuild small windows from tiles. Recompute `runningLargeIr` from batch hops + streaming → emit | Store new `batchIr`. Recompute `runningLargeIr`. Emit on mismatch. |
| **B** | No events → nothing | Timer fires (registered by `onBatchUpdate`). Recompute `runningLargeIr` from batch hops → emit if tail shifted. | Store new `batchIr`. Recompute. Emit on mismatch. |
| **C** | Same as A but `batchIr = null` → `runningLargeIr` = streaming only → emit | Rebuild small windows. `batchIr = null` → `runningLargeIr` = streaming only → emit | **Transition to type A:** `batchIr` goes null→value. Recompute includes batch. Mismatch → emit. |

### Day transition (advanceWatermark crosses midnight)

| Entity | What happens |
|--------|-------------|
| **A** | `largeYesterdayIr = largeTodayIr`, `largeTodayIr = init`, `currentDayStart` advances. `runningLargeIr` NOT reset (cumulative). Eviction corrects tail within one interval. |
| **B** | `advanceWatermark` fires from global watermark advancement. Same rotation. No events → `largeTodayIr` stays init. Eviction timer recomputes `runningLargeIr` with shifted tail hops. |
| **C** | Same as A but no batch component. Rotation is streaming-only. |

### Batch refresh (onBatchUpdate) edge cases

| Scenario | Guard | Behavior |
|----------|-------|----------|
| `newBatchEnd <= oldBatchEnd` | Early return | Skip (same or older batch) |
| `currentDayStart < 0` (uninitialized, no events) | Init `currentDayStart = newBatchEnd` | Safe: no events → no overlap. Recompute and emit. |
| `newBatchEnd > currentDayStart` (watermark lag) | Defer (return) | Store `batchIr` but don't recompute. Next eviction handles it after watermark advances. |
| `newBatchEnd >= currentDayStart` (normal) | Clear `largeYesterdayIr` | Batch covers through yesterday. Recompute without yesterday. |
| `newBatchEnd < currentDayStart` (stale batch catch-up) | Keep `largeYesterdayIr` | Batch doesn't cover yesterday. Include yesterday in recomputation. |

### Eviction edge cases

| Scenario | Behavior |
|----------|----------|
| `batchIr = null` (new entity, type C) | `runningLargeIr = init + streaming`. No NPE. |
| `batchIr != null` (types A, B) | `runningLargeIr = clone(collapsed) + mergeTailHops + streaming` |
| `earliestTileStart = MaxValue` (no tiles, large-windows-only) | Skip tile eviction. Still recompute `runningLargeIr` from batch hops. |
| Batch-only entity (type B), no `hasSmallWindows` | Timer registered unconditionally by `onBatchUpdate`. Eviction fires at `minEvictionInterval`. |

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
