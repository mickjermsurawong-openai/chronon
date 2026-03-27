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

Flink reads the batch IR table from Iceberg using a connected keyed stream.
Both real events and batch updates flow through the same `keyBy(entityKey)` routing.

```
Stream 1: real events (Kafka)                → keyBy(entityKey) ──┐
                                                                   ├→ CoProcessFunction
Stream 2: batch IR (Iceberg, periodic scan)  → keyBy(entityKey) ──┘
```

The Iceberg source uses streaming monitor mode: periodically checks for new snapshots
(e.g., every 30 minutes) and emits new/changed rows. This means:
- On startup: full table scan → every entity gets its batch IR loaded
- After GroupByUpload: incremental snapshot → only changed entities re-emitted
- Entities that only exist in batch (no streaming events) still get a Flink key slot

This solves the key superset problem: Flink doesn't need to have seen a streaming event
to serve an entity. The batch IR stream bootstraps all entities.

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

Recomputes `runningLargeIr` with the new batch data. Emits only if the merged value changed.

```
def onBatchUpdate(newBatchIr, newBatchEnd):
  oldBatchEnd = batchEndTs
  if newBatchEnd <= oldBatchEnd: return   // same or older batch, skip

  // Save old running sum for comparison
  oldRunningLargeIr = clone(runningLargeIr)

  // Store new batch state
  batchIr = newBatchIr
  batchEndTs = newBatchEnd

  // Rotate streaming state — batch now covers what yesterday covered
  largeYesterdayIr = init
  if newBatchEnd > currentDayStart:
    largeTodayIr = init    // batch overlaps today — clear and rebuild from events

  // Recompute running sum: new batch + remaining streaming
  runningLargeIr = clone(newBatchIr.collapsed)
  mergeTailHops(runningLargeIr, queryTs=now, batchEndTs=newBatchEnd, newBatchIr)
  for col where !isNoBatch(col):
    runningLargeIr(col) = merge(runningLargeIr(col), largeTodayIr(col))

  // Emit only on mismatch — most entities won't change materially
  if !equal(oldRunningLargeIr, runningLargeIr):
    emit(finalize(pack(cachedSmallWindowIr, runningLargeIr)))
```

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

## Bootstrapping

On Flink job startup:
1. Iceberg source emits all entities' batch IRs (bounded scan)
2. Each entity's `batchIr` state is populated
3. `runningLargeIr` is computed from batch IR (no streaming yet)
4. Emitted to KV store — all entities are immediately servable
5. As streaming events arrive, `runningLargeIr` is incrementally updated

No Kafka replay needed. No warm-up period for serving.

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
