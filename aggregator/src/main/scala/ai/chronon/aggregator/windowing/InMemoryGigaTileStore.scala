package ai.chronon.aggregator.windowing

import ai.chronon.aggregator.row.RowAggregator

/** In-memory GigaTileStore for tests. Extends InMemoryTileStore with batch state. */
class InMemoryGigaTileStore(windowedAgg: RowAggregator) extends InMemoryTileStore(windowedAgg) with GigaTileStore {
  private var _batchIr: FinalBatchIr = _
  private var _batchEndTs: Long = -1L
  private var _runningLargeIr: Array[Any] = windowedAgg.init

  override def getBatchIr: FinalBatchIr = _batchIr
  override def putBatchIr(ir: FinalBatchIr): Unit = _batchIr = ir

  override def getBatchEndTs: Long = _batchEndTs
  override def putBatchEndTs(ts: Long): Unit = _batchEndTs = ts

  override def getRunningLargeIr: Array[Any] = _runningLargeIr
  override def putRunningLargeIr(ir: Array[Any]): Unit = _runningLargeIr = ir
}
