package ai.chronon.aggregator.windowing

/** Extended TileStore for GigaTile: adds batch IR and running large-window IR state.
  *
  * GigaTile holds the FinalBatchIr in Flink state (loaded from Iceberg) and maintains
  * a runningLargeIr that's the fully merged value: batch collapsed + selected tail hops
  * + streaming daily accumulators.
  */
trait GigaTileStore extends TileStore {
  def getBatchIr: FinalBatchIr
  def putBatchIr(ir: FinalBatchIr): Unit

  def getBatchEndTs: Long
  def putBatchEndTs(ts: Long): Unit

  def getRunningLargeIr: Array[Any]
  def putRunningLargeIr(ir: Array[Any]): Unit
}
