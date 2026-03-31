package ai.chronon.spark.groupby

import ai.chronon.aggregator.row.RowAggregator
import ai.chronon.aggregator.test.{Column, NaiveAggregator}
import ai.chronon.aggregator.windowing._
import ai.chronon.api.Extensions._
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.api._
import ai.chronon.online.{GigaTileCodec, InMemoryKvStore}
import ai.chronon.online.serde.{AvroCodec, AvroConversions, ArrayRow}
import ai.chronon.spark.Extensions.DataframeOps
import ai.chronon.spark.GroupByUpload
import ai.chronon.spark.catalog.TableUtils
import ai.chronon.spark.utils.{DataFrameGen, MockApi, OnlineUtils, SparkTestBase}
import com.google.gson.Gson
import org.junit.Assert._
import org.scalatest.matchers.should.Matchers
import org.slf4j.LoggerFactory

/** End-to-end test: Spark GroupByUpload → upload table → read back value_bytes →
  * GigaTileCodec.decodeBatchIr → GigaTileStreamProcessor → finalized vectors.
  *
  * Validates the Spark→Flink handoff: that Avro bytes written by GroupByUpload are
  * correctly decoded by the Flink-side GigaTileCodec, and that the serving info
  * reaches KV for PUSH GroupBys.
  */
class GigaTileUploadIntegrationTest extends SparkTestBase with Matchers {
  @transient lazy val logger = LoggerFactory.getLogger(getClass)
  val gson = new Gson

  private val tableUtils = TableUtils(spark)
  private val createdDatabases = scala.collection.mutable.Set[String]()

  private def testNamespace(suffix: String): String = {
    val uuid = java.util.UUID.randomUUID().toString.replace("-", "").take(8)
    val ns = s"giga_upload_test_${suffix}_${uuid}"
    createdDatabases.add(ns)
    ns
  }

  override def afterAll(): Unit = {
    createdDatabases.foreach { db =>
      try { spark.sql(s"DROP DATABASE IF EXISTS $db CASCADE") }
      catch { case e: Exception => logger.warn(s"Failed to drop database $db", e) }
    }
    super.afterAll()
  }

  val TailBufferMillis: Long = new Window(2, TimeUnit.DAYS).millis
  val Epsilon = 1e-6

  def approxEqual(a: Any, b: Any): Boolean = (a, b) match {
    case (null, null)            => true
    case (null, _) | (_, null)   => false
    case (x: Double, y: Double)  => Math.abs(x - y) <= Epsilon * Math.max(1.0, Math.max(Math.abs(x), Math.abs(y)))
    case (x: Array[_], y: Array[_]) =>
      x.length == y.length && x.zip(y).forall { case (a, b) => approxEqual(a, b) }
    case _ => a == b
  }

  it should "decode GroupByUpload value_bytes through GigaTileCodec and match naive" in {
    val namespace = testNamespace("decode_upload")
    val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
    val yesterday = tableUtils.partitionSpec.before(today)
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $namespace")
    tableUtils.sql(s"USE $namespace")

    // Generate events
    val eventsTable = "events_giga_upload"
    val eventSchema = List(
      Column("user", StringType, 10),
      Column("amount", DoubleType, 500),
      Column("count_val", LongType, 100)
    )
    val eventDf = DataFrameGen.events(spark, eventSchema, count = 5000, partitions = 14)
    eventDf.save(s"$namespace.$eventsTable")

    // GroupBy with mixed small + large windows
    val windows = Seq(
      new Window(6, TimeUnit.HOURS),
      new Window(1, TimeUnit.DAYS),
      new Window(2, TimeUnit.DAYS),
      new Window(3, TimeUnit.DAYS),
      new Window(7, TimeUnit.DAYS)
    )
    val aggregations = Seq(
      Builders.Aggregation(Operation.SUM, "amount", windows),
      Builders.Aggregation(Operation.COUNT, "count_val", windows)
    )
    val groupByConf = Builders.GroupBy(
      sources = Seq(Builders.Source.events(Builders.Query(), table = eventsTable)),
      keyColumns = Seq("user"),
      aggregations = aggregations,
      metaData = Builders.MetaData(namespace = namespace, name = "giga_upload_e2e"),
      accuracy = Accuracy.TEMPORAL
    )
    // Set PUSH strategy
    groupByConf.setOnlineStrategy(OnlineStrategy.PUSH)

    // Use a batchEnd 2 days before today so there are streaming events post-batchEnd.
    // GroupByUpload uses endDs as the partition, batchEnd = endDs + 1 day.
    val twoDaysAgo = tableUtils.partitionSpec.before(yesterday)

    // Step 1: Run GroupByUpload — writes to upload table
    GroupByUpload.run(groupByConf, endDs = twoDaysAgo, tableUtilsOpt = Some(tableUtils))

    // Step 2: Read value_bytes and key_bytes from upload table (simulating Iceberg source)
    val uploadTable = groupByConf.metaData.uploadTable
    logger.info(s"Reading upload table: $uploadTable")
    val fullDf = tableUtils.loadTable(uploadTable)
    logger.info(s"Upload table total rows: ${fullDf.count()}, columns: ${fullDf.columns.mkString(", ")}")
    logger.info(s"key_json distinct values: ${fullDf.select("key_json").distinct().collect().map(_.getString(0)).mkString("|")}")
    fullDf.show(5, truncate = false)
    val uploadDf = fullDf.where(s"key_json IS NULL OR key_json != '${Constants.GroupByServingInfoKey}'")
      .select("key_bytes", "value_bytes")
    val uploadRows = uploadDf.collect()
    logger.info(s"Entity rows (excluding serving info): ${uploadRows.length}")
    assertTrue(s"Upload table should have entity rows, got ${uploadRows.length}", uploadRows.length > 0)
    logger.info(s"Upload table has ${uploadRows.length} entity rows")

    // Step 3: Decode value_bytes through GigaTileCodec (same path as BatchIrRowDecoder)
    val inputSchema: Seq[(String, DataType)] = Seq("amount" -> DoubleType, "count_val" -> LongType)
    val gigaCodec = new GigaTileCodec(groupByConf, inputSchema)

    // Pick a few entities to test
    val testEntities = uploadRows.take(5)
    for (row <- testEntities) {
      val valueBytes = row.getAs[Array[Byte]]("value_bytes")
      assertNotNull("value_bytes should not be null", valueBytes)
      assertTrue("value_bytes should be non-empty", valueBytes.length > 0)

      // Decode — this is the critical path: Spark-written Avro → Flink-side decode
      val batchIr = gigaCodec.decodeBatchIr(valueBytes)
      assertNotNull("decoded FinalBatchIr should not be null", batchIr)
      assertNotNull("collapsed should not be null", batchIr.collapsed)
      assertNotNull("tailHops should not be null", batchIr.tailHops)

      // Round-trip: encode → decode should preserve structure
      val reEncoded = gigaCodec.encodeBatchIr(batchIr)
      val reDecoded = gigaCodec.decodeBatchIr(reEncoded)
      assertEquals("collapsed length", batchIr.collapsed.length, reDecoded.collapsed.length)
      assertEquals("tailHops length", batchIr.tailHops.length, reDecoded.tailHops.length)
    }

    // Step 4: Pick one entity, feed through GigaTileStreamProcessor, verify against naive
    val megaTileAgg = new MegaTileAggregator(aggregations, inputSchema, tailBufferMillis = TailBufferMillis)
    val store = new InMemoryGigaTileStore(megaTileAgg.windowedAggregator)
    val processor = new GigaTileStreamProcessor(megaTileAgg, store)

    // Decode one entity's batch IR
    val entityRow = testEntities(0)
    val entityBatchIr = gigaCodec.decodeBatchIr(entityRow.getAs[Array[Byte]]("value_bytes"))

    // Decode entity key to identify which events belong to this entity
    val keyBytes = entityRow.getAs[Array[Byte]]("key_bytes")
    val keyCodec = AvroCodec.of(
      AvroConversions.fromChrononSchema(
        ai.chronon.api.StructType.from("Key", Array(("user", StringType)))).toString())
    val keyRecord = keyCodec.decode(keyBytes)
    val keyConverter = AvroConversions.genericRecordToChrononRowConverter(
      ai.chronon.api.StructType.from("Key", Array(("user", StringType))))
    val entityKey = keyConverter(keyRecord)(0).toString

    // batchEnd = endDs + 1 day (after(endDs) convention)
    val batchEnd = {
      val fmt = new java.text.SimpleDateFormat("yyyy-MM-dd")
      fmt.setTimeZone(java.util.TimeZone.getTimeZone("UTC"))
      fmt.parse(twoDaysAgo).getTime + 24 * 3600 * 1000L
    }

    // Load batch IR into processor
    processor.onBatchUpdate(entityBatchIr, batchEnd, batchEnd)

    // Read all events for this entity from the source table
    val entityEventsDf = tableUtils.sql(
      s"SELECT ts, amount, count_val FROM $namespace.$eventsTable WHERE user = '$entityKey'")
    val entityEvents = entityEventsDf.collect()
      .map { r =>
        val ts = r.getAs[Long]("ts")
        val amount = r.getAs[Any]("amount") match {
          case d: Double => d
          case l: Long   => l.toDouble
          case null      => null
        }
        val countVal = r.getAs[Any]("count_val") match {
          case l: Long => l
          case i: Int  => i.toLong
          case null    => null
        }
        new ArrayRow(Array(amount, countVal), ts): ai.chronon.api.Row
      }
      .sortBy(_.ts)

    // Flink processes ALL events from Kafka (including pre-batchEnd).
    // Small windows are fully covered by tiles — batchEnd only matters for large windows.
    val streamingEvents = entityEvents
    for (event <- streamingEvents) {
      processor.advanceWatermark(event.ts)
      processor.onEvent(event, event.ts)
    }

    // Query at a point after all events
    val queryTs = if (streamingEvents.nonEmpty) streamingEvents.map(_.ts).max + 1000L
                  else batchEnd + 3600 * 1000L
    // Run eviction at queryTs — always emits because irEqual default is always-mismatch
    processor.advanceWatermark(queryTs)
    val evictResult = processor.onEviction(queryTs)
    assertNotNull("eviction should produce a finalized vector", evictResult.finalizedVector)
    val gigaResult = evictResult.finalizedVector

    // Compute naive expected result
    val unpackedParts = aggregations.flatMap(_.unpack)
    val unpacked = unpackedParts.map(_.window).toArray
    val tailHops = unpacked.map(w => FiveMinuteResolution.calculateTailHop(w))
    val rowAgg = new RowAggregator(inputSchema, unpackedParts)
    val naiveAgg = new NaiveAggregator(rowAgg, unpacked, tailHops)
    val naiveResult = naiveAgg.aggregate(entityEvents, Seq(queryTs)).map(ir => rowAgg.finalize(ir))

    // Compare
    assertNotNull("giga result should not be null", gigaResult)
    logger.info(s"Entity=$entityKey events=${entityEvents.length} streaming=${streamingEvents.length}")
    logger.info(s"Giga:  ${gson.toJson(gigaResult)}")
    logger.info(s"Naive: ${gson.toJson(naiveResult(0))}")
    assertTrue(s"Giga tile result should match naive for entity=$entityKey",
               approxEqual(gigaResult, naiveResult(0)))
  }

  it should "write serving info to KV for PUSH GroupBys via GroupByUpload" in {
    val namespace = testNamespace("serving_info_kv")
    val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
    val yesterday = tableUtils.partitionSpec.before(today)
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $namespace")
    tableUtils.sql(s"USE $namespace")

    val eventsTable = "events_serving_info"
    val eventSchema = List(Column("user", StringType, 5), Column("amount", DoubleType, 100))
    val eventDf = DataFrameGen.events(spark, eventSchema, count = 500, partitions = 7)
    eventDf.save(s"$namespace.$eventsTable")

    val groupByConf = Builders.GroupBy(
      sources = Seq(Builders.Source.events(Builders.Query(), table = eventsTable)),
      keyColumns = Seq("user"),
      aggregations = Seq(Builders.Aggregation(Operation.SUM, "amount", Seq(new Window(1, TimeUnit.DAYS)))),
      metaData = Builders.MetaData(namespace = namespace, name = "giga_serving_info_e2e"),
      accuracy = Accuracy.TEMPORAL
    )
    groupByConf.setOnlineStrategy(OnlineStrategy.PUSH)

    // Build an API with InMemoryKvStore
    val inMemoryKvStore = OnlineUtils.buildInMemoryKVStore(s"giga_serving_info_test")
    val mockApi = new MockApi(() => inMemoryKvStore, namespace)

    // Run upload with API — should write serving info directly to KV for PUSH
    GroupByUpload.run(groupByConf, endDs = yesterday, tableUtilsOpt = Some(tableUtils),
                      apiOpt = Some(mockApi))

    // Verify serving info is in KV
    val dataset = groupByConf.batchDataset
    val servingInfoKey = Constants.GroupByServingInfoKey.getBytes(Constants.UTF8)
    val getRequest = ai.chronon.online.KVStore.GetRequest(servingInfoKey, dataset)
    val response = scala.concurrent.Await.result(inMemoryKvStore.get(getRequest), scala.concurrent.duration.Duration(10, "s"))
    assertTrue("serving info should be in KV", response.values.isSuccess)
    assertTrue("serving info should have bytes", response.values.get.nonEmpty)

    val servingInfoBytes = response.latest.get.bytes
    val servingInfoJson = new String(servingInfoBytes, Constants.UTF8)
    assertTrue("serving info should contain groupBy name",
               servingInfoJson.contains("giga_serving_info_e2e"))
    logger.info(s"Serving info written to KV: ${servingInfoJson.take(200)}...")
  }
}
