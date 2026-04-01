package ai.chronon.spark.other

import ai.chronon.api.DoubleType
import ai.chronon.api.IntType
import ai.chronon.api.LongType
import ai.chronon.api.StringType
import ai.chronon.api.StructField
import ai.chronon.api.StructType
import ai.chronon.spark.catalog.IncompatibleSchemaException
import ai.chronon.spark.catalog.TableUtils
import ai.chronon.spark.catalog.{DefaultFormatProvider, FormatProvider}
import ai.chronon.spark.submission.SparkSessionBuilder
import ai.chronon.spark.utils.{DataFrameGen, TestUtils}
import ai.chronon.spark.utils.TestUtils.makeDf
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.scalatest.flatspec.AnyFlatSpec

import scala.jdk.CollectionConverters._
import scala.util.Try

class TableUtilsFormatTest extends AnyFlatSpec {

  import TableUtilsFormatTest._

  private val FormatTestEnvVar = "format_test"
  val format: String = sys.env.getOrElse(FormatTestEnvVar, "hive")
  private val formatConfigs = format match {
    case "deltalake" =>
      Map(
        "spark.sql.extensions" -> "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog" -> "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "spark.chronon.table_write.format" -> "delta"
      )
    case _ => Map.empty[String, String]
  }
  val spark: SparkSession =
    SparkSessionBuilder.build("TableUtilsFormatTest", local = true, additionalConfig = Some(formatConfigs))
  val tableUtils: TableUtils = TableUtils(spark)

  it should "testing dynamic classloading" in {
    assertTrue(FormatProvider.from(spark).isInstanceOf[DefaultFormatProvider])
  }

  it should "test insertion of partitioned data and adding of columns" in {
    val dbName = s"db_${System.currentTimeMillis()}"
    val tableName = s"$dbName.test_table_1_$format"
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $dbName")
    val columns1 = Array(
      StructField("long_field", LongType),
      StructField("int_field", IntType),
      StructField("string_field", StringType)
    )
    val df1 = makeDf(
      spark,
      StructType(
        tableName,
        columns1 :+ StructField("ds", StringType)
      ),
      List(
        Row(1L, 2, "3", "2022-10-01")
      )
    )

    val df2 = makeDf(
      spark,
      StructType(
        tableName,
        columns1
          :+ StructField("double_field", DoubleType)
          :+ StructField("ds", StringType)
      ),
      List(
        Row(4L, 5, "6", 7.0, "2022-10-02")
      )
    )
    testInsertPartitions(spark, tableUtils, tableName, format, df1, df2, ds1 = "2022-10-01", ds2 = "2022-10-02")
  }

  it should "test insertion of partitioned data and removal of columns" in {
    val dbName = s"db_${System.currentTimeMillis()}"
    val tableName = s"$dbName.test_table_2_$format"
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $dbName")
    val columns1 = Array(
      StructField("long_field", LongType),
      StructField("int_field", IntType),
      StructField("string_field", StringType)
    )
    val df1 = makeDf(
      spark,
      StructType(
        tableName,
        columns1
          :+ StructField("double_field", DoubleType)
          :+ StructField("ds", StringType)
      ),
      List(
        Row(1L, 2, "3", 4.0, "2022-10-01")
      )
    )

    val df2 = makeDf(
      spark,
      StructType(
        tableName,
        columns1 :+ StructField("ds", StringType)
      ),
      List(
        Row(5L, 6, "7", "2022-10-02")
      )
    )
    testInsertPartitions(spark, tableUtils, tableName, format, df1, df2, ds1 = "2022-10-01", ds2 = "2022-10-02")
  }

  it should "test insertion of partitioned data and modification of columns" in {
    val dbName = s"db_${System.currentTimeMillis()}"
    val tableName = s"$dbName.test_table_3_$format"
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $dbName")
    val columns1 = Array(
      StructField("long_field", LongType),
      StructField("int_field", IntType)
    )
    val df1 = makeDf(
      spark,
      StructType(
        tableName,
        columns1
          :+ StructField("string_field", StringType)
          :+ StructField("ds", StringType)
      ),
      List(
        Row(1L, 2, "3", "2022-10-01")
      )
    )

    val df2 = makeDf(
      spark,
      StructType(
        tableName,
        columns1
          :+ StructField("string_field", DoubleType) // modified column data type
          :+ StructField("ds", StringType)
      ),
      List(
        Row(1L, 2, 3.0, "2022-10-02")
      )
    )

    testInsertPartitions(spark, tableUtils, tableName, format, df1, df2, ds1 = "2022-10-01", ds2 = "2022-10-02")
  }

  it should "return empty read format if table doesn't exist" in {
    val dbName = s"db_${System.currentTimeMillis()}"
    val tableName = s"$dbName.test_table_nonexistent_$format"
    assertTrue(FormatProvider.from(spark).readFormat(tableName).isEmpty)
    assertFalse(tableUtils.tableReachable(tableName))
  }

  it should "log a clean one-liner for non-existent table without stack trace" in {
    val loggerName = classOf[TableUtils].getName
    val underlying = org.slf4j.LoggerFactory.getLogger(loggerName).asInstanceOf[ch.qos.logback.classic.Logger]
    val appender = new ch.qos.logback.core.read.ListAppender[ch.qos.logback.classic.spi.ILoggingEvent]()
    appender.start()
    underlying.addAppender(appender)
    try {
      val tableName = s"db_nonexistent_${System.currentTimeMillis()}.no_such_table"
      assertFalse(tableUtils.tableReachable(tableName))

      val messages = appender.list.asScala.map(_.getFormattedMessage)
      val relevant = messages.filter(_.contains(tableName))
      assertTrue(s"Expected exactly one log message for $tableName, got: $relevant", relevant.size == 1)
      assertTrue(s"Expected clean message, got: ${relevant.head}",
        relevant.head == s"Cannot find table or view $tableName.")
    } finally {
      underlying.detachAppender(appender)
      appender.stop()
    }
  }
}

object TableUtilsFormatTest {
  private def testInsertPartitions(spark: SparkSession,
                                   tableUtils: TableUtils,
                                   tableName: String,
                                   format: String,
                                   df1: DataFrame,
                                   df2: DataFrame,
                                   ds1: String,
                                   ds2: String): Unit = {
    tableUtils.insertPartitions(df1, tableName, autoExpand = true)
    val addedColumns = df2.schema.fieldNames.filterNot(df1.schema.fieldNames.contains)
    val removedColumns = df1.schema.fieldNames.filterNot(df2.schema.fieldNames.contains)
    val inconsistentColumns = (
      for (
        (name1, dtype1) <- df1.schema.fields.map(structField => (structField.name, structField.dataType));
        (name2, dtype2) <- df2.schema.fields.map(structField => (structField.name, structField.dataType))
      ) yield {
        name1 == name2 && dtype1 != dtype2
      }
    ).filter(identity)

    if (inconsistentColumns.nonEmpty) {
      val insertTry = Try(tableUtils.insertPartitions(df2, tableName, autoExpand = true))
      val e = insertTry.failed.get.asInstanceOf[IncompatibleSchemaException]
      assertEquals(inconsistentColumns.length, e.inconsistencies.length)
      return
    }

    if (df2.schema != df1.schema) {
      val insertTry = Try(tableUtils.insertPartitions(df2, tableName))
      assertTrue(insertTry.failed.get.isInstanceOf[AnalysisException])
    }

    tableUtils.insertPartitions(df2, tableName, autoExpand = true)

    // check that we wrote out a table in the right format
    val readTableFormat = FormatProvider.from(spark).readFormat(tableName).get.toString
    assertTrue(s"Mismatch in table format: $readTableFormat; expected: $format", readTableFormat.toLowerCase == format)

    // check we have all the partitions written
    val returnedPartitions = tableUtils.partitions(tableName)
    assertTrue(returnedPartitions.toSet == Set(ds1, ds2))

    val dataRead1 = tableUtils.loadTable(tableName).where(col("ds") === ds1)
    val dataRead2 = tableUtils.loadTable(tableName).where(col("ds") === ds2)
    assertTrue(dataRead1.columns.length == dataRead2.columns.length)

    val totalColumnsCount = (df1.schema.fieldNames.toSet ++ df2.schema.fieldNames.toSet).size
    assertEquals(totalColumnsCount, dataRead1.columns.length)
    assertEquals(totalColumnsCount, dataRead2.columns.length)

    addedColumns.foreach(col => {
      dataRead1.foreach(row => assertTrue(Option(row.getAs[Any](col)).isEmpty))
    })
    removedColumns.foreach(col => {
      dataRead2.foreach(row => assertTrue(Option(row.getAs[Any](col)).isEmpty))
    })
  }
}
