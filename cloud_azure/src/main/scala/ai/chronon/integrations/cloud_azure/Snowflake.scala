package ai.chronon.integrations.cloud_azure

import ai.chronon.api.PartitionSpec
import ai.chronon.spark.catalog.Format
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import java.time.ZoneOffset
import scala.util.{Failure, Success, Try}

/** Snowflake Format implementation for reading tables and partition metadata via Spark Snowflake connector.
  *
  * Configuration is read from environment variables (set via teams.py executionInfo.env):
  *   - SNOWFLAKE_JDBC_URL: JDBC URL (e.g., jdbc:snowflake://account.snowflakecomputing.com/?user=x&db=y&schema=z&warehouse=w)
  *
  * Private key authentication (required, uses tiered lookup):
  *   1. SNOWFLAKE_PRIVATE_KEY: PEM-encoded private key content (PKCS#8 format) directly in the environment variable
  *   2. SNOWFLAKE_VAULT_URI: Azure Key Vault secret URI (e.g., https://<vault-name>.vault.azure.net/secrets/<secret-name>)
  *      - If SNOWFLAKE_PRIVATE_KEY is not set, retrieves the private key from Azure Key Vault
  *   3. If neither is set, an exception is thrown with instructions
  *
  * The partition column is determined by spark.chronon.partition.column (default: ds)
  */
case object Snowflake extends Format {

  @transient private lazy val snowflakeLogger = LoggerFactory.getLogger(getClass)

  private def toEpochMilli(value: Any): Long = value match {
    case d: java.sql.Date        => d.toLocalDate.atStartOfDay(ZoneOffset.UTC).toInstant.toEpochMilli
    case ld: java.time.LocalDate => ld.atStartOfDay(ZoneOffset.UTC).toInstant.toEpochMilli
    case other                   => throw new IllegalArgumentException(s"Unexpected date type: ${other.getClass}")
  }

  override def table(tableName: String, partitionFilters: String)(implicit sparkSession: SparkSession): DataFrame = {
    throw new UnsupportedOperationException(
      "Direct table reads are not supported for Snowflake format. Use a stagingQuery with EngineType.SNOWFLAKE to export the data first.")
  }

  override def createTable(tableName: String,
                           schema: StructType,
                           partitionColumns: List[String],
                           tableProperties: Map[String, String],
                           semanticHash: Option[String])(implicit sparkSession: SparkSession): Unit = {
    throw new UnsupportedOperationException("Table creation is not supported for Snowflake format.")
  }

  override def partitions(tableName: String, partitionFilters: String)(implicit
      sparkSession: SparkSession): List[Map[String, String]] = {
    val defaultPartitionColumn = sparkSession.conf.get("spark.chronon.partition.column", "ds")
    val partitionFormat = sparkSession.conf.get("spark.chronon.partition.format", "yyyy-MM-dd")

    // Parse table name to extract database and schema if provided
    val (database, schema, table) = parseTableName(tableName)

    // Discover the partition column from the table's clustering key or fall back to default
    val partitionColumn = getPartitionColumn(database, schema, table).getOrElse(defaultPartitionColumn)
    snowflakeLogger.info(s"Using partition column '$partitionColumn' for table $tableName")

    queryDistinctPartitions(tableName, partitionColumn, partitionFilters, partitionFormat)
  }

  /** Parse a potentially qualified table name into (database, schema, table) components.
    * Uses the JDBC URL defaults for database and schema if not specified in the table name.
    */
  private def parseTableName(tableName: String): (String, String, String) = {
    val jdbcParams = SnowflakeConnector.parseJdbcParams(getJdbcUrl)
    val defaultDb = jdbcParams.getOrElse("db", "")
    val defaultSchema = jdbcParams.getOrElse("schema", "")

    val parts = tableName.split("\\.")
    parts.length match {
      case 3 => (parts(0), parts(1), parts(2))
      case 2 => (defaultDb, parts(0), parts(1))
      case 1 => (defaultDb, defaultSchema, parts(0))
      case _ => throw new IllegalArgumentException(s"Invalid table name format: $tableName")
    }
  }

  /** Discovers the partition column for a Snowflake table by checking:
    * 1. The table's clustering key (first column if exists)
    * 2. Falls back to None if no clustering key is found
    */
  private def getPartitionColumn(database: String, schema: String, table: String)(implicit
      sparkSession: SparkSession): Option[String] = {
    val sfOptions = getSnowflakeOptions

    // Query INFORMATION_SCHEMA.TABLES to get the clustering key
    val clusteringKeyQuery =
      s"""
         |SELECT CLUSTERING_KEY
         |FROM ${database.toUpperCase}.INFORMATION_SCHEMA.TABLES
         |WHERE TABLE_SCHEMA = '${schema.toUpperCase}'
         |  AND TABLE_NAME = '${table.toUpperCase}'
         |""".stripMargin

    snowflakeLogger.info(s"Querying clustering key: $clusteringKeyQuery")

    try {
      val df = sparkSession.read
        .format("net.snowflake.spark.snowflake")
        .options(sfOptions)
        .option("query", clusteringKeyQuery)
        .load()

      val clusteringKey = df.collect().headOption.flatMap { row =>
        Option(row.getAs[String](0)).filter(_.nonEmpty)
      }

      clusteringKey.flatMap { key =>
        // Clustering key format is like "LINEAR(col1, col2)" or just "col1, col2"
        // Extract the first column name
        val cleanKey = key
          .replaceAll("(?i)LINEAR\\s*\\(", "")
          .replaceAll("\\)", "")
          .trim
        cleanKey.split(",").headOption.map(_.trim.toUpperCase)
      }
    } catch {
      case e: Exception =>
        snowflakeLogger.warn(s"Failed to query clustering key for $database.$schema.$table: ${e.getMessage}")
        None
    }
  }

  override def supportSubPartitionsFilter: Boolean = false

  private def getJdbcUrl: String =
    SnowflakeConnector.validateJdbcUrl(
      sys.env.getOrElse(
        "SNOWFLAKE_JDBC_URL",
        throw new IllegalStateException(
          "SNOWFLAKE_JDBC_URL not found in environment. " +
            "Expected format: jdbc:snowflake://account.snowflakecomputing.com/?user=x&db=y&schema=z&warehouse=w")
      )
    )

  private def getSnowflakeOptions: Map[String, String] = {
    val pem = SnowflakeConnector.getPrivateKeyPem(sys.env)
    SnowflakeConnector.buildSparkConnectorOptions(getJdbcUrl, pem)
  }

  private def queryDistinctPartitions(tableName: String,
                                      partitionColumn: String,
                                      partitionFilters: String,
                                      partitionFormat: String)(implicit
      sparkSession: SparkSession): List[Map[String, String]] = {
    snowflakeLogger.info(s"Querying partitions for table: $tableName using Spark Snowflake connector")

    val query = buildPartitionQuery(tableName, partitionColumn, partitionFilters, partitionFormat)
    snowflakeLogger.info(s"Executing partition query: $query")

    val sfOptions = getSnowflakeOptions
    val df = SnowflakeConnector.read(sparkSession, sfOptions, query)

    // Convert DataFrame to list of partition maps
    // Snowflake returns column names in uppercase, so we need to handle that
    val columnName = df.columns.find(_.equalsIgnoreCase(partitionColumn)).getOrElse(partitionColumn)

    val partitions = df
      .select(columnName)
      .distinct()
      .collect()
      .flatMap { row =>
        val value = row.get(0)
        if (value != null) Some(Map(columnName -> value.toString))
        else None
      }
      .toList
      .sortBy(_(columnName))

    snowflakeLogger.info(s"Found ${partitions.size} distinct partitions for table $tableName")
    partitions
  }

  private def buildPartitionQuery(tableName: String,
                                  partitionColumn: String,
                                  partitionFilters: String,
                                  partitionFormat: String): String = {
    // Convert Java DateTimeFormatter pattern to Snowflake format pattern
    // Common mappings: yyyy->YYYY, MM->MM, dd->DD
    val snowflakeFormat = partitionFormat
      .replace("yyyy", "YYYY")
      .replace("dd", "DD")
    // Cast to DATE first (handles timestamps, dates, and date strings), then format as string
    val formattedColumn = s"TO_VARCHAR($partitionColumn::DATE, '$snowflakeFormat') AS $partitionColumn"
    val baseQuery = s"SELECT DISTINCT $formattedColumn FROM $tableName"
    if (partitionFilters.nonEmpty) {
      s"$baseQuery WHERE $partitionFilters"
    } else {
      baseQuery
    }
  }

  override def maxTimestampDate(tableName: String, timestampColumn: String, partitionSpec: PartitionSpec)(implicit
      sparkSession: SparkSession): Option[String] = {
    Try {
      val sfOptions = getSnowflakeOptions
      val query = s"SELECT MAX($timestampColumn)::DATE AS max_val FROM $tableName"
      snowflakeLogger.info(s"Executing max timestamp date query: $query")

      val result = SnowflakeConnector
        .read(sparkSession, sfOptions, query)
        .collect()
        .headOption

      result.flatMap { row =>
        if (row.isNullAt(0)) None
        else {
          val utcMillis = toEpochMilli(row.get(0))
          Some(partitionSpec.at(utcMillis))
        }
      }
    } match {
      case Success(result) => result
      case Failure(e) =>
        snowflakeLogger.warn(s"Failed to get max timestamp date for $tableName: ${e.getMessage}")
        None
    }
  }

  override def virtualPartitions(tableName: String, timestampColumn: String, partitionSpec: PartitionSpec)(implicit
      sparkSession: SparkSession): List[String] = {
    Try {
      val sfOptions = getSnowflakeOptions
      val query =
        s"SELECT MIN($timestampColumn)::DATE AS min_val, MAX($timestampColumn)::DATE AS max_val FROM $tableName"
      snowflakeLogger.info(s"Executing virtual partitions query: $query")

      val result = SnowflakeConnector
        .read(sparkSession, sfOptions, query)
        .collect()
        .headOption

      result
        .flatMap { row =>
          if (row.isNullAt(0) || row.isNullAt(1)) None
          else {
            val minMillis = toEpochMilli(row.get(0))
            val maxMillis = toEpochMilli(row.get(1))
            Some(partitionSpec.expandRange(partitionSpec.at(minMillis), partitionSpec.at(maxMillis)))
          }
        }
        .getOrElse(List.empty)
    } match {
      case Success(partitions) => partitions
      case Failure(e) =>
        snowflakeLogger.warn(s"Failed to get virtual partitions for $tableName: ${e.getMessage}")
        List.empty
    }
  }
}
