package ai.chronon.integrations.cloud_azure

import ai.chronon.api
import ai.chronon.api.Extensions._
import ai.chronon.api.PartitionRange
import ai.chronon.api.ScalaJavaConversions.{IterableOps, MapOps}
import ai.chronon.spark.batch.StagingQuery
import ai.chronon.spark.catalog.{Format, TableUtils}

import java.sql.{Connection, DriverManager, Statement}
import java.util.{Properties, UUID}
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

/** Snowflake staging query implementation with key pair authentication.
  *
  * Required environment variables:
  * - SNOWFLAKE_JDBC_URL: JDBC URL (e.g., jdbc:snowflake://account.snowflakecomputing.com/?user=x&db=y&schema=z&warehouse=w)
  * - SNOWFLAKE_STORAGE_INTEGRATION: Name of the Snowflake storage integration for Azure (e.g., AZURE_ICEBERG_INT)
  *
  * Private key authentication (required, uses tiered lookup):
  * 1. SNOWFLAKE_PRIVATE_KEY: System environment variable with PEM-encoded private key content (PKCS#8 format)
  *    - Can be set via spark-submit: --conf spark.driverEnv.SNOWFLAKE_PRIVATE_KEY="$(cat key.pem)"
  * 2. SNOWFLAKE_VAULT_URI: Azure Key Vault secret URI in staging query configuration (metaData.executionInfo.env.common)
  *    - Format: https://<vault-name>.vault.azure.net/secrets/<secret-name>
  *    - If SNOWFLAKE_PRIVATE_KEY is not set, retrieves the private key from Azure Key Vault
  * 3. If neither is set, an exception is thrown with instructions
  *
  * The spark.sql.catalog.<catalog>.warehouse config should be set to an azure:// URL that is within
  * the storage integration's allowed locations (e.g., azure://account.blob.core.windows.net/container/path)
  */
class SnowflakeImport(stagingQueryConf: api.StagingQuery, endPartition: String, tableUtils: TableUtils)
    extends StagingQuery(stagingQueryConf: api.StagingQuery, endPartition: String, tableUtils: TableUtils) {

  private[cloud_azure] val formatStr = "parquet"

  // Environment variables from metaData.executionInfo.env
  private[cloud_azure] lazy val envVars: Map[String, String] = {
    Option(stagingQueryConf.metaData)
      .flatMap(m => Option(m.executionInfo))
      .flatMap(e => Option(e.env))
      .flatMap(env => Option(env.common))
      .map(_.toScala.toMap)
      .getOrElse(Map.empty)
  }

  // Snowflake JDBC connection configuration from metaData.executionInfo.env
  // URL should include all connection params except credentials, e.g.:
  // jdbc:snowflake://account.snowflakecomputing.com/?user=x&db=y&schema=z&warehouse=w
  private[cloud_azure] lazy val snowflakeJdbcUrl: String = {
    val jdbcUrl = SnowflakeConnector.validateJdbcUrl(
      envVars.getOrElse(
        "SNOWFLAKE_JDBC_URL",
        throw new IllegalStateException(
          "SNOWFLAKE_JDBC_URL not set in metaData.executionInfo.env " +
            "(e.g., jdbc:snowflake://account.snowflakecomputing.com/?user=x&db=y&schema=z&warehouse=w)")
      )
    )
    // Append MULTI_STATEMENT_COUNT=0 to enable multi-statement execution for BEGIN...END blocks
    if (jdbcUrl.contains("?")) s"$jdbcUrl&MULTI_STATEMENT_COUNT=0"
    else s"$jdbcUrl?MULTI_STATEMENT_COUNT=0"
  }

  // System env takes precedence (spark.driverEnv/executorEnv), then staging query config env
  private[cloud_azure] def getPrivateKeyPem(): String = {
    val sysEnv = System.getenv().asScala.toMap
    SnowflakeConnector.getPrivateKeyPem(envVars ++ sysEnv)
  }

  // Connection properties with authentication configured
  // See: https://docs.snowflake.com/en/developer-guide/jdbc/jdbc-configure#using-key-pair-authentication-and-key-rotation
  private[cloud_azure] lazy val snowflakeConnectionProperties: Properties = {
    val props = new Properties()
    val pemContent = getPrivateKeyPem()
    val privateKey = AzureKeyVaultHelper.parsePemPrivateKey(pemContent)
    props.put("privateKey", privateKey)
    props
  }

  private[cloud_azure] def executeSnowflakeQuery(query: String): Unit = {
    // Ensure JDBC driver is loaded
    Class.forName("net.snowflake.client.jdbc.SnowflakeDriver")

    var connection: Connection = null
    var statement: Statement = null
    try {
      logger.info(s"Connecting to Snowflake at: ${snowflakeJdbcUrl}")
      connection = DriverManager.getConnection(snowflakeJdbcUrl, snowflakeConnectionProperties)
      statement = connection.createStatement()

      logger.info(s"Executing Snowflake query...")
      statement.execute(query)
      logger.info(s"Snowflake query executed successfully")
    } finally {
      if (statement != null) {
        try { statement.close() }
        catch { case _: Exception => }
      }
      if (connection != null) {
        try { connection.close() }
        catch { case _: Exception => }
      }
    }
  }

  // Storage integration name for Snowflake COPY INTO
  private[cloud_azure] lazy val storageIntegration: String = {
    envVars.getOrElse(
      "SNOWFLAKE_STORAGE_INTEGRATION",
      throw new IllegalStateException("SNOWFLAKE_STORAGE_INTEGRATION not set in metaData.executionInfo.env")
    )
  }

  // Warehouse location in azure:// format
  private[cloud_azure] lazy val warehouseLocation: String = {
    val catalogName = Format.getCatalog(outputTable)(tableUtils.sparkSession)
    tableUtils.sparkSession.sessionState.conf
      .getConfString(s"spark.sql.catalog.${catalogName}.warehouse")
      .stripSuffix("/")
  }

  // Convert azure:// URL to abfss:// format for Spark
  // azure://account.blob.core.windows.net/container/path -> abfss://container@account.dfs.core.windows.net/path
  private[cloud_azure] lazy val sparkStoragePrefix: String = {
    val azurePattern = """azure://([^.]+)\.blob\.core\.windows\.net/([^/]+)/(.*)""".r
    warehouseLocation match {
      case azurePattern(account, container, path) =>
        s"abfss://$container@$account.dfs.core.windows.net/$path"
      case _ =>
        throw new IllegalStateException(
          s"Invalid warehouse location format: $warehouseLocation. " +
            "Expected: azure://account.blob.core.windows.net/container/path")
    }
  }

  private[cloud_azure] lazy val tempExportSubPath: String = {
    s"export/${outputTable.sanitize}_${UUID.randomUUID().toString}"
  }

  // URI for Snowflake COPY INTO (uses azure:// path)
  private[cloud_azure] def snowflakeExportUri(startPartition: String, endPartition: String): String =
    s"${warehouseLocation}/${tempExportSubPath}/${startPartition}_to_${endPartition}/"

  // URI for Spark to read parquet files (uses abfss:// path)
  private[cloud_azure] def sparkReadUri(startPartition: String, endPartition: String): String =
    s"${sparkStoragePrefix}/${tempExportSubPath}/${startPartition}_to_${endPartition}/"

  private[cloud_azure] def exportDataTemplate(uri: String, sql: String, setups: Seq[String]): String = {
    // Requirements for the sql string:
    // `ds` cannot be part of the projection, it is reserved for chronon.
    // It can be part of the WHERE clause.
    val setupStatements = setups.map(setup => s"${setup};").mkString("\n")

    // Snowflake uses COPY INTO for exporting data to external storage
    // The query result is first stored in a temp table, then exported
    val tempTableName = s"CHRONON_TEMP_${UUID.randomUUID().toString.replace("-", "_")}"

    val multiStatementQuery = if (setups.nonEmpty) {
      s"""BEGIN
         |${setupStatements}
         |
         |CREATE TEMPORARY TABLE ${tempTableName} AS (
         |   ${sql}
         |);
         |
         |COPY INTO '${uri}'
         |FROM ${tempTableName}
         |STORAGE_INTEGRATION = ${storageIntegration}
         |FILE_FORMAT = (TYPE = '${formatStr}')
         |OVERWRITE = TRUE
         |MAX_FILE_SIZE = 268435456;
         |
         |DROP TABLE ${tempTableName};
         |END;""".stripMargin
    } else {
      s"""BEGIN
         |CREATE TEMPORARY TABLE ${tempTableName} AS (
         |   ${sql}
         |);
         |
         |COPY INTO '${uri}'
         |FROM ${tempTableName}
         |STORAGE_INTEGRATION = ${storageIntegration}
         |FILE_FORMAT = (TYPE = '${formatStr}')
         |OVERWRITE = TRUE
         |MAX_FILE_SIZE = 268435456;
         |
         |DROP TABLE ${tempTableName};
         |END;""".stripMargin
    }
    multiStatementQuery
  }

  // Avoiding this temporarily due to permissions and configuration gaps (catalog vs catalog location)
  def compute_with_export(range: PartitionRange, setups: Seq[String], enableAutoExpand: Option[Boolean]): Unit = {
    // Step 1: Export data for the full range to a temp location
    val renderedQuery =
      StagingQuery.substitute(
        tableUtils,
        stagingQueryConf.query,
        range.start,
        range.end,
        endPartition
      )
    val snowflakeUri = snowflakeExportUri(range.start, range.end)
    val sparkUri = sparkReadUri(range.start, range.end)
    val renderedSetups = setups.map(s =>
      StagingQuery.substitute(
        tableUtils,
        s,
        range.start,
        range.end,
        endPartition
      ))
    val exportTemplate =
      exportDataTemplate(snowflakeUri, renderedQuery, renderedSetups)
    logger.info(s"Rendered Staging Query to run is:\n$exportTemplate")

    // Execute the Snowflake export query using JDBC
    val exportJobTry = Try {
      executeSnowflakeQuery(exportTemplate)
      Success(())
    }.flatten

    exportJobTry match {
      case Success(_) =>
        logger.info(
          s"Successfully exported data for range: ${range.start} to ${range.end} to temp location: ${snowflakeUri}")
      case Failure(exception) =>
        throw exception
    }

    // Step 2: Read the parquet data from temp location and write to final Iceberg table via TableUtils
    try {
      logger.info(s"Reading data from temp location: ${sparkUri}")
      val df = tableUtils.sparkSession.read.parquet(sparkUri)

      // Get partition columns from the staging query metadata
      val partitionCols: Seq[String] =
        Seq(range.partitionSpec.column) ++
          (Option(stagingQueryConf.metaData.additionalOutputPartitionColumns)
            .map(_.toScala)
            .getOrElse(Seq.empty))

      val tableProps = Option(stagingQueryConf.metaData.tableProperties)
        .map(_.toScala.toMap)
        .getOrElse(Map.empty[String, String])

      logger.info(s"Writing data to Iceberg table: $outputTable with partitions: ${partitionCols.mkString(", ")}")
      tableUtils.insertPartitions(
        df = df,
        tableName = outputTable,
        tableProperties = tableProps,
        partitionColumns = partitionCols.toList,
        autoExpand = enableAutoExpand.getOrElse(false)
      )

      logger.info(s"Successfully wrote data to Iceberg table $outputTable for range: $range")
    } catch {
      case ex: Throwable =>
        logger.error(s"Error writing to Iceberg table $outputTable", ex)
        throw ex
    } finally {
      // Step 3: Clean up temp directory
      cleanupTempDirectory()
    }

    logger.info(s"Finished writing Staging Query data to $outputTable")
  }

  // Spark-readable path for cleanup (abfss:// format)
  private[cloud_azure] lazy val sparkTempExportPrefix: String = {
    s"${sparkStoragePrefix}/${tempExportSubPath}"
  }

  private[cloud_azure] def cleanupTempDirectory(): Unit = {
    try {
      logger.info(s"Cleaning up temp directory: ${sparkTempExportPrefix}")
      val hadoopConf = tableUtils.sparkSession.sparkContext.hadoopConfiguration
      val fs = new org.apache.hadoop.fs.Path(sparkTempExportPrefix).getFileSystem(hadoopConf)
      val path = new org.apache.hadoop.fs.Path(sparkTempExportPrefix)
      if (fs.exists(path)) {
        fs.delete(path, true)
        logger.info(s"Successfully deleted temp directory: ${sparkTempExportPrefix}")
      } else {
        logger.info(s"Temp directory does not exist: ${sparkTempExportPrefix}")
      }
    } catch {
      case ex: Throwable =>
        logger.warn(s"Failed to cleanup temp directory ${sparkTempExportPrefix}: ${ex.getMessage}")
      // Don't throw, just log the warning as cleanup failure shouldn't fail the entire job
    }
  }

  override def compute(range: PartitionRange, setups: Seq[String], enableAutoExpand: Option[Boolean]): Unit = {
    val renderedQuery =
      StagingQuery.substitute(
        tableUtils,
        stagingQueryConf.query,
        range.start,
        range.end,
        endPartition
      )
    logger.info(s"Running query:\n$renderedQuery")
    val sfOptions = SnowflakeConnector.buildSparkConnectorOptions(snowflakeJdbcUrl, getPrivateKeyPem())
    val df = SnowflakeConnector.read(tableUtils.sparkSession, sfOptions, renderedQuery)

    val tableProps = Option(stagingQueryConf.metaData.tableProperties)
      .map(_.toScala.toMap)
      .getOrElse(Map.empty[String, String])

    val partitionCols: Seq[String] =
      Seq(range.partitionSpec.column) ++
        (Option(stagingQueryConf.metaData.additionalOutputPartitionColumns)
          .map(_.toScala)
          .getOrElse(Seq.empty))

    // Ensure the outputTable name is fully qualified for the Iceberg Catalog
    // e.g., "open_catalog.my_schema.my_iceberg_table"
    tableUtils.insertPartitions(
      df = df,
      tableName = outputTable, // Must use the catalog-prefixed name
      tableProperties = tableProps ++ Map("format-version" -> "2"),
      partitionColumns = partitionCols.toList,
      autoExpand = enableAutoExpand.getOrElse(false)
    )
  }
}
