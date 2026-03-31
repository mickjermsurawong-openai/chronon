package ai.chronon.integrations.cloud_azure

import org.apache.spark.sql.{DataFrame, SparkSession}

import java.net.URI

/** Shared Snowflake connection utilities used by both [[Snowflake]] (Format reader) and
  * [[SnowflakeImport]] (staging query). Consolidates JDBC URL parsing, PEM extraction,
  * Spark connector options building, and private key lookup.
  */
object SnowflakeConnector {

  def parseJdbcParams(jdbcUrl: String): Map[String, String] = {
    val queryString = new URI(jdbcUrl.replace("jdbc:snowflake://", "http://")).getQuery
    if (queryString != null && queryString.nonEmpty) {
      queryString.split("&").map(_.split("=", 2)).collect { case Array(k, v) => k -> v }.toMap
    } else {
      Map.empty[String, String]
    }
  }

  def extractPemBase64(pemContent: String): String =
    pemContent
      .replaceFirst("-----BEGIN[^-]*-----", "")
      .replaceFirst("-----END[^-]*-----", "")
      .replaceAll("\\s", "")

  def buildSparkConnectorOptions(jdbcUrl: String, pemContent: String): Map[String, String] = {
    val params = parseJdbcParams(jdbcUrl)
    val pemBase64 = extractPemBase64(pemContent)
    def require(key: String, label: String): String =
      params.getOrElse(key, throw new IllegalStateException(s"$label missing in SNOWFLAKE_JDBC_URL"))

    Map(
      "sfURL" -> jdbcUrl.split("\\?").head.replace("jdbc:snowflake://", ""),
      "pem_private_key" -> pemBase64,
      "sfUser" -> require("user", "User"),
      "sfDatabase" -> require("db", "Database"),
      "sfSchema" -> require("schema", "Schema"),
      "sfWarehouse" -> require("warehouse", "Warehouse"),
      "tracing" -> "all"
    )
  }

  /** Read via Spark Snowflake connector and lowercase all column names. */
  def read(sparkSession: SparkSession, options: Map[String, String], query: String): DataFrame = {
    val df = sparkSession.read
      .format("net.snowflake.spark.snowflake")
      .options(options)
      .option("query", query)
      .load()
    df.toDF(df.columns.map(_.toLowerCase): _*)
  }

  /** Tiered private key lookup: env var -> Azure Key Vault -> throw. */
  def getPrivateKeyPem(env: Map[String, String]): String = {
    env.get("SNOWFLAKE_PRIVATE_KEY") match {
      case Some(key) => key
      case None =>
        env.get("SNOWFLAKE_VAULT_URI") match {
          case Some(vaultUri) =>
            val (vaultUrl, secretName) = AzureKeyVaultHelper.parseSecretUri(vaultUri)
            AzureKeyVaultHelper.getSecret(vaultUrl, secretName)
          case None =>
            throw new IllegalStateException(
              "Snowflake private key not found. Provide SNOWFLAKE_PRIVATE_KEY or SNOWFLAKE_VAULT_URI.")
        }
    }
  }

  def validateJdbcUrl(jdbcUrl: String): String = {
    if (!jdbcUrl.startsWith("jdbc:snowflake://"))
      throw new IllegalStateException(s"SNOWFLAKE_JDBC_URL must start with 'jdbc:snowflake://'. Got: $jdbcUrl")
    jdbcUrl
  }
}
