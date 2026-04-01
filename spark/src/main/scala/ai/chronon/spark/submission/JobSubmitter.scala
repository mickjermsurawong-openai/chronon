package ai.chronon.spark.submission

import ai.chronon.api
import ai.chronon.api.ScalaJavaConversions.MapOps
import ai.chronon.spark.submission.JobSubmitterConstants.ConfTypeArgKeyword
import ai.chronon.spark.submission.JobSubmitterConstants.LocalConfPathArgKeyword
import ai.chronon.spark.submission.JobSubmitterConstants.OriginalModeArgKeyword
import ai.chronon.api.{JobStatusType, MetaData, ThriftJsonCodec}
import ai.chronon.api.thrift.TBase
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.ExecutionContext
import scala.reflect.ClassTag

sealed trait JobType
case object SparkJob extends JobType
case object FlinkJob extends JobType

trait JobSubmitter {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  def submit(jobType: JobType,
             submissionProperties: Map[String, String],
             jobProperties: Map[String, String],
             files: List[String],
             labels: Map[String, String],
             args: String*): String

  def status(jobId: String): JobStatusType

  def kill(jobId: String): Unit

  // --- URL methods ---

  def getJobUrl(jobId: String): Option[String] = None

  def getSparkUrl(jobId: String): Option[String] = None

  def getFlinkUrl(jobId: String): Option[String] = None

  def getFlinkInternalJobId(jobId: String): Option[String] = None

  def getLatestCheckpointPath(flinkInternalJobId: String, flinkStateUri: String): Option[String] = None

  // --- Lifecycle methods ---

  def close(): Unit = {}

  // Returns true if a cluster must be created before submitting the job. False if the submitter can submit directly to
  // an existing cluster (e.g. EKS).
  def isClusterCreateNeeded(isLongRunning: Boolean): Boolean = true

  def ensureClusterReady(clusterName: String, clusterConf: Option[Map[String, String]])(implicit
      ec: ExecutionContext): Option[String] = Some(clusterName)

  // --- Platform hooks (override per cloud) ---

  /** Deprecated env var names for cluster name. Override per cloud for backwards compatibility.
    * NodeSubmitter checks these after SparkClusterNameEnvVar.
    */
  def deprecatedClusterNameEnvVars: Seq[String] = Seq.empty

  def jarName: String = ""
  def flinkJarName: String = "flink_assembly_deploy.jar"
  def onlineClass: String = ""
  def tablePartitionsDataset: String = ""
  def dqMetricsDataset: String = ""

  def resolveConfPath(stagedFileUri: String): String = stagedFileUri.split("/").last

  def kvStoreApiProperties: Map[String, String] = Map.empty

  /** Returns infra/cloud-specific submission properties for Flink jobs.
    * Values here are consumed by submitters in submit() and never passed to FlinkJob.main.
    * Override per cloud to populate FlinkMainJarURI, FlinkCheckpointUri, EKS/connector keys, etc.
    */
  def buildFlinkSubmissionProps(env: Map[String, String],
                                version: String,
                                artifactPrefix: String): Map[String, String] = Map.empty

  /** Key used in submissionProperties to pass the cluster identifier to submit().
    * Override per cloud: GCP uses ClusterName, AWS uses ClusterId.
    */
  def clusterIdentifierKey: String = JobSubmitterConstants.ClusterName
}

object JobSubmitter {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  def getApplicationArgs(jobType: JobType, args: Array[String]): Array[String] = {
    val userArgs = args.filter(arg => !JobSubmitterConstants.SharedInternalArgs.exists(arg.startsWith))
    jobType match {
      case FlinkJob =>
        userArgs.filter(arg => !arg.startsWith(JobSubmitterConstants.ConfTypeArgKeyword))
      case _ => userArgs
    }
  }

  def getArgValue(args: Array[String], argKeyword: String): Option[String] = {
    args
      .find(_.startsWith(argKeyword))
      .map(_.split("="))
      .map(_(1))
  }

  def parseConf[T <: TBase[_, _]: Manifest: ClassTag](confPath: String): T =
    ThriftJsonCodec.fromJsonFile[T](confPath, check = false)

  def getMetadata(args: Array[String]): Option[MetaData] = {
    val localConfPathValue = getArgValue(args, LocalConfPathArgKeyword)
    val confTypeValue = getArgValue(args, ConfTypeArgKeyword)
    val originalMode = getArgValue(args, OriginalModeArgKeyword)

    if (localConfPathValue.isDefined) {
      val metadata = if (confTypeValue.isDefined) {
        confTypeValue.get match {
          case "joins"           => parseConf[api.Join](localConfPathValue.get).metaData
          case "group_bys"       => parseConf[api.GroupBy](localConfPathValue.get).metaData
          case "staging_queries" => parseConf[api.StagingQuery](localConfPathValue.get).metaData
          case "models"          => parseConf[api.Model](localConfPathValue.get).metaData
          case _ =>
            throw new IllegalArgumentException(
              s"Unable to retrieve object metadata due to invalid confType $confTypeValue"
            )
        }
      } else if (originalMode.isDefined && originalMode.get == "metastore") {
        // attempt to parse as a generic MetaData object
        parseConf[api.MetaData](localConfPathValue.get)
      } else {
        throw new IllegalArgumentException("Unable to retrieve object metadata")
      }

      Option(metadata)
    } else None
  }

  def getModeConfigProperties(args: Array[String]): Option[Map[String, String]] = {
    val maybeMetadata = getMetadata(args)
    val modeConfigProperties = if (maybeMetadata.isDefined) {
      val metadata = maybeMetadata.get

      val executionInfo = Option(metadata.getExecutionInfo)

      if (executionInfo.isEmpty) {
        None
      } else {
        val originalMode = getArgValue(args, OriginalModeArgKeyword)

        (Option(executionInfo.get.conf), originalMode) match {
          case (Some(conf), Some(mode)) =>
            val modeConfs = if (conf.isSetModeConfigs && conf.getModeConfigs.containsKey(mode)) {
              conf.getModeConfigs.get(mode).toScala
            } else if (conf.isSetCommon) {
              conf.getCommon.toScala
            } else {
              Map[String, String]()
            }
            Option(modeConfs)
          case _ => None
        }
      }
    } else None

    logger.info(s"Setting job properties: $modeConfigProperties")

    modeConfigProperties
  }

  def getClusterConfig(args: Array[String]): Option[Map[String, String]] = {
    val maybeMetadata = getMetadata(args)
    val clusterConfig = if (maybeMetadata.isDefined) {
      val metadata = maybeMetadata.get

      val executionInfo = Option(metadata.getExecutionInfo)

      if (executionInfo.isEmpty) {
        None
      } else {
        val originalMode = getArgValue(args, OriginalModeArgKeyword)

        (Option(executionInfo.get.clusterConf), originalMode) match {
          case (Some(clusterConf), Some(mode)) =>
            val modeConfig =
              if (clusterConf.isSetModeClusterConfigs && clusterConf.getModeClusterConfigs.containsKey(mode)) {
                clusterConf.getModeClusterConfigs.get(mode).toScala
              } else if (clusterConf.isSetCommon) {
                clusterConf.getCommon.toScala
              } else {
                Map[String, String]()
              }
            Option(modeConfig)
          case _ => None
        }
      }
    } else None
    clusterConfig
  }

}

object JobSubmitterConstants {
  val MainClass = "mainClass"
  val JarURI = "jarUri"
  val FlinkMainJarURI = "flinkMainJarUri"
  val FlinkPubSubConnectorJarURI = "flinkPubSubConnectorJarUri"
  val FlinkKinesisConnectorJarURI = "flinkKinesisConnectorJarUri"
  val FlinkJarsUri = "flinkJarsUri"
  val AdditionalJars = "additionalJars"
  val SavepointUri = "savepointUri"
  val FlinkCheckpointUri = "flinkCheckpointUri"

  val JobId = "jobId"

  // Only lowercase, numbers, and dashes allowed for key labels in Dataproc
  val JobType = "job-type"
  val MetadataName = "metadata-name"
  val ZiplineVersion = "zipline-version"

  val SparkJobType = "spark"
  val FlinkJobType = "flink"

  // Main class constants
  val BatchRunnerClass = "ai.chronon.spark.batch.BatchNodeRunner"
  val KVUploadRunnerClass = "ai.chronon.spark.kv_store.KVUploadNodeRunner"
  val ModelRunnerClass = "ai.chronon.spark.model.ModelNodeRunner"
  val FlinkMainClass = "ai.chronon.flink.FlinkJob"

  // EMR specific properties
  val ClusterInstanceCount = "clusterInstanceCount"
  val ClusterInstanceType = "clusterInstanceType"
  val ClusterIdleTimeout = "clusterIdleTimeout"
  val ClusterId = "jobFlowId"
  val ClusterName = "clusterName"
  val SubnetId = "subnetId"
  val SecurityGroupId = "securityGroupId"
  val EmrReleaseLabel = "emrReleaseLabel"

  // EKS (Flink on EKS) specific properties
  val EksServiceAccount = "eksServiceAccount"
  val EksNamespace = "eksNamespace"

  val JarUriArgKeyword = "--jar-uri"
  val JobTypeArgKeyword = "--job-type"
  val MainClassKeyword = "--main-class"
  val FlinkMainJarUriArgKeyword = "--flink-main-jar-uri"
  val FlinkPubSubJarUriArgKeyword = "--flink-pubsub-jar-uri"
  val FlinkKinesisJarUriArgKeyword = "--flink-kinesis-jar-uri"
  val FlinkJarsUriArgKeyword = "--flink-jars-uri"
  val AdditionalJarsUriArgKeyword = "--additional-jars"
  val FilesArgKeyword = "--files"
  val ConfTypeArgKeyword = "--conf-type"
  val ConfPathArgKeyword = "--conf-path"
  val LocalConfPathArgKeyword = "--local-conf-path"
  val OriginalModeArgKeyword = "--original-mode"
  val ZiplineVersionArgKeyword = "--zipline-version"
  val GroupByNameArgKeyword = "--groupby-name"
  val OnlineClassArgKeyword = "--online-class"
  val ApiPropsArgPrefix = "-Z"
  val LocalZiplineVersionArgKeyword = "--local-zipline-version"
  val StreamingManifestPathArgKeyword = "--streaming-manifest-path"
  val StreamingCheckpointPathArgKeyword = "--streaming-checkpoint-path"
  val StreamingModeArgKeyword = "--streaming-mode"

  val StreamingVersionCheckDeploy = "--version-check"
  val StreamingTopicOverrideArgKeyword = "--topic-override"

  val StreamingLatestSavepointArgKeyword = "--latest-savepoint"
  val StreamingCustomSavepointArgKeyword = "--custom-savepoint"
  val StreamingNoSavepointArgKeyword = "--no-savepoint"

  val JobIdArgKeyword = "--job-id"
  val EksServiceAccountArgKeyword = "--eks-service-account"
  val EksNamespaceArgKeyword = "--eks-namespace"

  val SharedInternalArgs: Set[String] = Set(
    JarUriArgKeyword,
    JobTypeArgKeyword,
    MainClassKeyword,
    FlinkMainJarUriArgKeyword,
    FlinkPubSubJarUriArgKeyword,
    FlinkKinesisJarUriArgKeyword,
    FlinkJarsUriArgKeyword,
    AdditionalJarsUriArgKeyword,
    LocalConfPathArgKeyword,
    OriginalModeArgKeyword,
    FilesArgKeyword,
    ZiplineVersionArgKeyword,
    LocalZiplineVersionArgKeyword,
    StreamingModeArgKeyword,
    StreamingLatestSavepointArgKeyword,
    StreamingCustomSavepointArgKeyword,
    StreamingNoSavepointArgKeyword,
    StreamingCheckpointPathArgKeyword,
    StreamingVersionCheckDeploy,
    JobIdArgKeyword,
    EksServiceAccountArgKeyword,
    EksNamespaceArgKeyword
  )

  // Generic spark cluster name environment variable - works across all cloud providers
  val SparkClusterNameEnvVar = "SPARK_CLUSTER_NAME"

  val GcpBigtableInstanceIdEnvVar = "GCP_BIGTABLE_INSTANCE_ID"
  val GcpProjectIdEnvVar = "GCP_PROJECT_ID"
  val GcpRegionEnvVar = "GCP_REGION"
  val GcpLocationEnvVar = "GCP_LOCATION"
  // Deprecated: Use ClusterNameEnvVar instead
  val GcpDataprocClusterNameEnvVar = "GCP_DATAPROC_CLUSTER_NAME"
  val EnableUploadKVClientEnvVar = "ENABLE_UPLOAD_CLIENTS"

  // AWS-specific environment variables
  // Deprecated: Use ClusterNameEnvVar instead
  val EmrClusterNameEnvVar = "EMR_CLUSTER_NAME"

  val TablePartitionsDatasetNameArgKeyword = "--table-partitions-dataset"
  val TableStatsDatasetNameArgKeyword = "--table-stats-dataset"

  val CheckIfJobIsRunning = "check-if-job-is-running"
  val StreamingDeploy = "deploy"

  // We use incremental checkpoints and we cap how many we keep around
  val MaxRetainedCheckpoints: String = "10"

  // Flink jobs built with thin jars need Spark catalyst/SQL deps on the classpath.
  // When FlinkJarsUri is provided, these filenames are resolved against that base path.
  val FlinkAdditionalJarNames: Array[String] = Array(
    "commons-collections4-4.4.jar",
    "commons-compiler-3.1.9.jar",
    "janino-3.1.9.jar",
    "json4s-ast_2.12-3.7.0-M11.jar",
    "json4s-core_2.12-3.7.0-M11.jar",
    "kryo-shaded-4.0.2.jar",
    "metrics-core-4.2.19.jar",
    "metrics-json-4.2.19.jar",
    "spark-catalyst_2.12-3.5.3.jar",
    "spark-common-utils_2.12-3.5.3.jar",
    "spark-core_2.12-3.5.3.jar",
    "spark-kvstore_2.12-3.5.3.jar",
    "spark-launcher_2.12-3.5.3.jar",
    "spark-hive_2.12-3.5.3.jar",
    "spark-network-common_2.12-3.5.3.jar",
    "spark-network-shuffle_2.12-3.5.3.jar",
    "spark-sql-api_2.12-3.5.3.jar",
    "spark-sql_2.12-3.5.3.jar",
    "spark-unsafe_2.12-3.5.3.jar",
    "xbean-asm9-shaded-4.23.jar"
  )

  def additionalFlinkJars(flinkJarsBasePath: String): Array[String] = {
    val base = if (flinkJarsBasePath.endsWith("/")) flinkJarsBasePath else flinkJarsBasePath + "/"
    FlinkAdditionalJarNames.map(base + _)
  }
}
