package ai.chronon.integrations.aws

import ai.chronon.api.JobStatusType
import ai.chronon.spark.submission.JobSubmitterConstants._
import ai.chronon.spark.submission.{JobSubmitter, JobType, FlinkJob => TypeFlinkJob, SparkJob => TypeSparkJob}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.emrserverless.EmrServerlessClient
import software.amazon.awssdk.services.emrserverless.model._

import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters._

class EmrServerlessSubmitter(
    emrServerlessClient: EmrServerlessClient,
    applicationId: Option[String],
    executionRoleArn: String,
    s3LogUri: String,
    eksFlinkSubmitter: Option[EksFlinkSubmitter] = None,
    dynamodbTableName: String = "",
    awsRegion: String = "",
    override val tablePartitionsDataset: String = "",
    override val dqMetricsDataset: String = "",
    flinkEksServiceAccount: Option[String] = None,
    flinkEksNamespace: Option[String] = None,
    eksClusterName: Option[String] = None,
    ingressBaseUrl: Option[String] = None,
    flinkHealthCheckFn: Option[String] => Boolean = _ => true
) extends JobSubmitter {

  private val DefaultEmrReleaseLabel = "emr-7.12.0"
  private val DefaultApplicationName = "chronon-serverless-app"

  private def getOrCreateApplication(releaseLabel: String, appName: String): String = {
    applicationId match {
      case Some(appId) =>
        logger.info(s"Using existing EMR Serverless application: $appId")
        appId

      case None =>
        val existingApps = {
          val buf = scala.collection.mutable.Buffer.empty[ApplicationSummary]
          var nextToken: Option[String] = None
          do {
            val requestBuilder = ListApplicationsRequest.builder()
            nextToken.foreach(requestBuilder.nextToken)
            val response = emrServerlessClient.listApplications(requestBuilder.build())
            buf ++= response.applications().asScala
            nextToken = Option(response.nextToken())
          } while (nextToken.isDefined)
          buf.filter(app => app.name() == appName && app.state() != ApplicationState.TERMINATED)
        }

        if (existingApps.nonEmpty) {
          val appId = existingApps.head.id()
          logger.info(s"Found existing EMR Serverless application: $appId")
          appId
        } else {
          logger.info(s"Creating new EMR Serverless application: $appName")
          val createRequest = CreateApplicationRequest
            .builder()
            .name(appName)
            .releaseLabel(releaseLabel)
            .`type`("Spark")
            .autoStartConfiguration(
              AutoStartConfig.builder().enabled(true).build()
            )
            .autoStopConfiguration(
              AutoStopConfig.builder().enabled(true).idleTimeoutMinutes(15).build()
            )
            .build()

          val response = emrServerlessClient.createApplication(createRequest)
          val newAppId = response.applicationId()
          logger.info(s"Created EMR Serverless application: $newAppId")

          waitForApplicationReady(newAppId)
          newAppId
        }
    }
  }

  private def waitForApplicationReady(appId: String): Unit = {
    var attempts = 0
    val maxAttempts = 60
    val sleepSeconds = 10

    while (attempts < maxAttempts) {
      val getRequest = GetApplicationRequest.builder().applicationId(appId).build()
      val app = emrServerlessClient.getApplication(getRequest).application()

      app.state() match {
        case ApplicationState.CREATED | ApplicationState.STARTED =>
          logger.info(s"Application $appId is ready (state: ${app.state()})")
          return
        case ApplicationState.CREATING | ApplicationState.STARTING =>
          logger.info(s"Waiting for application $appId (state: ${app.state()}, attempt $attempts/$maxAttempts)")
          Thread.sleep(sleepSeconds * 1000)
          attempts += 1
        case ApplicationState.STOPPING =>
          logger.info(
            s"Application $appId is stopping (attempt $attempts/$maxAttempts), waiting for terminal state"
          )
          Thread.sleep(sleepSeconds * 1000)
          attempts += 1
        case ApplicationState.TERMINATED | ApplicationState.STOPPED =>
          throw new RuntimeException(s"Application $appId entered terminal state: ${app.state()}")
        case _ =>
          throw new RuntimeException(s"Application $appId in unexpected state: ${app.state()}")
      }
    }
    throw new RuntimeException(s"Timeout waiting for application $appId to be ready")
  }

  override def submit(
      jobType: JobType,
      submissionProperties: Map[String, String],
      jobProperties: Map[String, String],
      files: List[String],
      labels: Map[String, String],
      args: String*
  ): String = {
    val userArgs = JobSubmitter.getApplicationArgs(jobType, args.toArray)

    jobType match {
      case TypeFlinkJob =>
        val jobId = submissionProperties.getOrElse(JobId, throw new RuntimeException("Job ID not found"))
        val mainClass = submissionProperties.getOrElse(MainClass, throw new RuntimeException("Main class not found"))
        val mainJarUri = submissionProperties.getOrElse(
          FlinkMainJarURI,
          throw new RuntimeException(s"Missing expected $FlinkMainJarURI"))
        val jarUri = submissionProperties.getOrElse(JarURI, throw new RuntimeException("Jar URI not found"))
        val flinkCheckpointPath = submissionProperties.getOrElse(
          FlinkCheckpointUri,
          throw new RuntimeException(s"Missing expected $FlinkCheckpointUri"))
        val maybeSavepointUri = submissionProperties.get(SavepointUri)
        val maybeKinesisConnectorJarUri = submissionProperties.get(FlinkKinesisConnectorJarURI)
        val maybeAdditionalJarsUri = submissionProperties.get(AdditionalJars)
        val additionalJars = maybeAdditionalJarsUri
          .map(_.split(",").map(_.trim).filter(_.nonEmpty))
          .getOrElse(Array.empty)
        val jarUris = Array(jarUri) ++ maybeKinesisConnectorJarUri.toList ++ additionalJars

        val serviceAccount = submissionProperties.getOrElse(
          EksServiceAccount,
          throw new RuntimeException(s"Missing expected $EksServiceAccount"))
        val namespace =
          submissionProperties.getOrElse(EksNamespace, throw new RuntimeException(s"Missing expected $EksNamespace"))

        val deploymentName = eksFlinkSubmitter
          .getOrElse(
            throw new RuntimeException("EksFlinkSubmitter is required for Flink jobs")
          )
          .submit(
            jobId = jobId,
            mainClass = mainClass,
            mainJarUri = mainJarUri,
            jarUris = jarUris,
            flinkCheckpointUri = flinkCheckpointPath,
            maybeSavepointUri = maybeSavepointUri,
            maybeFlinkJarsUri = submissionProperties.get(FlinkJarsUri),
            jobProperties = jobProperties,
            args = userArgs,
            serviceAccount = serviceAccount,
            namespace = namespace
          )
        s"flink:$namespace:$deploymentName"

      case TypeSparkJob =>
        submitSparkJob(submissionProperties, jobProperties, files, labels, args: _*)
    }
  }

  private def submitSparkJob(
      submissionProperties: Map[String, String],
      jobProperties: Map[String, String],
      files: List[String],
      labels: Map[String, String],
      args: String*
  ): String = {
    val mainClass = submissionProperties.getOrElse(MainClass, throw new RuntimeException("Main class not found"))
    val jarUri = submissionProperties.getOrElse(JarURI, throw new RuntimeException("Jar URI not found"))

    val appId = getOrCreateApplication(DefaultEmrReleaseLabel, DefaultApplicationName)

    val filesParam = if (files.nonEmpty) s" --files ${files.mkString(",")}" else ""

    val jobDriverBuilder = JobDriver
      .builder()
      .sparkSubmit(
        SparkSubmit
          .builder()
          .entryPoint(jarUri)
          .entryPointArguments(args.toList.asJava)
          .sparkSubmitParameters(s"--class $mainClass$filesParam")
          .build()
      )

    val configOverridesBuilder = ConfigurationOverrides
      .builder()
      .monitoringConfiguration(
        MonitoringConfiguration
          .builder()
          .s3MonitoringConfiguration(
            S3MonitoringConfiguration.builder().logUri(s3LogUri).build()
          )
          .cloudWatchLoggingConfiguration(
            CloudWatchLoggingConfiguration.builder().enabled(true).build()
          )
          .build()
      )

    if (jobProperties.nonEmpty) {
      configOverridesBuilder.applicationConfiguration(
        Configuration
          .builder()
          .classification("spark-defaults")
          .properties(jobProperties.asJava)
          .build()
      )
    }

    val jobName = submissionProperties.getOrElse(JobId, s"chronon-job-${System.currentTimeMillis()}")

    val startJobRunRequest = StartJobRunRequest
      .builder()
      .applicationId(appId)
      .executionRoleArn(executionRoleArn)
      .name(jobName)
      .jobDriver(jobDriverBuilder.build())
      .configurationOverrides(configOverridesBuilder.build())
      .tags(labels.asJava)
      .build()

    val response = emrServerlessClient.startJobRun(startJobRunRequest)
    val jobRunId = response.jobRunId()

    logger.info(s"Started EMR Serverless job run: $jobRunId on application $appId")
    logger.info(
      s"Monitor at: https://$awsRegion.console.aws.amazon.com/emr/home?region=$awsRegion#/serverless-applications/$appId/job-runs/$jobRunId"
    )

    s"$appId|$jobRunId"
  }

  override def status(jobId: String): JobStatusType = {
    if (jobId.startsWith("flink:")) {
      val parts = jobId.split(":", 3)
      val eksStatus = eksFlinkSubmitter
        .getOrElse(throw new RuntimeException("EksFlinkSubmitter is required for Flink jobs"))
        .status(deploymentName = parts(2), namespace = parts(1))
      eksStatus match {
        case JobStatusType.RUNNING if flinkHealthCheckFn(getFlinkUrl(jobId)) => JobStatusType.RUNNING
        case JobStatusType.RUNNING                                           => JobStatusType.PENDING
        case other                                                           => other
      }
    } else {
      try {
        val parts = jobId.split("\\|")
        if (parts.length != 2) {
          logger.debug(
            s"Unrecognized jobId format: $jobId (expected applicationId|jobRunId). Likely a legacy EMR Classic job.")
          return JobStatusType.UNKNOWN
        }

        val appId = parts(0)
        val jobRunId = parts(1)

        val getRequest = GetJobRunRequest
          .builder()
          .applicationId(appId)
          .jobRunId(jobRunId)
          .build()

        val jobRun = emrServerlessClient.getJobRun(getRequest).jobRun()
        val state = jobRun.state()

        state match {
          case JobRunState.SUBMITTED | JobRunState.PENDING | JobRunState.SCHEDULED => JobStatusType.PENDING
          case JobRunState.RUNNING                                                 => JobStatusType.RUNNING
          case JobRunState.SUCCESS                                                 => JobStatusType.SUCCEEDED
          case JobRunState.CANCELLING                                              => JobStatusType.RUNNING
          case JobRunState.FAILED | JobRunState.CANCELLED                          => JobStatusType.FAILED
          case _                                                                   => JobStatusType.UNKNOWN
        }
      } catch {
        case e: Exception =>
          logger.error(s"Error checking job status for jobId $jobId: ${e.getMessage}")
          JobStatusType.UNKNOWN
      }
    }
  }

  override def kill(jobId: String): Unit = {
    if (jobId.startsWith("flink:")) {
      val parts = jobId.split(":", 3)
      eksFlinkSubmitter
        .getOrElse(throw new RuntimeException("EksFlinkSubmitter is required for Flink jobs"))
        .delete(deploymentName = parts(2), namespace = parts(1))
    } else {
      try {
        val parts = jobId.split("\\|")
        if (parts.length != 2) {
          logger.debug(
            s"Unrecognized jobId format: $jobId (expected applicationId|jobRunId). Likely a legacy EMR Classic job.")
          return
        }

        val appId = parts(0)
        val jobRunId = parts(1)

        val cancelRequest = CancelJobRunRequest
          .builder()
          .applicationId(appId)
          .jobRunId(jobRunId)
          .build()

        emrServerlessClient.cancelJobRun(cancelRequest)
        logger.info(s"Cancelled EMR Serverless job run $jobRunId on application $appId")
      } catch {
        case e: Exception =>
          logger.error(s"Error killing job $jobId: ${e.getMessage}")
      }
    }
  }

  override def jarName: String = "cloud_aws_lib_deploy.jar"
  override def onlineClass: String = "ai.chronon.integrations.aws.AwsApiImpl"

  // Conf files are distributed via --files; Spark makes them available by filename in the working dir

  override def getJobUrl(jobId: String): Option[String] = {
    if (jobId.startsWith("flink:")) {
      val parts = jobId.split(":")
      if (parts.length == 3) {
        val namespace = parts(1)
        val deploymentName = parts(2)
        eksClusterName.map { clusterName =>
          s"https://$awsRegion.console.aws.amazon.com/eks/clusters/$clusterName/deployments/$deploymentName?namespace=$namespace&region=$awsRegion"
        }
      } else None
    } else if (jobId.contains("|")) {
      val parts = jobId.split("\\|")
      if (parts.length == 2) {
        val appId = parts(0)
        val jobRunId = parts(1)
        val logGroup = java.net.URLEncoder.encode("/aws/emr-serverless", "UTF-8")
        val logStream = java.net.URLEncoder.encode(s"applications/$appId/jobs/$jobRunId", "UTF-8")
        Some(
          s"https://$awsRegion.console.aws.amazon.com/cloudwatch/home?region=$awsRegion#logsV2:log-groups/log-group/$logGroup/log-events/$logStream"
        )
      } else None
    } else None
  }

  override def getSparkUrl(jobId: String): Option[String] = {
    if (jobId.startsWith("flink:")) {
      None
    } else if (jobId.contains("|")) {
      val parts = jobId.split("\\|")
      if (parts.length == 2) {
        try {
          val request = GetDashboardForJobRunRequest
            .builder()
            .applicationId(parts(0))
            .jobRunId(parts(1))
            .build()
          Some(emrServerlessClient.getDashboardForJobRun(request).url())
        } catch {
          case e: Exception =>
            logger.debug(s"Could not get Spark UI URL for $jobId: ${e.getMessage}")
            None
        }
      } else None
    } else None
  }

  override def getFlinkUrl(jobId: String): Option[String] = {
    if (!jobId.startsWith("flink:")) return None
    val parts = jobId.split(":", 3)
    if (parts.length != 3) return None
    val deploymentName = parts(2)
    ingressBaseUrl.map(base => s"${base.stripSuffix("/")}/flink/$deploymentName/")
  }

  override def buildFlinkSubmissionProps(env: Map[String, String],
                                         version: String,
                                         artifactPrefix: String): Map[String, String] = {
    val flinkJarUri = s"$artifactPrefix/release/$version/jars/$flinkJarName"
    val flinkStateUri = env.getOrElse(
      "FLINK_STATE_URI",
      throw new IllegalArgumentException("FLINK_STATE_URI must be set for GROUP_BY_STREAMING"))
    val eksServiceAccount = this.flinkEksServiceAccount
      .orElse(env.get("FLINK_EKS_SERVICE_ACCOUNT"))
      .getOrElse(throw new IllegalArgumentException("FLINK_EKS_SERVICE_ACCOUNT must be set for GROUP_BY_STREAMING"))
    val eksNamespace = this.flinkEksNamespace
      .orElse(env.get("FLINK_EKS_NAMESPACE"))
      .getOrElse(throw new IllegalArgumentException("FLINK_EKS_NAMESPACE must be set for GROUP_BY_STREAMING"))
    val base = Map(
      FlinkMainJarURI -> flinkJarUri,
      FlinkCheckpointUri -> s"$flinkStateUri/checkpoints",
      EksServiceAccount -> eksServiceAccount,
      EksNamespace -> eksNamespace
    )
    val enableKinesis = env.getOrElse("ENABLE_KINESIS", "false").toBoolean
    if (enableKinesis)
      base + (FlinkKinesisConnectorJarURI -> s"$artifactPrefix/release/$version/jars/connectors_kinesis_deploy.jar")
    else base
  }

  override def deprecatedClusterNameEnvVars: Seq[String] = Seq.empty

  // Serverless has no clusters to manage for batch; Flink goes to EKS
  override def isClusterCreateNeeded(isLongRunning: Boolean): Boolean = false

  override def ensureClusterReady(clusterName: String, clusterConf: Option[Map[String, String]])(implicit
      ec: ExecutionContext): Option[String] = None

  override def kvStoreApiProperties: Map[String, String] = Map(
    "AWS_DYNAMODB_TABLE_NAME" -> dynamodbTableName,
    "AWS_DEFAULT_REGION" -> awsRegion
  )

  override def close(): Unit = {
    try {
      emrServerlessClient.close()
    } catch {
      case _: Exception => logger.info("Error shutting down AWS clients.")
    }
  }
}

object EmrServerlessSubmitter {

  def apply(k8sConfig: Option[io.fabric8.kubernetes.client.Config] = None): EmrServerlessSubmitter = {
    val region = sys.env.getOrElse("AWS_REGION", sys.env.getOrElse("AWS_DEFAULT_REGION", "us-east-1"))
    val applicationId = sys.env.get("EMR_SERVERLESS_APP_ID")
    val executionRoleArn = sys.env.getOrElse(
      "EMR_EXECUTION_ROLE_ARN",
      throw new Exception("EMR_EXECUTION_ROLE_ARN environment variable not set")
    )
    val s3LogUri = sys.env.getOrElse(
      "EMR_LOG_URI",
      throw new Exception("EMR_LOG_URI environment variable not set")
    )
    val ingressBaseUrl = sys.env.get("HUB_BASE_URL")

    val client = EmrServerlessClient
      .builder()
      .region(Region.of(region))
      .build()

    new EmrServerlessSubmitter(
      client,
      applicationId,
      executionRoleArn,
      s3LogUri,
      eksFlinkSubmitter = Some(new EksFlinkSubmitter(k8sConfig, ingressBaseUrl = ingressBaseUrl)),
      awsRegion = region,
      flinkEksServiceAccount = sys.env.get("FLINK_EKS_SERVICE_ACCOUNT"),
      flinkEksNamespace = sys.env.get("FLINK_EKS_NAMESPACE"),
      eksClusterName = sys.env.get("EKS_CLUSTER_NAME"),
      ingressBaseUrl = ingressBaseUrl
    )
  }

  private[aws] def createSubmissionPropsMap(jobType: JobType, args: Array[String]): Map[String, String] = {
    val jarUri = JobSubmitter
      .getArgValue(args, JarUriArgKeyword)
      .getOrElse(throw new Exception("Missing required argument: " + JarUriArgKeyword))
    val mainClass = JobSubmitter
      .getArgValue(args, MainClassKeyword)
      .getOrElse(throw new Exception("Missing required argument: " + MainClassKeyword))

    jobType match {
      case TypeSparkJob =>
        val jobId = JobSubmitter
          .getArgValue(args, JobIdArgKeyword)
          .getOrElse(s"chronon-job-${System.currentTimeMillis()}")
        Map(
          MainClass -> mainClass,
          JarURI -> jarUri,
          JobId -> jobId
        )
      case TypeFlinkJob =>
        val jobId = JobSubmitter
          .getArgValue(args, JobIdArgKeyword)
          .getOrElse(throw new Exception("Missing required argument: " + JobIdArgKeyword))
        val flinkCheckpointUri = JobSubmitter
          .getArgValue(args, StreamingCheckpointPathArgKeyword)
          .getOrElse(throw new Exception(s"Missing required argument $StreamingCheckpointPathArgKeyword"))
        val flinkMainJarUri = JobSubmitter
          .getArgValue(args, FlinkMainJarUriArgKeyword)
          .getOrElse(throw new Exception("Missing required argument: " + FlinkMainJarUriArgKeyword))
        val eksServiceAccount = JobSubmitter
          .getArgValue(args, EksServiceAccountArgKeyword)
          .getOrElse(throw new Exception("Missing required argument: " + EksServiceAccountArgKeyword))
        val eksNamespace = JobSubmitter
          .getArgValue(args, EksNamespaceArgKeyword)
          .getOrElse(throw new Exception("Missing required argument: " + EksNamespaceArgKeyword))
        val maybeKinesisJarUri = JobSubmitter.getArgValue(args, FlinkKinesisJarUriArgKeyword)
        val maybeFlinkJarsUri = JobSubmitter.getArgValue(args, FlinkJarsUriArgKeyword)
        val maybeAdditionalJarsUri = JobSubmitter.getArgValue(args, AdditionalJarsUriArgKeyword)

        val baseJobProps = Map(
          JobId -> jobId,
          MainClass -> mainClass,
          JarURI -> jarUri,
          FlinkMainJarURI -> flinkMainJarUri,
          FlinkCheckpointUri -> flinkCheckpointUri,
          EksServiceAccount -> eksServiceAccount,
          EksNamespace -> eksNamespace
        ) ++ maybeKinesisJarUri.map(FlinkKinesisConnectorJarURI -> _) ++
          maybeFlinkJarsUri.map(FlinkJarsUri -> _) ++
          maybeAdditionalJarsUri.map(AdditionalJars -> _)

        val userPassedSavepoint = JobSubmitter.getArgValue(args, StreamingCustomSavepointArgKeyword)
        val maybeSavepointUri =
          if (userPassedSavepoint.isDefined) {
            userPassedSavepoint
          } else if (args.contains(StreamingLatestSavepointArgKeyword)) {
            System.err.println("Latest savepoint not yet supported for EMR Serverless, proceeding without savepoint")
            None
          } else {
            None
          }

        maybeSavepointUri match {
          case Some(uri) =>
            System.err.println(s"Deploying Flink app with savepoint uri $uri")
            baseJobProps + (SavepointUri -> uri)
          case None => baseJobProps
        }
    }
  }

  def main(args: Array[String]): Unit = {
    val internalArgs = SharedInternalArgs

    val userArgs = args.filter(arg => !internalArgs.exists(arg.startsWith))

    val jobTypeValue = JobSubmitter
      .getArgValue(args, JobTypeArgKeyword)
      .getOrElse(throw new Exception("Missing required argument: " + JobTypeArgKeyword))

    val jobType = jobTypeValue.toLowerCase match {
      case "spark" => TypeSparkJob
      case "flink" => TypeFlinkJob
      case _       => throw new Exception("Invalid job type. Must be 'spark' or 'flink'.")
    }

    val submissionProps = createSubmissionPropsMap(jobType, args)

    val files = args.find(_.startsWith(FilesArgKeyword)) match {
      case None => Array.empty[String]
      case Some(arg) =>
        val eqIdx = arg.indexOf('=')
        if (eqIdx < 0 || eqIdx == arg.length - 1) {
          throw new IllegalArgumentException(
            s"Malformed files argument: '$arg'. Expected format: ${FilesArgKeyword}=file1,file2")
        }
        arg.substring(eqIdx + 1).split(",")
    }

    val finalArgs = userArgs.toSeq
    val modeConfigProperties = JobSubmitter.getModeConfigProperties(args)

    val submitter = EmrServerlessSubmitter()
    val resultJobId = submitter.submit(
      jobType = jobType,
      submissionProperties = submissionProps,
      jobProperties = modeConfigProperties.getOrElse(Map.empty),
      files = files.toList,
      labels = Map.empty,
      finalArgs: _*
    )

    println(s"Job submitted with ID: $resultJobId")
  }
}
