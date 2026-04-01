import os

from ai.chronon.logger import get_logger
from ai.chronon.repo.constants import ROUTES, ZIPLINE_DIRECTORY
from ai.chronon.repo.default_runner import Runner
from ai.chronon.repo.utils import (
    JobType,
    check_output,
    extract_filename_from_path,
    get_customer_id,
    upload_to_blob_store,
)

LOG = get_logger()

# Azure-specific constants
KYUUBI_ENTRY = "ai.chronon.spark.submission.KyuubiSubmitter"
ZIPLINE_AZURE_JAR_DEFAULT = "cloud_azure_lib_deploy.jar"
ZIPLINE_AZURE_ONLINE_CLASS_DEFAULT = "ai.chronon.integrations.cloud_azure.AzureApiImpl"
ZIPLINE_AZURE_SERVICE_JAR = "service_assembly_deploy.jar"


class AzureRunner(Runner):
    def __init__(self, args):
        artifacts_prefix = os.environ.get(
            "ARTIFACT_PREFIX", f"abfss://zipline-artifacts-{get_customer_id()}"
        )
        azure_jar_path = AzureRunner.download_jar(
            ZIPLINE_DIRECTORY, artifacts_prefix, args["version"], ZIPLINE_AZURE_JAR_DEFAULT
        )
        service_jar_path = AzureRunner.download_jar(
            ZIPLINE_DIRECTORY, artifacts_prefix, args["version"], ZIPLINE_AZURE_SERVICE_JAR
        )
        jar_path = f"{service_jar_path}:{azure_jar_path}" if args["mode"] == "fetch" else azure_jar_path
        self.version = args.get("version", "latest")
        self.disable_cloud_logging = args.get("no_cloud_logging", False)

        super().__init__(args, os.path.expanduser(jar_path))

    @staticmethod
    def download_jar(destination_dir: str, artifacts_prefix: str, version: str, jar_name: str):
        """Download a jar from Azure Blob Storage."""
        from ai.chronon.repo.utils import download_from_blob_store

        destination_path = f"{destination_dir}/{jar_name}"
        source_uri = f"{artifacts_prefix}/release/{version}/jars/{jar_name}"

        if os.path.exists(destination_path):
            LOG.info(f"{destination_path} already exists locally, skipping download")
        else:
            LOG.info(f"Downloading {jar_name} from {source_uri}...")
            download_from_blob_store(source_uri, destination_path)

        return destination_path

    def generate_kyuubi_submitter_args(
        self,
        user_args: str,
        job_type: JobType = JobType.SPARK,
        local_files_to_upload=None,
    ):
        warehouse_prefix = os.environ.get(
            "WAREHOUSE_PREFIX", f"abfss://zipline-warehouse-{get_customer_id()}"
        )
        artifacts_prefix = os.environ.get(
            "ARTIFACT_PREFIX", f"abfss://zipline-artifacts-{get_customer_id()}"
        )
        remote_files = []
        for source_file in (local_files_to_upload or []):
            destination_file_path = f"metadata/{extract_filename_from_path(source_file)}"
            remote_files.append(
                upload_to_blob_store(
                    source_file, f"{warehouse_prefix}/{destination_file_path}"
                )
            )

        file_args = ",".join(remote_files)

        jar_uri = f"{artifacts_prefix}/release/{self.version}/jars/{ZIPLINE_AZURE_JAR_DEFAULT}"

        final_args = "{user_args} --jar-uri={jar_uri} --job-type={job_type} --main-class={main_class}"

        if job_type == JobType.SPARK:
            main_class = "ai.chronon.spark.Driver"
            result = final_args.format(
                user_args=user_args,
                jar_uri=jar_uri,
                job_type=job_type.value,
                main_class=main_class,
            )
            if file_args:
                result += f" --files={file_args}"
            return result
        else:
            raise ValueError(f"Invalid job type: {job_type}")

    def run(self):
        command_list = []
        if self.mode == "info":
            command_list.append(
                "python3 {script} --conf {conf} --ds {ds} --repo {repo}".format(
                    script=self.render_info, conf=self.conf, ds=self.ds, repo=self.repo
                )
            )
        elif self.sub_help or self.mode == "fetch":
            entrypoint = "ai.chronon.online.fetcher.FetcherMain"
            command_list.append(
                "java -cp {jar} {entrypoint} {subcommand} {args}".format(
                    jar=self.jar_path,
                    entrypoint=entrypoint,
                    args="--help" if self.sub_help else self._gen_final_args(),
                    subcommand=ROUTES[self.conf_type][self.mode],
                )
            )
        elif self.mode in ["streaming", "streaming-client"]:
            raise ValueError("Streaming is not supported for Azure yet.")
        else:
            local_files_to_upload = []
            if self.conf:
                local_files_to_upload.append(os.path.join(self.repo, self.conf))
            remote_conf_path = (
                extract_filename_from_path(self.conf)
                if self.conf
                else None
            )

            user_args = "{subcommand} {args} {additional_args}".format(
                subcommand=ROUTES[self.conf_type][self.mode],
                args=self._gen_final_args(
                    start_ds=self.start_ds,
                    override_conf_path=remote_conf_path,
                ),
                additional_args=os.environ.get("CHRONON_CONFIG_ADDITIONAL_ARGS", ""),
            )

            kyuubi_args = self.generate_kyuubi_submitter_args(
                local_files_to_upload=local_files_to_upload,
                user_args=user_args,
            )
            command = f"java -cp {self.jar_path} {KYUUBI_ENTRY} {kyuubi_args}"
            command_list.append(command)

        if len(command_list) == 1:
            output = check_output(command_list[0], streaming=True).decode("utf-8").split("\n")
            print(*output, sep="\n")
        elif len(command_list) > 1:
            raise ValueError("Parallel execution is not supported for Azure yet.")
