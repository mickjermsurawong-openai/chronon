import inspect
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Dict, List, Optional, Union

import gen_thrift.api.ttypes as ttypes
import gen_thrift.common.ttypes as common
from ai.chronon import utils
from ai.chronon import windows as window_utils
from ai.chronon.data_types import DataType, FieldsType
from ai.chronon.utils import ANY_SOURCE_TYPE, normalize_source, normalize_sources


class ModelBackend:
    VERTEXAI = ttypes.ModelBackend.VertexAI
    SAGEMAKER = ttypes.ModelBackend.SageMaker


class DeploymentStrategyType:
    # deploys the model in a blue-green fashion (~2x capacity) to another endpoint and gradually ramps traffic
    BLUE_GREEN = ttypes.DeploymentStrategyType.BLUE_GREEN

    # deploys the model in a rolling manner by gradually scaling down existing instances and scaling up new instances
    ROLLING = ttypes.DeploymentStrategyType.ROLLING

    # deploys the model immediately to the endpoint without any traffic ramping
    IMMEDIATE = ttypes.DeploymentStrategyType.IMMEDIATE


@dataclass
class ResourceConfig:
    min_replica_count: Optional[int] = None
    max_replica_count: Optional[int] = None
    machine_type: Optional[str] = None

    def to_thrift(self):
        return ttypes.ResourceConfig(
            minReplicaCount=self.min_replica_count,
            maxReplicaCount=self.max_replica_count,
            machineType=self.machine_type,
        )


@dataclass
class InferenceSpec:
    model_backend: Optional[ModelBackend] = None
    model_backend_params: Optional[Dict[str, str]] = None
    resource_config: Optional[ResourceConfig] = None

    def to_thrift(self):
        resource_config_thrift = None
        if self.resource_config:
            resource_config_thrift = self.resource_config.to_thrift()

        return ttypes.InferenceSpec(
            modelBackend=self.model_backend,
            modelBackendParams=self.model_backend_params,
            resourceConfig=resource_config_thrift,
        )


@dataclass
class TrainingSpec:
    # TODO: may want to try to support staging query as a training_data_source
    training_data_source: Optional[ANY_SOURCE_TYPE] = None
    training_data_window: Optional[Union[common.Window, str]] = None
    schedule: Optional[str] = None
    image: Optional[str] = None
    python_module: Optional[str] = None
    resource_config: Optional[ResourceConfig] = None
    job_configs: Optional[Dict[str, str]] = None

    def to_thrift(self):
        resource_config_thrift = None
        if self.resource_config:
            resource_config_thrift = self.resource_config.to_thrift()

        training_data_source_thrift = None
        if self.training_data_source:
            training_data_source_thrift = normalize_source(self.training_data_source)

        # Normalize window - convert string like "30d" or "24h" to common.Window
        training_data_window_thrift = None
        if self.training_data_window:
            training_data_window_thrift = window_utils.normalize_window(self.training_data_window)

        return ttypes.TrainingSpec(
            trainingDataSource=training_data_source_thrift,
            trainingDataWindow=training_data_window_thrift,
            schedule=self.schedule,
            image=self.image,
            pythonModule=self.python_module,
            resourceConfig=resource_config_thrift,
            jobConfigs=self.job_configs,
        )


@dataclass
class ServingContainerConfig:
    image: Optional[str] = None
    serving_health_route: Optional[str] = None
    serving_predict_route: Optional[str] = None
    serving_container_env_vars: Optional[Dict[str, str]] = None

    def to_thrift(self):
        return ttypes.ServingContainerConfig(
            image=self.image,
            servingHealthRoute=self.serving_health_route,
            servingPredictRoute=self.serving_predict_route,
            servingContainerEnvVars=self.serving_container_env_vars,
        )


@dataclass
class EndpointConfig:
    endpoint_name: Optional[str] = None
    additional_configs: Optional[Dict[str, str]] = None

    def to_thrift(self):
        return ttypes.EndpointConfig(
            endpointName=self.endpoint_name,
            additionalConfigs=self.additional_configs,
        )


@dataclass
class Metric:
    name: Optional[str] = None
    threshold: Optional[float] = None

    def to_thrift(self):
        return ttypes.Metric(
            name=self.name,
            threshold=self.threshold,
        )


@dataclass
class RolloutStrategy:
    rollout_type: Optional[DeploymentStrategyType] = None
    validation_traffic_percent_ramps: Optional[List[int]] = None
    validation_traffic_duration_mins: Optional[List[int]] = None
    rollout_metric_thresholds: Optional[List[Metric]] = None

    def to_thrift(self):
        rollout_metric_thresholds_thrift = None
        if self.rollout_metric_thresholds:
            rollout_metric_thresholds_thrift = [
                metric.to_thrift() for metric in self.rollout_metric_thresholds
            ]

        return ttypes.RolloutStrategy(
            rolloutType=self.rollout_type,
            validationTrafficPercentRamps=self.validation_traffic_percent_ramps,
            validationTrafficDurationMins=self.validation_traffic_duration_mins,
            rolloutMetricThresholds=rollout_metric_thresholds_thrift,
        )


@dataclass
class DeploymentSpec:
    container_config: Optional[ServingContainerConfig] = None
    endpoint_config: Optional[EndpointConfig] = None
    resource_config: Optional[ResourceConfig] = None
    rollout_strategy: Optional[RolloutStrategy] = None

    def to_thrift(self):
        container_config_thrift = None
        if self.container_config:
            container_config_thrift = self.container_config.to_thrift()

        endpoint_config_thrift = None
        if self.endpoint_config:
            endpoint_config_thrift = self.endpoint_config.to_thrift()

        resource_config_thrift = None
        if self.resource_config:
            resource_config_thrift = self.resource_config.to_thrift()

        rollout_strategy_thrift = None
        if self.rollout_strategy:
            rollout_strategy_thrift = self.rollout_strategy.to_thrift()

        return ttypes.DeploymentSpec(
            containerConfig=container_config_thrift,
            endpointConfig=endpoint_config_thrift,
            resourceConfig=resource_config_thrift,
            rolloutStrategy=rollout_strategy_thrift,
        )


def Model(
    version: str,
    inference_spec: Optional[InferenceSpec] = None,
    input_mapping: Optional[Dict[str, str]] = None,
    output_mapping: Optional[Dict[str, str]] = None,
    value_fields: Optional[FieldsType] = None,
    model_artifact_base_uri: Optional[str] = None,
    training_conf: Optional[TrainingSpec] = None,
    deployment_conf: Optional[DeploymentSpec] = None,
    output_namespace: Optional[str] = None,
    table_properties: Optional[Dict[str, str]] = None,
    tags: Optional[Dict[str, str]] = None,
) -> ttypes.Model:
    """
    Creates a Model object for ML model inference and orchestration.

    :param version:
        Version string for the model configuration
    :type version: str
    :param inference_spec:
        Model + model backend specific details necessary to perform inference
    :type inference_spec: InferenceSpec
    :param input_mapping:
        Spark SQL queries to transform input data to the format expected by the model
    :type input_mapping: Dict[str, str]
    :param output_mapping:
        Spark SQL queries to transform model output to desired output format
    :type output_mapping: Dict[str, str]
    :param value_fields:
        List of tuples of (field_name, DataType) defining the schema of the model's output values.
        If provided, creates a STRUCT schema that will be set as the model's valueSchema.
        Example: [('score', DataType.DOUBLE), ('category', DataType.STRING)]
    :type value_fields: FieldsType
    :param model_artifact_base_uri:
        Base URI where trained model artifacts are stored
    :type model_artifact_base_uri: str
    :param training_conf:
        Configs related to orchestrating model training jobs
    :type training_conf: TrainingSpec
    :param deployment_conf:
        Configs related to orchestrating model deployment
    :type deployment_conf: DeploymentSpec
    :param output_namespace:
        Namespace for the model output
    :type output_namespace: str
    :param table_properties:
        Additional table properties for the model output
    :type table_properties: Dict[str, str]
    :param tags:
        Additional metadata that does not directly affect computation, but is useful for management.
    :type tags: Dict[str, str]
    :return:
        A Model object
    """
    # Get caller's filename to assign team
    team = inspect.stack()[1].filename.split("/")[-2]

    assert isinstance(version, str), f"Version must be a string, but found {type(version).__name__}"

    # Create metadata
    meta_data = ttypes.MetaData(
        outputNamespace=output_namespace,
        team=team,
        tags=tags,
        tableProperties=table_properties,
        version=version,
    )

    # Convert inference_spec to thrift if provided
    inference_spec_thrift = None
    if inference_spec:
        inference_spec_thrift = inference_spec.to_thrift()

    # Create value schema if value_fields are provided
    value_schema = None
    if value_fields:
        schema_name = "model_value_schema"
        value_schema = DataType.STRUCT(schema_name, *value_fields)

    # Convert training_conf to thrift if provided
    training_conf_thrift = None
    if training_conf:
        training_conf_thrift = training_conf.to_thrift()

    # Convert deployment_conf to thrift if provided
    deployment_conf_thrift = None
    if deployment_conf:
        deployment_conf_thrift = deployment_conf.to_thrift()

    # Create and return the Model object
    model = ttypes.Model(
        metaData=meta_data,
        inferenceSpec=inference_spec_thrift,
        inputMapping=input_mapping,
        outputMapping=output_mapping,
        valueSchema=value_schema,
        modelArtifactBaseUri=model_artifact_base_uri,
        trainingConf=training_conf_thrift,
        deploymentConf=deployment_conf_thrift,
    )

    return model


def _get_model_transforms_output_table_name(
    model_transforms: ttypes.ModelTransforms, full_name: bool = False
):
    """Generate output table name for ModelTransforms"""
    utils.__set_name(model_transforms, ttypes.ModelTransforms, "models")
    return utils.output_table_name(model_transforms, full_name=full_name)


def ModelTransforms(
    sources: Sequence[ANY_SOURCE_TYPE],
    models: List[ttypes.Model],
    version: int,
    passthrough_fields: Optional[List[str]] = None,
    key_fields: Optional[FieldsType] = None,
    output_namespace: Optional[str] = None,
    table_properties: Optional[Dict[str, str]] = None,
    tags: Optional[Dict[str, str]] = None,
) -> ttypes.ModelTransforms:
    """
    ModelTransforms allows taking the output of existing sources (Event/Entity/Join) and
    enriching them with 1 or more model outputs. This can be used in GroupBys, Joins, or hit directly
    via the fetcher. The GroupBy path allows for async materialization of model outputs to the online KV store for low latency
    serving. The fetcher path allows for on-demand model inference during online serving (at the cost of higher latency / more
    model inference calls).

    Attributes:
     - sources: List of existing sources (Event/Entity/Join sources) to be enriched with model outputs
     - models: List of Model objects that will be used for inference on the source data
     - passthrough_fields: Fields from the source that we want to passthrough alongside the model outputs
    - key_fields: List of tuples of (field_name, DataType) defining the schema of the key fields.
        If provided, creates a STRUCT schema that will be set as the ModelTransforms' keySchema.
        Example: [('user_id', DataType.STRING), ('session_id', DataType.STRING)]
     - output_namespace: Namespace for the model output
     - table_properties: Additional table properties for the model output
     - tags: Additional metadata tags
    """
    # Get caller's filename to assign team
    team = inspect.stack()[1].filename.split("/")[-2]

    # Set names for Model objects if they don't have names yet
    if models:
        for model in models:
            if not model.metaData.name:
                utils.__set_name(model, ttypes.Model, "models")

    # Normalize all sources to ensure they are properly wrapped
    normalized_sources = normalize_sources(sources)

    # Create metadata
    meta_data = ttypes.MetaData(
        outputNamespace=output_namespace,
        team=team,
        tags=tags,
        tableProperties=table_properties,
        version=str(version),
    )

    # Create key schema if key_fields are provided
    key_schema = None
    if key_fields:
        schema_name = "modeltransform_key_schema"
        key_schema = DataType.STRUCT(schema_name, *key_fields)

    model_transforms = ttypes.ModelTransforms(
        sources=normalized_sources,
        models=models,
        passthroughFields=passthrough_fields,
        metaData=meta_data,
        keySchema=key_schema,
    )

    # Add the table property for output table name generation
    model_transforms.__class__.table = property(
        lambda self: _get_model_transforms_output_table_name(self, full_name=True)
    )

    return model_transforms
