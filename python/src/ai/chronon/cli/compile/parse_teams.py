import importlib
import importlib.util
import os
import sys
from copy import deepcopy
from enum import Enum
from typing import Any, Dict, Optional, Union

from ai.chronon.cli.logger import get_logger
from ai.chronon.cli.theme import console
from gen_thrift.api.ttypes import GroupBy, Join, JoinPart, MetaData, Model, ModelTransforms, Team
from gen_thrift.common.ttypes import (
    ClusterConfigProperties,
    ConfigProperties,
    EnvironmentVariables,
    ExecutionInfo,
)

logger = get_logger()

_DEFAULT_CONF_TEAM = "default"


def import_module_from_file(file_path):
    # Get the module name from the file path (without .py extension)
    module_name = file_path.split("/")[-1].replace(".py", "")

    # Create the module spec
    spec = importlib.util.spec_from_file_location(module_name, file_path)

    # Create the module based on the spec
    module = importlib.util.module_from_spec(spec)

    # Add the module to sys.modules
    sys.modules[module_name] = module

    # Execute the module
    spec.loader.exec_module(module)

    return module


_DEPRECATED_CATALOG = "DelegatingBigQueryMetastoreCatalog"
_REPLACEMENT_CATALOG = "org.apache.iceberg.spark.SparkCatalog"


def _check_deprecated_catalog(team_name: str, conf):
    """Check if a team's config references the deprecated DelegatingBigQueryMetastoreCatalog."""
    if conf is None:
        return
    if conf.common:
        for key, value in conf.common.items():
            val_str = value if isinstance(value, str) else str(value or "")
            if _DEPRECATED_CATALOG in val_str:
                raise ValueError(
                    f"Team '{team_name}' uses deprecated {_DEPRECATED_CATALOG} in conf key '{key}'. "
                    f"Please migrate to {_REPLACEMENT_CATALOG} with BigQueryMetastoreCatalog as the catalog-impl."
                )
    mode_configs = getattr(conf, "modeConfigs", None) or getattr(conf, "modeClusterConfigs", None)
    if mode_configs:
        for mode, mode_map in mode_configs.items():
            if mode_map:
                for key, value in mode_map.items():
                    val_str = value if isinstance(value, str) else str(value or "")
                    if _DEPRECATED_CATALOG in val_str:
                        raise ValueError(
                            f"Team '{team_name}' uses deprecated {_DEPRECATED_CATALOG} in conf key '{key}' "
                            f"(mode: {mode}). Please migrate to {_REPLACEMENT_CATALOG} with BigQueryMetastoreCatalog as the catalog-impl."
                        )


def load_teams(conf_root: str, print: bool = True) -> Dict[str, Team]:
    teams_file = os.path.join(conf_root, "teams.py")

    assert os.path.exists(teams_file), (
        f"Team config file: {teams_file} not found. You might be running this from the wrong directory."
    )

    team_module = import_module_from_file(teams_file)

    assert team_module is not None, (
        f"Team config file {teams_file} is not on the PYTHONPATH. You might need to add the your config "
        f"directory to the PYTHONPATH."
    )

    team_dict = {}

    if print:
        console.print(f"Pulling configuration from [cyan italic]{teams_file}[/cyan italic]")

    for name, obj in team_module.__dict__.items():
        if isinstance(obj, Team):
            obj.name = name
            _check_deprecated_catalog(name, obj.conf)
            team_dict[name] = obj

    return team_dict


def update_metadata(obj: Any, team_dict: Dict[str, Team]):
    assert obj is not None, "Cannot update metadata None object"

    metadata = obj.metaData

    assert obj.metaData is not None, "Cannot update empty metadata"

    name = obj.metaData.name
    team = obj.metaData.team

    assert team is not None, (
        f"Team name is required in metadata for {name}. This usually set by compiler. Internal error."
    )

    assert team in team_dict, f"Team '{team}' not found in teams.py. Please add an entry 🙏"

    assert _DEFAULT_CONF_TEAM in team_dict, (
        f"'{_DEFAULT_CONF_TEAM}' team not found in teams.py, please add an entry 🙏."
    )

    if not metadata.outputNamespace:
        metadata.outputNamespace = team_dict[team].outputNamespace

    namespace = metadata.outputNamespace

    def _set_part_metadata(part: Union[JoinPart, Model]):
        if part is None:
            return
        if not part.metaData:
            part.metaData = MetaData()
        if not part.metaData.outputNamespace:
            part.metaData.outputNamespace = namespace
        if not part.metaData.team:
            part.metaData.team = team
        merge_team_execution_info(part.metaData, team_dict, part.metaData.team)

    def _propagate_namespace_to_join_sources(sources):
        for src in sources or []:
            if src.joinSource and src.joinSource.join and src.joinSource.join.metaData:
                if not src.joinSource.join.metaData.outputNamespace:
                    src.joinSource.join.metaData.outputNamespace = namespace

    if isinstance(obj, Join):
        for jp in obj.joinParts or []:
            jp.useLongNames = obj.useLongNames
            _set_part_metadata(jp.groupBy)
    elif isinstance(obj, ModelTransforms):
        for m in obj.models or []:
            _set_part_metadata(m)
        _propagate_namespace_to_join_sources(obj.sources)
    elif isinstance(obj, GroupBy):
        _propagate_namespace_to_join_sources(obj.sources)

    if metadata.executionInfo is None:
        metadata.executionInfo = ExecutionInfo()

    merge_team_execution_info(metadata, team_dict, team)


def merge_team_execution_info(metadata: MetaData, team_dict: Dict[str, Team], team_name: str):
    default_team = team_dict.get(_DEFAULT_CONF_TEAM)
    if not metadata.executionInfo:
        metadata.executionInfo = ExecutionInfo()

    metadata.executionInfo.env = _merge_mode_maps(
        default_team.env if default_team else {},
        team_dict[team_name].env,
        metadata.executionInfo.env,
        env_or_config_attribute=EnvOrConfigAttribute.ENV,
    )

    metadata.executionInfo.conf = _merge_mode_maps(
        default_team.conf if default_team else {},
        team_dict[team_name].conf,
        metadata.executionInfo.conf,
        env_or_config_attribute=EnvOrConfigAttribute.CONFIG,
    )

    metadata.executionInfo.clusterConf = _merge_mode_maps(
        default_team.clusterConf if default_team else {},
        team_dict[team_name].clusterConf,
        metadata.executionInfo.clusterConf,
        env_or_config_attribute=EnvOrConfigAttribute.CLUSTER_CONFIG,
    )


def _merge_maps(*maps: Optional[Dict[str, str]]):
    """
    Merges multiple maps into one - with the later maps overriding the earlier ones.
    """

    result = {}

    for m in maps:
        if m is None:
            continue

        for key, value in m.items():
            result[key] = value

    return result


class EnvOrConfigAttribute(str, Enum):
    ENV = "modeEnvironments"
    CONFIG = "modeConfigs"
    CLUSTER_CONFIG = "modeClusterConfigs"


def _merge_mode_maps(
    *mode_maps: Optional[Union[EnvironmentVariables, ConfigProperties, ClusterConfigProperties]],
    env_or_config_attribute: EnvOrConfigAttribute,
):
    """
    Merges multiple environment variables into one - with the later maps overriding the earlier ones.
    """

    # Merge `common` to each individual mode map. Creates a new map
    def push_common_to_modes(
        mode_map: Union[EnvironmentVariables, ConfigProperties], mode_key: EnvOrConfigAttribute
    ):
        final_mode_map = deepcopy(mode_map)
        common = final_mode_map.common
        modes = getattr(final_mode_map, mode_key)

        if modes:
            for _ in modes:
                modes[_] = _merge_maps(common, modes[_])

        return final_mode_map

    filtered_mode_maps = [m for m in mode_maps if m]

    # Initialize the result with the first mode map
    result = None

    if len(filtered_mode_maps) >= 1:
        result = push_common_to_modes(filtered_mode_maps[0], env_or_config_attribute)

    # Merge each new mode map into the result
    for m in filtered_mode_maps[1:]:
        # We want to prepare the individual modes with `common` in incoming_mode_map
        incoming_mode_map = push_common_to_modes(m, env_or_config_attribute)

        # create new common
        incoming_common = incoming_mode_map.common
        new_common = _merge_maps(result.common, incoming_common)
        result.common = new_common

        current_modes = getattr(result, env_or_config_attribute)
        incoming_modes = getattr(incoming_mode_map, env_or_config_attribute)

        current_modes_keys = list(current_modes.keys()) if current_modes else []
        incoming_modes_keys = list(incoming_modes.keys()) if incoming_modes else []

        all_modes_keys = list(set(current_modes_keys + incoming_modes_keys))

        for mode in all_modes_keys:
            current_mode = current_modes.get(mode, {}) if current_modes else {}

            # if the incoming_mode is not found, we NEED to default to incoming_common
            incoming_mode = (
                incoming_modes.get(mode, incoming_common) if incoming_modes else incoming_common
            )

            # first to last with later ones overriding the earlier ones
            # common -> current mode level -> incoming mode level

            new_mode = _merge_maps(new_common, current_mode, incoming_mode)

            if current_modes is None:
                current_modes = {}
                setattr(result, env_or_config_attribute, current_modes)

            current_modes[mode] = new_mode

    return result
