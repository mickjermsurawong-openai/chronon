import os
import json
from unittest.mock import MagicMock, patch

from click.testing import CliRunner
from gen_thrift.api.ttypes import (
    EventSource,
    GroupBy,
    Join,
    JoinPart,
    JoinSource,
    MetaData,
    ModelTransforms,
    Source,
    Team,
)
from gen_thrift.common.ttypes import ExecutionInfo

from ai.chronon.cli.compile import parse_configs
from ai.chronon.cli.compile.compile_context import CompileContext
from ai.chronon.cli.compile.parse_teams import update_metadata
from ai.chronon.repo.compile import __compile, compile


def test_compile(repo):
    import sys
    sys.path.append(repo)
    results = __compile(chronon_root=repo, ignore_python_errors=True)
    assert len(results) != 0


def test_compile_gcp_resources(gcp_resources):
    """Test compilation of GCP resource examples."""
    import sys
    sys.path.insert(0, gcp_resources)
    results = __compile(chronon_root=gcp_resources, ignore_python_errors=True)
    assert len(results) != 0

def test_compile_aws_resources(aws_resources):
    """Test compilation of AWS resource examples."""
    import sys
    sys.path.insert(0, aws_resources)
    results = __compile(chronon_root=aws_resources, ignore_python_errors=True)
    assert len(results) != 0

def test_compile_azure_resources(azure_resources):
    """Test compilation of Azure resource examples."""
    import sys
    sys.path.insert(0, azure_resources)
    results = __compile(chronon_root=azure_resources, ignore_python_errors=True)
    assert len(results) != 0

def test_parse_configs_relative_source_file():
    """Test that sourceFile is stored as a path relative to chronon_root."""
    # Setup
    test_root = "/fake/root/path"
    test_file_path = "/fake/root/path/group_bys/team/test_group_by.py"
    test_input_dir = os.path.join(test_root, "group_bys")

    # Create a properly initialized GroupBy object with MetaData
    mock_obj = GroupBy()
    mock_obj.metaData = MetaData()

    # Create mock context
    mock_compile_context = MagicMock(spec=CompileContext)
    mock_compile_context.chronon_root = test_root
    mock_compile_context.teams_dict = {}
    mock_compile_context.validator = MagicMock()
    mock_compile_context.validator.validate_obj.return_value = []
    mock_compile_context.compile_status = MagicMock()

    # Configure mocks
    with patch('ai.chronon.cli.compile.parse_configs.from_file') as mock_from_file, \
         patch('ai.chronon.cli.compile.serializer.thrift_simple_json') as mock_serialize, \
         patch('glob.glob', return_value=[test_file_path]), \
         patch('ai.chronon.cli.compile.parse_teams.update_metadata'), \
         patch('ai.chronon.cli.compile.parse_configs.populate_column_hashes'):

        # Configure mock return values
        mock_from_file.return_value = {"team.test_group_by.test_var": mock_obj}
        mock_serialize.return_value = "{}"

        # Call the function being tested
        results = parse_configs.from_folder(GroupBy, test_input_dir, mock_compile_context)

    # Assertions
    assert len(results) == 1
    assert results[0].obj is not None
    assert hasattr(results[0].obj, 'metaData')
    assert results[0].obj.metaData is not None

    # The sourceFile should be a relative path from chronon_root
    expected_relative_path = "group_bys/team/test_group_by.py"
    assert results[0].obj.metaData.sourceFile == expected_relative_path
    assert not results[0].obj.metaData.sourceFile.startswith("/")  # Should be relative, not absolute


def test_compile_with_json_format(canary):
    """Test that compile command with --format json returns valid JSON output."""
    import sys
    sys.path.append(canary)

    runner = CliRunner()
    result = runner.invoke(compile, [
        '--chronon-root', canary,
        '-f', 'json',
        '--ignore-python-errors',
        '--force',
    ])

    # Check that the command executed successfully
    assert result.exit_code == 0, f"Command failed with output: {result.output}"

    # Verify that the output is valid JSON (should be clean JSON with no tracebacks)
    try:
        output_json = json.loads(result.output)
    except json.JSONDecodeError as e:
        assert False, f"Output is not valid JSON: {result.output}\nError: {e}"

    # Verify the JSON structure contains expected fields
    assert "status" in output_json, f"Output missing 'status' field: {output_json}"
    assert output_json["status"] == "success", f"Expected status 'success', got: {output_json['status']}"

    assert "results" in output_json, f"Output missing 'results' field: {output_json}"
    assert isinstance(output_json["results"], dict), f"'results' should be a dict, got: {type(output_json['results'])}"


def _make_team_dict(namespace="test_namespace"):
    """Helper to create a minimal team dict for update_metadata tests."""
    team = Team(outputNamespace=namespace)
    team.executionInfo = ExecutionInfo()
    default_team = Team(outputNamespace="default_ns")
    default_team.executionInfo = ExecutionInfo()
    return {"test_team": team, "default": default_team}


def test_update_metadata_propagates_namespace_to_groupby_join_source():
    """When a GroupBy uses a JoinSource whose Join has no outputNamespace,
    update_metadata should propagate the GroupBy's namespace to the embedded Join."""
    inner_join = Join(
        metaData=MetaData(name="test_team.parent_join", team="test_team"),
        left=Source(events=EventSource(table="some_table")),
    )
    assert inner_join.metaData.outputNamespace is None

    group_by = GroupBy(
        sources=[Source(joinSource=JoinSource(join=inner_join))],
        keyColumns=["user_id"],
        metaData=MetaData(name="test_team.my_gb", team="test_team"),
    )

    update_metadata(group_by, _make_team_dict("gb_namespace"))

    assert group_by.metaData.outputNamespace == "gb_namespace"
    assert inner_join.metaData.outputNamespace == "gb_namespace"


def test_update_metadata_does_not_overwrite_existing_join_source_namespace():
    """If the JoinSource's Join already has an outputNamespace, don't overwrite it."""
    inner_join = Join(
        metaData=MetaData(name="test_team.parent_join", team="test_team", outputNamespace="explicit_ns"),
        left=Source(events=EventSource(table="some_table")),
    )

    group_by = GroupBy(
        sources=[Source(joinSource=JoinSource(join=inner_join))],
        keyColumns=["user_id"],
        metaData=MetaData(name="test_team.my_gb", team="test_team"),
    )

    update_metadata(group_by, _make_team_dict("gb_namespace"))

    assert inner_join.metaData.outputNamespace == "explicit_ns"


def test_update_metadata_propagates_namespace_to_model_transforms_join_source():
    """Same propagation should work for ModelTransforms with JoinSource."""
    inner_join = Join(
        metaData=MetaData(name="test_team.parent_join", team="test_team"),
        left=Source(events=EventSource(table="some_table")),
    )

    mt = ModelTransforms(
        sources=[Source(joinSource=JoinSource(join=inner_join))],
        metaData=MetaData(name="test_team.my_mt", team="test_team"),
    )

    update_metadata(mt, _make_team_dict("mt_namespace"))

    assert mt.metaData.outputNamespace == "mt_namespace"
    assert inner_join.metaData.outputNamespace == "mt_namespace"
