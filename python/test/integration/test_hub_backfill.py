"""Cloud-agnostic hub backfill integration tests.

Exercises: compile -> upload diffs -> backfill -> poll workflow to success.
Replaces the former test_gcp_hub_quickstart.py and test_aws_hub_quickstart.py.
"""

import pytest
from click.testing import CliRunner

from .helpers.cli import compile_configs, submit_backfill
from .helpers.workflow import poll_workflow

# Demo join conf paths differ across clouds (variable names / versions vary).
DEMO_DERIVATIONS = {
    "gcp": "compiled/joins/gcp/demo.derivations_v1__2",
    "aws": "compiled/joins/aws/demo.derivations_v1__2",
    "azure": "compiled/joins/azure/demo.derivations_v3",
}


@pytest.mark.integration
def test_backfill_no_data(confs, chronon_root, hub_url, cloud):
    """Backfill with dates that have no input data should result in a failed workflow."""
    runner = CliRunner()
    compile_configs(runner, chronon_root)

    workflow_id = submit_backfill(
        runner, chronon_root, hub_url,
        confs[DEMO_DERIVATIONS[cloud]], "1969-01-01", "1969-01-01",
    )
    with pytest.raises(RuntimeError, match="ended with status FAILED"):
        poll_workflow(hub_url, workflow_id, timeout=1800, interval=45)


@pytest.mark.integration
def test_staging_query_backfill_multiday(confs, chronon_root, hub_url, cloud):
    """Multi-day backfill of a staging query exercises multi-step allocation."""
    runner = CliRunner()
    compile_configs(runner, chronon_root)

    workflow_id = submit_backfill(
        runner, chronon_root, hub_url,
        confs[f"compiled/staging_queries/{cloud}/exports.user_activities__0"],
        "2026-03-01", "2026-03-03",
    )
    poll_workflow(hub_url, workflow_id, timeout=1800, interval=45)
