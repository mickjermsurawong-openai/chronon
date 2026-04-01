"""Fixtures for Hub/Orchestrator integration tests.

Usage::

    # Run all integration tests (using ZIPLINE_TOKEN — service principal):
    ZIPLINE_TOKEN=<session_token> \\
        ZIPLINE_AUTH_URL=https://canary.zipline.ai \\
        HUB_URL=https://canary-orch.zipline.ai \\
        ./mill python.test_integration

    # Run all integration tests (using ZIPLINE_TOKEN — quick JWT):
    ZIPLINE_TOKEN=$(zipline auth get-access-token) \\
        HUB_URL=https://canary-orch.zipline.ai \\
        ./mill python.test_integration

    # Run all integration tests (using cloud IAM token):
    GCP_ID_TOKEN=$(gcloud auth print-identity-token) \\
        HUB_URL=https://canary-orch.zipline.ai \\
        ./mill python.test_integration

    # Run a specific test:
    PYTEST_ADDOPTS="-k test_backfill_no_data" \\
        ZIPLINE_TOKEN=<token> \\
        ZIPLINE_AUTH_URL=https://canary.zipline.ai \\
        HUB_URL=https://canary-orch.zipline.ai \\
        ./mill python.test_integration
"""

import logging
import os
import random
import string

import pytest

from .helpers.cleanup import AWSCleanup, AzureCleanup, GCPCleanup
from .helpers.templates import cleanup_test_configs, generate_test_configs, get_confs

logger = logging.getLogger(__name__)

def pytest_addoption(parser):
    parser.addoption(
        "--hub-url",
        default=os.environ.get("HUB_URL", "http://localhost:3903"),
        help="Hub base URL (env: HUB_URL)",
    )
    parser.addoption(
        "--cloud",
        default=os.environ.get("CLOUD", "gcp"),
        choices=("gcp", "aws", "azure"),
        help="Cloud provider (env: CLOUD)",
    )


# ---------------------------------------------------------------------------
# Session-scoped fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def hub_url(request) -> str:
    return request.config.getoption("--hub-url")


@pytest.fixture(scope="session")
def cloud(request) -> str:
    return request.config.getoption("--cloud")


@pytest.fixture(scope="session")
def version() -> str:
    """Zipline version for ``zipline run`` commands (env: VERSION)."""
    v = os.environ.get("VERSION")
    assert v, "VERSION env var is required for quickstart tests"
    return v


@pytest.fixture(scope="session")
def chronon_root() -> str:
    """Absolute path to the canary config root."""
    return os.path.join(os.path.dirname(os.path.dirname(__file__)), "canary")


# ---------------------------------------------------------------------------
# Per-test fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def chronon_env(monkeypatch, chronon_root, cloud):
    """Set up the environment variables and sys.path needed by canary configs."""
    monkeypatch.setenv("PYTHONPATH", chronon_root)
    monkeypatch.setenv("CUSTOMER_ID", "canary")
    monkeypatch.syspath_prepend(chronon_root)


def _random_suffix(length: int = 8) -> str:
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=length))


@pytest.fixture
def confs(test_id, cloud) -> dict[str, str]:
    """Compiled conf paths keyed by logical name, resolved with the current test_id."""
    return get_confs(cloud, test_id)


@pytest.fixture
def test_id(request, chronon_root, cloud):
    """Generate a unique test_id, render templates, and clean up afterwards."""
    tid = _random_suffix()
    logger.info("test_id=%s for %s", tid, request.node.name)

    generate_test_configs(tid, chronon_root, cloud=cloud)

    yield tid

    # --- teardown ---
    cleanup_test_configs(tid, chronon_root, cloud=cloud)

    try:
        if cloud == "gcp":
            project = os.environ.get("GCP_BQ_PROJECT", "canary-443022")
            dataset = os.environ.get("GCP_BQ_DATASET", "data")
            GCPCleanup(project, dataset).cleanup_tables(tid)
        elif cloud == "aws":
            database = os.environ.get("AWS_GLUE_DATABASE", "default")
            AWSCleanup(database).cleanup_tables(tid)
        elif cloud == "azure":
            catalog = os.environ.get("AZURE_CATALOG", "default")
            AzureCleanup(catalog).cleanup_tables(tid)
    except Exception:
        logger.exception("Table cleanup failed for test_id=%s", tid)
