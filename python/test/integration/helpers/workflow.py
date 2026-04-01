"""Workflow polling utilities for integration tests."""

import os
import time

import requests

WORKFLOW_STATUS = {
    0: "UNKNOWN",
    1: "WAITING",
    2: "RUNNING",
    3: "SUCCEEDED",
    4: "FAILED",
    5: "CANCELLED",
    6: "CANCEL_PENDING",
}

TERMINAL_STATUSES = {"SUCCEEDED", "FAILED", "CANCELLED", "UNKNOWN"}


def _get_auth_headers() -> dict:
    headers = {}
    zipline_token = os.environ.get("ZIPLINE_TOKEN")
    if zipline_token:
        from ai.chronon.repo.token_exchange import resolve_token

        auth_url = os.environ.get("ZIPLINE_AUTH_URL")
        jwt = resolve_token(zipline_token, auth_url)
        headers["Authorization"] = f"Bearer {jwt}"
    else:
        token = os.environ.get("GCP_ID_TOKEN") or os.environ.get("AWS_ID_TOKEN")
        if token:
            headers["Authorization"] = f"Bearer {token}"
    return headers


def poll_workflow(hub_url: str, workflow_id: str, timeout: int = 900, interval: int = 30) -> dict:
    """Poll ``GET /workflow/v2/{id}`` until the workflow reaches a terminal state.

    Raises ``RuntimeError`` if the terminal status is not ``SUCCEEDED``.
    """
    return poll_workflow_until(
        hub_url, workflow_id, target_statuses={"SUCCEEDED"}, timeout=timeout, interval=interval
    )


def poll_workflow_until(
    hub_url: str,
    workflow_id: str,
    target_statuses: set[str],
    timeout: int = 900,
    interval: int = 30,
) -> dict:
    """Poll until the workflow reaches one of *target_statuses*.

    If a terminal status is reached that is **not** in *target_statuses*,
    raises ``RuntimeError``.  If the timeout expires, raises ``AssertionError``.
    """
    url = f"{hub_url}/workflow/v2/{workflow_id}"

    deadline = time.time() + timeout
    while True:
        headers = _get_auth_headers()
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        data = resp.json()

        workflow = data.get("workflow", data)
        status_code = workflow.get("status")
        status_name = WORKFLOW_STATUS.get(status_code, "INVALID")
        print(f"Workflow {workflow_id} status: {status_code} ({status_name})")

        if status_name in target_statuses:
            return data

        if status_name in TERMINAL_STATUSES and status_name not in target_statuses:
            raise RuntimeError(
                f"Workflow {workflow_id} ended with status {status_name}"
            )

        if time.time() + interval > deadline:
            raise AssertionError(
                f"Workflow {workflow_id} did not reach {target_statuses} within {timeout}s "
                f"(last status: {status_name})"
            )
        time.sleep(interval)
