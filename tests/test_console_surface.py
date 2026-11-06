"""Workflow status and retry safety of the model-work SDK wrappers."""

from __future__ import annotations

import asyncio

import httpx
import pytest

from lbb import AsyncLbbClient, LbbClient, LbbError, models

STATUS = {
    "active_runs": 2,
    "archive_error": False,
    "archive_lag_ms": 0,
    "archived_sequence": 7,
    "durability": "object_storage",
    "enabled": True,
    "local_sequence": 7,
    "pending_archive_bytes": 0,
    "unarchived_events": 0,
}


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("epoch", [None, "18446744073709551615"])
def test_workflow_status_returns_a_model_and_preserves_the_epoch(
    asynchronous: bool, epoch: str | None
) -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json=STATUS)

    transport = httpx.MockTransport(handler)

    async def run() -> models.WorkflowStatusResponse:
        async with AsyncLbbClient("http://h", graph="main", transport=transport) as client:
            return await client.workflows.status(expected_graph_epoch=epoch)

    if asynchronous:
        result = asyncio.run(run())
    else:
        with LbbClient("http://h", graph="main", transport=transport) as client:
            result = client.workflows.status(expected_graph_epoch=epoch)

    assert isinstance(result, models.WorkflowStatusResponse)
    assert result.active_runs == 2
    assert len(seen) == 1
    assert seen[0].method == "GET"
    assert seen[0].url.path == "/v1/workflows/status"
    assert seen[0].url.params["graph"] == "main"
    assert seen[0].url.params.get("expected_graph_epoch") == epoch


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("route", ["fit-text", "calibration"])
@pytest.mark.parametrize("failure", ["lost-response", "server-error", "rate-limited"])
def test_model_work_retries_only_rate_limit_responses(
    asynchronous: bool, route: str, failure: str
) -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if failure == "lost-response":
            raise httpx.ReadError("response connection lost", request=request)
        status = 429 if failure == "rate-limited" and len(seen) == 1 else 503
        return httpx.Response(status, json={"error": {"message": "busy"}})

    transport = httpx.MockTransport(handler)
    error = httpx.ReadError if failure == "lost-response" else LbbError

    async def run() -> None:
        async with AsyncLbbClient(
            "http://h", graph="main", transport=transport, max_retries=2, retry_delay=0
        ) as client:
            with pytest.raises(error):
                if route == "fit-text":
                    await client.ontology.fit_text({"text": "one text"})
                else:
                    await client.models.calibration.start(job="route")

    if asynchronous:
        asyncio.run(run())
    else:
        with LbbClient(
            "http://h", graph="main", transport=transport, max_retries=2, retry_delay=0
        ) as client:
            with pytest.raises(error):
                if route == "fit-text":
                    client.ontology.fit_text({"text": "one text"})
                else:
                    client.models.calibration.start(job="route")

    assert len(seen) == (2 if failure == "rate-limited" else 1)
    expected_path = "/v1/ontology/fit-text" if route == "fit-text" else "/v1/models/calibration"
    assert all(request.method == "POST" and request.url.path == expected_path for request in seen)
