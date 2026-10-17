"""Task-scoped reads keep the data/control credentials separate in both transports."""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any

import httpx
import pytest

from lbb import AsyncLbbClient, LbbClient, WorkflowExecutionError

EPOCH = "18446744073709551615"
REFRESH = "/v1/workflows/turns/execution-credential"
QUERY = "/v1/query/sparql-text"
ANSWER = {"results": '{"head":{},"boolean":true}'}


def credential(**changes: Any) -> dict[str, Any]:
    issued = time.time_ns() // 1_000_000
    return {
        "token": "lbb_wke_execution.signature",
        "issued_at_ms": issued,
        "expires_at_ms": issued + 60000,
        "graph_id": "project",
        "graph_epoch": EPOCH,
        "workflow_id": "agent",
        "turn": 1,
        "attempt": 1,
        "run_id": "run-exact",
        "operations": ["sparql_read"],
        **changes,
    }


def task(issued: Any = None) -> dict[str, Any]:
    return {
        "execution_credential": issued,
        "token": "secret-lease",
        "turn": {
            "workflow_id": "agent",
            "number": 1,
            "attempt": 1,
            "run": {"id": "run-exact"},
        },
    }


@pytest.mark.parametrize("asynchronous", [False, True])
def test_task_read_refreshes_only_on_control_and_forces_exact_epoch(
    asynchronous: bool,
) -> None:
    seen: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json=credential() if request.url.path == REFRESH else ANSWER)

    stale = task(credential())
    arguments = {
        "api_key": "environment-admin",
        "graph": "project",
        "environment": "prod",
        "transport": httpx.MockTransport(handle),
    }
    if asynchronous:

        async def run() -> None:
            async with AsyncLbbClient("https://example.test", **arguments) as client:
                reader = client.workflow_turn_reader(stale)
                assert await reader.sparql("ASK {}") == ANSWER
                reader.close()
                with pytest.raises(WorkflowExecutionError):
                    await reader.sparql("ASK {}")

        asyncio.run(run())
    else:
        with LbbClient("https://example.test", **arguments) as client:
            reader = client.workflow_turn_reader(stale)
            assert reader.sparql("ASK {}") == ANSWER
            reader.close()
            with pytest.raises(WorkflowExecutionError):
                reader.sparql("ASK {}")
    assert [request.url.path for request in seen] == [REFRESH, QUERY]
    assert seen[0].headers["authorization"] == "Bearer environment-admin"
    assert seen[1].headers["authorization"] == "Bearer lbb_wke_execution.signature"
    assert json.loads(seen[0].content) == {
        "workflow_id": "agent",
        "turn": 1,
        "token": "secret-lease",
    }
    assert json.loads(seen[1].content) == {"query": "ASK {}"}
    for request in seen:
        assert dict(request.url.params) == {
            "graph": "project",
            "stack": "prod",
            "expected_graph_epoch": EPOCH,
        }


@pytest.mark.parametrize("asynchronous", [False, True])
def test_invalid_task_or_override_never_sends_admin_query(asynchronous: bool) -> None:
    seen: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json=ANSWER)

    invalid = [
        task(),
        task(credential(token="environment-admin")),
        task(credential(issued_at_ms=-1)),
        task(credential(issued_at_ms=1.5)),
        task(credential(issued_at_ms=1000, expires_at_ms=61001)),
        task(credential(issued_at_ms=1000, expires_at_ms=1000)),
        task(credential(graph_epoch="01")),
        task(credential(graph_epoch="18446744073709551616")),
        task(credential(operations=["write"])),
        task(credential(attempt=2)),
        task(credential(run_id="other")),
    ]
    no_run = task(credential())
    no_run["turn"].pop("run")
    invalid.append(no_run)

    async def async_run() -> None:
        async with AsyncLbbClient(
            "https://example.test",
            api_key="admin",
            transport=httpx.MockTransport(handle),
        ) as client:
            for value in invalid:
                with pytest.raises(WorkflowExecutionError):
                    client.workflow_turn_reader(value)
            reader = client.workflow_turn_reader(task(credential()))
            for key in [
                "request",
                "route",
                "reason",
                "graph",
                "expected_graph_epoch",
                "headers",
            ]:
                with pytest.raises(WorkflowExecutionError):
                    await reader.sparql({"query": "ASK {}", key: "override"})
            with pytest.raises(TypeError):
                await reader.sparql("ASK {}", headers={"authorization": "admin"})  # type: ignore[call-arg]

    if asynchronous:
        asyncio.run(async_run())
    else:
        with LbbClient(
            "https://example.test",
            api_key="admin",
            transport=httpx.MockTransport(handle),
        ) as client:
            for value in invalid:
                with pytest.raises(WorkflowExecutionError):
                    client.workflow_turn_reader(value)
            reader = client.workflow_turn_reader(task(credential()))
            for key in [
                "request",
                "route",
                "reason",
                "graph",
                "expected_graph_epoch",
                "headers",
            ]:
                with pytest.raises(WorkflowExecutionError):
                    reader.sparql({"query": "ASK {}", key: "override"})
            with pytest.raises(TypeError):
                reader.sparql("ASK {}", headers={"authorization": "admin"})  # type: ignore[call-arg]
    assert seen == []


@pytest.mark.parametrize("asynchronous", [False, True])
def test_refresh_cannot_change_identity_or_turn_into_admin_bearer(
    asynchronous: bool,
) -> None:
    for key, value in {
        "graph_id": "other",
        "graph_epoch": "7",
        "workflow_id": "other",
        "turn": 2,
        "attempt": 2,
        "run_id": "other",
        "token": "environment-admin",
    }.items():
        seen: list[httpx.Request] = []

        def handle(
            request: httpx.Request,
            *,
            seen: list[httpx.Request] = seen,
            key: str = key,
            value: Any = value,
        ) -> httpx.Response:
            seen.append(request)
            return httpx.Response(200, json=credential(**{key: value}))

        if asynchronous:

            async def run() -> None:
                async with AsyncLbbClient(
                    "https://example.test",
                    api_key="admin",
                    transport=httpx.MockTransport(handle),
                ) as client:
                    with pytest.raises(WorkflowExecutionError):
                        await client.workflow_turn_reader(task(credential())).sparql("ASK {}")

            asyncio.run(run())
        else:
            with LbbClient(
                "https://example.test",
                api_key="admin",
                transport=httpx.MockTransport(handle),
            ) as client:
                with pytest.raises(WorkflowExecutionError):
                    client.workflow_turn_reader(task(credential())).sparql("ASK {}")
        assert len(seen) == 1 and seen[0].url.path == REFRESH
        assert seen[0].headers["authorization"] == "Bearer admin"


@pytest.mark.parametrize("asynchronous", [False, True])
def test_batch_rejection_has_no_automatic_retry_or_credential_fallback(
    asynchronous: bool,
) -> None:
    seen: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if request.url.path == REFRESH:
            return httpx.Response(200, json=credential())
        return httpx.Response(429, json={"error": {"code": "batch_database_busy"}})

    if asynchronous:

        async def run() -> None:
            async with AsyncLbbClient(
                "https://example.test",
                api_key="admin",
                max_retries=5,
                retry_delay=0,
                transport=httpx.MockTransport(handle),
            ) as client:
                with pytest.raises(Exception) as error:
                    await client.workflow_turn_reader(task(credential())).sparql("ASK {}")
                assert getattr(error.value, "status_code", None) == 429

        asyncio.run(run())
    else:
        with LbbClient(
            "https://example.test",
            api_key="admin",
            max_retries=5,
            retry_delay=0,
            transport=httpx.MockTransport(handle),
        ) as client:
            with pytest.raises(Exception) as error:
                client.workflow_turn_reader(task(credential())).sparql("ASK {}")
            assert getattr(error.value, "status_code", None) == 429
    assert len(seen) == 2
    assert seen[0].headers["authorization"] == "Bearer admin"
    assert seen[1].headers["authorization"] == "Bearer lbb_wke_execution.signature"


@pytest.mark.parametrize("close_reader", [False, True])
def test_async_caller_or_heartbeat_owner_cancels_active_request(
    close_reader: bool,
) -> None:
    async def run() -> None:
        entered = asyncio.Event()
        stopped = asyncio.Event()

        async def handle(request: httpx.Request) -> httpx.Response:
            if request.url.path == REFRESH:
                return httpx.Response(200, json=credential())
            assert request.headers["authorization"] == "Bearer lbb_wke_execution.signature"
            entered.set()
            try:
                await asyncio.Event().wait()
            finally:
                stopped.set()
            return httpx.Response(200, json=ANSWER)

        async with AsyncLbbClient(
            "https://example.test",
            api_key="admin",
            transport=httpx.MockTransport(handle),
        ) as client:
            reader = client.workflow_turn_reader(task(credential()))
            pending = asyncio.create_task(reader.sparql("ASK {}"))
            await asyncio.wait_for(entered.wait(), 1)
            if close_reader:
                reader.close()
            else:
                pending.cancel()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(pending, 1)
            await asyncio.wait_for(stopped.wait(), 1)

    asyncio.run(run())


@pytest.mark.parametrize("asynchronous", [False, True])
def test_refresh_uses_read_budget_and_monotonic_lifetime_despite_wall_clock_skew(
    asynchronous: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from lbb import _workflow_execution as execution

    now = [100.0]
    monkeypatch.setattr(execution, "_monotonic_ms", lambda: now[0])
    wall = [9_000_000_000_000]
    monkeypatch.setattr(time, "time_ns", lambda: wall[0] * 1_000_000)
    seen: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(
            200,
            json=(
                credential(issued_at_ms=1_000_000, expires_at_ms=1_060_000)
                if request.url.path == REFRESH
                else ANSWER
            ),
        )

    def count() -> int:
        return sum(request.url.path == REFRESH for request in seen)

    async def run_async() -> None:
        async with AsyncLbbClient(
            "https://example.test", transport=httpx.MockTransport(handle)
        ) as client:
            reader = client.workflow_turn_reader(task(credential()))
            await reader.sparql("ASK {}", timeout=10)
            assert count() == 1
            wall[0] = 1
            now[0] += 49500
            await reader.sparql("ASK {}", timeout=1)
            assert count() == 1
            await reader.sparql("ASK {}", timeout=10)
            assert count() == 2
            now[0] += 29500
            await reader.sparql("ASK {}", timeout=120)
            assert count() == 3

    if asynchronous:
        asyncio.run(run_async())
    else:
        with LbbClient("https://example.test", transport=httpx.MockTransport(handle)) as client:
            reader = client.workflow_turn_reader(task(credential()))
            reader.sparql("ASK {}", timeout=10)
            assert count() == 1
            wall[0] = 1
            now[0] += 49500
            reader.sparql("ASK {}", timeout=1)
            assert count() == 1
            reader.sparql("ASK {}", timeout=10)
            assert count() == 2
            now[0] += 29500
            reader.sparql("ASK {}", timeout=120)
            assert count() == 3
    queries = [request for request in seen if request.url.path == QUERY]
    assert len(queries) == 4
    assert queries[-1].extensions["timeout"]["read"] == 30


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("missing", [False, True])
def test_refresh_delay_or_missing_lifetime_is_retryable_without_query(
    asynchronous: bool,
    missing: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from lbb import _workflow_execution as execution

    now = [100.0]
    monkeypatch.setattr(execution, "_monotonic_ms", lambda: now[0])
    seen: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        value = credential(issued_at_ms=1000, expires_at_ms=1400)
        if missing:
            value.pop("issued_at_ms")
        else:
            now[0] += 500
        return httpx.Response(200, json=value)

    async def run_async() -> None:
        async with AsyncLbbClient(
            "https://example.test", transport=httpx.MockTransport(handle)
        ) as client:
            await client.workflow_turn_reader(task(credential())).sparql("ASK {}")

    with pytest.raises((RuntimeError, TimeoutError)) as error:
        if asynchronous:
            asyncio.run(run_async())
        else:
            with LbbClient("https://example.test", transport=httpx.MockTransport(handle)) as client:
                client.workflow_turn_reader(task(credential())).sparql("ASK {}")
    assert not isinstance(error.value, WorkflowExecutionError)
    assert (
        "upgrade the server" in str(error.value)
        if missing
        else "lifetime elapsed" in str(error.value)
    )
    assert [request.url.path for request in seen] == [REFRESH]


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("status", [200, 401, 403])
def test_short_lifetime_caps_read_timeout_and_auth_errors_never_replay(
    asynchronous: bool,
    status: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from lbb import _workflow_execution as execution

    now = [100.0]
    monkeypatch.setattr(execution, "_monotonic_ms", lambda: now[0])
    seen: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if request.url.path == REFRESH:
            now[0] += 500  # The entire round trip reduces the available ten seconds.
            return httpx.Response(200, json=credential(issued_at_ms=1000, expires_at_ms=11000))
        return httpx.Response(status, json=ANSWER if status == 200 else {})

    async def run_async() -> Any:
        async with AsyncLbbClient(
            "https://example.test", transport=httpx.MockTransport(handle)
        ) as client:
            return await client.workflow_turn_reader(task(credential())).sparql("ASK {}")

    def run_sync() -> Any:
        with LbbClient("https://example.test", transport=httpx.MockTransport(handle)) as client:
            return client.workflow_turn_reader(task(credential())).sparql("ASK {}")

    if status == 200:
        assert (asyncio.run(run_async()) if asynchronous else run_sync()) == ANSWER
    else:
        with pytest.raises(Exception) as error:
            asyncio.run(run_async()) if asynchronous else run_sync()
        assert getattr(error.value, "status_code", None) == status
    assert [request.url.path for request in seen] == [REFRESH, QUERY]
    assert seen[1].extensions["timeout"]["read"] == 9.5
    assert seen[1].headers["authorization"] == "Bearer lbb_wke_execution.signature"
