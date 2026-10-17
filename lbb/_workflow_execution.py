"""Explicit task-bound bounded reads; the control credential never serves a query."""

from __future__ import annotations

import asyncio
import math
import re
import time
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from ._client_base import Body, RequestOptions, _coerce_body

if TYPE_CHECKING:
    from ._client_base import _BaseLbbClient

_READ_FIELDS = frozenset(
    {"query", "cursor", "as_of_commit_seq", "limit", "offset", "entailment", "profile"}
)
_IDENTITY_FIELDS = (
    "graph_id",
    "graph_epoch",
    "workflow_id",
    "turn",
    "attempt",
    "run_id",
)


class WorkflowExecutionError(ValueError):
    """Invalid/missing execution authority or unsafe helper arguments; no fallback."""


def _monotonic_ms() -> float:
    return time.monotonic_ns() / 1_000_000


def _credential(value: Any, operation: str = "sparql_read") -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise WorkflowExecutionError("Claim did not return a workflow execution credential")
    result = dict(value)
    epoch = result.get("graph_epoch")
    token = result.get("token")
    expiry = result.get("expires_at_ms")
    issued = result.get("issued_at_ms")
    if (
        not isinstance(token, str)
        or re.fullmatch(r"lbb_wke_[A-Za-z0-9._-]+", token) is None
        or type(expiry) is not int
        or expiry <= 0
        or (
            issued is not None
            and (type(issued) is not int or issued <= 0 or not 0 < expiry - issued <= 60000)
        )
        or not isinstance(epoch, str)
        or re.fullmatch(r"0|[1-9][0-9]{0,19}", epoch) is None
        or int(epoch) > (1 << 64) - 1
        or any(
            not isinstance(result.get(key), str) or not result[key]
            for key in ("graph_id", "workflow_id", "run_id")
        )
        or type(result.get("turn")) is not int
        or not 1 <= result["turn"] <= (1 << 64) - 1
        or type(result.get("attempt")) is not int
        or not 1 <= result["attempt"] <= (1 << 32) - 1
        or result.get("operations") != [operation]
    ):
        raise WorkflowExecutionError("Invalid workflow execution credential; no read was sent")
    result["operations"] = [operation]
    return result


def _body(request: str | Body) -> dict[str, Any]:
    body = {"query": request} if isinstance(request, str) else _coerce_body(request)
    if (
        not isinstance(body, Mapping)
        or not isinstance(body.get("query"), str)
        or not body["query"].strip()
        or any(key not in _READ_FIELDS for key in body)
    ):
        raise WorkflowExecutionError(
            "Workflow reads accept only query, cursor, as_of_commit_seq, limit, "
            "offset, entailment and profile"
        )
    return dict(body)


def _timeout(value: float) -> float:
    if isinstance(value, bool) or not math.isfinite(value) or value <= 0:
        raise WorkflowExecutionError("Workflow read timeout must be positive and finite")
    return min(value, 30.0)


class _TurnReader:
    def __init__(self, control: _BaseLbbClient, task: Body) -> None:
        task_value = _coerce_body(task)
        if not isinstance(task_value, Mapping):
            raise WorkflowExecutionError("Expected a claimed workflow turn task")
        credential = _credential(task_value.get("execution_credential"))
        turn = task_value.get("turn")
        run = turn.get("run") if isinstance(turn, Mapping) else None
        if (
            not isinstance(turn, Mapping)
            or not isinstance(run, Mapping)
            or not isinstance(run.get("id"), str)
            or not run["id"]
            or credential["workflow_id"] != turn.get("workflow_id")
            or credential["turn"] != turn.get("number")
            or credential["attempt"] != turn.get("attempt")
            or credential["run_id"] != run["id"]
            or not isinstance(task_value.get("token"), str)
            or not task_value["token"]
        ):
            raise WorkflowExecutionError("Execution credential does not match the claimed attempt")
        self._control = control
        self._credential = credential
        # The task may have waited before this reader was constructed.
        self._deadline: float | None = None
        self._identity = tuple(credential[key] for key in _IDENTITY_FIELDS)
        self._lease = {
            "workflow_id": turn["workflow_id"],
            "turn": turn["number"],
            "token": task_value["token"],
        }
        self._params = {
            "graph": credential["graph_id"],
            "expected_graph_epoch": credential["graph_epoch"],
        }
        self._closed = False

    def _check(self) -> None:
        if self._closed:
            raise WorkflowExecutionError("Workflow reader is closed; no read was sent")

    def _accept_refresh(self, response: Any, operation: str, started: float) -> dict[str, Any]:
        self._check()
        credential = _credential(response, operation)
        if tuple(credential[key] for key in _IDENTITY_FIELDS) != self._identity:
            raise WorkflowExecutionError("Refreshed credential changed graph or turn attempt")
        issued = credential.get("issued_at_ms")
        if issued is None:
            raise RuntimeError(
                "Workflow credential lifetime unavailable; upgrade the server "
                "before retrying this read"
            )
        # Charge the entire refresh round trip, independently of wall-clock skew.
        self._deadline = started + (credential["expires_at_ms"] - issued)
        self._credential = credential
        return credential

    def _read_options(self, timeout: float, credential: dict[str, Any]) -> RequestOptions:
        self._check()
        assert self._deadline is not None
        remaining = (self._deadline - _monotonic_ms()) / 1000.0
        if remaining <= 0:
            raise TimeoutError(
                "Workflow execution credential lifetime elapsed; retry the read with a live lease"
            )
        return {
            "retry": False,
            "max_retries": 0,
            "timeout": min(timeout, remaining),
            "headers": {"authorization": f"Bearer {credential['token']}"},
        }

    def close(self) -> None:
        """Refuse future work. Async readers also cancel their active requests."""
        self._closed = True


class WorkflowTurnReader(_TurnReader):
    """Synchronous reads for one claimed task; no automatic lease heartbeat.

    Call through the original control client to heartbeat/checkpoint/complete.
    Close this reader when that control channel loses the lease. Closing a sync
    reader rejects its returned result, but cannot interrupt a blocking transport;
    each call has a positive timeout bounded by the credential expiry.
    """

    def sparql(self, request: str | Body, *, timeout: float = 30.0) -> Any:
        return self._read("sparql_read", "/v1/query/sparql-text", _body(request), _timeout(timeout))

    def _read(
        self,
        operation: str,
        path: str,
        body: dict[str, Any],
        timeout: float,
    ) -> Any:
        self._check()
        credential = self._credential
        if self._deadline is None or self._deadline - _monotonic_ms() <= timeout * 1000 + 1000:
            started = _monotonic_ms()
            credential = self._accept_refresh(
                self._control._request(
                    "POST",
                    "/v1/workflows/turns/execution-credential",
                    params=self._params,
                    body=self._lease,
                    options={"retry": False, "max_retries": 0, "timeout": timeout},
                ),
                operation,
                started,
            )
        request = self._control._request
        answer = request(
            "POST",
            path,
            params=self._params,
            body=body,
            options=self._read_options(timeout, credential),
        )
        self._check()
        return answer


class AsyncWorkflowTurnReader(_TurnReader):
    """Async task-bound reads. Caller cancellation propagates to HTTP/refresh.

    Close on heartbeat failure to cancel all active reads; the server also
    validates the live claim and graph incarnation on each execution.
    """

    def __init__(self, control: _BaseLbbClient, task: Body) -> None:
        super().__init__(control, task)
        self._active: set[asyncio.Task[Any]] = set()

    def close(self) -> None:
        super().close()
        for task in self._active:
            task.cancel()

    async def sparql(self, request: str | Body, *, timeout: float = 30.0) -> Any:
        return await self._active_read(
            "sparql_read", "/v1/query/sparql-text", _body(request), _timeout(timeout)
        )

    async def _active_read(
        self,
        operation: str,
        path: str,
        body: dict[str, Any],
        timeout: float,
    ) -> Any:
        self._check()
        task = asyncio.create_task(self._read(operation, path, body, timeout))
        self._active.add(task)
        try:
            return await task
        finally:
            self._active.discard(task)

    async def _read(
        self,
        operation: str,
        path: str,
        body: dict[str, Any],
        timeout: float,
    ) -> Any:
        self._check()
        credential = self._credential
        if self._deadline is None or self._deadline - _monotonic_ms() <= timeout * 1000 + 1000:
            started = _monotonic_ms()
            credential = self._accept_refresh(
                await self._control._request(
                    "POST",
                    "/v1/workflows/turns/execution-credential",
                    params=self._params,
                    body=self._lease,
                    options={"retry": False, "max_retries": 0, "timeout": timeout},
                ),
                operation,
                started,
            )
        request = self._control._request
        answer = await request(
            "POST",
            path,
            params=self._params,
            body=body,
            options=self._read_options(timeout, credential),
        )
        self._check()
        return answer
