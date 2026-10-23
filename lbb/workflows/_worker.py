"""The workflow worker: claim a turn, run its handler, save its steps, and
complete or fail it.

The rules are those of the TypeScript ``WorkflowWorker``
(``sdk/typescript/src/workflows.ts``), and the scenarios of
``sdk/conformance/workflow-worker`` pin them for both SDKs:

- ``turns/claim`` and ``turns/fail`` are not retried: a lost answer must not
  claim twice or fail an attempt the server already moved on.
- ``turns/heartbeat`` renews the lease every third of it and is not retried:
  a heartbeat that fails ends the attempt here, and the server gives the
  turn to another worker when the lease ends.
- ``turns/operation``, ``turns/progress``, ``turns/checkpoint``,
  ``turns/wait-signal`` and ``turns/complete`` are retried: the same request
  again changes nothing more.
- A ``409`` or a ``404`` on a turn route, and ``409 workflow_fence_lost`` on a
  fenced commit, stop the turn: the worker sends nothing more for it. The
  server answers ``409 turn_lease_lost`` when the attempt's token no longer
  holds the turn (another attempt holds it, its lease ended, or it
  finished) and ``409 conflict`` for the other conflicts of a turn route;
  none of them lets the attempt go on.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import contextvars
import inspect
import logging
import re
import signal
import threading
import time
import unicodedata
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Generic, Literal, TypeVar, cast, overload

import httpx
from pydantic import BaseModel, ValidationError

from .. import models
from .._client_base import LbbError, _coerce_body
from .._workflow_execution import AsyncWorkflowTurnReader, WorkflowExecutionError
from ._results import (
    RunResultsCollector,
    message_instances,
    replayed_inputs,
    step_output_results,
    step_output_with_results,
)

if TYPE_CHECKING:
    from .._async_client import AsyncLbbClient
    from ._handles import WorkflowHandle

S = TypeVar("S")
M = TypeVar("M")
T = TypeVar("T")
ModelT = TypeVar("ModelT", bound=BaseModel)

_logger = logging.getLogger("lbb.workflows")

_BASE = "/v1/workflows"
_KEY = re.compile(r"[A-Za-z0-9_.-]{1,128}")
_MAX_STEPS = 64
_SAFE_INTEGER = 2**53 - 1
_DAY_MS = 86_400_000
# Progress writes of one step at most this often, by default.
_PROGRESS_EVERY_MS = 3_000
# The shortest wait a failure asks for: a wait of now (or of a clock behind the
# server's) waits a second, so a turn that keeps waiting cannot spin.
_MIN_WAIT_MS = 1_000
# How long a claim waits on the server for a turn, in a worker's loop.
_CLAIM_WAIT_MS = 10_000
_RELEASED = "the worker stopped and released the turn"


def _now_ms() -> int:
    return time.time_ns() // 1_000_000


class WorkflowError(Exception):
    """A permanent handler or replay error: the turn fails for good. Other
    exceptions fail the attempt, and the turn runs again up to its
    ``max_attempts``."""


class WorkflowWait(Exception):
    """Raise from a turn to wait for a cause outside it (a dependency that is
    not there yet, a spent budget) and count no attempt.

    The worker fails the turn with ``retry_at_ms``, and the turn runs again at
    ``retry_at_ms``, or at once on a resume or a run now. Its steps are kept.
    A step that meets ``429 workflow_budget_exceeded`` waits this way by
    itself.
    """

    def __init__(self, retry_at_ms: int, message: str = "the turn waits") -> None:
        super().__init__(message)
        self.retry_at_ms = retry_at_ms


def _wait_of(error: BaseException) -> int | None:
    """The wait of a raised error: a :class:`WorkflowWait`, or a spent budget."""
    if isinstance(error, WorkflowWait):
        return error.retry_at_ms
    if (
        isinstance(error, LbbError)
        and error.code == "workflow_budget_exceeded"
        and isinstance(error.retry_at_ms, int)
        and not isinstance(error.retry_at_ms, bool)
    ):
        return error.retry_at_ms
    return None


def _describe(error: BaseException) -> str:
    """An error as a turn's failure and the worker's log name it: an
    ``LbbError`` with its code and request id."""
    text = f"{type(error).__name__}: {error}"
    if isinstance(error, LbbError):
        about = [
            f"code {error.code}" if error.code else "",
            f"request {error.request_id}" if error.request_id else "",
        ]
        if any(about):
            text += f" ({', '.join(a for a in about if a)})"
    return text


class _Suspended(BaseException):
    """A sleep or a signal wait released the worker: the turn resumes later."""


class _TurnStopped(BaseException):
    """The turn is no longer this worker's: nothing more is sent for it."""


class _Released(Exception):
    """The worker stopped and gives the turn back."""

    def __str__(self) -> str:
        return _RELEASED


def _cancelling(task: asyncio.Task[Any]) -> bool:
    """Whether a cancel of ``task`` itself is pending (Python 3.11 and later)."""
    cancelling = getattr(task, "cancelling", None)
    return bool(cancelling()) if callable(cancelling) else False


TURN_LEASE_LOST_CODE = "turn_lease_lost"
"""The code of a turn route's answer that the attempt's token no longer holds
its turn: another attempt holds it, its lease ended, or it finished."""


def _gone(error: BaseException) -> bool:
    """A turn route's answer that the turn is not this worker's any more:
    ``409 turn_lease_lost``, another ``409`` (a server older than the code
    answers a lost lease ``409 conflict``) or a ``404`` (the instance is
    gone)."""
    return isinstance(error, LbbError) and (
        error.code == TURN_LEASE_LOST_CODE or error.status_code in (404, 409)
    )


# ---- JSON values ---------------------------------------------------------------


def _json(value: Any, path: str, seen: set[int]) -> Any:
    """A JSON copy of ``value``; a value JSON cannot hold raises a
    :class:`WorkflowError` that names where it is (``state.a.b[2]``)."""
    if value is None or isinstance(value, (bool, str)):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if value != value or value in (float("inf"), float("-inf")):
            raise WorkflowError(f"Values must be finite JSON numbers: {path}")
        return value
    if isinstance(value, BaseModel):
        return _json(_coerce_body(value), path, seen)
    if isinstance(value, (list, tuple, Mapping)):
        if id(value) in seen:
            raise WorkflowError(f"Workflow values must be JSON (no cycles): {path} is a cycle")
        seen.add(id(value))
        try:
            if isinstance(value, Mapping):
                out: dict[str, Any] = {}
                for key, item in value.items():
                    if not isinstance(key, str):
                        raise WorkflowError(
                            "Workflow values must be JSON serializable: "
                            f"{path} has a key of type {type(key).__name__}"
                        )
                    out[key] = _json(item, f"{path}.{key}", seen)
                return out
            return [_json(item, f"{path}[{i}]", seen) for i, item in enumerate(value)]
        finally:
            seen.discard(id(value))
    raise WorkflowError(
        "Workflow values must be JSON serializable (use None for no result): "
        f"{path} is {type(value).__name__}"
    )


def _copy(value: Any, where: str = "value") -> Any:
    """A JSON copy of a workflow value. ``where`` names the value in the error
    (``state``, ``continuation``, ``step "page.3"``), with the path to the
    field."""
    return _json(value, where, set())


# ---- the claimed task -----------------------------------------------------------


@dataclass(frozen=True)
class _Task:
    """What the worker reads of a claimed task (``WorkflowTurnTask``).

    The task is read field by field, as the TypeScript worker reads it: a
    value of an enum this SDK does not know (a new step kind, a new status)
    stays its text, so a newer server does not fail every attempt after the
    claim. Steps stay JSON objects."""

    raw: Mapping[str, Any]
    workflow_type: str
    version: str
    state: Any
    token: str
    lease_ms: int
    effect_prefix: str
    workflow_id: str
    number: int
    message_id: str
    message: Any
    run_id: str | None
    origin: models.WorkflowMessageOrigin | None
    steps: list[dict[str, Any]]
    #: The inputs a completion may report results for: set by a server that
    #: writes them.
    results_max: int | None = None


def _field(value: Mapping[str, Any], name: str, kind: type | tuple[type, ...]) -> Any:
    item = value.get(name)
    if not isinstance(item, kind) or isinstance(item, bool):
        raise WorkflowError(f"The claim answered a task without a valid {name}")
    return item


def _read_task(raw: Mapping[str, Any]) -> _Task:
    turn = raw.get("turn")
    if not isinstance(turn, Mapping):
        raise WorkflowError("The claim answered a task without a valid turn")
    steps = turn.get("steps") or []
    if not isinstance(steps, list) or not all(
        isinstance(step, Mapping) and isinstance(step.get("key"), str) for step in steps
    ):
        raise WorkflowError("The claim answered a task without valid steps")
    origin = turn.get("origin")
    parsed_origin: models.WorkflowMessageOrigin | None = None
    if isinstance(origin, Mapping):
        try:
            parsed_origin = models.WorkflowMessageOrigin.model_validate(origin)
        except ValidationError:
            parsed_origin = models.WorkflowMessageOrigin.model_construct(**origin)
    run = turn.get("run")
    run_id = run.get("id") if isinstance(run, Mapping) else None
    return _Task(
        raw=raw,
        workflow_type=_field(raw, "workflow_type", str),
        version=_field(turn, "version", str),
        state=raw.get("state"),
        token=_field(raw, "token", str),
        lease_ms=_field(raw, "lease_ms", int),
        effect_prefix=_field(raw, "effect_prefix", str),
        workflow_id=_field(turn, "workflow_id", str),
        number=_field(turn, "number", int),
        message_id=_field(turn, "message_id", str),
        message=turn.get("message"),
        run_id=run_id if isinstance(run_id, str) and run_id else None,
        origin=parsed_origin,
        steps=[dict(step) for step in steps],
        results_max=(
            raw["results_max"]
            if isinstance(raw.get("results_max"), int)
            and not isinstance(raw.get("results_max"), bool)
            and raw["results_max"] > 0
            else None
        ),
    )


# ---- definitions -----------------------------------------------------------------


@dataclass(frozen=True)
class WorkflowResult(Generic[S]):
    """What a turn's handler returns: the new ``state``, the turn's reply
    ``result``, and an optional ``continuation`` (``{"message": ...}``) from
    :meth:`WorkflowContext.continue_`."""

    state: S
    result: Any = None
    continuation: Mapping[str, Any] | None = None


@dataclass(frozen=True)
class WorkflowSignalResult(Generic[T]):
    """What :meth:`WorkflowContext.wait_for_signal` received: ``received`` is
    false when the wait timed out, and ``value`` is then ``None``."""

    received: bool
    value: T | None


StepKind = Literal["step", "model", "tool"]
StepFn = (
    Callable[["WorkflowStepContext"], "Awaitable[T] | T"] | Callable[[], "Awaitable[T] | T"]
)
Handler = Callable[["WorkflowContext", S, M], Awaitable[WorkflowResult[S]]]


class WorkflowDefinition(Generic[S, M]):
    """A version-pinned message handler. Register it with a
    :class:`WorkflowWorker`; start an instance of it with :meth:`start`."""

    def __init__(self, name: str, version: str, initial_state: S, handler: Handler[S, M]):
        if not isinstance(name, str) or not name or not isinstance(version, str) or not version:
            raise WorkflowError("A workflow needs a name and a version")
        self.name = name
        self.version = version
        self.initial_state: S = _copy(initial_state, "initial_state")
        self._handler = handler

    async def execute(self, ctx: WorkflowContext, state: Any, message: Any) -> WorkflowResult[S]:
        """Run the handler for one turn."""
        return await self._handler(ctx, cast(S, state), cast(M, message))

    async def start(
        self,
        client: AsyncLbbClient,
        id: str,
        *,
        lease_ms: int = 60_000,
        max_attempts: int = 3,
        on_exhausted: models.WorkflowOnExhausted | Literal["block", "skip"] | None = None,
    ) -> WorkflowHandle:
        """Create the instance ``id`` of this workflow
        (``client.workflows.start``) and return its handle.

        ``on_exhausted`` says what a turn that failed for good does: ``block``
        (the default) holds the inbox until a person retries or skips it;
        ``skip`` keeps the failed turn in the history and goes on with the
        next message. It is set here, at create.
        """
        return await client.workflows.start(
            self, id, lease_ms=lease_ms, max_attempts=max_attempts, on_exhausted=on_exhausted
        )


@overload
def workflow(
    *, name: str, version: str, initial_state: S
) -> Callable[[Handler[S, M]], WorkflowDefinition[S, M]]: ...
@overload
def workflow(
    *, name: str, version: str, initial_state: S, on_message: Handler[S, M]
) -> WorkflowDefinition[S, M]: ...
def workflow(
    *,
    name: str,
    version: str,
    initial_state: Any,
    on_message: Handler[Any, Any] | None = None,
) -> Any:
    """Define a version-pinned message handler. Put I/O, clock reads and
    randomness inside ``ctx.step``.

    Use it as a decorator::

        @workflow(name="counter", version="v1", initial_state={"count": 0})
        async def counter(ctx: WorkflowContext, state: dict, message: dict) -> WorkflowResult:
            value = await ctx.step("add", lambda s: state["count"] + message["add"])
            return WorkflowResult(state={"count": value}, result=value)

    or pass the handler as ``on_message``.
    """
    if on_message is not None:
        return WorkflowDefinition(name, version, initial_state, on_message)

    def define(handler: Handler[Any, Any]) -> WorkflowDefinition[Any, Any]:
        return WorkflowDefinition(name, version, initial_state, handler)

    return define


# ---- identities ------------------------------------------------------------------

# The engine identity of a shard: `starter.<name>` or `starter.<name>.<k>`.
_SHARD = re.compile(r"starter\.([^.]+)(?:\.([1-9][0-9]*))?")
# The engine identity of a fan-in identity: `starter.<name>.<word>`.
_FAN_IN = re.compile(r"starter\.([^.]+)\.([a-z][a-z0-9-]{0,30})")
_FAN_IN_ID = re.compile(r"[a-z][a-z0-9-]{0,30}")


def workflow_of_identity(id: str) -> str | None:
    """The named workflow of an engine identity: a shard's (``starter.<name>``,
    ``starter.<name>.<k>``) or a fan-in identity's (``starter.<name>.<word>``);
    ``None`` for an identity of no named workflow."""
    found = _SHARD.fullmatch(id) or _FAN_IN.fullmatch(id)
    return found.group(1) if found else None


def fan_in_identity(name: str, fan_in: str) -> str:
    """The engine identity of the fan-in identity ``fan_in`` of the workflow
    ``name`` (``fan_in`` of ``PUT /v1/workflows/named``):
    ``starter.<name>.<fan_in>``. Send to it with a deterministic message id,
    so a repeat is admitted once."""
    if not _FAN_IN_ID.fullmatch(fan_in):
        raise ValueError("a fan-in id matches [a-z][a-z0-9-]{0,30}")
    return f"starter.{name}.{fan_in}"


def is_fan_in_identity(id: str) -> bool:
    """Whether ``id`` is a fan-in identity (``starter.<name>.<word>``)."""
    return _FAN_IN.fullmatch(id) is not None


# ---- the run of a turn, for activity calls -------------------------------------------


@dataclass
class _ActivityTurn:
    """The run of a turn this process runs: the ``workflow`` its activity
    calls name, what those calls used, added up for the completion, and the
    steps whose function ran in this attempt."""

    workflow: dict[str, Any]
    usage: dict[str, int] = field(
        default_factory=lambda: {
            "calls": 0,
            "errors": 0,
            "tokens_in": 0,
            "tokens_out": 0,
            "cost_micro_usd": 0,
        }
    )
    #: Each step whose function ran in this attempt: its effect id and its
    #: key. At most 64 (the steps of a turn), cleared when the turn ends.
    steps: dict[str, str] = field(default_factory=dict)

    def add(self, **used: int) -> None:
        for key, value in used.items():
            self.usage[key] = self.usage.get(key, 0) + value

    #: What the turn did with each input (``write_owned``, ``review``,
    #: ``requeue``), for the completion's ``results``; ``None`` when the claim
    #: named no ``results_max``.
    results: RunResultsCollector | None = None
    #: The instances of the turn's trigger message.
    instances: frozenset[str] = frozenset()

    def step_of(self, effect_id: str | None) -> str | None:
        """The key of the step that makes an activity call: the step whose
        effect id is ``effect_id``, else the step with the longest effect id
        that ``effect_id`` starts with followed by ``.`` or ``:`` (an id made
        from it, such as ``f"{s.effect_id}.2"``). ``None`` when no step of the
        turn matches. The lookup reads the step runner's record and parses no
        step from the id."""
        if effect_id is None:
            return None
        exact = self.steps.get(effect_id)
        if exact is not None:
            return exact
        found: tuple[str, str] | None = None
        for step_id, key in self.steps.items():
            if (
                effect_id[len(step_id) : len(step_id) + 1] in (".", ":")
                and effect_id.startswith(step_id)
                and (found is None or len(step_id) > len(found[0]))
            ):
                found = (step_id, key)
        return found[1] if found is not None else None

    def reported(self) -> models.WorkflowRunUsage | None:
        """The usage a turn reports, or nothing when it made no activity call."""
        if self.usage["calls"] > 0 or self.usage["errors"] > 0:
            return models.WorkflowRunUsage(**self.usage)
        return None

    def report_results(
        self, message: Any = None, continuation: Any = None
    ) -> models.WorkflowRunResultsReport | None:
        """The results a turn reports: what its calls reported, and, at a
        completion (``message``), the instances of its trigger message that it
        named nowhere and that its continuation does not carry
        (``no_value``). ``None`` when the claim named no ``results_max``, or
        when no call reported an input."""
        results = self.results
        if results is None or not len(results):
            return None
        if message is not None:
            carried = set(message_instances(continuation))
            for iri in message_instances(message):
                if not results.has(iri) and iri not in carried:
                    results.add({"iri": iri, "label": "", "outcome": "no_value", "values": []})
        try:
            report = results.report()
            return models.WorkflowRunResultsReport.model_validate(report) if report else None
        except Exception:  # noqa: BLE001 - results never cost a completion
            _logger.warning("the run's results were left out of the turn's report", exc_info=True)
            return None


# The turns this process runs now, by their effect prefix.
_RUNNING: dict[str, _ActivityTurn] = {}
# The turn whose task (or a task or thread it started) is running now.
_CURRENT: contextvars.ContextVar[_ActivityTurn | None] = contextvars.ContextVar(
    "lbb_workflow_turn", default=None
)


def _turn_of_instance(iri: str) -> _ActivityTurn | None:
    """The running turn whose trigger message names ``iri``: a review item
    about an input belongs to the turn that reads it. ``None`` when none or
    more than one does."""
    found = [turn for turn in _RUNNING.values() if iri in turn.instances]
    return found[0] if len(found) == 1 else None


def _activity_turn(effect_id: str | None) -> _ActivityTurn | None:
    """The turn an activity call belongs to: the turn whose effect prefix
    starts ``effect_id`` (a step's ``effect_id``, or one made from it such as
    ``f"{s.effect_id}.2"``); else the turn of the running task; else, when
    this process runs one turn only, that turn. ``None`` outside a turn."""
    if effect_id is not None:
        prefix, slash, _ = effect_id.partition("/")
        if slash and prefix in _RUNNING:
            return _RUNNING[prefix]
    current = _CURRENT.get()
    if current is not None:
        return current
    if len(_RUNNING) == 1:
        return next(iter(_RUNNING.values()))
    return None


# ---- progress -----------------------------------------------------------------------


def _whole(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and 0 <= value <= _SAFE_INTEGER


def _unit_ok(unit: str) -> bool:
    return 1 <= len(unit) <= 32 and all(
        c in " _-" or unicodedata.category(c)[0] in "LN" for c in unit
    )


def _progress_details(
    phase: str, done: int, total: int | None, unit: str | None
) -> dict[str, Any]:
    """Progress details in the convention the console draws, checked as the
    server checks them."""
    text = phase.strip() if isinstance(phase, str) else ""
    if not text or len(text) > 80 or any(unicodedata.category(c) == "Cc" for c in text):
        raise WorkflowError("Progress phase must be text of 1 to 80 characters")
    if not _whole(done):
        raise WorkflowError("Progress done must be a whole number of at least 0")
    if total is not None and not _whole(total):
        raise WorkflowError("Progress total must be a whole number of at least 0")
    trimmed = unit.strip() if isinstance(unit, str) else None
    if unit is not None and not (trimmed and _unit_ok(trimmed)):
        raise WorkflowError("Progress unit must be a short word of 1 to 32 characters")
    details: dict[str, Any] = {"phase": text, "done": done}
    if total is not None:
        details["total"] = total
    if trimmed:
        details["unit"] = trimmed
    return details


_UNSET: Any = object()


class WorkflowStepContext:
    """What a step's function gets: its stable ``effect_id``, the ``fence`` of
    its turn, and progress reports.

    Pass ``effect_id`` to an external service as its idempotency key: a step
    that runs again after a crash sends the same id.
    """

    def __init__(
        self,
        *,
        effect_id: str,
        fence: models.WorkflowFence | None,
        graph: str | None,
        checkpoint: Any,
        heartbeat: Callable[[Any, Any], Awaitable[None]] | None = None,
        progress: Callable[[dict[str, Any]], None] | None = None,
    ) -> None:
        self.effect_id = effect_id
        #: The turn this step runs in, as ``workflow_fence`` of a commit names
        #: it: the server refuses a new commit once the token no longer holds
        #: the turn (``409 workflow_fence_lost``). ``write_owned`` sends it with
        #: a write to the turn's graph. A :class:`WorkflowWorker` always sets
        #: it; a test's own context may leave it out.
        self.fence = fence
        #: The graph of the turn: the graph the worker's client names. ``None``
        #: when that client names none (the server's default graph);
        #: ``write_owned`` then takes the graph a write names for the turn's.
        self.graph = graph
        #: The last progress checkpoint of a previous attempt, or ``None``.
        self.checkpoint = checkpoint
        self._heartbeat = heartbeat
        self._progress = progress
        try:
            self._loop: asyncio.AbstractEventLoop | None = asyncio.get_running_loop()
        except RuntimeError:
            self._loop = None
        self._thread = threading.get_ident()

    async def heartbeat(self, details: Any, *, checkpoint: Any = _UNSET) -> None:
        """Report progress details now and, with ``checkpoint``, save a resume
        cursor that the next attempt of this step reads as ``checkpoint``.
        Await it from the step's event loop."""
        if self._heartbeat is not None:
            await self._heartbeat(details, checkpoint)

    def progress(
        self, phase: str, *, done: int, total: int | None = None, unit: str | None = None
    ) -> None:
        """Report what the step does now in the progress convention
        (``{phase, done, total, unit}``), which the console draws as a Running
        line and a bar.

        Coalesced: a phase change is sent at once, other calls at most once
        per ``progress_every_ms`` of the worker (3 s by default, or half the
        step's ``heartbeat_timeout_ms`` when that is shorter), the last value
        wins, and the last value is sent when the step returns. Keeps the
        resume checkpoint of the last ``heartbeat``. Does not wait for the
        write; a failed write fails the step when it returns. ``unit`` names
        what ``done`` and ``total`` count: ``documents``, ``pages``,
        ``instances``, ``items`` or another short word. A sync step function
        runs in a thread and may call it from there.
        """
        details = _progress_details(phase, done, total, unit)
        report = self._progress
        if report is None:
            return
        loop = self._loop
        if loop is None or threading.get_ident() == self._thread:
            report(details)
            return
        # From the thread of a sync step function: hand the report to the
        # step's event loop, and raise its refusal here.
        done_here: concurrent.futures.Future[None] = concurrent.futures.Future()

        def on_loop() -> None:
            try:
                report(details)
            except BaseException as error:  # noqa: BLE001 - handed back to the caller
                done_here.set_exception(error)
            else:
                done_here.set_result(None)

        loop.call_soon_threadsafe(on_loop)
        done_here.result()


class WorkflowContext:
    """What a turn's handler gets: the turn's identity, and the durable
    operations of the turn (steps, signal waits, sleeps)."""

    def __init__(self, turn: _Turn) -> None:
        self._turn = turn
        task = turn.task
        self.workflow_id: str = task.workflow_id
        self.message_id: str = task.message_id
        self.turn: int = task.number
        #: Where the message came from, when its sender named it.
        self.origin: models.WorkflowMessageOrigin | None = task.origin
        #: The named workflow of this identity (a shard's or a fan-in
        #: identity's), if any.
        self.workflow_name: str | None = workflow_of_identity(self.workflow_id)

    @overload
    async def step(
        self,
        key: str,
        fn: StepFn[Any],
        *,
        model: type[ModelT],
        kind: StepKind = "step",
        timeout_ms: int = 300_000,
        heartbeat_timeout_ms: int | None = None,
    ) -> ModelT: ...
    @overload
    async def step(
        self,
        key: str,
        fn: StepFn[T],
        *,
        kind: StepKind = "step",
        timeout_ms: int = 300_000,
        heartbeat_timeout_ms: int | None = None,
    ) -> T: ...
    async def step(
        self,
        key: str,
        fn: StepFn[Any],
        *,
        model: type[BaseModel] | None = None,
        kind: StepKind = "step",
        timeout_ms: int = 300_000,
        heartbeat_timeout_ms: int | None = None,
    ) -> Any:
        """Run ``fn`` once as the named step ``key`` and save its output.

        A turn that runs again replays its saved steps by key and kind, in
        their order: ``fn`` does not run again, and the step returns the saved
        output. Await the steps one after the other, with distinct keys
        (``[A-Za-z0-9_.-]``, at most 128 characters, at most 64 per turn).

        ``fn`` takes the :class:`WorkflowStepContext` (or nothing) and returns
        JSON, or an awaitable of it. An ``async`` function runs on the event
        loop. A sync function runs in a thread (``asyncio.to_thread``), so a
        blocking call there holds no other turn's heartbeat; what it returns
        may be awaitable, and that runs on the loop. A Pydantic model comes
        back as that model only with ``model=``; return plain JSON otherwise.
        ``kind`` is ``step``, ``model`` or ``tool``. ``timeout_ms`` limits one
        attempt of the step (default 5 minutes), apart from the lease renewal;
        ``heartbeat_timeout_ms`` requires progress from the step that often.
        """
        output = await self._turn.checkpoint(
            key,
            "step",
            fn=fn,
            model=model,
            operation={
                "kind": kind,
                "timeout_ms": timeout_ms,
                "heartbeat_timeout_ms": heartbeat_timeout_ms,
            },
        )
        return model.model_validate(output) if model is not None else output

    async def wait_for_signal(
        self, key: str, *, name: str, timeout_ms: int
    ) -> WorkflowSignalResult[Any]:
        """Wait for the signal ``name`` without holding a worker: the turn
        resumes when it arrives or at ``timeout_ms``. A signal can arrive
        before the wait begins."""
        if (
            not isinstance(name, str)
            or not _KEY.fullmatch(name)
            or not _whole(timeout_ms)
            or timeout_ms < 1
            or timeout_ms > 365 * _DAY_MS
        ):
            raise WorkflowError("Invalid signal name/timeout")
        output = await self._turn.checkpoint(key, "signal", delay=timeout_ms, signal_name=name)
        if isinstance(output, Mapping) and output.get("received") is True:
            return WorkflowSignalResult(received=True, value=output.get("value"))
        return WorkflowSignalResult(received=False, value=None)

    async def sleep(self, key: str, milliseconds: int) -> None:
        """Save the turn and release the worker until the timer is due."""
        if not _whole(milliseconds) or milliseconds > 365 * _DAY_MS:
            raise WorkflowError("Invalid sleep duration")
        await self._turn.checkpoint(key, "sleep", delay=milliseconds)

    def continue_(self, state: S, message: Any, *, result: Any = None) -> WorkflowResult[S]:
        """Save ``state`` and append ``message`` at the inbox tail in one
        write: the rest of a large message goes to a new turn. (``continue``
        is a Python keyword.)"""
        return WorkflowResult(state=state, result=result, continuation={"message": message})

    async def blob_json(self, key: str) -> Any:
        """The JSON value of workflow blob ``key`` (``GET /v1/workflows/blobs``),
        with the worker's client. A blob's bytes never change, so a read
        outside a step reads the same value on every attempt."""
        return await self._turn._client.workflows.blobs.get_json(key)

    async def sparql(self, request: str | Mapping[str, Any], *, timeout: float = 30.0) -> Any:
        """SELECT or ASK for this live attempt, with its execution credential
        (``POST /v1/query/sparql-text``). Needs the worker's
        ``execution_reads``; call it inside ``ctx.step`` to save the answer."""
        return await self._turn.sparql(request, timeout)


def _is_async(fn: Callable[..., Any]) -> bool:
    return inspect.iscoroutinefunction(fn) or inspect.iscoroutinefunction(
        getattr(fn, "__call__", None)  # noqa: B004 - an object with an async __call__
    )


async def _call(fn: StepFn[Any], step: WorkflowStepContext) -> Any:
    """Call a step's function with its context (or with nothing, when it takes
    no argument). An async function runs on the loop; a sync one runs in a
    thread, so a blocking call holds no other turn. An awaitable it returns
    is awaited on the loop."""
    try:
        takes_none = not inspect.signature(fn).parameters
    except (TypeError, ValueError):
        takes_none = False

    def invoke() -> Any:
        if takes_none:
            return cast(Callable[[], Any], fn)()
        return cast(Callable[[Any], Any], fn)(step)

    value = invoke() if _is_async(fn) else await asyncio.to_thread(invoke)
    if inspect.isawaitable(value):
        value = await value
    return value


class _Turn:
    """One claimed attempt of a turn, from the claim to its completion,
    failure, suspension or loss."""

    def __init__(
        self,
        worker: WorkflowWorker,
        task: _Task,
        definition: WorkflowDefinition[Any, Any],
    ) -> None:
        self._worker = worker
        self._client = worker._client
        self.task = task
        self._definition = definition
        self._lease: dict[str, Any] = {
            "workflow_id": task.workflow_id,
            "turn": task.number,
            "token": task.token,
        }
        self._graph: str | None = getattr(self._client, "_graph", None)
        self._steps: list[dict[str, Any]] = list(task.steps)
        self._position = 0
        self._busy = False
        # The attempt runs no more code of its own: a sleep or a signal wait
        # suspended it, or it completes.
        self._stopped = False
        self._invalid: WorkflowError | None = None
        # Why the attempt ended here: a lost lease, a step's timeout, or a
        # release when the worker stops.
        self._abort: BaseException | None = None
        self._handler: asyncio.Task[Any] | None = None
        self._finishing = False
        self._heartbeats: asyncio.Task[None] | None = None
        self._reader: AsyncWorkflowTurnReader | None = None
        workflow: dict[str, Any] = {"identity": task.workflow_id, "turn": task.number}
        if task.run_id:
            workflow["run_id"] = task.run_id
        self.activity = _ActivityTurn(
            workflow=workflow,
            # A server that writes results names how many a run keeps whole.
            results=RunResultsCollector(task.results_max) if task.results_max else None,
            instances=frozenset(message_instances(task.message)),
        )

    # -- the lease --

    def _lease_body(self) -> models.WorkflowTurnLeaseRequest:
        return models.WorkflowTurnLeaseRequest(**self._lease)

    def lose(self, reason: BaseException) -> None:
        """End the attempt here: the handler stops at its next await, and the
        worker sends nothing more for the turn (a release fails it with
        ``retry_at_ms`` first)."""
        if self._abort is not None:
            return
        self._abort = reason
        if self._reader is not None:
            self._reader.close()
        handler = self._handler
        # The handler's own task raises the stop by itself.
        if handler is not None and not handler.done() and asyncio.current_task() is not handler:
            handler.cancel()

    def release(self) -> bool:
        """Give the turn back when the worker stops: ``False`` when the turn
        completes already."""
        if self._finishing:
            return False
        self.lose(_Released())
        return True

    def _check(self) -> None:
        if self._stopped:
            raise _Suspended()
        if self._abort is not None:
            raise _TurnStopped() from self._abort
        if self._invalid is not None:
            raise self._invalid

    def _start_heartbeat(self) -> None:
        self._stopped = False
        if self._heartbeats is None or self._heartbeats.done():
            self._heartbeats = asyncio.create_task(self._heartbeat_loop())

    def _stop_heartbeat(self) -> None:
        self._stopped = True
        if self._heartbeats is not None:
            self._heartbeats.cancel()
            self._heartbeats = None

    async def _heartbeat_loop(self) -> None:
        lease_ms = self.task.lease_ms
        every = max(20, lease_ms / 3) / 1000
        timeout = max(50, lease_ms // 3) / 1000
        while True:
            await asyncio.sleep(every)
            try:
                await self._client._request(
                    "POST",
                    f"{_BASE}/turns/heartbeat",
                    body=self._lease_body(),
                    options={"retry": False, "timeout": timeout},
                )
            except asyncio.CancelledError:
                raise
            except Exception as error:
                # The lease may be gone: another worker gets the turn when it
                # ends, and this attempt sends nothing more.
                if not self._stopped:
                    self.lose(error)
                return

    async def _turn_request(self, route: str, body: BaseModel) -> Any:
        """A retried request of a turn route. A ``409`` or a ``404`` ends the
        attempt here: the handler stops, wherever it waits."""
        try:
            return await self._client._request(
                "POST", f"{_BASE}/turns/{route}", body=body, options={"retry": True}
            )
        except LbbError as error:
            if _gone(error):
                self.lose(error)
                raise _TurnStopped() from error
            raise

    # -- steps --

    async def checkpoint(
        self,
        key: str,
        kind: Literal["step", "sleep", "signal"],
        *,
        fn: StepFn[Any] | None = None,
        model: type[BaseModel] | None = None,
        delay: int | None = None,
        signal_name: str | None = None,
        operation: Mapping[str, Any] | None = None,
    ) -> Any:
        self._check()
        if (
            self._busy
            or not isinstance(key, str)
            or not _KEY.fullmatch(key)
            or self._position >= _MAX_STEPS
        ):
            self._invalid = WorkflowError(
                "Await sequential steps with distinct valid keys; "
                "at 64 steps return ctx.continue_(state, cursor)"
            )
            raise self._invalid
        self._busy = True
        try:
            index = self._position
            self._position += 1
            if index < len(self._steps):
                old = self._steps[index]
                if (
                    old.get("key") != key
                    or old.get("kind") != kind
                    or old.get("delay_ms") != delay
                    or old.get("signal_name") != signal_name
                ):
                    self._invalid = WorkflowError(
                        "Step order changed: keep code for existing workflow versions"
                    )
                    raise self._invalid
                # A step that reported inputs saved them: report them again, as
                # they were in this run (a resume), or as an earlier attempt's.
                value, run, saved = step_output_results(old.get("output"))
                results = self.activity.results
                if results is not None:
                    same = run == self.activity.workflow.get("run_id")
                    for item in saved if same else replayed_inputs(saved):
                        results.add(item)
                return _copy(value)
            if any(step.get("key") == key for step in self._steps):
                self._invalid = WorkflowError("Step keys must be unique within a turn")
                raise self._invalid
            output: Any = None
            # What the step reported, kept in its saved output.
            reported: list[dict[str, Any]] = []
            if kind == "step":
                assert fn is not None
                results = self.activity.results
                if results is not None:
                    results.begin_step()
                try:
                    output = await self._run_step(key, index, fn, model, **(operation or {}))
                finally:
                    if results is not None:
                        reported = results.end_step()
            saved_output = step_output_with_results(
                output, self.activity.workflow.get("run_id") or "", reported
            )
            self._check()
            if kind != "step":
                self._stop_heartbeat()
            if kind == "signal":
                saved = await self._turn_request(
                    "wait-signal",
                    models.WorkflowTurnWaitSignalRequest(
                        **self._lease,
                        position=index,
                        key=key,
                        name=cast(str, signal_name),
                        timeout_ms=cast(int, delay),
                    ),
                )
                steps = saved.get("steps") if isinstance(saved, Mapping) else None
                if not isinstance(steps, list) or len(steps) <= index:
                    raise WorkflowError("The signal wait answered no step at its position")
                entry = dict(steps[index])
                self._steps.append(entry)
                if saved.get("status") == "waiting":
                    raise _Suspended()
                self._start_heartbeat()
                return _copy(entry.get("output"))
            await self._turn_request(
                "checkpoint",
                models.WorkflowTurnCheckpointRequest(
                    **self._lease,
                    position=index,
                    key=key,
                    kind=models.WorkflowCheckpointKind(kind),
                    output=saved_output,
                    delay_ms=delay,
                ),
            )
            self._steps.append(
                {"key": key, "kind": kind, "output": saved_output, "delay_ms": delay, "at_ms": 0}
            )
            if kind == "sleep":
                raise _Suspended()
            return _copy(output)
        finally:
            self._busy = False

    async def _run_step(
        self,
        key: str,
        index: int,
        fn: StepFn[Any],
        model: type[BaseModel] | None,
        *,
        kind: str = "step",
        timeout_ms: int = 300_000,
        heartbeat_timeout_ms: int | None = None,
    ) -> Any:
        progress_timeout = heartbeat_timeout_ms
        if (
            not _whole(timeout_ms)
            or timeout_ms < 100
            or timeout_ms > _DAY_MS
            or (
                progress_timeout is not None
                and (
                    not _whole(progress_timeout)
                    or progress_timeout < 100
                    or progress_timeout > timeout_ms
                )
            )
        ):
            raise WorkflowError("Invalid operation/progress timeout")
        begun = await self._turn_request(
            "operation",
            models.WorkflowTurnOperationRequest(
                **self._lease,
                key=key,
                position=index,
                kind=models.WorkflowOperationKind(kind),
                timeout_ms=timeout_ms,
                heartbeat_timeout_ms=progress_timeout,
            ),
        )
        self._check()  # A late begin answer cannot authorize I/O after lease loss.
        operation = begun.get("operation") if isinstance(begun, Mapping) else None
        cursor: Any = operation.get("checkpoint") if isinstance(operation, Mapping) else None
        loop = asyncio.get_running_loop()
        sequence = 0
        active = True
        pending: asyncio.Future[None] | None = None

        def report(details: Any, saved_cursor: Any = _UNSET) -> asyncio.Future[None]:
            """One progress write, in order behind the earlier ones."""
            nonlocal pending
            previous = pending

            async def write() -> None:
                nonlocal sequence, cursor
                if previous is not None:
                    await previous
                self._check()
                if saved_cursor is not _UNSET:
                    cursor = saved_cursor
                sequence += 1
                await self._turn_request(
                    "progress",
                    models.WorkflowTurnProgressRequest(
                        **self._lease,
                        key=key,
                        sequence=sequence,
                        details=details,
                        checkpoint=cursor,
                    ),
                )
                self._check()
                watch_progress()

            pending = asyncio.ensure_future(write())
            # The worker also observes this write if the handler forgets to await it.
            pending.add_done_callback(lambda f: f.cancelled() or f.exception())
            return pending

        # Coalesced `progress`: a phase change goes out at once, other values
        # at most once per interval, the last value wins.
        every_ms = float(self._worker._progress_every_ms)
        if progress_timeout is not None:
            every_ms = min(every_ms, max(50, progress_timeout // 2))
        last_phase: str | None = None
        last_sent = float("-inf")
        held: dict[str, Any] | None = None
        flush_timer: asyncio.TimerHandle | None = None
        progress_timer: asyncio.TimerHandle | None = None

        def flush() -> None:
            nonlocal held, flush_timer, last_phase, last_sent
            if flush_timer is not None:
                flush_timer.cancel()
                flush_timer = None
            if held is None:
                return
            details, held = held, None
            last_phase = details["phase"]
            last_sent = loop.time() * 1000
            report(details)

        def on_progress(details: dict[str, Any]) -> None:
            nonlocal held, flush_timer
            self._check()
            if not active:
                raise WorkflowError("Operation has already finished")
            held = details
            wait = last_sent + every_ms - loop.time() * 1000
            if details["phase"] != last_phase or wait <= 0:
                flush()
            elif flush_timer is None:
                flush_timer = loop.call_later(wait / 1000, flush)

        async def on_heartbeat(details: Any, checkpoint: Any) -> None:
            nonlocal held, flush_timer
            self._check()
            if not active:
                raise WorkflowError("Operation has already finished")
            saved_details = _copy(details, f'step "{key}" progress')
            saved_cursor = (
                _UNSET if checkpoint is _UNSET else _copy(checkpoint, f'step "{key}" checkpoint')
            )
            # An explicit heartbeat is newer than a held progress value.
            held = None
            if flush_timer is not None:
                flush_timer.cancel()
                flush_timer = None
            await report(saved_details, saved_cursor)

        def watch_progress() -> None:
            nonlocal progress_timer
            if progress_timer is not None:
                progress_timer.cancel()
            if progress_timeout is not None:
                progress_timer = loop.call_later(
                    progress_timeout / 1000,
                    self.lose,
                    TimeoutError(f"Operation {key} stopped reporting progress"),
                )

        deadline = loop.call_later(
            timeout_ms / 1000, self.lose, TimeoutError(f"Operation {key} timed out")
        )
        watch_progress()
        effect_id = f"{self.task.effect_prefix}/{key}"
        # An activity call with this id, or one made from it, names the step.
        self.activity.steps[effect_id] = key
        try:
            step = WorkflowStepContext(
                effect_id=effect_id,
                fence=models.WorkflowFence(**self._lease),
                graph=self._graph,
                checkpoint=_copy(cursor),
                heartbeat=on_heartbeat,
                progress=on_progress,
            )
            value = await _call(fn, step)
            if isinstance(value, BaseModel) and model is None:
                raise WorkflowError(
                    f'step "{key}" returned a {type(value).__name__}: pass '
                    f"model={type(value).__name__} to get it back when the step replays, "
                    "or return plain JSON"
                )
            output = _copy(value, f'step "{key}"')
            # The last progress value goes out before the step's checkpoint.
            flush()
            active = False
            if pending is not None:
                await pending
            return output
        finally:
            active = False
            deadline.cancel()
            if progress_timer is not None:
                progress_timer.cancel()
            if flush_timer is not None:
                flush_timer.cancel()

    async def sparql(self, request: str | Mapping[str, Any], timeout: float) -> Any:
        self._check()
        if self._reader is None:
            raise WorkflowExecutionError(
                "Enable execution_reads on this worker; no query was sent"
            )
        return await self._reader.sparql(request, timeout=timeout)

    # -- the attempt --

    async def _handle(self) -> WorkflowResult[Any]:
        """The handler, in its own task, so a lost lease cancels it even when
        a provider ignores cancellation of its own."""
        ctx = WorkflowContext(self)
        result = await self._definition.execute(
            ctx, _copy(self.task.state), _copy(self.task.message)
        )
        if isinstance(result, Mapping):
            # A plain mapping of the same three fields.
            result = WorkflowResult(
                state=result.get("state"),
                result=result.get("result"),
                continuation=result.get("continuation"),
            )
        if not isinstance(result, WorkflowResult):
            raise WorkflowError("A handler returns a WorkflowResult")
        return result

    async def run(self) -> None:
        prefix = self.task.effect_prefix
        _RUNNING[prefix] = self.activity
        token = _CURRENT.set(self.activity)
        try:
            await self._attempt()
        finally:
            if _RUNNING.get(prefix) is self.activity:
                del _RUNNING[prefix]
            self.activity.steps.clear()
            _CURRENT.reset(token)
            self._stop_heartbeat()
            if self._reader is not None:
                self._reader.close()

    async def _attempt(self) -> None:
        self._start_heartbeat()
        try:
            self._check()
            if self._worker._execution_reads:
                self._reader = AsyncWorkflowTurnReader(self._client, self.task.raw)
            self._handler = asyncio.create_task(self._handle())
            result = await self._handler
            self._check()
            if self._busy or self._position != len(self._steps):
                raise WorkflowError("Handler left unawaited steps or omitted saved steps")
            continuation = result.continuation
            if continuation is not None and not isinstance(continuation, Mapping):
                raise WorkflowError('A continuation is {"message": ...}')
            body = models.WorkflowTurnCompleteRequest(
                **self._lease,
                state=_copy(result.state, "state"),
                result=_copy(result.result, "result"),
                continuation=(
                    models.WorkflowTurnContinuation(
                        message=_copy(continuation.get("message"), "continuation.message")
                    )
                    if continuation is not None
                    else None
                ),
                usage=self.activity.reported(),
                results=self.activity.report_results(
                    self.task.message,
                    continuation.get("message") if continuation is not None else None,
                ),
            )
            # Serialize completion retries exactly; a lost answer must not rerun effects.
            self._finishing = True
            self._stop_heartbeat()
            try:
                await self._turn_request("complete", body)
            except LbbError as refused:
                # Results never cost a completion: a body too large for the
                # server goes again without them.
                if refused.status_code != 413 or body.results is None:
                    raise
                await self._turn_request("complete", body.model_copy(update={"results": None}))
        except _Suspended:
            return
        except _TurnStopped:
            # A release that came after the handler's last await still gives
            # the turn back.
            await self._ended_here()
        except asyncio.CancelledError:
            # The handler's cancel by `lose` ends the attempt here; a cancel of
            # the worker's own task goes on (Python 3.11 and later tell them
            # apart).
            outer = asyncio.current_task()
            if self._abort is None or (outer is not None and _cancelling(outer)):
                raise
            await self._ended_here()
        except Exception as error:
            if isinstance(error, LbbError) and error.code == "workflow_fence_lost":
                return  # The turn's next holder makes the commit.
            if self._abort is not None:
                await self._ended_here()
                return
            self._finishing = True
            self._stop_heartbeat()
            # A wait for a cause outside the turn counts no attempt.
            retry_at = _wait_of(error)
            await self._fail(
                _describe(error),
                non_retryable=retry_at is None
                and isinstance(error, (WorkflowError, WorkflowExecutionError)),
                retry_at_ms=(
                    max(retry_at, _now_ms() + _MIN_WAIT_MS) if retry_at is not None else None
                ),
            )

    async def _ended_here(self) -> None:
        """The attempt ended here: a release gives the turn back at once; a
        lost lease or a step's timeout sends nothing (the server expires the
        attempt and schedules its retry)."""
        if isinstance(self._abort, _Released):
            self._finishing = True
            self._stop_heartbeat()
            await self._fail(_RELEASED, retry_at_ms=_now_ms() + _MIN_WAIT_MS)

    async def _fail(
        self, error: str, *, non_retryable: bool = False, retry_at_ms: int | None = None
    ) -> None:
        await _send_fail(
            self._client,
            self._lease,
            error,
            non_retryable=non_retryable,
            retry_at_ms=retry_at_ms,
            usage=self.activity.reported(),
            results=self.activity.report_results(),
        )


async def _send_fail(
    client: AsyncLbbClient,
    lease: Mapping[str, Any],
    error: str,
    *,
    non_retryable: bool = False,
    retry_at_ms: int | None = None,
    usage: models.WorkflowRunUsage | None = None,
    results: models.WorkflowRunResultsReport | None = None,
) -> None:
    """``turns/fail``, sent once. A turn that is not this worker's any more
    (``409``, ``404``) has nothing to fail. A body too large for the server
    (``413``) goes again without its results."""

    async def send(report: models.WorkflowRunResultsReport | None) -> None:
        await client._request(
            "POST",
            f"{_BASE}/turns/fail",
            body=models.WorkflowTurnFailRequest(
                **lease,
                error=error[:1000],
                non_retryable=non_retryable,
                retry_at_ms=retry_at_ms,
                usage=usage,
                results=report,
            ),
            options={"retry": False},
        )

    try:
        try:
            await send(results)
        except LbbError as refused:
            if refused.status_code != 413 or results is None:
                raise
            await send(None)
    except LbbError as refused:
        if not _gone(refused):
            raise


# ---- the worker ----------------------------------------------------------------------


def _transient(error: BaseException) -> bool:
    return isinstance(error, httpx.TransportError) or (
        isinstance(error, LbbError) and (error.status_code == 429 or error.status_code >= 500)
    )


class WorkflowWorker:
    """Claims the turns of its workflow definitions and runs them, at most
    ``concurrency`` at once.

    ``run()`` serves until SIGTERM or SIGINT. On SIGTERM the worker stops
    claiming, lets its held turns finish for ``drain_seconds``, and then
    releases the rest: each released turn fails with ``retry_at_ms``, so it
    counts no attempt, keeps its saved steps, and another worker claims it a
    second later. SIGINT (and a second SIGTERM) releases at once; a second
    SIGINT ends the process (``KeyboardInterrupt``). A claim that waits on
    the server when the stop comes runs to its answer, and a turn it claims
    is released at once. ``run_async()`` serves inside your own event loop
    until :meth:`stop`.
    """

    def __init__(
        self,
        client: AsyncLbbClient,
        definitions: Sequence[WorkflowDefinition[Any, Any]],
        *,
        worker: str,
        concurrency: int = 1,
        execution_reads: bool = False,
        progress_every_ms: int = _PROGRESS_EVERY_MS,
        drain_seconds: float = 30.0,
        claim_wait_ms: int = _CLAIM_WAIT_MS,
    ) -> None:
        keys = [(d.name, d.version) for d in definitions]
        if not keys or len(set(keys)) != len(keys):
            raise WorkflowError("Register distinct workflow name/version pairs")
        if not isinstance(worker, str) or not worker:
            raise WorkflowError("A worker needs an id (worker=...)")
        if not _whole(concurrency) or concurrency < 1:
            raise WorkflowError("concurrency is a whole number of at least 1")
        self._client = client
        self._definitions = {(d.name, d.version): d for d in definitions}
        self._worker = worker
        self._concurrency = concurrency
        self._execution_reads = execution_reads
        self._progress_every_ms = progress_every_ms
        self._drain_seconds = drain_seconds
        self._claim_wait_ms = claim_wait_ms
        self._turns: set[_Turn] = set()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._stopping: asyncio.Event | None = None
        self._hurry: asyncio.Event | None = None

    @property
    def concurrency(self) -> int:
        """The most turns this worker runs at once."""
        return self._concurrency

    def register(self, definition: WorkflowDefinition[Any, Any]) -> None:
        """Add a definition; the next claim names it."""
        key = (definition.name, definition.version)
        if key in self._definitions:
            raise WorkflowError("Register distinct workflow name/version pairs")
        self._definitions[key] = definition

    async def _claim(self, wait_ms: int) -> Mapping[str, Any] | None:
        body = models.WorkflowTurnClaimRequest(
            worker=self._worker,
            include_execution_credential=True if self._execution_reads else None,
            workflows=[
                models.WorkflowWorkerType(workflow_type=name, version=version)
                for name, version in self._definitions
            ],
            wait_ms=wait_ms,
        )
        claimed = await self._client._request(
            "POST", f"{_BASE}/turns/claim", body=body, options={"retry": False}
        )
        task = claimed.get("task") if isinstance(claimed, Mapping) else None
        return task if isinstance(task, Mapping) else None

    async def _execute(self, raw: Mapping[str, Any]) -> None:
        task = _read_task(raw)
        definition = self._definitions.get((task.workflow_type, task.version))
        if definition is None:
            raise WorkflowError("Server returned an unregistered workflow version")
        turn = _Turn(self, task, definition)
        self._turns.add(turn)
        try:
            await turn.run()
        finally:
            self._turns.discard(turn)

    async def _release_claimed(self, raw: Mapping[str, Any]) -> None:
        """Give back a turn the worker claimed while it stopped, without
        running it: it counts no attempt."""
        turn = raw.get("turn")
        if not isinstance(turn, Mapping):
            return
        lease = {
            "workflow_id": turn.get("workflow_id"),
            "turn": turn.get("number"),
            "token": raw.get("token"),
        }
        try:
            await _send_fail(self._client, lease, _RELEASED, retry_at_ms=_now_ms() + _MIN_WAIT_MS)
        except Exception as error:
            # The lease ends by itself; the server gives the turn to a worker then.
            _logger.warning("could not release a claimed turn: %s", _describe(error))

    async def run_once(self, *, wait_ms: int = 0) -> bool:
        """Claim one turn (waiting up to ``wait_ms`` for one) and run it to its
        end: ``False`` when there was none."""
        task = await self._claim(wait_ms)
        if task is None:
            return False
        await self._execute(task)
        return True

    async def run_async(self) -> None:
        """Serve until :meth:`stop`: claim while fewer than ``concurrency``
        turns run, and run each claimed turn in its own task. A claim that
        meets a network failure, a ``429`` or a ``5xx`` waits and claims
        again; any other refusal ends the loop with that error."""
        self._loop = asyncio.get_running_loop()
        self._stopping = asyncio.Event()
        self._hurry = asyncio.Event()
        slots = asyncio.Semaphore(self._concurrency)
        running: set[asyncio.Task[None]] = set()
        stopped = asyncio.ensure_future(self._stopping.wait())
        backoff = 0.0
        try:
            while not self._stopping.is_set():
                slot = asyncio.ensure_future(slots.acquire())
                await asyncio.wait({slot, stopped}, return_when=asyncio.FIRST_COMPLETED)
                if not slot.done():
                    slot.cancel()
                    break
                claim = asyncio.ensure_future(self._claim(self._claim_wait_ms))
                await asyncio.wait({claim, stopped}, return_when=asyncio.FIRST_COMPLETED)
                if not claim.done():
                    # The stop came while the claim waits on the server: a
                    # cancel could lose a turn it claims there, so it runs to
                    # its answer, and a claimed turn goes back at once.
                    await asyncio.wait({claim})
                    slots.release()
                    if not claim.cancelled() and claim.exception() is None:
                        claimed = claim.result()
                        if claimed is not None:
                            await self._release_claimed(claimed)
                    break
                try:
                    task = claim.result()
                except Exception as error:
                    slots.release()
                    if not _transient(error):
                        raise
                    backoff = min(30.0, max(1.0, backoff * 2))
                    _logger.warning(
                        "claim failed (%s); claiming again in %.0f s", _describe(error), backoff
                    )
                    try:
                        await asyncio.wait_for(asyncio.shield(stopped), backoff)
                    except asyncio.TimeoutError:
                        pass
                    continue
                backoff = 0.0
                if task is None:
                    slots.release()
                    continue
                turn = asyncio.ensure_future(self._serve(task))
                running.add(turn)
                turn.add_done_callback(running.discard)
                turn.add_done_callback(lambda _: slots.release())
        finally:
            stopped.cancel()
            await self._drain(running)

    async def _serve(self, raw: Mapping[str, Any]) -> None:
        try:
            await self._execute(raw)
        except Exception as error:
            # The lease ends by itself; the server gives the turn to a worker then.
            _logger.error(
                "turn %s failed in the worker: %s",
                raw.get("effect_prefix"),
                _describe(error),
                exc_info=error,
            )

    async def _drain(self, running: set[asyncio.Task[None]]) -> None:
        """Let the held turns finish for the drain time, then release the rest."""
        if not running:
            return
        assert self._hurry is not None
        loop = asyncio.get_running_loop()
        deadline = loop.time() + max(0.0, self._drain_seconds)
        hurry = asyncio.ensure_future(self._hurry.wait())
        try:
            while running and not self._hurry.is_set():
                remaining = deadline - loop.time()
                if remaining <= 0:
                    break
                await asyncio.wait(
                    {*running, hurry}, timeout=remaining, return_when=asyncio.FIRST_COMPLETED
                )
        finally:
            hurry.cancel()
        released = [turn for turn in list(self._turns) if turn.release()]
        if released:
            _logger.info("released %d held turn(s)", len(released))
        if running:
            await asyncio.wait(running)

    def stop(self, *, drain_seconds: float | None = None) -> None:
        """Stop claiming. Held turns finish within ``drain_seconds`` (default:
        the worker's), and the rest are released. ``drain_seconds=0``
        releases them at once. Safe to call from another thread or a signal
        handler."""
        loop, stopping, hurry = self._loop, self._stopping, self._hurry
        if loop is None or stopping is None or hurry is None:
            return

        def apply() -> None:
            if drain_seconds is not None:
                self._drain_seconds = min(self._drain_seconds, drain_seconds)
                if drain_seconds <= 0:
                    hurry.set()
            elif stopping.is_set():
                hurry.set()  # A second stop releases at once.
            stopping.set()

        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            running = None
        if running is loop:
            apply()
        else:
            loop.call_soon_threadsafe(apply)

    def run(self) -> None:
        """Serve until SIGTERM or SIGINT, in a new event loop.

        SIGTERM drains the held turns for ``drain_seconds`` and releases the
        rest; a second SIGTERM releases at once. SIGINT releases at once, and
        a second SIGINT ends the process with ``KeyboardInterrupt``. The
        handlers need the main thread: off it, ``run()`` installs none, and
        :meth:`stop` (from another thread) ends it.
        """
        asyncio.run(self._run_with_signals())

    async def _run_with_signals(self) -> None:
        loop = asyncio.get_running_loop()
        on_loop: list[int] = []
        previous: dict[int, Any] = {}

        def install(number: int, handler: Callable[[], None]) -> None:
            try:
                loop.add_signal_handler(number, handler)
                on_loop.append(number)
                return
            except (NotImplementedError, RuntimeError, ValueError):
                pass
            # Windows: the plain signal module. Its handler runs between two
            # bytecodes of the main thread, so it hands the stop to the loop.
            try:
                previous[number] = signal.signal(
                    number, lambda *_args: loop.call_soon_threadsafe(handler)
                )
            except ValueError:
                pass  # Not the main thread: no handler.

        def on_sigint() -> None:
            self.stop(drain_seconds=0)
            # A second Ctrl-C ends the process: the default handler raises
            # KeyboardInterrupt.
            if signal.SIGINT in on_loop:
                loop.remove_signal_handler(signal.SIGINT)
                on_loop.remove(signal.SIGINT)
            elif signal.SIGINT in previous:
                signal.signal(signal.SIGINT, signal.default_int_handler)
                previous.pop(signal.SIGINT)

        install(signal.SIGTERM, self.stop)
        install(signal.SIGINT, on_sigint)
        try:
            await self.run_async()
        finally:
            for number in on_loop:
                loop.remove_signal_handler(number)
            for number, handler in previous.items():
                signal.signal(number, handler)


__all__ = [
    "WorkflowContext",
    "WorkflowDefinition",
    "WorkflowError",
    "WorkflowResult",
    "WorkflowSignalResult",
    "WorkflowStepContext",
    "WorkflowWait",
    "WorkflowWorker",
    "fan_in_identity",
    "is_fan_in_identity",
    "workflow",
    "workflow_of_identity",
]
