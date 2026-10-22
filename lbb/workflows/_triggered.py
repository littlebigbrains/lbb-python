"""Developer workflows (``workflow@v1``).

A triggered workflow of the starter workflow ``workflow`` sends the new and
changed instances of the class it watches to a workflow of your worker, as
trigger messages of type ``instances``. :func:`triggered_workflow` defines
that workflow: it checks each message and hands it to ``on_instances`` with
the workflow's state. :func:`write_owned` commits what your code owns on the
instances with ``replace_owned``, inside a named step, with the step's effect
id as the commit's idempotency key. The TypeScript SDK has the same calls
(``sdk/typescript/src/triggered-workflow-worker.ts``).
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal, TypeVar, cast, overload

from pydantic import BaseModel

from .. import models
from .._client_base import _coerce_body
from ..entities import ENTITY_ID_KEY_SENTINEL, ENTITY_IRI_NAMESPACE, entity_id_from_iri, entity_ref
from ._results import owned_write_inputs, requeued_inputs
from ._worker import (
    StepKind,
    WorkflowContext,
    WorkflowDefinition,
    WorkflowError,
    WorkflowResult,
    WorkflowStepContext,
    _call,
    _copy,
    is_fan_in_identity,
)

if TYPE_CHECKING:
    from .._async_client import AsyncLbbClient

S = TypeVar("S")

#: The ``type`` of every trigger message.
TRIGGER_MESSAGE_TYPE = "instances"

_REASONS = {reason.value for reason in models.WorkflowTriggerReason}


def _is_name(value: Any) -> bool:
    return isinstance(value, str) and len(value) > 0


def _is_string_list(value: Any) -> bool:
    return isinstance(value, list) and all(isinstance(item, str) for item in value)


def parse_trigger_message(value: Any, where: str = "message") -> models.WorkflowTriggerMessage:
    """Check that ``value`` is a trigger message and return it.

    ``workflow`` names the triggered workflow; a message stored before that
    field had its name carries it as ``starter``, which is read when
    ``workflow`` is absent. ``removed`` may be absent (read as empty). A
    ``reason`` this SDK does not know passes through as text, so a worker
    keeps running when the server adds one. Anything else raises a
    :class:`WorkflowError`, which fails the turn without a retry: a message of
    another shape does not become valid on a retry.
    """

    def refuse(problem: str) -> WorkflowError:
        return WorkflowError(f"The {where} is not a trigger message: {problem}")

    if isinstance(value, BaseModel):
        value = _coerce_body(value)
    if not isinstance(value, Mapping):
        raise refuse("it is not a JSON object")
    if value.get("type") != TRIGGER_MESSAGE_TYPE:
        raise refuse(f'type is {value.get("type")!r}, not "{TRIGGER_MESSAGE_TYPE}"')
    fields = dict(value)
    if "workflow" not in fields and _is_name(fields.get("starter")):
        fields["workflow"] = fields.pop("starter")
    if not _is_name(fields.get("workflow")):
        raise refuse("workflow is not a name")
    if not _is_name(fields.get("graph")):
        raise refuse("graph is not a graph id")
    seq = fields.get("as_of_commit_seq")
    if not isinstance(seq, int) or isinstance(seq, bool) or seq < 0:
        raise refuse("as_of_commit_seq is not a commit number")
    reason = fields.get("reason")
    if not _is_name(reason):
        raise refuse("reason is not a string")
    if not _is_string_list(fields.get("instances")):
        raise refuse("instances is not a list of IRIs")
    fields["removed"] = fields.get("removed") if fields.get("removed") is not None else []
    if not _is_string_list(fields["removed"]):
        raise refuse("removed is not a list of IRIs")
    if reason in _REASONS:
        return models.WorkflowTriggerMessage.model_validate(fields)
    # A reason of a newer server: checked as a known one, kept as its text.
    message = models.WorkflowTriggerMessage.model_validate({**fields, "reason": "changed"})
    return message.model_copy(update={"reason": reason})


def triggered_workflow_state(initial_state: S, saved: Any) -> S:
    """The state a developer workflow's handler receives: ``initial_state``
    merged under the saved state.

    The server creates the instance ``starter.<name>`` with the state ``{}``,
    so the first turn would otherwise read no keys at all. Keys of the saved
    state win. A ``None``, absent or empty saved state gives
    ``initial_state``. When either side is not a JSON object, a saved state
    other than those wins as it is.
    """
    if saved is None or (isinstance(saved, Mapping) and not saved):
        return cast(S, _copy(initial_state))
    if isinstance(initial_state, Mapping) and isinstance(saved, Mapping):
        return cast(S, {**_copy(initial_state), **saved})
    return cast(S, saved)


InstancesHandler = Callable[
    [WorkflowContext, models.WorkflowTriggerMessage, S], Awaitable["WorkflowResult[S] | None"]
]
FanInHandler = Callable[[WorkflowContext, Any, S], Awaitable["WorkflowResult[S] | None"]]


class TriggeredWorkflowDefinition(WorkflowDefinition[S, Any]):
    """The workflow of a developer workflow. Register it with a ``WorkflowWorker``;
    create the triggered workflow with ``workflow={"workflow_type": name,
    "version": version}`` (``client.use_workflow_starter``). Creating it
    creates the instance ``starter.<name>``, so the definition is never
    started by hand."""

    def __init__(
        self,
        name: str,
        version: str,
        initial_state: S,
        on_instances: InstancesHandler[S],
        on_fan_in: FanInHandler[S] | None = None,
    ) -> None:
        super().__init__(name, version, initial_state, self._on_message)
        self._on_instances = on_instances
        self._on_fan_in = on_fan_in

    def on_fan_in(self, handler: FanInHandler[S]) -> FanInHandler[S]:
        """Set the handler of the workflow's fan-in identity
        (``starter.<name>.<fan_in>``, ``fan_in`` of ``PUT /v1/workflows/named``).

        The shards' turns send it their outputs (``generated``, ``decided``)
        with deterministic ids. Read what the message names from where the
        outputs are kept (the graph, a blob), so the turns after a build find
        nothing new. A fan-in message without this handler fails the turn.
        """
        self._on_fan_in = handler
        return handler

    async def _on_message(
        self, ctx: WorkflowContext, saved: Any, message: Any
    ) -> WorkflowResult[S]:
        state = triggered_workflow_state(self.initial_state, saved)
        if is_fan_in_identity(ctx.workflow_id):
            if self._on_fan_in is None:
                raise WorkflowError(
                    f"{ctx.workflow_id} is a fan-in identity, and the workflow has no on_fan_in"
                )
            turn = await self._on_fan_in(ctx, message, state)
            if turn is None:
                return WorkflowResult(state=state)
            return WorkflowResult(state=turn.state, result=turn.result)
        turn = await self._on_instances(ctx, parse_trigger_message(message), state)
        if turn is None:
            return WorkflowResult(state=state)
        continuation = turn.continuation
        if continuation is not None:
            continuation = {
                "message": parse_trigger_message(continuation.get("message"), "continuation")
            }
        return WorkflowResult(state=turn.state, result=turn.result, continuation=continuation)


@overload
def triggered_workflow(
    *, name: str, version: str, initial_state: Any = None
) -> Callable[[InstancesHandler[Any]], TriggeredWorkflowDefinition[Any]]: ...
@overload
def triggered_workflow(
    *,
    name: str,
    version: str,
    initial_state: Any = None,
    on_instances: InstancesHandler[Any],
    on_fan_in: FanInHandler[Any] | None = None,
) -> TriggeredWorkflowDefinition[Any]: ...
def triggered_workflow(
    *,
    name: str,
    version: str,
    initial_state: Any = None,
    on_instances: InstancesHandler[Any] | None = None,
    on_fan_in: FanInHandler[Any] | None = None,
) -> Any:
    """Define the workflow of a developer workflow.

    ``name`` and ``version`` are the ``workflow_type`` and ``version`` the
    triggered workflow names. ``initial_state`` (default ``{}``) is merged
    under the saved state on each turn (see :func:`triggered_workflow_state`).
    ``on_instances(ctx, message, state)`` handles one message: read
    ``message.instances`` at ``message.as_of_commit_seq`` (or later), do the
    work inside ``ctx.step``, and write with :func:`write_owned`. Drop what
    you wrote for ``message.removed``. It returns a ``WorkflowResult``, or
    ``None`` to keep the state. Use it as a decorator::

        @triggered_workflow(name="word-count", version="v1")
        async def word_count(ctx, message, state):
            ...
    """
    initial = {} if initial_state is None else initial_state
    if on_instances is not None:
        return TriggeredWorkflowDefinition(name, version, initial, on_instances, on_fan_in)

    def define(handler: InstancesHandler[Any]) -> TriggeredWorkflowDefinition[Any]:
        return TriggeredWorkflowDefinition(name, version, initial, handler, on_fan_in)

    return define


# ---- owned writes -----------------------------------------------------------------

Entity = models.NamedEntityInput | Mapping[str, Any]


@dataclass
class OwnedWrite:
    """What a writer owns on its instances, and what it writes there now. The
    shapes are those of ``POST /v1/graph/commit``: generated models
    (``models.NamedEntityInput``, ``models.TripletInput``,
    ``models.EntityPropertiesInput``) or their JSON."""

    #: The instances this write is for. For the instances of a trigger
    #: message, ``[entity_ref(iri, type) for iri in message.instances]``.
    subjects: Sequence[Entity]
    #: Owned out-edge relations of the subjects.
    relations: Sequence[str] = ()
    #: Owned property fields of the subjects.
    properties: Sequence[str] = ()
    #: Entities the writer made and no longer makes.
    retract_entities: Sequence[Entity] = ()
    #: The owned edges to write now.
    triplets: Sequence[models.TripletInput | Mapping[str, Any]] = field(default_factory=list)
    #: The owned fields to write now.
    entity_properties: Sequence[models.EntityPropertiesInput | Mapping[str, Any]] = field(
        default_factory=list
    )


class OwnedWriteResult(BaseModel):
    """What :func:`write_owned` saved as its step's result."""

    #: The commit, or ``None`` when there was nothing to write.
    commit_seq: int | None
    #: The step's effect id, sent as the commit's idempotency key.
    idempotency_key: str
    #: The server answered with an earlier commit of the same key.
    replayed: bool
    #: What ``replace_owned`` removed; ``None`` on a replay or without subjects.
    owned: models.OwnedOutputsOutcome | None
    entities_written: int
    edges_written: int


EdgeIdempotency = models.EdgeIdempotencyMode | Literal["skip_unchanged", "append"]


def owned_commit_body(
    write: OwnedWrite, edge_idempotency: EdgeIdempotency = "skip_unchanged"
) -> models.TripletCommitFile | None:
    """The commit body of an owned write, or ``None`` when it writes nothing.
    The ``replace_owned`` part is present when the write names subjects or
    entities to retract."""
    subjects = [_coerce_body(item) for item in write.subjects]
    retract = [_coerce_body(item) for item in write.retract_entities]
    triplets = [_coerce_body(item) for item in write.triplets]
    properties = [_coerce_body(item) for item in write.entity_properties]
    if not subjects and not retract and not triplets and not properties:
        return None
    body: dict[str, Any] = {}
    if triplets:
        body["triplets"] = triplets
    if properties:
        body["entity_properties"] = properties
    body["edge_idempotency"] = models.EdgeIdempotencyMode(edge_idempotency).value
    if subjects or retract:
        replace: dict[str, Any] = {"subjects": subjects}
        if write.relations:
            replace["relations"] = list(write.relations)
        if write.properties:
            replace["properties"] = list(write.properties)
        if retract:
            replace["retract_entities"] = retract
        body["replace_owned"] = replace
    return models.TripletCommitFile.model_validate(body)


WriteFn = Callable[[WorkflowStepContext], "Awaitable[OwnedWrite] | OwnedWrite"]


async def write_owned(
    client: AsyncLbbClient,
    ctx: WorkflowContext,
    step_key: str,
    write: OwnedWrite | WriteFn,
    *,
    graph: str | None = None,
    edge_idempotency: EdgeIdempotency = "skip_unchanged",
    kind: StepKind = "step",
    timeout_ms: int = 300_000,
    heartbeat_timeout_ms: int | None = None,
) -> OwnedWriteResult:
    """Commit an owned write inside the step ``step_key``.

    In the same commit the server removes the owned edges and fields of
    ``subjects`` that this write does not set again, so a rerun replaces the
    earlier output. The step's effect id is the commit's idempotency key: a
    turn that runs again after a crash sends the same key, and the server
    answers with the first commit (``replayed: True``) instead of writing
    twice. A replayed turn whose step was saved does not call the server at
    all. The commit carries the step's fence when it writes the turn's graph,
    so a worker whose lease ended commits nothing new
    (``409 workflow_fence_lost``).

    Build the write from the step's inputs only (the message and earlier step
    results), so a second attempt sends the same body. ``write`` can be a
    function of the step's context that returns the write: it runs inside the
    step, so it can read inputs the turn does not save, and report progress.
    A replayed step does not call it. ``graph`` is the graph to write
    (default: the client's); pass ``message.graph``. ``edge_idempotency``
    (default ``skip_unchanged``) writes no new edge events for a rerun that
    writes the same output.
    """

    # The turn's run, when the worker records results. The step keeps what it
    # reports in its saved output, so a replay reports it again.
    turn = getattr(getattr(ctx, "_turn", None), "activity", None)
    results = turn.results if turn is not None else None

    async def commit(step: WorkflowStepContext) -> OwnedWriteResult:
        planned = await _call(write, step) if callable(write) else write
        body = owned_commit_body(planned, edge_idempotency)
        if body is None:
            return OwnedWriteResult(
                commit_seq=None,
                idempotency_key=step.effect_id,
                replayed=False,
                owned=None,
                entities_written=0,
                edges_written=0,
            )
        # The fence: a worker whose lease ended commits nothing new. It names a
        # turn of the turn's graph, so a write to another graph carries none:
        # that graph's engine holds no such turn. A worker whose client names
        # no graph takes the write's graph (`message.graph`) for the turn's.
        if step.fence is not None and (graph is None or step.graph is None or graph == step.graph):
            body = body.model_copy(update={"workflow_fence": step.fence})
        response = await client._request(
            "POST",
            "/v1/graph/commit",
            params={"graph": graph} if graph else None,
            body=body,
            idempotency_key=step.effect_id,
            options={"retry": True},
        )
        # The fields of `GraphCommitResponse` the step saves: the rest of the
        # answer may grow without failing a write that landed.
        owned = response.get("owned")
        # The step's own write, also when the server answers it as a replay of
        # a request this step sent before.
        if results is not None:
            for item in owned_write_inputs(body, "written"):
                results.add(item)
        return OwnedWriteResult(
            commit_seq=response["commit_seq"],
            idempotency_key=response.get("idempotency_key") or step.effect_id,
            replayed=bool(response.get("replayed")),
            owned=models.OwnedOutputsOutcome.model_validate(owned) if owned else None,
            entities_written=response.get("entities_written", 0),
            edges_written=response.get("edges_written", 0),
        )

    return await ctx.step(
        step_key,
        commit,
        model=OwnedWriteResult,
        kind=kind,
        timeout_ms=timeout_ms,
        heartbeat_timeout_ms=heartbeat_timeout_ms,
    )


@dataclass(frozen=True)
class FailedInstance:
    """One instance a turn could not do, and why."""

    #: The instance IRI, as the message named it.
    instance: str
    #: Why it failed, at most 1,000 characters.
    error: str | None = None


async def requeue_failed(
    client: AsyncLbbClient,
    ctx: WorkflowContext,
    step_key: str,
    failed: Sequence[FailedInstance],
    *,
    graph: str | None = None,
) -> models.WorkflowRequeueResponse | None:
    """Ask the watch to send instances again later
    (``POST /v1/workflows/named/requeue``), inside the step ``step_key``.

    The instances this turn could not do go back into the queue on this shard,
    with origin ``requeue``, after a backoff (1, 4, then 16 minutes). The
    server takes the attempt from this turn's message; past 3 the instances
    are exhausted and not sent again (the answer's ``exhausted``). Record the
    failure in the turn's result and counters and complete the turn: the
    shard goes on. Without failed instances it calls nothing.
    """
    if not failed:
        return None
    name = ctx.workflow_name
    if not name:
        raise WorkflowError(
            f"{ctx.workflow_id} is no shard of a named workflow: "
            "only a watch sends instances again"
        )
    body = models.WorkflowRequeueRequest(
        workflow_id=ctx.workflow_id,
        turn=ctx.turn,
        items=[
            models.WorkflowRequeueItem(
                instance=item.instance,
                error=item.error[:1000] if item.error is not None else None,
            )
            for item in failed
        ],
    )

    # The requeued instances failed in this run (a replay reports them too).
    turn = getattr(getattr(ctx, "_turn", None), "activity", None)
    if turn is not None and turn.results is not None:
        for item in requeued_inputs(failed):
            turn.results.add(item)

    async def requeue() -> models.WorkflowRequeueResponse:
        params: dict[str, Any] = {"name": name}
        if graph:
            params["graph"] = graph
        return await client._model_request(
            models.WorkflowRequeueResponse,
            "POST",
            "/v1/workflows/named/requeue",
            params=params,
            body=body,
            options={"retry": True},
        )

    return await ctx.step(step_key, requeue, model=models.WorkflowRequeueResponse)


__all__ = [
    "ENTITY_ID_KEY_SENTINEL",
    "ENTITY_IRI_NAMESPACE",
    "FailedInstance",
    "OwnedWrite",
    "OwnedWriteResult",
    "TRIGGER_MESSAGE_TYPE",
    "TriggeredWorkflowDefinition",
    "entity_id_from_iri",
    "entity_ref",
    "owned_commit_body",
    "parse_trigger_message",
    "requeue_failed",
    "triggered_workflow",
    "triggered_workflow_state",
    "write_owned",
]
