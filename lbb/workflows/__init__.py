"""Durable workflows in Python: a worker that claims turns and runs your
handlers, with the rules and the names of the TypeScript ``WorkflowWorker``.

A turn runs one message. Named steps (``ctx.step``) save their outputs, so a
turn that runs again after a crash replays them by key and kind, in order, and
does each effect once: each step's ``effect_id`` stays the same for the
instance, the message and the step. State, reply and an optional continuation
commit in one write when the turn completes::

    from lbb import AsyncLbbClient
    from lbb.workflows import WorkflowResult, WorkflowWorker, workflow

    @workflow(name="counter", version="v1", initial_state={"count": 0})
    async def counter(ctx, state, message):
        value = await ctx.step("add", lambda s: state["count"] + message["add"])
        return WorkflowResult(state={"count": value}, result=value)

    client = AsyncLbbClient("http://127.0.0.1:7400", api_key="...", graph="main")
    WorkflowWorker(client, [counter], worker="counter-1").run()

``client.workflows`` (an :class:`~lbb.AsyncLbbClient`) starts instances and
holds the engine's blobs. The guide is
``https://docs.littlebigbrain.com/guides/python-worker/``; the HTTP routes are
in ``docs/reference/workflows.md``.
"""

from ..entities import ENTITY_ID_KEY_SENTINEL, ENTITY_IRI_NAMESPACE, entity_id_from_iri, entity_ref
from ._activities import LbbActivities, is_activity_refused, lbb_activities, split_in_halves
from ._handles import WorkflowBlobsNamespace, WorkflowHandle, WorkflowNamespace
from ._triggered import (
    TRIGGER_MESSAGE_TYPE,
    FailedInstance,
    OwnedWrite,
    OwnedWriteResult,
    TriggeredWorkflowDefinition,
    owned_commit_body,
    parse_trigger_message,
    requeue_failed,
    triggered_workflow,
    triggered_workflow_state,
    write_owned,
)
from ._worker import (
    WorkflowContext,
    WorkflowDefinition,
    WorkflowError,
    WorkflowResult,
    WorkflowSignalResult,
    WorkflowStepContext,
    WorkflowWait,
    WorkflowWorker,
    fan_in_identity,
    is_fan_in_identity,
    workflow,
    workflow_of_identity,
)

# TODO(W5 #1274): the review primitive. When #1274 lands, add the `review`
# kinds (value, choice, grade, answer) to `LbbActivities.review`, and
# `wait_for_review` beside `wait_for_signal`, as the TypeScript SDK has them.
# TODO(W4 #1280): `capture_activities`, the TypeScript `captureActivities`
# for workflow tests, when #1280 lands.

__all__ = [
    "ENTITY_ID_KEY_SENTINEL",
    "ENTITY_IRI_NAMESPACE",
    "FailedInstance",
    "LbbActivities",
    "OwnedWrite",
    "OwnedWriteResult",
    "TRIGGER_MESSAGE_TYPE",
    "TriggeredWorkflowDefinition",
    "WorkflowBlobsNamespace",
    "WorkflowContext",
    "WorkflowDefinition",
    "WorkflowError",
    "WorkflowHandle",
    "WorkflowNamespace",
    "WorkflowResult",
    "WorkflowSignalResult",
    "WorkflowStepContext",
    "WorkflowWait",
    "WorkflowWorker",
    "entity_id_from_iri",
    "entity_ref",
    "fan_in_identity",
    "is_activity_refused",
    "is_fan_in_identity",
    "lbb_activities",
    "owned_commit_body",
    "parse_trigger_message",
    "requeue_failed",
    "split_in_halves",
    "triggered_workflow",
    "triggered_workflow_state",
    "workflow",
    "workflow_of_identity",
    "write_owned",
]
