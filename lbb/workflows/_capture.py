"""Activities that write nothing, for a workflow test: the TypeScript
``captureActivities`` (``sdk/typescript/src/activities.ts``).

Run a workflow on chosen inputs with them and read what it would write. The
models and the reads run (and spend) as they do live; each ``write_owned`` is
checked (a local check of its shape, then the commit's dry run,
``POST /v1/graph/commit?dry_run=true``) and kept; each ``review`` of every
kind is kept and filed nowhere; ``wait_for_review`` keeps its item and ends
at once without an answer (on a trigger shard it is refused, as live);
``requeue`` sends nothing back to the watch; and ``evolve`` runs as a dry
run.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Literal

from .. import models
from .._client_base import LbbError, SparqlResults, _coerce_body
from ._activities import (
    _REVIEW_ITEM_KINDS,
    LbbActivities,
    ReviewWaitResult,
    _item_kind,
    _refuse_trigger_shard,
    _review_item_body,
)
from ._triggered import (
    EdgeIdempotency,
    FailedInstance,
    OwnedWrite,
    OwnedWriteResult,
    WriteFn,
    owned_commit_body,
)
from ._worker import StepKind, WorkflowContext, WorkflowStepContext, _call

if TYPE_CHECKING:
    from .._async_client import AsyncLbbClient

# The entities and the names one `replace_owned` holds at most.
_REPLACE_OWNED_ENTITIES_MAX = 1_000
_REPLACE_OWNED_NAMES_MAX = 64


@dataclass(frozen=True)
class CapturedWrite:
    """One owned write a captured workflow made, as it would commit it."""

    #: The step key the workflow wrote under.
    step_key: str
    #: The graph it would write: the client's (``None``), or ``graph=``.
    graph: str | None
    #: The commit body: the owned triplets and fields and their ``replace_owned``.
    body: models.TripletCommitFile
    #: How it was checked: ``dry_run`` (the server's own validation), or
    #: ``shape`` (a local check of the body, for a capture without a client,
    #: or a body the local check refuses).
    check: Literal["dry_run", "shape"]
    #: Why the check refused it; empty when it passes.
    errors: list[str]
    #: What the dry run reports the write would do.
    report: models.GraphCommitDryRunResponse | None = None


@dataclass(frozen=True)
class CapturedReview:
    """One review item a captured workflow filed: a change (a suggestion) or
    a question of another kind."""

    request: Mapping[str, Any]


@dataclass(frozen=True)
class ActivityCaptureResult:
    """What a captured workflow would have written and filed, in call order."""

    writes: list[CapturedWrite] = field(default_factory=list)
    reviews: list[CapturedReview] = field(default_factory=list)


def _shape_errors(body: models.TripletCommitFile) -> list[str]:
    """A local check of an owned commit body: each named entity has a type
    and a name (or an id), each edge a source, a relation and a target, each
    field a name, and ``replace_owned`` holds what the commit accepts. The
    errors, empty when it passes."""
    errors: list[str] = []
    data = _coerce_body(body)

    def named(entity: Any, where: str) -> None:
        if not isinstance(entity, Mapping) or not (
            str(entity.get("type") or "").strip() and str(entity.get("name") or "").strip()
        ):
            errors.append(f"{where} needs a type and a name")

    for i, triplet in enumerate(data.get("triplets") or []):
        named(triplet.get("source"), f"triplets[{i}].source")
        named(triplet.get("target"), f"triplets[{i}].target")
        if not str(triplet.get("relation") or "").strip():
            errors.append(f"triplets[{i}].relation is empty")
    for i, entity in enumerate(data.get("entity_properties") or []):
        if not entity.get("entity_id"):
            named(entity, f"entity_properties[{i}]")
        properties = entity.get("properties") or {}
        fields = (
            [p.get("field") for p in properties]
            if isinstance(properties, list)
            else list(properties)
        )
        for j, name in enumerate(fields):
            if not str(name or "").strip():
                errors.append(f"entity_properties[{i}].properties[{j}] has no field")
    owned = data.get("replace_owned")
    if owned:
        # The commit's own checks of `replace_owned`.
        subjects = owned.get("subjects") or []
        retract = owned.get("retract_entities") or []
        entities = len(subjects) + len(retract)
        if entities == 0:
            errors.append(
                "replace_owned needs at least one subject or one entity in retract_entities"
            )
        if entities > _REPLACE_OWNED_ENTITIES_MAX:
            errors.append(
                f"replace_owned names {entities} entities; at most {_REPLACE_OWNED_ENTITIES_MAX}"
            )
        for name in ("relations", "properties"):
            if len(owned.get(name) or []) > _REPLACE_OWNED_NAMES_MAX:
                errors.append(
                    f"replace_owned.{name} holds at most {_REPLACE_OWNED_NAMES_MAX} names"
                )
        for i, subject in enumerate(subjects):
            named(subject, f"replace_owned.subjects[{i}]")
        for i, entity in enumerate(retract):
            named(entity, f"replace_owned.retract_entities[{i}]")
    return errors


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _captured_item(body: Mapping[str, Any], n: int) -> models.ReviewItem:
    """The open review item a captured question stands for; nothing is filed."""
    now = _now()
    return models.ReviewItem.model_validate(
        {
            **body,
            "id": f"captured_{n}",
            "object": "review_item",
            "key": body.get("key") or f"captured_{n}",
            "scope": body.get("scope") or "test",
            "status": "open",
            "revision": 1,
            "created": now,
            "updated": now,
        }
    )


def _captured_suggestion(
    request: Mapping[str, Any], workflow: str | None, n: int
) -> models.OntologyChangeSuggestion:
    item = {k: v for k, v in request.items() if v is not None and k != "workflow"}
    now = _now()
    return models.OntologyChangeSuggestion.model_validate(
        {
            **item,
            "suggestion_id": f"captured_{n}",
            "key": item.get("key") or f"captured_{n}",
            "anchor": item.get("anchor") or {"kind": "ontology"},
            "change": item.get("change") or [],
            "origin": item.get("origin")
            or {"kind": "agent", **({"id": f"starter:{workflow}"} if workflow else {})},
            "graph": {"tenant_id": "", "graph_id": "", "branch_id": "main"},
            "revision": 1,
            "status": "open",
            "created_at": now,
            "updated_at": now,
        }
    )


class _CapturedActivities(LbbActivities):
    """The activities of a capture: the models and the reads of ``inner``,
    and writes and reviews that are kept instead of sent."""

    def __init__(self, inner: LbbActivities, client: AsyncLbbClient | None, workflow: str) -> None:
        super().__init__(client)  # type: ignore[arg-type]
        self._inner = inner
        self._check_client = client
        self._test = {"workflow": workflow}
        self.writes: list[CapturedWrite] = []
        self.reviews: list[CapturedReview] = []
        self._filed = 0

    # -- the models and the reads, as they run live --

    async def reason(
        self, prompt: str, schema: Mapping[str, Any], **options: Any
    ) -> models.ReasonActivityResponse:
        return await self._inner.reason(prompt, schema, **{**options, "test": self._test})

    async def decide(
        self,
        state: Any,
        questions: Mapping[str, models.DecideActivityQuestion | Mapping[str, Any]],
        **options: Any,
    ) -> models.DecideActivityResponse:
        return await self._inner.decide(state, questions, **{**options, "test": self._test})

    async def embed(self, texts: Sequence[str], **options: Any) -> models.EmbedActivityResponse:
        return await self._inner.embed(texts, **options)

    async def pages(self, sha256: str, *, from_: int = 0, to: int | None = None) -> dict[str, Any]:
        return await self._inner.pages(sha256, from_=from_, to=to)

    async def sparql(self, query: str, **options: Any) -> SparqlResults:
        return await self._inner.sparql(query, **options)

    async def ontology(self) -> models.OntologyView:
        return await self._inner.ontology()

    async def review_items(self, keys: Sequence[str]) -> list[models.ReviewItem]:
        return await self._inner.review_items(keys)

    async def list_reviews(self, **options: Any) -> models.OntologyChangeSuggestionList:
        return await self._inner.list_reviews(**options)

    async def review_item(self, key: str) -> models.OntologyChangeSuggestion | None:
        return await self._inner.review_item(key)

    async def evolve(
        self, ops: Sequence[Any], *, dry_run: bool = False
    ) -> models.OntologyEvolveResponse:
        # A test changes no ontology.
        return await self._inner.evolve(ops, dry_run=True)

    # -- what is kept --

    async def _check(
        self, body: models.TripletCommitFile, graph: str | None
    ) -> tuple[Literal["dry_run", "shape"], list[str], models.GraphCommitDryRunResponse | None]:
        errors = _shape_errors(body)
        client = self._check_client
        if client is None or errors:
            return "shape", errors, None
        params: dict[str, Any] = {"dry_run": "true"}
        if graph:
            params["graph"] = graph
        try:
            raw = await client._request("POST", "/v1/graph/commit", params=params, body=body)
        except LbbError as error:
            if error.status_code >= 500 or error.status_code == 429:
                raise
            # The dry run refuses what the commit refuses: on an RDF-native
            # graph (`rdf_native_graph`) the commit of an owned write is
            # refused too.
            return "dry_run", [str(error)], None
        return "dry_run", [], models.GraphCommitDryRunResponse.model_validate(raw)

    async def write_owned(
        self,
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
        async def keep(step: WorkflowStepContext) -> OwnedWriteResult:
            planned = await _call(write, step) if callable(write) else write
            body = owned_commit_body(planned, edge_idempotency)
            if body is not None:
                check, errors, report = await self._check(body, graph)
                self.writes.append(
                    CapturedWrite(
                        step_key=step_key,
                        graph=graph,
                        body=body,
                        check=check,
                        errors=errors,
                        report=report,
                    )
                )
            return OwnedWriteResult(
                commit_seq=None,
                idempotency_key=step.effect_id,
                replayed=False,
                owned=None,
                entities_written=0,
                edges_written=0,
            )

        return await ctx.step(
            step_key,
            keep,
            model=OwnedWriteResult,
            kind=kind,
            timeout_ms=timeout_ms,
            heartbeat_timeout_ms=heartbeat_timeout_ms,
        )

    async def requeue(
        self,
        ctx: WorkflowContext,
        step_key: str,
        failed: Sequence[FailedInstance],
        *,
        graph: str | None = None,
    ) -> models.WorkflowRequeueResponse | None:
        # A test sends nothing back to the watch.
        return None

    async def review(  # type: ignore[override]
        self, request: Any, *, workflow: str | None = None
    ) -> models.ReviewItem | models.OntologyChangeSuggestion:
        # Every kind of review item is kept, not filed.
        kept = dict(_coerce_body(request))
        self.reviews.append(CapturedReview(request=kept))
        self._filed += 1
        if _item_kind(kept) in _REVIEW_ITEM_KINDS:
            return _captured_item(_review_item_body(kept, workflow), self._filed)
        return _captured_suggestion(kept, workflow, self._filed)

    async def wait_for_review(
        self,
        ctx: WorkflowContext,
        key: str,
        request: models.ReviewItemCreateRequest | Mapping[str, Any],
        *,
        timeout_ms: int,
        workflow: str | Mapping[str, Any] | None = None,
    ) -> ReviewWaitResult:
        # An item a test asks is kept and never answered: the wait ends at once.
        # A trigger shard is refused, as the live call refuses it.
        _refuse_trigger_shard(ctx)
        kept = dict(_coerce_body(request))
        self.reviews.append(CapturedReview(request=kept))
        self._filed += 1
        item = _captured_item(_review_item_body(kept, workflow), self._filed)
        return ReviewWaitResult(answered=False, answer=None, item=item)


class ActivityCapture:
    """Activities that write nothing, and what they kept."""

    def __init__(self, activities: _CapturedActivities) -> None:
        self._activities = activities

    @property
    def activities(self) -> LbbActivities:
        """The activities to hand the workflow. The models and the reads run
        as they do live; ``write_owned`` checks its write and keeps it instead
        of committing it, and ``review`` keeps its item instead of filing it
        (the item it answers is ``open``); ``evolve`` runs as a dry run."""
        return self._activities

    def result(self) -> ActivityCaptureResult:
        """What was kept so far."""
        return ActivityCaptureResult(
            writes=list(self._activities.writes), reviews=list(self._activities.reviews)
        )

    def reset(self) -> None:
        """Forget what was kept."""
        self._activities.writes.clear()
        self._activities.reviews.clear()


def capture_activities(
    activities: LbbActivities,
    *,
    client: AsyncLbbClient | None = None,
    workflow: str = "test",
) -> ActivityCapture:
    """Activities that write nothing: run a workflow on chosen inputs with
    them, and read what it would write (``result()``).

    The models and the reads run (and spend) as they do live; each
    ``write_owned`` is checked (a local check of its shape, then the commit's
    dry run, ``POST /v1/graph/commit?dry_run=true``) and kept; each ``review``
    of every kind is kept and filed nowhere; ``wait_for_review`` keeps its
    item and ends at once without an answer (on a trigger shard it is
    refused with the live call's ``WorkflowError``); ``requeue`` sends nothing back
    to the watch (it answers ``None``); and ``evolve`` runs as a dry run. The
    reads (``review_items``, ``list_reviews``, ``review_item``) run. On an
    RDF-native graph the dry run refuses the write, as the commit would
    (``rdf_native_graph``).

    The ``reason`` and ``decide`` calls name the test (``test: {workflow}``,
    default ``test``): the call log shows them under ``tests/<workflow>``, and
    calibration, model checks and trials leave them out. ``client`` checks the
    writes; it defaults to the client of ``activities``. Without one, the
    writes get the local check only::

        capture = capture_activities(lbb_activities(client), workflow="sinks")
        workflow = create_workflow(capture.activities)
        # … run the workflow on a trigger message …
        result = capture.result()
    """
    check_client = client if client is not None else getattr(activities, "_client", None)
    return ActivityCapture(_CapturedActivities(activities, check_client, workflow))


__all__ = [
    "ActivityCapture",
    "ActivityCaptureResult",
    "CapturedReview",
    "CapturedWrite",
    "capture_activities",
]
