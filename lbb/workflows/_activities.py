"""LBB activities: the calls a workflow's code makes from a durable step.

``reason``, ``decide`` and ``embed`` run LBB's own models
(``POST /v1/workflows/activities/*``): LBB meters them against the
environment's model budget for workflows and writes them to the call log.
Pass the step's ``effect_id`` and a step that runs again after a crash gets
the answer it got the first time, at no cost. The other activities are thin
helpers over routes the client already has: parsed pages, SPARQL, owned
writes, review items and ontology changes. A model activity called from a
step of a :class:`~lbb.workflows.WorkflowWorker` turn names the turn's run and
the step (``workflow``), so the call log links the call to the run and the
step, and its usage adds up for the turn's completion: the worker's code does
nothing for it.
Reference: ``docs/reference/workflow-activities.md``.
"""

from __future__ import annotations

from collections.abc import Awaitable, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, TypeVar, overload

from pydantic import BaseModel

from .. import models
from .._client_base import LbbError, RequestOptions, SparqlResults, _coerce_body
from ._triggered import (
    EdgeIdempotency,
    FailedInstance,
    OwnedWrite,
    OwnedWriteResult,
    WriteFn,
    requeue_failed,
    write_owned,
)
from ._worker import (
    StepKind,
    WorkflowContext,
    WorkflowError,
    WorkflowStepContext,
    _activity_turn,
    _ActivityTurn,
)

if TYPE_CHECKING:
    from .._async_client import AsyncLbbClient

AnswerT = TypeVar("AnswerT", bound=BaseModel)

#: Keys one read of :meth:`LbbActivities.review_items` names, at most (the
#: list route's bound).
REVIEW_KEYS_PER_READ = 100
#: The review kinds ``POST /v1/reviews`` files; a ``change`` is a suggestion.
_REVIEW_ITEM_KINDS = ("value", "choice", "grade", "answer")

#: Output tokens of a ``reason`` call can take minutes: its default timeout.
REASON_TIMEOUT_SECONDS = 300.0
#: Pages one ``GET /v1/files/pages`` returns at most.
PAGES_PER_CALL = 50


def is_activity_refused(error: BaseException) -> bool:
    """Whether ``error`` is the refusal of a model activity:
    ``422 activity_request_refused``.

    The model provider refused the request as too large or malformed, or the
    reasoning model gave no usable answer. The same request fails again;
    split it (see :func:`split_in_halves`), make it smaller, or raise
    ``max_tokens``.
    """
    return isinstance(error, LbbError) and error.code == "activity_request_refused"


def split_in_halves(items: Sequence[Any]) -> tuple[list[Any], list[Any]] | None:
    """The two halves of ``items`` (the first one the smaller when the count is
    odd), or ``None`` for fewer than two items, which cannot be split. The
    split of a refused request: send each half as its own request."""
    if len(items) < 2:
        return None
    middle = len(items) // 2
    return list(items[:middle]), list(items[middle:])


@dataclass(frozen=True)
class ReviewWaitResult:
    """What :meth:`LbbActivities.wait_for_review` returns: the answer, or none
    when the wait timed out (``answered`` false, ``answer`` ``None``), and the
    item as it was read last."""

    answered: bool
    answer: models.ReviewAnswer | None
    item: models.ReviewItem


def _review_item_body(
    request: Any,
    workflow: str | Mapping[str, Any] | None = None,
    run: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """The body of ``POST /v1/reviews`` for a request (the TypeScript
    ``reviewItemBody``). ``workflow`` (or the request's own ``workflow``)
    names the workflow that asks, by its name or as a
    ``ReviewWorkflowOrigin``; ``run`` is the run that waits for the answer
    (``identity`` and ``turn``). An item that names no workflow is filed by
    the run itself: its origin id is the run's identity."""
    body = {k: v for k, v in dict(_coerce_body(request)).items() if v is not None}
    named: Any = body.pop("workflow", None)
    if named is None:
        named = workflow
    if isinstance(named, str):
        named = {"name": named}
    elif isinstance(named, Mapping):
        named = {k: v for k, v in _coerce_body(named).items() if v is not None}
    asking = {**named, **run} if named and run else named
    if body.get("origin") is None:
        origin: dict[str, Any] = {"kind": "agent"}
        if named and named.get("name"):
            origin["id"] = f"starter:{named['name']}"
        elif run:
            origin["id"] = run["identity"]
        body["origin"] = origin
    if asking:
        body["workflow"] = asking
    if run:
        body["wait"] = {**run, "signal": ""}
    return body


def _refuse_trigger_shard(ctx: WorkflowContext) -> None:
    """A trigger shard (``starter.<name>``) serves many inputs, and a wait
    would hold them all: ``wait_for_review`` refuses it, live and in a
    capture."""
    if ctx.workflow_id.startswith("starter."):
        raise WorkflowError(
            f"wait_for_review needs a run with its own identity: {ctx.workflow_id} serves "
            "many inputs, and a wait would hold them all. File the item with review() "
            "and take the answer with a review trigger."
        )


def _item_kind(request: Any) -> Any:
    kind = _coerce_body(request).get("kind") if request is not None else None
    return getattr(kind, "value", kind)


async def _counted(
    turn: _ActivityTurn | None, answer: Awaitable[AnswerT]
) -> AnswerT:
    """Add a model activity's usage to its turn: an answer that is not
    ``cached`` is one call; a refusal (422) or a model that did not answer
    (5xx) is a call that failed. A kept answer cost nothing and is no call."""
    if turn is None:
        return await answer
    try:
        answered = await answer
    except LbbError as error:
        if error.status_code == 422 or error.status_code >= 500:
            turn.add(calls=1, errors=1)
        raise
    if not getattr(answered, "cached", False):
        turn.add(
            calls=1,
            tokens_in=getattr(answered, "tokens_in", 0) or 0,
            tokens_out=getattr(answered, "tokens_out", 0) or 0,
            cost_micro_usd=getattr(answered, "cost_micro_usd", 0) or 0,
        )
    return answered


class LbbActivities:
    """The activities of a workflow, bound to one async client and its graph
    (a workflow worker serves the graph of its client). Make one with
    :func:`lbb_activities`; a test can hand a workflow its own object with
    the same methods::

        lbb = lbb_activities(client)
        answer = await ctx.step(
            "define-p3",
            lambda s: lbb.reason(prompt, schema, effect_id=s.effect_id, label="define p3"),
            model=models.ReasonActivityResponse,
        )
    """

    def __init__(self, client: AsyncLbbClient) -> None:
        self._client = client

    def _call(
        self,
        effect_id: str | None,
        label: str | None,
        instance: str | None,
        extra: RequestOptions | None = None,
    ) -> tuple[dict[str, Any], RequestOptions, _ActivityTurn | None]:
        """The common fields and call options of a model activity. With an
        effect id a retry is safe (a repeat returns the kept answer), so 5xx
        and network failures are retried; without one only a ``429`` the
        server returns before it runs the request is."""
        # The turn of the step that makes the call (none outside a turn), and
        # the step whose effect id is the call's or starts it (none for another id).
        turn = _activity_turn(effect_id)
        fields: dict[str, Any] = {}
        if effect_id is not None:
            fields["effect_id"] = effect_id
        if label is not None:
            fields["label"] = label
        if turn is not None:
            workflow = dict(turn.workflow)
            step = turn.step_of(effect_id)
            if step is not None:
                workflow["step"] = step
            if instance is not None:
                workflow["instance"] = instance
            fields["workflow"] = models.ActivityWorkflow(**workflow)
        options: RequestOptions = {"retry": True if effect_id is not None else "rate_limited"}
        options.update(extra or {})
        return fields, options, turn

    async def reason(
        self,
        prompt: str,
        schema: Mapping[str, Any],
        *,
        system: str | None = None,
        max_tokens: int | None = None,
        effect_id: str | None = None,
        label: str | None = None,
        instance: str | None = None,
        test: models.ActivityTest | Mapping[str, Any] | None = None,
    ) -> models.ReasonActivityResponse:
        """The reasoning model answers ``prompt`` with JSON that follows
        ``schema`` (``POST /v1/workflows/activities/reason``).

        ``effect_id`` is the effect id of the step that makes the call: pass
        ``s.effect_id`` of ``ctx.step(key, lambda s: ...)`` as it is. The server
        keeps the first answer under it, and the same request again returns
        that answer with ``cached: True`` at no cost. A step that makes several
        calls of one activity gives each its own id, such as
        ``f"{s.effect_id}.2"``. ``label`` names the call in the call log (at
        most 64 characters); ``instance`` says what the call works on (an
        instance IRI or a document's name). A request the model cannot answer
        fails with ``422 activity_request_refused``: see
        :func:`is_activity_refused`. One attempt waits up to 5 minutes.
        ``test`` (``{"workflow": name}``) marks the call as a workflow test's:
        :func:`capture_activities` sets it.
        """
        fields, options, turn = self._call(
            effect_id, label, instance, {"timeout": REASON_TIMEOUT_SECONDS}
        )
        body = models.ReasonActivityRequest.model_validate(
            {
                "prompt": prompt,
                "schema": dict(schema),
                "system": system,
                "max_tokens": max_tokens,
                "test": _coerce_body(test) if test is not None else None,
                **fields,
            }
        )
        return await _counted(
            turn,
            self._client._model_request(
                models.ReasonActivityResponse,
                "POST",
                "/v1/workflows/activities/reason",
                body=body,
                options=options,
            ),
        )

    async def decide(
        self,
        state: Any,
        questions: Mapping[str, models.DecideActivityQuestion | Mapping[str, Any]],
        *,
        effect_id: str | None = None,
        label: str | None = None,
        instance: str | None = None,
        test: models.ActivityTest | Mapping[str, Any] | None = None,
    ) -> models.DecideActivityResponse:
        """The decision model answers each choice question about ``state`` with
        one option, a confidence and the probability of each option
        (``POST /v1/workflows/activities/decide``).

        ``questions`` maps a question id to ``{"instructions": ...,
        "criteria": {option id: what the option means}}``. A question the
        model did not answer is missing from ``answers``. ``test`` marks the
        call as a workflow test's, as for :meth:`reason`.
        """
        fields, options, turn = self._call(effect_id, label, instance)
        body = models.DecideActivityRequest(
            state=state,
            questions={
                key: models.DecideActivityQuestion.model_validate(_coerce_body(question))
                for key, question in questions.items()
            },
            test=models.ActivityTest.model_validate(_coerce_body(test)) if test else None,
            **fields,
        )
        return await _counted(
            turn,
            self._client._model_request(
                models.DecideActivityResponse,
                "POST",
                "/v1/workflows/activities/decide",
                body=body,
                options=options,
            ),
        )

    async def embed(
        self,
        texts: Sequence[str],
        *,
        effect_id: str | None = None,
        label: str | None = None,
        instance: str | None = None,
    ) -> models.EmbedActivityResponse:
        """The server's embedding model embeds ``texts``: 1 to 256, each at
        most 8,000 characters (``POST /v1/workflows/activities/embed``)."""
        if isinstance(texts, str):
            raise TypeError("texts is a sequence of strings, not one string")
        fields, options, turn = self._call(effect_id, label, instance)
        body = models.EmbedActivityRequest(texts=[str(text) for text in texts], **fields)
        return await _counted(
            turn,
            self._client._model_request(
                models.EmbedActivityResponse,
                "POST",
                "/v1/workflows/activities/embed",
                body=body,
                options=options,
            ),
        )

    async def pages(self, sha256: str, *, from_: int = 0, to: int | None = None) -> dict[str, Any]:
        """The parsed pages of a file, each line with its id, text, box and
        words (``GET /v1/files/pages``, 50 pages per call). ``to`` is the
        first page not read; without it, it reads to the end of the file."""
        first = await self._client.files.pages(
            sha256,
            from_=from_,
            to=min(to if to is not None else from_ + PAGES_PER_CALL, from_ + PAGES_PER_CALL),
        )
        end = min(to if to is not None else first["page_count"], first["page_count"])
        pages = list(first["pages"])
        at = first["to"]
        while at < end:
            more = await self._client.files.pages(
                sha256, from_=at, to=min(end, at + PAGES_PER_CALL)
            )
            pages.extend(more["pages"])
            at += PAGES_PER_CALL
        return {**first, "from": from_, "to": max(first["to"], end), "pages": pages}

    async def sparql(
        self,
        query: str,
        *,
        as_of_commit_seq: int | None = None,
        consistency: str | None = None,
        limit: int | None = None,
    ) -> SparqlResults:
        """A SPARQL query (SELECT or ASK), its rows flattened to
        ``{variable: value}`` in ``.rows`` (``POST /v1/query/sparql-text``).
        Pass ``as_of_commit_seq`` to read at a trigger message's commit;
        ``consistency="strong"`` reads the commits that are not published yet,
        your own writes included."""
        return await self._client.sparql(
            query, as_of_commit_seq=as_of_commit_seq, consistency=consistency, limit=limit
        )

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
        """Commit an owned write inside the step ``step_key``, with the step's
        effect id as the idempotency key: :func:`~lbb.workflows.write_owned`
        bound to this client."""
        return await write_owned(
            self._client,
            ctx,
            step_key,
            write,
            graph=graph,
            edge_idempotency=edge_idempotency,
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
        """Send the instances a turn could not do back to the watch, inside the
        step ``step_key``: :func:`~lbb.workflows.requeue_failed` bound to this
        client. Past 3 attempts the instances are exhausted. Without failed
        instances it calls nothing."""
        return await requeue_failed(self._client, ctx, step_key, failed, graph=graph)

    @overload
    async def review(
        self,
        request: models.ReviewItemCreateRequest,
        *,
        workflow: str | None = None,
    ) -> models.ReviewItem: ...

    @overload
    async def review(
        self,
        request: models.OntologyChangeSuggestionCreateRequest,
        *,
        workflow: str | None = None,
    ) -> models.OntologyChangeSuggestion: ...

    @overload
    async def review(
        self,
        request: Mapping[str, Any],
        *,
        workflow: str | None = None,
    ) -> Any: ...

    async def review(
        self,
        request: models.ReviewItemCreateRequest
        | models.OntologyChangeSuggestionCreateRequest
        | Mapping[str, Any],
        *,
        workflow: str | None = None,
    ) -> models.ReviewItem | models.OntologyChangeSuggestion:
        """Ask a person. A request of kind ``value``, ``choice``, ``grade`` or
        ``answer`` files a review item (``POST /v1/reviews``) and returns a
        :class:`~lbb.models.ReviewItem`: a value to confirm or correct, a
        choice, a grade or a free-text answer. Any other request files a
        suggestion (``POST /v1/ontology/suggestions``): facts or ontology
        changes for a person to accept.

        ``workflow`` names the triggered workflow that files it (pass
        ``message.workflow``): without an ``origin`` in the request, the
        item's origin is then ``{"kind": "agent", "id": "starter:<workflow>"}``,
        so the workflow's status counts it. The same ``key`` again updates the
        item, so a failed call is retried; an answered review item comes back
        with its answer.
        """
        if _item_kind(request) in _REVIEW_ITEM_KINDS:
            return await self._create_item(_review_item_body(request, workflow))
        body = dict(_coerce_body(request))
        if body.get("origin") is None:
            body["origin"] = {"kind": "agent", **({"id": f"starter:{workflow}"} if workflow else {})}
        return await self._client.ontology.suggestion_create(
            models.OntologyChangeSuggestionCreateRequest.model_validate(body)
        )

    async def review_items(self, keys: Sequence[str]) -> list[models.ReviewItem]:
        """The review items filed under ``keys``, of every status and kind
        (``GET /v1/reviews?key=…``, 100 keys a read): a workflow reads them
        before it writes, so a person's answer wins over what it would write
        itself, and an item asked again on new evidence keeps the answer in
        force (``previous_answer``) until a new one. A suggestion is a
        ``change`` item, answered as its status says."""
        unique = list(dict.fromkeys(keys))
        out: list[models.ReviewItem] = []
        for at in range(0, len(unique), REVIEW_KEYS_PER_READ):
            chunk = unique[at : at + REVIEW_KEYS_PER_READ]
            raw = await self._client.reviews.list(key=chunk, limit=len(chunk))
            out.extend(models.ReviewItemList.model_validate(raw).data)
        return out

    async def _create_item(self, body: Mapping[str, Any]) -> models.ReviewItem:
        raw = await self._client.reviews.create(
            models.ReviewItemCreateRequest.model_validate(body).model_dump(
                mode="json", by_alias=True, exclude_none=True
            )
        )
        return models.ReviewItem.model_validate(raw)

    async def wait_for_review(
        self,
        ctx: WorkflowContext,
        key: str,
        request: models.ReviewItemCreateRequest | Mapping[str, Any],
        *,
        timeout_ms: int,
        workflow: str | Mapping[str, Any] | None = None,
    ) -> ReviewWaitResult:
        """Ask a person and wait for the answer without holding a worker.

        The item (of kind ``value``, ``choice``, ``grade`` or ``answer``) is
        filed in the step ``<key>.ask`` with this run as the one that waits;
        then the turn waits for the answer's signal (``<key>.answer``, at most
        ``timeout_ms``). An answer that came before the wait is taken at once.
        When the wait times out, the step ``<key>.read`` reads the item again,
        so an answer whose signal was lost still counts. ``workflow`` names the
        workflow that asks, as for :meth:`review`.

        Only for a run with its own identity: a trigger shard
        (``starter.<name>``) serves many inputs, and a wait would hold them
        all, so it is refused there; file with :meth:`review` and take the
        answer with a review trigger instead.
        """
        _refuse_trigger_shard(ctx)
        run = {"identity": ctx.workflow_id, "turn": ctx.turn}
        body = _review_item_body(request, workflow, run)

        async def ask(_: WorkflowStepContext) -> models.ReviewItem:
            return await self._create_item(body)

        item = await ctx.step(f"{key}.ask", ask, model=models.ReviewItem)
        if getattr(item.status, "value", item.status) != "open" and item.answer is not None:
            return ReviewWaitResult(answered=True, answer=item.answer, item=item)
        signal = item.wait.signal if item.wait is not None else None
        if not signal:
            raise WorkflowError(f"review item {item.id} names no signal to wait for")
        got = await ctx.wait_for_signal(f"{key}.answer", name=signal, timeout_ms=timeout_ms)
        if got.received:
            answer = models.ReviewAnswer.model_validate(got.value)
            return ReviewWaitResult(answered=True, answer=answer, item=item)

        # The wait timed out: the answer may have come and its signal not (a
        # dropped event). Read the item again.
        async def read(_: WorkflowStepContext) -> models.ReviewItem:
            return models.ReviewItem.model_validate(await self._client.reviews.get(item.id))

        now = await ctx.step(f"{key}.read", read, model=models.ReviewItem)
        if getattr(now.status, "value", now.status) == "answered" and now.answer is not None:
            return ReviewWaitResult(answered=True, answer=now.answer, item=now)
        return ReviewWaitResult(answered=False, answer=None, item=now)

    async def list_reviews(
        self,
        *,
        status: str | None = None,
        origin_id: str | None = None,
        key: str | None = None,
        anchor: str | None = None,
        limit: int | None = None,
    ) -> models.OntologyChangeSuggestionList:
        """The review items, newest update first, with counts per status
        (``GET /v1/ontology/suggestions``). Filter by ``origin_id``
        (``starter:<workflow>``, ``fit:<source>``), ``status``, ``key`` or
        ``anchor``."""
        return await self._client.ontology.suggestions(
            status=status, origin_id=origin_id, key=key, anchor=anchor, limit=limit
        )

    async def review_item(self, key: str) -> models.OntologyChangeSuggestion | None:
        """The suggestion (a ``change`` review item) filed under ``key``, with
        its evidence and facts, or ``None`` when none is: a merge of new
        evidence into an item reads it first. :meth:`review_items` reads the
        items of every kind."""
        listed = await self._client.ontology.suggestions(key=key, limit=1)
        found = next((item for item in listed.suggestions if item.key == key), None)
        if found is None:
            return None
        return await self._client.ontology.suggestion_get(found.suggestion_id)

    async def evolve(
        self, ops: Sequence[Any], *, dry_run: bool = False
    ) -> models.OntologyEvolveResponse:
        """Change the ontology (``POST /v1/ontology/evolve``). With
        ``dry_run=True`` it writes nothing and answers what the change would
        do: ``no_op`` when the ontology has it already."""
        body = models.OntologyEvolveRequest.model_validate(
            {"ops": [_coerce_body(op) for op in ops]}
        )
        return await self._client.ontology.evolve(body, dry_run=dry_run)

    async def ontology(self) -> models.OntologyView:
        """The graph's ontology: classes, relations and properties
        (``GET /v1/ontology``)."""
        return await self._client.ontology.view()


def lbb_activities(client: AsyncLbbClient) -> LbbActivities:
    """The LBB activities of ``client``, for the graph the client is scoped to
    (a workflow worker serves the graph of its client)."""
    return LbbActivities(client)


__all__ = [
    "LbbActivities",
    "ReviewWaitResult",
    "lbb_activities",
    "is_activity_refused",
    "split_in_halves",
]
