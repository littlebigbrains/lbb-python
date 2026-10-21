"""Captured activities (``capture_activities``) and ``wait_for_review``: what
a capture checks, keeps and sends, as the TypeScript tests of
``captureActivities`` and ``waitForReview`` pin. The conformance suite runs
both with a real worker (``sdk/conformance/workflow-worker``)."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable
from types import SimpleNamespace
from typing import Any

import httpx
import pytest

from lbb import AsyncLbbClient
from lbb.workflows import (
    OwnedWrite,
    WorkflowError,
    WorkflowSignalResult,
    capture_activities,
    lbb_activities,
)

WRITE = OwnedWrite(
    subjects=[{"type": "Fixture", "name": "f1"}],
    properties=["width_cm"],
    entity_properties=[{"type": "Fixture", "name": "f1", "properties": {"width_cm": 55}}],
)
DRY_RUN = {"dry_run": True, "op_count": 1, "triplets": 0, "entity_properties": 1}
USAGE = {"model": "m", "tokens_in": 1, "tokens_out": 1, "cost_micro_usd": 1, "cached": False}


class Context:
    """The parts of a workflow context the activities use: steps run at
    once, and a signal wait answers what the test gives it."""

    def __init__(self, workflow_id: str = "asker", signals: list[Any] | None = None) -> None:
        self.workflow_id = workflow_id
        self.turn = 3
        self.keys: list[str] = []
        self.waits: list[dict[str, Any]] = []
        self.signals = list(signals or [])

    async def step(self, key: str, fn: Callable[[Any], Any], **options: Any) -> Any:
        self.keys.append(key)
        out = fn(SimpleNamespace(effect_id=f"stable/{key}", fence=None, graph=None))
        out = await out if isinstance(out, Awaitable) else out
        model = options.get("model")
        return model.model_validate(out.model_dump(mode="json")) if model else out

    async def wait_for_signal(self, key: str, *, name: str, timeout_ms: int) -> Any:
        self.waits.append({"key": key, "name": name, "timeout_ms": timeout_ms})
        return self.signals.pop(0) if self.signals else WorkflowSignalResult(False, None)


def run(
    answers: list[tuple[int, Any]],
    body: Callable[[AsyncLbbClient], Awaitable[None]],
) -> list[httpx.Request]:
    seen: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        status, answer = answers.pop(0) if answers else (200, {})
        return httpx.Response(status, json=answer)

    async def main() -> None:
        async with AsyncLbbClient(
            "http://h", graph="tender", retry_delay=0, transport=httpx.MockTransport(handle)
        ) as client:
            await body(client)

    asyncio.run(main())
    return seen


def sent(request: httpx.Request) -> Any:
    return json.loads(request.content) if request.content else None


def test_a_capture_checks_owned_writes_with_the_dry_run_and_keeps_them() -> None:
    async def body(client: AsyncLbbClient) -> None:
        capture = capture_activities(lbb_activities(client))
        ctx = Context()
        written = await capture.activities.write_owned(ctx, "write", WRITE)  # type: ignore[arg-type]
        assert ctx.keys == ["write"]
        assert written.commit_seq is None, "nothing was committed"
        filed = await capture.activities.review(
            {
                "kind": "value",
                "workflow": "sinks",
                "key": "starter:sinks:value:1:material",
                "title": "Set material of f1 to Keramik",
            }
        )
        assert getattr(filed.status, "value", filed.status) == "open"
        assert filed.origin.model_dump(mode="json", exclude_none=True) == {
            "kind": "agent",
            "id": "starter:sinks",
        }
        result = capture.result()
        assert [write.check for write in result.writes] == ["dry_run"]
        assert result.writes[0].errors == []
        assert result.writes[0].report is not None and result.writes[0].report.op_count == 1
        assert [review.request["title"] for review in result.reviews] == [
            "Set material of f1 to Keramik"
        ]
        capture.reset()
        assert capture.result().writes == [] and capture.result().reviews == []

    seen = run([(200, DRY_RUN)], body)
    assert len(seen) == 1, "a review item is kept, not filed"
    assert seen[0].url.path == "/v1/graph/commit"
    assert seen[0].url.params["dry_run"] == "true"
    assert "idempotency-key" not in seen[0].headers
    assert sent(seen[0])["replace_owned"] == {
        "subjects": [{"type": "Fixture", "name": "f1"}],
        "properties": ["width_cm"],
    }
    assert "workflow_fence" not in sent(seen[0])


def test_a_capture_checks_the_shape_first_then_the_dry_run_an_rdf_native_graph_refuses() -> None:
    refusal = "an RDF-native graph refuses a triplet / property commit dry run"

    async def body(client: AsyncLbbClient) -> None:
        capture = capture_activities(lbb_activities(client))
        wrong = OwnedWrite(
            subjects=WRITE.subjects,
            properties=WRITE.properties,
            entity_properties=WRITE.entity_properties,
            triplets=[
                {
                    "source": {"type": "Fixture", "name": "f1"},
                    "relation": "",
                    "target": {"type": "", "name": "s"},
                }
            ],
        )
        await capture.activities.write_owned(Context(), "write", wrong)  # type: ignore[arg-type]
        plain = OwnedWrite(subjects=[], entity_properties=WRITE.entity_properties)
        await capture.activities.write_owned(Context(), "plain", plain)  # type: ignore[arg-type]
        await capture.activities.write_owned(Context(), "again", WRITE)  # type: ignore[arg-type]
        shape, refused, again = capture.result().writes
        assert shape.check == "shape"
        assert shape.errors == [
            "triplets[0].target needs a type and a name",
            "triplets[0].relation is empty",
        ]
        # The dry run's refusal is the commit's: the write cannot land there.
        assert (refused.check, refused.errors, refused.report) == ("dry_run", [refusal], None)
        assert (again.check, again.errors) == ("dry_run", [])

    seen = run(
        [
            (
                400,
                {
                    "error": {
                        "type": "invalid_request_error",
                        "code": "rdf_native_graph",
                        "message": refusal,
                    }
                },
            ),
            (200, DRY_RUN),
        ],
        body,
    )
    assert len(seen) == 2, "a write of the wrong shape is not sent"


def test_a_capture_without_a_client_checks_the_shape_with_the_commits_limits() -> None:
    async def main() -> None:
        # Activities of the test's own, with no client to check with.
        offline = capture_activities(SimpleNamespace())  # type: ignore[arg-type]
        await offline.activities.write_owned(Context(), "write", WRITE)  # type: ignore[arg-type]
        many = OwnedWrite(
            subjects=WRITE.subjects,
            properties=[f"p{i}" for i in range(65)],
            entity_properties=WRITE.entity_properties,
        )
        await offline.activities.write_owned(Context(), "many", many)  # type: ignore[arg-type]
        ok, too_many = offline.result().writes
        assert (ok.check, ok.errors) == ("shape", [])
        assert too_many.errors == ["replace_owned.properties holds at most 64 names"]

    asyncio.run(main())


def test_a_captures_reason_and_decide_calls_name_the_test() -> None:
    async def body(client: AsyncLbbClient) -> None:
        capture = capture_activities(lbb_activities(client), workflow="sinks")
        await capture.activities.reason("p", {"type": "object"})
        await capture.activities.decide(
            {}, {"q": {"instructions": "?", "criteria": {"a": "a", "b": "b"}}}
        )

    seen = run([(200, {"value": {}, **USAGE}), (200, {"answers": {}, **USAGE})], body)
    assert [(request.url.path, sent(request)["test"]) for request in seen] == [
        ("/v1/workflows/activities/reason", {"workflow": "sinks"}),
        ("/v1/workflows/activities/decide", {"workflow": "sinks"}),
    ]


def test_a_capture_keeps_review_items_of_every_kind_and_files_none() -> None:
    question = {
        "kind": "value",
        "title": "Width of S 4.1",
        "key": "sinks:s41:width_cm",
        "question": {"property": "width_cm", "proposed": "60"},
        "workflow": "sinks",
    }

    async def body(client: AsyncLbbClient) -> None:
        capture = capture_activities(lbb_activities(client), workflow="sinks")
        item = await capture.activities.review(question)
        assert getattr(item.status, "value", item.status) == "open"
        assert getattr(item.kind, "value", item.kind) == "value"
        assert getattr(item.scope, "value", item.scope) == "test"
        change = await capture.activities.review({"title": "Add a class", "change": []})
        assert getattr(change.status, "value", change.status) == "open"
        ctx = Context()
        waited = await capture.activities.wait_for_review(
            ctx,  # type: ignore[arg-type]
            "ask",
            {**question, "key": "sinks:s41:depth_cm"},
            timeout_ms=60_000,
        )
        assert waited.answered is False and waited.answer is None
        assert ctx.keys == [] and ctx.waits == [], "the wait ends at once"
        # A trigger shard is refused, as the live call refuses it, and
        # nothing is kept.
        with pytest.raises(WorkflowError, match="needs a run with its own identity"):
            await capture.activities.wait_for_review(
                Context("starter.sinks"),  # type: ignore[arg-type]
                "ask",
                {**question, "key": "sinks:s41:height_cm"},
                timeout_ms=60_000,
            )
        assert [review.request["title"] for review in capture.result().reviews] == [
            "Width of S 4.1",
            "Add a class",
            "Width of S 4.1",
        ]
        preview = await capture.activities.evolve([{"op": "add_entity_type", "name": "Sink"}])
        assert preview.dry_run is True

    evolved = {
        "applied": [],
        "base_ontology_version": 1,
        "dry_run": True,
        "graph": {"tenant_id": "t", "graph_id": "tender", "branch_id": "main"},
        "messages": [],
        "no_op": False,
        "ontology_version": 1,
        "publishable": True,
    }
    seen = run([(200, evolved)], body)
    assert [request.url.path for request in seen] == ["/v1/ontology/evolve"]
    assert seen[0].url.params["dry_run"] == "true", "a test changes no ontology"


CHOICE = {
    "kind": "choice",
    "key": "asker:floor",
    "title": "Which floor covering?",
    "question": {"options": [{"id": "a", "label": "A"}, {"id": "b", "label": "B"}]},
}


def item(**more: Any) -> dict[str, Any]:
    return {
        "id": "rev_3",
        "object": "review_item",
        "key": "asker:floor",
        "kind": "choice",
        "scope": "live",
        "status": "open",
        "revision": 1,
        "title": "Which floor covering?",
        "origin": {"kind": "agent", "id": "asker"},
        "wait": {"identity": "asker", "turn": 3, "signal": "review.rev_3"},
        "created": "2026-10-21T10:00:00Z",
        "updated": "2026-10-21T10:00:00Z",
        **more,
    }


def test_wait_for_review_takes_an_answer_that_came_first() -> None:
    ctx = Context()

    async def body(client: AsyncLbbClient) -> None:
        waited = await lbb_activities(client).wait_for_review(
            ctx,  # type: ignore[arg-type]
            "floor",
            CHOICE,
            timeout_ms=1_000,
        )
        assert waited.answered is True
        assert waited.answer is not None
        assert waited.answer.model_dump(mode="json", exclude_none=True) == {"decision": "cant_tell"}

    seen = run([(200, item(status="answered", answer={"decision": "cant_tell"}))], body)
    assert ctx.keys == ["floor.ask"]
    assert ctx.waits == [], "an answered item needs no wait"
    # An item that names no workflow is filed by the run itself.
    assert sent(seen[0])["origin"] == {"kind": "agent", "id": "asker"}
    assert sent(seen[0])["wait"] == {"identity": "asker", "turn": 3, "signal": ""}
    assert "workflow" not in sent(seen[0])


def test_wait_for_review_is_refused_on_a_trigger_shard() -> None:
    async def body(client: AsyncLbbClient) -> None:
        with pytest.raises(WorkflowError, match="needs a run with its own identity"):
            await lbb_activities(client).wait_for_review(
                Context("starter.rooms"),  # type: ignore[arg-type]
                "floor",
                CHOICE,
                timeout_ms=1_000,
            )

    assert run([], body) == [], "a refused wait files nothing"
