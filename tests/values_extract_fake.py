"""A fake LBB for the Python values-extract example
(``sdk/python/examples/values_extract``): the workflow engine, the model
activities with rules a test gives, SPARQL rows a test gives, commits with
their idempotency keys, review items and requeue. The real ``WorkflowWorker``
and ``lbb_activities`` run against it."""

from __future__ import annotations

import asyncio
import hashlib
import json
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

import httpx

from lbb import AsyncLbbClient
from lbb.workflows import WorkflowWorker, lbb_activities

EXAMPLES = Path(__file__).resolve().parents[1] / "examples"
if str(EXAMPLES) not in sys.path:
    sys.path.insert(0, str(EXAMPLES))

from values_extract.workflow import DEFAULTS, Settings, create_workflow  # noqa: E402

BASE = "/v1/workflows/turns"

# The rows of a query: `s`, `label`, and `text` and a page box when the record has them.
Rows = Callable[[str], list[dict[str, str]]]
# The value of a reason request, or None for a refusal (422).
Reason = Callable[[dict[str, Any]], "dict[str, Any] | None"]
# The answers of a decide request, by question id.
Decide = Callable[[dict[str, Any]], dict[str, Any]]
# The answer a person gave to a value item, or None while it is open.
ReviewAnswer = Callable[[dict[str, Any]], "dict[str, Any] | None"]
# Why the graph refused an answer's value (`effects.commit_error`), or None.
ReviewRefused = Callable[[dict[str, Any]], "str | None"]

GRAPH = {"tenant_id": "t", "graph_id": "main", "branch_id": "main"}
TIME = "2026-10-21T10:00:00Z"


def iri_of(name: str) -> str:
    digest = hashlib.sha256(f"{DEFAULTS.class_name}:{name}".encode()).hexdigest()[:32]
    return f"https://littlebigbrain.com/e/{digest}"


def message(
    iris: list[str], removed: list[str] | None = None, *, workflow: str = "values-extract"
) -> dict[str, Any]:
    return {
        "type": "instances",
        "workflow": workflow,
        "graph": "main",
        "as_of_commit_seq": 3,
        "reason": "changed",
        "instances": iris,
        "removed": removed or [],
    }


class Killed(BaseException):
    """The worker's process dies: no failure and no completion are sent."""


class FakeLbb:
    """The routes the example calls, in memory. The engine saves each
    checkpoint on the task, so a task claimed again replays its steps."""

    def __init__(
        self,
        rows: Rows,
        *,
        reason: Reason,
        decide: Decide,
        review_status: Callable[[], str] = lambda: "open",
        review_answer: ReviewAnswer = lambda request: None,
        review_refused: ReviewRefused = lambda request: None,
    ) -> None:
        self.rows = rows
        self.reason = reason
        self.decide = decide
        self.review_status = review_status
        self.review_answer = review_answer
        self.review_refused = review_refused
        self.tasks: list[dict[str, Any]] = []
        self.pending: list[dict[str, Any]] = []
        self.saved: dict[str, list[dict[str, Any]]] = {}
        self.calls: list[tuple[str, Any, httpx.Headers]] = []
        self.model_calls: list[str] = []
        self.kept: dict[str, Any] = {}
        self.commits: dict[str, dict[str, Any]] = {}
        self.reviews: dict[str, dict[str, Any]] = {}
        # The review items of another kind than a change, by key.
        self.questions: dict[str, dict[str, Any]] = {}
        # Items asked again on new evidence: the answer in force they hold.
        self.previous: dict[str, dict[str, Any]] = {}
        self.requeued: list[str] = []
        self.completed: list[dict[str, Any]] = []
        self.crash_after: str | None = None

    def queue(self, message: dict[str, Any], *, state: Any = None) -> None:
        """A task for the message, claimed next."""
        number = len(self.tasks) + 1
        workflow_id = f"starter.{message['workflow']}"
        self.tasks.append(
            {
                "workflow_type": "values-extract",
                "state": state if state is not None else {},
                "token": f"token-{number}",
                "lease_ms": 60_000,
                "effect_prefix": f"e{number}",
                "turn": {
                    "workflow_id": workflow_id,
                    "number": number,
                    "message_id": f"{workflow_id}-{number}",
                    "message": message,
                    "version": "v1",
                    "status": "running",
                    "attempt": 1,
                    "created_at_ms": 1,
                    "updated_at_ms": 1,
                    "ready_at_ms": 1,
                    "steps": [],
                    "result": None,
                    "state_after": None,
                    "sequence": 1,
                    "run": {"id": f"run-{number}", "started_at_ms": 1, "queue_before": 1},
                },
            }
        )
        self.pending.append(self.tasks[-1])

    def claim_again(self, number: int) -> None:
        """The claim of a turn whose attempt ended without an answer: its saved
        steps come with it."""
        task = json.loads(json.dumps(self.tasks[number - 1]))
        task["token"] = f"{task['token']}-again"
        task["turn"]["attempt"] += 1
        task["turn"]["steps"] = self.saved.get(task["effect_prefix"], [])
        self.pending = [task]

    def sparql(self, query: str) -> dict[str, Any]:
        bindings = []
        for row in self.rows(query):
            binding = {"s": {"type": "uri", "value": row["s"]}}
            for name, value in row.items():
                if name != "s":
                    binding[name] = {"type": "literal", "value": value}
            bindings.append(binding)
        doc = {"head": {"vars": ["s", "label", "text"]}, "results": {"bindings": bindings}}
        return {"results": json.dumps(doc)}

    def model(self, kind: str, body: dict[str, Any]) -> httpx.Response:
        effect_id = body.get("effect_id")
        if effect_id in self.kept:
            return httpx.Response(200, json={**self.kept[effect_id], "cached": True})
        self.model_calls.append(effect_id)
        usage = {"model": "mock", "tokens_in": 10, "tokens_out": 5, "cost_micro_usd": 1}
        if kind == "reason":
            value = self.reason(body)
            if value is None:
                return httpx.Response(
                    422,
                    json={
                        "error": {
                            "type": "invalid_request_error",
                            "code": "activity_request_refused",
                            "message": "the model gave no usable answer",
                        }
                    },
                )
            answer = {"value": value, **usage, "cached": False}
        else:
            answer = {"answers": self.decide(body), **usage, "cached": False}
        self.kept[effect_id] = answer
        return httpx.Response(200, json=answer)

    def question(self, request: dict[str, Any]) -> dict[str, Any]:
        """A value item as the server keeps it: an answered one stays as
        answered, unless new evidence brings a value other than the answer in
        force and the one the person decided on, which asks again and keeps
        that answer as ``previous_answer``."""
        key = request["key"]
        before = self.questions.get(key)
        if before is not None:
            was = self.item_of(key, before)
            answer = was.get("answer") if was["status"] == "answered" else None
            in_force = answer is not None and answer["decision"] in ("accept", "correct")
            changed = (
                bool(before.get("evidence_hash") and request.get("evidence_hash"))
                and before["evidence_hash"] != request["evidence_hash"]
            )
            asked = request["question"].get("value")
            held = answer.get("value", before["question"].get("value")) if answer else None
            if in_force and changed and held != asked and before["question"].get("value") != asked:
                self.previous[key] = {**answer, "value": held}
            elif answer is not None:
                return was
        self.questions[key] = request
        return self.item_of(key, request)

    def item_of(self, key: str, request: dict[str, Any]) -> dict[str, Any]:
        previous = self.previous.get(key)
        # An item asked again waits for a new answer.
        answer = None if previous else self.review_answer(request)
        refused = self.review_refused(request) if answer else None
        item = {
            "id": f"rev_{key[-8:]}",
            "object": "review_item",
            "created": TIME,
            "updated": TIME,
            "kind": request["kind"],
            "scope": "live",
            # A refused answer does not stand: the item is open again.
            "status": "answered" if answer and not refused else "open",
            "key": key,
            "title": request["title"],
            "rationale": request.get("rationale"),
            "origin": request["origin"],
            "workflow": request.get("workflow"),
            "subject": request.get("subject"),
            "question": request["question"],
            "evidence": request.get("evidence", []),
            "evidence_hash": request.get("evidence_hash"),
            "facts": request.get("facts"),
            "revision": 1,
        }
        if previous:
            item["previous_answer"] = previous
        if answer:
            item["answer"] = answer
        if refused:
            item["effects"] = {"commit_error": refused, "delivery": {"state": "none"}}
        return item

    def change_item(self, key: str) -> dict[str, Any] | None:
        """A suggestion as a ``change`` review item, answered as its status says."""
        filed = self.reviews.get(key)
        if filed is None:
            return None
        decision = {"accepted": "accept", "dismissed": "reject"}.get(filed["status"])
        item = {
            "id": f"rev_{key[-8:]}",
            "object": "review_item",
            "created": TIME,
            "updated": TIME,
            "kind": "change",
            "scope": "live",
            "status": "answered"
            if decision
            else ("withdrawn" if filed["status"] == "superseded" else "open"),
            "key": key,
            "title": filed["title"],
            "origin": filed.get("origin") or {"kind": "agent"},
            "revision": 1,
        }
        if decision:
            item["answer"] = {"decision": decision}
        return item

    def suggestion(self, key: str) -> dict[str, Any]:
        filed = self.reviews[key]
        return {
            "suggestion_id": "sg_" + key[-8:],
            "graph": GRAPH,
            "key": key,
            "status": filed["status"],
            "title": filed["title"],
            "anchor": {"kind": "ontology"},
            "origin": filed.get("origin") or {"kind": "agent"},
            "change": [],
            "facts": filed.get("facts"),
            "revision": 1,
            "created_at": TIME,
            "updated_at": TIME,
        }

    async def handle(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        body = json.loads(request.content) if request.content else None
        self.calls.append((path, body, request.headers))
        if path == f"{BASE}/claim":
            task = self.pending.pop(0) if self.pending else None
            return httpx.Response(200, json={"task": task})
        if path == f"{BASE}/checkpoint":
            prefix = {t["token"]: t["effect_prefix"] for t in self.tasks}
            effect_prefix = prefix.get(body["token"].removesuffix("-again"))
            assert effect_prefix
            steps = self.saved.setdefault(effect_prefix, [])
            if body["position"] == len(steps):
                steps.append(
                    {
                        "key": body["key"],
                        "kind": body["kind"],
                        "output": body.get("output"),
                        "at_ms": 1,
                    }
                )
            return httpx.Response(200, json={})
        if path == f"{BASE}/complete":
            self.completed.append(body)
            return httpx.Response(200, json={})
        if path.startswith(BASE):
            return httpx.Response(200, json={})
        if path == "/v1/workflows/activities/reason":
            return self.model("reason", body)
        if path == "/v1/workflows/activities/decide":
            return self.model("decide", body)
        if path == "/v1/query/sparql-text":
            return httpx.Response(200, json=self.sparql(body["query"]))
        if path == "/v1/graph/commit":
            key = request.headers["idempotency-key"]
            replayed = key in self.commits
            self.commits.setdefault(key, body)
            if self.crash_after == "commit":
                self.crash_after = None
                raise Killed()
            return httpx.Response(
                200,
                json={
                    "commit_seq": list(self.commits).index(key) + 1,
                    "idempotency_key": key,
                    "replayed": replayed,
                    "entities_written": len(body.get("entity_properties", [])),
                    "edges_written": len(body.get("triplets", [])),
                },
            )
        if path == "/v1/reviews" and request.method == "POST":
            return httpx.Response(200, json=self.question(body))
        if path == "/v1/reviews":
            keys = [key for key in request.url.params.get("key", "").split(",") if key]
            data = [
                item
                for key in dict.fromkeys(keys)
                if (
                    item := self.item_of(key, self.questions[key])
                    if key in self.questions
                    else self.change_item(key)
                )
                is not None
            ]
            return httpx.Response(
                200, json={"object": "list", "data": data, "has_more": False, "next_cursor": None}
            )
        if path == "/v1/ontology/suggestions" and request.method == "GET":
            key = request.url.params.get("key")
            found = [self.suggestion(key)] if key in self.reviews else []
            return httpx.Response(
                200,
                json={
                    "graph": GRAPH,
                    "ontology_version": 1,
                    "counts": {"open": 0, "accepted": 0, "dismissed": 0, "superseded": 0},
                    "suggestions": [{**item, "comment_count": 0} for item in found],
                    "truncated": False,
                },
            )
        if path == "/v1/ontology/suggestions/detail":
            wanted = request.url.params.get("suggestion_id")
            key = next(k for k in self.reviews if "sg_" + k[-8:] == wanted)
            return httpx.Response(200, json=self.suggestion(key))
        if path == "/v1/ontology/suggestions":
            status = self.reviews.get(body["key"], {}).get("status") or self.review_status()
            self.reviews[body["key"]] = {**body, "status": status}
            return httpx.Response(
                200,
                json={
                    "suggestion_id": "sg_" + body["key"][-8:],
                    "graph": {"tenant_id": "t", "graph_id": "main", "branch_id": "main"},
                    "key": body["key"],
                    "status": status,
                    "title": body["title"],
                    "anchor": {"kind": "ontology"},
                    "origin": body["origin"],
                    "change": [],
                    "facts": body.get("facts"),
                    "revision": 1,
                    "created_at": "2026-10-21T10:00:00Z",
                    "updated_at": "2026-10-21T10:00:00Z",
                },
            )
        if path == "/v1/workflows/named/requeue":
            self.requeued += [item["instance"] for item in body["items"]]
            return httpx.Response(
                200,
                json={
                    "attempt": 1,
                    "requeued": [item["instance"] for item in body["items"]],
                    "exhausted": [],
                    "not_before": "2026-10-21T10:01:00Z",
                },
            )
        raise AssertionError(f"unexpected route {path}")

    def entity_rows(self, key: str) -> list[dict[str, Any]]:
        """The rows of the entity a commit key names, over every commit."""
        return [
            row
            for commit in self.commits.values()
            for row in commit.get("entity_properties", [])
            if row.get("key") == key
        ]

    def written(self, name: str) -> dict[str, Any]:
        """The values written on an instance: its owned properties, over every commit."""
        values: dict[str, Any] = {}
        for commit in self.commits.values():
            for row in commit.get("entity_properties", []):
                if row["type"] == DEFAULTS.class_name and row["name"] == name:
                    values.update(row["properties"])
        return values


def serve(fake: FakeLbb, settings: Settings = DEFAULTS, runs: int = 1) -> None:
    """Run the example's workflow with the real worker, ``runs`` claims."""

    async def main() -> None:
        async with AsyncLbbClient(
            "http://localhost:7400",
            graph="main",
            retry_delay=0,
            transport=httpx.MockTransport(fake.handle),
        ) as client:
            workflow = create_workflow(lbb_activities(client), settings)
            worker = WorkflowWorker(client, [workflow], worker="py-1")
            for _ in range(runs):
                await worker.run_once()

    asyncio.run(main())
