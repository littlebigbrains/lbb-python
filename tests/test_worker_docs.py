"""Every Python example of the guide "Run a worker in Python"
(``docs-site/src/content/docs/guides/python-worker.md``) runs here, in order,
against a fake LBB: a small workflow engine with an inbox per instance,
signals, saved steps, continuations and triggered workflows. Then each
workflow the page defines runs one message with the real ``WorkflowWorker``."""

from __future__ import annotations

import asyncio
import json
import re
import signal
from pathlib import Path
from typing import Any

import httpx
import pytest

import lbb
from lbb.workflows import WorkflowDefinition

NOTE = "https://littlebigbrain.com/e/0123456789abcdef0123456789abcdef"


def _page() -> Path | None:
    for parent in Path(__file__).resolve().parents:
        page = parent / "docs-site" / "src" / "content" / "docs" / "guides" / "python-worker.md"
        if page.is_file():
            return page
    return None


def _turn(workflow_id: str, number: int, message: Any, **more: Any) -> dict[str, Any]:
    return {
        "workflow_id": workflow_id,
        "number": number,
        "message_id": f"{workflow_id}-{number}",
        "message": message,
        "version": "v1",
        "status": "queued",
        "attempt": 1,
        "created_at_ms": 1,
        "updated_at_ms": 1,
        "ready_at_ms": 1,
        "steps": [],
        "result": None,
        "state_after": None,
        "sequence": 1,
        **more,
    }


CONFLICT = {"error": {"type": "conflict_error", "code": "conflict", "message": "stale lease"}}


class FakeEngine:
    """The routes the guide's examples call, with the engine's rules for a
    turn: a claimed turn is held by its token until it completes, fails or
    sleeps, and no other claim gets it; a turn route answers ``409`` to a
    token that does not hold the turn; a repeated completion with the same
    body answers as the first one did. A sleep is due at once, and a signal
    wait answers at once: with a sent signal, or as timed out."""

    def __init__(self) -> None:
        self.instances: dict[str, dict[str, Any]] = {}
        self.signals: dict[str, list[dict[str, Any]]] = {}
        self.completed: list[tuple[str, dict[str, Any]]] = []
        self.failed: list[dict[str, Any]] = []
        self.commits: list[dict[str, Any]] = []
        self.dry_runs: list[dict[str, Any]] = []
        self.reviews: list[dict[str, Any]] = []
        self.claims = 0
        # SIGTERM went to the serving worker since the last claimed turn.
        self.signalled = False

    def create(self, id: str, workflow_type: str, state: Any = None, version: str = "v1") -> None:
        self.instances.setdefault(
            id,
            {
                "type": workflow_type,
                "version": version,
                "state": state,
                "inbox": [],
                "turn": 0,
                # The token of the claim that holds the head turn, if any.
                "holder": None,
                "claims": 0,
                # Each completed turn's body, for a repeat of its completion.
                "completions": {},
            },
        )

    def send(self, id: str, message: Any) -> dict[str, Any]:
        instance = self.instances[id]
        instance["inbox"].append({"message": message, "steps": []})
        return _turn(id, instance["turn"] + len(instance["inbox"]), message)

    def task(self, id: str) -> dict[str, Any]:
        instance = self.instances[id]
        head = instance["inbox"][0]
        number = instance["turn"] + 1
        instance["claims"] += 1
        instance["holder"] = token = f"{id}:{number}:{instance['claims']}"
        return {
            "workflow_type": instance["type"],
            "state": instance["state"],
            "token": token,
            "lease_ms": 60_000,
            "effect_prefix": f"{id}-{number}",
            "turn": _turn(
                id,
                number,
                head["message"],
                version=instance["version"],
                status="running",
                steps=head["steps"],
                run={"id": f"run-{id}-{number}", "started_at_ms": 1, "queue_before": 1},
            ),
        }

    def holds(self, body: dict[str, Any]) -> bool:
        """Whether the request's token holds the turn it names."""
        instance = self.instances.get(body.get("workflow_id", ""))
        return (
            instance is not None
            and bool(instance["inbox"])
            and instance["holder"] == body.get("token")
            and body.get("turn") == instance["turn"] + 1
        )

    async def handle(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        body: Any = json.loads(request.content) if request.content else None
        if path == "/v1/workflows/instances":
            self.create(body["id"], body["workflow_type"], body.get("state"), body["version"])
            return httpx.Response(200, json={})
        if path == "/v1/workflows/instances/message":
            return httpx.Response(200, json=self.send(body["workflow_id"], body["message"]))
        if path == "/v1/workflows/turns/signal":
            self.signals.setdefault(body["workflow_id"], []).append(body)
            return httpx.Response(200, json=_turn(body["workflow_id"], body["turn"], None))
        if path == "/v1/workflows/triggered":
            name = body["name"]
            self.create(f"starter.{name}", body["workflow"]["workflow_type"], {})
            self.send(
                f"starter.{name}",
                {
                    "type": "instances",
                    "workflow": name,
                    "graph": "main",
                    "as_of_commit_seq": 1,
                    "reason": "backfill",
                    "instances": [NOTE],
                    "removed": [],
                },
            )
            return httpx.Response(200, json={"name": name, "status": {"state": "running"}})
        if path == "/v1/workflows/turns/claim":
            self.claims += 1
            types = {(w["workflow_type"], w["version"]) for w in body["workflows"]}
            for id, instance in self.instances.items():
                held = instance["holder"] is not None
                if instance["inbox"] and not held and (instance["type"], instance["version"]) in types:
                    self.signalled = False
                    return httpx.Response(200, json={"task": self.task(id)})
            if any(instance["holder"] is not None for instance in self.instances.values()):
                # A turn still runs: an empty claim answers after a while, as
                # a long poll does.
                await asyncio.sleep(min(0.02, body.get("wait_ms", 0) / 1000))
                return httpx.Response(200, json={"task": None})
            # Nothing runs and nothing waits: a worker that serves until
            # SIGTERM gets one, once.
            serving = signal.getsignal(signal.SIGTERM) not in (signal.SIG_DFL, signal.SIG_IGN, None)
            if serving and not self.signalled:
                self.signalled = True
                signal.raise_signal(signal.SIGTERM)
            return httpx.Response(200, json={"task": None})
        if path == "/v1/workflows/turns/complete":
            instance = self.instances[body["workflow_id"]]
            fingerprint = json.dumps(body, sort_keys=True)
            if instance["completions"].get(body["turn"]) == fingerprint:
                return httpx.Response(200, json={})  # An exact repeat.
            if not self.holds(body):
                return httpx.Response(409, json=CONFLICT)
            instance["inbox"].pop(0)
            instance["turn"] += 1
            instance["holder"] = None
            instance["state"] = body["state"]
            instance["completions"][body["turn"]] = fingerprint
            self.completed.append((body["workflow_id"], body))
            if body.get("continuation"):
                self.send(body["workflow_id"], body["continuation"]["message"])
            return httpx.Response(200, json={})
        if path.startswith("/v1/workflows/turns/") and not self.holds(body):
            return httpx.Response(409, json=CONFLICT)
        if path == "/v1/workflows/turns/checkpoint":
            instance = self.instances[body["workflow_id"]]
            if body["kind"] == "sleep":
                instance["holder"] = None  # The turn waits; here it is due at once.
            steps = instance["inbox"][0]["steps"]
            steps.append(
                {
                    "key": body["key"],
                    "kind": body["kind"],
                    "output": body.get("output"),
                    "delay_ms": body.get("delay_ms"),
                    "at_ms": 1,
                }
            )
            return httpx.Response(200, json={})
        if path == "/v1/workflows/turns/wait-signal":
            sent = self.signals.get(body["workflow_id"], [])
            match = next((s for s in sent if s["name"] == body["name"]), None)
            output = (
                {"received": True, "value": match["value"]}
                if match
                else {"received": False, "value": None}
            )
            step = {
                "key": body["key"],
                "kind": "signal",
                "output": output,
                "delay_ms": body["timeout_ms"],
                "signal_name": body["name"],
                "at_ms": 1,
            }
            head = self.instances[body["workflow_id"]]["inbox"][0]
            head["steps"].append(step)
            turn = _turn(body["workflow_id"], body["turn"], None, status="running")
            turn["steps"] = head["steps"]
            return httpx.Response(200, json=turn)
        if path == "/v1/workflows/turns/fail":
            self.failed.append(body)
            instance = self.instances[body["workflow_id"]]
            instance["holder"] = None
            # A failed message leaves the inbox here (the guide's examples
            # never fail); a wait keeps it for the next claim.
            if body.get("retry_at_ms") is None:
                instance["inbox"].pop(0)
            return httpx.Response(200, json={})
        if path.startswith("/v1/workflows/turns/"):
            return httpx.Response(200, json={})
        if path == "/v1/workflows/activities/reason":
            return httpx.Response(
                200,
                json={
                    "value": {"code": "P3"},
                    "model": "mock",
                    "tokens_in": 9,
                    "tokens_out": 3,
                    "cost_micro_usd": 1,
                    "cached": False,
                },
            )
        if path == "/v1/query/sparql-text":
            doc = {
                "head": {"vars": ["s", "text"]},
                "results": {
                    "bindings": [
                        {
                            "s": {"type": "uri", "value": NOTE},
                            "text": {"type": "literal", "value": "one two three"},
                        }
                    ]
                },
            }
            return httpx.Response(200, json={"results": json.dumps(doc)})
        if path == "/v1/reviews":
            self.reviews.append(body)
            id = f"rev_{len(self.reviews)}"
            item = {
                **body,
                "id": id,
                "object": "review_item",
                "scope": "live",
                "status": "open",
                "revision": 1,
                "wait": {**body["wait"], "signal": f"review.{id}"},
                "created": "2026-10-21T10:00:00Z",
                "updated": "2026-10-21T10:00:00Z",
            }
            return httpx.Response(200, json=item)
        if path == "/v1/graph/commit" and request.url.params.get("dry_run") == "true":
            self.dry_runs.append(body)
            return httpx.Response(
                200, json={"dry_run": True, "op_count": 1, "triplets": 0, "entity_properties": 1}
            )
        if path == "/v1/graph/commit":
            self.commits.append(body)
            return httpx.Response(
                200, json={"commit_seq": len(self.commits), "idempotency_key": "k", "replayed": False}
            )
        raise AssertionError(f"the guide calls a route the fake does not have: {path}")


@pytest.fixture
def fake(monkeypatch: pytest.MonkeyPatch) -> FakeEngine:
    engine = FakeEngine()

    class Client(lbb.AsyncLbbClient):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            kwargs.setdefault("transport", httpx.MockTransport(engine.handle))
            kwargs.setdefault("retry_delay", 0)
            super().__init__(*args, **kwargs)

    monkeypatch.setattr(lbb, "AsyncLbbClient", Client)
    return engine


def test_every_example_of_the_guide_runs(fake: FakeEngine) -> None:
    page = _page()
    if page is None:
        pytest.skip("the docs site is not in this tree")
    blocks = re.findall(r"```python\n(.*?)```", page.read_text(), re.S)
    assert len(blocks) >= 10
    namespace: dict[str, Any] = {"__name__": "python_worker_guide"}
    for number, block in enumerate(blocks):
        exec(compile(block, f"{page.name} block {number}", "exec"), namespace)
    # The quick start's message ran.
    assert ("counter-1", {"count": 2}) in [(id, body["state"]) for id, body in fake.completed]
    # The captured word count ran on its own instance and version: one dry
    # run, no commit, and the watch's own turn is still there.
    assert [id for id, _ in fake.completed].count("word-count-test-1") == 1
    assert fake.dry_runs[0]["entity_properties"][0]["properties"] == {"word_count": 3}
    assert "workflow_fence" not in fake.dry_runs[0]
    assert fake.commits == []

    # One message for each workflow the guide defines, with the real worker.
    definitions = [v for v in namespace.values() if isinstance(v, WorkflowDefinition)]
    initial = {d.name: d.initial_state for d in definitions}
    for id, workflow_type, message in [
        ("codes-1", "define-codes", {"prompt": "Define the code of a washbasin"}),
        ("review-1", "review-draft", {"task": "a reply to a customer"}),
        ("pages-1", "pages", {"pages": list(range(45))}),
        ("crawler-1", "crawler", {}),
        ("invoice-1", "invoice", {"order": "o-1", "paid": True}),
        ("rooms-1", "rooms", {"room": "R 1.04"}),
    ]:
        fake.create(id, workflow_type, initial[workflow_type])
        fake.send(id, message)
    # A person answered the room's question.
    answer = {"decision": "choose", "option": "tiles"}
    fake.signals["rooms-1"] = [{"name": "review.rev_1", "value": answer}]
    assert {d.name for d in definitions} >= {
        "counter",
        "define-codes",
        "review-draft",
        "pages",
        "crawler",
        "word-count",
        "invoice",
        "rooms",
    }
    worker = lbb.workflows.WorkflowWorker(namespace["client"], definitions, worker="docs", claim_wait_ms=0)
    worker.run()

    assert fake.failed == []
    states = {id: body["state"] for id, body in fake.completed}
    results = {id: body.get("result") for id, body in fake.completed}
    assert states["codes-1"] == {"code": "P3"}
    assert states["review-1"] == {"sent": 1}
    assert results["review-1"] == "Draft for a reply to a customer"
    # 45 pages in turns of 20: two continuations.
    assert states["pages-1"] == {"read": 45}
    assert [id for id, _ in fake.completed].count("pages-1") == 3
    assert states["crawler-1"] == {"pages": 3}
    assert states["invoice-1"] == {"last": "o-1"}
    assert states["rooms-1"] == {"floor": "tiles"}
    (asked,) = fake.reviews
    assert asked["wait"] == {"identity": "rooms-1", "turn": 1, "signal": ""}
    # The triggered workflow wrote the word count of its instance, fenced.
    (commit,) = fake.commits
    assert commit["entity_properties"][0]["properties"] == {"word_count": 3}
    assert commit["workflow_fence"]["workflow_id"] == "starter.notes"


def _readme() -> Path | None:
    for parent in Path(__file__).resolve().parents:
        readme = parent / "README.md"
        if readme.is_file() and "## Run a workflow worker" in readme.read_text():
            return readme
    return None


def test_the_readme_example_runs(fake: FakeEngine) -> None:
    readme = _readme()
    if readme is None:
        pytest.skip("the Python SDK README is not in this tree")
    section = readme.read_text().split("## Run a workflow worker", 1)[1].split("\n## ", 1)[0]
    (block,) = re.findall(r"```python\n(.*?)```", section, re.S)
    fake.create("counter-1", "counter", {"count": 0})
    fake.send("counter-1", {"add": 3})
    exec(compile(block, f"{readme.name} worker example", "exec"), {"__name__": "readme"})
    assert fake.failed == []
    assert [(id, body["state"]) for id, body in fake.completed] == [("counter-1", {"count": 3})]
