"""``lbb.workflows``: what the conformance suite does not pin, because it is
Python's own: concurrency, the stop and its release of held turns, SIGTERM,
the JSON rules of step outputs, models of steps, and the helpers."""

from __future__ import annotations

import asyncio
import inspect
import json
import os
import signal
import threading
import time
from collections.abc import Callable
from typing import Any

import httpx
import pytest

from lbb import AsyncLbbClient, LbbError, models
from lbb.workflows import OwnedWrite as Write
from lbb.workflows import (
    WorkflowContext,
    WorkflowError,
    WorkflowResult,
    WorkflowStepContext,
    WorkflowWorker,
    capture_activities,
    entity_ref,
    fan_in_identity,
    is_activity_refused,
    is_fan_in_identity,
    lbb_activities,
    owned_commit_body,
    parse_trigger_message,
    split_in_halves,
    triggered_workflow_state,
    workflow,
    workflow_of_identity,
    write_owned,
)

BASE = "/v1/workflows/turns"


def task(number: int = 1, *, steps: list[Any] | None = None, lease_ms: int = 60_000) -> dict[str, Any]:
    return {
        "workflow_type": "counter",
        "state": {"count": 0},
        "token": f"token-{number}",
        "lease_ms": lease_ms,
        "effect_prefix": f"prefix-{number}",
        "turn": {
            "workflow_id": f"agent-{number}",
            "number": 1,
            "message_id": f"m{number}",
            "message": {"add": number},
            "version": "v1",
            "status": "running",
            "attempt": 1,
            "created_at_ms": 1,
            "updated_at_ms": 1,
            "ready_at_ms": 1,
            "steps": steps or [],
            "result": None,
            "state_after": None,
            "sequence": 1,
            "run": {"id": f"run-{number}", "started_at_ms": 1, "queue_before": 1},
        },
    }


class Fake:
    """A server that answers by route: claims hand out the queued tasks, and
    every other route answers ``{}`` unless a handler is set."""

    def __init__(self, tasks: list[dict[str, Any]]) -> None:
        self.tasks = list(tasks)
        self.calls: list[tuple[str, Any]] = []
        self.routes: dict[str, Callable[[Any], Any]] = {}

    async def handle(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        body = json.loads(request.content) if request.content else None
        self.calls.append((path, body))
        if path == f"{BASE}/claim":
            if self.tasks:
                return httpx.Response(200, json={"task": self.tasks.pop(0)})
            # An empty queue: wait as a long poll does, a little.
            await asyncio.sleep(min(0.05, (body or {}).get("wait_ms", 0) / 1000))
            return httpx.Response(200, json={"task": None})
        if path in self.routes:
            answer = self.routes[path](body)
            return await answer if inspect.isawaitable(answer) else answer
        return httpx.Response(200, json={})

    def bodies(self, route: str) -> list[Any]:
        return [body for path, body in self.calls if path == f"{BASE}/{route}"]

    def client(self) -> AsyncLbbClient:
        return AsyncLbbClient(
            "http://localhost:7400",
            graph="main",
            retry_delay=0,
            transport=httpx.MockTransport(self.handle),
        )


def test_the_worker_runs_at_most_concurrency_turns_at_once() -> None:
    fake = Fake([task(1), task(2), task(3)])
    running = 0
    most = 0
    release = asyncio.Event()

    @workflow(name="counter", version="v1", initial_state={"count": 0})
    async def counter(ctx: WorkflowContext, state: Any, message: Any) -> WorkflowResult[Any]:
        nonlocal running, most

        async def work(_: WorkflowStepContext) -> int:
            nonlocal running, most
            running += 1
            most = max(most, running)
            await release.wait()
            running -= 1
            return int(message["add"])

        return WorkflowResult(state={"count": await ctx.step("add", work)})

    async def main() -> None:
        async with fake.client() as client:
            worker = WorkflowWorker(client, [counter], worker="w", concurrency=2, claim_wait_ms=10)
            served = asyncio.ensure_future(worker.run_async())
            for _ in range(100):
                await asyncio.sleep(0.01)
                if running == 2:
                    break
            # Two turns hold both slots: the third waits for a slot.
            assert len(fake.tasks) == 1
            release.set()
            for _ in range(200):
                await asyncio.sleep(0.01)
                if len(fake.bodies("complete")) == 3:
                    break
            worker.stop()
            await served

    asyncio.run(main())
    assert most == 2
    assert sorted(body["state"]["count"] for body in fake.bodies("complete")) == [1, 2, 3]


def test_stop_drains_a_finishing_turn_and_releases_a_held_one() -> None:
    fake = Fake([task(1), task(2)])
    started = asyncio.Event()

    @workflow(name="counter", version="v1", initial_state={"count": 0})
    async def counter(ctx: WorkflowContext, state: Any, message: Any) -> WorkflowResult[Any]:
        async def work(_: WorkflowStepContext) -> int:
            if message["add"] == 2:
                started.set()
                await asyncio.Event().wait()  # Never finishes by itself.
            await asyncio.sleep(0.05)
            return 1

        await ctx.step("first", lambda s: "saved")
        return WorkflowResult(state={"count": await ctx.step("work", work)})

    async def main() -> None:
        async with fake.client() as client:
            worker = WorkflowWorker(
                client, [counter], worker="w", concurrency=2, claim_wait_ms=10, drain_seconds=0.3
            )
            served = asyncio.ensure_future(worker.run_async())
            await asyncio.wait_for(started.wait(), 2)
            began = time.monotonic()
            worker.stop()
            await served
            # The drain waited for the held turn, then released it.
            assert time.monotonic() - began >= 0.25

    asyncio.run(main())
    # Turn 1 finished within the drain; turn 2 was released: it fails with
    # `retry_at_ms` (no attempt), and its saved step stays.
    assert [body["token"] for body in fake.bodies("complete")] == ["token-1"]
    (released,) = fake.bodies("fail")
    assert released["token"] == "token-2"
    assert released["non_retryable"] is False
    assert released["retry_at_ms"] >= time.time() * 1000 - 5_000
    assert "released" in released["error"]
    assert [body["key"] for body in fake.bodies("checkpoint") if body["token"] == "token-2"] == [
        "first"
    ]


def test_sigterm_stops_the_worker_and_sigint_releases_at_once() -> None:
    if not hasattr(signal, "SIGTERM") or os.name == "nt":
        pytest.skip("POSIX signals")
    fake = Fake([task(1)])

    @workflow(name="counter", version="v1", initial_state={"count": 0})
    async def counter(ctx: WorkflowContext, state: Any, message: Any) -> WorkflowResult[Any]:
        async def work(_: WorkflowStepContext) -> int:
            # The turn is held: SIGINT releases it at once, without the drain.
            os.kill(os.getpid(), signal.SIGINT)
            await asyncio.Event().wait()
            return 1

        return WorkflowResult(state={"count": await ctx.step("work", work)})

    client = fake.client()
    worker = WorkflowWorker(client, [counter], worker="w", claim_wait_ms=10, drain_seconds=30)
    began = time.monotonic()
    worker.run()
    assert time.monotonic() - began < 10
    assert len(fake.bodies("fail")) == 1
    assert fake.bodies("complete") == []

    # SIGTERM with nothing held: the worker stops claiming and returns.
    fake = Fake([])
    client = fake.client()
    worker = WorkflowWorker(client, [counter], worker="w", claim_wait_ms=10)
    timer = threading.Timer(0.2, lambda: os.kill(os.getpid(), signal.SIGTERM))
    timer.start()
    worker.run()
    timer.join()
    assert fake.bodies("claim")


def test_a_transient_claim_failure_waits_and_claims_again() -> None:
    fake = Fake([])
    failures = [503]

    async def main() -> None:
        original = fake.handle

        async def handle(request: httpx.Request) -> httpx.Response:
            if request.url.path == f"{BASE}/claim" and failures:
                status = failures.pop()
                fake.calls.append((request.url.path, None))
                return httpx.Response(status, json={"error": {"code": "storage_degraded"}})
            return await original(request)

        client = AsyncLbbClient(
            "http://localhost:7400", retry_delay=0, transport=httpx.MockTransport(handle)
        )

        @workflow(name="counter", version="v1", initial_state={})
        async def counter(ctx: WorkflowContext, state: Any, message: Any) -> WorkflowResult[Any]:
            return WorkflowResult(state=state)

        worker = WorkflowWorker(client, [counter], worker="w", claim_wait_ms=10)
        served = asyncio.ensure_future(worker.run_async())
        for _ in range(300):
            await asyncio.sleep(0.01)
            if len(fake.bodies("claim")) >= 2:
                break
        worker.stop()
        await served
        await client.aclose()

    asyncio.run(main())
    # One request per claim: the failed claim was not retried as a request,
    # and the loop claimed again after its wait.
    assert len(fake.bodies("claim")) >= 2


def test_a_refused_claim_ends_the_loop() -> None:
    def handle(request: httpx.Request) -> httpx.Response:
        return httpx.Response(401, json={"error": {"code": "unauthorized", "message": "no key"}})

    @workflow(name="counter", version="v1", initial_state={})
    async def counter(ctx: WorkflowContext, state: Any, message: Any) -> WorkflowResult[Any]:
        return WorkflowResult(state=state)

    async def main() -> None:
        async with AsyncLbbClient(
            "http://localhost:7400", transport=httpx.MockTransport(handle)
        ) as client:
            with pytest.raises(LbbError):
                await WorkflowWorker(client, [counter], worker="w").run_async()

    asyncio.run(main())


def run_one(definition: Any, *tasks: dict[str, Any], **worker: Any) -> Fake:
    fake = Fake(list(tasks))

    async def main() -> None:
        async with fake.client() as client:
            for _ in tasks:
                await WorkflowWorker(client, [definition], worker="w", **worker).run_once()

    asyncio.run(main())
    return fake


def test_a_step_that_returns_a_model_needs_model_to_get_it_back() -> None:
    answer = models.WorkflowRunUsage(calls=2)

    @workflow(name="counter", version="v1", initial_state={})
    async def loose(ctx: WorkflowContext, state: Any, message: Any) -> WorkflowResult[Any]:
        await ctx.step("usage", lambda s: answer)
        return WorkflowResult(state=state)

    fake = run_one(loose, task())
    (failed,) = fake.bodies("fail")
    assert failed["non_retryable"] is True
    assert "model=WorkflowRunUsage" in failed["error"]

    seen: list[Any] = []

    @workflow(name="counter", version="v1", initial_state={})
    async def typed(ctx: WorkflowContext, state: Any, message: Any) -> WorkflowResult[Any]:
        usage = await ctx.step("usage", lambda s: answer, model=models.WorkflowRunUsage)
        seen.append(usage)
        return WorkflowResult(state={"calls": usage.calls})

    saved = [{"key": "usage", "kind": "step", "output": {"calls": 2}, "at_ms": 1}]
    fake = run_one(typed, task(1), task(2, steps=saved))
    # The same model on the first run and on the replay.
    assert seen == [answer, answer]
    assert [body["state"] for body in fake.bodies("complete")] == [{"calls": 2}, {"calls": 2}]
    assert fake.bodies("checkpoint")[0]["output"] == {"calls": 2}


@pytest.mark.parametrize(
    ("value", "where"),
    [
        ({"rows": [1, float("nan")]}, 'step "page.0".rows[1]'),
        ({"rows": {1: "a"}}, 'step "page.0".rows has a key of type int'),
        ({"seen": {"a"}}, 'step "page.0".seen is set'),
    ],
)
def test_a_value_json_cannot_hold_fails_the_turn_and_names_where(value: Any, where: str) -> None:
    @workflow(name="counter", version="v1", initial_state={})
    async def counter(ctx: WorkflowContext, state: Any, message: Any) -> WorkflowResult[Any]:
        await ctx.step("page.0", lambda s: value)
        return WorkflowResult(state=state)

    fake = run_one(counter, task())
    (failed,) = fake.bodies("fail")
    assert failed["non_retryable"] is True
    assert where in failed["error"]
    assert fake.bodies("checkpoint") == []


def test_a_step_function_may_take_no_argument_and_be_sync() -> None:
    def plain() -> int:
        return 3

    @workflow(name="counter", version="v1", initial_state={})
    async def counter(ctx: WorkflowContext, state: Any, message: Any) -> WorkflowResult[Any]:
        a = await ctx.step("a", plain)
        b = await ctx.step("b", lambda s: s.effect_id)
        return WorkflowResult(state={"a": a, "b": b}, result=a)

    fake = run_one(counter, task())
    assert fake.bodies("complete")[0]["state"] == {"a": 3, "b": "prefix-1/b"}


def test_activity_calls_of_concurrent_turns_name_their_own_run() -> None:
    fake = Fake([task(1), task(2)])
    reasoned = {
        "value": {},
        "model": "m",
        "tokens_in": 1,
        "tokens_out": 1,
        "cost_micro_usd": 1,
        "cached": False,
    }
    fake.routes["/v1/workflows/activities/reason"] = lambda body: httpx.Response(200, json=reasoned)
    both = asyncio.Event()
    waiting = 0

    async def main() -> None:
        async with fake.client() as client:
            lbb = lbb_activities(client)

            @workflow(name="counter", version="v1", initial_state={})
            async def counter(ctx: WorkflowContext, state: Any, message: Any) -> WorkflowResult[Any]:
                async def ask(_: WorkflowStepContext) -> None:
                    nonlocal waiting
                    waiting += 1
                    if waiting == 2:
                        both.set()
                    await both.wait()
                    # No effect id: the turn comes from the running task.
                    await lbb.reason("p", {"type": "object"})

                await ctx.step("ask", ask)
                return WorkflowResult(state=state)

            worker = WorkflowWorker(client, [counter], worker="w", concurrency=2, claim_wait_ms=10)
            served = asyncio.ensure_future(worker.run_async())
            for _ in range(300):
                await asyncio.sleep(0.01)
                if len(fake.bodies("complete")) == 2:
                    break
            worker.stop()
            await served

    asyncio.run(main())
    asked = [body for path, body in fake.calls if path.endswith("/activities/reason")]
    assert sorted(body["workflow"]["run_id"] for body in asked) == ["run-1", "run-2"]
    for done in fake.bodies("complete"):
        assert done["usage"]["calls"] == 1


def answer_activities(fake: Fake) -> None:
    used = {"model": "m", "tokens_in": 1, "tokens_out": 1, "cost_micro_usd": 1, "cached": False}
    answers = {
        "reason": {"value": {}, **used},
        "decide": {"answers": {}, **used},
        "embed": {"vectors": [[0.1]], "dim": 1, **used},
    }
    for name, answer in answers.items():
        fake.routes[f"/v1/workflows/activities/{name}"] = (
            lambda body, answer=answer: httpx.Response(200, json=answer)
        )


def activity_calls(fake: Fake) -> list[tuple[Any, Any, Any]]:
    return [
        (body.get("effect_id"), body.get("workflow"), body.get("test"))
        for path, body in fake.calls
        if "/activities/" in path
    ]


RUN = {"identity": "agent-1", "turn": 1, "run_id": "run-1"}
OBJECT = {"type": "object"}
QUESTIONS = {"kind": {"instructions": "?", "criteria": {"a": "a", "b": "b"}}}


def test_a_call_in_a_step_names_the_steps_key_also_with_an_id_made_from_it() -> None:
    fake = Fake([task()])
    answer_activities(fake)

    async def main() -> None:
        async with fake.client() as client:
            lbb = lbb_activities(client)

            @workflow(name="counter", version="v1", initial_state={})
            async def counter(ctx: WorkflowContext, state: Any, message: Any) -> WorkflowResult[Any]:
                async def define(s: WorkflowStepContext) -> None:
                    await lbb.reason("p", OBJECT, effect_id=s.effect_id)
                    # More calls of one step, with ids made from the step's.
                    await lbb.decide({}, QUESTIONS, effect_id=f"{s.effect_id}.2")
                    await lbb.embed(["t"], effect_id=f"{s.effect_id}:texts")
                    # An id that only starts with the step's text names no step.
                    await lbb.reason("p", OBJECT, effect_id=f"{s.effect_id}x")

                async def page(s: WorkflowStepContext) -> None:
                    await lbb.reason("p", OBJECT, effect_id=f"{s.effect_id}.1")

                await ctx.step("define", define)
                # A step key with a dot: the longest step id that starts the call's wins.
                await ctx.step("page", lambda s: None)
                await ctx.step("page.2", page)
                # A call outside a step names the turn and no step.
                await lbb.reason("p", OBJECT)
                return WorkflowResult(state=state)

            assert await WorkflowWorker(client, [counter], worker="w").run_once()
            # The steps go with the turn: outside a turn a call names no run and no step.
            await lbb.reason("p", OBJECT, effect_id="prefix-1/define")

    asyncio.run(main())
    assert [(effect_id, workflow) for effect_id, workflow, _ in activity_calls(fake)] == [
        ("prefix-1/define", {**RUN, "step": "define"}),
        ("prefix-1/define.2", {**RUN, "step": "define"}),
        ("prefix-1/define:texts", {**RUN, "step": "define"}),
        ("prefix-1/definex", RUN),
        ("prefix-1/page.2.1", {**RUN, "step": "page.2"}),
        (None, RUN),
        ("prefix-1/define", None),
    ]


def test_a_captures_calls_in_a_step_name_the_step_as_the_live_calls_do() -> None:
    fake = Fake([task()])
    answer_activities(fake)

    async def main() -> None:
        async with fake.client() as client:
            capture = capture_activities(lbb_activities(client), workflow="sinks")

            @workflow(name="counter", version="v1", initial_state={})
            async def counter(ctx: WorkflowContext, state: Any, message: Any) -> WorkflowResult[Any]:
                async def define(s: WorkflowStepContext) -> None:
                    await capture.activities.reason("p", OBJECT, effect_id=s.effect_id)
                    await capture.activities.decide({}, QUESTIONS, effect_id=f"{s.effect_id}.2")

                await ctx.step("define", define)
                return WorkflowResult(state=state)

            assert await WorkflowWorker(client, [counter], worker="w").run_once()

    asyncio.run(main())
    assert activity_calls(fake) == [
        ("prefix-1/define", {**RUN, "step": "define"}, {"workflow": "sinks"}),
        ("prefix-1/define.2", {**RUN, "step": "define"}, {"workflow": "sinks"}),
    ]


def test_write_owned_to_another_graph_carries_no_fence() -> None:
    commits: list[tuple[str, Any]] = []
    fake = Fake([task()])

    def commit(body: Any) -> httpx.Response:
        commits.append(("", body))
        return httpx.Response(
            200, json={"commit_seq": 4, "idempotency_key": "k", "replayed": False}
        )

    fake.routes["/v1/graph/commit"] = commit

    async def main() -> None:
        async with fake.client() as client:

            @workflow(name="counter", version="v1", initial_state={})
            async def counter(
                ctx: WorkflowContext, state: Any, message: Any
            ) -> WorkflowResult[Any]:
                write = Write(subjects=[{"type": "Note", "name": "n1"}], properties=["words"])
                own = await write_owned(client, ctx, "own", write)
                other = await write_owned(client, ctx, "other", write, graph="archive")
                return WorkflowResult(state={"own": own.commit_seq, "other": other.commit_seq})

            await WorkflowWorker(client, [counter], worker="w").run_once()

    asyncio.run(main())
    assert commits[0][1]["workflow_fence"] == {
        "workflow_id": "agent-1",
        "turn": 1,
        "token": "token-1",
    }
    assert "workflow_fence" not in commits[1][1]



def test_start_creates_the_instance_with_on_exhausted() -> None:
    seen: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={})

    @workflow(name="counter", version="v1", initial_state={"count": 0})
    async def counter(ctx: WorkflowContext, state: Any, message: Any) -> WorkflowResult[Any]:
        return WorkflowResult(state=state)

    async def main() -> None:
        async with AsyncLbbClient(
            "http://localhost:7400", graph="crm", transport=httpx.MockTransport(handle)
        ) as client:
            handle_ = await counter.start(client, "agent", on_exhausted="skip")
            assert handle_.id == "agent"
            other = await client.workflows.start(counter, "plain")
            assert other.id == "plain"

    asyncio.run(main())
    assert json.loads(seen[0].content) == {
        "id": "agent",
        "workflow_type": "counter",
        "version": "v1",
        "state": {"count": 0},
        "lease_ms": 60000,
        "max_attempts": 3,
        "on_exhausted": "skip",
    }
    assert "on_exhausted" not in json.loads(seen[1].content)
    assert seen[0].url.params["graph"] == "crm"


def test_blobs_put_the_raw_bytes_and_read_them_back() -> None:
    seen: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if request.method == "POST":
            return httpx.Response(
                200, json={"key": "rooms/ab", "sha256": "ab", "bytes": 9, "created": True}
            )
        return httpx.Response(200, content=b'{"rows": 2}')

    async def main() -> None:
        async with AsyncLbbClient(
            "http://h", graph="main", transport=httpx.MockTransport(handle)
        ) as client:
            blobs = client.workflows.blobs
            blob = await blobs.put("catalogue", workflow_id="starter.rooms")
            assert blob.key == "rooms/ab"
            assert await blobs.get_json("rooms/ab") == {"rows": 2}
            with pytest.raises(ValueError):
                await blobs.put(b"x" * (8 * 1024 * 1024 + 1), workflow_id="w")

    asyncio.run(main())
    assert seen[0].content == b"catalogue"
    assert seen[0].url.params["workflow_id"] == "starter.rooms"
    assert seen[0].headers["content-type"] == "application/octet-stream"
    assert seen[1].url.params["key"] == "rooms/ab"


def test_the_identities_of_a_named_workflow() -> None:
    assert workflow_of_identity("starter.rooms") == "rooms"
    assert workflow_of_identity("starter.rooms.2") == "rooms"
    assert workflow_of_identity("starter.rooms.merge") == "rooms"
    assert workflow_of_identity("agent") is None
    assert fan_in_identity("rooms", "merge") == "starter.rooms.merge"
    assert is_fan_in_identity("starter.rooms.merge")
    assert not is_fan_in_identity("starter.rooms.2")
    with pytest.raises(ValueError):
        fan_in_identity("rooms", "Merge")


def test_a_trigger_message_is_checked_and_reads_older_shapes() -> None:
    message = parse_trigger_message(
        {
            "type": "instances",
            "starter": "notes",
            "graph": "crm",
            "as_of_commit_seq": 4,
            "reason": "a_new_reason",
            "instances": ["https://littlebigbrain.com/e/0123456789abcdef0123456789abcdef"],
        }
    )
    assert message.workflow == "notes"
    assert message.removed == []
    # A reason of a newer server passes through as its text.
    assert message.reason == "a_new_reason"
    for bad in ({"type": "other"}, {"type": "instances", "workflow": "n"}, None):
        with pytest.raises(WorkflowError):
            parse_trigger_message(bad)


def test_triggered_state_merges_the_initial_state_under_the_saved_one() -> None:
    assert triggered_workflow_state({"runs": 0}, {}) == {"runs": 0}
    assert triggered_workflow_state({"runs": 0}, None) == {"runs": 0}
    assert triggered_workflow_state({"runs": 0, "x": 1}, {"runs": 4}) == {"runs": 4, "x": 1}
    assert triggered_workflow_state({"runs": 0}, [1]) == [1]


def test_an_owned_write_builds_one_commit_with_replace_owned_for_subjects() -> None:
    iri = "https://littlebigbrain.com/e/0123456789abcdef0123456789abcdef"
    body = owned_commit_body(
        Write(
            subjects=[entity_ref(iri, "Note")],
            properties=["word_count"],
            entity_properties=[{**entity_ref(iri, "Note", "n1"), "properties": {"word_count": 3}}],
        )
    )
    assert body is not None
    dumped = body.model_dump(mode="json", exclude_none=True)
    assert dumped["edge_idempotency"] == "skip_unchanged"
    assert dumped["replace_owned"]["properties"] == ["word_count"]
    assert dumped["replace_owned"]["subjects"][0]["key"].endswith("0123456789abcdef0123456789abcdef")
    assert dumped["entity_properties"][0]["properties"] == {"word_count": 3}
    assert owned_commit_body(Write(subjects=[])) is None


def test_activity_helpers() -> None:
    refused = LbbError(422, "{}", {"code": "activity_request_refused"})
    assert is_activity_refused(refused)
    assert not is_activity_refused(LbbError(429, "{}", {"code": "workflow_budget_exceeded"}))
    assert split_in_halves([1, 2, 3]) == ([1], [2, 3])
    assert split_in_halves([1]) is None


def test_review_files_an_item_with_the_workflow_as_its_origin() -> None:
    seen: list[Any] = []

    def handle(request: httpx.Request) -> httpx.Response:
        seen.append(json.loads(request.content))
        return httpx.Response(
            200,
            json={
                "suggestion_id": "sg_1",
                "graph": {"tenant_id": "t", "graph_id": "crm", "branch_id": "main"},
                "key": "k",
                "status": "open",
                "title": "t",
                "anchor": {"kind": "ontology"},
                "origin": {"kind": "agent", "id": "starter:notes"},
                "change": [],
                "revision": 1,
                "created_at": "2026-10-21T10:00:00Z",
                "updated_at": "2026-10-21T10:00:00Z",
            },
        )

    async def main() -> None:
        async with AsyncLbbClient(
            "http://h", graph="crm", transport=httpx.MockTransport(handle)
        ) as client:
            item = await lbb_activities(client).review(
                {"key": "k", "title": "t", "change": []}, workflow="notes"
            )
            assert item.suggestion_id == "sg_1"

    asyncio.run(main())
    assert seen[0]["origin"] == {"kind": "agent", "id": "starter:notes"}


def test_a_blocking_sync_step_holds_no_other_turn() -> None:
    # A sync step function runs in a thread: while it blocks, the event loop
    # sends the heartbeats of both held turns.
    fake = Fake([task(1, lease_ms=150), task(2, lease_ms=150)])
    reported: list[str] = []

    @workflow(name="counter", version="v1", initial_state={"count": 0})
    async def counter(ctx: WorkflowContext, state: Any, message: Any) -> WorkflowResult[Any]:
        def blocking(s: WorkflowStepContext) -> int:
            # The `requests` or sync-client call of a real step.
            time.sleep(0.6)
            # Progress from the step's thread goes through the loop.
            s.progress("reading pages", done=1, total=2, unit="pages")
            reported.append(threading.current_thread().name)
            return 1

        async def waiting(_: WorkflowStepContext) -> int:
            await asyncio.sleep(0.6)
            return 2

        value = await ctx.step("work", blocking if message["add"] == 1 else waiting)
        return WorkflowResult(state={"count": value})

    async def main() -> None:
        async with fake.client() as client:
            worker = WorkflowWorker(client, [counter], worker="w", concurrency=2, claim_wait_ms=10)
            served = asyncio.ensure_future(worker.run_async())
            for _ in range(300):
                await asyncio.sleep(0.01)
                if len(fake.bodies("complete")) == 2:
                    break
            worker.stop()
            await served

    asyncio.run(main())
    beats = [body["token"] for body in fake.bodies("heartbeat")]
    assert beats.count("token-1") >= 4 and beats.count("token-2") >= 4, beats
    assert reported and reported[0] != threading.main_thread().name
    (progress,) = [b for b in fake.bodies("progress") if b["token"] == "token-1"]
    assert progress["details"] == {"phase": "reading pages", "done": 1, "total": 2, "unit": "pages"}
    assert sorted(body["state"]["count"] for body in fake.bodies("complete")) == [1, 2]


def test_a_release_after_the_handler_s_last_await_still_gives_the_turn_back() -> None:
    fake = Fake([task(1)])
    holder: list[WorkflowWorker] = []

    @workflow(name="counter", version="v1", initial_state={"count": 0})
    async def counter(ctx: WorkflowContext, state: Any, message: Any) -> WorkflowResult[Any]:
        await ctx.step("first", lambda s: 1)
        # The release lands after the last await, as the handler returns.
        for turn in list(holder[0]._turns):
            turn.release()
        return WorkflowResult(state={"count": 1})

    async def main() -> None:
        async with fake.client() as client:
            worker = WorkflowWorker(client, [counter], worker="w")
            holder.append(worker)
            await worker.run_once()

    asyncio.run(main())
    assert fake.bodies("complete") == []
    (released,) = fake.bodies("fail")
    assert released["retry_at_ms"] > 0 and released["non_retryable"] is False
    assert "released" in released["error"]


def test_a_stop_lets_a_waiting_claim_answer_and_releases_its_turn() -> None:
    fake = Fake([])
    answered: list[str] = []
    asked = asyncio.Event()

    async def claim(_: Any) -> httpx.Response:
        asked.set()
        try:
            await asyncio.sleep(0.3)  # The claim waits on the server.
        except asyncio.CancelledError:
            answered.append("cancelled")
            raise
        answered.append("answered")
        return httpx.Response(200, json={"task": task(1)})

    @workflow(name="counter", version="v1", initial_state={})
    async def counter(ctx: WorkflowContext, state: Any, message: Any) -> WorkflowResult[Any]:
        raise AssertionError("a turn claimed at the stop does not run")

    async def main() -> None:
        async with fake.client() as client:
            fake.routes[f"{BASE}/claim"] = claim
            worker = WorkflowWorker(client, [counter], worker="w")
            served = asyncio.ensure_future(worker.run_async())
            await asyncio.wait_for(asked.wait(), 2)
            worker.stop(drain_seconds=0)
            await served

    # The fake's claim route answers before the queue: route it first.
    original = fake.handle

    async def handle(request: httpx.Request) -> httpx.Response:
        if request.url.path == f"{BASE}/claim":
            fake.calls.append((request.url.path, None))
            return await fake.routes[f"{BASE}/claim"](None)
        return await original(request)

    fake.handle = handle  # type: ignore[method-assign]
    asyncio.run(main())
    assert answered == ["answered"]
    (released,) = fake.bodies("fail")
    assert released["token"] == "token-1" and released["retry_at_ms"] > 0
    assert fake.bodies("operation") == [] and fake.bodies("complete") == []


def test_a_failure_names_the_code_and_the_request_of_an_lbb_error() -> None:
    @workflow(name="counter", version="v1", initial_state={})
    async def counter(ctx: WorkflowContext, state: Any, message: Any) -> WorkflowResult[Any]:
        async def call(_: WorkflowStepContext) -> None:
            raise LbbError(
                400,
                "{}",
                {"code": "invalid_param", "message": "bad page", "request_id": "req_42"},
            )

        await ctx.step("call", call)
        return WorkflowResult(state=state)

    fake = run_one(counter, task())
    (failed,) = fake.bodies("fail")
    assert failed["error"] == "LbbError: bad page (code invalid_param, request req_42)"


def test_a_second_sigint_ends_the_process() -> None:
    if os.name == "nt":
        pytest.skip("POSIX signals")
    fake = Fake([task(1)])

    async def slow_fail(_: Any) -> httpx.Response:
        # The release of the first Ctrl-C hangs: the second ends the run.
        await asyncio.sleep(5)
        return httpx.Response(200, json={})

    fake.routes[f"{BASE}/fail"] = slow_fail

    @workflow(name="counter", version="v1", initial_state={})
    async def counter(ctx: WorkflowContext, state: Any, message: Any) -> WorkflowResult[Any]:
        async def held(_: WorkflowStepContext) -> None:
            await asyncio.Event().wait()

        await ctx.step("held", held)
        return WorkflowResult(state=state)

    def interrupt() -> None:
        while not fake.bodies("operation"):
            time.sleep(0.01)
        os.kill(os.getpid(), signal.SIGINT)
        while not fake.bodies("fail"):
            time.sleep(0.01)
        os.kill(os.getpid(), signal.SIGINT)

    sender = threading.Thread(target=interrupt)
    sender.start()
    began = time.monotonic()
    with pytest.raises(KeyboardInterrupt):
        WorkflowWorker(fake.client(), [counter], worker="w", claim_wait_ms=10).run()
    sender.join()
    assert time.monotonic() - began < 4
    assert signal.getsignal(signal.SIGINT) is signal.default_int_handler
