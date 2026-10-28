"""The workflow worker conformance suite (``sdk/conformance/workflow-worker``)
on ``lbb.workflows.WorkflowWorker``: each scenario's scripted server exchange, and the
requests the worker must send. The TypeScript suite runs the same scenarios
(``sdk/typescript/src/worker-conformance.test.ts``); the format is in the
suite's README."""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Mapping
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl

import httpx
import pytest
from pydantic import BaseModel

from lbb import AsyncLbbClient
from lbb.workflows import (
    ActivityCaptureResult,
    FailedInstance,
    OwnedWrite,
    WorkflowContext,
    WorkflowError,
    WorkflowResult,
    WorkflowStepContext,
    WorkflowWait,
    WorkflowWorker,
    capture_activities,
    lbb_activities,
    triggered_workflow,
    workflow,
)


def _suite() -> Path:
    for parent in Path(__file__).resolve().parents:
        for candidate in (parent / "conformance", parent / "sdk" / "conformance"):
            if (candidate / "workflow-worker" / "scenarios").is_dir():
                return candidate / "workflow-worker" / "scenarios"
    raise FileNotFoundError("sdk/conformance/workflow-worker/scenarios not found")


SCENARIOS = sorted(_suite().glob("*.json"))


def _merge(base: Any, overrides: Any) -> Any:
    if isinstance(base, dict) and isinstance(overrides, dict):
        out = dict(base)
        for key, value in overrides.items():
            out[key] = _merge(base.get(key), value) if key in base else value
        return out
    return overrides


def _path(value: Any, path: str) -> Any:
    for part in [p for p in path.split(".") if p]:
        if isinstance(value, Mapping):
            value = value[part]
        elif isinstance(value, list):
            value = value[int(part)]
        else:
            value = getattr(value, part)
    return value


def _with_index(value: Any, i: int) -> Any:
    """A template with ``{i}`` in its texts replaced by ``i`` (``$repeat``)."""
    if isinstance(value, str):
        return value.replace("{i}", str(i))
    if isinstance(value, list):
        return [_with_index(item, i) for item in value]
    if isinstance(value, dict):
        return {key: _with_index(item, i) for key, item in value.items()}
    return value


def _matches(want: Any, got: Any, arrived_ms: int, missing: bool = False) -> bool:
    if isinstance(want, dict) and "$absent" in want:
        return missing
    if missing:
        return False
    if isinstance(want, dict) and "$any" in want:
        return True
    if isinstance(want, dict) and "$all" in want:
        return all(_matches(each, got, arrived_ms) for each in want["$all"])
    if isinstance(want, dict) and "$length" in want:
        return isinstance(got, list) and len(got) == want["$length"]
    if isinstance(want, dict) and "$json_bytes_at_most" in want:
        size = len(json.dumps(got, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
        return size <= want["$json_bytes_at_most"]
    if isinstance(want, dict) and "$at_least_now_plus_ms" in want:
        return (
            isinstance(got, (int, float))
            and got >= arrived_ms + want["$at_least_now_plus_ms"] - 100
        )
    if isinstance(want, dict):
        return isinstance(got, dict) and all(
            _matches(item, got.get(key), arrived_ms, key not in got) for key, item in want.items()
        )
    if isinstance(want, list):
        return (
            isinstance(got, list)
            and len(want) == len(got)
            and all(_matches(w, g, arrived_ms) for w, g in zip(want, got, strict=True))
        )
    return bool(want == got and type(want) is type(got)) or (
        isinstance(want, (int, float))
        and isinstance(got, (int, float))
        and not isinstance(want, bool)
        and not isinstance(got, bool)
        and want == got
    )


class _Server:
    """The scripted server: each request must match the next exchange."""

    def __init__(self, scenario: dict[str, Any]) -> None:
        self.scenario = scenario
        self.exchanges: list[dict[str, Any]] = scenario["exchanges"]
        self.bodies: list[Any] = []
        self.problems: list[str] = []

    async def handle(self, request: httpx.Request) -> httpx.Response:
        arrived_ms = time.time_ns() // 1_000_000
        index = len(self.bodies)
        raw = request.content.decode()
        try:
            body: Any = json.loads(raw) if raw else None
        except ValueError:
            body = raw
        self.bodies.append(body)
        where = f"request {index} ({request.method} {request.url.path})"
        if index >= len(self.exchanges):
            self.problems.append(f"{where}: no exchange is left for it, body {raw}")
            return httpx.Response(500, json={"error": {"code": "unexpected", "message": "unexpected"}})
        exchange = self.exchanges[index]
        want = exchange["request"]
        if request.method != want["method"] or request.url.path != want["path"]:
            self.problems.append(f"{where}: expected {want['method']} {want['path']}")
        query = dict(parse_qsl(request.url.query.decode()))
        for key, value in want.get("query", {}).items():
            if query.get(key) != value:
                self.problems.append(f"{where}: query {key} is {query.get(key)!r}, not {value!r}")
        for key, value in want.get("headers", {}).items():
            if request.headers.get(key) != value:
                got = request.headers.get(key)
                self.problems.append(f"{where}: header {key} is {got!r}, not {value!r}")
        if "raw" in want and raw != want["raw"]:
            self.problems.append(f"{where}: body {raw!r}, not {want['raw']!r}")
        if "body" in want and not _matches(want["body"], body, arrived_ms):
            self.problems.append(f"{where}: body {raw} does not match {json.dumps(want['body'])}")
        if "same_body_as" in want and body != self.bodies[want["same_body_as"]]:
            self.problems.append(f"{where}: body differs from request {want['same_body_as']}")
        answer = exchange.get("response", {})
        if answer.get("delay_ms"):
            await asyncio.sleep(answer["delay_ms"] / 1000)
        if answer.get("transport_error"):
            raise httpx.ConnectError("the connection failed", request=request)
        if "claim" in answer:
            claim = answer["claim"]
            task = None if claim is None else _merge(self.scenario["task"], claim)
            return httpx.Response(200, json={"task": task})
        return httpx.Response(answer.get("status", 200), json=answer.get("body", {}))


def _dump(value: Any) -> Any:
    """A model or a list of models as the JSON the server sends."""
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json", by_alias=True, exclude_none=True)
    if isinstance(value, list):
        return [_dump(item) for item in value]
    return value


def _captured(result: ActivityCaptureResult) -> dict[str, Any]:
    """What a capture kept, in the suite's names."""
    writes = []
    for write in result.writes:
        kept = {
            "step_key": write.step_key,
            "graph": write.graph,
            "body": _dump(write.body),
            "check": write.check,
            "errors": write.errors,
            "report": _dump(write.report),
        }
        writes.append({key: value for key, value in kept.items() if value is not None})
    return {
        "writes": writes,
        "reviews": [{"request": dict(review.request)} for review in result.reviews],
        "results": result.results,
    }


class _Program:
    """The scenario's handler program, run with the SDK's context: the
    activities of the client, or of a capture when the scenario names one."""

    def __init__(self, client: AsyncLbbClient, capture: Mapping[str, Any] | None = None) -> None:
        self.client = client
        self.capture = (
            capture_activities(lbb_activities(client), workflow=capture["workflow"])
            if capture is not None
            else None
        )
        self.activities = self.capture.activities if self.capture else lbb_activities(client)
        self.ran: list[str] = []
        self.effect_ids: list[str] = []

    def resolve(self, value: Any, outputs: dict[str, Any], state: Any, message: Any) -> Any:
        if isinstance(value, dict):
            if "$repeat" in value:
                return [
                    self.resolve(_with_index(value["of"], i), outputs, state, message)
                    for i in range(value["$repeat"])
                ]
            if "$chars" in value:
                return str(value.get("char", "q")) * value["$chars"]
            if "$ref" in value:
                key, _, rest = value["$ref"].partition(".")
                return _path(outputs[key], rest)
            if "$message" in value:
                return _path(message, value["$message"])
            if "$state" in value:
                return _path(state, value["$state"])
            return {k: self.resolve(v, outputs, state, message) for k, v in value.items()}
        if isinstance(value, list):
            return [self.resolve(v, outputs, state, message) for v in value]
        return value

    async def act(self, ctx: WorkflowContext, step: WorkflowStepContext, action: dict[str, Any]) -> None:
        if "heartbeat" in action:
            if "checkpoint" in action:
                await step.heartbeat(action["heartbeat"], checkpoint=action["checkpoint"])
            else:
                await step.heartbeat(action["heartbeat"])
        elif "progress" in action:
            step.progress(
                action["progress"],
                done=action["done"],
                total=action.get("total"),
                unit=action.get("unit"),
            )
        elif "sleep_ms" in action:
            await asyncio.sleep(action["sleep_ms"] / 1000)
        elif "block" in action:
            await asyncio.Event().wait()
        elif "raise" in action:
            raise (WorkflowError if action["raise"] == "workflow_error" else RuntimeError)(
                action.get("message", "raised")
            )
        elif "reason" in action:
            # `effect_id`: true for the step's own, text for an id made from it.
            made = action.get("effect_id")
            suffix = made if isinstance(made, str) else ""
            await self.activities.reason(
                action["reason"]["prompt"],
                action["reason"]["schema"],
                effect_id=f"{step.effect_id}{suffix}" if made else None,
            )
        elif "review" in action:
            await self.activities.review(action["review"])
        elif "blob_put" in action:
            await self.client.workflows.blobs.put(action["blob_put"], workflow_id=ctx.workflow_id)
        else:
            raise AssertionError(f"unknown action {action}")

    async def run(
        self, ops: list[dict[str, Any]], ctx: WorkflowContext, state: Any, message: Any
    ) -> WorkflowResult[Any] | None:
        outputs: dict[str, Any] = {}

        def value(v: Any) -> Any:
            return self.resolve(v, outputs, state, message)

        for op in ops:
            if "step" in op:
                key = op["step"]

                async def fn(s: WorkflowStepContext, op: dict[str, Any] = op, key: str = key) -> Any:
                    self.ran.append(key)
                    self.effect_ids.append(s.effect_id)
                    for action in op.get("do", []):
                        await self.act(ctx, s, action)
                    return value(op.get("returns"))

                options: dict[str, Any] = {
                    k: op[k] for k in ("kind", "timeout_ms", "heartbeat_timeout_ms") if k in op
                }
                outputs[key] = await ctx.step(key, fn, **options)
            elif "sleep" in op:
                await ctx.sleep(op["sleep"], op["ms"])
                outputs[op["sleep"]] = None
            elif "wait_for_signal" in op:
                got = await ctx.wait_for_signal(
                    op["wait_for_signal"], name=op["name"], timeout_ms=op["timeout_ms"]
                )
                outputs[op["wait_for_signal"]] = {"received": got.received, "value": got.value}
            elif "write_owned" in op:
                write = OwnedWrite(**value(op["write"]))
                graph = value(op.get("graph"))
                written = await self.activities.write_owned(
                    ctx, op["write_owned"], write, graph=graph
                )
                outputs[op["write_owned"]] = written.model_dump(mode="json")
            elif "requeue" in op:
                failed = [FailedInstance(**item) for item in op["failed"]]
                graph = value(op.get("graph"))
                answer = await self.activities.requeue(ctx, op["requeue"], failed, graph=graph)
                outputs[op["requeue"]] = answer.model_dump(mode="json") if answer else None
            elif "wait_for_review" in op:
                waited = await self.activities.wait_for_review(
                    ctx, op["wait_for_review"], op["request"], timeout_ms=op["timeout_ms"]
                )
                outputs[op["wait_for_review"]] = {
                    "answered": waited.answered,
                    "answer": _dump(waited.answer),
                    "item": _dump(waited.item),
                }
            elif "review" in op:
                outputs[op["review"]] = _dump(await self.activities.review(op["request"]))
            elif "review_items" in op:
                outputs[op["review_items"]] = _dump(await self.activities.review_items(op["keys"]))
            elif "wait" in op:
                raise WorkflowWait(op["wait"])
            elif "raise" in op:
                raise (WorkflowError if op["raise"] == "workflow_error" else RuntimeError)(
                    op.get("message", "raised")
                )
            elif "return" in op:
                ret = op["return"]
                return WorkflowResult(state=value(ret.get("state")), result=value(ret.get("result")))
            elif "continue" in op:
                ret = op["continue"]
                return ctx.continue_(value(ret["state"]), value(ret["message"]))
            elif "keep" in op:
                return None
            else:
                raise AssertionError(f"unknown operation {op}")
        raise AssertionError("the program ends without a return")


def _definition(scenario: dict[str, Any], program: _Program) -> Any:
    spec = scenario["workflow"]
    if spec["type"] == "plain":

        async def handler(ctx: WorkflowContext, state: Any, message: Any) -> WorkflowResult[Any]:
            result = await program.run(scenario["handler"], ctx, state, message)
            assert result is not None
            return result

        return workflow(
            name=spec["name"],
            version=spec["version"],
            initial_state=spec.get("initial_state"),
            on_message=handler,
        )

    async def on_instances(ctx: WorkflowContext, message: Any, state: Any) -> Any:
        return await program.run(scenario["on_instances"], ctx, state, message)

    async def on_fan_in(ctx: WorkflowContext, message: Any, state: Any) -> Any:
        return await program.run(scenario["on_fan_in"], ctx, state, message)

    async def on_schedule(ctx: WorkflowContext, message: Any, state: Any) -> Any:
        return await program.run(
            scenario["on_schedule"], ctx, state, message.model_dump(mode="json", by_alias=True)
        )

    async def on_events(ctx: WorkflowContext, events: Any, message: Any, state: Any) -> Any:
        # The program reads the batch's message with its events beside it.
        value = message.model_dump(mode="json", by_alias=True)
        value["events"] = [event.model_dump(mode="json", by_alias=True) for event in events]
        return await program.run(scenario["on_events"], ctx, state, value)

    return triggered_workflow(
        name=spec["name"],
        version=spec["version"],
        initial_state=spec.get("initial_state"),
        on_instances=on_instances if "on_instances" in scenario else None,
        on_fan_in=on_fan_in if "on_fan_in" in scenario else None,
        on_schedule=on_schedule if "on_schedule" in scenario else None,
        on_events=on_events if "on_events" in scenario else None,
    )


async def _run(scenario: dict[str, Any]) -> None:
    server = _Server(scenario)
    async with AsyncLbbClient(
        "http://localhost:7400",
        graph=scenario.get("client_graph", "main"),
        max_retries=2,
        retry_delay=0,
        transport=httpx.MockTransport(server.handle),
    ) as client:
        program = _Program(client, scenario.get("capture"))
        options = scenario["workflow"].get("worker", {})
        worker = WorkflowWorker(client, [_definition(scenario, program)], worker="conformance", **options)
        outcomes: list[str] = []
        for _ in range(scenario.get("runs", 1)):
            try:
                outcomes.append("claimed" if await worker.run_once() else "idle")
            except Exception:
                outcomes.append("error")
        expect = scenario["expect"]
        if expect.get("settle_ms"):
            await asyncio.sleep(expect["settle_ms"] / 1000)
    problems = list(server.problems)
    if len(server.bodies) < len(server.exchanges):
        problems.append(
            f"{len(server.exchanges) - len(server.bodies)} exchange(s) were not used: the worker "
            f"sent {len(server.bodies)} request(s)"
        )
    if outcomes != expect["outcomes"]:
        problems.append(f"outcomes {outcomes}, not {expect['outcomes']}")
    if "ran" in expect and program.ran != expect["ran"]:
        problems.append(f"ran {program.ran}, not {expect['ran']}")
    if "effect_ids" in expect and program.effect_ids != expect["effect_ids"]:
        problems.append(f"effect ids {program.effect_ids}, not {expect['effect_ids']}")
    if "captured" in expect:
        assert program.capture is not None, "the scenario expects a capture and names none"
        captured = _captured(program.capture.result())
        if not _matches(expect["captured"], captured, 0):
            problems.append(
                f"captured {json.dumps(captured)} does not match {json.dumps(expect['captured'])}"
            )
    assert not problems, f"{scenario['name']}: {scenario['rule']}\n" + "\n".join(problems)


@pytest.mark.parametrize("path", SCENARIOS, ids=[path.stem for path in SCENARIOS])
def test_scenario(path: Path) -> None:
    asyncio.run(_run(json.loads(path.read_text())))


def test_the_suite_covers_the_named_rules() -> None:
    names = {path.stem for path in SCENARIOS}
    for rule in ("replay", "lease", "fence", "wait", "fan-in", "signal", "timeout", "review", "captured", "owns"):
        assert any(rule in name for name in names), rule
