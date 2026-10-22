"""Workflow tests (``client.workflows.tests``): each method's route, query,
body and idempotency key, ``all`` and ``wait`` (sync and async)."""

from __future__ import annotations

import json
import unittest
from typing import Any

import httpx

from lbb import AsyncLbbClient, LbbClient, models

ID = "wtest_1791967648726a1b2c3d4e5f6"


def workflow_test(status: str) -> dict[str, Any]:
    return {
        "id": ID,
        "object": "workflow_test",
        "created": "2026-10-20T10:00:00.000000Z",
        "workflow": {"starter": "values.extract", "name": "sinks"},
        "installed": True,
        "status": status,
        "fingerprint": "ab" * 16,
        "settings": {
            "class": "https://example.com/Sink",
            "fields": [],
            "properties": [{"name": "width_cm", "value_type": "f64", "unit": "cm"}],
            "gate": 0.8,
        },
        "uses": {"extract": "mock-generative", "decide": "mock-decision"},
        "inputs": [{"iri": "lbb:e/1", "label": "S 4.1", "text": "ca. 60x40 cm"}],
        "results": [],
        "counts": {
            "inputs_done": 0,
            "passed": 0,
            "review": 0,
            "no_value": 0,
            "failed": 0,
            "answered": 0,
        },
        "estimate_micro_usd": 1100,
        "cost_micro_usd": 0,
        "dataset": None,
    }


ANSWERS: list[dict[str, Any]] = [
    workflow_test("queued"),
    workflow_test("running"),
    workflow_test("done"),
    {"object": "list", "data": [workflow_test("done")], "has_more": False},
    {"object": "list", "data": [workflow_test("done")], "has_more": True, "next_cursor": ID},
    {"object": "list", "data": [workflow_test("done")], "has_more": False},
    workflow_test("done"),
    workflow_test("cancelled"),
]

ROUTES = [
    ("POST", "/v1/workflows/named/tests"),
    ("GET", "/v1/workflows/named/tests/get"),
    ("GET", "/v1/workflows/named/tests/get"),
    ("GET", "/v1/workflows/named/tests"),
    ("GET", "/v1/workflows/named/tests"),
    ("GET", "/v1/workflows/named/tests"),
    ("POST", "/v1/workflows/named/tests/answer"),
    ("POST", "/v1/workflows/named/tests/cancel"),
]


def capturing_transport(
    captured: list[httpx.Request], responses: list[dict[str, Any]]
) -> httpx.MockTransport:
    queue = list(responses)

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(200, json=queue.pop(0) if queue else {})

    return httpx.MockTransport(handler)


class WorkflowTestsTests(unittest.TestCase):
    def check(self, seen: list[httpx.Request], answers: list[Any]) -> None:
        self.assertEqual([(r.method, r.url.path) for r in seen], ROUTES)
        create, answer = seen[0], seen[6]
        self.assertEqual(json.loads(create.content), {"name": "sinks", "sample": 3})
        self.assertTrue(create.headers["idempotency-key"].startswith("workflow-tests.create:"))
        self.assertEqual(
            json.loads(answer.content),
            {
                "id": ID,
                "iri": "lbb:e/1",
                "property": "width_cm",
                "answer": {"decision": "correct", "value": 45},
            },
        )
        self.assertTrue(answer.headers["idempotency-key"].startswith("workflow-tests.answer:"))
        self.assertEqual(dict(seen[3].url.params), {"graph": "crm", "name": "sinks"})
        self.assertEqual(dict(seen[4].url.params), {"graph": "crm", "name": "sinks", "limit": "1"})
        self.assertEqual(seen[5].url.params["cursor"], ID)
        self.assertEqual(seen[7].url.params["id"], ID)
        self.assertEqual(answers[0].status.value, "queued")
        self.assertEqual(answers[1].status.value, "done")
        self.assertIsNone(answers[1].dataset)
        self.assertIsInstance(answers[2], models.WorkflowTestList)
        self.assertEqual(answers[2].data[0].id, ID)
        self.assertEqual([test.id for test in answers[3]], [ID, ID])
        self.assertIsInstance(answers[3][0], models.WorkflowTest)
        self.assertEqual(answers[5].status.value, "cancelled")

    def test_each_method_sends_its_route_body_and_key(self) -> None:
        seen: list[httpx.Request] = []
        with LbbClient(
            "http://h", graph="crm", transport=capturing_transport(seen, ANSWERS)
        ) as client:
            tests = client.workflows.tests
            answers = [
                tests.create(name="sinks", sample=3),
                tests.wait(ID, poll_interval=0),
                tests.list(name="sinks"),
                list(tests.all(name="sinks", limit=1)),
                tests.answer(ID, "lbb:e/1", "width_cm", {"decision": "correct", "value": 45}),
                tests.cancel(ID),
            ]
            self.assertFalse(hasattr(tests, "dataset"))
        self.check(seen, answers)

    def test_wait_raises_when_the_test_is_still_open_at_its_timeout(self) -> None:
        seen: list[httpx.Request] = []
        running = [workflow_test("running")] * 2
        with LbbClient(
            "http://h", graph="crm", transport=capturing_transport(seen, running)
        ) as client:
            with self.assertRaises(TimeoutError) as raised:
                client.workflows.tests.wait(ID, timeout=0, poll_interval=0)
        self.assertEqual(str(raised.exception), f"workflow test {ID} is still running")
        self.assertEqual(len(seen), 1)


class AsyncWorkflowTestsTests(unittest.IsolatedAsyncioTestCase):
    async def test_the_async_client_awaits_each_method(self) -> None:
        seen: list[httpx.Request] = []
        async with AsyncLbbClient(
            "http://h", graph="crm", transport=capturing_transport(seen, ANSWERS)
        ) as client:
            tests = client.workflows.tests
            answers = [
                await tests.create(name="sinks", sample=3),
                await tests.wait(ID, poll_interval=0),
                await tests.list(name="sinks"),
                [test async for test in tests.all(name="sinks", limit=1)],
                await tests.answer(ID, "lbb:e/1", "width_cm", {"decision": "correct", "value": 45}),
                await tests.cancel(ID),
            ]
            self.assertFalse(hasattr(tests, "dataset"))
        WorkflowTestsTests.check(self, seen, answers)  # type: ignore[arg-type]

    async def test_wait_raises_when_the_test_is_still_open_at_its_timeout(self) -> None:
        seen: list[httpx.Request] = []
        running = [workflow_test("running")] * 2
        async with AsyncLbbClient(
            "http://h", graph="crm", transport=capturing_transport(seen, running)
        ) as client:
            with self.assertRaises(TimeoutError) as raised:
                await client.workflows.tests.wait(ID, timeout=0, poll_interval=0)
        self.assertEqual(str(raised.exception), f"workflow test {ID} is still running")
        self.assertEqual(len(seen), 1)


if __name__ == "__main__":
    unittest.main()


class WorkflowTestOnARevisionTests(unittest.TestCase):
    def test_a_test_on_a_named_revision_sends_it_and_reads_it_back(self) -> None:
        seen: list[httpx.Request] = []
        revision = "wrev_00000000000000000000000000000001"
        answer = {**workflow_test("queued"), "revision": revision}
        with LbbClient(
            "http://h", graph="crm", transport=capturing_transport(seen, [answer])
        ) as client:
            test = client.workflows.tests.create(name="sinks", revision=revision, sample=3)
        self.assertEqual(
            json.loads(seen[0].content), {"name": "sinks", "revision": revision, "sample": 3}
        )
        self.assertEqual(test.revision, revision)


class WorkflowTestOnOtherModelsTests(unittest.IsolatedAsyncioTestCase):
    async def test_a_test_on_other_models_sends_them_and_reads_them_back(self) -> None:
        haiku = {"provider": "anthropic", "model": "claude-haiku-5-5", "effort": "low"}
        answer = {**workflow_test("queued"), "models": {"extract": haiku}}
        seen: list[httpx.Request] = []
        with LbbClient(
            "http://h", graph="crm", transport=capturing_transport(seen, [answer])
        ) as client:
            test = client.workflows.tests.create(
                name="sinks",
                models={"extract": models.ModelTrialModel(**haiku)},
                sample=3,
            )
        self.assertEqual(
            json.loads(seen[0].content),
            {"name": "sinks", "models": {"extract": haiku}, "sample": 3},
        )
        self.assertEqual(test.models["extract"].model, "claude-haiku-5-5")
        seen.clear()
        async with AsyncLbbClient(
            "http://h", graph="crm", transport=capturing_transport(seen, [answer])
        ) as aclient:
            await aclient.workflows.tests.create(name="sinks", models={"decide": haiku})
        self.assertEqual(json.loads(seen[0].content)["models"], {"decide": haiku})
