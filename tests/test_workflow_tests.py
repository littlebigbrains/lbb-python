"""Workflow tests (``client.workflows.tests``): each method's route, query,
body and idempotency key, and ``wait`` (sync and async)."""

from __future__ import annotations

import json
import unittest
from typing import Any

import httpx

from lbb import AsyncLbbClient, LbbClient

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
    }


ANSWERS: list[dict[str, Any]] = [
    workflow_test("queued"),
    workflow_test("running"),
    workflow_test("done"),
    {"object": "list", "data": [workflow_test("done")], "has_more": False},
    workflow_test("done"),
    workflow_test("cancelled"),
    {
        "kind": "values",
        "test": ID,
        "fingerprint": "ab" * 16,
        "records": [],
        "expected": {"about": [], "install": {"properties": {}}, "records": []},
    },
]

ROUTES = [
    ("POST", "/v1/workflows/triggered/tests"),
    ("GET", "/v1/workflows/triggered/tests/get"),
    ("GET", "/v1/workflows/triggered/tests/get"),
    ("GET", "/v1/workflows/triggered/tests"),
    ("POST", "/v1/workflows/triggered/tests/answer"),
    ("POST", "/v1/workflows/triggered/tests/cancel"),
    ("GET", "/v1/workflows/triggered/tests/dataset"),
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
        create, answer = seen[0], seen[4]
        self.assertEqual(
            json.loads(create.content),
            {"workflow": {"starter": "values.extract", "name": "sinks"}, "sample": 3},
        )
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
        self.assertEqual(seen[3].url.params["name"], "sinks")
        self.assertEqual(seen[5].url.params["id"], ID)
        self.assertEqual(answers[0].status.value, "queued")
        self.assertEqual(answers[1].status.value, "done")
        self.assertEqual(answers[2].data[0].id, ID)
        self.assertEqual(answers[4].status.value, "cancelled")
        self.assertEqual(answers[5].test, ID)

    def test_each_method_sends_its_route_body_and_key(self) -> None:
        seen: list[httpx.Request] = []
        with LbbClient(
            "http://h", graph="crm", transport=capturing_transport(seen, ANSWERS)
        ) as client:
            tests = client.workflows.tests
            answers = [
                tests.create(workflow={"starter": "values.extract", "name": "sinks"}, sample=3),
                tests.wait(ID, poll_interval=0),
                tests.list(name="sinks"),
                tests.answer(ID, "lbb:e/1", "width_cm", {"decision": "correct", "value": 45}),
                tests.cancel(ID),
                tests.dataset(ID),
            ]
        self.check(seen, answers)


class AsyncWorkflowTestsTests(unittest.IsolatedAsyncioTestCase):
    async def test_the_async_client_awaits_each_method(self) -> None:
        seen: list[httpx.Request] = []
        async with AsyncLbbClient(
            "http://h", graph="crm", transport=capturing_transport(seen, ANSWERS)
        ) as client:
            tests = client.workflows.tests
            answers = [
                await tests.create(
                    workflow={"starter": "values.extract", "name": "sinks"}, sample=3
                ),
                await tests.wait(ID, poll_interval=0),
                await tests.list(name="sinks"),
                await tests.answer(ID, "lbb:e/1", "width_cm", {"decision": "correct", "value": 45}),
                await tests.cancel(ID),
                await tests.dataset(ID),
            ]
        WorkflowTestsTests.check(self, seen, answers)  # type: ignore[arg-type]


if __name__ == "__main__":
    unittest.main()
