"""Evals datasets (``client.evals.datasets``): each method's route, query,
body and idempotency key (sync and async)."""

from __future__ import annotations

import json
import unittest
from typing import Any

import httpx

from lbb import AsyncLbbClient, LbbClient

ID = "ds_1791967648726a1b2c3d4e5f6"
TEST_ID = "wtest_1791967648726a1b2c3d4e5f6"

ITEM: dict[str, Any] = {
    "name": "S 1",
    "text": "S 1 Waschtisch, 60 cm breit.",
    "values": [{"property": "width_cm", "value": 60, "quote": "60 cm"}],
    "leave_out": [],
}


def dataset(count: int, items: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    body: dict[str, Any] = {
        "id": ID,
        "object": "eval_dataset",
        "created": "2026-10-21T10:00:00.000000Z",
        "name": "sinks",
        "kind": "values",
        "install": {"properties": {"width_cm": "f64"}},
        "item_count": count,
    }
    if items is not None:
        body["items"] = items
    return body


ANSWERS: list[dict[str, Any]] = [
    dataset(1, [ITEM]),
    dataset(1, [ITEM]),
    {"object": "list", "data": [dataset(1)], "has_more": False},
    dataset(1, [ITEM]),
    dataset(2, [ITEM, {**ITEM, "name": "S 2"}]),
    {
        "records": [{"name": "S 1", "text": ITEM["text"]}],
        "expected": {
            "about": [],
            "install": {"properties": {"width_cm": "f64"}},
            "records": [{k: v for k, v in ITEM.items() if k != "text"}],
        },
    },
]

ROUTES = [
    ("POST", "/v1/evals/datasets"),
    ("POST", "/v1/evals/datasets"),
    ("GET", "/v1/evals/datasets"),
    ("GET", "/v1/evals/datasets/get"),
    ("POST", "/v1/evals/datasets/items"),
    ("GET", "/v1/evals/datasets/export"),
]


def capturing_transport(
    captured: list[httpx.Request], responses: list[dict[str, Any]]
) -> httpx.MockTransport:
    queue = list(responses)

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(200, json=queue.pop(0) if queue else {})

    return httpx.MockTransport(handler)


class EvalDatasetsTests(unittest.TestCase):
    def check(self, seen: list[httpx.Request], answers: list[Any]) -> None:
        self.assertEqual([(r.method, r.url.path) for r in seen], ROUTES)
        created, saved, add = seen[0], seen[1], seen[4]
        self.assertEqual(
            json.loads(created.content),
            {
                "name": "sinks",
                "kind": "values",
                "items": [ITEM],
                "install": {"properties": {"width_cm": "f64"}},
            },
        )
        self.assertTrue(created.headers["idempotency-key"].startswith("eval-datasets.create:"))
        self.assertEqual(
            json.loads(saved.content),
            {"name": "sinks answers", "kind": "values", "source": {"kind": "test", "id": TEST_ID}},
        )
        self.assertEqual(saved.headers["idempotency-key"], "save-1")
        self.assertEqual(seen[2].url.params["limit"], "5")
        self.assertEqual(seen[3].url.params["id"], ID)
        self.assertEqual(json.loads(add.content), {"id": ID, "items": [ITEM]})
        self.assertTrue(add.headers["idempotency-key"].startswith("eval-datasets.items:"))
        self.assertEqual(seen[5].url.params["id"], ID)
        self.assertEqual(answers[0].id, ID)
        self.assertEqual(answers[2].data[0].item_count, 1)
        self.assertEqual(len(answers[3].items), 1)
        self.assertEqual(answers[4].item_count, 2)
        self.assertEqual(answers[5].records[0].text, ITEM["text"])

    def test_each_method_sends_its_route_body_and_key(self) -> None:
        seen: list[httpx.Request] = []
        with LbbClient(
            "http://h", graph="crm", transport=capturing_transport(seen, ANSWERS)
        ) as client:
            datasets = client.evals.datasets
            answers = [
                datasets.create(
                    "sinks", items=[ITEM], install={"properties": {"width_cm": "f64"}}
                ),
                datasets.create(
                    "sinks answers",
                    source={"kind": "test", "id": TEST_ID},
                    idempotency_key="save-1",
                ),
                datasets.list(limit=5),
                datasets.get(ID),
                datasets.add_items(ID, [ITEM]),
                datasets.export(ID),
            ]
        self.check(seen, answers)


class AsyncEvalDatasetsTests(unittest.IsolatedAsyncioTestCase):
    async def test_the_async_client_awaits_each_method(self) -> None:
        seen: list[httpx.Request] = []
        async with AsyncLbbClient(
            "http://h", graph="crm", transport=capturing_transport(seen, ANSWERS)
        ) as client:
            datasets = client.evals.datasets
            answers = [
                await datasets.create(
                    "sinks", items=[ITEM], install={"properties": {"width_cm": "f64"}}
                ),
                await datasets.create(
                    "sinks answers",
                    source={"kind": "test", "id": TEST_ID},
                    idempotency_key="save-1",
                ),
                await datasets.list(limit=5),
                await datasets.get(ID),
                await datasets.add_items(ID, [ITEM]),
                await datasets.export(ID),
            ]
        EvalDatasetsTests.check(self, seen, answers)  # type: ignore[arg-type]


if __name__ == "__main__":
    unittest.main()
