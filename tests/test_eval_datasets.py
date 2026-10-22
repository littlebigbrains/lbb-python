"""Evals datasets (``client.evals.datasets``): each method's route, query,
body and idempotency key, ``all`` and ``delete`` (sync and async)."""

from __future__ import annotations

import json
import unittest
from typing import Any

import httpx

from lbb import AsyncLbbClient, LbbClient, models

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
    {"object": "list", "data": [dataset(1)], "has_more": True, "next_cursor": ID},
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
    {"id": ID, "object": "eval_dataset", "deleted": True},
]

ROUTES = [
    ("POST", "/v1/evals/datasets"),
    ("POST", "/v1/evals/datasets"),
    ("GET", "/v1/evals/datasets"),
    ("GET", "/v1/evals/datasets"),
    ("GET", "/v1/evals/datasets"),
    ("GET", "/v1/evals/datasets/get"),
    ("POST", "/v1/evals/datasets/items"),
    ("GET", "/v1/evals/datasets/export"),
    ("DELETE", "/v1/evals/datasets"),
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
        created, saved, add, deleted = seen[0], seen[1], seen[6], seen[8]
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
        self.assertEqual(dict(seen[3].url.params), {"graph": "crm", "limit": "1"})
        self.assertEqual(seen[4].url.params["cursor"], ID)
        self.assertEqual(seen[5].url.params["id"], ID)
        self.assertEqual(json.loads(add.content), {"id": ID, "items": [ITEM]})
        self.assertTrue(add.headers["idempotency-key"].startswith("eval-datasets.items:"))
        self.assertEqual(seen[7].url.params["id"], ID)
        self.assertEqual(dict(deleted.url.params), {"graph": "crm", "id": ID})
        self.assertEqual(deleted.content, b"")
        self.assertTrue(deleted.headers["idempotency-key"].startswith("eval-datasets.delete:"))
        self.assertIsInstance(answers[0], models.EvalDataset)
        self.assertEqual(answers[0].id, ID)
        self.assertEqual(answers[2].data[0].item_count, 1)
        self.assertEqual([item.id for item in answers[3]], [ID, ID])
        self.assertIsInstance(answers[3][0], models.EvalDataset)
        self.assertEqual(len(answers[4].items), 1)
        self.assertEqual(answers[5].item_count, 2)
        self.assertEqual(answers[6].records[0].text, ITEM["text"])
        self.assertIsInstance(answers[7], models.EvalDatasetDeleted)
        self.assertEqual((answers[7].id, answers[7].object, answers[7].deleted), (ID, "eval_dataset", True))

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
                list(datasets.all(limit=1)),
                datasets.get(ID),
                datasets.add_items(ID, [ITEM]),
                datasets.export(ID),
                datasets.delete(ID),
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
                [item async for item in datasets.all(limit=1)],
                await datasets.get(ID),
                await datasets.add_items(ID, [ITEM]),
                await datasets.export(ID),
                await datasets.delete(ID),
            ]
        EvalDatasetsTests.check(self, seen, answers)  # type: ignore[arg-type]


if __name__ == "__main__":
    unittest.main()
