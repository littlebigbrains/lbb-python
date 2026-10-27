"""``client.evals.experiments``: each operation maps to its route with typed
answers, the sync and async clients page and wait the same way, and a create
and a cancel carry idempotency keys while an estimate and a read carry none."""

from __future__ import annotations

import json
import unittest
from typing import Any

import httpx

from lbb import AsyncLbbClient, LbbClient, LbbError, models

from test_client import capturing_transport  # isort: skip

EXPERIMENT_ID = "exp_1791967648726a1b2c3d4e5f6"
DATASET_ID = "ds_1791967648726a1b2c3d4e5f6"
B = {"model": {"use": "extract", "model": {"provider": "mock", "model": "mock-candidate"}}}


ARM_REPORT = {
    "score": 0.5,
    "precision": 1.0,
    "recall": 0.5,
    "review_share": 0.0,
    "cost_micro_usd_per_item": 1100,
    "failed": 0,
}


def experiment(status: str = "running", id: str = EXPERIMENT_ID) -> dict[str, Any]:
    arm = {
        "revision": "wrev_" + "ab" * 16,
        "models": {},
        "fingerprint": "ab" * 16,
        "test": "wtest_1791967648726a1b2c3d4e5f6",
        "cost_micro_usd": 0,
    }
    return {
        "id": id,
        "object": "eval_experiment",
        "created": "2026-10-26T10:00:00.000000Z",
        "workflow": {"name": "sinks", "created": "2026-10-20T10:00:00.000000Z"},
        "dataset": {"id": DATASET_ID, "name": "sinks", "items": 6},
        "a": arm,
        "b": arm,
        "change": {"kind": "model", "use": "extract"},
        "status": status,
        "max_cost_micro_usd": 5_000_000,
        "estimate_micro_usd": 13_200,
        "cost_micro_usd": 0,
        "report": {
            "items": 0,
            "a": ARM_REPORT,
            "b": ARM_REPORT,
            "delta": 0.0,
            "ci_low": 0.0,
            "ci_high": 0.0,
            "outcome": "too_few",
            "qualifies": False,
        },
    }


ESTIMATE = {
    "object": "eval_experiment_estimate",
    "items": 6,
    "a_micro_usd": 6600,
    "b_micro_usd": 6600,
    "estimate_micro_usd": 13200,
    "max_cost_micro_usd": 5_000_000,
}


class EvalExperimentsTests(unittest.TestCase):
    def test_each_operation_maps_to_its_route_with_typed_answers(self) -> None:
        seen: list[httpx.Request] = []
        responses: list[dict[str, Any]] = [
            {"json": experiment("queued")},
            {"json": ESTIMATE},
            {"json": experiment()},
            {"json": experiment("cancelled")},
        ]
        with LbbClient("http://h", graph="crm", transport=capturing_transport(seen, responses)) as client:
            started = client.evals.experiments.create("sinks", DATASET_ID, B, max_items=6)
            estimate = client.evals.experiments.estimate("sinks", DATASET_ID, B, max_cost_usd=2.5)
            read = client.evals.experiments.get(EXPERIMENT_ID)
            cancelled = client.evals.experiments.cancel(EXPERIMENT_ID)
        self.assertIsInstance(started, models.EvalExperiment)
        self.assertEqual(started.status, models.EvalExperimentStatus.queued)
        self.assertIsInstance(estimate, models.EvalExperimentEstimate)
        self.assertEqual(estimate.estimate_micro_usd, 13200)
        self.assertEqual(read.id, EXPERIMENT_ID)
        self.assertEqual(cancelled.status, models.EvalExperimentStatus.cancelled)
        self.assertEqual(
            [(r.method, r.url.path, dict(r.url.params)) for r in seen],
            [
                ("POST", "/v1/evals/experiments", {"graph": "crm"}),
                ("POST", "/v1/evals/experiments/estimate", {"graph": "crm"}),
                ("GET", "/v1/evals/experiments/get", {"graph": "crm", "id": EXPERIMENT_ID}),
                ("POST", "/v1/evals/experiments/cancel", {"graph": "crm", "id": EXPERIMENT_ID}),
            ],
        )
        self.assertEqual(
            json.loads(seen[0].content),
            {"workflow": "sinks", "dataset": DATASET_ID, "b": B, "max_items": 6},
        )
        self.assertEqual(
            json.loads(seen[1].content),
            {"workflow": "sinks", "dataset": DATASET_ID, "b": B, "max_cost_usd": 2.5},
        )
        self.assertTrue(
            seen[0].headers["idempotency-key"].startswith("eval-experiments.create:")
        )
        self.assertNotIn("idempotency-key", seen[1].headers)
        self.assertTrue(
            seen[3].headers["idempotency-key"].startswith("eval-experiments.cancel:")
        )

    def test_a_create_keeps_the_callers_key(self) -> None:
        seen: list[httpx.Request] = []
        with LbbClient("http://h", transport=capturing_transport(seen, {"json": experiment()})) as client:
            client.evals.experiments.create("sinks", DATASET_ID, B, idempotency_key="mine-1")
        self.assertEqual(seen[0].headers["idempotency-key"], "mine-1")

    def test_all_pages_through_the_list_with_its_filters(self) -> None:
        seen: list[httpx.Request] = []
        first = "exp_1791967648726a1b2c3d4e5f8"
        second = "exp_1791967648726a1b2c3d4e5f7"
        responses: list[dict[str, Any]] = [
            {"json": {"object": "list", "data": [experiment(id=first)], "has_more": True, "next_cursor": second}},
            {"json": {"object": "list", "data": [experiment(id=second)], "has_more": False}},
        ]
        with LbbClient("http://h", transport=capturing_transport(seen, responses)) as client:
            ids = [
                item.id
                for item in client.evals.experiments.all(workflow="sinks", status="done", limit=1)
            ]
        self.assertEqual(ids, [first, second])
        self.assertEqual(dict(seen[0].url.params).get("workflow"), "sinks")
        self.assertEqual(dict(seen[0].url.params).get("status"), "done")
        self.assertEqual(dict(seen[1].url.params).get("cursor"), second)

    def test_wait_reads_until_the_experiment_ends(self) -> None:
        seen: list[httpx.Request] = []
        responses: list[dict[str, Any]] = [
            {"json": experiment("queued")},
            {"json": experiment("running")},
            {"json": experiment("done")},
        ]
        with LbbClient("http://h", transport=capturing_transport(seen, responses)) as client:
            done = client.evals.experiments.wait(EXPERIMENT_ID, poll_interval=0.0)
        self.assertEqual(done.status, models.EvalExperimentStatus.done)
        self.assertEqual(len(seen), 3)

    def test_wait_raises_a_timeout_error_when_the_experiment_stays_open(self) -> None:
        seen: list[httpx.Request] = []
        with LbbClient("http://h", transport=capturing_transport(seen, [{"json": experiment()}] * 20)) as client:
            with self.assertRaises(TimeoutError):
                client.evals.experiments.wait(EXPERIMENT_ID, timeout=0.0, poll_interval=0.0)
            with self.assertRaises(ValueError):
                client.evals.experiments.wait(EXPERIMENT_ID, poll_interval=-1.0)

    def test_a_refusal_raises_the_experiments_code(self) -> None:
        seen: list[httpx.Request] = []
        body = {
            "error": {
                "type": "invalid_request_error",
                "code": "experiment_arms_mixed",
                "message": "B changes one part of A",
                "param": "b",
            }
        }
        with LbbClient("http://h", transport=capturing_transport(seen, {"status": 400, "json": body})) as client:
            with self.assertRaises(LbbError) as raised:
                client.evals.experiments.create("sinks", DATASET_ID, {"revision": "wrev_1", **B})
        self.assertEqual(raised.exception.code, "experiment_arms_mixed")


class AsyncEvalExperimentsTests(unittest.IsolatedAsyncioTestCase):
    async def test_the_async_namespace_awaits_each_method(self) -> None:
        seen: list[httpx.Request] = []
        responses: list[dict[str, Any]] = [
            {"json": experiment("queued")},
            {"json": ESTIMATE},
            {"json": {"object": "list", "data": [experiment()], "has_more": False}},
            {"json": {"object": "list", "data": [experiment()], "has_more": False}},
            {"json": experiment("cancelled")},
            {"json": experiment("running")},
            {"json": experiment("done")},
        ]
        async with AsyncLbbClient("http://h", transport=capturing_transport(seen, responses)) as client:
            started = await client.evals.experiments.create("sinks", DATASET_ID, B)
            estimate = await client.evals.experiments.estimate("sinks", DATASET_ID, B)
            page = await client.evals.experiments.list(workflow="sinks")
            ids = [item.id async for item in client.evals.experiments.all()]
            cancelled = await client.evals.experiments.cancel(EXPERIMENT_ID)
            done = await client.evals.experiments.wait(EXPERIMENT_ID, poll_interval=0.0)
        self.assertEqual(started.id, EXPERIMENT_ID)
        self.assertEqual(estimate.items, 6)
        self.assertEqual(page.data[0].id, EXPERIMENT_ID)
        self.assertEqual(ids, [EXPERIMENT_ID])
        self.assertEqual(cancelled.status, models.EvalExperimentStatus.cancelled)
        self.assertEqual(done.status, models.EvalExperimentStatus.done)


if __name__ == "__main__":
    unittest.main()
