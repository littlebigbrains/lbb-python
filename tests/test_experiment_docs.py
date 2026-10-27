"""Every Python example of the guide "Compare two versions of a workflow"
(``docs-site/src/content/docs/guides/experiments.mdx``) runs here, in order,
against a fake LBB with the server's rules for experiments: the estimate
writes nothing, a start answers a ``queued`` experiment, a read answers it
``done`` with a report, and the choice and the activation that name the
experiment answer."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs

import httpx
import pytest

import lbb

DATASET = "ds_1791967648726a1b2c3d4e5f6"
EXPERIMENT = "exp_1791967648726a1b2c3d4e5f6"
REVISION = "wrev_00000000000000000000000000000001"
MODEL = {"provider": "anthropic", "model": "claude-haiku-5-5"}


def _guide(name: str) -> Path | None:
    for parent in Path(__file__).resolve().parents:
        page = parent / "docs-site" / "src" / "content" / "docs" / "guides" / name
        if page.is_file():
            return page
    return None


class FakeLbb:
    """The experiments routes the guide calls, and the two writes that apply B."""

    def __init__(self) -> None:
        self.routes: list[tuple[str, str]] = []
        self.status = "queued"
        self.applied: list[tuple[str, dict[str, Any]]] = []

    def _experiment(self) -> dict[str, Any]:
        arm = {"models": {"extract": MODEL}, "fingerprint": "ab" * 16, "test": "wtest_1" + "0" * 24}
        done = self.status == "done"
        return {
            "id": EXPERIMENT,
            "object": "eval_experiment",
            "created": "2026-10-26T10:00:00.000000Z",
            "workflow": {"name": "sinks", "created": "2026-10-20T10:00:00.000000Z"},
            "dataset": {"id": DATASET, "name": "sinks test", "items": 2},
            "a": {**arm, "revision": REVISION, "cost_micro_usd": 2200 if done else 0},
            "b": {**arm, "revision": REVISION, "cost_micro_usd": 2200 if done else 0},
            "change": {"kind": "model", "use": "extract"},
            "status": self.status,
            "finish_reason": "all_items" if done else None,
            "max_cost_micro_usd": 2_000_000,
            "estimate_micro_usd": 4400,
            "cost_micro_usd": 4400 if done else 0,
            "report": {
                "items": 2 if done else 0,
                "a": {"score": 0.5, "precision": 1.0, "recall": 0.5, "review_share": 0.0,
                      "cost_micro_usd_per_item": 1100, "failed": 0},
                "b": {"score": 1.0, "precision": 1.0, "recall": 1.0, "review_share": 0.0,
                      "cost_micro_usd_per_item": 1100, "failed": 0},
                "delta": 0.5,
                "ci_low": 0.5,
                "ci_high": 0.5,
                "outcome": "too_few",
                "qualifies": False,
            },
            "items": [
                {
                    "item": "S 1",
                    "input": "urn:lbb:item:" + DATASET + ":000000000000000000000001",
                    "a": {"score": 0.5, "correct": 1, "wrong": 0, "missed": 1, "unlabeled": 0,
                          "review": 0, "cost_micro_usd": 1100},
                    "b": {"score": 1.0, "correct": 2, "wrong": 0, "missed": 0, "unlabeled": 0,
                          "review": 0, "cost_micro_usd": 1100},
                }
            ]
            if done
            else [],
        }

    def handle(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        params = {key: values[0] for key, values in parse_qs(request.url.query.decode()).items()}
        body: dict[str, Any] = json.loads(request.content) if request.content else {}
        self.routes.append((request.method, path))
        assert params.get("graph") == "tender", "the guide names the graph on the client"
        if (request.method, path) == ("POST", "/v1/evals/experiments/estimate"):
            assert body["b"]["model"]["use"] == "extract"
            assert "idempotency-key" not in request.headers
            return httpx.Response(
                200,
                json={
                    "object": "eval_experiment_estimate",
                    "items": 2,
                    "a_micro_usd": 2200,
                    "b_micro_usd": 2200,
                    "estimate_micro_usd": 4400,
                    "max_cost_micro_usd": 5_000_000,
                },
            )
        if (request.method, path) == ("POST", "/v1/evals/experiments"):
            assert request.headers.get("idempotency-key"), "a start is sent without a key"
            assert body["max_cost_usd"] == 2 and body["workflow"] == "sinks"
            self.status = "queued"
            return httpx.Response(200, json=self._experiment())
        if (request.method, path) == ("GET", "/v1/evals/experiments/get"):
            assert params["id"] == EXPERIMENT
            self.status = "done"
            return httpx.Response(200, json=self._experiment())
        if (request.method, path) == ("POST", "/v1/evals/experiments/cancel"):
            assert request.headers.get("idempotency-key"), "a cancel is sent without a key"
            self.status = "cancelled"
            return httpx.Response(200, json=self._experiment())
        if (request.method, path) == ("PUT", "/v1/models/choices"):
            assert body["experiment"] == EXPERIMENT and body["workflow"] == "sinks"
            self.applied.append(("choice", body))
            return httpx.Response(
                200,
                json={
                    "id": "mch_" + "0" * 24,
                    "object": "model_choice",
                    "created": "2026-10-26T11:00:00.000000Z",
                    "use": "extract",
                    "scope": "workflow",
                    "graph": "tender",
                    "workflow": "sinks",
                    "model": body["model"],
                    "source": "person",
                    "since": "2026-10-26T11:00:00.000000Z",
                    "by": "key:k1",
                    "experiment": EXPERIMENT,
                    "choosable": True,
                    "options": [],
                },
            )
        if (request.method, path) == ("POST", "/v1/workflows/named/revisions/activate"):
            assert body == {"name": "sinks", "id": REVISION, "experiment": EXPERIMENT}
            self.applied.append(("activation", body))
            return httpx.Response(
                200,
                json={
                    "object": "workflow",
                    "name": "sinks",
                    "created": "2026-10-20T10:00:00.000000Z",
                    "starter": "values.extract",
                    "workflow": {"workflow_type": "lbb.values.extract", "version": "v1"},
                    "runs_in": "lbb",
                    "shards": 1,
                    "window": 4,
                    "owns": {},
                    "triggers": [],
                    "paused": False,
                    "status": {"state": "ready", "pending": 0},
                    "active_revision": REVISION,
                },
            )
        raise AssertionError(f"the guide calls a route the fake does not have: {path}")


@pytest.fixture
def fake(monkeypatch: pytest.MonkeyPatch) -> FakeLbb:
    engine = FakeLbb()

    class Client(lbb.LbbClient):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            kwargs.setdefault("transport", httpx.MockTransport(engine.handle))
            kwargs.setdefault("retry_delay", 0)
            super().__init__(*args, **kwargs)

    monkeypatch.setattr(lbb, "LbbClient", Client)
    monkeypatch.setenv("LBB_URL", "http://lbb.test")
    monkeypatch.setenv("LBB_API_KEY", "lbb_test_key")
    return engine


def test_every_example_of_the_experiments_guide_runs(fake: FakeLbb) -> None:
    page = _guide("experiments.mdx")
    if page is None:
        pytest.skip("the docs site is not in this tree")
    blocks = re.findall(r"```python\n(.*?)```", page.read_text(), re.S)
    assert blocks, f"{page.name} has no Python example"
    namespace: dict[str, Any] = {"__name__": page.stem}
    for number, block in enumerate(blocks):
        exec(compile(block, f"{page.name} block {number}", "exec"), namespace)
    # The estimate came first and wrote nothing; the start and the wait gave
    # the finished experiment; the report names the difference.
    assert namespace["cost"].estimate_micro_usd == 4400
    assert namespace["started"].status == lbb.models.EvalExperimentStatus.queued
    done = namespace["done"]
    assert done.status == lbb.models.EvalExperimentStatus.done
    assert namespace["report"].delta == 0.5
    # Both ways of applying B named the experiment.
    assert [kind for kind, _ in fake.applied] == ["choice", "activation"]
    assert namespace["chosen"].experiment == EXPERIMENT
    assert fake.routes[0] == ("POST", "/v1/evals/experiments/estimate")
    assert fake.routes[-1] == ("POST", "/v1/evals/experiments/cancel")
