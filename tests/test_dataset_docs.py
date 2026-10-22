"""Every Python example of the guides "Test a workflow before it writes"
(``docs-site/src/content/docs/guides/test-a-workflow.mdx``) and "Datasets of
examples" (``…/guides/datasets.mdx``) runs here, in order, against a fake LBB
with the server's rules for tests and datasets: a test has one dataset, a
save of a test whose dataset exists brings that dataset up to date, and a
deleted dataset answers 404."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs

import httpx
import pytest

import lbb

TEST_ID = "wtest_1791967648726a1b2c3d4e5f6"
IRI = "https://example.com/sink/s-4-1"


def _guide(name: str) -> Path | None:
    for parent in Path(__file__).resolve().parents:
        page = parent / "docs-site" / "src" / "content" / "docs" / "guides" / name
        if page.is_file():
            return page
    return None


def _not_found(message: str) -> httpx.Response:
    return httpx.Response(
        404,
        json={"error": {"type": "invalid_request_error", "code": "not_found", "message": message}},
    )


class FakeLbb:
    """The tests and datasets routes the guides call."""

    def __init__(self) -> None:
        self.tests: dict[str, dict[str, Any]] = {}
        self.datasets: dict[str, dict[str, Any]] = {}
        self.deleted: set[str] = set()
        self.routes: list[tuple[str, str]] = []
        # A test that a person answered: the datasets guide saves it.
        self.tests[TEST_ID] = self._test(TEST_ID, "sinks", "done")
        self.tests[TEST_ID]["results"][0]["answers"] = {
            "width_cm": {"decision": "correct", "value": 45}
        }

    def _test(self, id: str, name: str, status: str) -> dict[str, Any]:
        return {
            "id": id,
            "object": "workflow_test",
            "created": "2026-10-20T10:00:00.000000Z",
            "workflow": {"starter": "values.extract", "name": name},
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
            "inputs": [{"iri": IRI, "label": "S 4.1", "text": "Waschtisch, ca. 60x40 cm"}],
            "results": [
                {
                    "iri": IRI,
                    "values": [
                        {
                            "property": "width_cm",
                            "value": "60",
                            "quote": "ca. 60x40 cm",
                            "support": 0.9,
                            "outcome": "written",
                        }
                    ],
                    "failed": False,
                }
            ],
            "counts": {
                "inputs_done": 1,
                "passed": 1,
                "review": 0,
                "no_value": 0,
                "failed": 0,
                "answered": 0,
            },
            "estimate_micro_usd": 1100,
            "cost_micro_usd": 1100,
            "dataset": None,
        }

    def _items_of(self, test: dict[str, Any]) -> list[dict[str, Any]]:
        items = []
        for result in test["results"]:
            answers = result.get("answers") or {}
            if not answers:
                continue
            values = [
                {"property": property, "value": answer.get("value", 60)}
                for property, answer in answers.items()
            ]
            items.append({"name": "S 4.1", "iri": result["iri"], "text": "Waschtisch", "values": values})
        return items

    def _listed(self, dataset: dict[str, Any]) -> dict[str, Any]:
        return {key: value for key, value in dataset.items() if key != "items"}

    def _create(self, body: dict[str, Any]) -> dict[str, Any]:
        source = body.get("source")
        if source is not None:
            test = self.tests[source["id"]]
            items = self._items_of(test)
            linked = test.get("dataset")
            if linked is not None and linked not in self.deleted:
                dataset = self.datasets[linked]
                dataset.update(name=body["name"], items=items, item_count=len(items))
                return dataset
        else:
            items = body.get("items", [])
        id = f"ds_{len(self.datasets) + 1:027d}"
        dataset = {
            "id": id,
            "object": "eval_dataset",
            "created": "2026-10-21T10:00:00.000000Z",
            "name": body["name"],
            "kind": body.get("kind", "values"),
            "install": body.get("install", {"properties": {"width_cm": "f64"}}),
            "about": body.get("about", []),
            "item_count": len(items),
            "items": items,
        }
        self.datasets[id] = dataset
        if source is not None:
            self.tests[source["id"]]["dataset"] = id
        return dataset

    def handle(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        params = {key: values[0] for key, values in parse_qs(request.url.query.decode()).items()}
        body: dict[str, Any] = json.loads(request.content) if request.content else {}
        self.routes.append((request.method, path))
        assert params.get("graph") == "tender", "the guides name the graph on the client"
        if request.method != "GET":
            assert request.headers.get("idempotency-key"), f"{path} is sent without a key"
        if (request.method, path) == ("POST", "/v1/workflows/named/tests"):
            assert body == {"name": "sinks", "sample": 6}
            id = f"wtest_{len(self.tests) + 1:027d}"
            self.tests[id] = self._test(id, body["name"], "queued")
            return httpx.Response(200, json=self.tests[id])
        if (request.method, path) == ("GET", "/v1/workflows/named/tests/get"):
            test = self.tests[params["id"]]
            test["status"] = "done"
            return httpx.Response(200, json=test)
        if (request.method, path) == ("GET", "/v1/workflows/named/tests"):
            data = [
                test
                for test in reversed(self.tests.values())
                if params.get("name") in (None, test["workflow"]["name"])
            ]
            return httpx.Response(200, json={"object": "list", "data": data, "has_more": False})
        if (request.method, path) == ("POST", "/v1/workflows/named/tests/answer"):
            test = self.tests[body["id"]]
            (result,) = [result for result in test["results"] if result["iri"] == body["iri"]]
            result.setdefault("answers", {})[body["property"]] = body["answer"]
            test["counts"]["answered"] = len(result["answers"])
            return httpx.Response(200, json=test)
        if (request.method, path) == ("POST", "/v1/evals/datasets"):
            return httpx.Response(200, json=self._create(body))
        if (request.method, path) == ("GET", "/v1/evals/datasets"):
            data = [
                self._listed(dataset)
                for id, dataset in reversed(self.datasets.items())
                if id not in self.deleted
            ]
            return httpx.Response(200, json={"object": "list", "data": data, "has_more": False})
        if path in (
            "/v1/evals/datasets/get",
            "/v1/evals/datasets/export",
            "/v1/evals/datasets/items",
        ):
            dataset_id = str(params.get("id") or body.get("id"))
            if dataset_id not in self.datasets or dataset_id in self.deleted:
                return _not_found(f"dataset {dataset_id}")
            dataset = self.datasets[dataset_id]
            if path.endswith("/items"):
                items = {item["name"]: item for item in dataset["items"]}
                items.update({item["name"]: item for item in body["items"]})
                dataset["items"] = list(items.values())
                dataset["item_count"] = len(items)
                return httpx.Response(200, json=dataset)
            if path.endswith("/export"):
                records = [
                    {"name": item["name"], "text": item["text"], "iri": item.get("iri")}
                    for item in dataset["items"]
                ]
                expected = {
                    "about": dataset["about"],
                    "install": dataset["install"],
                    "records": [
                        {key: value for key, value in item.items() if key not in ("text", "iri")}
                        for item in dataset["items"]
                    ],
                }
                return httpx.Response(200, json={"records": records, "expected": expected})
            return httpx.Response(200, json=dataset)
        if (request.method, path) == ("DELETE", "/v1/evals/datasets"):
            if params["id"] not in self.datasets:
                return _not_found(f"dataset {params['id']}")
            self.deleted.add(params["id"])
            return httpx.Response(
                200, json={"id": params["id"], "object": "eval_dataset", "deleted": True}
            )
        raise AssertionError(f"the guide calls a route the fake does not have: {path}")


@pytest.fixture
def fake(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> FakeLbb:
    engine = FakeLbb()

    class Client(lbb.LbbClient):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            kwargs.setdefault("transport", httpx.MockTransport(engine.handle))
            kwargs.setdefault("retry_delay", 0)
            super().__init__(*args, **kwargs)

    monkeypatch.setattr(lbb, "LbbClient", Client)
    monkeypatch.setenv("LBB_URL", "http://lbb.test")
    monkeypatch.setenv("LBB_API_KEY", "lbb_test_key")
    # The export example writes records.json and expected.json.
    monkeypatch.chdir(tmp_path)
    return engine


def _run(page: Path) -> dict[str, Any]:
    blocks = re.findall(r"```python\n(.*?)```", page.read_text(), re.S)
    assert blocks, f"{page.name} has no Python example"
    namespace: dict[str, Any] = {"__name__": page.stem}
    for number, block in enumerate(blocks):
        exec(compile(block, f"{page.name} block {number}", "exec"), namespace)
    return namespace


def test_every_example_of_the_test_guide_runs(fake: FakeLbb) -> None:
    page = _guide("test-a-workflow.mdx")
    if page is None:
        pytest.skip("the docs site is not in this tree")
    namespace = _run(page)
    test = namespace["test"]
    # The answer reached the test, and the test names its one dataset.
    assert test.counts.answered == 1
    dataset, again = namespace["dataset"], namespace["again"]
    assert namespace["saved"].dataset == dataset.id == again.id
    assert len(fake.datasets) == 1
    assert ("POST", "/v1/workflows/named/tests") in fake.routes
    assert ("GET", "/v1/evals/datasets/export") in fake.routes


def test_every_example_of_the_datasets_guide_runs(fake: FakeLbb, tmp_path: Path) -> None:
    page = _guide("datasets.mdx")
    if page is None:
        pytest.skip("the docs site is not in this tree")
    namespace = _run(page)
    saved, dataset = namespace["saved"], namespace["dataset"]
    assert fake.tests[TEST_ID]["dataset"] == saved.id
    assert saved.item_count == 1
    # The added item joined the first; the export wrote both sample files.
    assert dataset.item_count == 2
    records = json.loads((tmp_path / "records.json").read_text())
    expected = json.loads((tmp_path / "expected.json").read_text())
    assert [record["name"] for record in records] == ["S 1", "S 2"]
    assert expected["install"]["units"] == {"width_cm": "cm", "depth_cm": "cm"}
    assert [item["name"] for item in expected["records"]] == ["S 1", "S 2"]
    # The delete took the dataset away: a read answers 404.
    assert namespace["deleted"].deleted is True
    with pytest.raises(lbb.LbbError) as raised:
        namespace["client"].evals.datasets.get(dataset.id)
    assert raised.value.status_code == 404
