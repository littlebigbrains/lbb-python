"""Every Python example of the page "Build a workflow"
(``docs-site/src/content/docs/start/first-workflow.mdx``) runs here, in
order, against a fake LBB with the server's rules for the routes the page
calls.

The fake declares what the ontology declares and refuses the rest: a commit
of an undeclared class or field, a test or a workflow that names a property
the graph lacks. A commit is published one read of the publication status
later, and a test or a workflow reads only published records. Its models are
the local stack's mocks: they answer the template's sample texts from the
answer key (``sdk/templates/values-extract/samples/expected.json``) with
the key's quotes, at a support of 0.92, or 0.6 for a quote with "about". A
value below the gate, or a number whose quote states no unit, goes to
review. A test writes nothing; the
workflow writes the values it reads. A workflow reads as
``GET /v1/workflows/named`` answers it, and its runs come in the list
envelope, newest first: the backfill's step (``more``), then the step that
wrote (``done``)."""

from __future__ import annotations

import json
import re
import time
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs

import httpx
import pytest

import lbb

GRAPH = {"tenant_id": "acme", "graph_id": "main", "branch_id": "main"}
TEXT_TYPES = {"text", "keyword"}
NUMBER_TYPES = {"i64", "f64"}
SUPPORT = 0.92
ABOUT_SUPPORT = 0.6
GATE = 0.8
UNIT = re.compile(r"\d\s*(mm|cm|m)\b")


def _ancestor_file(*parts: str) -> Path | None:
    for parent in Path(__file__).resolve().parents:
        path = parent.joinpath(*parts)
        if path.is_file():
            return path
    return None


def _error(status: int, code: str, message: str, param: str | None = None) -> httpx.Response:
    body: dict[str, Any] = {
        "type": "invalid_request_error",
        "code": code,
        "message": message,
        "request_id": "req_fake",
        "doc_url": None,
    }
    if param is not None:
        body["param"] = param
    return httpx.Response(status, json={"error": body})


class Refused(Exception):
    def __init__(self, response: httpx.Response) -> None:
        self.response = response


def _refuse(status: int, code: str, message: str, param: str | None = None) -> Refused:
    return Refused(_error(status, code, message, param))


class FakeLbb:
    """The graph, its ontology, its records, the tests, the datasets and the
    workflows of one graph, ``main``."""

    def __init__(self, answer_key: dict[str, Any]) -> None:
        self.key = {record["name"]: record for record in answer_key["records"]}
        self.created = False
        self.classes: set[str] = set()
        self.properties: dict[str, dict[str, Any]] = {}
        # (class, name) -> {"id", "attributes"}; published ones are readable.
        self.records: dict[tuple[str, str], dict[str, Any]] = {}
        self.head = 0
        self.published = 0
        self.keys: dict[str, Any] = {}
        self.tests: dict[str, dict[str, Any]] = {}
        self.test_installs: dict[str, dict[str, Any]] = {}
        self.datasets: dict[str, dict[str, Any]] = {}
        self.workflows: dict[str, dict[str, Any]] = {}
        self.workflow_bodies: list[dict[str, Any]] = []
        self.runs: dict[str, list[dict[str, Any]]] = {}
        self.routes: list[tuple[str, str]] = []

    # ---- the graph ---------------------------------------------------------

    def _commit(self) -> int:
        self.head += 1
        return self.head

    def _published_records(self, class_: str) -> list[dict[str, Any]]:
        rows = [
            record
            for (record_class, _), record in self.records.items()
            if record_class == class_ and record["seq"] <= self.published
        ]
        return sorted(rows, key=lambda record: record["iri"])

    def create_graph(self) -> dict[str, Any]:
        if self.created:
            raise _refuse(409, "conflict", "graph main already exists")
        self.created = True
        return {"graph": GRAPH, "commit_seq": 0, "ontology_version": 0}

    def evolve(self, body: dict[str, Any]) -> dict[str, Any]:
        if not self.created:
            raise _refuse(404, "not_found", "graph main does not exist")
        allowed = {
            "add_entity_type": {"op", "name"},
            "add_property": {"op", "name", "value_type", "required", "allowed_values", "unit"},
        }
        for op in body["ops"]:
            fields = allowed.get(op["op"])
            if fields is None or set(op) - fields:
                raise _refuse(400, "invalid_request", f"unknown op or field in {op}", "ops")
            if op["op"] == "add_entity_type":
                self.classes.add(op["name"])
                continue
            value_type = op.get("value_type", "text")
            if "unit" in op and value_type not in NUMBER_TYPES:
                raise _refuse(400, "invalid_request", "a unit is for i64 and f64", "ops")
            self.properties[op["name"]] = {"value_type": value_type, "unit": op.get("unit")}
        self._commit()
        return {
            "graph": GRAPH,
            "base_ontology_version": 0,
            "ontology_version": 1,
            "dry_run": False,
            "publishable": True,
            "no_op": False,
            "applied": [],
            "messages": [],
        }

    def commit(self, body: dict[str, Any]) -> dict[str, Any]:
        seq = self._commit()
        for item in body.get("entity_properties", []):
            if item["type"] not in self.classes:
                raise _refuse(400, "invalid_request", f"no entity type {item['type']}")
            unknown = set(item["properties"]) - set(self.properties)
            if unknown:
                raise _refuse(400, "invalid_request", f"no property named {sorted(unknown)}")
            key = (item["type"], item["name"])
            record = self.records.setdefault(
                key,
                {
                    "id": f"{len(self.records) + 1:032x}",
                    "type": item["type"],
                    "name": item["name"],
                    "attributes": {},
                },
            )
            record["iri"] = f"https://littlebigbrain.com/e/{record['id']}"
            record["attributes"].update(item["properties"])
            record["seq"] = seq
        written = len(body.get("entity_properties", []))
        return {
            "commit_seq": seq,
            "replayed": False,
            "entities_written": written,
            "edges_written": 0,
        }

    def publication_status(self) -> dict[str, Any]:
        # A read first sees the head publishing, the next one sees it current.
        state = "current" if self.published == self.head else "publishing"
        status = {
            "state": state,
            "epoch": 1,
            "head_seq": self.head,
            "target_seq": self.head,
            "published_seq": self.published,
            "lag_commits": self.head - self.published,
            "last_progress_at_micros": 1,
            "retry": {"eventual_read_available": True, "message": "", "retry_after_ms": 0},
        }
        self.published = self.head
        return status

    def entity(self, params: dict[str, str]) -> dict[str, Any]:
        record = self.records.get((params["type"], params["name"]))
        if record is None or record["seq"] > self.published:
            raise _refuse(404, "not_found", "no such record")
        return {
            "entity": {"id": record["id"], "type": record["type"], "name": record["name"]},
            "attributes": dict(record["attributes"]),
            "snapshot": {"served_at_seq": self.published},
            "metadata": {},
            "current_state": [],
            "outgoing": [],
            "incoming": [],
            "history": [],
            "observations": [],
        }

    # ---- values.extract ----------------------------------------------------

    def _resolve(self, install: dict[str, Any]) -> dict[str, Any]:
        """The settings of a ``values.extract`` create, as the server
        resolves them: the class, the fields and each property with its type
        and unit, all declared."""
        if install.get("starter") not in ("values.extract", "values.extract@v1"):
            raise _refuse(400, "workflow_param_invalid", "a test runs values.extract", "install")
        watch = install.get("watch") or {}
        if watch.get("class") not in self.classes:
            raise _refuse(400, "workflow_param_invalid", "the class is not declared", "watch")
        for field in watch.get("fields", []):
            if field not in self.properties:
                raise _refuse(400, "workflow_param_invalid", f"no field {field}", "watch")
        names = (install.get("params") or {}).get("properties") or []
        properties = []
        for name in names:
            if name not in self.properties:
                raise _refuse(400, "workflow_param_invalid", f"no property {name}", "params")
            declared = self.properties[name]
            properties.append({"name": name, **{k: v for k, v in declared.items() if v}})
        if not properties:
            raise _refuse(400, "workflow_param_invalid", "name the properties", "params")
        if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,62}", install.get("name", "")):
            raise _refuse(400, "workflow_param_invalid", "a name is lower case", "name")
        return {
            "class": f"https://littlebigbrain.com/class/{watch['class'].lower()}",
            "fields": [
                {"path": [f"https://littlebigbrain.com/p/{field}"], "name": field, "max": 40000}
                for field in watch.get("fields", [])
            ],
            "properties": properties,
            "gate": GATE,
        }

    def _class_records(self, settings: dict[str, Any]) -> list[dict[str, Any]]:
        """The published records of the settings' class, by IRI."""
        name = next(c for c in self.classes if settings["class"].endswith(f"/{c.lower()}"))
        return self._published_records(name)

    def _read(self, record: dict[str, Any], settings: dict[str, Any]) -> list[dict[str, Any]]:
        """What the mock models read from a record: the key's values of the
        properties the workflow fills, with the key's quotes. A value below
        the gate, or a number whose quote states no unit, goes to review."""
        names = {prop["name"] for prop in settings["properties"]}
        key = self.key.get(record["name"], {"values": []})
        read = []
        for value in key["values"]:
            if value["property"] not in names:
                continue
            support = ABOUT_SUPPORT if "about" in value["quote"] else SUPPORT
            number = self.properties[value["property"]]["value_type"] in NUMBER_TYPES
            unconfirmed = number and not UNIT.search(value["quote"])
            read.append(
                {
                    "property": value["property"],
                    "value": str(value["value"]),
                    "quote": value["quote"],
                    "support": support,
                    "outcome": "review" if support < GATE or unconfirmed else "written",
                }
            )
        return read

    def start_test(self, body: dict[str, Any]) -> dict[str, Any]:
        if ("name" in body) == ("install" in body):
            raise _refuse(400, "workflow_param_invalid", "name or install", "name")
        install = body["install"]
        settings = self._resolve(install)
        sample = body.get("sample", 6)
        if not 1 <= sample <= 20:
            raise _refuse(400, "workflow_param_invalid", "`sample` is 1 to 20", "sample")
        records = self._class_records(settings)[:sample]
        if not records:
            raise _refuse(
                400,
                "workflow_param_invalid",
                "the watched class has no published instances to test yet",
                "sample",
            )
        id = f"wtest_{len(self.tests) + 1:027d}"
        test = {
            "id": id,
            "object": "workflow_test",
            "created": "2026-10-21T10:00:00.000000Z",
            "workflow": {"starter": "values.extract", "name": install["name"]},
            "installed": False,
            "status": "queued",
            "fingerprint": "ab" * 16,
            "settings": settings,
            "uses": {"extract": "mock-generative", "decide": "mock-decision"},
            "inputs": [
                {"iri": record["iri"], "label": record["name"], "text": ""} for record in records
            ],
            "results": [],
            "counts": {
                "inputs_done": 0,
                "passed": 0,
                "review": 0,
                "no_value": 0,
                "failed": 0,
                "answered": 0,
            },
            "estimate_micro_usd": 1100 * len(records),
            "cost_micro_usd": 0,
            "dataset": None,
        }
        self.tests[id] = test
        self.test_installs[id] = install
        return test

    def read_test(self, id: str) -> dict[str, Any]:
        test = self.tests.get(id)
        if test is None:
            raise _refuse(404, "not_found", f"no test {id}")
        # Each read moves an open test on: queued, running, done.
        if test["status"] == "queued":
            test["status"] = "running"
        elif test["status"] == "running":
            self._run_test(test)
        return test

    def _run_test(self, test: dict[str, Any]) -> None:
        settings = test["settings"]
        for input in test["inputs"]:
            record = next(r for r in self.records.values() if r["iri"] == input["iri"])
            input["text"] = record["attributes"].get("text", "")
            values = self._read(record, settings)
            test["results"].append({"iri": input["iri"], "values": values, "failed": False})
            counts = test["counts"]
            counts["inputs_done"] += 1
            counts["passed"] += sum(value["outcome"] == "written" for value in values)
            counts["review"] += sum(value["outcome"] == "review" for value in values)
            counts["no_value"] += len(settings["properties"]) - len(values)
        test["cost_micro_usd"] = test["estimate_micro_usd"]
        test["status"] = "done"
        test["finished"] = "2026-10-21T10:00:05.000000Z"

    def answer(self, body: dict[str, Any]) -> dict[str, Any]:
        test = self.tests[body["id"]]
        results = [result for result in test["results"] if result["iri"] == body["iri"]]
        if not results:
            raise _refuse(400, "workflow_param_invalid", "no read input with that iri", "iri")
        names = {prop["name"] for prop in test["settings"]["properties"]}
        if body["property"] not in names:
            raise _refuse(400, "workflow_param_invalid", "not a property of the test", "property")
        answer = body["answer"]
        if answer is not None:
            decision = answer.get("decision")
            if decision not in ("accept", "reject", "correct", "cant_tell"):
                raise _refuse(400, "invalid_request", "a value item takes that decision", "answer")
            if decision == "correct" and "value" not in answer:
                raise _refuse(400, "invalid_request", "correct needs a value", "answer")
        answers = results[0].setdefault("answers", {})
        if answer is None:
            answers.pop(body["property"], None)
        else:
            answers[body["property"]] = answer
        test["counts"]["answered"] = sum(len(r.get("answers") or {}) for r in test["results"])
        return test

    def save_dataset(self, body: dict[str, Any]) -> dict[str, Any]:
        source = body["source"]
        test = self.tests[source["id"]]
        labels = {input["iri"]: input for input in test["inputs"]}
        items = []
        for result in test["results"]:
            answers = result.get("answers") or {}
            if not answers:
                continue
            read = {value["property"]: value["value"] for value in result["values"]}
            values, leave_out = [], []
            for property, answer in answers.items():
                if answer["decision"] == "accept" and property in read:
                    values.append({"property": property, "value": read[property]})
                elif answer["decision"] == "correct":
                    values.append({"property": property, "value": answer["value"]})
                    if property in read:
                        leave_out.append({"property": property, "value": read[property]})
                elif answer["decision"] in ("reject", "accept"):
                    leave_out.append({"property": property, "value": None})
            input = labels[result["iri"]]
            items.append(
                {
                    "name": input["label"],
                    "iri": result["iri"],
                    "text": input["text"],
                    "values": values,
                    "leave_out": leave_out,
                }
            )
        id = test["dataset"] or f"ds_{len(self.datasets) + 1:027d}"
        dataset = {
            "id": id,
            "object": "eval_dataset",
            "created": "2026-10-21T10:01:00.000000Z",
            "name": body["name"],
            "kind": body.get("kind", "values"),
            "install": {
                "properties": {p["name"]: p["value_type"] for p in test["settings"]["properties"]},
                "units": {
                    p["name"]: p["unit"] for p in test["settings"]["properties"] if p.get("unit")
                },
            },
            "about": [],
            "item_count": len(items),
            "items": items,
        }
        self.datasets[id] = dataset
        test["dataset"] = id
        return dataset

    def put_workflow(self, body: dict[str, Any]) -> dict[str, Any]:
        settings = self._resolve(body)
        name = body["name"]
        self.workflow_bodies.append(body)
        workflow = self.workflows.setdefault(
            name,
            {
                "name": name,
                "params": body.get("params"),
                "status": {"state": "backfilling", "counters": {}, "pending": 0},
                "queued": [],
            },
        )
        workflow["settings"] = settings
        self.runs.setdefault(name, [])
        # The answer of a create is the workflow in the triggered shape.
        return {
            "starter": "values.extract",
            "version": "v1",
            "name": name,
            "watch": {"class": settings["class"], "fields": settings["fields"]},
            "params": workflow["params"],
            "status": workflow["status"],
        }

    def _named(self, workflow: dict[str, Any]) -> dict[str, Any]:
        """A workflow as ``GET /v1/workflows/named`` answers it."""
        settings = workflow["settings"]
        return {
            "object": "workflow",
            "name": workflow["name"],
            "starter": "values.extract",
            "workflow": {"workflow_type": "lbb.values.extract", "version": "v1"},
            "runs_in": "lbb",
            "shards": 1,
            "paused": False,
            "triggers": [
                {
                    "id": "watch",
                    "kind": "watch",
                    "watch": {"class": settings["class"], "fields": settings["fields"]},
                }
            ],
            "params": workflow["params"],
            "status": workflow["status"],
        }

    def read_workflow(self, name: str) -> dict[str, Any]:
        workflow = self.workflows.get(name)
        if workflow is None:
            raise _refuse(404, "named_workflow_not_found", f"no workflow {name}")
        # Each read moves the workflow on: the backfill queues the records,
        # then one step reads them and writes the values it reads.
        status = workflow["status"]
        if status["state"] == "backfilling":
            records = self._class_records(workflow["settings"])
            workflow["queued"] = [record["iri"] for record in records]
            status.update(state="ready", pending=len(records))
            self._record_run(workflow, "more", queue=(0, len(records)), units=0)
        elif workflow["queued"]:
            self._run_step(workflow)
        return self._named(workflow)

    def _record_run(
        self, workflow: dict[str, Any], outcome: str, *, queue: tuple[int, int], units: int
    ) -> None:
        """A run summary of an LBB step, newest first, as the server keeps it."""
        runs = self.runs[workflow["name"]]
        at = 1_792_000_000_000 + 1_000 * len(runs)
        runs.insert(
            0,
            {
                "v": 1,
                "id": f"run_{len(runs) + 1:04d}",
                "workflow": workflow["name"],
                "identity": f"starter.{workflow['name']}",
                "workflow_type": "lbb.values.extract",
                "version": "v1",
                "started_at_ms": at,
                "ended_at_ms": at + 400,
                "outcome": outcome,
                "attempts": 1,
                "queue_before": queue[0],
                "queue_after": queue[1],
                "units": units,
                "unit": "instance",
            },
        )

    def _run_step(self, workflow: dict[str, Any]) -> None:
        settings = workflow["settings"]
        counters = workflow["status"]["counters"]
        queued, workflow["queued"] = workflow["queued"], []
        for iri in queued:
            record = next(r for r in self.records.values() if r["iri"] == iri)
            values = self._read(record, settings)
            for value in values:
                if value["outcome"] != "written":
                    continue
                declared = self.properties[value["property"]]
                number = declared["value_type"] in NUMBER_TYPES
                record["attributes"][value["property"]] = (
                    float(value["value"]) if number else value["value"]
                )
            record["seq"] = self._commit()
            for name, count in (
                ("instances", 1),
                ("values_proposed", len(values)),
                ("written", sum(value["outcome"] == "written" for value in values)),
                ("review", sum(value["outcome"] == "review" for value in values)),
                ("commits", 1),
            ):
                counters[name] = counters.get(name, 0) + count
        workflow["status"]["pending"] = 0
        self._record_run(workflow, "done", queue=(len(queued), 0), units=len(queued))

    def list_runs(self, params: dict[str, str]) -> dict[str, Any]:
        runs = self.runs.get(params["name"])
        if runs is None:
            raise _refuse(404, "named_workflow_not_found", f"no workflow {params['name']}")
        assert "before" not in params, "the client pages with cursor"
        limit = int(params.get("limit", 50))
        start = int(params.get("cursor", 0))
        page = runs[start : start + limit]
        more = start + limit < len(runs)
        return {
            "object": "list",
            "data": page,
            "has_more": more,
            "next_cursor": str(start + limit) if more else None,
        }

    # ---- the routes ----------------------------------------------------------

    def handle(self, request: httpx.Request) -> httpx.Response:
        try:
            return httpx.Response(200, json=self._route(request))
        except Refused as refused:
            return refused.response

    def _route(self, request: httpx.Request) -> Any:
        path = request.url.path
        params = {key: values[0] for key, values in parse_qs(request.url.query.decode()).items()}
        body: Any = json.loads(request.content) if request.content else None
        route = (request.method, path)
        self.routes.append(route)
        assert params.get("graph") == "main", f"{path} is sent without graph=main"
        key = request.headers.get("idempotency-key")
        if route == ("POST", "/v1/graph/commit"):
            if not key:
                raise _refuse(400, "missing_idempotency_key", "missing Idempotency-Key header")
            if key in self.keys:
                return {**self.keys[key], "replayed": True}
            self.keys[key] = self.commit(body)
            return self.keys[key]
        if route == ("POST", "/v1/graph/create"):
            return self.create_graph()
        if route == ("POST", "/v1/ontology/evolve"):
            return self.evolve(body)
        if route == ("GET", "/v1/graph/publication-status"):
            return self.publication_status()
        if route == ("GET", "/v1/graph/entity"):
            return self.entity(params)
        if route == ("POST", "/v1/workflows/named/tests"):
            assert key, "a test is started with an idempotency key"
            return self.start_test(body)
        if route == ("GET", "/v1/workflows/named/tests/get"):
            return self.read_test(params["id"])
        if route == ("POST", "/v1/workflows/named/tests/answer"):
            assert key, "an answer is sent with an idempotency key"
            return self.answer(body)
        if route == ("POST", "/v1/evals/datasets"):
            assert key, "a dataset is saved with an idempotency key"
            return self.save_dataset(body)
        if route == ("PUT", "/v1/workflows/triggered"):
            return self.put_workflow(body)
        if route == ("GET", "/v1/workflows/named"):
            return self.read_workflow(params["name"])
        if route == ("GET", "/v1/workflows/named/runs"):
            return self.list_runs(params)
        raise AssertionError(f"the page calls a route the fake does not have: {route}")


@pytest.fixture
def page() -> Path:
    path = _ancestor_file("docs-site", "src", "content", "docs", "start", "first-workflow.mdx")
    if path is None:
        pytest.skip("the docs site is not in this tree")
    return path


@pytest.fixture
def fake(monkeypatch: pytest.MonkeyPatch) -> FakeLbb:
    key_path = _ancestor_file("sdk", "templates", "values-extract", "samples", "expected.json")
    if key_path is None:
        pytest.skip("the values-extract samples are not in this tree")
    engine = FakeLbb(json.loads(key_path.read_text()))

    class Client(lbb.LbbClient):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            kwargs.setdefault("transport", httpx.MockTransport(engine.handle))
            kwargs.setdefault("retry_delay", 0)
            super().__init__(*args, **kwargs)

    monkeypatch.setattr(lbb, "LbbClient", Client)
    monkeypatch.setattr(time, "sleep", lambda seconds: None)
    monkeypatch.setenv("LBB_URL", "http://lbb.test")
    monkeypatch.setenv("LBB_API_KEY", "lbb_test_key")
    return engine


def _run(page: Path) -> dict[str, Any]:
    blocks = re.findall(r"```python\n(.*?)```", page.read_text(), re.S)
    assert blocks, f"{page.name} has no Python example"
    namespace: dict[str, Any] = {"__name__": page.stem}
    for number, block in enumerate(blocks, start=1):
        exec(compile(block, f"{page.name} block {number}", "exec"), namespace)
    return namespace


def _records(path: Path) -> dict[str, str]:
    records = json.loads((path.parents[0] / "records.json").read_text())
    return {record["name"]: record["text"] for record in records}


def test_every_example_of_the_page_runs(page: Path, fake: FakeLbb) -> None:
    namespace = _run(page)

    # The records are the template's samples, word for word: the mocks know
    # these texts only.
    key_path = _ancestor_file("sdk", "templates", "values-extract", "samples", "expected.json")
    assert key_path is not None
    samples = _records(key_path)
    for name in ("T-160", "R-3"):
        assert fake.records[("Product", name)]["attributes"]["text"] == samples[name]

    # The test read both records and wrote nothing. The width of R-3 states
    # no unit, so it goes to review.
    test = namespace["test"]
    assert test.status.value == "done"
    assert sorted(each.label for each in test.inputs) == ["R-3", "T-160"]
    values = {
        (namespace["labels"][result.iri], value.property): value
        for result in test.results or []
        for value in result.values or []
    }
    assert values[("T-160", "width_cm")].value == "160"
    assert values[("T-160", "depth_cm")].quote == "depth 800 mm"
    assert ("T-160", "color") not in values
    assert ("R-3", "depth_cm") not in values
    assert values[("R-3", "width_cm")].outcome == "review"
    assert test.counts.passed == 5 and test.counts.review == 1 and test.counts.no_value == 2

    # The correction reached the test, and the dataset holds R-3 with it.
    r3 = namespace["r3"]
    (answered,) = [result for result in fake.tests[test.id]["results"] if result["iri"] == r3.iri]
    assert answered["answers"] == {"material": {"decision": "correct", "value": "sheet steel"}}
    dataset = namespace["dataset"]
    assert dataset.item_count == 1
    (item,) = fake.datasets[dataset.id]["items"]
    assert item["name"] == "R-3"
    assert item["values"] == [{"property": "material", "value": "sheet steel"}]
    assert item["leave_out"] == [{"property": "material", "value": "Stahlblech"}]
    assert fake.tests[test.id]["dataset"] == dataset.id

    # The workflow was created with the body the test installed.
    assert fake.workflow_bodies == [fake.test_installs[test.id]]
    counters = namespace["workflow"].status.counters
    assert (counters["instances"], counters["written"], counters["review"]) == (2, 5, 1)
    # The runs, newest first: the step that wrote, then the backfill's.
    runs = namespace["runs"]
    assert [run.outcome.value for run in runs.data] == ["done", "more"]
    assert runs.data[0].units == 2 and runs.has_more is False

    # It wrote the values the model read: the answers stayed in the test,
    # and the width of R-3 waits for a person.
    assert namespace["record"]["attributes"] == {
        "text": samples["R-3"],
        "material": "Stahlblech",
        "color": "lichtgrau",
    }
    assert ("GET", "/v1/graph/publication-status") in fake.routes


def test_a_test_before_the_records_are_published_is_refused(fake: FakeLbb) -> None:
    client = lbb.LbbClient("http://lbb.test", api_key="lbb_test_key", graph="main")
    client.create_graph()
    client.ontology.evolve(
        {
            "ops": [
                {"op": "add_entity_type", "name": "Product"},
                {"op": "add_property", "name": "text", "value_type": "text"},
                {"op": "add_property", "name": "width_cm", "value_type": "f64", "unit": "cm"},
            ]
        }
    )
    client.commit(
        {
            "entity_properties": [
                {"type": "Product", "name": "D-120", "properties": {"text": "120 cm"}}
            ]
        },
        idempotency_key="records-1",
    )
    install = {
        "starter": "values.extract",
        "name": "product",
        "watch": {"class": "Product", "fields": ["text"]},
        "params": {"properties": ["width_cm"]},
    }
    with pytest.raises(lbb.LbbError) as raised:
        client.workflows.tests.create(install=install, sample=2)
    assert raised.value.status_code == 400
    # A property the graph lacks is refused too.
    client.wait_for_published(1)
    install["params"] = {"properties": ["height_cm"]}
    with pytest.raises(lbb.LbbError) as raised:
        client.workflows.tests.create(install=install, sample=2)
    assert raised.value.param == "params"
