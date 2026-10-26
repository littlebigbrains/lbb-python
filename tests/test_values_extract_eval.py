"""The eval of the Python values-extract port against LBB's values.extract,
as ``sdk/templates/_evals/values-extract.test.ts`` runs the TypeScript
template (``docs/architecture/workflows-dogfooding.md`` 2.6.8).

The port runs with the real worker on ``values-extract.json``, with models
that give the recorded answers, as lbb-server's ``template_evals`` test runs
the Rust path; that test wrote ``values-extract.rust.json``. The port must
make the same model calls (prompts, schema, questions) and the same outputs:
values, sources, review items and counters. Then both are graded against the
samples' answer key, and the bar of 2.6.8 holds. One rule differs by design:
LBB's values.extract converts a number the text states in another unit, and
the template sends it to review. The width and depth of the sample ``T-160``
and the width of ``S 9.9`` are such numbers; the grades leave them out for
both paths.
"""

from __future__ import annotations

import json
import re
import struct
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from lbb import entity_id_from_iri
from lbb.entities import ENTITY_ID_KEY_SENTINEL

# The fake puts sdk/python/examples on the path of the example's modules.
from values_extract_fake import EXAMPLES, FakeLbb, message, serve  # isort: skip
from value_grading import grade  # isort: skip
from values_extract.workflow import Settings  # isort: skip


def _evals() -> Path | None:
    for parent in Path(__file__).resolve().parents:
        folder = parent / "templates" / "_evals"
        if (folder / "values-extract.rust.json").is_file():
            return folder
    return None


EVALS = _evals()
pytestmark = pytest.mark.skipif(EVALS is None, reason="the template evals are not in this tree")
SAMPLES = EXAMPLES / "values_extract" / "samples"
# The values a text states in another unit of the property's kind: LBB
# converts them to the property's unit and writes them, the port sends them to
# review. ``shown`` is the value as the text states it, ``written`` the value
# LBB writes.
CONVERTED = [
    {"instance": "T-160", "property": "width_cm", "shown": "1.6 m", "written": 160},
    {"instance": "T-160", "property": "depth_cm", "shown": "800 mm", "written": 80},
    {"instance": "S 9.9", "property": "width_cm", "shown": "600 mm", "written": 60},
]


def _converted(instance: Any, prop: Any) -> bool:
    return any(c["instance"] == instance and c["property"] == prop for c in CONVERTED)


def _comparable(outputs: dict[str, Any]) -> dict[str, Any]:
    """The outputs without the converted values and their sources."""
    return {
        name: {
            "values": {k: v for k, v in o["values"].items() if not _converted(name, k)},
            "sources": [s for s in o["sources"] if not _converted(name, s["property"])],
        }
        for name, o in outputs.items()
    }


def _besides(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """The review items without those of the converted values."""
    return [i for i in items if not _converted(i["instance"], i["question"]["property"])]


def _load() -> tuple[dict[str, Any], dict[str, Any]]:
    assert EVALS is not None
    fixture = json.loads((EVALS / "values-extract.json").read_text(encoding="utf-8"))
    rust = json.loads((EVALS / "values-extract.rust.json").read_text(encoding="utf-8"))
    return fixture, rust


def _f32(value: float) -> float:
    return float(struct.unpack("f", struct.pack("f", value))[0])


def _choice(p: float) -> dict[str, Any]:
    """The decision model of the fixture: `supported` at the recorded
    probability, in f32, as the Rust test's models answer."""
    f = _f32(p)
    rest = _f32((1 - f) / 2)
    return {
        "choice": "supported" if f >= 0.5 else "not_supported",
        "confidence": max(f, 1 - f),
        "probabilities": {"supported": f, "weak": rest, "not_supported": rest},
    }


def _round(value: float, places: int = 4) -> float:
    return round(value, places)


def _run() -> dict[str, Any]:
    fixture, rust = _load()
    texts = {r["name"]: r["text"] for r in json.loads((SAMPLES / "records.json").read_text())}
    records = [
        {**r, "text": texts[r["name"]]} if r.get("sample") else r for r in fixture["records"]
    ]
    iris: dict[str, str] = rust["iris"]
    calls: list[dict[str, Any]] = []
    reasons: list[dict[str, Any]] = []

    def rows(query: str) -> list[dict[str, str]]:
        out = []
        for r in records:
            if f"<{iris[r['name']]}>" not in query:
                continue
            row = {"s": iris[r["name"]], "class": fixture["install"]["class"], "label": r["name"]}
            if r.get("text") is not None:
                row["text"] = r["text"]
            row.update({k: str(v) for k, v in (r.get("box") or {}).items()})
            out.append(row)
        return out

    def reason(body: dict[str, Any]) -> dict[str, Any] | None:
        prompt = json.loads(body["prompt"])
        reasons.append(
            {"system": body["system"], "schema": body["schema"], "max_tokens": body["max_tokens"]}
        )
        # A refused call is still a call the model was asked.
        calls.append({"kind": "reason", "instance": prompt["instance"], "prompt": prompt})
        answer = fixture["answers"][prompt["instance"]]
        if answer.get("failure"):
            return None
        return {"values": answer["values"]}

    def decide(body: dict[str, Any]) -> dict[str, Any]:
        state = body["state"]
        calls.append(
            {
                "kind": "decide",
                "instance": state["record"],
                "state": state,
                "questions": body["questions"],
            }
        )
        support = fixture["answers"].get(state["record"], {}).get("support", {})
        answers = {}
        for qid, question in body["questions"].items():
            found = re.search(r"\nProperty: (\S+) ", question["instructions"])
            assert found
            if found[1] in support:
                answers[qid] = _choice(support[found[1]])
        return answers

    install = fixture["install"]
    settings = Settings(
        class_name=install["class"],
        text_field=install["text_field"],
        properties=install["properties"],
        context=install["context"],
        gate=install["gate"],
        units=install.get("units") or {},
    )
    fake = FakeLbb(rows, reason=reason, decide=decide)
    fake.queue(message([iris[r["name"]] for r in records], workflow="eval"))
    serve(fake, settings)
    assert len(fake.completed) == 1, "one turn holds the fixture's records"
    calls.sort(key=lambda c: f"{c['instance']}\u0000{c['kind']}")
    return {
        "fake": fake,
        "calls": calls,
        "reason": reasons[0],
        "state": fake.completed[0]["state"],
        "outputs": _outputs(fake, records, iris),
        "review": _review(fake, iris),
    }


def _outputs(
    fake: FakeLbb, records: list[dict[str, Any]], iris: dict[str, str]
) -> dict[str, Any]:
    """The values and sources the run wrote on each record, read as the
    TypeScript eval reads its graph."""
    (commit,) = fake.commits.values()
    outputs = {}
    for r in records:
        key = ENTITY_ID_KEY_SENTINEL + entity_id_from_iri(iris[r["name"]])
        values: dict[str, Any] = {}
        for row in fake.entity_rows(key):
            values.update(
                {k: _round(v) if isinstance(v, (int, float)) and not isinstance(v, bool) else v
                 for k, v in row["properties"].items()}
            )
        sources = []
        for edge in commit.get("triplets", []):
            if edge["source"].get("key") != key or edge["relation"] != "HAS_VALUE_SOURCE":
                continue
            (target,) = fake.entity_rows(edge["target"]["key"])
            region = edge["evidence"].get("region")
            sources.append(
                {
                    "property": str(target["properties"]["property"]),
                    "value": str(target["properties"]["value"]),
                    "support": _round(float(target["properties"]["support"])),
                    "name": target["name"],
                    "quote": edge["evidence"]["text"],
                    "source_id": edge["evidence"]["source_id"],
                    "region": (
                        {k: _round(region[k], 3) for k in ("page", "x0", "y0", "x1", "y1")}
                        if region
                        else None
                    ),
                }
            )
        sources.sort(key=lambda s: s["property"])
        outputs[r["name"]] = {"values": values, "sources": sources}
    return outputs


def _review(fake: FakeLbb, iris: dict[str, str]) -> list[dict[str, Any]]:
    """The value items the port filed, in the shape of the Rust record."""
    name_of = {iri: name for name, iri in iris.items()}

    def props(row: dict[str, Any]) -> dict[str, Any]:
        return {k: _round(v) if k == "support" else v for k, v in row["properties"].items()}

    items = []
    for key, request in fake.questions.items():
        facts = request["facts"]
        question = request["question"]
        confidence = question.get("confidence")
        items.append(
            {
                "key": key,
                "kind": request["kind"],
                "status": fake.item_of(key, request)["status"],
                "title": request["title"],
                "rationale": request["rationale"],
                "origin": request["origin"],
                "summary": facts["summary"],
                "instance": name_of[request["subject"]["iri"]],
                "question": {
                    "property": question["property"],
                    "value": question["value"],
                    "unit": question.get("unit"),
                    "confidence": None if confidence is None else _round(confidence),
                },
                "on_instance": props(facts["entity_properties"][0]),
                "on_source": props(facts["entity_properties"][1]),
                "quote": request["evidence"][0]["quote"],
                "evidence_hash": request["evidence_hash"],
            }
        )
    return sorted(items, key=lambda i: i["key"])


def _iso(value: Any) -> str:
    """A date as JavaScript's ``toISOString`` writes it."""
    when = datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(timezone.utc)
    return when.strftime("%Y-%m-%dT%H:%M:%S.") + f"{when.microsecond // 1000:03d}Z"


def _normal(outputs: dict[str, Any], properties: dict[str, str]) -> dict[str, Any]:
    def value(v: Any, kind: str | None) -> Any:
        if kind in ("date_time", "datetime"):
            return _iso(v)
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            return _round(v)
        return v

    return {
        name: {
            "values": {k: value(v, properties.get(k)) for k, v in o["values"].items()},
            "sources": [
                {
                    **s,
                    "value": _iso(s["value"])
                    if properties.get(s["property"]) == "date_time"
                    else s["value"],
                }
                for s in o["sources"]
            ],
        }
        for name, o in outputs.items()
    }


def test_the_port_makes_the_model_calls_of_lbbs_values_extract() -> None:
    _, rust = _load()
    run = _run()
    assert run["reason"] == rust["reason"]

    def question(calls: list[dict[str, Any]], instance: str, prop: str) -> str:
        decision = next(c for c in calls if c["instance"] == instance and c["kind"] == "decide")
        return next(
            q["instructions"]
            for q in decision["questions"].values()
            if f"\nProperty: {prop} " in q["instructions"]
        )

    # The question about a converted number shows the conversion in LBB's
    # version only: those questions differ, and only those.
    for c in CONVERTED:
        shown, written = c["shown"], c["written"]
        assert f"\nValue: {shown} (written as {written} cm)\n" in question(
            rust["calls"], c["instance"], c["property"]
        )
        number = shown.split(" ")[0]
        assert f"\nValue: {number}\n" in question(run["calls"], c["instance"], c["property"])

    def converted_question(instance: str, instructions: str) -> bool:
        return any(
            c["instance"] == instance and f"\nProperty: {c['property']} " in instructions
            for c in CONVERTED
        )

    def same(calls: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Every call, each decision call without its questions about converted
        numbers: the other questions of those records compare too."""
        return [
            {
                **c,
                "questions": {
                    qid: q
                    for qid, q in c["questions"].items()
                    if not converted_question(c["instance"], q["instructions"])
                },
            }
            if c["kind"] == "decide"
            else c
            for c in calls
        ]

    assert same(run["calls"]) == same(rust["calls"])
    counts = {
        "reason": sum(c["kind"] == "reason" for c in run["calls"]),
        "decide": sum(c["kind"] == "decide" for c in run["calls"]),
    }
    assert counts == rust["call_counts"]


def test_the_port_writes_the_values_sources_and_review_items_of_lbbs_values_extract() -> None:
    fixture, rust = _load()
    run = _run()
    properties = fixture["install"]["properties"]
    outputs = _normal(run["outputs"], properties)
    rust_outputs = _normal(rust["outputs"], properties)
    assert _comparable(outputs) == _comparable(rust_outputs)
    assert _besides(run["review"]) == _besides(rust["review"])
    # A converted value: LBB writes it in the property's unit; the port does
    # not write it and files it for review, with LBB's reason.
    for c in CONVERTED:
        name, prop = c["instance"], c["property"]
        assert rust_outputs[name]["values"][prop] == c["written"], c
        assert prop not in outputs[name]["values"], c
        (item,) = [
            i for i in run["review"] if i["instance"] == name and i["question"]["property"] == prop
        ]
        unit = c["shown"].split(" ")[1]
        assert item["rationale"].startswith(
            f"The unit is not confirmed: the text states {unit}; the property holds cm."
        )
    assert [i for i in rust["review"] if _converted(i["instance"], i["question"]["property"])] == []
    # The counters, but the commits (LBB commits per instance, the port per
    # turn) and the converted values, which LBB writes and the port reviews.
    counters = {
        k: v for k, v in run["state"].items() if v != 0 and k not in ("commits", "removed")
    }
    expected = {k: v for k, v in rust["counters"].items() if k != "commits"}
    expected["written"] -= len(CONVERTED)
    expected["review"] = expected.get("review", 0) + len(CONVERTED)
    assert counters == expected
    # The refused instance goes back to the watch; LBB's counts it failed.
    name_of = {iri: name for name, iri in rust["iris"].items()}
    assert [name_of[iri] for iri in run["fake"].requeued] == ["S 9.3"]


def test_the_grades_of_both_paths_on_the_samples_meet_the_bar() -> None:
    fixture, rust = _load()
    run = _run()
    full_key = json.loads((SAMPLES / "expected.json").read_text())["records"]
    prices = fixture["prices_micro_usd"]
    samples = [r["name"] for r in fixture["records"] if r.get("sample")]
    proposed = sum(len(fixture["answers"].get(n, {}).get("values") or []) for n in samples)
    # The key's values the text states in another unit are the converted ones,
    # and LBB writes each as the key states it, in the property's unit.
    stated = [(r["name"], v["property"]) for r in full_key for v in r["values"] if v.get("stated")]
    assert stated == [(c["instance"], c["property"]) for c in CONVERTED if c["instance"] in samples]
    for r in full_key:
        for v in r["values"]:
            if v.get("stated"):
                assert rust["outputs"][r["name"]]["values"][v["property"]] == v["value"]
    # The grades leave the converted values out for both paths: the rule that
    # differs by design.
    key = [
        {**r, "values": [v for v in r["values"] if not _converted(r["name"], v["property"])]}
        for r in full_key
    ]

    def grade_of(
        outputs: dict[str, Any], calls: list[dict[str, Any]], items: list[dict[str, Any]]
    ) -> dict[str, Any]:
        mine = [c for c in calls if c["instance"] in samples]
        written = _comparable(outputs)
        return grade(
            key,
            {n: written[n]["values"] for n in samples},
            sum(i["instance"] in samples for i in _besides(items)),
            proposed,
            sum(prices[c["kind"]] for c in mine),
            len(samples),
        )

    rust_grade = grade_of(rust["outputs"], rust["calls"], rust["review"])
    port = grade_of(run["outputs"], run["calls"], run["review"])
    assert port == rust_grade
    assert port["precision"] >= rust_grade["precision"]
    assert port["recall"] >= rust_grade["recall"] - 0.02
    assert port["review_share"] <= rust_grade["review_share"] + 0.05
    assert port["cost_per_instance_micro_usd"] <= rust_grade["cost_per_instance_micro_usd"] * 1.2
    # The grades the record holds (docs/testing/retrieval-quality-ledger.md).
    assert rust_grade == {
        "precision": 1,
        "recall": 0.8333,
        "review_share": 0.1667,
        "cost_per_instance_micro_usd": 1100,
        "written": 15,
        "correct": 15,
        "expected": 18,
    }
