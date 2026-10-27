"""The Python values-extract example (``sdk/python/examples/values_extract``)
on its samples, with the real ``WorkflowWorker`` and ``lbb_activities`` against a fake
LBB: the workflow engine, the model activities with the mock rules of the
TypeScript template's test, SPARQL over the sample records, commits with
their idempotency keys, review items and requeue.

The mock reasoning model reads the first size (``120 x 80 cm``, else
``width 1.6 m`` and ``depth 800 mm``, or ``Breite 43``) with the unit the text
states, the first material and the color of each text. The mock decision
model says a quote from a sentence about an accessory (``Accessory:``) or a
variant under a condition (``For two screens:``) does not support its value,
and gives a quote that says ``about`` 0.6, below the gate. The expected values
come from ``samples/expected.json``.
"""

from __future__ import annotations

import json
import re
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from lbb import entity_id_from_iri, stable_hash_hex
from lbb.workflows import entity_ref

# The fake puts sdk/python/examples on the path of the example's modules.
from values_extract_fake import EXAMPLES, FakeLbb, Killed, iri_of, message, serve  # isort: skip
from values_extract.workflow import (  # isort: skip
    DEFAULTS,
    REPAIR_NOTE,
    Settings,
    check,
    coerce,
    failed_checks,
    merge_attempts,
    ontology,
    quote_holds,
    legacy_value_key,
    trigger,
    unit_problem,
    value_item_key,
)

SAMPLES = EXAMPLES / "values_extract" / "samples"
RECORDS: list[dict[str, str]] = json.loads((SAMPLES / "records.json").read_text())
EXPECTED: dict[str, Any] = json.loads((SAMPLES / "expected.json").read_text())
MATERIALS = [
    "beech veneer",
    "oak veneer",
    "aluminium",
    "brushed stainless steel",
    "stainless steel",
    "bamboo",
    "Stahlblech",
    "plastic",
    "steel",
]


def sized(words: str) -> re.Pattern[str]:
    """A size with the unit the text states it in, if any: ``width 1.6 m``, ``Breite 43``."""
    return re.compile(rf"(?:{words}) (?:about )?(\d+(?:\.\d+)?)(?: (mm|cm|m)\b)?", re.I)


def read_values(text: str) -> list[dict[str, str]]:
    """The mock reasoning model: the first size, material and color of a text."""
    values: list[dict[str, str]] = []
    size = re.search(r"(\d+) x (\d+) cm", text)
    if size:
        values += [
            {"property": "width_cm", "value": size[1], "unit": "cm", "quote": size[0]},
            {"property": "depth_cm", "value": size[2], "unit": "cm", "quote": size[0]},
        ]
    else:
        for prop, words in (("width_cm", "width|Breite"), ("depth_cm", "depth|Tiefe")):
            if found := sized(words).search(text):
                values.append(
                    {"property": prop, "value": found[1], "unit": found[2] or "", "quote": found[0]}
                )
    found = [
        (m.start(), name)
        for name in MATERIALS
        if (m := re.search(rf"(?<![^\W\d_]){re.escape(name)}(?![^\W\d_])", text))
    ]
    if found:
        material = min(found)[1]
        values.append({"property": "material", "value": material, "unit": "", "quote": material})
    color = re.search(r"(?:Color|Farbe): ([^\W\d_]+)", text)
    if color:
        values.append({"property": "color", "value": color[1], "unit": "", "quote": color[0]})
    return values


def support(text: str, instructions: str, options: list[str]) -> dict[str, Any]:
    """The mock decision model: the support of each quote, from the quote and the sentence
    that holds it."""
    found = re.search(r'Quote: "(.*)"$', instructions, re.S)
    assert found
    quote = found[1]
    sentence = next((s for s in re.split(r"(?<=\.)\s+", text) if quote in s), "")
    if re.match(r"(?:Accessory|For [^:]+):", sentence):
        p = 0.05
    else:
        p = 0.6 if re.search(r"\babout\b", quote, re.I) else 0.92
    return {
        "choice": "supported" if p >= 0.5 else "not_supported",
        "confidence": max(p, 1 - p),
        "probabilities": {o: p if o == "supported" else (1 - p) / 2 for o in options},
    }


def fake_for(
    records: list[dict[str, str]],
    *,
    proposals: Callable[[str], list[dict[str, str]]] | None = None,
    asked: Callable[[dict[str, Any]], list[dict[str, str]]] | None = None,
    review_status: Callable[[], str] = lambda: "open",
    review_answer: Callable[[dict[str, Any]], dict[str, Any] | None] = lambda request: None,
    review_refused: Callable[[dict[str, Any]], str | None] = lambda request: None,
) -> FakeLbb:
    """A fake LBB with the mock models of the template's test, over ``records``."""

    def rows(query: str) -> list[dict[str, str]]:
        return [
            {"s": iri_of(r["name"]), "label": r["name"], **({"text": r["text"]} if "text" in r else {})}
            for r in records
            if iri_of(r["name"]) in query
        ]

    def reason(body: dict[str, Any]) -> dict[str, Any]:
        request = json.loads(body["prompt"])
        if asked is not None:
            return {"values": asked(request)}
        return {"values": (proposals or read_values)(request["text"])}

    def decide(body: dict[str, Any]) -> dict[str, Any]:
        text = body["state"]["text"]
        return {
            qid: support(text, q["instructions"], list(q["criteria"]))
            for qid, q in body["questions"].items()
        }

    return FakeLbb(
        rows,
        reason=reason,
        decide=decide,
        review_status=review_status,
        review_answer=review_answer,
        review_refused=review_refused,
    )


def matches(got: Any, want: dict[str, Any]) -> bool:
    if isinstance(want["value"], (int, float)):
        return bool(got == want["value"])
    accepted = want.get("accept") or [str(want["value"]).lower()]
    return str(got).strip().lower() in accepted


# The values of the answer key that the port gives a person to review, with
# the reason it gives: a number the text states in another unit (the key's
# ``stated``, which LBB's values.extract converts), a number whose quote
# states no unit, and a quote that says ``about`` (the mock decision model
# gives it 0.6, below the gate).
REVIEWED = {
    ("T-160", "width_cm"): "The unit is not confirmed: the text states m; the property holds cm.",
    ("T-160", "depth_cm"): "The unit is not confirmed: the text states mm; the property holds cm.",
    ("K-17", "width_cm"): "The text supports the value with 0.60, below the gate 0.8.",
    ("R-3", "width_cm"): "The unit is not confirmed: the text states no unit; the property holds cm.",
}


def test_the_samples_give_the_values_of_the_answer_key_and_none_it_leaves_out() -> None:
    fake = fake_for(RECORDS)
    fake.queue(message([iri_of(r["name"]) for r in RECORDS]))
    serve(fake)

    for record in EXPECTED["records"]:
        values = fake.written(record["name"])
        for want in record["values"]:
            if (record["name"], want["property"]) in REVIEWED:
                assert want["property"] not in values, (record["name"], want)
            else:
                assert matches(values.get(want["property"]), want), (record["name"], want)
        for out in record["leave_out"]:
            if out["value"] is None:
                assert out["property"] not in values
            else:
                assert not matches(values.get(out["property"]), out)
        assert sorted(values) == sorted(
            v["property"]
            for v in record["values"]
            if (record["name"], v["property"]) not in REVIEWED
        )
    # One value item per value in review, with its reason.
    asked = {
        (item["subject"]["name"], item["question"]["property"]): item["rationale"]
        for item in fake.questions.values()
    }
    assert sorted(asked) == sorted(REVIEWED)
    for key, rationale in asked.items():
        assert rationale.startswith(REVIEWED[key]), (key, rationale)
    (done,) = fake.completed
    assert done["state"] == {
        "instances": 6,
        "values_proposed": 22,
        "written": 16,
        "review": 4,
        "weak_support": 2,
        "quote_not_found": 0,
        "invalid_value": 0,
        "unknown_property": 0,
        "duplicate": 0,
        "failed": 0,
        "retried": 0,
        "commits": 1,
        "removed": 0,
    }
    # Each model call names the turn's run, and their usage goes with the completion.
    assert done["usage"]["calls"] == len(fake.model_calls) == 12
    reason = next(body for path, body, _ in fake.calls if path.endswith("/activities/reason"))
    assert reason["workflow"]["run_id"] == "run-1"
    assert reason["effect_id"] == "e1/values-0"
    assert fake.reviews == {}


def test_each_written_value_has_a_source_whose_evidence_is_a_quote_of_the_text() -> None:
    fake = fake_for(RECORDS)
    fake.queue(message([iri_of(r["name"]) for r in RECORDS]))
    serve(fake)
    (commit,) = fake.commits.values()
    edges = commit["triplets"]
    assert len(edges) == 16
    for edge in edges:
        record = next(r for r in RECORDS if r["name"] == edge["source"]["name"])
        assert edge["relation"] == "HAS_VALUE_SOURCE"
        assert edge["evidence"]["text"] in record["text"]
        assert edge["target"]["type"] == "ValueSource"
        assert edge["confidence"] >= DEFAULTS.gate
    # The commit replaces what the workflow owns on its subjects.
    assert commit["replace_owned"]["relations"] == ["HAS_VALUE_SOURCE"]
    assert commit["workflow_fence"]["workflow_id"] == "starter.values-extract"


def test_a_value_below_the_gate_goes_to_review_and_the_checks_run_in_lbbs_order() -> None:
    record = {"name": "S 9.1", "text": "S 9.1 Bench of plastic. Width about 70 cm, depth 40 cm."}
    # The checks of values.extract: the property, then the first value of it,
    # then the type, then the quote. A second material is a duplicate even
    # when the first one fails a later check.
    proposals = [
        {"property": "width_cm", "value": "70", "unit": "cm", "quote": "Width about 70 cm"},
        {"property": "depth_cm", "value": "forty", "unit": "cm", "quote": "depth 40 cm"},
        {"property": "material", "value": "plastic", "unit": "", "quote": "Bench of plastic"},
        {"property": "material", "value": "wood", "unit": "", "quote": "made of wood"},
        {"property": "height_cm", "value": "80", "unit": "", "quote": "Bench of plastic"},
    ]
    fake = fake_for([record], proposals=lambda text: proposals)
    fake.queue(message([iri_of(record["name"])]))
    serve(fake)

    assert fake.written(record["name"]) == {"material": "plastic"}
    state = fake.completed[0]["state"]
    assert (state["values_proposed"], state["written"], state["review"]) == (5, 1, 1)
    assert (state["invalid_value"], state["duplicate"], state["unknown_property"]) == (1, 1, 1)
    ((key, item),) = fake.questions.items()
    assert item["kind"] == "value"
    # The key of LBB's values.extract: its instance and property.
    entity = entity_id_from_iri(iri_of(record["name"]))
    assert key == value_item_key("values-extract", iri_of(record["name"]), "width_cm")
    assert key == (
        f"starter:values-extract:value:{stable_hash_hex(['value_item_v2', entity, 'width_cm'])[:24]}"
    )
    assert item["title"] == "Set width_cm of S 9.1 to 70"
    assert item["rationale"] == "The text supports the value with 0.60, below the gate 0.8."
    assert item["question"] == {
        "property": "width_cm",
        "value": 70,
        "unit": "cm",
        "confidence": item["question"]["confidence"],
    }
    assert item["evidence"][0]["quote"] == "Width about 70 cm"
    assert item["origin"] == {
        "kind": "agent",
        "id": "starter:values-extract",
        "label": "values.extract",
    }
    assert item["facts"]["entity_properties"][0]["properties"] == {"width_cm": 70}

    # Accepted, the value is written with the others the next time the instance is read.
    accepting = fake_for(
        [record], proposals=lambda text: proposals, review_answer=lambda request: {"decision": "accept"}
    )
    accepting.queue(message([iri_of(record["name"])]))
    serve(accepting)
    assert accepting.written(record["name"]) == {"width_cm": 70, "material": "plastic"}


def run_again(fake: FakeLbb, name: str) -> None:
    """A second message for the instance ``name``: its own turn."""
    fake.queue(message([iri_of(name)]))
    serve(fake)


def width(value: str, quote: str, unit: str = "cm") -> list[dict[str, str]]:
    return [{"property": "width_cm", "value": value, "unit": unit, "quote": quote}]


def test_a_persons_correction_stays_when_new_evidence_reads_another_value_which_asks_again() -> None:
    record = {"name": "S 9.3", "text": "S 9.3 Bench of plastic. Width about 70 cm, depth 40 cm."}
    proposals = width("70", "Width about 70 cm")
    fake = fake_for(
        [record],
        proposals=lambda text: proposals,
        review_answer=lambda request: {"decision": "correct", "value": 72},
    )
    run_again(fake, "S 9.3")
    assert fake.written("S 9.3") == {"width_cm": 72}
    # A new version of the text reads 80 above the gate: the person is asked
    # again, and the corrected 72 stays until they answer.
    record["text"] = "S 9.3 Bench of plastic. Width 80 cm."
    proposals[:] = width("80", "Width 80 cm")
    run_again(fake, "S 9.3")
    assert fake.written("S 9.3") == {"width_cm": 72}
    key = value_item_key("values-extract", iri_of("S 9.3"), "width_cm")
    item = fake.item_of(key, fake.questions[key])
    assert item["status"] == "open"
    assert item["question"]["value"] == 80
    assert item["previous_answer"] == {"decision": "correct", "value": 72}


def test_the_value_a_person_decided_on_quoted_from_another_span_asks_nothing() -> None:
    record = {"name": "S 9.2", "text": "S 9.2 Bench. Width about 70 cm."}
    proposals = width("70", "Width about 70 cm")
    fake = fake_for(
        [record],
        proposals=lambda text: proposals,
        review_answer=lambda request: {"decision": "correct", "value": 72},
    )
    run_again(fake, "S 9.2")
    # The same text; the model quotes another span of it and spells the unit
    # otherwise. The person decided on 70 already.
    proposals[:] = width("70", "about 70 cm", unit="CM")
    run_again(fake, "S 9.2")
    assert fake.written("S 9.2") == {"width_cm": 72}
    assert fake.previous == {}


def test_a_rejected_value_is_not_written_again_even_above_the_gate() -> None:
    record = {"name": "S 9.6", "text": "S 9.6 Bench. Width about 70 cm."}
    proposals = width("70", "Width about 70 cm")
    fake = fake_for(
        [record], proposals=lambda text: proposals, review_answer=lambda request: {"decision": "reject"}
    )
    run_again(fake, "S 9.6")
    assert fake.written("S 9.6") == {}
    record["text"] = "S 9.6 Bench. Width 70 cm."
    proposals[:] = width("70", "Width 70 cm")
    run_again(fake, "S 9.6")
    assert fake.written("S 9.6") == {}


def test_a_value_the_graph_refused_is_not_written_when_a_later_text_states_it() -> None:
    record = {"name": "S 9.7", "text": "S 9.7 Bench. Width about 70 cm."}
    proposals = width("70", "Width about 70 cm")
    fake = fake_for(
        [record],
        proposals=lambda text: proposals,
        review_answer=lambda request: {"decision": "correct", "value": 72},
        review_refused=lambda request: "the shape MaxWidth rejects 72",
    )
    run_again(fake, "S 9.7")
    assert fake.written("S 9.7") == {}
    # The text now states 72 above the gate: the graph refused it, so it is
    # not written; the item stays open for the person.
    record["text"] = "S 9.7 Bench. Width 72 cm."
    proposals[:] = width("72", "Width 72 cm")
    run_again(fake, "S 9.7")
    assert fake.written("S 9.7") == {}


def test_a_held_value_keeps_its_source_link() -> None:
    record = {"name": "S 9.9", "text": "S 9.9 Bench. Width about 70 cm."}
    proposals = width("70", "Width about 70 cm")
    fake = fake_for(
        [record], proposals=lambda text: proposals, review_answer=lambda request: {"decision": "accept"}
    )
    run_again(fake, "S 9.9")
    assert fake.written("S 9.9") == {"width_cm": 70}
    # New evidence with another value: asked again; the accepted 70 and its
    # source link stay, so the instance's write replaces neither.
    record["text"] = "S 9.9 Bench. Width 85 cm."
    proposals[:] = width("85", "Width 85 cm")
    run_again(fake, "S 9.9")
    assert fake.written("S 9.9") == {"width_cm": 70}
    held = list(fake.commits.values())[-1]["replace_owned"]
    assert held.get("relations", []) == []
    assert "width_cm" not in held["properties"]
    assert len(fake.previous) == 1


@pytest.mark.parametrize(
    ("status", "writes"), [("accepted", {"width_cm": 70}), ("open", {}), ("dismissed", {})]
)
def test_a_value_an_earlier_version_filed_as_a_suggestion_keeps_working(
    status: str, writes: dict[str, Any]
) -> None:
    record = {"name": "S 9.4", "text": "S 9.4 Bench. Width about 70 cm."}
    fake = fake_for([record], proposals=lambda text: width("70", "Width about 70 cm"))
    iri = iri_of("S 9.4")
    # The key of the suggestion: one per value.
    key = legacy_value_key("values-extract", iri, {"property": "width_cm", "value": "70"})
    fake.reviews[key] = {
        "key": key,
        "title": "Set width_cm of S 9.4 to 70",
        "status": status,
        "origin": {"kind": "agent", "id": "starter:values-extract"},
        "facts": {
            "entity_properties": [
                {**entity_ref(iri, "Product", "S 9.4"), "properties": {"width_cm": 70}}
            ],
            "triplets": [],
        },
    }
    run_again(fake, "S 9.4")
    # An accepted one is written again, an open one stays the question (no
    # value item), and a dismissed one is a reject of its value.
    assert fake.questions == {}
    assert fake.reviews[key]["status"] == status
    assert fake.written("S 9.4") == writes


def test_a_worker_killed_during_the_write_replays_its_steps_and_writes_once() -> None:
    fake = fake_for(RECORDS)
    fake.queue(message([iri_of(r["name"]) for r in RECORDS]))
    # The process dies after the commit landed and before the step was saved.
    fake.crash_after = "commit"
    with pytest.raises(Killed):
        serve(fake)
    assert fake.completed == []
    calls_before = list(fake.model_calls)
    keys_before = list(fake.commits)

    # Another worker claims the turn again with the saved steps.
    fake.claim_again(1)
    serve(fake)

    # The saved steps replay: no model call runs again, and the write step
    # sends the same effect id, which the server answers as a replay.
    assert fake.model_calls == calls_before
    assert list(fake.commits) == keys_before == ["e1/write"]
    commits = [h["idempotency-key"] for path, _, h in fake.calls if path == "/v1/graph/commit"]
    assert commits == ["e1/write", "e1/write"]
    (done,) = fake.completed
    assert done["state"]["written"] == 16
    # The write was a replay the second time: this turn made no new commit.
    assert done["state"]["commits"] == 0


def test_values_take_their_property_type_and_a_quote_is_in_the_text() -> None:
    assert coerce("60", "f64") == {"value": 60}
    assert coerce("60,5", "f64") == {"value": 60.5}
    assert coerce("1.5", "i64") == {"error": "`1.5` is not a whole number"}
    assert coerce("ja", "bool") == {"value": True}
    assert coerce("2026-10-21", "date_time") == {"value": "2026-10-21T00:00:00Z"}
    assert "error" in coerce("2026-13-01", "datetime")
    assert quote_holds("about 60x40 cm", "60", "D-1 about 60x40 cm, white")
    assert not quote_holds("60x40", "70", "D-1 about 60x40 cm")
    assert unit_problem("cm", "cm", "width 60 cm") is None
    assert unit_problem("cm", "mm", "600 mm") == "the text states mm; the property holds cm"
    assert unit_problem("cm", "", "60") == "the text states no unit; the property holds cm"
    checked = check(
        [{"property": "width_cm", "value": "600 mm", "unit": "", "quote": "width 600 mm here"}],
        DEFAULTS.properties,
        "width 600 mm here",
        DEFAULTS.units,
    )
    assert checked[0]["value"] == "600"
    assert checked[0]["unconfirmed"] == "the text states mm; the property holds cm"


def test_the_trigger_and_the_ontology_fit_the_settings() -> None:
    created = trigger()
    assert created["workflow"] == {"workflow_type": "values-extract", "version": "v1"}
    assert created["class_"] == "Product"
    assert created["owns"]["properties"] == ["width_cm", "depth_cm", "material", "color"]
    units = {op["name"]: op.get("unit") for op in ontology() if op["op"] == "add_property"}
    assert units["width_cm"] == "cm" and units["material"] is None


def test_the_samples_are_the_templates() -> None:
    template = Path(__file__).resolve().parents[2] / "templates" / "values-extract" / "samples"
    if not template.is_dir():
        pytest.skip("the TypeScript template is not in this tree")
    for name in ("records.json", "expected.json"):
        assert (SAMPLES / name).read_bytes() == (template / name).read_bytes()


# ---- repair: one retry with the failed checks ---------------------------------------------------

BENCH = {"name": "S 9.2", "text": "S 9.2 Bench of plastic. Width 70 cm, depth 40 cm."}


def misquoting(again: str = "right") -> Callable[[dict[str, Any]], list[dict[str, str]]]:
    """The first answer misquotes both values; a request with ``failed_checks`` gets the right
    quotes (or, with ``again="wrong"``, the same wrong ones)."""

    def asked(request: dict[str, Any]) -> list[dict[str, str]]:
        quote = (lambda text: text) if "failed_checks" in request and again == "right" else (
            lambda text: f"{text}!"
        )
        return [
            {"property": "width_cm", "value": "70", "unit": "cm", "quote": quote("Width 70 cm")},
            {"property": "depth_cm", "value": "40", "unit": "cm", "quote": quote("depth 40 cm")},
        ]

    return asked


def reason_bodies(fake: FakeLbb) -> list[dict[str, Any]]:
    return [body for path, body, _ in fake.calls if path.endswith("/activities/reason")]


def test_without_repair_a_failed_check_is_no_reason_to_ask_again() -> None:
    fake = fake_for([BENCH], asked=misquoting())
    fake.queue(message([iri_of(BENCH["name"])]))
    serve(fake)

    (done,) = fake.completed
    assert fake.written(BENCH["name"]) == {}
    assert done["state"]["quote_not_found"] == 2
    assert done["state"]["retried"] == 0
    (body,) = reason_bodies(fake)
    assert "attempt" not in body


def test_with_repair_one_retry_gets_the_failed_checks_and_its_values_pass_the_same_gate() -> None:
    fake = fake_for([BENCH], asked=misquoting())
    fake.queue(message([iri_of(BENCH["name"])]))
    serve(fake, Settings(repair=1))

    (done,) = fake.completed
    # The retry's values are written like any value: through the decision call and the gate.
    assert fake.written(BENCH["name"]) == {"width_cm": 70, "depth_cm": 40}
    assert done["state"]["retried"] == 1
    assert done["state"]["written"] == 2
    assert done["state"]["quote_not_found"] == 2
    first, retry = reason_bodies(fake)
    # Only the retry names its attempt; the first call reads as attempt 1.
    assert ("attempt" in first, retry["attempt"]) == (False, 2)
    assert retry["effect_id"] == f"{first['effect_id']}.repair"
    # The retry asks the same input, plus the failed checks and nothing else.
    asked_first, asked_retry = json.loads(first["prompt"]), json.loads(retry["prompt"])
    assert "failed_checks" not in asked_first
    assert asked_retry.pop("repair") == REPAIR_NOTE
    assert asked_retry.pop("failed_checks") == [
        {
            "check": "quote_not_found",
            "property": "width_cm",
            "message": "the quote is not in the text",
            "value": "70",
            "quote": "Width 70 cm!",
        },
        {
            "check": "quote_not_found",
            "property": "depth_cm",
            "message": "the quote is not in the text",
            "value": "40",
            "quote": "depth 40 cm!",
        },
    ]
    assert asked_retry == asked_first
    assert sum(path.endswith("/activities/decide") for path, _, _ in fake.calls) == 1


def test_a_clean_answer_is_not_asked_again_and_a_retry_that_fails_leaves_nothing() -> None:
    clean = fake_for(RECORDS[:1])
    clean.queue(message([iri_of(RECORDS[0]["name"])]))
    serve(clean, Settings(repair=1))
    assert [body.get("attempt") for body in reason_bodies(clean)] == [None]

    fake = fake_for([BENCH], asked=misquoting("wrong"))
    fake.queue(message([iri_of(BENCH["name"])]))
    serve(fake, Settings(repair=1))
    (done,) = fake.completed
    assert [body.get("attempt") for body in reason_bodies(fake)] == [None, 2]
    assert fake.written(BENCH["name"]) == {}
    assert done["state"]["quote_not_found"] == 4
    assert done["state"]["retried"] == 1
    assert not any(path.endswith("/activities/decide") for path, _, _ in fake.calls)


def test_a_repair_keeps_the_values_that_passed_and_fills_the_failed_ones() -> None:
    properties = {"width_cm": "f64", "material": "text"}
    text = "Waschtisch, width_cm: 55 cm, material: Keramik"
    units = {"width_cm": "cm"}
    first = check(
        [
            {"property": "width_cm", "value": "55", "unit": "cm", "quote": "width_cm: 55 cm"},
            {"property": "material", "value": "Keramik", "unit": "", "quote": "material: Glas"},
        ],
        properties,
        text,
        units,
    )
    second = check(
        [
            {"property": "width_cm", "value": "56", "unit": "cm", "quote": "width_cm: 55 cm"},
            {"property": "material", "value": "Keramik", "unit": "", "quote": "material: Keramik"},
        ],
        properties,
        text,
        units,
    )
    merged = merge_attempts(first, second)
    assert [
        (v["property"], v["value"], v["outcome"], v.get("attempt"), "typed" in v) for v in merged
    ] == [
        ("width_cm", "55", "", None, True),
        ("material", "Keramik", "quote_not_found", None, False),
        ("width_cm", "56", "duplicate", 2, False),
        ("material", "Keramik", "", 2, True),
    ]
    # A repair value for a property whose first value passed is a duplicate
    # whatever its checks said, so the property counts once.
    failing = check(
        [{"property": "width_cm", "value": "wide", "unit": "cm", "quote": "width_cm: 55 cm"}],
        properties,
        text,
        units,
    )
    assert [(v["property"], v["outcome"], v.get("attempt")) for v in merge_attempts(first, failing)] == [
        ("width_cm", "", None),
        ("material", "quote_not_found", None),
        ("width_cm", "duplicate", 2),
    ]


def test_each_failed_check_makes_one_diagnostic_that_holds_the_models_words_and_no_label() -> None:
    properties = {"width_cm": "f64", "material": "text"}
    text = "Waschtisch, width_cm: 55 cm, material: Keramik"

    def failures(values: list[Any]) -> list[tuple[str, str | None]]:
        checked = check(values, properties, text, {"width_cm": "cm"})
        return [(f["check"], f.get("property")) for f in failed_checks({"values": values}, checked)]

    assert failed_checks({"items": []}, []) == [
        {"check": "answer_shape", "message": "the answer is not a JSON object with a `values` list"}
    ]
    assert [f["check"] for f in failed_checks(None, [])] == ["answer_shape"]
    assert failures(["width"]) == [("answer_shape", None)]
    assert failures(
        [
            {"property": "material", "value": "Keramik", "unit": ""},
            {"property": "width_cm", "value": 55, "unit": "cm", "quote": "width_cm: 55 cm"},
        ]
    ) == [("missing_field", "material"), ("missing_field", "width_cm")]
    assert failures(
        [
            {"property": "height_cm", "value": "9", "unit": "cm", "quote": "height_cm: 9 cm"},
            {"property": "width_cm", "value": "wide", "unit": "cm", "quote": "width_cm: 55 cm"},
            {"property": "material", "value": "Keramik", "unit": "", "quote": "material: Glas"},
        ]
    ) == [
        ("unknown_property", "height_cm"),
        ("invalid_value", "width_cm"),
        ("quote_not_found", "material"),
    ]
    # A second value of a property is no failed check: the first one stands.
    assert (
        failures(
            [
                {"property": "material", "value": "Keramik", "unit": "", "quote": "material: Keramik"},
                {"property": "material", "value": "Glas", "unit": "", "quote": "material: Keramik"},
            ]
        )
        == []
    )
    long = [{"property": "material", "value": "Keramik", "unit": "", "quote": "x" * 400}]
    (diagnostic,) = failed_checks({"values": long}, check(long, properties, text))
    assert len(diagnostic["quote"]) == 200


def test_a_refused_retry_leaves_the_first_answer() -> None:
    fake = fake_for([BENCH], asked=misquoting())
    plain_reason = fake.reason

    def reason(body: dict[str, Any]) -> dict[str, Any] | None:
        if "failed_checks" in json.loads(body["prompt"]):
            return None  # the fake answers 422: a refusal
        return plain_reason(body)

    fake.reason = reason
    fake.queue(message([iri_of(BENCH["name"])]))
    serve(fake, Settings(repair=1))
    (done,) = fake.completed
    assert done["state"]["failed"] == 0
    assert done["state"]["quote_not_found"] == 2
    assert done["state"]["retried"] == 1
    assert fake.written(BENCH["name"]) == {}
