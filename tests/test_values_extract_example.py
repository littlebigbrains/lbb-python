"""The Python values-extract example (``sdk/python/examples/values_extract``)
on its samples, with the real ``WorkflowWorker`` and ``lbb_activities`` against a fake
LBB: the workflow engine, the model activities with the mock rules of the
TypeScript template's test, SPARQL over the sample records, commits with
their idempotency keys, review items and requeue.

The mock reasoning model reads the first size (``60x40 cm`` or ``Breite 60
cm``) with its unit, the first material and the water supply of each text;
the mock decision model says a quote from a sentence about an accessory
(``Zubehör``) or a conditional variant (``Bei Einsatz``) does not support its
value. The expected values come from ``samples/expected.json``.
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
    check,
    coerce,
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
    "Sanitärkeramik",
    "Stahl emailliert",
    "Mineralguss",
    "Edelstahl",
    "Kristallglas",
    "Keramik",
    "Glas",
]


def read_values(text: str) -> list[dict[str, str]]:
    """The mock reasoning model: the first size, material and water supply of a text."""
    values: list[dict[str, str]] = []
    size = re.search(r"(?:ca\. )?(\d+)x(\d+) cm", text)
    width = re.search(r"Breite (\d+) cm", text)
    if size:
        values += [
            {"property": "width_cm", "value": size[1], "unit": "cm", "quote": size[0]},
            {"property": "depth_cm", "value": size[2], "unit": "cm", "quote": size[0]},
        ]
    elif width:
        values.append({"property": "width_cm", "value": width[1], "unit": "cm", "quote": width[0]})
    found = [
        (m.start(), name)
        for name in MATERIALS
        if (m := re.search(rf"(?<![^\W\d_]){re.escape(name)}(?![^\W\d_])", text))
    ]
    if found:
        material = min(found)[1]
        values.append({"property": "material", "value": material, "unit": "", "quote": material})
    water = re.search(r"Anschluss[^.]*?(Kalt- und Warmwasser|Kaltwasser)", text)
    if water:
        values.append(
            {
                "property": "water_supply",
                "value": "kalt" if water[1] == "Kaltwasser" else "kalt und warm",
                "unit": "",
                "quote": water[0],
            }
        )
    return values


def support(text: str, instructions: str, options: list[str]) -> dict[str, Any]:
    """The mock decision model: the support of each quote, from the sentence that holds it."""
    found = re.search(r'Quote: "(.*)"$', instructions, re.S)
    assert found
    quote = found[1]
    sentence = next((s for s in re.split(r"(?<=\.)\s+", text) if quote in s), "")
    p = 0.05 if re.search("Zubehör|Bei Einsatz", sentence) else 0.6 if "etwa" in sentence else 0.92
    return {
        "choice": "supported" if p >= 0.5 else "not_supported",
        "confidence": max(p, 1 - p),
        "probabilities": {o: p if o == "supported" else (1 - p) / 2 for o in options},
    }


def fake_for(
    records: list[dict[str, str]],
    *,
    proposals: Callable[[str], list[dict[str, str]]] | None = None,
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
        text = json.loads(body["prompt"])["text"]
        return {"values": (proposals or read_values)(text)}

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


def test_the_samples_give_the_values_of_the_answer_key_and_none_it_leaves_out() -> None:
    fake = fake_for(RECORDS)
    fake.queue(message([iri_of(r["name"]) for r in RECORDS]))
    serve(fake)

    for record in EXPECTED["records"]:
        values = fake.written(record["name"])
        for want in record["values"]:
            assert matches(values.get(want["property"]), want), (record["name"], want)
        for out in record["leave_out"]:
            if out["value"] is None:
                assert out["property"] not in values
            else:
                assert not matches(values.get(out["property"]), out)
        assert sorted(values) == sorted(v["property"] for v in record["values"])
    (done,) = fake.completed
    assert done["state"] == {
        "instances": 6,
        "values_proposed": 22,
        "written": 20,
        "review": 0,
        "weak_support": 2,
        "quote_not_found": 0,
        "invalid_value": 0,
        "unknown_property": 0,
        "duplicate": 0,
        "failed": 0,
        "commits": 1,
        "removed": 0,
    }
    # Each model call names the turn's run, and their usage goes with the completion.
    assert done["usage"]["calls"] == len(fake.model_calls) == 12
    reason = next(body for path, body, _ in fake.calls if path.endswith("/activities/reason"))
    assert reason["workflow"]["run_id"] == "run-1"
    assert reason["effect_id"] == "e1/values-0"
    assert fake.reviews == {}
    assert fake.questions == {}


def test_each_written_value_has_a_source_whose_evidence_is_a_quote_of_the_text() -> None:
    fake = fake_for(RECORDS)
    fake.queue(message([iri_of(r["name"]) for r in RECORDS]))
    serve(fake)
    (commit,) = fake.commits.values()
    edges = commit["triplets"]
    assert len(edges) == 20
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
    record = {"name": "S 9.1", "text": "S 9.1 Becken aus Kunststoff. Breite etwa 70 cm, Tiefe 40 cm."}
    # The checks of values.extract: the property, then the first value of it,
    # then the type, then the quote. A second material is a duplicate even
    # when the first one fails a later check.
    proposals = [
        {"property": "width_cm", "value": "70", "unit": "cm", "quote": "Breite etwa 70 cm"},
        {"property": "depth_cm", "value": "vierzig", "unit": "cm", "quote": "Tiefe 40 cm"},
        {"property": "material", "value": "Kunststoff", "unit": "", "quote": "Becken aus Kunststoff"},
        {"property": "material", "value": "Holz", "unit": "", "quote": "aus Holz gefertigt"},
        {"property": "height_cm", "value": "80", "unit": "", "quote": "Becken aus Kunststoff"},
    ]
    fake = fake_for([record], proposals=lambda text: proposals)
    fake.queue(message([iri_of(record["name"])]))
    serve(fake)

    assert fake.written(record["name"]) == {"material": "Kunststoff"}
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
    assert item["evidence"][0]["quote"] == "Breite etwa 70 cm"
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
    assert accepting.written(record["name"]) == {"width_cm": 70, "material": "Kunststoff"}


def run_again(fake: FakeLbb, name: str) -> None:
    """A second message for the instance ``name``: its own turn."""
    fake.queue(message([iri_of(name)]))
    serve(fake)


def width(value: str, quote: str, unit: str = "cm") -> list[dict[str, str]]:
    return [{"property": "width_cm", "value": value, "unit": unit, "quote": quote}]


def test_a_persons_correction_stays_when_new_evidence_reads_another_value_which_asks_again() -> None:
    record = {"name": "S 9.3", "text": "S 9.3 Becken aus Kunststoff. Breite etwa 70 cm, Tiefe 40 cm."}
    proposals = width("70", "Breite etwa 70 cm")
    fake = fake_for(
        [record],
        proposals=lambda text: proposals,
        review_answer=lambda request: {"decision": "correct", "value": 72},
    )
    run_again(fake, "S 9.3")
    assert fake.written("S 9.3") == {"width_cm": 72}
    # A new version of the text reads 80 above the gate: the person is asked
    # again, and the corrected 72 stays until they answer.
    record["text"] = "S 9.3 Becken aus Kunststoff. Breite 80 cm."
    proposals[:] = width("80", "Breite 80 cm")
    run_again(fake, "S 9.3")
    assert fake.written("S 9.3") == {"width_cm": 72}
    key = value_item_key("values-extract", iri_of("S 9.3"), "width_cm")
    item = fake.item_of(key, fake.questions[key])
    assert item["status"] == "open"
    assert item["question"]["value"] == 80
    assert item["previous_answer"] == {"decision": "correct", "value": 72}


def test_the_value_a_person_decided_on_quoted_from_another_span_asks_nothing() -> None:
    record = {"name": "S 9.2", "text": "S 9.2 Becken. Breite etwa 70 cm."}
    proposals = width("70", "Breite etwa 70 cm")
    fake = fake_for(
        [record],
        proposals=lambda text: proposals,
        review_answer=lambda request: {"decision": "correct", "value": 72},
    )
    run_again(fake, "S 9.2")
    # The same text; the model quotes another span of it and spells the unit
    # otherwise. The person decided on 70 already.
    proposals[:] = width("70", "etwa 70 cm", unit="CM")
    run_again(fake, "S 9.2")
    assert fake.written("S 9.2") == {"width_cm": 72}
    assert fake.previous == {}


def test_a_rejected_value_is_not_written_again_even_above_the_gate() -> None:
    record = {"name": "S 9.6", "text": "S 9.6 Becken. Breite etwa 70 cm."}
    proposals = width("70", "Breite etwa 70 cm")
    fake = fake_for(
        [record], proposals=lambda text: proposals, review_answer=lambda request: {"decision": "reject"}
    )
    run_again(fake, "S 9.6")
    assert fake.written("S 9.6") == {}
    record["text"] = "S 9.6 Becken. Breite 70 cm."
    proposals[:] = width("70", "Breite 70 cm")
    run_again(fake, "S 9.6")
    assert fake.written("S 9.6") == {}


def test_a_value_the_graph_refused_is_not_written_when_a_later_text_states_it() -> None:
    record = {"name": "S 9.7", "text": "S 9.7 Becken. Breite etwa 70 cm."}
    proposals = width("70", "Breite etwa 70 cm")
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
    record["text"] = "S 9.7 Becken. Breite 72 cm."
    proposals[:] = width("72", "Breite 72 cm")
    run_again(fake, "S 9.7")
    assert fake.written("S 9.7") == {}


def test_a_held_value_keeps_its_source_link() -> None:
    record = {"name": "S 9.9", "text": "S 9.9 Becken. Breite etwa 70 cm."}
    proposals = width("70", "Breite etwa 70 cm")
    fake = fake_for(
        [record], proposals=lambda text: proposals, review_answer=lambda request: {"decision": "accept"}
    )
    run_again(fake, "S 9.9")
    assert fake.written("S 9.9") == {"width_cm": 70}
    # New evidence with another value: asked again; the accepted 70 and its
    # source link stay, so the instance's write replaces neither.
    record["text"] = "S 9.9 Becken. Breite 85 cm."
    proposals[:] = width("85", "Breite 85 cm")
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
    record = {"name": "S 9.4", "text": "S 9.4 Becken. Breite etwa 70 cm."}
    fake = fake_for([record], proposals=lambda text: width("70", "Breite etwa 70 cm"))
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
                {**entity_ref(iri, "EquipmentItem", "S 9.4"), "properties": {"width_cm": 70}}
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
    assert done["state"]["written"] == 20
    # The write was a replay the second time: this turn made no new commit.
    assert done["state"]["commits"] == 0


def test_values_take_their_property_type_and_a_quote_is_in_the_text() -> None:
    assert coerce("60", "f64") == {"value": 60}
    assert coerce("60,5", "f64") == {"value": 60.5}
    assert coerce("1.5", "i64") == {"error": "`1.5` is not a whole number"}
    assert coerce("ja", "bool") == {"value": True}
    assert coerce("2026-10-21", "date_time") == {"value": "2026-10-21T00:00:00Z"}
    assert "error" in coerce("2026-13-01", "datetime")
    assert quote_holds("ca. 60x40 cm", "60", "S 4.1 ca. 60x40 cm, weiß")
    assert not quote_holds("60x40", "70", "S 4.1 ca. 60x40 cm")
    assert unit_problem("cm", "cm", "Breite 60 cm") is None
    assert unit_problem("cm", "mm", "600 mm") == "the text states mm; the property holds cm"
    assert unit_problem("cm", "", "60") == "the text states no unit; the property holds cm"
    checked = check(
        [{"property": "width_cm", "value": "600 mm", "unit": "", "quote": "Breite 600 mm hier"}],
        DEFAULTS.properties,
        "Breite 600 mm hier",
        DEFAULTS.units,
    )
    assert checked[0]["value"] == "600"
    assert checked[0]["unconfirmed"] == "the text states mm; the property holds cm"


def test_the_trigger_and_the_ontology_fit_the_settings() -> None:
    created = trigger()
    assert created["workflow"] == {"workflow_type": "values-extract", "version": "v1"}
    assert created["class_"] == "EquipmentItem"
    assert created["owns"]["properties"] == ["width_cm", "depth_cm", "material", "water_supply"]
    units = {op["name"]: op.get("unit") for op in ontology() if op["op"] == "add_property"}
    assert units["width_cm"] == "cm" and units["material"] is None


def test_the_samples_are_the_templates() -> None:
    template = Path(__file__).resolve().parents[2] / "templates" / "values-extract" / "samples"
    if not template.is_dir():
        pytest.skip("the TypeScript template is not in this tree")
    for name in ("records.json", "expected.json"):
        assert (SAMPLES / name).read_bytes() == (template / name).read_bytes()
