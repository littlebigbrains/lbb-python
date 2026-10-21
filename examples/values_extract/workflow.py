"""values-extract: fill properties of a class from the text of its instances,
each value with a verbatim quote.

The Python port of the template ``sdk/templates/values-extract/workflow.ts``,
with the same rules, prompts, checks, review items and counters. Those are
the rules of LBB's ``values.extract@v1``.

A triggered workflow of the starter workflow ``workflow`` watches a class and
its text field, and sends the new and changed instances to this workflow.
For each instance the workflow:

1. asks the reasoning model (``reason``) for the values of the properties the
   text states, each with the words that state it;
2. checks each value without a model, in this order: its property is one the
   workflow fills, it is the first value of its property, it takes its
   property's type, and its quote is in the text. A number of a property with
   a unit must come in that unit, with the unit in its quote; otherwise it
   goes to review at most;
3. asks the decision model (``decide``) how well each quote supports its
   value;
4. writes the values at or above the gate, each with a ``ValueSource`` whose
   evidence is the quote (with the instance's page box as its region, when
   the instance has one), and asks a person about each value between 0.5
   and the gate: a ``value`` review item in the property's unit, one per
   property of an instance. A value below 0.5 is dropped.

A person's answer wins over the model, for the evidence it was given on (the
value's quote and the unit the text states). A value a person accepted or
corrected is written as answered; one a person rejected, or one the graph
refused, is not written again. New evidence with another value asks again,
and the answered value and its source stay until the person answers. A value
an earlier version filed as a suggestion keeps working: an accepted one is
written again, an open one stays the question, and a dismissed one is a
reject of its value.

Every model call runs in a step with the step's effect id, so a turn that
runs again after a crash pays for nothing twice. The write's request id is
its step's effect id, which no other turn repeats. An instance whose model
call is refused goes back to the watch (``lbb.requeue``, at most 3 times)
and keeps its values; the turn completes, so the shard goes on. A spent
budget fails the turn with the time to retry, and the worker waits without
an attempt.
"""

from __future__ import annotations

import json
import math
import re
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal
from urllib.parse import quote as url_quote

from lbb import models, stable_hash_hex
from lbb.workflows import (
    FailedInstance,
    LbbActivities,
    OwnedWrite,
    TriggeredWorkflowDefinition,
    WorkflowContext,
    WorkflowResult,
    WorkflowStepContext,
    entity_id_from_iri,
    entity_ref,
    is_activity_refused,
    triggered_workflow,
)

ValueType = Literal["text", "keyword", "i64", "f64", "bool", "date_time", "datetime"]


@dataclass
class Settings:
    """The settings: edit them for your class. The defaults fit the samples."""

    #: The class whose instances the workflow reads.
    class_name: str = "EquipmentItem"
    #: The property that holds each instance's text.
    text_field: str = "text"
    #: The properties to fill, with their types.
    properties: dict[str, ValueType] = field(
        default_factory=lambda: {
            "width_cm": "f64",
            "depth_cm": "f64",
            "material": "text",
            "water_supply": "keyword",
        }
    )
    #: One sentence about what the text is, for the models.
    context: str = "Entries of the sanitary equipment list of a school building tender, in German."
    #: A value whose support is below this goes to review (from 0.5) or is dropped.
    gate: float = 0.8
    #: The unit of each number property (cm, mm, kg, m2, …), declared in the
    #: ontology by ``ontology()``.
    units: dict[str, str] = field(default_factory=lambda: {"width_cm": "cm", "depth_cm": "cm"})


DEFAULTS = Settings()

#: The workflow type and version the triggered workflow names.
WORKFLOW_TYPE = {"workflow_type": "values-extract", "version": "v1"}


def trigger(settings: Settings = DEFAULTS) -> dict[str, Any]:
    """The triggered workflow that sends the instances to this workflow:
    ``client.use_workflow_starter(**trigger())``."""
    return {
        "starter": "workflow",
        "name": "values-extract",
        "class_": settings.class_name,
        "fields": [settings.text_field],
        "workflow": WORKFLOW_TYPE,
        "owns": {
            "classes": ["ValueSource"],
            "relations": ["HAS_VALUE_SOURCE"],
            "properties": list(settings.properties),
        },
        "batch": 8,
    }


def ontology(settings: Settings = DEFAULTS) -> list[dict[str, Any]]:
    """The classes, properties and relation the workflow reads and writes, for
    ``lbb.evolve``."""

    def prop(name: str, value_type: str, unit: str | None = None) -> dict[str, Any]:
        op: dict[str, Any] = {"op": "add_property", "name": name, "value_type": value_type}
        if unit:
            op["unit"] = unit
        return op

    return [
        {"op": "add_entity_type", "name": settings.class_name},
        {"op": "add_entity_type", "name": "ValueSource"},
        prop(settings.text_field, "text"),
        *(
            prop(name, value_type, unit_of(settings, name))
            for name, value_type in settings.properties.items()
        ),
        prop("property", "keyword"),
        prop("value", "text"),
        prop("support", "f64"),
        {
            "op": "add_relation",
            "name": "HAS_VALUE_SOURCE",
            "domain": [settings.class_name],
            "range": ["ValueSource"],
        },
    ]


#: What a value became: the outcomes of LBB's ``values.extract``.
OUTCOMES = (
    "written",
    "review",
    "weak_support",
    "quote_not_found",
    "invalid_value",
    "unknown_property",
    "duplicate",
)
#: The counters of the workflow, those of LBB's ``values.extract``.
ZERO: dict[str, int] = {
    "instances": 0,
    "values_proposed": 0,
    **{outcome: 0 for outcome in OUTCOMES},
    "failed": 0,
    "commits": 0,
    "removed": 0,
}

SYSTEM = """You read the text of one record of a knowledge graph and fill in property values the text states. Rules:
- Give a value only when the text states it. Do not guess and do not compute.
- Give the values of the record's own item in its base version. Leave out the values of a part, an accessory or an alternative the text names (the battery of a drill, the shade of a lamp), and the values of a variant that holds only under a condition ("if …", "when used with …").
- A value the text gives as approximate ("about 40 kg", "approx. 2 m") is stated: give it without the approximation.
- Each value quotes the words of the text that state it in `quote`, copied exactly, character for character. A value without an exact quote is dropped.
- Write each value in its property's type: a number for i64 and f64 (digits, a dot for decimals, without its unit), true or false for bool, YYYY-MM-DD for datetime, and the words of the text for text and keyword.
- Give a number as the text states it, and in `unit` the unit the text states it in, as a symbol (mm, cm, m, m2, l, kg, kW, h). Do not convert it to the property's unit. Give an empty `unit` when the text states no unit, and for a value that is not a number.
- Give at most one value per property. Leave a property out when the text does not state it."""  # noqa: E501

SUPPORT = {
    "supported": (
        "The quote states this value of this property for the record's own item. "
        "A value stated as approximate counts: 'approx. 40 kg' supports a weight of 40."
    ),
    "weak": "The text implies the value but does not state it.",
    "not_supported": (
        "The quote does not state this value, or it states the value of a part, an accessory, "
        "an alternative or a variant under a condition, not of the record's own item."
    ),
}

#: Instances one turn reads: two steps each, in a turn of 64 steps.
TURN_INSTANCES = 24
#: Characters of a text the models read.
TEXT_CHARS = 40_000
#: Below this support a value is dropped.
SUPPORT_MIN = 0.5
_PLAIN_IRI = re.compile(r"[^\s<>\"{}|\\^`]+")
_ENTITY_IRI = re.compile(r"https://littlebigbrain\.com/e/[0-9a-fA-F]{32}")
_LABEL = "http://www.w3.org/2000/01/rdf-schema#label"
_P = "https://littlebigbrain.com/p/"
_BOX = ("page", "x0", "y0", "x1", "y1")
_NUMBER = re.compile(r"[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?")
_RFC3339 = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})")


def _normalize(text: str) -> str:
    return " ".join(text.split())


def _number(value: float) -> int | float:
    """A number as JavaScript keeps it: a whole float is the integer (``60``,
    not ``60.0``), so the JSON and the text of a value are those of the
    TypeScript template."""
    return int(value) if value.is_integer() and abs(value) < 2**53 else value


def _text(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _js_json(value: Any) -> str:
    """``JSON.stringify``: the text the TypeScript template sends."""
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def coerce(raw: str, value_type: str) -> dict[str, Any]:
    """The value of ``raw`` in its property's type (``{"value": v}``), or why
    it does not take the type (``{"error": why}``)."""
    text = raw.strip()
    if not text:
        return {"error": "the value is empty"}
    cleaned = re.sub(r"\s+", "", text.replace(",", "."))
    number = float(cleaned) if _NUMBER.fullmatch(cleaned) else math.nan
    if value_type == "i64":
        if not math.isfinite(number):
            return {"error": f"`{text}` is not a number"}
        if number.is_integer():
            return {"value": _number(number)}
        return {"error": f"`{text}` is not a whole number"}
    if value_type == "f64":
        if math.isfinite(number):
            return {"value": _number(number)}
        return {"error": f"`{text}` is not a number"}
    if value_type == "bool":
        lower = text.lower()
        if lower in ("true", "yes", "ja"):
            return {"value": True}
        if lower in ("false", "no", "nein"):
            return {"value": False}
        return {"error": f"`{text}` is not true or false"}
    if value_type in ("date_time", "datetime"):
        value = f"{text}T00:00:00Z" if len(text.encode()) == 10 else text
        try:
            ok = bool(_RFC3339.fullmatch(value)) and bool(
                datetime.fromisoformat(value.replace("Z", "+00:00"))
            )
        except ValueError:
            ok = False
        return {"value": value} if ok else {"error": f"`{text}` is not a date"}
    return {"value": text}


def typed(raw: str, value_type: str) -> Any:
    """The value of ``raw`` in its property's type, or ``None`` when it does not
    take the type."""
    return coerce(raw, value_type).get("value")


def quote_problem(quote: str, value: str, text: str) -> str | None:
    """Why a quote does not hold, or ``None`` when it does: it is in the text,
    with at least three words or the value in it."""
    q = _normalize(quote)
    words = len(q.split(" ")) if q else 0
    if not words:
        return "the quote is empty"
    if q not in _normalize(text):
        return "the quote is not in the text"
    v = _normalize(value).lower()
    if words >= 3 or (v != "" and v in q.lower()):
        return None
    return "the quote has fewer than 3 words and does not hold the value"


def quote_holds(quote: str, value: str, text: str) -> bool:
    """A quote must be in the text, with at least three words or the value in it."""
    return quote_problem(quote, value, text) is None


def unit_of(settings: Settings, prop: str) -> str | None:
    """The unit a number property holds, from the settings; none for other types."""
    if settings.properties.get(prop) in ("i64", "f64"):
        return settings.units.get(prop)
    return None


def _unit_text(text: str) -> str:
    return re.sub(r"\.+$", "", text.strip()).lower().replace("²", "2").replace("³", "3")


def unit_problem(unit: str, stated: str, quote: str) -> str | None:
    """Why a number of a property that holds ``unit`` cannot be confirmed in
    that unit: the text states no unit or another one, or the quote does not
    hold it. ``None`` when it is confirmed. The messages are those of LBB's
    ``values.extract``; a number in another unit is LBB's to convert."""
    want = _unit_text(unit)
    got = _unit_text(stated)
    if not got:
        return f"the text states no unit; the property holds {unit}"
    if got != want:
        return f"the text states {stated.strip()}; the property holds {unit}"
    # Not after a letter, and not before a letter or a digit.
    pattern = rf"(?<![^\W\d_]){re.escape(want)}(?![^\W_])"
    if re.search(pattern, _unit_text(quote)):
        return None
    return f"the quote does not state the unit {unit}"


def split_number_unit(text: str) -> tuple[str, str] | None:
    """A number and the unit after it, read apart (``600 mm`` gives ``600`` and
    ``mm``), as LBB does when a model gives the unit in ``value``; ``None`` when
    the text does not start with a number or has no unit after it."""
    trimmed = text.strip()
    end = next((i for i, c in enumerate(trimmed) if c not in "0123456789.,+- "), -1)
    if end < 0:
        return None
    number = trimmed[:end].strip()
    unit = trimmed[end:].strip()
    return (number, unit) if re.search(r"[0-9]", number) and unit else None


def check(
    values: list[Mapping[str, Any]],
    properties: Mapping[str, str],
    text: str,
    units: Mapping[str, str] | None = None,
) -> list[dict[str, Any]]:
    """The checks without a model, in the order of LBB's ``values.extract``.
    Each value comes back with its ``outcome`` (empty when it passed) and,
    when it took its type, ``typed``."""
    units = units or {}
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for item in values:
        prop = str(item.get("property") or "")
        raw = str(item.get("value") or "")
        stated = str(item.get("unit") or "").strip()
        quote = str(item.get("quote") or "")
        # A number given with its unit in `value` (`600 mm`), against the
        # rules: read the two apart.
        split = None if stated else split_number_unit(raw)
        if split:
            raw, stated = split
        checked: dict[str, Any] = {"property": prop, "value": raw, "quote": quote, "outcome": ""}
        value_type = properties.get(prop)
        if not value_type:
            out.append({**checked, "outcome": "unknown_property"})
            continue
        if prop in seen:
            out.append({**checked, "outcome": "duplicate"})
            continue
        seen.add(prop)
        coerced = coerce(raw, value_type)
        if "error" in coerced:
            out.append({**checked, "outcome": "invalid_value", "detail": coerced["error"]})
            continue
        problem = quote_problem(quote, raw, text)
        if problem:
            out.append({**checked, "outcome": "quote_not_found", "detail": problem})
            continue
        unit = units.get(prop) if value_type in ("i64", "f64") else None
        unconfirmed = unit_problem(unit, stated, quote) if unit else None
        checked.update(value=_text(coerced["value"]), typed=coerced["value"])
        if unconfirmed:
            checked["unconfirmed"] = unconfirmed
        if stated:
            # The unit the text states, folded: part of the value's evidence.
            checked["stated_unit"] = _unit_text(stated)
        out.append(checked)
    return out


def value_item_key(workflow: str, iri: str, prop: str) -> str:
    """The key of the value item of a property of an instance, as LBB's
    values.extract gives it."""
    digest = stable_hash_hex(["value_item_v2", entity_id_from_iri(iri), prop])[:24]
    return f"starter:{workflow}:value:{digest}"


def legacy_value_key(workflow: str, iri: str, v: Mapping[str, Any]) -> str:
    """The key an earlier version filed a value under, as a suggestion: one per
    value."""
    digest = stable_hash_hex([entity_id_from_iri(iri), v["property"], v["value"]])[:24]
    return f"starter:{workflow}:value:{digest}"


def evidence_of(v: Mapping[str, Any]) -> str:
    """The evidence a value is read from, as LBB marks it: its quote and the
    unit the text states."""
    return stable_hash_hex(
        ["values_evidence_v2", _normalize(v["quote"]), v.get("stated_unit") or ""]
    )[:32]


#: No value: a comparison with it never holds.
_NONE: Any = object()


def _value_in_force(item: Mapping[str, Any]) -> dict[str, Any] | None:
    """The value of an answered item that holds in the graph: an accept or a
    correction the graph took (``value`` and whether it was ``corrected``)."""
    answer = item.get("answer")
    if not answer or item.get("status") != "answered" or (item.get("effects") or {}).get(
        "commit_error"
    ):
        return None
    decision = answer.get("decision")
    if decision == "correct":
        value = answer.get("value")
    elif decision == "accept":
        value = answer.get("value")
        if value is None:
            value = (item.get("question") or {}).get("value")
    else:
        return None
    return None if value is None else {"value": value, "corrected": decision == "correct"}


def _rejected_value(item: Mapping[str, Any]) -> Any:
    """The proposed value a person rejected on an item."""
    if (item.get("answer") or {}).get("decision") != "reject":
        return _NONE
    return (item.get("question") or {}).get("value", _NONE)


def _refused_value(item: Mapping[str, Any]) -> Any:
    """The value of an answer the graph refused to write: the item is open
    again for the person."""
    if not (item.get("effects") or {}).get("commit_error"):
        return _NONE
    answer = item.get("answer") or {}
    if answer.get("decision") == "correct":
        return answer.get("value", _NONE)
    if answer.get("decision") == "accept":
        value = answer.get("value")
        return value if value is not None else (item.get("question") or {}).get("value", _NONE)
    return _NONE


def _enum(value: Any) -> Any:
    return getattr(value, "value", value)


def _chars(text: str, n: int) -> str:
    """The first ``n`` characters (code points, as Rust counts them)."""
    return text[:n]


@dataclass
class _Instance:
    """The text of one instance as the workflow reads it."""

    label: str
    text: str
    box: dict[str, Any] | None


def create_workflow(lbb: LbbActivities, settings: Settings = DEFAULTS) -> TriggeredWorkflowDefinition[Any]:
    """The workflow, for a ``WorkflowWorker``. ``lbb`` is ``lbb_activities(client)``, or a
    mock in a test."""
    # The properties in name order, each with the name the property registry
    # gives its type, as LBB's values.extract sends them.
    names = sorted(settings.properties)

    def type_name(name: str) -> str:
        value_type = settings.properties[name]
        return "datetime" if value_type == "date_time" else value_type

    units = {name: unit for name in names if (unit := unit_of(settings, name))}
    schema = {
        "type": "object",
        "additionalProperties": False,
        "required": ["values"],
        "properties": {
            "values": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["property", "value", "unit", "quote"],
                    "properties": {
                        "property": {"type": "string", "enum": names},
                        "value": {
                            "type": "string",
                            "description": (
                                "The value in the property's type; a number as the text states it."
                            ),
                        },
                        "unit": {
                            "type": "string",
                            "description": (
                                "The unit the text states a number in, as a symbol; "
                                "empty when it states none or the value is not a number."
                            ),
                        },
                        "quote": {
                            "type": "string",
                            "description": (
                                "The words of the text that state the value, copied exactly."
                            ),
                        },
                    },
                },
            }
        },
    }

    def region(box: Mapping[str, str]) -> dict[str, Any] | None:
        """The instance's page box as a region, when it has a valid one."""
        try:
            page, x0, y0, x1, y1 = (float(box[b]) for b in _BOX)
        except (KeyError, ValueError):
            return None
        if not all(math.isfinite(v) for v in (page, x0, y0, x1, y1)):
            return None
        if page < 0 or not all(0 <= v <= 1 for v in (x0, y0, x1, y1)):
            return None
        if x0 > x1 or y0 > y1:
            return None
        return {"page": int(page), "x0": x0, "y0": y0, "x1": x1, "y1": y1, "confidence": 0}

    def source(iri: str, prop: str) -> dict[str, Any]:
        """The source entity of a property's value on an instance."""
        return {"type": "ValueSource", "name": prop, "key": f"value:{entity_id_from_iri(iri)}:{prop}"}

    def value_facts(iri: str, record: _Instance, v: Mapping[str, Any]) -> dict[str, Any]:
        """The facts of one value: the property on the instance, its source,
        and the edge with the quote."""
        support = v.get("support", 0)
        target = {**source(iri, v["property"]), "name": _chars(f"{v['property']} = {v['value']}", 200)}
        instance = entity_ref(iri, settings.class_name, record.label)
        box = (
            {**record.box, "confidence": min(1, max(0, support))} if record.box is not None else None
        )
        evidence: dict[str, Any] = {"text": v["quote"], "source_id": _chars(record.label, 200)}
        if box:
            evidence["region"] = box
        return {
            "on_instance": {**instance, "properties": {v["property"]: v["typed"]}},
            "on_source": {
                **target,
                "properties": {"property": v["property"], "value": v["value"], "support": support},
            },
            "edge": {
                "source": instance,
                "relation": "HAS_VALUE_SOURCE",
                "target": target,
                "confidence": support,
                "evidence": evidence,
            },
        }

    def reader(
        message: models.WorkflowTriggerMessage, iris: list[str]
    ) -> Callable[[], Awaitable[dict[str, _Instance]]]:
        """The label, text and page box of each instance of the turn, read once
        per attempt at the message's commit."""
        cache: dict[str, _Instance] | None = None

        async def texts() -> dict[str, _Instance]:
            nonlocal cache
            if cache is not None:
                return cache
            # An instance that is no entity of the graph is not read (LBB skips it too).
            plain = [iri for iri in iris if _PLAIN_IRI.fullmatch(iri) and _ENTITY_IRI.fullmatch(iri)]
            field_iri = f"{_P}{url_quote(settings.text_field.lower(), safe='')}"
            optional_box = "\n".join(f"  OPTIONAL {{ ?s <{_P}{b}> ?{b} }}" for b in _BOX)
            query = (
                "# values-extract: texts\n"
                f"SELECT ?s ?label ?text {' '.join(f'?{b}' for b in _BOX)} WHERE {{\n"
                f"  VALUES ?s {{ {' '.join(f'<{iri}>' for iri in plain)} }}\n"
                "  ?s a ?class .\n"
                f"  OPTIONAL {{ ?s <{_LABEL}> ?label }}\n"
                f"  OPTIONAL {{ ?s <{field_iri}> ?text }}\n"
                f"{optional_box}\n"
                "}"
            )
            rows = (await lbb.sparql(query, as_of_commit_seq=message.as_of_commit_seq)).rows()
            found: dict[str, dict[str, Any]] = {}
            for row in rows:
                at = found.setdefault(row["s"], {"labels": set(), "texts": set(), "box": {}})
                if row.get("label") and _normalize(row["label"]):
                    at["labels"].add(_normalize(row["label"]))
                if row.get("text") and _normalize(row["text"]):
                    at["texts"].add(_normalize(row["text"]))
                for b in _BOX:
                    if row.get(b) is not None:
                        at["box"].setdefault(b, row[b])
            out: dict[str, _Instance] = {}
            for iri, at in found.items():
                # As LBB composes a text from its fields: `<field>: <values>`,
                # each value's whitespace as single spaces, the values in order.
                values = sorted(at["texts"])
                text = (
                    _chars(f"{settings.text_field}: {', '.join(values)}", TEXT_CHARS)
                    if values
                    else ""
                )
                label = sorted(at["labels"])[0] if at["labels"] else entity_id_from_iri(iri)
                out[iri] = _Instance(label=label, text=text, box=region(at["box"]))
            cache = out
            return out

        return texts

    async def on_instances(
        ctx: WorkflowContext, message: models.WorkflowTriggerMessage, state: dict[str, int]
    ) -> WorkflowResult[dict[str, int]]:
        tally = dict(ZERO)
        # An instance that left the class keeps what was written for it; its
        # values go with its fields when it is retracted.
        tally["removed"] = len(message.removed)
        batch = message.instances[:TURN_INSTANCES]
        texts = reader(message, batch)
        reads: list[dict[str, Any]] = []
        for i, iri in enumerate(batch):

            async def read_values(s: WorkflowStepContext, i: int = i, iri: str = iri) -> dict[str, Any]:
                s.progress("reading values", done=i, total=len(batch), unit="instances")
                record = (await texts()).get(iri)
                if record is None:
                    return {"values": [], "missing": True}
                if not record.text.strip() or not names:
                    return {"values": []}
                prompt = _js_json(
                    {
                        "class": settings.class_name,
                        "instance": record.label,
                        "context": settings.context,
                        "properties": [
                            {
                                "name": name,
                                "type": type_name(name),
                                **({"unit": units[name]} if name in units else {}),
                            }
                            for name in names
                        ],
                        "text": record.text,
                    }
                )
                try:
                    answer = await lbb.reason(
                        prompt,
                        schema,
                        system=SYSTEM,
                        max_tokens=8_000,
                        effect_id=s.effect_id,
                        label=f"{_chars(record.label, 50)} values",
                    )
                except Exception as error:
                    if not is_activity_refused(error):
                        raise
                    return {"values": [], "failed": str(error)}
                proposed = (answer.value or {}).get("values") or []
                return {"values": check(proposed, settings.properties, record.text, units)}

            proposed = await ctx.step(f"values-{i}", read_values)

            async def read_support(
                s: WorkflowStepContext, i: int = i, iri: str = iri, proposed: dict[str, Any] = proposed
            ) -> dict[str, Any]:
                asked = [n for n, v in enumerate(proposed["values"]) if "typed" in v]
                if not asked:
                    return proposed
                s.progress("asking the decision model", done=i, total=len(batch), unit="instances")
                record = (await texts())[iri]
                questions = {}
                for n in asked:
                    v = proposed["values"][n]
                    questions[f"v{n}"] = {
                        "instructions": (
                            f"The state holds the text of one record of the class "
                            f"{settings.class_name}. A model read the value of a property from "
                            "the text and quoted the words that state it. How well does the "
                            "quote support the value?\n"
                            f"Property: {v['property']} ({type_name(v['property'])})\n"
                            f"Value: {v['value']}\n"
                            f'Quote: "{v["quote"]}"'
                        ),
                        "criteria": SUPPORT,
                    }
                try:
                    decided = await lbb.decide(
                        {
                            "class": settings.class_name,
                            "record": record.label,
                            "context": settings.context,
                            "text": record.text,
                        },
                        questions,
                        effect_id=s.effect_id,
                        label=f"{_chars(record.label, 50)} support",
                    )
                except Exception as error:
                    if not is_activity_refused(error):
                        raise
                    return {"values": [], "failed": str(error)}
                values = []
                for n, v in enumerate(proposed["values"]):
                    if "typed" not in v:
                        values.append(v)
                        continue
                    a = decided.answers.get(f"v{n}")
                    support: float | None = None
                    if a is not None and a.probabilities and "supported" in a.probabilities:
                        support = a.probabilities["supported"]
                    elif a is not None and a.choice is not None:
                        support = (
                            (a.confidence if a.confidence is not None else 1)
                            if a.choice == "supported"
                            else 0
                        )
                    # A value whose unit is not confirmed goes to review at most.
                    if support is None:
                        outcome = "weak_support"
                    elif support >= settings.gate and not v.get("unconfirmed"):
                        outcome = "written"
                    elif support >= SUPPORT_MIN:
                        outcome = "review"
                    else:
                        outcome = "weak_support"
                    out = {**v, "outcome": outcome}
                    if outcome == "review" and "detail" not in v and v.get("unconfirmed"):
                        out["detail"] = v["unconfirmed"]
                    if support is not None:
                        out["support"] = support
                    values.append(out)
                return {"values": values}

            reads.append(await ctx.step(f"support-{i}", read_support))

        # A person's answer wins over the model, for the evidence it was given
        # on. One read of the items of the turn's values: the value item of
        # each property of an instance, and the suggestion an earlier version
        # filed for each value.
        candidates = [
            (i, v)
            for i, read in enumerate(reads)
            for v in read["values"]
            if v["outcome"] in ("written", "review")
        ]

        def key_of(i: int, prop: str) -> str:
            return value_item_key(message.workflow, batch[i], prop)

        def legacy_key_of(i: int, v: Mapping[str, Any]) -> str:
            return legacy_value_key(message.workflow, batch[i], v)

        def dump(item: Any) -> dict[str, Any]:
            return dict(item.model_dump(mode="json", by_alias=True, exclude_none=True))

        async def read_answers(s: WorkflowStepContext) -> dict[str, Any]:
            items: dict[str, Any] = {}
            legacy: dict[str, Any] = {}
            if not candidates:
                return {"items": items, "legacy": legacy}
            keys = [
                key
                for i, v in candidates
                for key in (key_of(i, v["property"]), legacy_key_of(i, v))
            ]
            for item in await lbb.review_items(keys):
                if _enum(item.kind) == "value":
                    items[item.key] = dump(item)
                elif _enum(item.kind) == "change":
                    status = _enum(item.status)
                    decision = _enum(item.answer.decision) if item.answer else None
                    accepted = status == "answered" and decision == "accept"
                    # An accepted suggestion is written again with its facts.
                    filed = await lbb.review_item(item.key) if accepted else None
                    legacy[item.key] = {
                        "status": "accepted"
                        if accepted
                        else ("dismissed" if status == "answered" else status),
                        "facts": dump(filed.facts) if filed and filed.facts else None,
                    }
            return {"items": items, "legacy": legacy}

        known = await ctx.step("answers", read_answers)

        def decide(i: int, v: Mapping[str, Any]) -> dict[str, Any] | None:
            """What a person's answer decides for a value: ``write`` the
            answered value, ``hold`` the property as it is in the graph (and
            ``ask`` again), ``skip`` it, or write the ``facts`` of a suggestion
            an earlier version filed and a person accepted."""
            item = known["items"].get(key_of(i, v["property"]))
            if item is not None:
                # The same evidence, the value in force, or the value the item
                # proposed when it was answered (the person decided on it)
                # asks nothing.
                in_force = _value_in_force(item)
                if in_force is not None:
                    same = not item.get("evidence_hash") or item["evidence_hash"] == evidence_of(v)
                    decided = (item.get("question") or {}).get("value", _NONE) == v["typed"]
                    if same or decided or in_force["value"] == v["typed"]:
                        return {"kind": "write", **in_force}
                    return {"kind": "hold", "ask": True}
                # Asked again: hold, and update the question only for a value
                # it does not ask yet.
                if item.get("previous_answer"):
                    asks = (item.get("question") or {}).get("value", _NONE)
                    return {
                        "kind": "hold",
                        "ask": asks != v["typed"] and _rejected_value(item) != v["typed"],
                    }
                # A value a person rejected, or one the graph refused, is not
                # written again.
                if v["typed"] in (_rejected_value(item), _refused_value(item)):
                    return {"kind": "skip"}
                return None
            legacy = known["legacy"].get(legacy_key_of(i, v))
            if legacy is not None:
                if legacy["status"] == "accepted":
                    return {"kind": "facts", "facts": legacy["facts"]} if legacy["facts"] else {"kind": "skip"}
                if legacy["status"] == "dismissed":
                    return {"kind": "skip"}
                # An open suggestion stays the question.
                if legacy["status"] == "open" and v["outcome"] == "review":
                    return {"kind": "skip"}
            return None

        def asks(i: int, v: Mapping[str, Any]) -> bool:
            decision = decide(i, v)
            if decision is not None:
                return decision["kind"] == "hold" and bool(decision["ask"])
            return bool(v["outcome"] == "review")

        # The value items: one per property of an instance, for a value between
        # 0.5 and the gate, or new evidence against an answer in force.
        asking = [(i, v) for i, v in candidates if asks(i, v)]

        async def file_reviews(s: WorkflowStepContext) -> list[dict[str, Any]]:
            kept: list[dict[str, Any]] = []
            for n, (i, v) in enumerate(asking):
                s.progress("filing review items", done=n, total=len(asking), unit="items")
                iri = batch[i]
                record = (await texts())[iri]
                facts = value_facts(iri, record, v)
                unit = unit_of(settings, v["property"])
                support = v.get("support", 0)
                rationale = (
                    f"The unit is not confirmed: {v['unconfirmed']}. The text supports the "
                    f"value with {support:.2f}."
                    if v.get("unconfirmed")
                    else f"The text supports the value with {support:.2f}, below the gate "
                    f"{settings.gate}."
                )
                # The quote, with the instance's page box as its region.
                evidence: dict[str, Any] = {"quote": _chars(v["quote"], 600), "source_id": iri}
                if facts["edge"]["evidence"].get("region"):
                    evidence["region"] = facts["edge"]["evidence"]["region"]
                evidence["confidence"] = support
                item = await lbb.review(
                    {
                        "kind": "value",
                        "workflow": {"name": message.workflow},
                        "origin": {
                            "kind": "agent",
                            "id": f"starter:{message.workflow}",
                            "label": "values.extract",
                        },
                        "key": key_of(i, v["property"]),
                        "title": _chars(f"Set {v['property']} of {record.label} to {v['value']}", 200),
                        "rationale": rationale,
                        "subject": {"iri": iri, "type": settings.class_name, "name": record.label},
                        "question": {
                            "property": v["property"],
                            "value": v["typed"],
                            **({"unit": unit} if unit else {}),
                            "confidence": support,
                        },
                        "evidence": [evidence],
                        "evidence_hash": evidence_of(v),
                        "facts": {
                            "triplets": [facts["edge"]],
                            "entity_properties": [facts["on_instance"], facts["on_source"]],
                            "summary": _chars(f"{v['property']} = {v['value']}", 500),
                        },
                    }
                )
                if decide(i, v) is not None:
                    continue  # asked again: the answered value holds
                # Answered already (a person answered between the read and now).
                in_force = _value_in_force(dump(item))
                if in_force is not None:
                    kept.append({"i": i, "property": v["property"], **in_force})
            return kept

        answered = await ctx.step("review", file_reviews)

        def answered_as(v: Mapping[str, Any], answer: Mapping[str, Any]) -> dict[str, Any]:
            """A value as a person answered it: a correction in place of the
            read value, with support 1."""
            out = {**v, "typed": answer["value"], "value": _text(answer["value"])}
            if answer["corrected"]:
                out["support"] = 1
            return out

        def plan(i: int, record: _Instance) -> tuple[list[Any], list[Any], set[str]]:
            """What the write sets on instance ``i``: its entity properties,
            its triplets and the properties it writes."""
            entity_properties: list[Any] = []
            triplets: list[Any] = []
            written: set[str] = set()
            for v in reads[i]["values"]:
                if v["outcome"] not in ("written", "review"):
                    continue
                decision = decide(i, v)
                if decision is not None and decision["kind"] == "facts":
                    entity_properties.extend(decision["facts"].get("entity_properties", []))
                    triplets.extend(decision["facts"].get("triplets", []))
                    written.add(v["property"])
                    continue
                if decision is not None and decision["kind"] == "write":
                    kept: Mapping[str, Any] | None = answered_as(v, decision)
                elif decision is not None:
                    kept = None
                else:
                    answer = next(
                        (a for a in answered if a["i"] == i and a["property"] == v["property"]),
                        None,
                    )
                    if answer is not None:
                        kept = answered_as(v, answer)
                    else:
                        kept = v if v["outcome"] == "written" else None
                if kept is None:
                    continue
                facts = value_facts(batch[i], record, kept)
                entity_properties.extend([facts["on_instance"], facts["on_source"]])
                triplets.append(facts["edge"])
                written.add(v["property"])
            return entity_properties, triplets, written

        def held_of(i: int) -> set[str]:
            """The properties of instance ``i`` whose answered value stays as it
            is in the graph."""
            return {
                v["property"]
                for v in reads[i]["values"]
                if v["outcome"] in ("written", "review")
                and (decision := decide(i, v)) is not None
                and decision["kind"] == "hold"
            }

        async def owned_write(indexes: list[int], held: set[str]) -> OwnedWrite:
            """One owned write of instances; a held property is neither set nor
            replaced."""
            records = await texts()
            subjects: list[dict[str, Any]] = []
            entity_properties: list[Any] = []
            triplets: list[Any] = []
            retract: list[dict[str, Any]] = []
            for i in indexes:
                iri = batch[i]
                record = records.get(iri)
                if record is None:
                    continue
                subjects.append(entity_ref(iri, settings.class_name, record.label))
                props, edges, written = plan(i, record)
                entity_properties.extend(props)
                triplets.extend(edges)
                retract.extend(source(iri, p) for p in names if p not in written and p not in held)
            return OwnedWrite(
                subjects=subjects,
                # A held property keeps its source link: an instance that holds
                # one does not replace its links (a source it no longer has is
                # retracted whole, with its link).
                relations=[] if held else ["HAS_VALUE_SOURCE"],
                properties=[p for p in names if p not in held],
                entity_properties=entity_properties,
                triplets=triplets,
                retract_entities=retract,
            )

        failed = [
            FailedInstance(instance=iri, error=reads[i]["failed"])
            for i, iri in enumerate(batch)
            if "failed" in reads[i]
        ]
        writable = [
            i for i in range(len(batch)) if not reads[i].get("missing") and "failed" not in reads[i]
        ]
        holding = [i for i in writable if held_of(i)]

        async def plan_write(s: WorkflowStepContext) -> OwnedWrite:
            s.progress("writing facts", done=0, total=len(batch), unit="instances")
            return await owned_write([i for i in writable if i not in holding], set())

        writes = [await lbb.write_owned(ctx, "write", plan_write)]
        # An instance that holds an answered value is written on its own, so
        # the held property stays out of its replace.
        for i in holding:

            async def plan_held(s: WorkflowStepContext, i: int = i) -> OwnedWrite:
                return await owned_write([i], held_of(i))

            writes.append(await lbb.write_owned(ctx, f"write-held-{i}", plan_held))
        # Refused instances go back to the watch; the turn completes.
        await lbb.requeue(ctx, "requeue", failed, graph=message.graph)

        for read in reads:
            if read.get("missing"):
                continue
            tally["instances"] += 1
            if "failed" in read:
                tally["failed"] += 1
            for v in read["values"]:
                tally["values_proposed"] += 1
                if v["outcome"]:
                    tally[v["outcome"]] += 1
        for write in writes:
            if write.commit_seq is not None and not write.replayed:
                tally["commits"] += 1
        after = {key: state.get(key, 0) + tally[key] for key in ZERO}
        result = {"counters": tally}
        # More instances than a turn reads: the rest goes to a new turn.
        rest = message.instances[TURN_INSTANCES:]
        if rest:
            more = message.model_copy(update={"instances": rest, "removed": []})
            return ctx.continue_(after, more, result=result)
        return WorkflowResult(state=after, result=result)

    return triggered_workflow(
        name=WORKFLOW_TYPE["workflow_type"],
        version=WORKFLOW_TYPE["version"],
        initial_state=ZERO,
        on_instances=on_instances,
    )
