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
   the instance has one), and files a review item for each value between 0.5
   and the gate. A value below 0.5 is dropped.

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
        out.append(checked)
    return out


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

        # The review items: one per value between 0.5 and the gate, keyed by
        # its instance, property and value. An accepted one is written again.
        below = [
            (i, v) for i, read in enumerate(reads) for v in read["values"] if v["outcome"] == "review"
        ]

        async def file_reviews(s: WorkflowStepContext) -> list[dict[str, Any]]:
            kept: list[dict[str, Any]] = []
            for n, (i, v) in enumerate(below):
                s.progress("filing review items", done=n, total=len(below), unit="items")
                iri = batch[i]
                record = (await texts())[iri]
                facts = value_facts(iri, record, v)
                support = v.get("support", 0)
                key_hash = stable_hash_hex([entity_id_from_iri(iri), v["property"], v["value"]])
                rationale = (
                    f"The unit is not confirmed: {v['unconfirmed']}. The text supports the "
                    f'value with {support:.2f}: "{_chars(v["quote"], 600)}"'
                    if v.get("unconfirmed")
                    else f"The text supports the value with {support:.2f}, below the gate "
                    f'{settings.gate}: "{_chars(v["quote"], 600)}"'
                )
                filed = await lbb.review(
                    {
                        "key": f"starter:{message.workflow}:value:{key_hash[:24]}",
                        "title": _chars(f"Set {v['property']} of {record.label} to {v['value']}", 200),
                        "rationale": rationale,
                        "origin": {
                            "kind": "agent",
                            "id": f"starter:{message.workflow}",
                            "label": "values.extract",
                        },
                        "facts": {
                            "triplets": [facts["edge"]],
                            "entity_properties": [facts["on_instance"], facts["on_source"]],
                            "summary": _chars(f"{v['property']} = {v['value']}", 500),
                        },
                        "evidence": {
                            "records": 1,
                            "samples": [
                                {
                                    "entity": iri,
                                    "quote": _chars(v["quote"], 600),
                                    "support": v.get("support"),
                                }
                            ],
                        },
                    },
                    workflow=message.workflow,
                )
                if filed.status.value == "accepted" and filed.facts is not None:
                    kept.append(
                        {
                            "i": i,
                            "property": v["property"],
                            "facts": filed.facts.model_dump(
                                mode="json", by_alias=True, exclude_none=True
                            ),
                        }
                    )
            return kept

        accepted = await ctx.step("review", file_reviews)

        failed = [
            FailedInstance(instance=iri, error=reads[i]["failed"])
            for i, iri in enumerate(batch)
            if "failed" in reads[i]
        ]

        async def plan_write(s: WorkflowStepContext) -> OwnedWrite:
            s.progress("writing facts", done=0, total=len(batch), unit="instances")
            records = await texts()
            subjects: list[dict[str, Any]] = []
            entity_properties: list[dict[str, Any]] = []
            triplets: list[dict[str, Any]] = []
            retract: list[dict[str, Any]] = []
            for i, iri in enumerate(batch):
                read = reads[i]
                record = records.get(iri)
                if read.get("missing") or "failed" in read or record is None:
                    continue
                subjects.append(entity_ref(iri, settings.class_name, record.label))
                written: set[str] = set()
                for v in read["values"]:
                    if v["outcome"] != "written":
                        continue
                    facts = value_facts(iri, record, v)
                    entity_properties.extend([facts["on_instance"], facts["on_source"]])
                    triplets.append(facts["edge"])
                    written.add(v["property"])
                for a in accepted:
                    if a["i"] != i:
                        continue
                    entity_properties.extend(a["facts"].get("entity_properties", []))
                    triplets.extend(a["facts"].get("triplets", []))
                    written.add(a["property"])
                retract.extend(source(iri, p) for p in names if p not in written)
            return OwnedWrite(
                subjects=subjects,
                relations=["HAS_VALUE_SOURCE"],
                properties=names,
                entity_properties=entity_properties,
                triplets=triplets,
                retract_entities=retract,
            )

        write = await lbb.write_owned(ctx, "write", plan_write)
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
