"""Run results: what a turn did with each input, in the shape a workflow test
gives its inputs.

The worker fills them from three calls, without new calls in a workflow's
code: ``write_owned`` (each subject is ``written``, or ``no_value`` when the
write sets nothing on it), ``review`` with a subject (``review``, with the
item's id) and ``requeue`` (``failed``, with its error). It sends them with
the turn's completion or failure (``results``) when the claim names
``results_max``, cut to 512 KiB. A step keeps what it reported in its saved
output, so a replay reports it again: as it was in the same run (a resume),
or as ``duplicate`` writes of an earlier attempt. A capture
(``capture_activities``) derives them the same way, so a local test and a live
run show the same records. The TypeScript worker does the same, byte for byte
(``sdk/typescript/src/run-results.ts``).
Design: ``docs/architecture/run-results.md``.
"""

from __future__ import annotations

import json
import math
from collections.abc import Iterable, Mapping, Sequence
from decimal import Decimal
from typing import Any

#: Inputs a run keeps whole, at most, when the claim names no bound.
WORKFLOW_RUN_RESULTS_MAX = 200
#: Values one input keeps, at most.
WORKFLOW_RUN_RESULT_VALUES_MAX = 32
#: Bytes of a report, at most (UTF-8 of its JSON): the server takes at most
#: 1 MiB for a whole completion, so the results never cost the completion.
WORKFLOW_RUN_RESULTS_MAX_BYTES = 512 * 1024
_QUOTE_MAX = 600
_TEXT_MAX = 512
_ERROR_MAX = 500
_IRI_MAX = 2048
_OUTCOME_MAX = 64
#: Bytes of the results a step keeps in its saved output, at most.
_STEP_RESULTS_MAX_BYTES = 32 * 1024
#: Bytes of a step's saved output, at most (the engine keeps 64 KiB).
_STEP_OUTPUT_MAX_BYTES = 60 * 1024
#: The marker of a step output that keeps the step's results.
_STEP_MARKER = "lbb_step_results"
_IRI_NAMESPACE = "https://littlebigbrain.com/e/"
_ID_KEY_SENTINEL = "\u0000lbb-eid:"

_RANK = {"no_value": 0, "duplicate": 1, "written": 2, "review": 3, "failed": 4}
_OUTCOMES = list(_RANK)


# ---- JSON as the TypeScript worker writes it ---------------------------------------


def _js_number(number: float) -> str:
    """A float as JavaScript writes it (``Number.prototype.toString``): ``60``
    for 60.0, ``1e-7`` for 1e-07. Python and JavaScript both give the shortest
    digits that read back as the same float, so only the layout differs."""
    if math.isnan(number) or math.isinf(number):
        return "null"
    if number == 0:
        return "0"
    sign = "-" if number < 0 else ""
    _, digit_tuple, exponent = Decimal(repr(abs(number))).normalize().as_tuple()
    digits = "".join(str(digit) for digit in digit_tuple)
    k = len(digits)
    n = k + int(exponent)
    if k <= n <= 21:
        text = digits + "0" * (n - k)
    elif 0 < n <= 21:
        text = f"{digits[:n]}.{digits[n:]}"
    elif -6 < n <= 0:
        text = "0." + "0" * (-n) + digits
    else:
        e = n - 1
        mantissa = digits[0] + (f".{digits[1:]}" if k > 1 else "")
        text = f"{mantissa}e{'+' if e >= 0 else '-'}{abs(e)}"
    return sign + text


def _js_json(value: Any) -> str:
    """``value`` as JavaScript's ``JSON.stringify`` writes it: compact, text
    unescaped past ASCII, numbers in JavaScript's layout."""
    if value is None:
        return "null"
    if value is True:
        return "true"
    if value is False:
        return "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return _js_number(value)
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, Mapping):
        return (
            "{"
            + ",".join(
                f"{json.dumps(str(key), ensure_ascii=False)}:{_js_json(item)}"
                for key, item in value.items()
            )
            + "}"
        )
    if isinstance(value, (list, tuple)):
        return "[" + ",".join(_js_json(item) for item in value) + "]"
    return _js_json(_plain(value))


def json_bytes(value: Any) -> int:
    """The size of a value as the server reads it: UTF-8 of its JSON, as the
    TypeScript worker measures it."""
    return len(_js_json(value).encode("utf-8"))


# ---- inputs ---------------------------------------------------------------------------


def _clip(text: str, limit: int) -> str:
    return text[:limit]


def _key_of(item: Mapping[str, Any]) -> str:
    """What names an input within a run: its IRI, else its label."""
    return str(item.get("iri") or item.get("label") or "")


def _clipped(out: dict[str, Any], name: str, limit: int) -> None:
    """Cut an optional text of ``out`` to ``limit``, or leave it out when it
    is empty or absent."""
    text = out.get(name)
    if text:
        out[name] = _clip(str(text), limit)
    else:
        out.pop(name, None)


def _fit(item: Mapping[str, Any]) -> dict[str, Any]:
    """An input cut to its bounds: its values, and the length of each text."""
    values = []
    for raw in (item.get("values") or [])[:WORKFLOW_RUN_RESULT_VALUES_MAX]:
        value = dict(raw)
        value["property"] = _clip(str(value.get("property", "")), _TEXT_MAX)
        value["value"] = _clip(str(value.get("value", "")), _TEXT_MAX)
        value["quote"] = _clip(str(value.get("quote", "")), _QUOTE_MAX)
        value["outcome"] = _clip(str(value.get("outcome", "")), _OUTCOME_MAX)
        for name in ("detail", "stated", "unconfirmed", "source", "review_item"):
            _clipped(value, name, _TEXT_MAX)
        values.append(value)
    out = dict(item)
    _clipped(out, "iri", _IRI_MAX)
    out["label"] = _clip(str(item.get("label") or ""), _TEXT_MAX)
    out["values"] = values
    _clipped(out, "error", _ERROR_MAX)
    return out


def _value_key(value: Mapping[str, Any]) -> str:
    """A value's identity for the merge: the same value reported twice is one."""
    return _js_json(value)


def merge_inputs(a: Mapping[str, Any], b: Mapping[str, Any]) -> dict[str, Any]:
    """Two reports of one input, as one: the outcome that asks more of a
    person, the values of both (each once, at most 32), the first label, page
    box and error, and the sum of the costs."""
    values = list(a.get("values") or [])
    seen = {_value_key(value) for value in values}
    for value in b.get("values") or []:
        if len(values) >= WORKFLOW_RUN_RESULT_VALUES_MAX:
            break
        key = _value_key(value)
        if key not in seen:
            seen.add(key)
            values.append(value)
    out = dict(a)
    if _RANK[b["outcome"]] > _RANK[a["outcome"]]:
        out["outcome"] = b["outcome"]
    out["label"] = a.get("label") or b.get("label") or ""
    out["values"] = values
    for name in ("region", "error"):
        if out.get(name) is None and b.get(name) is not None:
            out[name] = b[name]
    cost = (a.get("cost_micro_usd") or 0) + (b.get("cost_micro_usd") or 0)
    if cost:
        out["cost_micro_usd"] = cost
    return _fit(out)


def fit_report(report: Mapping[str, Any]) -> dict[str, Any]:
    """A report cut to 512 KiB, as the server cuts its stored results: while
    it is larger, the values of the last half of the inputs that still have
    them go, then the last half of the inputs. The counts stay whole."""
    inputs = [_fit(item) for item in report.get("inputs") or []]
    out = {**report, "inputs": inputs}
    cut = len(inputs)
    while json_bytes(out) > WORKFLOW_RUN_RESULTS_MAX_BYTES and inputs:
        if cut > 0:
            start = cut // 2
            for at in range(start, len(inputs)):
                inputs[at] = {**inputs[at], "values": []}
            cut = start
        else:
            del inputs[len(inputs) // 2 :]
    return out


class RunResultsCollector:
    """The inputs of one run as it goes: each input once (a second report
    merges into the first), in the order the run first named them. Past
    ``limit`` inputs it keeps only each input's outcome, for the counts."""

    def __init__(self, limit: int = WORKFLOW_RUN_RESULTS_MAX) -> None:
        self.limit = limit
        self._kept: list[dict[str, Any]] = []
        self._positions: dict[str, int] = {}
        self._rest: dict[str, str] = {}
        # What the step that runs now reported, for its saved output.
        self._step_log: list[dict[str, Any]] | None = None

    def begin_step(self) -> None:
        """A step starts: what it reports is kept for its saved output too."""
        self._step_log = []

    def end_step(self) -> list[dict[str, Any]]:
        """The step ended: what it reported, in order."""
        log = self._step_log or []
        self._step_log = None
        return log

    def add(self, item: Mapping[str, Any]) -> None:
        if self._step_log is not None:
            self._step_log.append(dict(item))
        key = _key_of(item)
        if key in self._positions:
            at = self._positions[key]
            self._kept[at] = merge_inputs(self._kept[at], item)
        elif key in self._rest:
            if _RANK[item["outcome"]] > _RANK[self._rest[key]]:
                self._rest[key] = item["outcome"]
        elif len(self._kept) < self.limit:
            self._positions[key] = len(self._kept)
            self._kept.append(_fit(item))
        else:
            self._rest[key] = item["outcome"]

    def has(self, key: str) -> bool:
        return key in self._positions or key in self._rest

    def __len__(self) -> int:
        return len(self._kept) + len(self._rest)

    def inputs(self) -> list[dict[str, Any]]:
        return [dict(item) for item in self._kept]

    def report(self) -> dict[str, Any] | None:
        """The report a completion sends, or ``None`` when nothing was
        reported: the inputs in order and the counts of every input, at most
        512 KiB."""
        if not len(self):
            return None
        counts = dict.fromkeys(_OUTCOMES, 0)
        for item in self._kept:
            counts[item["outcome"]] += 1
        for outcome in self._rest.values():
            counts[outcome] += 1
        return fit_report({"inputs": self.inputs(), "counts": counts})


# ---- the saved output of a step -------------------------------------------------------


def step_output_with_results(value: Any, run: str, inputs: Sequence[Mapping[str, Any]]) -> Any:
    """The output a step saves: its value as it is, or, when the step reported
    inputs, its value with those inputs and the run that reported them
    (``{lbb_step_results: 1, run, results, value}``), so a replay of the step
    reports them again. The inputs keep their values while they fit in 32 KiB,
    then only their IRI, label and outcome; the whole output stays under the
    engine's 64 KiB. Text with a lone surrogate has no UTF-8 size: then the
    step saves its value alone."""
    if not inputs:
        return value
    try:
        results = _step_results(value, inputs)
    except UnicodeEncodeError:
        return value
    if not results:
        return value
    return {_STEP_MARKER: 1, "run": run, "results": results, "value": value}


def _step_results(value: Any, inputs: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """The inputs a step's saved output keeps beside ``value``."""
    budget = min(_STEP_RESULTS_MAX_BYTES, _STEP_OUTPUT_MAX_BYTES - json_bytes(value))
    results: list[dict[str, Any]] = []
    size = 2
    for item in inputs:
        whole = _fit(item)
        brief: dict[str, Any] = {}
        if whole.get("iri"):
            brief["iri"] = whole["iri"]
        brief["label"] = whole.get("label") or ""
        brief["outcome"] = whole["outcome"]
        grown = json_bytes(whole) + 1
        if size + grown <= budget:
            results.append(whole)
            size += grown
            continue
        small = json_bytes(brief) + 1
        if size + small > budget:
            break
        results.append(brief)
        size += small
    return results


def step_output_results(saved: Any) -> tuple[Any, str | None, list[dict[str, Any]]]:
    """A saved step output read back: its value, the run that reported its
    inputs, and those inputs (none for an output without them)."""
    if (
        isinstance(saved, Mapping)
        and saved.get(_STEP_MARKER) == 1
        and len(saved) == 4
        and "value" in saved
        and isinstance(saved.get("run"), str)
        and isinstance(saved.get("results"), list)
    ):
        results = [dict(item) for item in saved["results"] if isinstance(item, Mapping)]
        return saved["value"], saved["run"], results
    return saved, None, []


# ---- what the calls report ------------------------------------------------------------


def _value_text(value: Any) -> str:
    """A property value as text, as the TypeScript worker writes it: a number,
    a word, or the items of a list."""
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float):
        return _js_number(value)
    if isinstance(value, (list, tuple)):
        return ", ".join(_value_text(item) for item in value)
    if isinstance(value, Mapping):
        # A typed value (`{"f64": 1.5}`) is its one entry.
        if len(value) == 1:
            return _value_text(next(iter(value.values())))
        return _js_json(value)
    return str(value)


def _plain(value: Any) -> Any:
    """A model or a mapping as plain JSON data."""
    dump = getattr(value, "model_dump", None)
    if callable(dump):
        return dump(mode="json", by_alias=True, exclude_none=True)
    return value


def _iri_of(entity: Mapping[str, Any]) -> str | None:
    """The IRI a commit's entity reference names, when it names one by id."""
    if entity.get("entity_id"):
        return f"{_IRI_NAMESPACE}{entity['entity_id']}"
    key = entity.get("key")
    if isinstance(key, str) and key.startswith(_ID_KEY_SENTINEL):
        return f"{_IRI_NAMESPACE}{key[len(_ID_KEY_SENTINEL) :]}"
    return None


def _same_entity(a: Mapping[str, Any], b: Mapping[str, Any]) -> bool:
    ia, ib = _iri_of(a), _iri_of(b)
    if ia or ib:
        return ia == ib
    if a.get("key") or b.get("key"):
        return a.get("type") == b.get("type") and a.get("key") == b.get("key")
    return a.get("type") == b.get("type") and a.get("name") == b.get("name")


def _fields_of(properties: Any) -> list[tuple[str, Any]]:
    if isinstance(properties, list):
        return [
            (str(item["field"]), item.get("value"))
            for item in properties
            if isinstance(item, Mapping) and "field" in item
        ]
    if isinstance(properties, Mapping):
        return list(properties.items())
    return []


def _number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _region_of(region: Any) -> dict[str, Any] | None:
    region = _plain(region)
    if not isinstance(region, Mapping):
        return None
    names = ("page", "x0", "y0", "x1", "y1")
    if not all(_number(region.get(name)) for name in names):
        return None
    return {name: region[name] for name in names}


def _value(property_: str, text: str, outcome: str, **more: Any) -> dict[str, Any]:
    value: dict[str, Any] = {"property": property_, "value": text, "quote": "", "outcome": outcome}
    value.update({name: item for name, item in more.items() if item is not None and item != ""})
    return value


def owned_write_inputs(body: Any, outcome: str) -> list[dict[str, Any]]:
    """The inputs of an owned write's commit body: each subject ``outcome``
    (``written``, or ``duplicate`` when an earlier attempt wrote it), with the
    fields the write sets on it and its out-edges (an edge's evidence gives
    the quote, the support, the source and the page box). A subject the write
    sets nothing on has no value."""
    data = _plain(body)
    replace = data.get("replace_owned") or {}
    subjects = replace.get("subjects") or []
    out: list[dict[str, Any]] = []
    for subject in subjects:
        values: list[dict[str, Any]] = []
        for fields in data.get("entity_properties") or []:
            if _same_entity(fields, subject):
                for field, field_value in _fields_of(fields.get("properties")):
                    values.append(_value(field, _value_text(field_value), outcome))
        for triplet in data.get("triplets") or []:
            if not _same_entity(triplet.get("source") or {}, subject):
                continue
            evidence = triplet.get("evidence")
            quote = evidence if isinstance(evidence, str) else ""
            more: dict[str, Any] = {}
            if isinstance(evidence, Mapping):
                quote = evidence.get("text") or ""
            if _number(triplet.get("confidence")):
                more["support"] = triplet["confidence"]
            if isinstance(evidence, Mapping):
                more["source"] = evidence.get("source_id")
                more["region"] = _region_of(evidence.get("region"))
            target = triplet.get("target") or {}
            values.append(
                _value(
                    triplet.get("relation", ""),
                    str(target.get("name", "")),
                    outcome,
                    quote=quote,
                    **more,
                )
            )
        item: dict[str, Any] = {}
        iri = _iri_of(subject)
        if iri:
            item["iri"] = iri
        item["label"] = subject.get("name") or ""
        item["outcome"] = outcome if values else "no_value"
        item["values"] = values
        out.append(_fit(item))
    return out


def review_input(request: Any, item: Any) -> dict[str, Any] | None:
    """The input of a review item filed about a subject: ``review``, with the
    question's property and value, the first evidence's quote and place, and
    the item's id. An item answered already is ``answered``: the write that
    follows reports the input. ``None`` for an item without a subject."""
    data = _plain(request)
    if not isinstance(data, Mapping):
        return None
    subject = data.get("subject") or {}
    if not subject.get("iri") and not subject.get("name"):
        return None
    filed = _plain(item) if item is not None else {}
    status = filed.get("status") if isinstance(filed, Mapping) else None
    answered = status is not None and status != "open"
    evidence = (data.get("evidence") or [{}])[0] or {}
    question = data.get("question") or {}
    support = question.get("confidence")
    if support is None:
        support = evidence.get("confidence")
    asked = question.get("value")
    if asked is None:
        asked = data.get("title") or ""
    result: dict[str, Any] = {}
    if subject.get("iri"):
        result["iri"] = subject["iri"]
    result["label"] = subject.get("name") or ""
    result["outcome"] = "no_value" if answered else "review"
    result["values"] = [
        _value(
            question.get("property") or data.get("kind") or "review",
            _value_text(asked),
            "answered" if answered else "review",
            quote=evidence.get("quote") or "",
            support=support if _number(support) else None,
            review_item=filed.get("id") if isinstance(filed, Mapping) else None,
            source=evidence.get("source_id"),
            region=_region_of(evidence.get("region")),
        )
    ]
    return _fit(result)


def requeued_inputs(failed: Iterable[Any]) -> list[dict[str, Any]]:
    """The inputs a turn requeued: each ``failed``, with its error."""
    out = []
    for item in failed:
        instance = getattr(item, "instance", None) or (
            item.get("instance") if isinstance(item, Mapping) else None
        )
        error = getattr(item, "error", None) or (
            item.get("error") if isinstance(item, Mapping) else None
        )
        entry: dict[str, Any] = {"iri": instance, "label": "", "outcome": "failed", "values": []}
        if error:
            entry["error"] = error
        out.append(_fit(entry))
    return out


def replayed_inputs(inputs: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """The inputs a step saved, reported again by another run (a later
    attempt replays the step): what it wrote is the earlier attempt's, so each
    written input is ``duplicate``."""
    out = []
    for item in inputs:
        entry = dict(item)
        if entry.get("outcome") == "written":
            entry["outcome"] = "duplicate"
            entry["values"] = [
                {**value, "outcome": "duplicate"} if value.get("outcome") == "written" else value
                for value in entry.get("values") or []
            ]
        out.append(entry)
    return out


def message_instances(message: Any) -> list[str]:
    """The instances a trigger message (or a continuation's message) names."""
    instances = message.get("instances") if isinstance(message, Mapping) else None
    if not isinstance(instances, list):
        return []
    return [iri for iri in instances if isinstance(iri, str)]
