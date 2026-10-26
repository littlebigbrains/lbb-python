"""The grades of extracted values, as ``sdk/templates/_evals/grading.ts``
gives them.

Every grader of extracted values follows the same rules (that file, this
one and lbb-server's experiment score), and
``sdk/templates/_evals/grade-cases.json`` holds their cases
(``test_grade_cases.py``; ``docs/architecture/experiments.md``, Scoring): a
number matches by value; a text matches the value or one of ``accept``,
trimmed and in lower case; a value of a property the labels do not name is
unlabeled and counts in no grade; labels of one property in ``values`` are
alternatives; a null in ``values`` labels nothing, and a null in
``leave_out`` means no value at all.
"""

from __future__ import annotations

import re
from typing import Any

_DECIMAL = re.compile(r"[+-]?([0-9]+\.?[0-9]*|\.[0-9]+)([eE][+-]?[0-9]+)?")


def _round(value: float) -> float:
    return round(value, 4)


def _text(value: Any) -> str:
    # The spelling of a JSON value, as the other graders give it.
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return "null"
    return str(value).strip().lower()


def _number(got: Any) -> float | None:
    """A written number, or a text that is a decimal number; else ``None``."""
    if isinstance(got, bool):
        return None
    if isinstance(got, (int, float)):
        return float(got)
    if isinstance(got, str) and _DECIMAL.fullmatch(got.strip()):
        return float(got.strip())
    return None


def matches(got: Any, want: dict[str, Any]) -> bool:
    """Whether a written value is the label's value."""
    value = want["value"]
    if value is None:
        return False
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return _number(got) == value
    spelled = _text(got)
    return _text(value) == spelled or any(_text(a) == spelled for a in want.get("accept") or [])


def _judge(labels: dict[str, Any], prop: str, got: Any) -> str:
    wanted = [
        w for w in labels.get("values") or [] if w["property"] == prop and w["value"] is not None
    ]
    left = [w for w in labels.get("leave_out") or [] if w["property"] == prop]
    if not wanted and not left:
        return "unlabeled"
    if any(w["value"] is None or matches(got, w) for w in left):
        return "wrong"
    return "correct" if any(matches(got, w) for w in wanted) else "wrong"


def _expected(labels: dict[str, Any]) -> list[str]:
    seen: list[str] = []
    for w in labels.get("values") or []:
        if w["value"] is not None and w["property"] not in seen:
            seen.append(w["property"])
    return seen


def grade_item(
    labels: dict[str, Any],
    written: dict[str, Any],
    review: dict[str, Any] | None = None,
    failed: bool = False,
) -> dict[str, Any]:
    """The grade of one item: its counts, precision, recall and score (F1)."""
    expected = _expected(labels)
    if failed:
        return {
            "correct": 0,
            "wrong": 0,
            "missed": len(expected),
            "unlabeled": 0,
            "review": 0,
            "precision": 0,
            "recall": 0,
            "score": 0,
        }
    correct = wrong = unlabeled = 0
    right: set[str] = set()
    for prop, value in written.items():
        verdict = _judge(labels, prop, value)
        if verdict == "correct":
            correct += 1
            right.add(prop)
        elif verdict == "wrong":
            wrong += 1
        else:
            unlabeled += 1
    precision = correct / (correct + wrong) if correct + wrong else 1
    recall = correct / len(expected) if expected else 1
    score = 2 * precision * recall / (precision + recall) if precision + recall else 0
    return {
        "correct": correct,
        "wrong": wrong,
        "missed": sum(prop not in right for prop in expected),
        "unlabeled": unlabeled,
        "review": len(review or {}),
        "precision": _round(precision),
        "recall": _round(recall),
        "score": _round(score),
    }


def grade(
    key: list[dict[str, Any]],
    written: dict[str, dict[str, Any]],
    review_items: int,
    proposed: int,
    cost: float,
    instances: int,
) -> dict[str, Any]:
    """The grades of the values a run wrote, against an answer key of labels per record."""
    written_n = correct = expected = 0
    for record in key:
        item = grade_item(record, written.get(record["name"], {}))
        written_n += item["correct"] + item["wrong"]
        correct += item["correct"]
        expected += len(_expected(record))
    return {
        "precision": _round(correct / written_n) if written_n else 1,
        "recall": _round(correct / expected) if expected else 1,
        "review_share": _round(review_items / proposed) if proposed else 0,
        "cost_per_instance_micro_usd": _round(cost / instances) if instances else 0,
        "written": written_n,
        "correct": correct,
        "expected": expected,
    }
