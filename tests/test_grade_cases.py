"""The cases every grader of extracted values runs
(``sdk/templates/_evals/grade-cases.json``): the Python grader must give each
item's counts and grades, and each run's grades, as ``grading.ts`` and
lbb-server's experiment score give them."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from value_grading import grade, grade_item  # isort: skip


def _fixture() -> Path | None:
    """The shared cases: in the monorepo under ``sdk/templates/_evals``, in the
    public repo copied to ``conformance/`` by the export."""
    for parent in Path(__file__).resolve().parents:
        for candidate in (
            parent / "templates" / "_evals" / "grade-cases.json",
            parent / "conformance" / "grade-cases.json",
        ):
            if candidate.is_file():
                return candidate
    return None


CASES = _fixture()
pytestmark = pytest.mark.skipif(
    CASES is None, reason="the shared grader cases are not in this tree"
)


def _cases() -> dict[str, Any]:
    assert CASES is not None
    cases: dict[str, Any] = json.loads(CASES.read_text())
    return cases


def test_every_item_case_gives_its_counts_and_grades() -> None:
    items = _cases()["items"]
    assert items
    for item in items:
        labels = {"values": item["values"], "leave_out": item.get("leave_out") or []}
        got = grade_item(labels, item["written"], item.get("review"), item.get("failed", False))
        assert got == item["expect"], item["case"]


def test_every_run_case_gives_its_grades() -> None:
    runs = _cases()["runs"]
    assert runs
    for run in runs:
        got = grade(
            run["key"],
            run["written"],
            run["reviewItems"],
            run["proposed"],
            run["cost"],
            run["instances"],
        )
        assert got == run["expect"], run["case"]
