"""Run results as the Python worker builds them: measured, cut and saved as
the TypeScript worker does (``sdk/typescript/src/run-results.test.ts`` pins
the same numbers)."""

from __future__ import annotations

from typing import Any

from lbb.workflows._results import (
    WORKFLOW_RUN_RESULTS_MAX_BYTES,
    _js_json,
    fit_report,
    json_bytes,
    step_output_results,
    step_output_with_results,
)


def test_a_report_is_measured_as_utf8_of_its_json_as_the_typescript_worker_measures_it() -> None:
    value = {
        "a": 60.0,
        "b": "Größe – Küche",
        "c": 1e-7,
        "d": 0.5,
        "e": [1e21, 123456789012345680000.0, -0.000001],
        "f": None,
        "g": True,
    }
    assert _js_json(value) == (
        '{"a":60,"b":"Größe – Küche","c":1e-7,"d":0.5,'
        '"e":[1e+21,123456789012345680000,-0.000001],"f":null,"g":true}'
    )
    assert json_bytes(value) == 112


def _inputs(count: int, chars: int) -> list[dict[str, Any]]:
    """``count`` inputs with 32 values of ``chars`` characters each."""
    return [
        {
            "label": f"n{i}",
            "outcome": "written",
            "values": [
                {"property": f"p{k}", "value": "é" * chars, "quote": "", "outcome": "written"}
                for k in range(32)
            ],
        }
        for i in range(count)
    ]


def test_a_report_past_512_kib_loses_the_values_of_its_last_inputs_first() -> None:
    report = fit_report({"inputs": _inputs(200, 512), "counts": {"written": 230}})
    assert json_bytes(report) <= WORKFLOW_RUN_RESULTS_MAX_BYTES
    assert len(report["inputs"]) == 200
    assert report["counts"]["written"] == 230
    kept = [item for item in report["inputs"] if item["values"]]
    # The same cut as the TypeScript worker's.
    assert len(kept) == 12
    assert len(report["inputs"][11]["values"]) == 32
    assert report["inputs"][12]["values"] == []


def test_a_step_keeps_what_it_reported_in_its_saved_output_and_reads_it_back() -> None:
    reported = _inputs(3, 100) + _inputs(3, 512)
    saved = step_output_with_results({"commit_seq": 12}, "run-1", reported)
    value, run, results = step_output_results(saved)
    assert value == {"commit_seq": 12}
    assert run == "run-1"
    assert len(results) == 6
    assert len(results[2]["values"]) == 32
    assert results[3] == {"label": "n0", "outcome": "written"}
    assert json_bytes(saved) <= 60 * 1024
    assert step_output_results({"commit_seq": 12}) == ({"commit_seq": 12}, None, [])
    assert step_output_with_results(7, "run-1", []) == 7
    # Text with a lone surrogate has no UTF-8 size: the value is saved alone.
    assert step_output_with_results("\ud800", "run-1", reported) == "\ud800"
    lone = [{"iri": "e:1", "label": "\udc00", "outcome": "written"}]
    assert step_output_with_results({"commit_seq": 12}, "run-1", lone) == {"commit_seq": 12}
