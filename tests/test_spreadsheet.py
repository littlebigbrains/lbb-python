"""Places in a spreadsheet: the fragment of each place is fixed, and reads back."""

import json
import unittest
from pathlib import Path
from typing import Any

from lbb.spreadsheet import (
    SpreadsheetPlace,
    cell_ref,
    column_letters,
    column_number,
    parse_spreadsheet_fragment,
    spreadsheet_fragment,
)


def _shared() -> dict[str, Any]:
    """The table shared with the TypeScript SDK's test: in the monorepo under
    ``sdk/conformance``, in the public repo copied to ``conformance/`` by the export."""
    for parent in Path(__file__).resolve().parents:
        for candidate in (
            parent / "conformance" / "spreadsheet-places.json",
            parent / "sdk" / "conformance" / "spreadsheet-places.json",
        ):
            if candidate.is_file():
                shared: dict[str, Any] = json.loads(candidate.read_text(encoding="utf-8"))
                return shared
    raise FileNotFoundError("spreadsheet-places.json not found")


SHARED = _shared()


def _place(shared: dict[str, Any]) -> SpreadsheetPlace:
    if shared["kind"] == "cells":
        return SpreadsheetPlace(
            "cells",
            shared["sheet"],
            row=shared["row"],
            col=shared["col"],
            to_row=shared["to_row"],
            to_col=shared["to_col"],
        )
    return SpreadsheetPlace(shared["kind"], shared["sheet"], start=shared["from"], end=shared["to"])


VECTORS = [(_place(entry["place"]), entry["fragment"]) for entry in SHARED["places"]]


class SpreadsheetTest(unittest.TestCase):
    def test_a_place_has_a_fixed_fragment_and_it_reads_back(self) -> None:
        self.assertGreaterEqual(len(VECTORS), 10)
        for place, fragment in VECTORS:
            self.assertEqual(spreadsheet_fragment(place), fragment)
            self.assertEqual(parse_spreadsheet_fragment(fragment), place)
            self.assertEqual(parse_spreadsheet_fragment("#" + fragment), place)

    def test_a_fragment_that_names_no_place_is_none(self) -> None:
        self.assertGreaterEqual(len(SHARED["invalid"]), 10)
        for bad in SHARED["invalid"]:
            self.assertIsNone(parse_spreadsheet_fragment(bad), repr(bad))
        self.assertIsNone(parse_spreadsheet_fragment(None))
        with self.assertRaises(ValueError):
            spreadsheet_fragment(SpreadsheetPlace("rows", "", start=1, end=1))

    def test_columns_count_in_letters(self) -> None:
        for n, letters in [(1, "A"), (26, "Z"), (27, "AA"), (52, "AZ"), (53, "BA"), (702, "ZZ"), (703, "AAA"), (512, "SR")]:
            self.assertEqual(column_letters(n), letters)
            self.assertEqual(column_number(letters), n)
        self.assertEqual(cell_ref(2, 3), "B3")
        self.assertIsNone(column_number("AAAA"))
        with self.assertRaises(ValueError):
            column_letters(0)


if __name__ == "__main__":
    unittest.main()
