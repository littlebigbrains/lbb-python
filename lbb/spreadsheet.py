"""Places in a spreadsheet: cell addresses (A1) and the fragment that names a
place in the evidence of a fact read from an XLSX workbook.

The fragment of a place is ``<sheet>!<range>`` on the ``Document`` IRI, the
form spreadsheet links use (``…/e/41ab…#Sales!B2:D9``). The range is one cell
(``B2``), a rectangle of cells (``B2:D9``), whole rows (``5:7``) or whole
columns (``B:D``). A sheet name that is not a plain word is single-quoted,
with a quote in the name written twice (``'Q3 Sales'!B2``, ``'Jane''s'!A1``),
and the whole fragment is percent-encoded (``Q3%20Sales``; ``(``, ``)`` and
``*`` stay as they are). Rows count from 1. The TypeScript SDK has the same functions (``spreadsheetFragment``).
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from urllib.parse import quote, unquote

__all__ = [
    "SPREADSHEET_MAX_COLUMNS",
    "SPREADSHEET_MAX_ROWS",
    "SpreadsheetPlace",
    "cell_ref",
    "column_letters",
    "column_number",
    "parse_spreadsheet_fragment",
    "spreadsheet_fragment",
]

# The reader's limits (``FILE_XLSX_MAX_COLUMNS``, ``FILE_XLSX_MAX_ROWS``): a place past
# them names nothing the reader holds.
SPREADSHEET_MAX_COLUMNS = 512
SPREADSHEET_MAX_ROWS = 50_000

_BARE_SHEET = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_LOOKS_LIKE_CELL = re.compile(r"[A-Za-z]{1,3}[0-9]+")
_CELL = re.compile(r"([A-Za-z]{1,3})([1-9][0-9]{0,6})")
_ROWS = re.compile(r"([1-9][0-9]{0,6}):([1-9][0-9]{0,6})")
_COLS = re.compile(r"([A-Za-z]{1,3}):([A-Za-z]{1,3})")
_SPLIT = re.compile(r"(?:'((?:[^']|'')+)'|([^!']+))!(.+)", re.DOTALL)


@dataclass(frozen=True)
class SpreadsheetPlace:
    """A place in a sheet. Rows and columns count from 1; both ends are included.

    ``kind`` is ``"cells"`` (``row``, ``col``, ``to_row``, ``to_col``),
    ``"rows"`` or ``"cols"`` (``start`` to ``end``).
    """

    kind: str
    sheet: str
    row: int = 0
    col: int = 0
    to_row: int = 0
    to_col: int = 0
    start: int = 0
    end: int = 0


def column_letters(column: int) -> str:
    """A column number (from 1) as its letters: 1 is ``A``, 27 is ``AA``."""
    if isinstance(column, bool) or column < 1:
        raise ValueError("column must be 1 or more")
    letters = ""
    n = column
    while n > 0:
        letters = chr(65 + (n - 1) % 26) + letters
        n = (n - 1) // 26
    return letters


def column_number(letters: str) -> int | None:
    """The number (from 1) of a column's letters, or None for text that is no column."""
    if not re.fullmatch(r"[A-Za-z]{1,3}", letters):
        return None
    n = 0
    for letter in letters.upper():
        n = n * 26 + ord(letter) - 64
    return n


def cell_ref(column: int, row: int) -> str:
    """The A1 reference of a cell: ``cell_ref(2, 3)`` is ``B3``."""
    if isinstance(row, bool) or row < 1:
        raise ValueError("row must be 1 or more")
    return f"{column_letters(column)}{row}"


def _sheet_name(sheet: str) -> str:
    if _BARE_SHEET.fullmatch(sheet) and not _LOOKS_LIKE_CELL.fullmatch(sheet):
        return sheet
    return "'" + sheet.replace("'", "''") + "'"


def _range_text(place: SpreadsheetPlace) -> str:
    if place.kind == "cells":
        if place.to_row == place.row and place.to_col == place.col:
            return cell_ref(place.col, place.row)
        return f"{cell_ref(place.col, place.row)}:{cell_ref(place.to_col, place.to_row)}"
    if place.kind == "rows":
        return f"{place.start}:{place.end}"
    if place.kind == "cols":
        return f"{column_letters(place.start)}:{column_letters(place.end)}"
    raise ValueError(f"unknown kind {place.kind!r}")


def spreadsheet_fragment(place: SpreadsheetPlace) -> str:
    """The fragment of a place, without its ``#``: ``Sales!B2:D9``, or
    ``'Q3%20Sales'!B2``. A sheet name of any characters round-trips through
    :func:`parse_spreadsheet_fragment`."""
    if not place.sheet:
        raise ValueError("a place needs a sheet")
    text = f"{_sheet_name(place.sheet)}!{_range_text(place)}"
    # The characters ``encodeURIComponent`` leaves alone, and ``:``: the same
    # fragment as the TypeScript SDK writes.
    return quote(text, safe=":!'()*")


def parse_spreadsheet_fragment(fragment: str | None) -> SpreadsheetPlace | None:
    """The place a fragment names (with or without its ``#``), or None for a
    fragment of no known form. An end before its start is None."""
    text = (fragment or "").removeprefix("#")
    if not text:
        return None
    try:
        text = unquote(text, errors="strict")
    except UnicodeDecodeError:
        return None
    split = _SPLIT.fullmatch(text)
    if not split:
        return None
    sheet = split.group(1).replace("''", "'") if split.group(1) is not None else split.group(2)
    rng = split.group(3)
    rows = _ROWS.fullmatch(rng)
    if rows:
        start, end = int(rows.group(1)), int(rows.group(2))
        return (
            SpreadsheetPlace("rows", sheet, start=start, end=end)
            if end >= start and end <= SPREADSHEET_MAX_ROWS
            else None
        )
    cols = _COLS.fullmatch(rng)
    if cols:
        first_col, last_col = column_number(cols.group(1)), column_number(cols.group(2))
        if (
            first_col is None
            or last_col is None
            or last_col < first_col
            or last_col > SPREADSHEET_MAX_COLUMNS
        ):
            return None
        return SpreadsheetPlace("cols", sheet, start=first_col, end=last_col)
    parts = rng.split(":")
    if len(parts) > 2 or "" in parts:
        return None
    first = _CELL.fullmatch(parts[0])
    last = _CELL.fullmatch(parts[-1])
    if not first or not last:
        return None
    col = column_number(first.group(1))
    to_col = column_number(last.group(1))
    row, to_row = int(first.group(2)), int(last.group(2))
    if (
        col is None
        or to_col is None
        or to_row < row
        or to_col < col
        or to_row > SPREADSHEET_MAX_ROWS
        or to_col > SPREADSHEET_MAX_COLUMNS
    ):
        return None
    return SpreadsheetPlace("cells", sheet, row=row, col=col, to_row=to_row, to_col=to_col)
