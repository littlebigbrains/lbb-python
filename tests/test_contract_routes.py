"""Contract-drift guard for the hand-written client method surface (S8).

The generated artifacts (``contracts/openapi.json``, ``schema.ts``,
``models.py``) are gated by the ``contracts`` CI job, but the hand-written
request methods in :mod:`lbb.client` embed route strings that nothing checks
against the contract. A new route can land in the contract with no client
method, and — the case this guards — a typo'd path or a route removed from the
server can linger in the client and only fail at runtime.

This test extracts every ``(method, path)`` literal the client issues and
asserts each is a real operation in ``contracts/openapi.json``.
"""

from __future__ import annotations

import json
import re
import unittest
from pathlib import Path

_HERE = Path(__file__).resolve()


def _find_openapi() -> Path:
    # Walk up so the test works both in the monorepo (repo root three levels
    # up) and in the public lbb-python repo, where the package sits at the
    # repository root next to contracts/.
    for parent in _HERE.parents:
        candidate = parent / "contracts" / "openapi.json"
        if candidate.is_file():
            return candidate
    raise FileNotFoundError("contracts/openapi.json not found in any ancestor")


_OPENAPI = _find_openapi()
_CLIENT_PACKAGE = _HERE.parents[1] / "lbb"
_CLIENT_MODULES = (
    _CLIENT_PACKAGE / "client.py",
    _CLIENT_PACKAGE / "_client_base.py",
    _CLIENT_PACKAGE / "_sync_client.py",
    _CLIENT_PACKAGE / "_async_client.py",
)

# Matches `self._request("POST", "/v1/graph/commit"` and the awaited async form;
# only static string-literal paths are captured (the client uses no f-string
# routes — dynamic paths would be invisible here and are covered by the methods
# that build them).
_REQUEST_CALL = re.compile(r'_request\(\s*"([A-Z]+)"\s*,\s*"([^"]+)"')

_HTTP_METHODS = {"get", "post", "put", "patch", "delete"}


def _spec_operations() -> set[tuple[str, str]]:
    spec = json.loads(_OPENAPI.read_text())
    return {
        (method.upper(), path)
        for path, methods in spec["paths"].items()
        for method in methods
        if method.lower() in _HTTP_METHODS
    }


def _client_routes() -> set[tuple[str, str]]:
    source = "\n".join(path.read_text() for path in _CLIENT_MODULES if path.is_file())
    return set(_REQUEST_CALL.findall(source))


class ContractRouteCoverage(unittest.TestCase):
    def test_client_source_files_exist(self) -> None:
        self.assertTrue(_OPENAPI.is_file(), f"missing {_OPENAPI}")
        self.assertTrue(_CLIENT_MODULES[0].is_file(), f"missing {_CLIENT_MODULES[0]}")

    def test_routes_were_parsed(self) -> None:
        # Guards the extraction itself: if the client were refactored to build
        # routes dynamically, an empty set would make the coverage test vacuous.
        self.assertTrue(_client_routes(), "no _request routes parsed from the client package")

    def test_every_client_route_exists_in_contract(self) -> None:
        spec = _spec_operations()
        missing = sorted(route for route in _client_routes() if route not in spec)
        self.assertEqual(
            missing,
            [],
            "client calls route(s) absent from contracts/openapi.json — a typo or "
            "a route removed from the server. Fix the client, or regenerate the "
            f"contract from the canonical monorepo. Offending: {missing}",
        )


if __name__ == "__main__":
    unittest.main()
