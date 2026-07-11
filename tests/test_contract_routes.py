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

import ast
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

    def test_every_model_request_declares_its_generated_response_type(self) -> None:
        source = (_CLIENT_PACKAGE / "_client_base.py").read_text()
        tree = ast.parse(source)
        mismatches: list[str] = []
        checked = 0
        for owner in (node for node in tree.body if isinstance(node, ast.ClassDef)):
            for method in (
                node for node in owner.body if isinstance(node, ast.FunctionDef)
            ):
                calls = [
                    call
                    for call in ast.walk(method)
                    if isinstance(call, ast.Call)
                    and isinstance(call.func, ast.Attribute)
                    and call.func.attr == "_model_request"
                    and call.args
                ]
                if not calls:
                    continue
                checked += 1
                declared = ast.unparse(method.returns) if method.returns else ""
                generated = ast.unparse(calls[0].args[0])
                if declared != generated:
                    mismatches.append(
                        f"{owner.name}.{method.name}: {declared or '<missing>'} != {generated}"
                    )
        self.assertGreater(checked, 20, "model-return audit became vacuous")
        self.assertEqual(
            mismatches,
            [],
            "high-level generated-model methods must declare exactly the model "
            f"they validate and return: {mismatches}",
        )

    def test_async_surface_awaits_every_generated_model_helper(self) -> None:
        base_tree = ast.parse((_CLIENT_PACKAGE / "_client_base.py").read_text())
        async_tree = ast.parse((_CLIENT_PACKAGE / "_async_client.py").read_text())
        async_methods = {
            owner.name: {
                method.name: method
                for method in owner.body
                if isinstance(method, ast.AsyncFunctionDef)
            }
            for owner in async_tree.body
            if isinstance(owner, ast.ClassDef)
        }
        counterparts = {
            "_BaseLbbClient": "AsyncLbbClient",
            "_GraphNamespace": "_AsyncGraphNamespace",
            "_FactsNamespace": "_AsyncFactsNamespace",
            "_ContextNamespace": "_AsyncContextNamespace",
            "_OntologyNamespace": "_AsyncOntologyNamespace",
            "_QueryNamespace": "_AsyncQueryNamespace",
            "_SchemaNamespace": "_AsyncSchemaNamespace",
            "_EntityNamespace": "_AsyncEntityNamespace",
        }
        missing: list[str] = []
        mismatches: list[str] = []
        checked = 0
        for owner in (
            node
            for node in base_tree.body
            if isinstance(node, ast.ClassDef) and node.name in counterparts
        ):
            for method in (
                node for node in owner.body if isinstance(node, ast.FunctionDef)
            ):
                if not any(
                    isinstance(call, ast.Call)
                    and isinstance(call.func, ast.Attribute)
                    and call.func.attr == "_model_request"
                    for call in ast.walk(method)
                ):
                    continue
                checked += 1
                async_owner = counterparts[owner.name]
                async_method = async_methods.get(async_owner, {}).get(method.name)
                label = f"{owner.name}.{method.name}"
                if async_method is None:
                    missing.append(label)
                    continue
                declared = ast.unparse(method.returns) if method.returns else ""
                async_declared = (
                    ast.unparse(async_method.returns) if async_method.returns else ""
                )
                if async_declared != declared:
                    mismatches.append(f"{label}: {async_declared} != {declared}")
        self.assertGreater(checked, 30, "async generated-model audit became vacuous")
        self.assertEqual(missing, [], f"async helpers returned bare coroutines: {missing}")
        self.assertEqual(
            mismatches,
            [],
            f"async helpers must resolve to the same generated model: {mismatches}",
        )


if __name__ == "__main__":
    unittest.main()
