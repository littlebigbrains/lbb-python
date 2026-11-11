"""A developer's base ontologies: the routes of ``ontology.bases`` (sync and
async), the version argument of the starters, and the terms built from a stored
document, pinned by one fixed document (``contracts/ontology-bases/acme.json``)."""

from __future__ import annotations

import importlib.util
import json
import unittest
from pathlib import Path
from typing import Any

import httpx

from lbb import AsyncLbbClient, LbbClient
from lbb.bases import base_terms, iri_segment
from lbb.models import OntologyBaseDetail, OntologyBaseList, OntologyBaseStoreResponse


def _fixture_dir() -> Path | None:
    """``contracts/ontology-bases`` above the package, absent in the public repo."""
    for parent in Path(__file__).resolve().parents:
        candidate = parent / "contracts" / "ontology-bases"
        if candidate.is_dir():
            return candidate
    return None


FIXTURES = _fixture_dir()
DOCUMENT: dict[str, Any] = {
    "format": 1,
    "id": "acme",
    "version": "1.0.0",
    "label": "Acme legal",
    "description": "Agreements.",
    "sources": [],
    "classes": [
        {"name": "Agreement", "description": "A contract.", "properties": []},
        {
            "name": "Master",
            "description": "A master contract.",
            "superTypes": ["Agreement"],
            "properties": [],
        },
    ],
    "properties": [],
    "relations": [],
    "shared": [],
    "questions": [],
}
STORED = {"id": "acme", "version": "1.0.0", "created": True, "checksum": "blake3:ab"}
LISTED = {
    "bases": [
        {
            "id": "acme",
            "label": "Acme legal",
            "versions": ["1.0.0"],
            "latest": "1.0.0",
        }
    ]
}
DETAIL = {
    "id": "acme",
    "version": "1.0.0",
    "checksum": "blake3:ab",
    "document": DOCUMENT,
}


def capturing_transport(
    captured: list[httpx.Request], responses: list[dict[str, Any]]
) -> httpx.MockTransport:
    queue = list(responses)

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(200, json=queue.pop(0))

    return httpx.MockTransport(handler)


class BaseRouteTests(unittest.TestCase):
    def test_each_operation_maps_to_its_route(self) -> None:
        seen: list[httpx.Request] = []
        responses = [STORED, LISTED, DETAIL, DETAIL, DETAIL, {}, {}, {}]
        with LbbClient(
            "http://h", graph="main", transport=capturing_transport(seen, responses)
        ) as client:
            stored = client.ontology.bases.store(DOCUMENT)
            listed = client.ontology.bases.list()
            detail = client.ontology.bases.get("acme")
            client.ontology.bases.get("acme", version="1.0.0")
            terms = client.ontology.bases.terms("acme")
        self.assertIsInstance(stored, OntologyBaseStoreResponse)
        self.assertTrue(stored.created)
        self.assertIsInstance(listed, OntologyBaseList)
        self.assertIsInstance(detail, OntologyBaseDetail)
        self.assertEqual(terms.classes["Agreement"].name, "acme.Agreement")
        self.assertEqual(
            [(request.method, request.url.path) for request in seen],
            [
                ("POST", "/v1/ontology/bases"),
                ("GET", "/v1/ontology/bases"),
                ("GET", "/v1/ontology/bases/detail"),
                ("GET", "/v1/ontology/bases/detail"),
                ("GET", "/v1/ontology/bases/detail"),
            ],
        )
        self.assertEqual(json.loads(seen[0].content)["document"]["id"], "acme")
        self.assertEqual(seen[2].url.params["base"], "acme")
        self.assertNotIn("version", seen[2].url.params)
        self.assertEqual(seen[3].url.params["version"], "1.0.0")

    def test_starters_take_a_version(self) -> None:
        seen: list[httpx.Request] = []
        with LbbClient(
            "http://h",
            graph="main",
            transport=capturing_transport(seen, [{}, {}, {}]),
        ) as client:
            for call in (
                lambda: client.ontology.starters.get("acme", version="1.0.0"),
                lambda: client.ontology.starters.apply("acme", version="1.0.0"),
                lambda: client.ontology.starters.update("acme", version="1.0.0"),
            ):
                try:
                    call()
                except Exception:  # the empty answers do not validate
                    pass
        self.assertEqual(seen[0].url.params["version"], "1.0.0")
        self.assertEqual(json.loads(seen[1].content), {"starter": "acme", "version": "1.0.0"})
        self.assertEqual(json.loads(seen[2].content), {"starter": "acme", "version": "1.0.0"})


class AsyncBaseRouteTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_bases_are_awaited_and_typed(self) -> None:
        seen: list[httpx.Request] = []
        async with AsyncLbbClient(
            "http://h",
            graph="main",
            transport=capturing_transport(seen, [STORED, LISTED, DETAIL, DETAIL]),
        ) as client:
            bases = client.ontology.bases
            stored = await bases.store(DOCUMENT)
            listed = await bases.list()
            detail = await bases.get("acme")
            terms = await bases.terms("acme", version="1.0.0")
        self.assertIsInstance(stored, OntologyBaseStoreResponse)
        self.assertIsInstance(listed, OntologyBaseList)
        self.assertIsInstance(detail, OntologyBaseDetail)
        self.assertEqual(terms.classes["Master"].super_types, ("acme.Agreement",))
        self.assertEqual(seen[3].url.params["version"], "1.0.0")


class BaseTermsTests(unittest.TestCase):
    def test_a_qualified_name_keeps_its_dot_in_the_iri_segment(self) -> None:
        self.assertEqual(iri_segment("acme.Agreement"), "acme.agreement")
        self.assertEqual(iri_segment("acme.OWNED_BY"), "acme.owned_by")
        self.assertEqual(iri_segment("acme.a b"), "acme.a%20b")
        self.assertEqual(iri_segment("acme.é"), "acme.%C3%A9")

    @unittest.skipIf(FIXTURES is None, "the fixed base is not in this repository")
    def test_the_terms_of_the_fixed_base_are_the_committed_pin(self) -> None:
        assert FIXTURES is not None
        document = json.loads((FIXTURES / "acme.json").read_text())
        pinned = json.loads((FIXTURES / "acme.terms.json").read_text())
        terms = base_terms(document)
        built = {
            "id": terms.id,
            "version": terms.version,
            "label": terms.label,
            "classes": {
                key: {
                    "name": value.name,
                    "iri": value.iri,
                    "superTypes": list(value.super_types),
                }
                for key, value in terms.classes.items()
            },
            "properties": {
                key: {
                    "name": value.name,
                    "iri": value.iri,
                    "valueType": value.value_type,
                }
                for key, value in terms.properties.items()
            },
            "relations": {
                key: {
                    "name": value.name,
                    "iri": value.iri,
                    "domain": list(value.domain),
                    "range": list(value.range),
                    "cardinality": value.cardinality,
                    "inverse": value.inverse,
                    "inverseIri": value.inverse_iri,
                }
                for key, value in terms.relations.items()
            },
            "questions": [
                {"id": q.id, "text": q.text, "sparql": q.sparql} for q in terms.questions
            ],
        }
        self.assertEqual(built, pinned)

    @unittest.skipIf(FIXTURES is None, "the fixed base is not in this repository")
    def test_the_generated_module_holds_the_runtime_terms(self) -> None:
        assert FIXTURES is not None
        spec = importlib.util.spec_from_file_location("acme_terms", FIXTURES / "acme.terms.py")
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        terms = base_terms(json.loads((FIXTURES / "acme.json").read_text()))
        generated = module.acme
        self.assertEqual(generated.classes.Agreement, terms.classes["Agreement"])
        self.assertEqual(generated.properties.renewal_date, terms.properties["renewal_date"])
        self.assertEqual(generated.relations.OWNED_BY, terms.relations["OWNED_BY"])
        self.assertEqual(generated.version, terms.version)
        self.assertEqual(tuple(generated.questions), terms.questions)


if __name__ == "__main__":
    unittest.main()
