"""Ontology starters: the four routes on the client-level and graph-scoped
ontology namespaces (sync and async), and the generated term constants."""

from __future__ import annotations

import json
import unittest
from typing import Any

import httpx

from lbb import AsyncLbbClient, LbbClient, LbbError
from lbb.models import (
    OntologyStarterApplyResponse,
    OntologyStarterDetail,
    OntologyStarterList,
    OntologyStarterUpdateResponse,
)
from lbb.starters import crm, documents, work

GRAPH = {"tenant_id": "tenant", "graph_id": "main", "branch_id": "main"}
STATUS = {
    "state": "applied",
    "adds": {
        "classes": 0,
        "super_types": 0,
        "properties": 0,
        "relations": 0,
        "widened_relations": 0,
    },
    "present": {"classes": 20, "properties": 69, "relations": 16},
    "conflicts": [],
}
DOCUMENT = {
    "format": 1,
    "id": "crm",
    "version": "1.0.0",
    "label": "CRM",
    "description": "CRM",
    "sources": ["hubspot"],
    "classes": [{"name": "Party", "description": "A party.", "properties": []}],
    "properties": [],
    "relations": [],
    "shared": [],
    "questions": [],
    "ops": [{"op": "add_entity_type", "name": "Party"}],
}
LIST = {"graph": GRAPH, "ontology_version": None, "starters": []}
DETAIL = {
    "graph": GRAPH,
    "ontology_version": 3,
    "starter": DOCUMENT,
    "status": STATUS,
    "missing_ops": [],
}
APPLIED = {
    "graph": GRAPH,
    "starter": "crm",
    "version": "1.0.0",
    "dry_run": True,
    "no_op": False,
    "applied_ops": [{"op": "add_entity_type", "name": "Party"}],
    "ontology_version": 4,
    "status": STATUS,
}
UPDATED = {"suggestion_id": "sg_1", "created": True}
VIEW = {"graph": GRAPH, "ontology_version": 1, "entity_types": [], "relations": []}
RESPONSES: list[dict[str, Any]] = [
    {"json": LIST},
    {"json": DETAIL},
    {"json": APPLIED},
    {"json": UPDATED},
]


def capturing_transport(
    captured: list[httpx.Request], responses: list[dict[str, Any]]
) -> httpx.MockTransport:
    queue = list(responses)

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(200, json=queue.pop(0)["json"])

    return httpx.MockTransport(handler)


class StarterRouteTests(unittest.TestCase):
    def assert_requests(self, seen: list[httpx.Request], graph: str) -> None:
        self.assertEqual(
            [(request.method, request.url.path) for request in seen],
            [
                ("GET", "/v1/ontology/starters"),
                ("GET", "/v1/ontology/starters/detail"),
                ("POST", "/v1/ontology/starters/apply"),
                ("POST", "/v1/ontology/starters/update"),
            ],
        )
        for request in seen:
            self.assertEqual(request.url.params["graph"], graph)
        self.assertEqual(seen[1].url.params["starter"], "crm")
        self.assertEqual(
            json.loads(seen[2].content),
            {"starter": "crm", "dry_run": True, "expected_ontology_version": 3},
        )
        self.assertEqual(json.loads(seen[3].content), {"starter": "crm"})

    def test_client_ontology_starters_are_typed(self) -> None:
        seen: list[httpx.Request] = []
        with LbbClient(
            "http://h", graph="main", transport=capturing_transport(seen, RESPONSES)
        ) as client:
            listed = client.ontology.starters.list()
            detail = client.ontology.starters.get("crm")
            applied = client.ontology.starters.apply(
                "crm", dry_run=True, expected_ontology_version=3
            )
            updated = client.ontology.starters.update("crm")
        self.assertIsInstance(listed, OntologyStarterList)
        self.assertIsNone(listed.ontology_version)
        self.assertIsInstance(detail, OntologyStarterDetail)
        self.assertEqual(detail.starter.classes[0].name, "Party")
        self.assertIsInstance(applied, OntologyStarterApplyResponse)
        self.assertTrue(applied.dry_run)
        self.assertIsInstance(updated, OntologyStarterUpdateResponse)
        self.assertEqual(updated.suggestion_id, "sg_1")
        self.assert_requests(seen, "main")

    def test_graph_scoped_ontology_targets_its_graph(self) -> None:
        seen: list[httpx.Request] = []
        with LbbClient(
            "http://h",
            graph="main",
            transport=capturing_transport(seen, [*RESPONSES, {"json": VIEW}]),
        ) as client:
            sales = client.graph("sales").ontology
            sales.starters.list()
            sales.starters.get("crm")
            sales.starters.apply("crm", dry_run=True, expected_ontology_version=3)
            sales.starters.update("crm")
            sales.view()
        self.assert_requests(seen[:4], "sales")
        self.assertEqual(seen[4].url.path, "/v1/ontology")
        self.assertEqual(seen[4].url.params["graph"], "sales")

    def test_apply_without_options_sends_only_the_starter(self) -> None:
        seen: list[httpx.Request] = []
        with LbbClient(
            "http://h", transport=capturing_transport(seen, [{"json": APPLIED}])
        ) as client:
            client.ontology.starters.apply("work")
        self.assertEqual(json.loads(seen[0].content), {"starter": "work"})
        self.assertNotIn("graph", seen[0].url.params)


    def test_a_refused_apply_exposes_its_conflicts(self) -> None:
        conflict = {
            "kind": "property",
            "name": "priority",
            "starter": "keyword",
            "graph": "i64",
            "message": "property priority is i64 in the graph",
        }
        body = {
            "error": {
                "type": "conflict_error",
                "code": "starter_conflict",
                "message": "the graph holds 1 term(s) of the Work starter differently",
                "param": "starter",
                "details": {"conflicts": [conflict]},
            }
        }

        def handler(_request: httpx.Request) -> httpx.Response:
            return httpx.Response(409, json=body)

        with LbbClient("http://h", transport=httpx.MockTransport(handler)) as client:
            with self.assertRaises(LbbError) as raised:
                client.ontology.starters.apply("work")
        self.assertEqual(raised.exception.status_code, 409)
        self.assertEqual(raised.exception.code, "starter_conflict")
        self.assertEqual(raised.exception.details, {"conflicts": [conflict]})


class AsyncStarterRouteTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_graph_scoped_starters_are_awaited_and_typed(self) -> None:
        seen: list[httpx.Request] = []
        async with AsyncLbbClient(
            "http://h", transport=capturing_transport(seen, RESPONSES)
        ) as client:
            starters = client.graph("main").ontology.starters
            listed = await starters.list()
            detail = await starters.get("crm")
            applied = await starters.apply(
                "crm", dry_run=True, expected_ontology_version=3
            )
            updated = await starters.update("crm")
        self.assertIsInstance(listed, OntologyStarterList)
        self.assertIsInstance(detail, OntologyStarterDetail)
        self.assertIsInstance(applied, OntologyStarterApplyResponse)
        self.assertIsInstance(updated, OntologyStarterUpdateResponse)
        StarterRouteTests().assert_requests(seen, "main")


class StarterTermTests(unittest.TestCase):
    def test_terms_carry_the_iris_the_rdf_projection_mints(self) -> None:
        self.assertEqual(crm.id, "crm")
        self.assertEqual(
            crm.classes.Organization.iri,
            "https://littlebigbrain.com/class/organization",
        )
        self.assertEqual(crm.classes.User.super_types, ("Person",))
        self.assertEqual(
            crm.classes.LineItem.iri, "https://littlebigbrain.com/class/lineitem"
        )
        self.assertEqual(crm.properties.domain.iri, "https://littlebigbrain.com/p/domain")
        self.assertEqual(crm.properties.close_date.value_type, "date_time")
        self.assertEqual(
            crm.relations.WORKS_AT.iri, "https://littlebigbrain.com/r/works_at"
        )
        self.assertEqual(crm.relations.WORKS_AT.inverse, "EMPLOYS")
        self.assertEqual(
            crm.relations.WORKS_AT.inverse_iri, "https://littlebigbrain.com/r/employs"
        )
        self.assertIsNone(work.relations.RELATED_TO.inverse_iri)
        self.assertEqual(documents.classes.Page.super_types, ("Document",))

    def test_questions_are_listed_in_order(self) -> None:
        self.assertEqual(len(crm.questions), 30)
        self.assertEqual(crm.questions[0].id, "crm-01")
        self.assertEqual(documents.questions[0].id, "doc-01")
        self.assertEqual(work.questions[-1].id, "work-18")
        self.assertTrue(all(question.text for question in crm.questions))


if __name__ == "__main__":
    unittest.main()
