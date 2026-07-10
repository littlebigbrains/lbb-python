"""Unit tests for the HTTP client using httpx's MockTransport (no server)."""

from __future__ import annotations

import json
import unittest
from typing import Any
from unittest.mock import patch

import httpx

from lbb import AsyncLbbClient, LbbClient, LbbError, ListPage, __version__
from lbb.models import (
    AskResponse,
    CreateGraphResponse,
    EntityExplorerRow,
    GraphEdgeRow,
    GraphSummaryResponse,
    SchemaBundleView,
    SparqlSelectResponse,
)

SNAPSHOT = {"commit_seq": 7, "indexed_seq": 7}
GRAPH = {"tenant_id": "tenant", "graph_id": "main", "branch_id": "main"}


ResponseSpec = dict[str, Any]


def summary_payload() -> dict[str, Any]:
    return {
        "snapshot": SNAPSHOT,
        "ontology_version": 3,
        "entity_count": 2,
        "observation_count": 1,
        "edge_event_count": 4,
        "current_edge_count": 3,
        "entity_types": [{"name": "SERVICE", "count": 2}],
        "relations": [{"name": "CALLS", "count": 3}],
    }


def entity_list_payload() -> dict[str, Any]:
    return {
        "object": "list",
        "data": [
            {
                "id": "e1",
                "entity_type": "SERVICE",
                "name": "auth-service",
                "aliases": [],
                "created_at_commit": 1,
                "out_degree": 2,
                "in_degree": 0,
                "observation_count": 1,
                "attributes": {"slo": 0.999},
            }
        ],
        "has_more": False,
        "next_cursor": None,
        "snapshot": SNAPSHOT,
        "total_count": 1,
    }


def edge_list_payload() -> dict[str, Any]:
    entity = {"id": "e1", "type": "SERVICE", "name": "auth-service"}
    peer = {"id": "e2", "type": "DATABASE", "name": "user-db"}
    return {
        "object": "list",
        "data": [
            {
                "edge_event_id": "edge1",
                "source": entity,
                "relation": {"id": 1, "name": "WRITES_TO"},
                "target": peer,
                "confidence": 0.93,
                "valid_time": {"granularity": "instant"},
                "evidence": [],
                "reducer": "latest",
                "superseded": [],
            }
        ],
        "has_more": False,
        "next_cursor": None,
        "snapshot": SNAPSHOT,
        "total_count": 1,
    }


def schema_view_payload() -> dict[str, Any]:
    return {
        "graph": GRAPH,
        "ontology_version": 3,
        "enforce_mode": "warn",
        "classes": [],
        "relations": [],
        "shape_count": 1,
        "constraint_shape_count": 1,
        "audit_summary": {"conforms": False, "violation_count": 1},
    }


def sparql_select_payload() -> dict[str, Any]:
    return {
        "snapshot": SNAPSHOT,
        "vars": ["svc"],
        "solutions": [],
        "row_page": {
            "returned": 0,
            "total": 0,
            "offset": 0,
            "limit": 25,
            "has_more": False,
        },
    }


def capturing_transport(
    captured: list[httpx.Request],
    responses: ResponseSpec | list[ResponseSpec] | None = None,
) -> httpx.MockTransport:
    queue = list(responses if isinstance(responses, list) else [responses or {"json": {}}])

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        item = queue.pop(0) if queue else {"json": {}}
        status = item.get("status", 200)
        headers = item.get("headers", {})
        if "text" in item:
            return httpx.Response(status, text=item["text"], headers=headers)
        return httpx.Response(status, json=item.get("json", {}), headers=headers)

    return httpx.MockTransport(handler)


class SyncClientTests(unittest.TestCase):
    def test_create_graph_uses_http_scope_and_returns_typed_response(self) -> None:
        seen: list[httpx.Request] = []
        payload = {
            "commit_seq": 0,
            "graph": {"tenant_id": "tenant", "graph_id": "research", "branch_id": "analysis"},
            "ontology_version": 1,
        }
        with LbbClient(
            "http://h",
            graph="research",
            branch="analysis",
            transport=capturing_transport(seen, {"json": payload}),
        ) as client:
            result = client.create_graph()

        self.assertIsInstance(result, CreateGraphResponse)
        self.assertEqual(result.graph.graph_id, "research")
        self.assertEqual(seen[0].method, "POST")
        self.assertEqual(str(seen[0].url).split("?")[0], "http://h/v1/graph/create")
        self.assertEqual(
            dict(seen[0].url.params), {"graph": "research", "branch": "analysis"}
        )

    def test_namespace_facts_create_injects_auth_scope_version_and_idempotency(self) -> None:
        seen: list[httpx.Request] = []
        with LbbClient(
            "http://h:7400/",
            api_key="lbb_sk_test",
            transport=capturing_transport(seen, {"json": {"commit": {"commit_seq": 1}}}),
        ) as client:
            result = client.graph("main", branch="b").facts.create(
                {"triplets": []}, idempotency_key="ik_py_1"
            )

        self.assertEqual(result["commit"]["commit_seq"], 1)
        request = seen[0]
        self.assertEqual(request.method, "POST")
        self.assertEqual(str(request.url).split("?")[0], "http://h:7400/v1/graph/commit")
        self.assertEqual(dict(request.url.params), {"graph": "main", "branch": "b"})
        self.assertEqual(request.headers["authorization"], "Bearer lbb_sk_test")
        self.assertEqual(request.headers["lbb-version"], "2026-06-22")
        self.assertEqual(request.headers["idempotency-key"], "ik_py_1")
        self.assertEqual(json.loads(request.content), {"triplets": []})

    def test_search_namespace_encodes_query_params(self) -> None:
        seen: list[httpx.Request] = []
        with LbbClient("http://h", transport=capturing_transport(seen)) as client:
            client.search.hybrid(
                "customer identity",
                top_k=5,
                source="persisted",
                consistency="strong",
                targets=["concepts", "entities"],
            )
        params = dict(seen[0].url.params)
        self.assertEqual(str(seen[0].url).split("?")[0], "http://h/v1/search")
        self.assertEqual(params["query"], "customer identity")
        self.assertEqual(params["top_k"], "5")
        self.assertEqual(params["source"], "persisted")
        self.assertEqual(params["consistency"], "strong")
        self.assertEqual(params["targets"], "concepts,entities")

    def test_search_namespace_is_callable_for_quick_hybrid_search(self) -> None:
        seen: list[httpx.Request] = []
        with LbbClient("http://h", transport=capturing_transport(seen)) as client:
            client.search("customer identity", top_k=5, source="persisted")

        self.assertEqual(str(seen[0].url).split("?")[0], "http://h/v1/search")
        self.assertEqual(
            dict(seen[0].url.params),
            {"query": "customer identity", "top_k": "5", "source": "persisted"},
        )

    def test_entities_namespace_encodes_list_filters(self) -> None:
        seen: list[httpx.Request] = []
        with LbbClient("http://h", graph="g", transport=capturing_transport(seen)) as client:
            client.entities.list(type="SERVICE", limit=10, query="billing")
        params = dict(seen[0].url.params)
        self.assertEqual(str(seen[0].url).split("?")[0], "http://h/v1/graph/entities")
        self.assertEqual(params["graph"], "g")
        self.assertEqual(params["type"], "SERVICE")
        self.assertEqual(params["limit"], "10")
        self.assertEqual(params["q"], "billing")

    def test_sparql_select_posts_structured_body_with_group_keys(self) -> None:
        seen: list[httpx.Request] = []
        with LbbClient("http://h", graph="g", transport=capturing_transport(seen)) as client:
            client.sparql_select(
                {
                    "patterns": [
                        {
                            "subject": {"var": "c"},
                            "predicate": "TOUCHES",
                            "object": {"var": "comp"},
                        }
                    ],
                    "group_keys": [{"property": {"var": "c", "field": "area", "as": "area"}}],
                    "aggregates": [{"func": "count", "as": "n"}],
                }
            )
        request = seen[0]
        self.assertEqual(request.method, "POST")
        self.assertEqual(str(request.url).split("?")[0], "http://h/v1/query/sparql")
        body = json.loads(request.content)
        self.assertEqual(body["group_keys"][0]["property"]["field"], "area")

    def test_search_feedback_posts_labels_and_exports(self) -> None:
        seen: list[httpx.Request] = []
        with LbbClient("http://h", graph="g", transport=capturing_transport(seen)) as client:
            client.search_feedback(
                {
                    "query": "customer identity",
                    "search_id": "srch_1",
                    "labels": [
                        {
                            "target": {
                                "kind": "entity",
                                "entity": {"entity_type": "PERSON", "name": "ada"},
                            },
                            "rank": 1,
                            "grade": 3,
                        }
                    ],
                },
                idempotency_key="fb_1",
            )
            client.search_feedback_export()
        post = seen[0]
        self.assertEqual(post.method, "POST")
        self.assertEqual(str(post.url).split("?")[0], "http://h/v1/search/feedback")
        self.assertEqual(post.headers["idempotency-key"], "fb_1")
        self.assertEqual(json.loads(post.content)["search_id"], "srch_1")
        export = seen[1]
        self.assertEqual(export.method, "GET")
        self.assertEqual(str(export.url).split("?")[0], "http://h/v1/search/feedback/export")

    def test_ask_posts_question_and_returns_grounded_answer(self) -> None:
        seen: list[httpx.Request] = []
        answer = {
            "mode": "resident_planner",
            "answer": "user-db stores it.",
            "grounding": {"candidates": [], "constrained": True},
            "citations": [],
            "confidence": 0.82,
            "explain": {
                "timings": {"ground_ms": 1.0, "retrieve_ms": 2.0, "total_ms": 3.0},
                "narrowing": {"vocab_candidates": 3, "entities": 9, "assertions": 14},
                "embedding": "bge",
            },
            "snapshot": SNAPSHOT,
            "ask_id": "ask_1",
        }
        with LbbClient(
            "http://h", graph="g", transport=capturing_transport(seen, {"json": answer})
        ) as client:
            result = client.ask(
                {"question": "which databases store customer identity data?", "top_k": 8}
            )
        self.assertEqual(result["ask_id"], "ask_1")
        self.assertEqual(result["mode"], "resident_planner")
        request = seen[0]
        self.assertEqual(request.method, "POST")
        self.assertEqual(str(request.url).split("?")[0], "http://h/v1/ask")
        self.assertEqual(dict(request.url.params)["graph"], "g")
        self.assertNotIn("idempotency-key", request.headers)
        self.assertEqual(
            json.loads(request.content),
            {"question": "which databases store customer identity data?", "top_k": 8},
        )
        # The response validates against the generated typed model.
        AskResponse.model_validate(result)

    def test_context_namespace_returns_typed_models_without_raw_request(self) -> None:
        seen: list[httpx.Request] = []
        answer = {
            "mode": "resident_planner",
            "answer": "user-db stores it.",
            "grounding": {"candidates": [], "constrained": True},
            "citations": [],
            "confidence": 0.82,
            "explain": {
                "timings": {"ground_ms": 1.0, "retrieve_ms": 2.0, "total_ms": 3.0},
                "narrowing": {"vocab_candidates": 3, "entities": 9, "assertions": 14},
                "embedding": "bge",
            },
            "snapshot": SNAPSHOT,
            "ask_id": "ask_1",
        }
        with LbbClient(
            "http://h",
            graph="g",
            max_retries=1,
            retry_delay=0,
            transport=capturing_transport(
                seen,
                [
                    {"status": 503, "json": {"error": {"message": "retry"}}},
                    {"json": answer},
                ],
            ),
        ) as client:
            result = client.context.ask({"question": "what stores identity data?"})

        self.assertIsInstance(result, AskResponse)
        self.assertEqual(result.ask_id, "ask_1")
        self.assertEqual(len(seen), 2)
        self.assertEqual(str(seen[1].url).split("?")[0], "http://h/v1/ask")

    def test_typed_query_namespace_retries_read_only_post(self) -> None:
        seen: list[httpx.Request] = []
        with LbbClient(
            "http://h",
            graph="g",
            max_retries=1,
            retry_delay=0,
            transport=capturing_transport(
                seen,
                [
                    {"status": 503, "json": {"error": {"message": "retry"}}},
                    {"json": sparql_select_payload()},
                ],
            ),
        ) as client:
            result = client.query.structured({"patterns": [], "select": []})

        self.assertIsInstance(result, SparqlSelectResponse)
        self.assertEqual(len(seen), 2)
        self.assertEqual(str(seen[1].url).split("?")[0], "http://h/v1/query/sparql")

    def test_entity_iterator_follows_cursors_and_returns_typed_rows(self) -> None:
        seen: list[httpx.Request] = []
        first = entity_list_payload()
        first["has_more"] = True
        first["next_cursor"] = "cursor-2"
        first["total_count"] = 2
        second = entity_list_payload()
        second["data"][0]["id"] = "e2"
        second["data"][0]["name"] = "billing-service"
        second["total_count"] = 2
        with LbbClient(
            "http://h",
            graph="g",
            transport=capturing_transport(seen, [{"json": first}, {"json": second}]),
        ) as client:
            rows = list(client.entities.iter(limit=1))

        self.assertEqual([row.name for row in rows], ["auth-service", "billing-service"])
        self.assertTrue(all(isinstance(row, EntityExplorerRow) for row in rows))
        self.assertEqual(dict(seen[1].url.params)["cursor"], "cursor-2")

    def test_raw_response_exposes_retry_metadata_and_request_options(self) -> None:
        seen: list[httpx.Request] = []
        events: list[str] = []

        def on_request(request: httpx.Request) -> None:
            events.append(f"request:{request.method}")

        def on_response(response: httpx.Response) -> None:
            events.append(f"response:{response.status_code}")

        with LbbClient(
            "http://h",
            max_retries=0,
            retry_delay=0,
            transport=capturing_transport(
                seen,
                [
                    {"status": 503, "json": {"error": {"message": "retry"}}},
                    {"headers": {"x-request-id": "req_dx"}, "json": {"ok": True}},
                ],
            ),
            event_hooks={"request": [on_request], "response": [on_response]},
        ) as client:
            response = client.raw_request(
                "GET",
                "/health",
                options={"max_retries": 1, "headers": {"x-client-trace": "trace-1"}},
            )

        self.assertEqual(response.data, {"ok": True})
        self.assertEqual(response.request_id, "req_dx")
        self.assertEqual(response.attempts, 2)
        self.assertEqual(response.retry_count, 1)
        self.assertGreaterEqual(response.elapsed_ms, 0)
        self.assertEqual(seen[0].headers["x-client-trace"], "trace-1")
        self.assertEqual(seen[0].headers["user-agent"], f"littlebigbrain/{__version__}")
        self.assertEqual(events, ["request:GET", "response:503", "request:GET", "response:200"])

    def test_graph_edges_scopes_and_pages_entity_edges(self) -> None:
        seen: list[httpx.Request] = []
        with LbbClient("http://h", graph="g", transport=capturing_transport(seen)) as client:
            client.graph_edges(
                type="SERVICE",
                name="auth",
                direction="out",
                limit=150,
                offset=150,
                as_of_commit_seq=42,
            )
        request = seen[0]
        self.assertEqual(str(request.url).split("?")[0], "http://h/v1/graph/edges")
        params = dict(request.url.params)
        self.assertEqual(params["type"], "SERVICE")
        self.assertEqual(params["name"], "auth")
        self.assertEqual(params["direction"], "out")
        self.assertEqual(params["limit"], "150")
        self.assertEqual(params["offset"], "150")
        self.assertEqual(params["as_of_commit_seq"], "42")

    def test_commit_dry_run_sets_dry_run_and_no_idempotency_key(self) -> None:
        seen: list[httpx.Request] = []
        with LbbClient("http://h", graph="g", transport=capturing_transport(seen)) as client:
            client.commit_dry_run({"triplets": []})
        request = seen[0]
        self.assertEqual(request.method, "POST")
        self.assertEqual(str(request.url).split("?")[0], "http://h/v1/graph/commit")
        self.assertEqual(dict(request.url.params)["dry_run"], "true")
        self.assertNotIn("idempotency-key", request.headers)

    def test_entities_list_projects_fields_and_bulk_ids(self) -> None:
        seen: list[httpx.Request] = []
        with LbbClient("http://h", graph="g", transport=capturing_transport(seen)) as client:
            client.entities.list(fields=["title", "status"], ids=["abc", "def"])
        params = dict(seen[0].url.params)
        self.assertEqual(params["fields"], "title,status")
        self.assertEqual(params["ids"], "abc,def")

    def test_entities_filter_by_attributes_builds_structured_sparql(self) -> None:
        seen: list[httpx.Request] = []
        with LbbClient("http://h", graph="g", transport=capturing_transport(seen)) as client:
            client.entities.filter_by_attributes(
                patterns=[
                    {
                        "subject": {"var": "svc"},
                        "predicate": "WRITES_TO",
                        "object": {"var": "db"},
                    }
                ],
                where=[
                    {"field": "slo", "op": "ge", "value": 0.99},
                    {"var": "db", "field": "tier", "value": "prod"},
                ],
                select=["svc"],
                limit=25,
            )
        self.assertEqual(str(seen[0].url).split("?")[0], "http://h/v1/query/sparql")
        self.assertEqual(dict(seen[0].url.params), {"graph": "g"})
        self.assertEqual(
            json.loads(seen[0].content),
            {
                "patterns": [
                    {
                        "subject": {"var": "svc"},
                        "predicate": "WRITES_TO",
                        "object": {"var": "db"},
                    }
                ],
                "filters": [
                    {
                        "compare": {
                            "op": "ge",
                            "left": {"property": {"var": "svc", "field": "slo"}},
                            "right": {"value": {"f64": 0.99}},
                        }
                    },
                    {
                        "compare": {
                            "op": "eq",
                            "left": {"property": {"var": "db", "field": "tier"}},
                            "right": {"value": {"str": "prod"}},
                        }
                    },
                ],
                "select": ["svc"],
                "limit": 25,
            },
        )

    def test_ontology_view_counts_sets_query_param(self) -> None:
        seen: list[httpx.Request] = []
        with LbbClient("http://h", transport=capturing_transport(seen)) as client:
            client.ontology_view()
            client.ontology_view(counts=True)
        self.assertEqual(str(seen[0].url).split("?")[0], "http://h/v1/ontology")
        self.assertNotIn("counts", dict(seen[0].url.params))
        self.assertEqual(dict(seen[1].url.params)["counts"], "true")

    def test_schema_namespace_uses_v1_schema_routes(self) -> None:
        seen: list[httpx.Request] = []
        with LbbClient("http://h", graph="main", transport=capturing_transport(seen)) as client:
            client.schema.view()
            client.schema.view(audit=True)
            client.schema.preview({"desired_mode": "warn"})
            client.schema.publish({"preview_digest": "sha256:test", "desired_mode": "warn"})
            client.schema.audit()

        self.assertEqual(str(seen[0].url).split("?")[0], "http://h/v1/schema")
        self.assertEqual(dict(seen[0].url.params), {"graph": "main"})
        self.assertEqual(str(seen[1].url).split("?")[0], "http://h/v1/schema")
        self.assertEqual(dict(seen[1].url.params), {"graph": "main", "audit": "true"})
        self.assertEqual(str(seen[2].url).split("?")[0], "http://h/v1/schema/preview")
        self.assertEqual(json.loads(seen[2].content), {"desired_mode": "warn"})
        self.assertEqual(str(seen[3].url).split("?")[0], "http://h/v1/schema/publish")
        self.assertEqual(str(seen[4].url).split("?")[0], "http://h/v1/schema/audit")

    def test_facts_import_serializes_ndjson_with_params(self) -> None:
        seen: list[httpx.Request] = []
        with LbbClient(
            "http://h",
            transport=capturing_transport(seen, {"json": {"triplets": 1, "properties": 1}}),
        ) as client:
            result = client.graph("research").facts.import_ndjson(
                [
                    {
                        "source": {"type": "Author", "name": "Ada", "key": "orcid:1"},
                        "relation": "AFFILIATED_WITH",
                        "target": {"type": "University", "name": "Cambridge", "key": "ror:1"},
                    },
                    {
                        "type": "Author",
                        "name": "Ada",
                        "key": "orcid:1",
                        "properties": {"h_index": 52},
                    },
                ],
                batch=500,
                strict=True,
            )
        self.assertEqual(result["triplets"], 1)
        request = seen[0]
        self.assertEqual(str(request.url).split("?")[0], "http://h/v1/graph/import")
        self.assertEqual(dict(request.url.params)["graph"], "research")
        self.assertEqual(dict(request.url.params)["batch"], "500")
        self.assertEqual(dict(request.url.params)["strict"], "true")
        self.assertEqual(request.headers["content-type"], "application/x-ndjson")
        self.assertRegex(request.headers["idempotency-key"], r"^import:")
        lines = request.content.decode().split("\n")
        self.assertEqual(len(lines), 2)
        self.assertEqual(json.loads(lines[0])["relation"], "AFFILIATED_WITH")
        self.assertEqual(json.loads(lines[1])["properties"]["h_index"], 52)

    def test_facts_import_rdf_posts_ntriples_with_params(self) -> None:
        seen: list[httpx.Request] = []
        with LbbClient(
            "http://h",
            transport=capturing_transport(seen, {"json": {"imported_triplets": 1}}),
        ) as client:
            body = "<http://ex/s> <http://ex/p> <http://ex/o> .\n"
            result = client.graph("research").facts.import_rdf(
                body,
                batch=500,
                strict=True,
                blank_node_scope="document-42",
                resource_type="RdfResource",
                edge_idempotency="append",
            )
        self.assertEqual(result["imported_triplets"], 1)
        request = seen[0]
        self.assertEqual(str(request.url).split("?")[0], "http://h/v1/graph/import/rdf")
        params = dict(request.url.params)
        self.assertEqual(params["graph"], "research")
        self.assertEqual(params["batch"], "500")
        self.assertEqual(params["strict"], "true")
        self.assertEqual(params["format"], "ntriples")
        self.assertEqual(params["blank_node_scope"], "document-42")
        self.assertEqual(params["resource_type"], "RdfResource")
        self.assertEqual(params["edge_idempotency"], "append")
        self.assertEqual(request.headers["content-type"], "application/n-triples")
        self.assertRegex(request.headers["idempotency-key"], r"^import-rdf:")
        self.assertEqual(request.content.decode(), body)

    def test_import_rdf_keeps_published_ntriples_keyword_compatible(self) -> None:
        body = "<http://ex/s> <http://ex/p> <http://ex/o> .\n"
        for scoped in (False, True):
            seen: list[httpx.Request] = []
            with LbbClient(
                "http://h",
                transport=capturing_transport(seen, {"json": {"imported_triplets": 1}}),
            ) as client:
                if scoped:
                    result = client.graph("research").facts.import_rdf(ntriples=body)
                else:
                    result = client.import_rdf(ntriples=body)
            self.assertEqual(result["imported_triplets"], 1)
            self.assertEqual(seen[0].content.decode(), body)
            self.assertEqual(seen[0].headers["content-type"], "application/n-triples")

    def test_facts_import_rdf_supports_turtle_and_base_iri(self) -> None:
        seen: list[httpx.Request] = []
        with LbbClient(
            "http://h",
            transport=capturing_transport(seen, {"json": {"imported_triplets": 1}}),
        ) as client:
            body = "@prefix ex: <http://ex/> . ex:s ex:p ex:o ."
            result = client.graph("research").facts.import_rdf(
                body,
                format="turtle",
                base_iri="http://base/",
                graph_uri="http://ex/graph",
            )
        self.assertEqual(result["imported_triplets"], 1)
        request = seen[0]
        params = dict(request.url.params)
        self.assertEqual(params["format"], "turtle")
        self.assertEqual(params["base_iri"], "http://base/")
        self.assertEqual(params["graph_uri"], "http://ex/graph")
        self.assertEqual(request.headers["content-type"], "text/turtle")

    def test_graph_export_rdf_returns_text(self) -> None:
        seen: list[httpx.Request] = []
        turtle = "<http://ex/s> <http://ex/p> <http://ex/o> <http://ex/g> .\n"
        with LbbClient(
            "http://h",
            transport=capturing_transport(
                seen,
                {
                    "text": turtle,
                    "headers": {"content-type": "application/n-quads; charset=utf-8"},
                },
            ),
        ) as client:
            result = client.graph("research", branch="draft").export_rdf(
                format="nquads",
                max_triples=500,
                as_of_commit_seq=7,
            )
        self.assertEqual(result, turtle)
        request = seen[0]
        self.assertEqual(str(request.url).split("?")[0], "http://h/v1/graph/export/rdf")
        params = dict(request.url.params)
        self.assertEqual(params["graph"], "research")
        self.assertEqual(params["branch"], "draft")
        self.assertEqual(params["max_triples"], "500")
        self.assertEqual(params["as_of_commit_seq"], "7")
        self.assertEqual(params["format"], "nquads")

    def test_graph_retract_posts_edges(self) -> None:
        seen: list[httpx.Request] = []
        with LbbClient(
            "http://h",
            transport=capturing_transport(seen, {"json": {"retracted_edges": 1}}),
        ) as client:
            result = client.graph("research").retract(
                {"entities": [{"type": "Author", "name": "Garen"}]}
            )
        self.assertEqual(result["retracted_edges"], 1)
        request = seen[0]
        self.assertEqual(str(request.url).split("?")[0], "http://h/v1/graph/retract")
        self.assertEqual(dict(request.url.params)["graph"], "research")
        self.assertIn("idempotency-key", request.headers)

    def test_raw_request_returns_metadata(self) -> None:
        seen: list[httpx.Request] = []
        with LbbClient(
            "http://h",
            transport=capturing_transport(
                seen,
                {
                    "json": {"ok": True},
                    "headers": {"x-request-id": "req_py", "lbb-version": "2026-06-22"},
                },
            ),
        ) as client:
            response = client.raw_request("GET", "/v1/status")
        self.assertEqual(response.data, {"ok": True})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.request_id, "req_py")
        self.assertEqual(response.version, "2026-06-22")

    def test_raw_response_and_route_model_helpers_validate_generated_models(self) -> None:
        seen: list[httpx.Request] = []
        with LbbClient(
            "http://h",
            graph="main",
            transport=capturing_transport(
                seen,
                [
                    {"json": summary_payload()},
                    {"json": summary_payload()},
                    {"json": schema_view_payload()},
                    {"json": sparql_select_payload()},
                ],
            ),
        ) as client:
            raw = client.raw_request("GET", "/v1/graph/summary")
            summary = raw.model(GraphSummaryResponse)
            summary_again = client.summary_model()
            schema = client.schema.view_model()
            rows = client.entities.filter_by_attributes_model(
                patterns=[
                    {
                        "subject": {"var": "svc"},
                        "predicate": "WRITES_TO",
                        "object": {"var": "db"},
                    }
                ],
                where={"field": "slo", "op": "ge", "value": 0.99},
                select=["svc"],
                limit=25,
            )

        self.assertIsInstance(summary, GraphSummaryResponse)
        self.assertEqual(summary.entity_count, 2)
        self.assertIsInstance(summary_again, GraphSummaryResponse)
        self.assertIsInstance(schema, SchemaBundleView)
        self.assertFalse(schema.audit_summary.conforms)
        self.assertIsInstance(rows, SparqlSelectResponse)
        self.assertEqual(rows.vars, ["svc"])
        self.assertEqual(str(seen[2].url).split("?")[0], "http://h/v1/schema")
        self.assertEqual(str(seen[3].url).split("?")[0], "http://h/v1/query/sparql")

    def test_list_page_helpers_validate_entity_and_edge_rows(self) -> None:
        seen: list[httpx.Request] = []
        with LbbClient(
            "http://h",
            graph="main",
            transport=capturing_transport(
                seen,
                [
                    {"json": entity_list_payload()},
                    {"json": edge_list_payload()},
                ],
            ),
        ) as client:
            entities = client.entities.list_page(fields="*")
            edges = client.graph_edges_page(type="SERVICE", name="auth-service", direction="out")

        self.assertIsInstance(entities, ListPage)
        self.assertIsInstance(entities.data[0], EntityExplorerRow)
        assert entities.data[0].attributes is not None
        self.assertEqual(entities.data[0].attributes["slo"], 0.999)
        self.assertIsInstance(edges, ListPage)
        self.assertIsInstance(edges.data[0], GraphEdgeRow)
        self.assertEqual(edges.data[0].relation.name, "WRITES_TO")
        self.assertEqual(dict(seen[0].url.params)["fields"], "*")
        self.assertEqual(dict(seen[1].url.params)["direction"], "out")

    def test_retries_retryable_failures(self) -> None:
        seen: list[httpx.Request] = []
        with LbbClient(
            "http://h",
            max_retries=1,
            retry_delay=0,
            transport=capturing_transport(
                seen,
                [
                    {"status": 503, "json": {"error": {"message": "retry", "code": "api"}}},
                    {"json": {"ok": True}},
                ],
            ),
        ) as client:
            client.status()
        self.assertEqual(len(seen), 2)

    def test_retry_after_header_controls_safe_retry_delay(self) -> None:
        seen: list[httpx.Request] = []
        with patch("lbb._sync_client.time.sleep") as sleep:
            with LbbClient(
                "http://h",
                max_retries=1,
                retry_delay=0.1,
                transport=capturing_transport(
                    seen,
                    [
                        {"status": 429, "headers": {"retry-after": "2"}, "json": {}},
                        {"json": {"ok": True}},
                    ],
                ),
            ) as client:
                client.status()
        sleep.assert_called_once_with(2.0)

    def test_retries_read_only_post_searches(self) -> None:
        calls = (
            ("graph_search", "/v1/graph/search"),
            ("multi_search", "/v1/search/multi"),
            ("full_text_search", "/v1/search/full-text"),
            ("embedding_search", "/v1/search/embedding"),
        )
        for method_name, path in calls:
            with self.subTest(method=method_name):
                seen: list[httpx.Request] = []
                with LbbClient(
                    "http://h",
                    max_retries=1,
                    retry_delay=0,
                    transport=capturing_transport(
                        seen,
                        [
                            {"status": 429, "headers": {"retry-after": "0"}, "json": {}},
                            {"json": {"ok": True}},
                        ],
                    ),
                ) as client:
                    result = getattr(client, method_name)({"query": "identity"})
                self.assertEqual(result, {"ok": True})
                self.assertEqual(len(seen), 2)
                self.assertEqual(str(seen[0].url).split("?")[0], f"http://h{path}")

    def test_retries_body_based_hybrid_search(self) -> None:
        seen: list[httpx.Request] = []
        with LbbClient(
            "http://h",
            max_retries=1,
            retry_delay=0,
            transport=capturing_transport(
                seen,
                [
                    {"status": 503, "json": {"error": {"message": "retry"}}},
                    {"json": {"ok": True}},
                ],
            ),
        ) as client:
            result = client.search.hybrid({"query": "identity"})
        self.assertEqual(result, {"ok": True})
        self.assertEqual(len(seen), 2)

    def test_invalid_success_json_includes_response_context(self) -> None:
        with LbbClient(
            "http://h",
            transport=capturing_transport(
                [],
                {"status": 200, "text": "not-json", "headers": {"x-request-id": "req_json"}},
            ),
        ) as client:
            with self.assertRaisesRegex(ValueError, r"HTTP 200 \(request req_json\)"):
                client.status()

    def test_does_not_retry_unsafe_writes_without_idempotency_key(self) -> None:
        seen: list[httpx.Request] = []
        with LbbClient(
            "http://h",
            max_retries=1,
            retry_delay=0,
            transport=capturing_transport(
                seen,
                [
                    {"status": 503, "json": {"error": {"message": "retry", "code": "api"}}},
                    {"json": {"ok": True}},
                ],
            ),
        ) as client:
            with self.assertRaises(LbbError):
                client.delete_graph(confirm="main")
        self.assertEqual(len(seen), 1)

    def test_retries_idempotency_keyed_writes(self) -> None:
        seen: list[httpx.Request] = []
        with LbbClient(
            "http://h",
            max_retries=1,
            retry_delay=0,
            transport=capturing_transport(
                seen,
                [
                    {"status": 503, "json": {"error": {"message": "retry", "code": "api"}}},
                    {"json": {"ok": True}},
                ],
            ),
        ) as client:
            client.graph("main").facts.create({"triplets": []}, idempotency_key="retry-safe")
        self.assertEqual(len(seen), 2)
        self.assertEqual(seen[0].headers["idempotency-key"], "retry-safe")
        self.assertEqual(seen[1].headers["idempotency-key"], "retry-safe")

    def test_non_2xx_raises_structured_lbb_error(self) -> None:
        with LbbClient(
            "http://h",
            transport=capturing_transport(
                [],
                {
                    "status": 401,
                    "json": {
                        "error": {
                            "type": "auth_error",
                            "code": "unauthorized",
                            "message": "missing bearer",
                            "request_id": "req_body",
                        }
                    },
                },
            ),
        ) as client:
            with self.assertRaises(LbbError) as ctx:
                client.status()
        self.assertEqual(ctx.exception.status_code, 401)
        self.assertEqual(ctx.exception.code, "unauthorized")
        self.assertEqual(ctx.exception.type, "auth_error")
        self.assertEqual(ctx.exception.request_id, "req_body")
        self.assertEqual(str(ctx.exception), "missing bearer")

    def test_sparql_posts_text_and_parses_select_rows(self) -> None:
        seen: list[httpx.Request] = []
        envelope = {
            "results": json.dumps(
                {
                    "head": {"vars": ["s", "o"]},
                    "results": {
                        "bindings": [
                            {
                                "s": {"type": "uri", "value": "https://littlebigbrain.com/e/a"},
                                "o": {"type": "literal", "value": "Acme"},
                            },
                            # Sparse row: `o` is unbound and omitted per the spec.
                            {"s": {"type": "uri", "value": "https://littlebigbrain.com/e/b"}},
                        ]
                    },
                }
            ),
            "row_page": {"returned": 2, "total": 2, "offset": 0, "limit": 50, "has_more": False},
        }
        with LbbClient(
            "http://h",
            graph="main",
            transport=capturing_transport(seen, {"json": envelope}),
        ) as client:
            results = client.sparql("SELECT ?s ?o WHERE { ?s ?p ?o }", reason=True, limit=50)

        # The request is a POST of the query text plus the engine options.
        request = seen[0]
        self.assertEqual(request.method, "POST")
        self.assertEqual(str(request.url).split("?")[0], "http://h/v1/query/sparql-text")
        self.assertEqual(dict(request.url.params), {"graph": "main"})
        self.assertEqual(
            json.loads(request.content),
            {"query": "SELECT ?s ?o WHERE { ?s ?p ?o }", "reason": True, "limit": 50},
        )
        # The envelope's results string is parsed into typed bindings + flat rows.
        self.assertEqual(results.vars, ["s", "o"])
        self.assertIsNone(results.boolean)
        self.assertEqual(len(results), 2)
        self.assertEqual(
            results.rows(),
            [
                {"s": "https://littlebigbrain.com/e/a", "o": "Acme"},
                {"s": "https://littlebigbrain.com/e/b"},
            ],
        )
        self.assertEqual(list(results), results.rows())
        assert results.row_page is not None
        self.assertEqual(results.row_page["total"], 2)

    def test_sparql_parses_ask_boolean(self) -> None:
        seen: list[httpx.Request] = []
        envelope = {
            "results": json.dumps({"head": {}, "boolean": True}),
            "row_page": {"returned": 0, "total": 0, "offset": 0, "limit": 0, "has_more": False},
        }
        with LbbClient(
            "http://h", transport=capturing_transport(seen, {"json": envelope})
        ) as client:
            results = client.sparql("ASK { ?s ?p ?o }")
        self.assertTrue(results.boolean)
        self.assertEqual(results.rows(), [])

    def test_sparql_select_posts_structured_body(self) -> None:
        seen: list[httpx.Request] = []
        with LbbClient(
            "http://h",
            graph="main",
            transport=capturing_transport(seen, {"json": {"vars": ["s"], "solutions": []}}),
        ) as client:
            client.sparql_select({"patterns": [], "select": ["s"], "limit": 5})
        request = seen[0]
        self.assertEqual(request.method, "POST")
        self.assertEqual(str(request.url).split("?")[0], "http://h/v1/query/sparql")
        self.assertEqual(json.loads(request.content), {"patterns": [], "select": ["s"], "limit": 5})


class AsyncClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_create_graph_returns_typed_response(self) -> None:
        payload = {"commit_seq": 0, "graph": GRAPH, "ontology_version": 1}
        async with AsyncLbbClient(
            "http://h", transport=capturing_transport([], {"json": payload})
        ) as client:
            result = await client.create_graph()
        self.assertIsInstance(result, CreateGraphResponse)

    async def test_async_retries_retryable_failures(self) -> None:
        seen: list[httpx.Request] = []
        async with AsyncLbbClient(
            "http://h",
            max_retries=1,
            retry_delay=0,
            transport=capturing_transport(
                seen,
                [
                    {"status": 503, "json": {"error": {"message": "retry"}}},
                    {"json": {"ok": True}},
                ],
            ),
        ) as client:
            await client.status()
        self.assertEqual(len(seen), 2)

    async def test_async_roundtrip_and_scope(self) -> None:
        seen: list[httpx.Request] = []
        async with AsyncLbbClient(
            "http://h",
            api_key="k",
            graph="g",
            transport=capturing_transport(seen, {"json": {"state": []}}),
        ) as client:
            result = await client.current_state({"entity": {"entity_type": "SERVICE", "name": "x"}})
        self.assertEqual(result, {"state": []})
        self.assertEqual(str(seen[0].url).split("?")[0], "http://h/v1/query/state")
        self.assertEqual(seen[0].headers["authorization"], "Bearer k")

    async def test_async_sparql_parses_rows(self) -> None:
        seen: list[httpx.Request] = []
        envelope = {
            "results": json.dumps(
                {
                    "head": {"vars": ["s"]},
                    "results": {"bindings": [{"s": {"type": "uri", "value": "x"}}]},
                }
            ),
            "row_page": {"returned": 1, "total": 1, "offset": 0, "limit": 50, "has_more": False},
        }
        async with AsyncLbbClient(
            "http://h", graph="g", transport=capturing_transport(seen, {"json": envelope})
        ) as client:
            results = await client.sparql("SELECT ?s WHERE { ?s ?p ?o }")
        self.assertEqual(str(seen[0].url).split("?")[0], "http://h/v1/query/sparql-text")
        self.assertEqual(results.rows(), [{"s": "x"}])

    async def test_async_model_and_page_helpers(self) -> None:
        seen: list[httpx.Request] = []
        async with AsyncLbbClient(
            "http://h",
            graph="g",
            transport=capturing_transport(
                seen,
                [
                    {"json": summary_payload()},
                    {"json": entity_list_payload()},
                ],
            ),
        ) as client:
            summary = await client.summary_model()
            page = await client.entities.list_page(fields=["slo"])

        self.assertIsInstance(summary, GraphSummaryResponse)
        self.assertEqual(summary.current_edge_count, 3)
        self.assertIsInstance(page, ListPage)
        self.assertIsInstance(page.data[0], EntityExplorerRow)
        self.assertEqual(dict(seen[1].url.params)["fields"], "slo")

    async def test_async_entity_iterator_follows_cursors(self) -> None:
        seen: list[httpx.Request] = []
        first = entity_list_payload()
        first["has_more"] = True
        first["next_cursor"] = "cursor-2"
        second = entity_list_payload()
        second["data"][0]["id"] = "e2"
        second["data"][0]["name"] = "billing-service"
        async with AsyncLbbClient(
            "http://h",
            graph="g",
            transport=capturing_transport(seen, [{"json": first}, {"json": second}]),
        ) as client:
            rows = [row async for row in client.entities.iter(limit=1)]

        self.assertEqual([row.name for row in rows], ["auth-service", "billing-service"])
        self.assertEqual(dict(seen[1].url.params)["cursor"], "cursor-2")


if __name__ == "__main__":
    unittest.main()
