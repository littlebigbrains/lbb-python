"""Unit tests for the HTTP client using httpx's MockTransport (no server)."""

from __future__ import annotations

import json
import unittest
from typing import Any
from unittest.mock import patch

import httpx
from pydantic import ValidationError

import lbb.models as model_module
from lbb import (
    RESET,
    AsyncLbbClient,
    LbbCapabilityError,
    LbbClient,
    LbbError,
    QueryAskResult,
    RetryEvent,
    __version__,
)
from lbb.models import (
    AddEntityTypeOp,
    AdditiveOntologyEvolveRequest,
    CreateGraphResponse,
    GraphDeleteResponse,
    GraphForkResponse,
    GraphReloadResponse,
    GraphSummaryResponse,
    OntologyChangeSuggestion,
    OntologyChangeSuggestionList,
    OntologyDraft,
    OntologyEvolveRequest,
    SchemaBundleView,
    SearchFeedbackExportResponse,
    SearchFeedbackSummaryResponse,
    SparqlSelectResponse,
    TrainModelJobStatusResponse,
    TripletCommitFile,
    WidenRelationOp,
    WorkflowInstanceDeleteResponse,
    WorkflowSignalRequest,
)

SNAPSHOT = {"commit_seq": 7, "compacted_seq": 7}
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


def publication_status_payload(
    state: str,
    *,
    head_seq: int = 7,
    target_seq: int = 7,
    published_seq: int = 0,
    stage: str | None = None,
) -> dict[str, Any]:
    return {
        "state": state,
        "epoch": 1,
        "head_seq": head_seq,
        "head_generation": head_seq,
        "target_seq": target_seq,
        "target_head_generation": target_seq,
        "published_seq": published_seq,
        "published_generation": published_seq or None,
        "lag_commits": max(0, target_seq - published_seq),
        "current_stage": stage,
        "last_progress_at_micros": 42,
        "retry": {
            "retry_after_ms": 0,
            "eventual_read_available": published_seq > 0,
            "message": "wait" if state != "blocked" else "inspect the failed job",
        },
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


def backfill_status_payload(status: str) -> dict[str, Any]:
    result = None
    if status == "succeeded":
        result = {
            "batches": 2,
            "continuation": None,
            "embedded": 8,
            "entities_total": 10,
            "failed": 0,
            "final_index_job_id": "index-1",
            "index_lineage": None,
            "indexed_commit_seq": 7,
            "missing": 1,
            "model_id": "stored",
            "processed": 10,
            "skipped": 1,
            "source_commit_seq": 7,
            "source_snapshot_token": "snapshot:7",
            "truncated": False,
        }
    return {
        "attempts": 1,
        "enqueued_at_micros": 1,
        "graph": GRAPH,
        "idempotency_key": "backfill-1",
        "job_id": "backfill-job-1",
        "progress": None,
        "result": result,
        "status": status,
        "terminal_error": None,
        "updated_at_micros": 2,
    }


def sparql_text_envelope(snapshot: dict[str, Any] | None = None) -> dict[str, Any]:
    """A one-row ``/v1/query/sparql-text`` envelope, with an optional snapshot."""
    envelope: dict[str, Any] = {
        "results": json.dumps(
            {
                "head": {"vars": ["s"]},
                "results": {"bindings": [{"s": {"type": "uri", "value": "x"}}]},
            }
        ),
        "row_page": {
            "returned": 1,
            "total": 1,
            "offset": 0,
            "limit": 50,
            "has_more": False,
        },
    }
    if snapshot is not None:
        envelope["snapshot"] = snapshot
    return envelope


def search_sparql_envelope() -> dict[str, Any]:
    """A ``/v1/query/sparql-text`` envelope of a query with a search triple."""
    envelope = sparql_text_envelope()
    envelope["search"] = {
        "plan": "filter_first",
        "top": 3,
        "hits": 1,
        "complete": True,
        "allowed": 25,
        "candidates": 25,
        "rounds": 1,
        "clusters_probed": 4,
        "entries_considered": 25,
        "embeddings": ["product"],
        "model_id": "openai/text-embedding-3-small",
        "lag_commits": 0,
        "timings": {"total_ms": 17},
        "usage": {"texts": 1, "tokens_estimate": 4, "cost_usd_estimate": 0},
    }
    envelope["trace_id"] = "tr_1"
    return envelope


def entity_detail_payload() -> dict[str, Any]:
    entity = {"id": "e1", "type": "Ticket", "name": "Login fails"}
    return {
        "snapshot": SNAPSHOT,
        "entity": entity,
        "attributes": {"priority": "high"},
        "metadata": {"entity": entity, "snapshot": SNAPSHOT, "object_kind": "unavailable"},
        "current_state": [],
        "outgoing": [],
        "incoming": [],
        "history": [],
        "observations": [],
        "rdf_relations": {"outgoing": [], "incoming": []},
        "unavailable_sections": ["history"],
    }


def capturing_transport(
    captured: list[httpx.Request],
    responses: ResponseSpec | list[ResponseSpec] | None = None,
) -> httpx.MockTransport:
    queue = list(
        responses if isinstance(responses, list) else [responses or {"json": {}}]
    )

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
    def test_generated_models_exclude_retired_request_time_shacl_dtos(self) -> None:
        retired = [
            "ShaclQueryRequest",
            "ShaclNodeShape",
            "ShaclValidationReport",
            "ShaclViolation",
        ]
        self.assertEqual([name for name in retired if hasattr(model_module, name)], [])

    def test_durable_import_capability_gates_and_streams(self) -> None:
        seen: list[httpx.Request] = []
        produced = 0

        def lines() -> Any:
            nonlocal produced
            produced += 1
            yield {"type": "Service", "name": "api", "properties": {}}
            produced += 1
            yield b'{"type":"Service","name":"db","properties":{}}'

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            if request.url.path == "/version":
                return httpx.Response(
                    200, json={"capabilities": ["durable_import_jobs_v1"]}
                )
            request.read()
            return httpx.Response(
                202,
                json={
                    "job_id": "import:1",
                    "state": "queued",
                    "idempotent_replay": False,
                    "upload_bytes": len(request.content),
                },
            )

        with LbbClient("http://h", transport=httpx.MockTransport(handler)) as client:
            accepted = client.submit_import_ndjson(lines(), idempotency_key="source:1")

        self.assertEqual(accepted.job_id, "import:1")
        self.assertEqual(produced, 2)
        self.assertEqual(seen[1].headers["idempotency-key"], "source:1")
        self.assertEqual(seen[1].headers["content-type"], "application/x-ndjson")
        self.assertEqual(len(seen[1].content.splitlines()), 2)

    def test_durable_import_does_not_fallback_without_capability(self) -> None:
        seen: list[httpx.Request] = []
        with LbbClient(
            "http://h",
            transport=capturing_transport(seen, {"json": {"capabilities": []}}),
        ) as client:
            with self.assertRaises(LbbCapabilityError):
                client.submit_import_ndjson([], idempotency_key="source:2")
        self.assertEqual([request.url.path for request in seen], ["/version"])

    def test_durable_import_rejects_empty_source_before_post(self) -> None:
        seen: list[httpx.Request] = []
        with LbbClient(
            "http://h",
            transport=capturing_transport(
                seen, {"json": {"capabilities": ["durable_import_jobs_v1"]}}
            ),
        ) as client:
            with self.assertRaisesRegex(ValueError, "requires at least one NDJSON"):
                client.submit_import_ndjson([], idempotency_key="source:empty")
        self.assertEqual([request.url.path for request in seen], ["/version"])

    def test_metadata_exposes_only_bounded_index_detail_option(self) -> None:
        seen: list[httpx.Request] = []
        with LbbClient(
            "http://h", graph="g", transport=capturing_transport(seen)
        ) as client:
            client.metadata()
            client.metadata(
                include_indexes=False,
            )

        self.assertEqual(dict(seen[0].url.params), {"graph": "g"})
        self.assertEqual(
            dict(seen[1].url.params),
            {
                "graph": "g",
                "include_indexes": "false",
            },
        )

    def test_create_graph_uses_http_scope_and_returns_typed_response(self) -> None:
        seen: list[httpx.Request] = []
        payload = {
            "commit_seq": 0,
            "graph": {
                "tenant_id": "tenant",
                "graph_id": "research",
                "branch_id": "main",
            },
            "ontology_version": 1,
        }
        with LbbClient(
            "http://h",
            graph="research",
            transport=capturing_transport(seen, {"json": payload}),
        ) as client:
            result = client.create_graph()

        self.assertIsInstance(result, CreateGraphResponse)
        self.assertEqual(result.graph.graph_id, "research")
        self.assertEqual(seen[0].method, "POST")
        self.assertEqual(str(seen[0].url).split("?")[0], "http://h/v1/graph/create")
        self.assertEqual(dict(seen[0].url.params), {"graph": "research"})

    def test_namespace_facts_create_injects_auth_scope_version_and_idempotency(
        self,
    ) -> None:
        seen: list[httpx.Request] = []
        with LbbClient(
            "http://h:7400/",
            api_key="lbb_sk_test",
            transport=capturing_transport(
                seen, {"json": {"commit": {"commit_seq": 1}}}
            ),
        ) as client:
            result = client.graph("main").facts.create(
                {"triplets": []}, idempotency_key="ik_py_1"
            )

        self.assertEqual(result["commit"]["commit_seq"], 1)
        request = seen[0]
        self.assertEqual(request.method, "POST")
        self.assertEqual(
            str(request.url).split("?")[0], "http://h:7400/v1/graph/commit"
        )
        self.assertEqual(dict(request.url.params), {"graph": "main"})
        self.assertEqual(request.headers["authorization"], "Bearer lbb_sk_test")
        self.assertEqual(request.headers["lbb-version"], "2026-07-23")
        self.assertEqual(request.headers["idempotency-key"], "ik_py_1")
        self.assertEqual(json.loads(request.content), {"triplets": []})

    def test_facts_create_accepts_flat_entity_properties_in_the_model(self) -> None:
        # The flat `{ field: value }` map validates as the generated model and
        # reaches the wire unchanged, next to the verbose `{field, value}` list.
        ticket = {"type": "Ticket", "key": "4821", "name": "login fails after 3.1"}
        flat = {
            "description": "Sent back to the login screen.",
            "priority": 2,
            "score": 0.5,
            "open": True,
            "labels": ["auth", "3.1"],
            "builds": [310, 311],
        }
        verbose = [{"field": "priority", "value": {"i64": 2}}]
        body = TripletCommitFile.model_validate(
            {
                "entity_properties": [
                    {**ticket, "properties": flat},
                    {**ticket, "properties": verbose},
                ]
            }
        )
        seen: list[httpx.Request] = []
        with LbbClient(
            "http://h",
            transport=capturing_transport(
                seen, {"json": {"commit": {"commit_seq": 1}}}
            ),
        ) as client:
            client.graph("main").facts.create(body, idempotency_key="ik_flat")

        sent = json.loads(seen[0].content)["entity_properties"]
        self.assertEqual(sent[0]["properties"], flat)
        self.assertEqual(sent[1]["properties"], verbose)
        # A flat value is a scalar or an array, never a typed object.
        with self.assertRaises(ValidationError):
            TripletCommitFile.model_validate(
                {
                    "entity_properties": [
                        {**ticket, "properties": {"priority": {"i64": 2}}}
                    ]
                }
            )

    def test_removed_query_surfaces_are_absent_from_the_client(self) -> None:
        with LbbClient("http://h") as client:
            for name in (
                "search",
                "context",
                "graph_search",
                "multi_search",
                "full_text_search",
                "embedding_search",
                "analytics",
                "vocab_export",
                "embedding_config",
                "backfill_embeddings",
                "promote_embedding",
                "delete_branch",
                "merge_branch",
                "observe",
                "planner_dataset",
                "planner_preference_dataset",
                "promote_planner",
            ):
                self.assertFalse(
                    hasattr(client, name), f"LbbClient must not expose {name}"
                )
            self.assertFalse(hasattr(client.query, "analytics"))
            scoped = client.graph("g")
            for name in (
                "embedding_config",
                "backfill_embeddings",
                "promote_embedding",
                "delete_branch",
            ):
                self.assertFalse(
                    hasattr(scoped, name), f"graph namespace must not expose {name}"
                )

    def test_base_family_read_methods_are_removed(self) -> None:
        # Their routes answered 429 on every graph and are gone from the server.
        with LbbClient("http://h") as client:
            for name in ("current_state", "history", "why", "governed_conflicts"):
                self.assertFalse(hasattr(client, name), f"{name} must be gone")
            self.assertFalse(hasattr(client.entities, "sample"))
            self.assertFalse(hasattr(client.query, "conflicts"))

    def test_ontology_evolve_models_have_stable_discriminated_names(self) -> None:
        op = AddEntityTypeOp(op="add_entity_type", name="CUSTOMER")
        request = AdditiveOntologyEvolveRequest(ops=[op])
        self.assertEqual(
            request.model_dump(mode="json"),
            {"ops": [{"name": "CUSTOMER", "op": "add_entity_type"}]},
        )

        with self.assertRaises(ValidationError) as raised:
            OntologyEvolveRequest.model_validate(
                {"ops": [{"kind": "add_entity_type", "name": "CUSTOMER"}]}
            )
        errors = raised.exception.errors()
        self.assertEqual(len(errors), 1, errors)
        self.assertIn("op", errors[0]["msg"])

    def test_sparql_select_posts_structured_body_with_group_keys(self) -> None:
        seen: list[httpx.Request] = []
        with LbbClient(
            "http://h", graph="g", transport=capturing_transport(seen)
        ) as client:
            client.sparql_select(
                {
                    "patterns": [
                        {
                            "subject": {"var": "c"},
                            "predicate": "TOUCHES",
                            "object": {"var": "comp"},
                        }
                    ],
                    "group_keys": [
                        {"property": {"var": "c", "field": "area", "as": "area"}}
                    ],
                    "aggregates": [{"func": "count", "as": "n"}],
                }
            )
        request = seen[0]
        self.assertEqual(request.method, "POST")
        self.assertEqual(str(request.url).split("?")[0], "http://h/v1/query/sparql")
        body = json.loads(request.content)
        self.assertEqual(body["group_keys"][0]["property"]["field"], "area")

    def test_a5_read_your_writes_floor_and_default_consistency(self) -> None:
        """A5: the write→floor→read loop, plus min_indexed_seq/consistency threading
        and the client-level default consistency."""
        seen: list[httpx.Request] = []
        responses = [
            {"json": {"commit_seq": 128, "snapshot_token": "t", "op_count": 1}},
            {"json": {"snapshot": SNAPSHOT, "vars": [], "solutions": []}},
            {"json": summary_payload()},
            {"json": {"conforms": True, "result_count": 0}},
        ]
        with LbbClient(
            "http://h",
            graph="g",
            transport=capturing_transport(seen, responses),
            default_consistency="strong",
        ) as client:
            commit_seq = client.commit({"triplets": []})["commit_seq"]
            # The floor threads onto the structured-SPARQL body.
            client.sparql_select({"patterns": []}, min_indexed_seq=commit_seq)
            # On the summary (URL) route the floor + default consistency ride the
            # query string.
            client.summary(min_indexed_seq=commit_seq)
            # Durable conformance uses the same client default, but has no
            # read-your-writes floor: strong waits for an exact report.
            client.ontology_conformance()

        self.assertEqual(commit_seq, 128)
        sparql_body = json.loads(seen[1].content)
        self.assertEqual(sparql_body["min_indexed_seq"], 128)
        # sparql_select did not set consistency explicitly, so the client default
        # is folded into the body.
        self.assertEqual(sparql_body["consistency"], "strong")
        summary_params = dict(seen[2].url.params)
        self.assertEqual(summary_params["min_indexed_seq"], "128")
        self.assertEqual(summary_params["consistency"], "strong")
        conformance_params = dict(seen[3].url.params)
        self.assertEqual(
            conformance_params,
            {"graph": "g", "consistency": "strong"},
        )

    def test_a5_per_call_consistency_wins_over_default(self) -> None:
        seen: list[httpx.Request] = []
        with LbbClient(
            "http://h",
            graph="g",
            transport=capturing_transport(
                seen, [{"json": {"snapshot": SNAPSHOT, "results": []}}]
            ),
            default_consistency="strong",
        ) as client:
            client.sparql_select({"patterns": []}, consistency="eventual")
        self.assertEqual(json.loads(seen[0].content)["consistency"], "eventual")

    def test_search_feedback_posts_labels_and_exports(self) -> None:
        seen: list[httpx.Request] = []
        export_payload = {
            "graph": {"tenant_id": "default", "graph_id": "g", "branch_id": "main"},
            "feedback_graph": {
                "tenant_id": "default",
                "graph_id": "__lbb_feedback",
                "branch_id": "main",
            },
            "rows": [],
            "counts": {
                "raw_events": 0,
                "deduped_events": 0,
                "positives": 0,
                "hard_negatives": 0,
                "ignored": 0,
                "train": 0,
                "eval": 0,
                "excluded_targets": 0,
            },
        }
        with LbbClient(
            "http://h",
            graph="g",
            transport=capturing_transport(
                seen, [{"json": {}}, {"json": export_payload}]
            ),
        ) as client:
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
            exported = client.search_feedback_export()
        self.assertIsInstance(exported, SearchFeedbackExportResponse)
        self.assertEqual(exported.counts.excluded_targets, 0)
        post = seen[0]
        self.assertEqual(post.method, "POST")
        self.assertEqual(str(post.url).split("?")[0], "http://h/v1/search/feedback")
        self.assertEqual(post.headers["idempotency-key"], "fb_1")
        self.assertEqual(json.loads(post.content)["search_id"], "srch_1")
        export = seen[1]
        self.assertEqual(export.method, "GET")
        self.assertEqual(
            str(export.url).split("?")[0], "http://h/v1/search/feedback/export"
        )

    def test_search_feedback_summary_returns_constant_size_model(self) -> None:
        seen: list[httpx.Request] = []
        payload = {
            "graph": {"tenant_id": "default", "graph_id": "g", "branch_id": "main"},
            "feedback_graph": {
                "tenant_id": "default",
                "graph_id": "__lbb_feedback",
                "branch_id": "main",
            },
            "raw_events": 12,
            "deduped_events": 10,
            "grades": {"grade_0": 2, "grade_1": 1, "grade_2": 3, "grade_3": 4},
            "splits": {"train": 8, "eval": 2},
            "excluded_targets": 1,
            "latest_label_sequence": 12,
            "latest_label_micros": 123456,
            "promoted_models": [{"kind": "fusion", "run": 7}],
            "objects_scanned": 12,
            "truncated": False,
        }
        with LbbClient(
            "http://h",
            graph="g",
            transport=capturing_transport(seen, {"json": payload}),
        ) as client:
            summary = client.search_feedback_summary()

        self.assertIsInstance(summary, SearchFeedbackSummaryResponse)
        self.assertEqual(summary.latest_label_sequence, 12)
        self.assertEqual(summary.promoted_models[0].run, 7)
        self.assertEqual(
            str(seen[0].url).split("?")[0], "http://h/v1/search/feedback/summary"
        )

    def test_schema_namespace_returns_typed_models(self) -> None:
        seen: list[httpx.Request] = []
        with LbbClient(
            "http://h",
            graph="g",
            transport=capturing_transport(
                seen,
                [
                    {"json": schema_view_payload()},
                    {
                        "json": {
                            "graph": GRAPH,
                            "ontology_version": 3,
                            "shapes_version": 2,
                            "enforce_mode": "warn",
                            "activated": True,
                            "audit": {"conforms": True, "result_count": 0},
                            "messages": [],
                        }
                    },
                ],
            ),
        ) as client:
            schema = client.schema.view_model()
            published = client.schema.publish_model(
                {
                    "desired_mode": "warn",
                    "shapes": {"source": "@prefix sh: <http://www.w3.org/ns/shacl#> ."},
                },
                idempotency_key="schema-1",
            )

        self.assertIsInstance(schema, SchemaBundleView)
        self.assertTrue(published.activated)
        self.assertEqual(
            [str(request.url).split("?")[0] for request in seen],
            [
                "http://h/v1/schema",
                "http://h/v1/schema/publish",
            ],
        )
        self.assertEqual(seen[1].headers["idempotency-key"], "schema-1")

    def test_preserved_model_dataset_routes(self) -> None:
        seen: list[httpx.Request] = []
        with LbbClient(
            "http://h",
            graph="g",
            transport=capturing_transport(
                seen,
                [{"json": {}} for _ in range(3)],
            ),
        ) as client:
            client.shadow_eval({"queries": [], "challenger": {}})
            client.suggest_dataset(limit=12, split_seq=9)
            client.extractor_dataset(limit=13, split_seq=10)

        self.assertEqual(
            [str(request.url).split("?")[0] for request in seen],
            [
                "http://h/v1/models/shadow-eval",
                "http://h/v1/models/suggest-dataset",
                "http://h/v1/models/extractor-dataset",
            ],
        )

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
        self.assertEqual(
            events, ["request:GET", "response:503", "request:GET", "response:200"]
        )

    def test_retryable_false_body_short_circuits_retries(self) -> None:
        # A 429 the server marks terminal in the body (`retryable: false`, e.g. a
        # durable quota rejection) is surfaced at once, not retried to the budget.
        seen: list[httpx.Request] = []
        with LbbClient(
            "http://h",
            max_retries=5,
            retry_delay=0,
            transport=capturing_transport(
                seen,
                [
                    {
                        "status": 429,
                        "json": {
                            "error": {
                                "code": "training_budget_exceeded",
                                "retryable": False,
                            }
                        },
                    },
                    {"json": {"ok": True}},
                ],
            ),
        ) as client:
            with self.assertRaises(LbbError) as ctx:
                client.raw_request("GET", "/v1/status")
        self.assertEqual(ctx.exception.status_code, 429)
        self.assertIs(ctx.exception.retryable, False)
        self.assertEqual(len(seen), 1)  # terminal body ⇒ no retry

    def test_deadline_budget_binds_before_max_retries(self) -> None:
        # A retry_budget_ms shorter than the advertised Retry-After stops the loop
        # before the count cap: the server suggests 5s, the budget is 0, so the
        # first 429 surfaces without burning any of the five allowed retries.
        seen: list[httpx.Request] = []
        with LbbClient(
            "http://h",
            max_retries=5,
            retry_delay=0,
            retry_budget_ms=0,
            transport=capturing_transport(
                seen,
                [
                    {
                        "status": 429,
                        "headers": {"retry-after": "5"},
                        "json": {"error": {"code": "ingest_busy"}},
                    },
                    {"json": {"ok": True}},
                ],
            ),
        ) as client:
            with self.assertRaises(LbbError) as ctx:
                client.raw_request("GET", "/v1/status")
        self.assertEqual(ctx.exception.status_code, 429)
        self.assertEqual(len(seen), 1)  # deadline bound the retry, not the count

    def test_naked_lb_5xx_is_retried_with_backoff(self) -> None:
        # A bare LB 502 with an HTML body (no error envelope) is a transient
        # server_busy-equivalent: retried, then the recovered success is returned.
        seen: list[httpx.Request] = []
        with LbbClient(
            "http://h",
            max_retries=3,
            retry_delay=0,
            transport=capturing_transport(
                seen,
                [
                    {"status": 502, "text": "<html>502 Bad Gateway</html>"},
                    {"json": {"ok": True}},
                ],
            ),
        ) as client:
            result = client.raw_request("GET", "/v1/status")
        self.assertEqual(result.data, {"ok": True})
        self.assertEqual(result.attempts, 2)
        self.assertEqual(len(seen), 2)

    def test_retry_after_body_field_used_when_header_absent(self) -> None:
        # With no Retry-After *header*, the backoff honors the server's body hint
        # `error.retry_after_seconds` rather than blind jitter.
        from lbb._client_base import _retry_delay_seconds

        response = httpx.Response(
            503,
            json={"error": {"code": "ingest_busy", "retry_after_seconds": 4}},
        )
        self.assertEqual(_retry_delay_seconds(response, 0.1, 0), 4.0)

    def test_jittered_backoff_is_bounded(self) -> None:
        # Full-jitter exponential: uniform(0, base * 2**attempt), capped at 60s.
        from lbb._client_base import _jittered_backoff

        for attempt in range(6):
            ceiling = min(0.5 * (2**attempt), 60.0)
            for _ in range(50):
                delay = _jittered_backoff(0.5, attempt)
                self.assertGreaterEqual(delay, 0.0)
                self.assertLessEqual(delay, ceiling)

    def test_on_retry_hook_surfaces_absorbed_retries(self) -> None:
        # The ergonomic surface hides retries (returns only .data); the on_retry
        # hook makes each absorbed retry observable.
        events: list = []
        seen: list[httpx.Request] = []
        with LbbClient(
            "http://h",
            max_retries=3,
            retry_delay=0,
            on_retry=events.append,
            transport=capturing_transport(
                seen,
                [
                    {"status": 429, "json": {"error": {"code": "ingest_busy"}}},
                    {"json": {"ok": True}},
                ],
            ),
        ) as client:
            result = client.raw_request("GET", "/v1/status")
        self.assertEqual(result.attempts, 2)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].status_code, 429)
        self.assertEqual(events[0].error_code, "ingest_busy")
        self.assertEqual(events[0].attempt, 1)
        self.assertGreaterEqual(events[0].delay_seconds, 0.0)

    def test_commit_dry_run_sets_dry_run_and_no_idempotency_key(self) -> None:
        seen: list[httpx.Request] = []
        with LbbClient(
            "http://h", graph="g", transport=capturing_transport(seen)
        ) as client:
            client.commit_dry_run({"triplets": []})
        request = seen[0]
        self.assertEqual(request.method, "POST")
        self.assertEqual(str(request.url).split("?")[0], "http://h/v1/graph/commit")
        self.assertEqual(dict(request.url.params)["dry_run"], "true")
        self.assertNotIn("idempotency-key", request.headers)

    def test_entities_filter_by_attributes_builds_structured_sparql(self) -> None:
        seen: list[httpx.Request] = []
        with LbbClient(
            "http://h", graph="g", transport=capturing_transport(seen)
        ) as client:
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

    def test_ontology_evolve_dry_run_is_typed_and_explicit(self) -> None:
        seen: list[httpx.Request] = []
        payload = {
            "graph": GRAPH,
            "base_ontology_version": 1,
            "ontology_version": 2,
            "dry_run": True,
            "publishable": True,
            "no_op": False,
            "applied": [],
            "messages": ["dry run"],
        }
        with LbbClient(
            "http://h",
            transport=capturing_transport(seen, {"json": payload}),
        ) as client:
            result = client.ontology.evolve(
                {"ops": [{"op": "add_entity_type", "name": "CUSTOMER"}]},
                dry_run=True,
            )
        self.assertTrue(result.dry_run)
        self.assertTrue(result.publishable)
        self.assertFalse(result.no_op)
        self.assertEqual(dict(seen[0].url.params)["dry_run"], "true")

    def test_model_body_leaves_unset_optional_fields_off_the_wire(self) -> None:
        # The server decodes `add_range` and `allow_data_conflicts` as
        # `#[serde(default)]` Vec / bool and answers 400 to an explicit null.
        seen: list[httpx.Request] = []
        payload = {
            "graph": GRAPH,
            "base_ontology_version": 1,
            "ontology_version": 2,
            "dry_run": False,
            "publishable": True,
            "no_op": False,
            "applied": [],
            "messages": [],
        }
        with LbbClient(
            "http://h", transport=capturing_transport(seen, {"json": payload})
        ) as client:
            client.ontology.evolve(
                OntologyEvolveRequest(
                    ops=[
                        WidenRelationOp(
                            op="widen_relation", relation="R", add_domain=["T"]
                        )
                    ]
                )
            )
        body = json.loads(seen[0].content)
        self.assertEqual(
            body,
            {"ops": [{"op": "widen_relation", "relation": "R", "add_domain": ["T"]}]},
        )

        def null_keys(value: Any, path: str = "$") -> list[str]:
            if isinstance(value, dict):
                return [
                    found
                    for key, item in value.items()
                    for found in (
                        [f"{path}.{key}"]
                        if item is None
                        else null_keys(item, f"{path}.{key}")
                    )
                ]
            if isinstance(value, list):
                return [
                    found
                    for index, item in enumerate(value)
                    for found in null_keys(item, f"{path}[{index}]")
                ]
            return []

        self.assertEqual(null_keys(body), [])

    def test_model_body_keeps_null_in_a_required_json_value(self) -> None:
        # `value` is a required `serde_json::Value`: null is a valid signal
        # value, and an absent field answers 400 `missing field`.
        seen: list[httpx.Request] = []
        with LbbClient("http://h", transport=capturing_transport(seen)) as client:
            client.raw_request(
                "POST",
                "/v1/workflows/signal",
                body=WorkflowSignalRequest(run_id="run-1", name="approve", value=None),
            )
        self.assertEqual(
            json.loads(seen[0].content),
            {"name": "approve", "run_id": "run-1", "value": None},
        )

    def test_ontology_draft_lifecycle_is_typed_and_retry_safe(self) -> None:
        seen: list[httpx.Request] = []
        payload = {
            "draft_id": "draft-1",
            "graph": GRAPH,
            "status": "validated",
            "base_snapshot": SNAPSHOT,
            "base_ontology_version": 3,
            "request": {
                "connector_name": "finance",
                "user_stories": [],
                "competency_questions": [],
                "selected_patterns": [],
                "samples": [{"evidence_ref": "record-1", "record": {"id": 1}}],
            },
            "evidence_refs": ["record-1"],
            "proposed_ops": [{"op": "add_entity_type", "name": "CONNECTOR_FINANCE"}],
            "cq_analyses": [],
            "cq_coverage": 1.0,
            "superfluous_element_rate": 0.0,
            "structural_pitfalls": [],
            "confidence": 0.05,
        }
        with LbbClient(
            "http://h",
            transport=capturing_transport(seen, [{"json": payload}] * 5),
        ) as client:
            created = client.ontology.draft_create(payload["request"])
            fetched = client.ontology.draft_get("draft-1")
            validated = client.ontology.draft_validate("draft-1")
            promoted = client.ontology.draft_promote(
                "draft-1", idempotency_key="promote-draft-1"
            )
            rejected = client.ontology.draft_reject("draft-1", "not selected")
        for result in [created, fetched, validated, promoted, rejected]:
            self.assertIsInstance(result, OntologyDraft)
        self.assertEqual(seen[1].url.params["draft_id"], "draft-1")
        self.assertEqual(seen[3].headers["idempotency-key"], "promote-draft-1")
        self.assertEqual(seen[4].url.params["reason"], "not selected")

    def test_ontology_suggestion_lifecycle_is_typed(self) -> None:
        seen: list[httpx.Request] = []
        suggestion = {
            "suggestion_id": "sg_1",
            "graph": GRAPH,
            "key": "hubspot-main/deals",
            "status": "open",
            "title": "Add class Deal",
            "anchor": {"kind": "ontology"},
            "origin": {"kind": "integration", "id": "hubspot-main"},
            "change": [{"op": "add_entity_type", "name": "Deal"}],
            "evidence": {"records": 311},
            "comments": [],
            "revision": 1,
            "created_at": "2026-09-29T10:00:00Z",
            "updated_at": "2026-09-29T10:00:00Z",
        }
        listing = {
            "graph": GRAPH,
            "ontology_version": 7,
            "counts": {"open": 1, "accepted": 0, "dismissed": 0, "superseded": 0},
            "suggestions": [],
            "truncated": False,
        }
        with LbbClient(
            "http://h",
            transport=capturing_transport(
                seen, [{"json": listing}] + [{"json": suggestion}] * 7
            ),
        ) as client:
            listed = client.ontology.suggestions(status="open", limit=10)
            created = client.ontology.suggestion_create(
                {
                    "title": "Add class Deal",
                    "origin": {"kind": "integration", "id": "hubspot-main"},
                    "change": [{"op": "add_entity_type", "name": "Deal"}],
                }
            )
            fetched = client.ontology.suggestion_get("sg_1")
            validated = client.ontology.suggestion_validate("sg_1")
            accepted = client.ontology.suggestion_accept("sg_1")
            dismissed = client.ontology.suggestion_dismiss(
                "sg_1", "not now", author="ana"
            )
            superseded = client.ontology.suggestion_supersede("sg_1", "field gone")
            commented = client.ontology.suggestion_comment("sg_1", "why?")
        self.assertIsInstance(listed, OntologyChangeSuggestionList)
        for result in [
            created,
            fetched,
            validated,
            accepted,
            dismissed,
            superseded,
            commented,
        ]:
            self.assertIsInstance(result, OntologyChangeSuggestion)
        self.assertEqual(seen[0].url.params["status"], "open")
        self.assertEqual(seen[0].url.params["limit"], "10")
        self.assertEqual(seen[2].url.path, "/v1/ontology/suggestions/detail")
        self.assertEqual(seen[4].url.params["suggestion_id"], "sg_1")
        self.assertEqual(json.loads(seen[4].content), {})
        self.assertEqual(
            json.loads(seen[5].content), {"reason": "not now", "author": "ana"}
        )
        self.assertEqual(json.loads(seen[7].content), {"text": "why?"})

    def test_durable_trainer_submit_and_poll_are_typed(self) -> None:
        seen: list[httpx.Request] = []
        payload = {
            "job_id": "train_model:abc",
            "status": "pending",
            "graph": GRAPH,
            "kind": "fusion",
            "attempts": 0,
            "enqueued_at_micros": 10,
            "updated_at_micros": 10,
        }
        with LbbClient(
            "http://h",
            transport=capturing_transport(seen, {"json": payload}),
        ) as client:
            submitted = client.train_submit(
                {"kind": "fusion", "force": True},
                idempotency_key="fiqa-fusion-1",
            )
        with LbbClient(
            "http://h",
            transport=capturing_transport(seen, {"json": payload}),
        ) as client:
            polled = client.train_job(submitted.job_id)
        self.assertIsInstance(submitted, TrainModelJobStatusResponse)
        self.assertIsInstance(polled, TrainModelJobStatusResponse)
        self.assertEqual(seen[0].headers["idempotency-key"], "fiqa-fusion-1")
        self.assertEqual(dict(seen[1].url.params)["job_id"], "train_model:abc")

    def test_graph_deletion_is_typed(self) -> None:
        seen: list[httpx.Request] = []
        graph_payload = {
            "ok": True,
            "graph_id": "main",
            "deleted_objects": 10,
            "deleted_feedback_objects": 1,
            "deleted_bytes": 100,
        }
        with LbbClient(
            "http://h",
            graph="main",
            transport=capturing_transport(seen, {"json": graph_payload}),
        ) as client:
            deleted = client.delete_graph(confirm="main")
        self.assertIsInstance(deleted, GraphDeleteResponse)
        self.assertEqual(seen[0].method, "POST")
        self.assertEqual(str(seen[0].url).split("?")[0], "http://h/v1/graph/delete")
        self.assertEqual(dict(seen[0].url.params), {"graph": "main", "confirm": "main"})

    def test_workflow_instance_deletion_is_typed(self) -> None:
        seen: list[httpx.Request] = []
        with LbbClient(
            "http://h",
            graph="crm",
            transport=capturing_transport(seen, {"json": {"deleted": True}}),
        ) as client:
            deleted = client.workflow_delete_instance("hubspot-1")
        self.assertIsInstance(deleted, WorkflowInstanceDeleteResponse)
        self.assertTrue(deleted.deleted)
        self.assertEqual(seen[0].method, "POST")
        self.assertEqual(
            str(seen[0].url).split("?")[0], "http://h/v1/workflows/instances/delete"
        )
        self.assertEqual(dict(seen[0].url.params), {"graph": "crm"})
        self.assertEqual(json.loads(seen[0].content), {"workflow_id": "hubspot-1"})

    def test_facts_import_serializes_ndjson_with_params(self) -> None:
        seen: list[httpx.Request] = []
        with LbbClient(
            "http://h",
            transport=capturing_transport(
                seen, {"json": {"triplets": 1, "properties": 1}}
            ),
        ) as client:
            result = client.graph("research").facts.import_ndjson(
                [
                    {
                        "source": {"type": "Author", "name": "Ada", "key": "orcid:1"},
                        "relation": "AFFILIATED_WITH",
                        "target": {
                            "type": "University",
                            "name": "Cambridge",
                            "key": "ror:1",
                        },
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

    def test_facts_import_rdf_many_defers_intermediate_publications(self) -> None:
        seen: list[httpx.Request] = []
        responses = [
            {"json": {"imported_triplets": 1, "committed_commit_seq": seq}}
            for seq in (1, 2, 3)
        ]
        with LbbClient(
            "http://h", transport=capturing_transport(seen, responses)
        ) as client:
            result = client.graph("research").facts.import_rdf_many(
                ["<a> <p> <b> .", "<b> <p> <c> .", "<c> <p> <d> ."],
                idempotency_key="perritos",
            )
        self.assertEqual(result["final_sequence"], 3)
        self.assertEqual(
            [dict(item.url.params).get("build") for item in seen],
            ["false", "false", None],
        )
        self.assertEqual(
            [item.headers["idempotency-key"] for item in seen],
            ["perritos:1", "perritos:2", "perritos:3"],
        )

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

    def test_fork_graph_pins_confirm_to_destination_and_is_typed(self) -> None:
        seen: list[httpx.Request] = []
        payload = {
            "ok": True,
            "queued": True,
            "src_graph_id": "research",
            "dst_graph_id": "research-copy",
            "job_id": "graph_fork:abc",
            "poll": "GET /v1/graph/metadata?graph=research-copy",
        }
        with LbbClient(
            "http://h",
            graph="research",
            transport=capturing_transport(seen, {"json": payload}),
        ) as client:
            result = client.fork_graph("research", "research-copy")
        self.assertIsInstance(result, GraphForkResponse)
        self.assertEqual(result.dst_graph_id, "research-copy")
        request = seen[0]
        self.assertEqual(request.method, "POST")
        self.assertEqual(str(request.url).split("?")[0], "http://h/v1/graph/fork")
        params = dict(request.url.params)
        self.assertEqual(params["src"], "research")
        self.assertEqual(params["dst"], "research-copy")
        # confirm must equal the destination graph id to authorize the fork.
        self.assertEqual(params["confirm"], "research-copy")

    def test_reload_posts_ndjson_with_confirm_and_rollback_anchor(self) -> None:
        seen: list[httpx.Request] = []
        payload = {
            "dry_run": True,
            "lines_read": 2,
            "entities_added": 1,
            "entities_changed": 0,
            "entities_removed": 1,
            "edges_added": 1,
            "edges_changed": 0,
            "edges_removed": 0,
            "error_count": 0,
            "idempotency_key": "reload:xyz",
            "new_commit_seq": 8,
            "new_snapshot_token": "snap-new",
            "prior_commit_seq": 7,
            "prior_snapshot_token": "snap-old",
        }
        with LbbClient(
            "http://h",
            graph="main",
            transport=capturing_transport(seen, {"json": payload}),
        ) as client:
            result = client.reload(
                [
                    {
                        "source": {"type": "Author", "name": "Ada", "key": "orcid:1"},
                        "relation": "AFFILIATED_WITH",
                        "target": {
                            "type": "University",
                            "name": "Cambridge",
                            "key": "ror:1",
                        },
                    },
                    {
                        "type": "Author",
                        "name": "Ada",
                        "key": "orcid:1",
                        "properties": {"h_index": 52},
                    },
                ],
                confirm="main",
                dry_run=True,
                strict=True,
                observed_at="2026-07-20T00:00:00Z",
            )
        self.assertIsInstance(result, GraphReloadResponse)
        self.assertTrue(result.dry_run)
        # prior_* is the rollback anchor.
        self.assertEqual(result.prior_commit_seq, 7)
        self.assertEqual(result.prior_snapshot_token, "snap-old")
        request = seen[0]
        self.assertEqual(request.method, "POST")
        self.assertEqual(str(request.url).split("?")[0], "http://h/v1/graph/reload")
        params = dict(request.url.params)
        self.assertEqual(params["confirm"], "main")
        self.assertEqual(params["dry_run"], "true")
        self.assertEqual(params["strict"], "true")
        self.assertEqual(params["observed_at"], "2026-07-20T00:00:00Z")
        self.assertEqual(request.headers["content-type"], "application/x-ndjson")
        self.assertRegex(request.headers["idempotency-key"], r"^reload:")
        lines = request.content.decode().split("\n")
        self.assertEqual(len(lines), 2)
        self.assertEqual(json.loads(lines[0])["relation"], "AFFILIATED_WITH")
        self.assertEqual(json.loads(lines[1])["properties"]["h_index"], 52)

    def test_reload_accepts_prebuilt_ndjson_and_omits_unset_flags(self) -> None:
        seen: list[httpx.Request] = []
        payload = {
            "dry_run": False,
            "lines_read": 1,
            "entities_added": 0,
            "entities_changed": 0,
            "entities_removed": 0,
            "edges_added": 0,
            "edges_changed": 1,
            "edges_removed": 0,
            "error_count": 0,
            "idempotency_key": "reload:custom",
            "new_commit_seq": 2,
            "new_snapshot_token": "snap",
            "prior_commit_seq": 1,
            "prior_snapshot_token": "snap0",
        }
        ndjson = '{"type":"Author","name":"Ada","key":"orcid:1","properties":{}}'
        with LbbClient(
            "http://h",
            graph="main",
            transport=capturing_transport(seen, {"json": payload}),
        ) as client:
            client.reload(ndjson, confirm="main", idempotency_key="reload:custom")
        request = seen[0]
        self.assertEqual(request.content.decode(), ndjson)
        self.assertEqual(request.headers["idempotency-key"], "reload:custom")
        params = dict(request.url.params)
        # Unset flags are dropped from the query string.
        self.assertNotIn("dry_run", params)
        self.assertNotIn("strict", params)
        self.assertNotIn("observed_at", params)

    def test_raw_request_returns_metadata(self) -> None:
        seen: list[httpx.Request] = []
        with LbbClient(
            "http://h",
            transport=capturing_transport(
                seen,
                {
                    "json": {"ok": True},
                    "headers": {"x-request-id": "req_py", "lbb-version": "2026-07-23"},
                },
            ),
        ) as client:
            response = client.raw_request("GET", "/v1/status")
        self.assertEqual(response.data, {"ok": True})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.request_id, "req_py")
        self.assertEqual(response.version, "2026-07-23")

    def test_raw_response_and_route_model_helpers_validate_generated_models(
        self,
    ) -> None:
        seen: list[httpx.Request] = []
        with LbbClient(
            "http://h",
            graph="main",
            transport=capturing_transport(
                seen,
                [
                    {"json": summary_payload()},
                    {"json": summary_payload()},
                    {"json": sparql_select_payload()},
                ],
            ),
        ) as client:
            raw = client.raw_request("GET", "/v1/graph/summary")
            summary = raw.model(GraphSummaryResponse)
            summary_again = client.summary_model()
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
        self.assertIsInstance(rows, SparqlSelectResponse)
        self.assertEqual(rows.vars, ["svc"])
        self.assertEqual(str(seen[2].url).split("?")[0], "http://h/v1/query/sparql")

    def test_retries_retryable_failures(self) -> None:
        seen: list[httpx.Request] = []
        with LbbClient(
            "http://h",
            max_retries=1,
            retry_delay=0,
            transport=capturing_transport(
                seen,
                [
                    {
                        "status": 503,
                        "json": {"error": {"message": "retry", "code": "api"}},
                    },
                    {"json": {"ok": True}},
                ],
            ),
        ) as client:
            client.status()
        self.assertEqual(len(seen), 2)

    def test_error_exposes_retry_metadata(self) -> None:
        error = LbbError(
            503,
            "busy",
            {
                "code": "server_busy",
                "message": "the server is briefly busy; retry after the indicated delay",
                "retryable": True,
                "retry_after_seconds": 2,
            },
        )
        self.assertTrue(error.retryable)
        self.assertEqual(error.retry_after_seconds, 2)

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

    def test_invalid_success_json_includes_response_context(self) -> None:
        with LbbClient(
            "http://h",
            transport=capturing_transport(
                [],
                {
                    "status": 200,
                    "text": "not-json",
                    "headers": {"x-request-id": "req_json"},
                },
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
                    {
                        "status": 503,
                        "json": {"error": {"message": "retry", "code": "api"}},
                    },
                    {"json": {"ok": True}},
                ],
            ),
        ) as client:
            with self.assertRaises(LbbError):
                client.embeddings.delete("people")
        self.assertEqual(len(seen), 1)

    def test_retries_idempotent_whole_graph_delete(self) -> None:
        seen: list[httpx.Request] = []
        payload = {
            "ok": True,
            "graph_id": "main",
            "deleted_objects": 0,
            "deleted_feedback_objects": 0,
            "deleted_bytes": 0,
        }
        with LbbClient(
            "http://h",
            max_retries=1,
            retry_delay=0,
            transport=capturing_transport(
                seen,
                [
                    {
                        "status": 503,
                        "json": {"error": {"message": "retry", "code": "api"}},
                    },
                    {"json": payload},
                ],
            ),
        ) as client:
            result = client.delete_graph(confirm="main")
        self.assertTrue(result.ok)
        self.assertEqual(len(seen), 2)

    def test_retries_idempotency_keyed_writes(self) -> None:
        seen: list[httpx.Request] = []
        with LbbClient(
            "http://h",
            max_retries=1,
            retry_delay=0,
            transport=capturing_transport(
                seen,
                [
                    {
                        "status": 503,
                        "json": {"error": {"message": "retry", "code": "api"}},
                    },
                    {"json": {"ok": True}},
                ],
            ),
        ) as client:
            client.graph("main").facts.create(
                {"triplets": []}, idempotency_key="retry-safe"
            )
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

    def test_endpoint_error_preserves_code_and_migration_guidance(self) -> None:
        with LbbClient(
            "http://h",
            transport=capturing_transport(
                [],
                {
                    "status": 421,
                    "json": {
                        "error": {
                            "type": "routing_error",
                            "code": "stack_endpoint_required",
                            "message": "use the composite stack endpoint",
                        }
                    },
                },
            ),
        ) as client:
            with self.assertRaises(LbbError) as ctx:
                client.status()
        self.assertEqual(ctx.exception.status_code, 421)
        self.assertEqual(ctx.exception.code, "stack_endpoint_required")
        self.assertIn("endpoint_url", ctx.exception.endpoint_hint or "")

    def test_composite_endpoint_421_403_are_terminal(self) -> None:
        # Misdirection (421) and authorization (403) are not retryable by status
        # (only 429/5xx are). A retry would waste the budget and delay the
        # actionable endpoint hint, so a generous budget must be spent on exactly
        # ONE attempt. Pins the contract against retry-classification drift.
        for status, code in (
            (421, "stack_endpoint_required"),
            (403, "stack_endpoint_mismatch"),
        ):
            seen: list[httpx.Request] = []
            with LbbClient(
                "http://h",
                max_retries=5,
                retry_delay=0,
                transport=capturing_transport(
                    seen,
                    [
                        {
                            "status": status,
                            "json": {
                                "error": {
                                    "type": "routing_error",
                                    "code": code,
                                    "message": "misrouted",
                                }
                            },
                        }
                    ]
                    * 6,
                ),
            ) as client:
                with self.assertRaises(LbbError) as ctx:
                    client.status()
            self.assertEqual(ctx.exception.status_code, status)
            self.assertEqual(ctx.exception.code, code)
            self.assertIsNotNone(ctx.exception.endpoint_hint)
            self.assertEqual(len(seen), 1)  # terminal ⇒ no retry

    def test_sparql_posts_text_and_parses_select_rows(self) -> None:
        seen: list[httpx.Request] = []
        envelope = {
            "results": json.dumps(
                {
                    "head": {"vars": ["s", "o"]},
                    "results": {
                        "bindings": [
                            {
                                "s": {
                                    "type": "uri",
                                    "value": "https://littlebigbrain.com/e/a",
                                },
                                "o": {"type": "literal", "value": "Acme"},
                            },
                            # Sparse row: `o` is unbound and omitted per the spec.
                            {
                                "s": {
                                    "type": "uri",
                                    "value": "https://littlebigbrain.com/e/b",
                                }
                            },
                        ]
                    },
                }
            ),
            "row_page": {
                "returned": 2,
                "total": 2,
                "offset": 0,
                "limit": 50,
                "has_more": False,
            },
        }
        with LbbClient(
            "http://h",
            graph="main",
            transport=capturing_transport(seen, {"json": envelope}),
        ) as client:
            results = client.sparql(
                "SELECT ?s ?o WHERE { ?s ?p ?o }", reason=True, limit=50
            )

        # The request is a POST of the query text plus the engine options.
        request = seen[0]
        self.assertEqual(request.method, "POST")
        self.assertEqual(
            str(request.url).split("?")[0], "http://h/v1/query/sparql-text"
        )
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

    def test_sparql_cursor_preserves_continuation_and_snapshot(self) -> None:
        seen: list[httpx.Request] = []
        envelope = {
            "results": json.dumps(
                {"head": {"vars": ["s"]}, "results": {"bindings": []}}
            ),
            "next_cursor": "opaque-next",
            "snapshot": SNAPSHOT,
        }
        with LbbClient(
            "http://h", transport=capturing_transport(seen, {"json": envelope})
        ) as client:
            first = client.query.sparql("ordered query LIMIT 25", cursor="")
            client.sparql("ordered query LIMIT 25", cursor=first.next_cursor)
        self.assertEqual(first.next_cursor, "opaque-next")
        self.assertEqual(first.snapshot, SNAPSHOT)
        self.assertEqual(json.loads(seen[0].content)["cursor"], "")
        self.assertEqual(json.loads(seen[1].content)["cursor"], "opaque-next")

    def test_sparql_parses_ask_boolean(self) -> None:
        seen: list[httpx.Request] = []
        envelope = {
            "results": json.dumps({"head": {}, "boolean": True}),
            "row_page": {
                "returned": 0,
                "total": 0,
                "offset": 0,
                "limit": 0,
                "has_more": False,
            },
        }
        with LbbClient(
            "http://h", transport=capturing_transport(seen, {"json": envelope})
        ) as client:
            results = client.sparql("ASK { ?s ?p ?o }")
        self.assertTrue(results.boolean)
        self.assertEqual(results.rows(), [])

    def test_sparql_pins_as_of_commit_seq_and_exposes_snapshot(self) -> None:
        seen: list[httpx.Request] = []
        snapshot = {
            "commit_seq": 9,
            "compacted_seq": 9,
            "as_of_commit_seq": 4,
            "served_at_seq": 4,
        }
        with LbbClient(
            "http://h",
            graph="main",
            transport=capturing_transport(
                seen, {"json": sparql_text_envelope(snapshot=snapshot)}
            ),
        ) as client:
            results = client.query.sparql(
                "SELECT ?s WHERE { ?s ?p ?o }", as_of_commit_seq=4
            )
        self.assertEqual(
            json.loads(seen[0].content),
            {"query": "SELECT ?s WHERE { ?s ?p ?o }", "as_of_commit_seq": 4},
        )
        self.assertEqual(dict(seen[0].url.params), {"graph": "main"})
        self.assertEqual(results.snapshot, snapshot)
        self.assertEqual(results.rows(), [{"s": "x"}])

    def test_sparql_profile_asks_for_and_exposes_the_measurements(self) -> None:
        seen: list[httpx.Request] = []
        profile = {"total_ms": 2.5, "result_cache": "bypassed", "join_orders": []}
        envelope = {
            "results": json.dumps({"head": {"vars": []}, "results": {"bindings": []}}),
            "profile": profile,
        }
        with LbbClient(
            "http://h", transport=capturing_transport(seen, {"json": envelope})
        ) as client:
            profiled = client.sparql("SELECT * WHERE { ?s ?p ?o }", profile=True)
            client.query.sparql("SELECT * WHERE { ?s ?p ?o }")
        self.assertEqual(json.loads(seen[0].content)["profile"], True)
        self.assertNotIn("profile", json.loads(seen[1].content))
        self.assertEqual(profiled.profile, profile)

    def test_sparql_request_records_a_trace_and_search_reports_its_plan(self) -> None:
        seen: list[httpx.Request] = []
        query = (
            "PREFIX search: <https://littlebigbrain.com/search#> "
            'SELECT ?s WHERE { ?s search:similarTo "card payments" } LIMIT 3'
        )
        with LbbClient(
            "http://h",
            transport=capturing_transport(
                seen, [{"json": search_sparql_envelope()}, {"json": sparql_text_envelope()}]
            ),
        ) as client:
            found = client.sparql(query, request="card payments")
            plain = client.query.sparql(query)
        self.assertEqual(json.loads(seen[0].content)["request"], "card payments")
        self.assertNotIn("request", json.loads(seen[1].content))
        self.assertEqual(found.rows(), [{"s": "x"}])
        self.assertEqual(found.trace_id, "tr_1")
        assert found.search is not None
        self.assertEqual(found.search["plan"], "filter_first")
        self.assertTrue(found.search["complete"])
        self.assertIsNone(plain.search)
        self.assertIsNone(plain.trace_id)

    def test_query_update_posts_sparql_update_with_an_idempotency_key(self) -> None:
        seen: list[httpx.Request] = []
        update = (
            "INSERT DATA { <https://example.com/sku/2> "
            '<http://www.w3.org/2000/01/rdf-schema#label> "Road shoe" }'
        )
        with LbbClient(
            "http://h",
            graph="catalog",
            transport=capturing_transport(
                seen, [{"status": 204, "text": ""}, {"status": 204, "text": ""}]
            ),
        ) as client:
            answer = client.query.update(update)
            client.query.update(update, idempotency_key="catalog-2026-10-03")
        self.assertIsNone(answer)
        self.assertEqual(seen[0].method, "POST")
        self.assertEqual(str(seen[0].url), "http://h/update?graph=catalog")
        self.assertEqual(seen[0].headers["content-type"], "application/sparql-update")
        self.assertEqual(seen[0].content.decode(), update)
        self.assertTrue(seen[0].headers["idempotency-key"].startswith("sparql-update:"))
        self.assertEqual(seen[1].headers["idempotency-key"], "catalog-2026-10-03")

    def test_entities_detail_reads_one_record_at_a_commit(self) -> None:
        seen: list[httpx.Request] = []
        with LbbClient(
            "http://h",
            graph="support",
            default_consistency="strong",
            transport=capturing_transport(
                seen, [{"json": entity_detail_payload()}, {"json": entity_detail_payload()}]
            ),
        ) as client:
            raw = client.entities.detail(type="Ticket", key="4821", edges=50)
            typed = client.entities.detail_model(id="e1", as_of_commit_seq=7)
        self.assertEqual(raw["attributes"], {"priority": "high"})
        self.assertIsInstance(typed, model_module.EntityDetailResponse)
        self.assertEqual(typed.entity.name, "Login fails")
        self.assertEqual(seen[0].url.path, "/v1/graph/entity")
        self.assertEqual(
            dict(seen[0].url.params),
            {
                "graph": "support",
                "type": "Ticket",
                "key": "4821",
                "consistency": "strong",
                "edges": "50",
            },
        )
        self.assertEqual(seen[1].url.params["as_of_commit_seq"], "7")
        self.assertNotIn("as_of", seen[1].url.params)

    def test_planner_stats_reads_one_page(self) -> None:
        seen: list[httpx.Request] = []
        stats = {"served_at_seq": None, "predicates": [], "next_cursor": None}
        with LbbClient(
            "http://h", graph="main", transport=capturing_transport(seen, {"json": stats})
        ) as client:
            self.assertEqual(client.planner_stats(), stats)
            client.planner_stats(cursor="3a", limit=50)
            client.graph("other").planner_stats(limit=1)
        self.assertEqual(
            [str(request.url) for request in seen],
            [
                "http://h/v1/graph/planner-stats?graph=main",
                "http://h/v1/graph/planner-stats?graph=main&cursor=3a&limit=50",
                "http://h/v1/graph/planner-stats?graph=other&limit=1",
            ],
        )
        self.assertEqual({request.method for request in seen}, {"GET"})

    def test_sparql_retries_read_your_writes_pending_until_the_floor_is_served(
        self,
    ) -> None:
        # A read right after a write: the published generation does not cover
        # the floor yet, so the server answers 429 with a Retry-After. The
        # query is read-only, so the client waits and asks again.
        seen: list[httpx.Request] = []
        events: list[RetryEvent] = []
        with LbbClient(
            "http://h",
            graph="main",
            retry_delay=0,
            on_retry=events.append,
            transport=capturing_transport(
                seen,
                [
                    {
                        "status": 429,
                        "headers": {"retry-after": "0"},
                        "json": {
                            "error": {
                                "code": "read_your_writes_pending",
                                "retryable": True,
                            }
                        },
                    },
                    {
                        "json": sparql_text_envelope(
                            snapshot={
                                "commit_seq": 12,
                                "compacted_seq": 12,
                                "served_at_seq": 12,
                            }
                        )
                    },
                ],
            ),
        ) as client:
            results = client.sparql(
                "SELECT ?s WHERE { ?s ?p ?o }",
                consistency="eventual",
                min_indexed_seq=12,
            )
        self.assertEqual(len(seen), 2)
        self.assertEqual(
            dict(seen[1].url.params),
            {"graph": "main", "consistency": "eventual", "min_indexed_seq": "12"},
        )
        self.assertEqual(
            [event.error_code for event in events], ["read_your_writes_pending"]
        )
        assert results.snapshot is not None
        self.assertEqual(results.snapshot["served_at_seq"], 12)

    def test_sparql_does_not_retry_a_server_error_or_a_transport_failure(
        self,
    ) -> None:
        # Only a 429 is retried: the server refused the query before it ran. A
        # query that timed out (503) or a lost connection would run again.
        seen: list[httpx.Request] = []
        with LbbClient(
            "http://h",
            retry_delay=0,
            transport=capturing_transport(
                seen,
                [
                    {
                        "status": 503,
                        "json": {"error": {"code": "query_deadline_exceeded"}},
                    },
                    {"json": sparql_text_envelope()},
                ],
            ),
        ) as client:
            with self.assertRaises(LbbError) as ctx:
                client.sparql("SELECT ?s WHERE { ?s ?p ?o }")
        self.assertEqual(ctx.exception.code, "query_deadline_exceeded")
        self.assertEqual(len(seen), 1)

        attempts: list[httpx.Request] = []

        def refuse(request: httpx.Request) -> httpx.Response:
            attempts.append(request)
            raise httpx.ConnectError("connection refused", request=request)

        with LbbClient(
            "http://h", retry_delay=0, transport=httpx.MockTransport(refuse)
        ) as client:
            with self.assertRaises(httpx.ConnectError):
                client.sparql("SELECT ?s WHERE { ?s ?p ?o }")
        self.assertEqual(len(attempts), 1)

    def test_sparql_select_posts_structured_body(self) -> None:
        seen: list[httpx.Request] = []
        with LbbClient(
            "http://h",
            graph="main",
            transport=capturing_transport(
                seen, {"json": {"vars": ["s"], "solutions": []}}
            ),
        ) as client:
            client.sparql_select({"patterns": [], "select": ["s"], "limit": 5})
        request = seen[0]
        self.assertEqual(request.method, "POST")
        self.assertEqual(str(request.url).split("?")[0], "http://h/v1/query/sparql")
        self.assertEqual(
            json.loads(request.content), {"patterns": [], "select": ["s"], "limit": 5}
        )

    def test_wait_for_published_follows_server_managed_stages(self) -> None:
        seen: list[httpx.Request] = []
        with LbbClient(
            "http://h",
            transport=capturing_transport(
                seen,
                [
                    {"json": publication_status_payload("building", stage="rdf")},
                    {
                        "json": publication_status_payload(
                            "current", published_seq=7, stage=None
                        )
                    },
                ],
            ),
        ) as client:
            status = client.wait_for_published(7, poll_interval=0)
        self.assertEqual(status.state, model_module.PublicationState.current)
        self.assertEqual(status.published_seq, 7)
        self.assertEqual(
            [request.url.path for request in seen],
            [
                "/v1/graph/publication-status",
                "/v1/graph/publication-status",
            ],
        )

    def test_wait_for_published_surfaces_blocked_publication(self) -> None:
        with LbbClient(
            "http://h",
            transport=capturing_transport(
                [],
                {
                    "json": publication_status_payload(
                        "blocked", stage="verify published generation"
                    )
                },
            ),
        ) as client:
            with self.assertRaisesRegex(RuntimeError, "inspect the failed job"):
                client.wait_for_published(7, poll_interval=0)

    def test_wait_for_published_reports_lifecycle_watermarks_on_timeout(self) -> None:
        with LbbClient(
            "http://h",
            transport=capturing_transport(
                [],
                {
                    "json": publication_status_payload(
                        "building",
                        head_seq=9,
                        target_seq=9,
                        published_seq=7,
                        stage="verify_generation",
                    )
                },
            ),
        ) as client:
            with self.assertRaisesRegex(
                TimeoutError,
                "state=building, head=9, target=9, published=7, stage=verify_generation",
            ):
                client.wait_for_published(9, timeout=0, poll_interval=0)

    def test_graph_wait_for_published_keeps_the_graph_scope(self) -> None:
        seen: list[httpx.Request] = []
        with LbbClient(
            "http://h",
            transport=capturing_transport(
                seen,
                {"json": publication_status_payload("current", published_seq=7)},
            ),
        ) as client:
            status = client.graph("perritos").wait_for_published(7, poll_interval=0)
        self.assertEqual(status.published_seq, 7)
        self.assertEqual(dict(seen[0].url.params), {"graph": "perritos"})

    def test_activity_reads_the_graphs_background_work(self) -> None:
        payload = {
            "graph_id": "perritos",
            "epoch": 0,
            "observed_at_micros": 10,
            "idle": False,
            "head_seq": 3,
            "published_seq": 2,
            "target_seq": 3,
            "lag_commits": 1,
            "publication": "building",
            "write_limit": {
                "pending_commits": 1,
                "max_pending_commits": 256,
                "pending_bytes": 300,
                "max_pending_bytes": 4294967296,
            },
            "embeddings": [],
            "items": [
                {
                    "id": "graph-import:abc",
                    "kind": "import",
                    "state": "running",
                    "stage": "committing_group",
                    "subject": None,
                    "progress": {"done": 100, "total": 400, "unit": "bytes"},
                    "target_seq": None,
                    "attempts": 1,
                    "enqueued_at_micros": 1,
                    "updated_at_micros": 2,
                    "finished_at_micros": None,
                    "error": None,
                }
            ],
        }
        seen: list[httpx.Request] = []
        with LbbClient(
            "http://h",
            transport=capturing_transport(seen, [{"json": payload}, {"json": payload}]),
        ) as client:
            raw = client.graph("perritos").activity()
            typed = client.graph("perritos").activity_model()
        self.assertEqual(raw["items"][0]["kind"], "import")
        self.assertEqual(typed.publication, model_module.PublicationState.building)
        self.assertEqual(typed.items[0].progress.total, 400)
        self.assertEqual(
            [(request.method, request.url.path) for request in seen],
            [("GET", "/v1/graph/activity"), ("GET", "/v1/graph/activity")],
        )
        self.assertEqual(dict(seen[0].url.params), {"graph": "perritos"})

    def test_model_activity_reads_one_month_of_the_stacks_model_use(self) -> None:
        counters = {
            "calls": 2,
            "items": 300,
            "tokens_estimate": 9000,
            "cost_micro_usd": 180,
            "gpu_seconds": 0,
            "errors": 0,
            "last_at_ms": 1_790_000_000_000,
        }
        model = {
            "feature": "index",
            "provider": "openrouter",
            "model": "openai/text-embedding-3-small",
        }
        payload = {
            "month": "2026-09",
            "months": ["2026-09", "2026-10"],
            "managed": [{**model, "label": "OpenAI text-embedding-3-small"}],
            "totals": [{**model, **counters, "graphs": 1}],
            "by_day": [{**model, **counters, "day": "2026-09-30"}],
            "by_graph": [{**model, **counters, "graph": "main"}],
        }
        seen: list[httpx.Request] = []
        with LbbClient(
            "http://h",
            transport=capturing_transport(seen, [{"json": payload}, {"json": payload}]),
        ) as client:
            raw = client.model_activity(month="2026-09")
            typed = client.model_activity_model()
        self.assertEqual(raw["totals"][0]["items"], 300)
        self.assertEqual(typed.totals[0].feature, model_module.ModelActivityFeature.index)
        self.assertEqual(typed.by_graph[0].graph, "main")
        self.assertEqual(
            [(request.method, request.url.path) for request in seen],
            [("GET", "/v1/models/activity"), ("GET", "/v1/models/activity")],
        )
        self.assertEqual(seen[0].url.params.get("month"), "2026-09")
        self.assertNotIn("month", seen[1].url.params)


class SearchAndEvalsNamespaceTests(unittest.TestCase):
    """The search setup, search, and evals namespaces send the documented requests."""

    def _client(self, seen: list[httpx.Request]) -> LbbClient:
        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            return httpx.Response(200, json={"ok": True})

        return LbbClient("http://h", transport=httpx.MockTransport(handler))

    @staticmethod
    def _body(request: httpx.Request) -> dict[str, Any]:
        return json.loads(request.content or b"{}")

    def test_embeddings_namespace_routes(self) -> None:
        seen: list[httpx.Request] = []
        service = "https://x.test/class/service"
        with self._client(seen) as client:
            client.embeddings.list()
            client.embeddings.get("service")
            client.embeddings.declare(
                service,
                name="service",
                from_=["label", "calls/label"],
                exclude=["notes"],
                title="display_name",
                model="m",
                dim=8,
            )
            client.embeddings.preview(
                service, from_=["label"], title="label", sample=2, iris=["https://x.test/e/a"]
            )
            client.embeddings.set_model("openai/text-embedding-3-large", dim=3072)
            client.embeddings.refresh("service")
            client.embeddings.delete("service")
            client.embeddings.search(
                "fraud checks",
                embedding="service",
                filter_=[{"class": service}, {"via": "calls", "to": "payment-service"}],
                top_k=5,
                include=["text"],
                probe=12,
                request="which services check fraud?",
                explain=True,
                rerank=True,
            )
            client.embeddings.search_settings()
            client.embeddings.set_search_settings(rerank=False)
        routes = [(request.method, request.url.path) for request in seen]
        self.assertEqual(
            routes,
            [
                ("GET", "/v1/embeddings"),
                ("GET", "/v1/embeddings"),
                ("PUT", "/v1/embeddings"),
                ("POST", "/v1/embeddings/preview"),
                ("PUT", "/v1/embeddings/model"),
                ("POST", "/v1/embeddings/refresh"),
                ("DELETE", "/v1/embeddings"),
                ("POST", "/v1/search"),
                ("GET", "/v1/search/settings"),
                ("PUT", "/v1/search/settings"),
            ],
        )
        self.assertEqual(seen[1].url.params["name"], "service")
        self.assertEqual(
            self._body(seen[2]),
            {
                "class": service,
                "name": "service",
                "from": ["label", "calls/label"],
                "exclude": ["notes"],
                "title": "display_name",
                "model": "m",
                "dim": 8,
            },
        )
        self.assertEqual(self._body(seen[3])["title"], "label")
        self.assertEqual(self._body(seen[3])["sample"], 2)
        self.assertEqual(self._body(seen[3])["iris"], ["https://x.test/e/a"])
        self.assertEqual(self._body(seen[4]), {"model": "openai/text-embedding-3-large", "dim": 3072})
        self.assertEqual(seen[5].url.params["name"], "service")
        self.assertEqual(seen[6].url.params["confirm"], "service")
        search = self._body(seen[7])
        self.assertEqual(
            search["filter"],
            [{"class": service}, {"via": "calls", "to": "payment-service"}],
        )
        self.assertTrue(search["explain"])
        self.assertEqual(search["top_k"], 5)
        self.assertEqual(search["probe"], 12)
        self.assertEqual(search["include"], ["text"])
        self.assertEqual(search["request"], "which services check fraud?")
        self.assertIs(search["rerank"], True)
        self.assertEqual(self._body(seen[9]), {"rerank": False})

    def test_a_plain_search_sends_only_the_text(self) -> None:
        seen: list[httpx.Request] = []
        with self._client(seen) as client:
            client.embeddings.search("card payments")
        self.assertEqual(self._body(seen[0]), {"text": "card payments"})

    def test_evals_namespace_routes(self) -> None:
        seen: list[httpx.Request] = []
        with self._client(seen) as client:
            client.evals.summary()
            client.evals.traces(limit=5, unlabeled=True)
            client.evals.trace("t1")
            client.evals.judge(trace_id="t1", limit=3)
            client.evals.goldens()
            client.evals.accept_golden("g1", consistency="strong")
            client.evals.delete_golden("g1")
            client.evals.run(consistency="eventual")
            client.evals.results(limit=2)
            client.evals.settings()
            client.evals.set_settings({"judge": "auto"})
        routes = [(request.method, request.url.path) for request in seen]
        self.assertEqual(routes[0], ("GET", "/v1/evals"))
        self.assertIn(("GET", "/v1/evals/traces"), routes)
        self.assertIn(("GET", "/v1/evals/goldens"), routes)
        self.assertEqual(seen[1].url.params["limit"], "5")
        self.assertEqual(len(seen), 11)


CALL_ID = "00000001790000000000-n1-0000000001-0"


def model_check_payload(**overrides: Any) -> dict[str, Any]:
    """One check of a rerank call, as ``GET /v1/models/checks`` lists it."""
    usage = {
        "tokens_in": 8000,
        "tokens_out": 3000,
        "cache_read": 0,
        "cache_write": 0,
        "cost_micro_usd": 92000,
        "ms": 41000,
    }
    payload: dict[str, Any] = {
        "v": 1,
        "call": CALL_ID,
        "job": "rerank",
        "provider": "typesafe",
        "model": "jev-latest",
        "graph": "main",
        "call_at_ms": 1_790_000_000_000,
        "summary": "refund policy",
        "judge": {
            "provider": "anthropic",
            "model": "claude-opus-5-5",
            "effort": "xhigh",
            "rubric": "relevance/1",
            "verdict": "partly",
            "score": 0.62,
            "reason": "two of four hits answer the text",
            "usage": usage,
            "at_ms": 1_790_000_050_000,
        },
        "history": [],
        "truth": {"verdict": "partly", "score": 0.62, "by": "judge"},
    }
    payload.update(overrides)
    return payload


def tuning_session_payload(**overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "id": "t1",
        "status": "queued",
        "created_at_ms": 1_790_000_000_000,
        "queries_choose": 0,
        "queries_test": 0,
        "graded_pairs": 0,
        "judge_cost_micro_usd": 0,
    }
    payload.update(overrides)
    return payload


NDJSON = {"content-type": "application/x-ndjson"}
BUSY = {"status": 503, "json": {"error": {"message": "busy", "code": "api_error"}}}


class ModelChecksAndTuningTests(unittest.TestCase):
    """The model checks, search tuning and search settings send the documented requests."""

    def test_set_search_settings_sends_the_named_fields_and_reset_as_null(self) -> None:
        seen: list[httpx.Request] = []
        settings = {"rerank": True, "rerank_depth": 60, "rerank_available": True}
        with LbbClient(
            "http://h", graph="main", transport=capturing_transport(seen, {"json": settings})
        ) as client:
            written = client.embeddings.set_search_settings(
                rerank=True, rerank_depth=60, blend=RESET, probe_factor=2.0
            )
            client.embeddings.set_search_settings(probe_factor=RESET)
            client.embeddings.set_search_settings()
        self.assertEqual(written["rerank_depth"], 60)
        self.assertEqual(
            [(request.method, request.url.path) for request in seen],
            [("PUT", "/v1/search/settings")] * 3,
        )
        self.assertEqual(seen[0].url.params["graph"], "main")
        self.assertEqual(
            json.loads(seen[0].content),
            {"rerank": True, "rerank_depth": 60, "blend": None, "probe_factor": 2.0},
        )
        self.assertEqual(json.loads(seen[1].content), {"probe_factor": None})
        self.assertEqual(json.loads(seen[2].content), {})

    def test_search_tuning_starts_lists_reads_and_applies_sessions(self) -> None:
        seen: list[httpx.Request] = []
        done = tuning_session_payload(
            status="done",
            proposal={
                "variant": "r1v1",
                "settings": {"rerank": True, "rerank_depth": 60},
                "test": {
                    "baseline_ndcg_at_10": 0.7,
                    "ndcg_at_10": 0.8,
                    "delta": 0.1,
                    "ci_low": 0.02,
                    "ci_high": 0.18,
                    "queries": 14,
                },
                "latency_delta_ms": 40,
                "cost_delta_micro_usd_per_search": 0,
            },
        )
        responses = [
            {"json": tuning_session_payload()},
            {"json": tuning_session_payload(id="t2")},
            {"json": {"sessions": [done]}},
            {"json": done},
            {"json": {**done, "applied_at_ms": 1_790_000_100_000, "applied_by": "key:k1"}},
        ]
        with LbbClient(
            "http://h", graph="main", transport=capturing_transport(seen, responses)
        ) as client:
            queued = client.embeddings.search_tuning.start()
            client.embeddings.search_tuning.start(queries=12, rounds=1)
            listed = client.embeddings.search_tuning.list(limit=5)
            session = client.embeddings.search_tuning.get("t1")
            applied = client.embeddings.search_tuning.apply("t1")
        self.assertEqual(queued["status"], "queued")
        self.assertEqual(listed["sessions"][0]["id"], "t1")
        self.assertEqual(session["proposal"]["settings"]["rerank_depth"], 60)
        self.assertEqual(applied["applied_by"], "key:k1")
        self.assertEqual(
            [(request.method, request.url.path) for request in seen],
            [
                ("POST", "/v1/search/tuning"),
                ("POST", "/v1/search/tuning"),
                ("GET", "/v1/search/tuning"),
                ("GET", "/v1/search/tuning/get"),
                ("POST", "/v1/search/tuning/apply"),
            ],
        )
        self.assertEqual(json.loads(seen[0].content), {})
        self.assertEqual(json.loads(seen[1].content), {"queries": 12, "rounds": 1})
        self.assertEqual(dict(seen[2].url.params), {"graph": "main", "limit": "5"})
        self.assertEqual(dict(seen[3].url.params), {"graph": "main", "id": "t1"})
        self.assertEqual(dict(seen[4].url.params), {"graph": "main", "id": "t1"})
        self.assertEqual(seen[4].content, b"")

    def test_judge_budget_posts_are_not_retried_unless_asked(self) -> None:
        seen: list[httpx.Request] = []
        responses = [
            BUSY,
            BUSY,
            BUSY,
            {"json": {"queued": True}},
            BUSY,
            {"json": tuning_session_payload(status="done")},
        ]
        with LbbClient(
            "http://h",
            max_retries=1,
            retry_delay=0,
            transport=capturing_transport(seen, responses),
        ) as client:
            with self.assertRaises(LbbError):
                client.embeddings.search_tuning.start(queries=12)
            self.assertEqual(len(seen), 1, "a session spends the judge budget")
            with self.assertRaises(LbbError):
                client.checks.check_call(CALL_ID)
            self.assertEqual(len(seen), 2, "a check spends the judge budget")
            queued = client.checks.check_call(CALL_ID, options={"retry": True})
            self.assertEqual(queued, {"queued": True})
            self.assertEqual(len(seen), 4)
            applied = client.embeddings.search_tuning.apply("t1")
        self.assertEqual(applied["status"], "done")
        self.assertEqual(len(seen), 6, "apply sets the same settings again")

    def test_checks_read_the_call_log_and_check_a_call_now(self) -> None:
        seen: list[httpx.Request] = []
        row = {
            "id": CALL_ID,
            "at_ms": 1_790_000_000_000,
            "job": "rerank",
            "provider": "typesafe",
            "model": "jev-latest",
            "ok": True,
            "sampled": True,
            "summary": "refund policy",
            "check": {"verdict": "right", "score": 0.9, "by": "judge", "reviewed": False},
        }
        responses = [
            {"json": {"calls": [row], "next_after": CALL_ID}},
            {"json": {"calls": []}},
            {"json": {"call": {"id": CALL_ID}, "check": model_check_payload()}},
            {"json": {"queued": True}},
        ]
        with LbbClient("http://h", transport=capturing_transport(seen, responses)) as client:
            page = client.checks.calls(
                job=model_module.ModelJob.rerank, day="2026-10-04", checked=False, limit=20
            )
            client.checks.calls(after=CALL_ID, checked=True)
            detail = client.checks.call(CALL_ID)
            queued = client.checks.check_call(CALL_ID)
        self.assertEqual(page["calls"][0]["check"]["verdict"], "right")
        self.assertEqual(detail["check"]["judge"]["rubric"], "relevance/1")
        self.assertEqual(queued, {"queued": True})
        self.assertEqual(
            [(request.method, request.url.path) for request in seen],
            [
                ("GET", "/v1/models/calls"),
                ("GET", "/v1/models/calls"),
                ("GET", "/v1/models/calls/get"),
                ("POST", "/v1/models/calls/check"),
            ],
        )
        self.assertEqual(
            dict(seen[0].url.params),
            {"job": "rerank", "day": "2026-10-04", "checked": "false", "limit": "20"},
        )
        self.assertEqual(dict(seen[1].url.params), {"after": CALL_ID, "checked": "true"})
        self.assertEqual(dict(seen[2].url.params), {"id": CALL_ID})
        self.assertEqual(dict(seen[3].url.params), {"id": CALL_ID})
        self.assertEqual(seen[3].content, b"")

    def test_checks_list_review_summarize_and_export_a_month(self) -> None:
        seen: list[httpx.Request] = []
        corrected = model_check_payload(
            review={
                "by": "key:k1",
                "at_ms": 1_790_000_100_000,
                "agree": False,
                "verdict": "wrong",
                "note": "the hits are about returns",
            },
            truth={"verdict": "wrong", "score": 0.0, "by": "person"},
        )
        lines = [{"call": None, "check": model_check_payload()}, {"call": None, "check": corrected}]
        responses = [
            {"json": {"checks": [corrected], "next_after": "c0"}},
            {"json": model_check_payload(review={"by": "key:k1", "at_ms": 1, "agree": True})},
            {"json": corrected},
            {"json": {"month": "2026-10", "jobs": [], "judge": {"agreement": 0.5}}},
            {"text": "".join(json.dumps(line) + "\n" for line in lines), "headers": NDJSON},
            {"text": "", "headers": NDJSON},
        ]
        with LbbClient(
            "http://h", graph="crm", transport=capturing_transport(seen, responses)
        ) as client:
            listed = client.checks.list(
                job="rerank",
                month="2026-10",
                verdict=model_module.CheckVerdict.wrong,
                reviewed=True,
                limit=10,
            )
            agreed = client.checks.review(CALL_ID, agree=True)
            reviewed = client.checks.review(
                CALL_ID,
                agree=False,
                verdict="wrong",
                reference={"grades": {"https://x.test/e/a": 0}},
                note="the hits are about returns",
            )
            summary = client.checks.summary(month="2026-10")
            exported = client.checks.export(job="rerank", month="2026-10")
            empty = client.checks.export()
        self.assertEqual(listed["checks"][0]["truth"]["by"], "person")
        self.assertIs(agreed["review"]["agree"], True)
        self.assertEqual(reviewed["truth"]["verdict"], "wrong")
        self.assertEqual(summary["judge"]["agreement"], 0.5)
        self.assertEqual(exported, lines)
        self.assertEqual(empty, [])
        self.assertEqual(
            [(request.method, request.url.path) for request in seen],
            [
                ("GET", "/v1/models/checks"),
                ("POST", "/v1/models/checks/review"),
                ("POST", "/v1/models/checks/review"),
                ("GET", "/v1/models/checks/summary"),
                ("GET", "/v1/models/checks/export"),
                ("GET", "/v1/models/checks/export"),
            ],
        )
        self.assertEqual(
            dict(seen[0].url.params),
            {
                "graph": "crm",
                "job": "rerank",
                "month": "2026-10",
                "verdict": "wrong",
                "reviewed": "true",
                "limit": "10",
            },
        )
        self.assertEqual(dict(seen[1].url.params), {"graph": "crm", "id": CALL_ID})
        self.assertEqual(json.loads(seen[1].content), {"agree": True})
        self.assertEqual(
            json.loads(seen[2].content),
            {
                "agree": False,
                "verdict": "wrong",
                "reference": {"grades": {"https://x.test/e/a": 0}},
                "note": "the hits are about returns",
            },
        )
        self.assertEqual(seen[3].url.params["month"], "2026-10")
        self.assertEqual(
            dict(seen[4].url.params), {"graph": "crm", "job": "rerank", "month": "2026-10"}
        )
        self.assertEqual(dict(seen[5].url.params), {"graph": "crm"})


class AsyncModelChecksAndTuningTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_checks_tuning_and_settings(self) -> None:
        seen: list[httpx.Request] = []
        line = {"call": None, "check": model_check_payload()}
        responses = [
            {"json": {"month": "2026-10", "jobs": []}},
            {"text": json.dumps(line) + "\n", "headers": NDJSON},
            {"json": model_check_payload(review={"by": "token", "at_ms": 1, "agree": True})},
            BUSY,
            {"json": tuning_session_payload()},
            {"json": {"rerank": False, "rerank_available": True}},
        ]
        async with AsyncLbbClient(
            "http://h",
            max_retries=1,
            retry_delay=0,
            transport=capturing_transport(seen, responses),
        ) as client:
            summary = await client.checks.summary()
            exported = await client.checks.export(month="2026-10")
            agreed = await client.checks.review(CALL_ID, agree=True, note="right order")
            with self.assertRaises(LbbError):
                await client.checks.check_call(CALL_ID)
            session = await client.embeddings.search_tuning.start(rounds=2)
            settings = await client.embeddings.set_search_settings(rerank=False, blend=RESET)
        self.assertEqual(summary["month"], "2026-10")
        self.assertEqual(exported, [line])
        self.assertIs(agreed["review"]["agree"], True)
        self.assertEqual(session["id"], "t1")
        self.assertIs(settings["rerank"], False)
        self.assertEqual(len(seen), 6, "the check that failed was sent once")
        self.assertEqual(json.loads(seen[2].content), {"agree": True, "note": "right order"})
        self.assertEqual(json.loads(seen[4].content), {"rounds": 2})
        self.assertEqual(json.loads(seen[5].content), {"rerank": False, "blend": None})


def rewrite_payload(**overrides: Any) -> dict[str, Any]:
    """A ``POST /v1/query/rewrite`` response without a run."""
    payload: dict[str, Any] = {
        "route": {
            "kind": "lookup",
            "confidence": 0.92,
            "by": "router",
            "probabilities": {"lookup": 0.92, "search": 0.08},
        },
        "query": {
            "sparql": "SELECT ?s WHERE { ?s ?p ?o }",
            "entailment": "none",
        },
        "rationale": "The question names services by a condition.",
        "attempts": 1,
        "grounding": {
            "commit_seq": 7,
            "classes": 3,
            "properties": 5,
            "embeddings": 0,
            "age_ms": 10,
        },
        "models": [],
        "timings": {
            "ground_ms": 1,
            "route_ms": 2,
            "rewrite_ms": 3,
            "run_ms": 4,
            "total_ms": 10,
        },
    }
    payload.update(overrides)
    return payload


def rewrite_run_payload() -> dict[str, Any]:
    """A rewrite response whose run returned one row and an eval trace."""
    result = sparql_text_envelope({"commit_seq": 7, "compacted_seq": 7, "served_at_seq": 7})
    result["trace_id"] = "tr_1"
    return rewrite_payload(result=result)


class QueryRewriteTests(unittest.TestCase):
    """``query.rewrite`` and ``query.ask`` send the documented request."""

    def test_rewrite_sends_only_the_given_fields_and_consistency_on_the_url(self) -> None:
        seen: list[httpx.Request] = []
        with LbbClient(
            "http://h",
            graph="main",
            transport=capturing_transport(seen, {"json": rewrite_payload()}),
        ) as client:
            response = client.query.rewrite(
                "Which services exist?",
                mode="route",
                consistency="strong",
            )
        self.assertEqual(response["route"]["kind"], "lookup")
        self.assertEqual(seen[0].method, "POST")
        self.assertEqual(seen[0].url.path, "/v1/query/rewrite")
        self.assertEqual(seen[0].url.params["graph"], "main")
        self.assertEqual(seen[0].url.params["consistency"], "strong")
        self.assertEqual(
            json.loads(seen[0].content),
            {"question": "Which services exist?", "mode": "route"},
        )

    def test_rewrite_can_ask_for_the_graph_description(self) -> None:
        seen: list[httpx.Request] = []
        with LbbClient(
            "http://h",
            graph="main",
            transport=capturing_transport(seen, {"json": rewrite_payload()}),
        ) as client:
            client.query.rewrite("Which services exist?", mode="route", include_grounding=True)
        self.assertEqual(
            json.loads(seen[0].content),
            {"question": "Which services exist?", "mode": "route", "include_grounding": True},
        )

    def test_rewrite_is_not_retried_unless_the_caller_asks(self) -> None:
        failure = {
            "status": 503,
            "json": {"error": {"message": "try again", "code": "rewrite_model_unavailable"}},
        }
        seen: list[httpx.Request] = []
        with LbbClient(
            "http://h",
            max_retries=1,
            retry_delay=0,
            transport=capturing_transport(seen, [failure, {"json": rewrite_payload()}]),
        ) as client:
            with self.assertRaises(LbbError) as raised:
                client.query.rewrite("Which services exist?")
        self.assertEqual(raised.exception.code, "rewrite_model_unavailable")
        self.assertEqual(len(seen), 1)

        seen.clear()
        with LbbClient(
            "http://h",
            max_retries=1,
            retry_delay=0,
            transport=capturing_transport(seen, [failure, {"json": rewrite_payload()}]),
        ) as client:
            client.query.rewrite("Which services exist?", options={"retry": True})
        self.assertEqual(len(seen), 2)

    def test_rewrite_serializes_steps_and_enums(self) -> None:
        seen: list[httpx.Request] = []
        with LbbClient(
            "http://h", transport=capturing_transport(seen, {"json": rewrite_payload()})
        ) as client:
            client.query.rewrite(
                "Which services exist?",
                context="Services of the platform team.",
                previous=[
                    model_module.QueryRewriteStep(sparql="SELECT * WHERE { ?s ?p ?o }", rows=0),
                    {"sparql": "ASK {}", "error": "no rows"},
                ],
                route=model_module.QueryRoute.lookup,
                mode=model_module.QueryRewriteMode.rewrite,
                run=False,
                limit=10,
                as_of_commit_seq=0,
                today="2026-10-04",
            )
        self.assertEqual(
            json.loads(seen[0].content),
            {
                "question": "Which services exist?",
                "context": "Services of the platform team.",
                "previous": [
                    {"sparql": "SELECT * WHERE { ?s ?p ?o }", "rows": 0},
                    {"sparql": "ASK {}", "error": "no rows"},
                ],
                "route": "lookup",
                "mode": "rewrite",
                "run": False,
                "limit": 10,
                "as_of_commit_seq": 0,
                "today": "2026-10-04",
            },
        )
        self.assertNotIn("consistency", seen[0].url.params)

    def test_ask_runs_the_rewrite_and_parses_its_rows(self) -> None:
        seen: list[httpx.Request] = []
        with LbbClient(
            "http://h",
            default_consistency="eventual",
            transport=capturing_transport(seen, {"json": rewrite_run_payload()}),
        ) as client:
            answer = client.query.ask(
                "Which services exist?", route="lookup", limit=50, as_of_commit_seq=7
            )
        self.assertIsInstance(answer, QueryAskResult)
        self.assertEqual(seen[0].url.params["consistency"], "eventual")
        self.assertEqual(
            json.loads(seen[0].content),
            {
                "question": "Which services exist?",
                "route": "lookup",
                "run": True,
                "limit": 50,
                "as_of_commit_seq": 7,
            },
        )
        self.assertEqual(answer.route["kind"], "lookup")
        self.assertEqual(answer.query, {"sparql": "SELECT ?s WHERE { ?s ?p ?o }", "entailment": "none"})
        self.assertEqual(answer.rationale, "The question names services by a condition.")
        self.assertEqual(answer.rows, [{"s": "x"}])
        self.assertEqual(answer.vars, ["s"])
        self.assertIsNone(answer.boolean)
        self.assertEqual(answer.snapshot, {"commit_seq": 7, "compacted_seq": 7, "served_at_seq": 7})
        self.assertIsNone(answer.error)
        self.assertEqual(answer.trace_id, "tr_1")
        self.assertEqual(answer.rewrite["attempts"], 1)

    def test_ask_without_a_run_keeps_the_route_rationale_and_error(self) -> None:
        unanswerable = rewrite_payload(
            route={"kind": "unanswerable", "confidence": 0.8, "by": "rewriter"},
            query=None,
            rationale="The graph holds no salaries.",
        )
        failed = rewrite_payload(attempts=2, error="unknown prefix ex")
        seen: list[httpx.Request] = []
        with LbbClient(
            "http://h",
            transport=capturing_transport(seen, [{"json": unanswerable}, {"json": failed}]),
        ) as client:
            none = client.query.ask("What does Ada earn?")
            broken = client.query.ask("Which services exist?")
        self.assertEqual(json.loads(seen[0].content), {"question": "What does Ada earn?", "run": True})
        self.assertEqual(none.route["kind"], "unanswerable")
        self.assertIsNone(none.query)
        self.assertEqual(none.rows, [])
        self.assertEqual(none.vars, [])
        self.assertIsNone(none.trace_id)
        self.assertEqual(broken.error, "unknown prefix ex")
        self.assertEqual(broken.rewrite["attempts"], 2)

    def test_ask_returns_the_answer_of_an_ask_query(self) -> None:
        payload = rewrite_payload(
            result={
                "results": json.dumps({"head": {}, "boolean": True}),
                "row_page": {
                    "returned": 0,
                    "total": 0,
                    "offset": 0,
                    "limit": 100,
                    "has_more": False,
                },
            }
        )
        with LbbClient(
            "http://h", transport=capturing_transport([], {"json": payload})
        ) as client:
            answer = client.query.ask("Is there any fact?")
        self.assertTrue(answer.boolean)
        self.assertEqual(answer.rows, [])


class AsyncQueryRewriteTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_rewrite_and_ask(self) -> None:
        seen: list[httpx.Request] = []
        async with AsyncLbbClient(
            "http://h",
            transport=capturing_transport(
                seen, [{"json": rewrite_payload()}, {"json": rewrite_run_payload()}]
            ),
        ) as client:
            response = await client.query.rewrite("Which services exist?", consistency="strong")
            answer = await client.query.ask("Which services exist?", context="team notes")
        self.assertEqual(response["query"]["entailment"], "none")
        self.assertEqual(seen[0].url.params["consistency"], "strong")
        self.assertEqual(json.loads(seen[0].content), {"question": "Which services exist?"})
        self.assertEqual(
            json.loads(seen[1].content),
            {"question": "Which services exist?", "context": "team notes", "run": True},
        )
        self.assertIsInstance(answer, QueryAskResult)
        self.assertEqual(answer.rows, [{"s": "x"}])
        self.assertEqual(answer.trace_id, "tr_1")

    async def test_async_rewrite_is_not_retried(self) -> None:
        seen: list[httpx.Request] = []
        async with AsyncLbbClient(
            "http://h",
            max_retries=1,
            retry_delay=0,
            transport=capturing_transport(
                seen,
                [
                    {"status": 503, "json": {"error": {"code": "rewrite_model_unavailable"}}},
                    {"json": rewrite_payload()},
                ],
            ),
        ) as client:
            with self.assertRaises(LbbError):
                await client.query.ask("Which services exist?")
        self.assertEqual(len(seen), 1)


class AsyncClientTests(unittest.IsolatedAsyncioTestCase):

    async def test_async_wait_for_published_follows_server_managed_stages(
        self,
    ) -> None:
        responses = iter(
            [
                httpx.Response(
                    200,
                    json=publication_status_payload("building", stage="build"),
                ),
                httpx.Response(
                    200,
                    json=publication_status_payload("current", published_seq=7),
                ),
            ]
        )

        def handler(_request: httpx.Request) -> httpx.Response:
            return next(responses)

        async with AsyncLbbClient(
            "http://h", transport=httpx.MockTransport(handler)
        ) as client:
            status = await client.wait_for_published(7, poll_interval=0)
        self.assertEqual(status.state, model_module.PublicationState.current)
        self.assertEqual(status.published_seq, 7)

    async def test_async_model_activity_reads_the_month(self) -> None:
        payload = {
            "month": "2026-10",
            "months": [],
            "managed": [],
            "totals": [],
            "by_day": [],
            "by_graph": [],
        }
        seen: list[httpx.Request] = []
        async with AsyncLbbClient(
            "http://h",
            transport=capturing_transport(seen, [{"json": payload}, {"json": payload}]),
        ) as client:
            raw = await client.model_activity("2026-10")
            typed = await client.model_activity_model()
        self.assertEqual(raw["month"], "2026-10")
        self.assertIsInstance(typed, model_module.ModelActivityResponse)
        self.assertEqual(typed.month, "2026-10")
        self.assertEqual(seen[0].url.path, "/v1/models/activity")
        self.assertEqual(seen[0].url.params.get("month"), "2026-10")
        self.assertNotIn("month", seen[1].url.params)

    async def test_async_sparql_search_update_and_entity_detail(self) -> None:
        seen: list[httpx.Request] = []
        async with AsyncLbbClient(
            "http://h",
            graph="g",
            transport=capturing_transport(
                seen,
                [
                    {"json": search_sparql_envelope()},
                    {"status": 204, "text": ""},
                    {"json": entity_detail_payload()},
                ],
            ),
        ) as client:
            found = await client.query.sparql("SELECT ?s WHERE { ?s ?p ?o }", request="q")
            updated = await client.query.update("INSERT DATA { <a:b> <a:c> <a:d> }")
            record = await client.entities.detail_model(id="e1")
        self.assertEqual(found.trace_id, "tr_1")
        assert found.search is not None
        self.assertEqual(found.search["plan"], "filter_first")
        self.assertEqual(json.loads(seen[0].content)["request"], "q")
        self.assertIsNone(updated)
        self.assertEqual(seen[1].url.path, "/update")
        self.assertTrue(seen[1].headers["idempotency-key"].startswith("sparql-update:"))
        self.assertIsInstance(record, model_module.EntityDetailResponse)
        self.assertEqual(seen[2].url.params["id"], "e1")

    async def test_async_workflow_instance_deletion_is_typed(self) -> None:
        seen: list[httpx.Request] = []
        async with AsyncLbbClient(
            "http://h",
            transport=capturing_transport(seen, {"json": {"deleted": False}}),
        ) as client:
            deleted = await client.workflow_delete_instance("hubspot-1")
        self.assertIsInstance(deleted, WorkflowInstanceDeleteResponse)
        self.assertFalse(deleted.deleted)
        self.assertEqual(seen[0].url.path, "/v1/workflows/instances/delete")
        self.assertEqual(json.loads(seen[0].content), {"workflow_id": "hubspot-1"})

    async def test_async_graph_wait_for_published_keeps_the_graph_scope(
        self,
    ) -> None:
        seen: list[httpx.Request] = []
        async with AsyncLbbClient(
            "http://h",
            transport=capturing_transport(
                seen,
                {"json": publication_status_payload("current", published_seq=7)},
            ),
        ) as client:
            status = await client.graph("perritos").wait_for_published(
                7, poll_interval=0
            )
        self.assertEqual(status.published_seq, 7)
        self.assertEqual(dict(seen[0].url.params), {"graph": "perritos"})

    async def test_async_import_rdf_many_defers_intermediate_publications(
        self,
    ) -> None:
        seen: list[httpx.Request] = []
        next_seq = 0

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal next_seq
            next_seq += 1
            seen.append(request)
            return httpx.Response(
                200,
                json={"imported_triplets": 1, "committed_commit_seq": next_seq},
            )

        async with AsyncLbbClient(
            "http://h", transport=httpx.MockTransport(handler)
        ) as client:
            result = await client.graph("research").facts.import_rdf_many(
                ["<a> <p> <b> .", "<b> <p> <c> ."],
                idempotency_key="perritos",
            )
        self.assertEqual(result["final_sequence"], 2)
        self.assertEqual(
            [dict(item.url.params).get("build") for item in seen],
            ["false", None],
        )

    async def test_async_durable_import_streams_async_iterable(self) -> None:
        seen: list[httpx.Request] = []
        produced = 0

        async def lines() -> Any:
            nonlocal produced
            produced += 1
            yield {"type": "Service", "name": "api", "properties": {}}
            produced += 1
            yield b'{"type":"Service","name":"db","properties":{}}'

        async def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            if request.url.path == "/version":
                return httpx.Response(
                    200, json={"capabilities": ["durable_import_jobs_v1"]}
                )
            await request.aread()
            return httpx.Response(
                202,
                json={
                    "job_id": "import:async",
                    "state": "queued",
                    "idempotent_replay": False,
                    "upload_bytes": len(request.content),
                },
            )

        async with AsyncLbbClient(
            "http://h", transport=httpx.MockTransport(handler)
        ) as client:
            accepted = await client.submit_import_ndjson(
                lines(), idempotency_key="source:async"
            )

        self.assertEqual(accepted.job_id, "import:async")
        self.assertEqual(produced, 2)
        self.assertEqual(len(seen[1].content.splitlines()), 2)

    async def test_async_durable_import_rejects_empty_source_before_post(self) -> None:
        seen: list[httpx.Request] = []
        async with AsyncLbbClient(
            "http://h",
            transport=capturing_transport(
                seen, {"json": {"capabilities": ["durable_import_jobs_v1"]}}
            ),
        ) as client:
            with self.assertRaisesRegex(ValueError, "requires at least one NDJSON"):
                await client.submit_import_ndjson(
                    [], idempotency_key="source:async-empty"
                )
        self.assertEqual([request.url.path for request in seen], ["/version"])

    async def test_async_create_graph_returns_typed_response(self) -> None:
        payload = {"commit_seq": 0, "graph": GRAPH, "ontology_version": 1}
        async with AsyncLbbClient(
            "http://h", transport=capturing_transport([], {"json": payload})
        ) as client:
            result = await client.create_graph()
        self.assertIsInstance(result, CreateGraphResponse)

    async def test_async_fork_graph_returns_typed_response(self) -> None:
        seen: list[httpx.Request] = []
        payload = {
            "ok": True,
            "queued": True,
            "src_graph_id": "research",
            "dst_graph_id": "research-copy",
            "job_id": "graph_fork:abc",
            "poll": "GET /v1/graph/metadata?graph=research-copy",
        }
        async with AsyncLbbClient(
            "http://h",
            graph="research",
            transport=capturing_transport(seen, {"json": payload}),
        ) as client:
            result = await client.fork_graph("research", "research-copy")
        self.assertIsInstance(result, GraphForkResponse)
        self.assertEqual(result.dst_graph_id, "research-copy")
        self.assertEqual(dict(seen[0].url.params)["confirm"], "research-copy")

    async def test_async_reload_posts_ndjson_and_is_typed(self) -> None:
        seen: list[httpx.Request] = []
        payload = {
            "dry_run": False,
            "lines_read": 1,
            "entities_added": 0,
            "entities_changed": 1,
            "entities_removed": 0,
            "edges_added": 0,
            "edges_changed": 0,
            "edges_removed": 0,
            "error_count": 0,
            "idempotency_key": "reload:async",
            "new_commit_seq": 3,
            "new_snapshot_token": "snap-new",
            "prior_commit_seq": 2,
            "prior_snapshot_token": "snap-old",
        }
        async with AsyncLbbClient(
            "http://h",
            graph="main",
            transport=capturing_transport(seen, {"json": payload}),
        ) as client:
            result = await client.reload(
                '{"type":"Author","name":"Ada","properties":{}}',
                confirm="main",
            )
        self.assertIsInstance(result, GraphReloadResponse)
        self.assertEqual(result.prior_commit_seq, 2)
        request = seen[0]
        self.assertEqual(str(request.url).split("?")[0], "http://h/v1/graph/reload")
        self.assertEqual(dict(request.url.params)["confirm"], "main")
        self.assertEqual(request.headers["content-type"], "application/x-ndjson")

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

    async def test_async_retryable_false_body_short_circuits(self) -> None:
        # Async parity: a `retryable: false` body is terminal, not retried.
        seen: list[httpx.Request] = []
        async with AsyncLbbClient(
            "http://h",
            max_retries=5,
            retry_delay=0,
            transport=capturing_transport(
                seen,
                [
                    {
                        "status": 429,
                        "json": {"error": {"code": "quota", "retryable": False}},
                    },
                    {"json": {"ok": True}},
                ],
            ),
        ) as client:
            with self.assertRaises(LbbError):
                await client.raw_request("GET", "/v1/status")
        self.assertEqual(len(seen), 1)

    async def test_async_naked_lb_5xx_is_retried(self) -> None:
        # Async parity: a bare LB 504 (HTML body, no envelope) is retried.
        seen: list[httpx.Request] = []
        async with AsyncLbbClient(
            "http://h",
            max_retries=3,
            retry_delay=0,
            transport=capturing_transport(
                seen,
                [
                    {"status": 504, "text": "<html>504 Gateway Timeout</html>"},
                    {"json": {"ok": True}},
                ],
            ),
        ) as client:
            result = await client.raw_request("GET", "/v1/status")
        self.assertEqual(result.attempts, 2)
        self.assertEqual(len(seen), 2)

    async def test_async_deadline_budget_binds(self) -> None:
        # Async parity: a 0 budget stops before the count cap under a 5s hint.
        seen: list[httpx.Request] = []
        async with AsyncLbbClient(
            "http://h",
            max_retries=5,
            retry_delay=0,
            retry_budget_ms=0,
            transport=capturing_transport(
                seen,
                [
                    {
                        "status": 429,
                        "headers": {"retry-after": "5"},
                        "json": {"error": {"code": "ingest_busy"}},
                    },
                    {"json": {"ok": True}},
                ],
            ),
        ) as client:
            with self.assertRaises(LbbError):
                await client.raw_request("GET", "/v1/status")
        self.assertEqual(len(seen), 1)

    async def test_async_roundtrip_and_scope(self) -> None:
        seen: list[httpx.Request] = []
        async with AsyncLbbClient(
            "http://h",
            api_key="k",
            graph="g",
            transport=capturing_transport(seen, {"json": {"solutions": []}}),
        ) as client:
            result = await client.sparql_select({"patterns": [], "select": []})
        self.assertEqual(result, {"solutions": []})
        self.assertEqual(str(seen[0].url).split("?")[0], "http://h/v1/query/sparql")
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
            "row_page": {
                "returned": 1,
                "total": 1,
                "offset": 0,
                "limit": 50,
                "has_more": False,
            },
        }
        async with AsyncLbbClient(
            "http://h",
            graph="g",
            transport=capturing_transport(seen, {"json": envelope}),
        ) as client:
            results = await client.sparql("SELECT ?s WHERE { ?s ?p ?o }")
        self.assertEqual(
            str(seen[0].url).split("?")[0], "http://h/v1/query/sparql-text"
        )
        self.assertEqual(results.rows(), [{"s": "x"}])

    async def test_async_sparql_cursor_preserves_continuation(self) -> None:
        seen: list[httpx.Request] = []
        envelope = {
            "results": json.dumps({"head": {"vars": []}, "results": {"bindings": []}}),
            "next_cursor": "next",
            "snapshot": SNAPSHOT,
        }
        async with AsyncLbbClient(
            "http://h", transport=capturing_transport(seen, {"json": envelope})
        ) as client:
            first = await client.query.sparql("ordered query LIMIT 25", cursor="")
            await client.sparql("ordered query LIMIT 25", cursor=first.next_cursor)
        self.assertEqual(first.snapshot, SNAPSHOT)
        self.assertEqual(json.loads(seen[0].content)["cursor"], "")
        self.assertEqual(json.loads(seen[1].content)["cursor"], "next")

    async def test_async_sparql_retries_a_429_only_and_pins_a_commit(self) -> None:
        # Async parity: a 429 is retried, a 5xx is not, and `as_of_commit_seq`
        # travels in the body.
        pinned = {"commit_seq": 9, "compacted_seq": 9, "as_of_commit_seq": 4}
        seen: list[httpx.Request] = []
        async with AsyncLbbClient(
            "http://h",
            retry_delay=0,
            transport=capturing_transport(
                seen,
                [
                    {
                        "status": 429,
                        "headers": {"retry-after": "0"},
                        "json": {"error": {"code": "read_your_writes_pending"}},
                    },
                    {"json": sparql_text_envelope(snapshot=pinned)},
                    {"status": 503, "json": {"error": {"code": "storage_degraded"}}},
                    {"json": sparql_text_envelope()},
                ],
            ),
        ) as client:
            results = await client.query.sparql(
                "SELECT ?s WHERE { ?s ?p ?o }", as_of_commit_seq=4
            )
            with self.assertRaises(LbbError):
                await client.sparql("SELECT ?s WHERE { ?s ?p ?o }")
        self.assertEqual(results.snapshot, pinned)
        self.assertEqual(len(seen), 3)
        self.assertEqual(json.loads(seen[1].content)["as_of_commit_seq"], 4)

    async def test_async_summary_model_helper(self) -> None:
        seen: list[httpx.Request] = []
        async with AsyncLbbClient(
            "http://h",
            graph="g",
            transport=capturing_transport(seen, {"json": summary_payload()}),
        ) as client:
            summary = await client.summary_model()

        self.assertIsInstance(summary, GraphSummaryResponse)
        self.assertEqual(summary.current_edge_count, 3)

    def test_typed_suggestion_helper_validates_before_transport_and_sets_idempotency(
        self,
    ) -> None:
        seen: list[httpx.Request] = []
        ack = {
            "accepted": 1,
            "receipt_id": "signal-receipt:r1",
            "event_id": "signal-event:r1:0",
            "replayed": False,
            "accepted_count": 1,
            "trainable_count": 1,
            "excluded_count": 0,
            "exclusions": {},
        }
        with LbbClient(
            "http://h",
            graph="g",
            transport=capturing_transport(seen, [{"json": ack}]),
        ) as client:
            with self.assertRaises(ValidationError):
                client.suggestion_adopted({"text": "missing typed identity"})
            self.assertEqual(seen, [], "malformed payload never reaches transport")
            response = client.suggestion_adopted(
                {
                    "v": 1,
                    "suggestion_id": "s-1",
                    "candidate_id": "c-1",
                    "prefix": "sto",
                    "text": "STORES",
                    "rank": 0,
                },
                idempotency_key="suggestion-retry-1",
            )
        self.assertEqual(response["trainable_count"], 1)
        self.assertEqual(seen[0].headers["idempotency-key"], "suggestion-retry-1")


if __name__ == "__main__":
    unittest.main()
