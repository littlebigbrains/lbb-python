"""Record writes on a graph that stores RDF (every new graph).

The refusals below are the bodies the server sends (``rdf_native_write_refused``
in lbb-graph-query, ``json_error_with_code`` in lbb-server). The Rust route test
``rdf_native_client_writes`` pins the same texts, so a change to either side
fails a test.
"""

from __future__ import annotations

import asyncio
import json
import unittest
from typing import Any

import httpx

from lbb import AsyncLbbClient, LbbClient, LbbError
from lbb._client_base import _NATIVE_PROBE_BODY
from lbb.models import GraphCommitResponse, GraphRetractResponse

BARE_COMMIT = "a triplet / property / observation commit"
BARE_DRY_RUN = "a triplet / property commit dry run"
ENTITY_RETRACT = "entity retract (use replace_owned.retract_entities for RDF entity cleanup)"


def refusal(operation: str) -> httpx.Response:
    return httpx.Response(
        400,
        json={
            "error": {
                "type": "invalid_request_error",
                "code": "rdf_native_graph",
                "message": (
                    f"{operation} is not available on this graph: its truth is written by "
                    "the RDF-native routes. Write RDF statements with POST "
                    "/v1/graph/import/rdf, `graph rdf bulk-load`, or SPARQL Update instead."
                ),
                "param": None,
                "request_id": None,
                "doc_url": "https://docs.littlebigbrain.com/sdks/errors/#rdf_native_graph",
            }
        },
    )


COMMIT_RESPONSE = {
    "graph_created": False,
    "idempotency_key": "k",
    "replayed": False,
    "commit_seq": 7,
    "visibility_token": "v7",
    "entities_written": 2,
    "edges_written": 1,
    "commit": {
        "commit_seq": 7,
        "visibility_token": "v7",
        "observation_ids": [],
        "entity_ids": [],
        "edge_event_ids": [],
        "idempotent_replay": False,
        "no_op": False,
        "skipped_edges": [],
        "written_properties": [],
    },
}


def server(truth: str):
    """A transport whose graph stores RDF (``rdf_native``) or records, as its two
    record routes answer, and the requests it saw."""
    seen: list[httpx.Request] = []
    current = {"truth": truth}

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        body = json.loads(request.content) if request.content else {}
        dry_run = "dry_run" in request.url.params
        if request.url.path == "/v1/graph/commit":
            if current["truth"] == "rdf_native" and body.get("replace_owned") is None:
                return refusal(BARE_DRY_RUN if dry_run else BARE_COMMIT)
            doomed = any(
                entity.get("type") == "Nope"
                for entity in (body.get("replace_owned") or {}).get("retract_entities") or []
            )
            if doomed:
                return httpx.Response(
                    400,
                    json={
                        "error": {
                            "type": "invalid_request_error",
                            "code": "invalid_input",
                            "message": "replace_owned.retract_entities: unknown entity type 'Nope'",
                        }
                    },
                )
            if dry_run:
                return httpx.Response(
                    200,
                    json={
                        "dry_run": True,
                        "op_count": 3,
                        "triplets": 1,
                        "entity_properties": 0,
                    },
                )
            owned = body.get("replace_owned")
            response = dict(COMMIT_RESPONSE)
            if owned is not None:
                response["owned"] = {
                    "edges_retracted": 0,
                    "fields_removed": 0,
                    "entities_retracted": len(owned.get("retract_entities") or []),
                }
            return httpx.Response(200, json=response)
        if request.url.path == "/version":
            return httpx.Response(200, json={"capabilities": ["durable_import_jobs_v1"]})
        if request.url.path == "/v1/graph/import-jobs":
            return httpx.Response(
                202,
                json={
                    "job_id": "import:job-1",
                    "state": "queued",
                    "idempotent_replay": False,
                    "upload_bytes": len(request.content),
                },
            )
        if request.url.path == "/v1/graph/retract":
            entities = body.get("entities") or []
            if current["truth"] == "rdf_native" and entities:
                return refusal(ENTITY_RETRACT)
            return httpx.Response(
                200,
                json={
                    "commit_seq": 6,
                    "visibility_token": "v6",
                    "retracted_edges": len(body.get("edges") or []),
                    "retracted_entities": len(entities),
                },
            )
        return httpx.Response(404, json={})

    transport = httpx.MockTransport(handler)
    # The graph turns out to be of the other kind: ``become(transport, "...")``.
    transport.current = current  # type: ignore[attr-defined]
    return transport, seen


def become(transport: httpx.MockTransport, truth: str) -> None:
    transport.current["truth"] = truth  # type: ignore[attr-defined]


FACTS: dict[str, Any] = {
    "triplets": [
        {
            "source": {"type": "SERVICE", "name": "api"},
            "relation": "WRITES_TO",
            "target": {"type": "DATABASE", "name": "primary"},
            "confidence": 0.9,
            "evidence": "runbook",
        },
        {
            "source": {"type": "SERVICE", "name": "worker"},
            "relation": "WRITES_TO",
            "target": {"type": "DATABASE", "name": "primary"},
            "evidence": {"text": "deploy notes"},
        },
    ],
    "entity_properties": [
        {"type": "SERVICE", "name": "api", "properties": {"tier": "gold"}}
    ],
    "edge_idempotency": "skip_unchanged",
    "property_merge": "merge",
    "observed_at": "2026-11-01T00:00:00Z",
}
EDGE = {
    "source": {"type": "SERVICE", "name": "api"},
    "relation": "WRITES_TO",
    "target": {"type": "DATABASE", "name": "primary"},
}


def body_of(request: httpx.Request) -> dict[str, Any]:
    return json.loads(request.content)


class RdfNativeCommitTests(unittest.TestCase):
    def test_commit_sends_the_same_facts_that_own_nothing(self) -> None:
        transport, seen = server("rdf_native")
        with LbbClient("http://h", graph="main", transport=transport) as client:
            done = client.commit(FACTS, idempotency_key="load-1")
        self.assertEqual(done["commit_seq"], 7)
        self.assertEqual(len(seen), 2)
        self.assertNotIn("replace_owned", body_of(seen[0]))
        owned = body_of(seen[1])
        # One subject selects the route; no relation or property is owned, so no
        # earlier statement is removed.
        self.assertEqual(
            owned.pop("replace_owned"),
            {
                "subjects": [{"type": "SERVICE", "name": "api"}],
                "relations": [],
                "properties": [],
            },
        )
        # Every fact and option of the caller is kept, unchanged.
        self.assertEqual(owned, FACTS)
        # The refused request wrote nothing, so the retry replays under the key.
        self.assertEqual(seen[0].headers["idempotency-key"], "load-1")
        self.assertEqual(seen[1].headers["idempotency-key"], "load-1")

    def test_the_client_remembers_a_graph_that_stores_rdf(self) -> None:
        transport, seen = server("rdf_native")
        with LbbClient("http://h", graph="main", transport=transport) as client:
            client.commit(FACTS)
            seen.clear()
            client.commit(FACTS)
            self.assertEqual(len(seen), 1)
            self.assertIn("replace_owned", body_of(seen[0]))
            # The graph namespace of the same graph shares what it learned.
            seen.clear()
            client.graph("main").facts.create(FACTS)
            self.assertEqual(len(seen), 1)
            # Another graph is not assumed to store RDF.
            seen.clear()
            client.graph("other").facts.create(FACTS)
            self.assertEqual(len(seen), 2)
            self.assertNotIn("replace_owned", body_of(seen[0]))

    def test_graph_facts_create_on_a_new_graph_works_on_the_first_call(self) -> None:
        transport, seen = server("rdf_native")
        with LbbClient("http://h", transport=transport) as client:
            created = client.graph("kb").facts.create(FACTS)
        self.assertEqual(created["commit_seq"], 7)
        self.assertEqual(len(seen), 2)
        self.assertEqual(seen[0].url.params["graph"], "kb")
        self.assertIn("replace_owned", body_of(seen[1]))

    def test_typed_commits_follow_the_same_flow(self) -> None:
        transport, seen = server("rdf_native")
        with LbbClient("http://h", graph="main", transport=transport) as client:
            typed = client.commit_model(FACTS)
            created = client.graph("main").facts.create_model(FACTS)
            report = client.commit_dry_run_model(FACTS)
        self.assertIsInstance(typed, GraphCommitResponse)
        self.assertIsInstance(created, GraphCommitResponse)
        self.assertTrue(report.dry_run)
        self.assertEqual(len(seen), 4)

    def test_a_graph_of_records_gets_one_plain_request(self) -> None:
        transport, seen = server("property_graph")
        with LbbClient("http://h", graph="main", transport=transport) as client:
            client.commit(FACTS)
        self.assertEqual(len(seen), 1)
        self.assertNotIn("replace_owned", body_of(seen[0]))

    def test_a_commit_with_replace_owned_is_sent_once_as_given(self) -> None:
        transport, seen = server("rdf_native")
        owned = {
            **FACTS,
            "replace_owned": {
                "subjects": [{"type": "SERVICE", "name": "api"}],
                "relations": ["WRITES_TO"],
            },
        }
        with LbbClient("http://h", graph="main", transport=transport) as client:
            client.commit(owned)
        self.assertEqual(len(seen), 1)
        self.assertEqual(body_of(seen[0])["replace_owned"], owned["replace_owned"])

    def test_another_refusal_is_not_retried(self) -> None:
        seen: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            return httpx.Response(
                400,
                json={
                    "error": {
                        "type": "invalid_request_error",
                        "code": "invalid_input",
                        "message": "x",
                    }
                },
            )

        with LbbClient(
            "http://h", graph="main", transport=httpx.MockTransport(handler)
        ) as client:
            with self.assertRaises(LbbError) as caught:
                client.commit(FACTS)
        self.assertEqual(caught.exception.code, "invalid_input")
        self.assertEqual(len(seen), 1)

    def test_properties_alone_name_the_subject_with_an_entity_id_as_its_key(self) -> None:
        transport, seen = server("rdf_native")
        with LbbClient("http://h", graph="main", transport=transport) as client:
            client.commit(
                {
                    "entity_properties": [
                        {
                            "type": "SERVICE",
                            "name": "api",
                            "entity_id": "0123456789abcdef0123456789abcdef",
                            "properties": {"tier": "gold"},
                        }
                    ]
                }
            )
        self.assertEqual(
            body_of(seen[1])["replace_owned"],
            {
                "subjects": [
                    {
                        "type": "SERVICE",
                        "name": "api",
                        "key": "\u0000lbb-eid:0123456789abcdef0123456789abcdef",
                    }
                ],
                "relations": [],
                "properties": [],
            },
        )

    def test_a_dry_run_previews_the_owned_commit(self) -> None:
        transport, seen = server("rdf_native")
        with LbbClient("http://h", graph="main", transport=transport) as client:
            report = client.commit_dry_run(FACTS)
        self.assertTrue(report["dry_run"])
        self.assertEqual(len(seen), 2)
        self.assertTrue(all(r.url.params["dry_run"] == "true" for r in seen))
        self.assertIn("replace_owned", body_of(seen[1]))
        self.assertNotIn("idempotency-key", seen[0].headers)

    def test_entity_embeddings_are_refused_with_what_to_do_instead(self) -> None:
        transport, seen = server("rdf_native")
        vectors = {
            **FACTS,
            "entity_embeddings": [
                {"type": "SERVICE", "name": "api", "model_id": "m", "vector": [0.1, 0.2]}
            ],
        }
        with LbbClient("http://h", graph="main", transport=transport) as client:
            for _ in range(2):
                with self.assertRaises(LbbError) as caught:
                    client.commit(vectors)
                error = caught.exception
                self.assertEqual(error.status_code, 400)
                self.assertEqual(error.code, "rdf_native_graph")
                self.assertIn("entity_embeddings", str(error))
                self.assertIn("embeddings.declare", str(error))
                self.assertIn("POST /v1/search", str(error))
                self.assertIn("Nothing was written", str(error))
        # The refused request went out, and a remembered graph does not refuse
        # alone: the server is asked once more. The owned form was never sent.
        self.assertEqual(len(seen), 2)
        self.assertTrue(all("replace_owned" not in body_of(r) for r in seen))

    def test_a_remembered_graph_that_takes_records_is_not_refused_by_the_client_alone(
        self,
    ) -> None:
        transport, seen = server("rdf_native")
        vectors = {
            **FACTS,
            "entity_embeddings": [
                {"type": "SERVICE", "name": "api", "model_id": "m", "vector": [0.1, 0.2]}
            ],
        }
        with LbbClient("http://h", graph="main", transport=transport) as client:
            client.commit(FACTS)
            become(transport, "property_graph")
            seen.clear()
            done = client.commit(vectors)
            self.assertEqual(done["commit_seq"], 7)
            self.assertEqual(len(seen), 1)
            self.assertNotIn("replace_owned", body_of(seen[0]))
            # The stale entry is gone: the next commit asks the server first again.
            seen.clear()
            client.commit(FACTS)
            self.assertEqual(len(seen), 1)
            self.assertNotIn("replace_owned", body_of(seen[0]))

    def test_confidence_and_valid_time_without_evidence_are_refused_not_dropped(self) -> None:
        transport, seen = server("rdf_native")
        link = {
            "source": {"type": "SERVICE", "name": "api"},
            "relation": "WRITES_TO",
            "target": {"type": "DATABASE", "name": "primary"},
        }
        with LbbClient("http://h", graph="main", transport=transport) as client:
            cases = [
                ({"triplets": [{**link, "confidence": 0.4}]}, "confidence"),
                (
                    {"triplets": [{**link, "valid_time": {"start": "2020-01-01"}}]},
                    "valid_time",
                ),
                (
                    {"triplets": [link], "observed_at": "2026-11-01T00:00:00Z"},
                    "observed_at",
                ),
            ]
            for body, words in cases:
                with self.assertRaises(LbbError) as caught:
                    client.commit(body)
                error = caught.exception
                self.assertEqual(error.code, "rdf_native_graph")
                self.assertIn("evidence", str(error))
                self.assertIn(words, str(error))
                self.assertIn("Nothing was written", str(error))
            # Only plain requests went out: the owned form was never sent.
            self.assertTrue(all("replace_owned" not in body_of(r) for r in seen))

    def test_confidence_and_valid_time_with_evidence_go_through(self) -> None:
        transport, seen = server("rdf_native")
        link = {
            "source": {"type": "SERVICE", "name": "api"},
            "relation": "WRITES_TO",
            "target": {"type": "DATABASE", "name": "primary"},
        }
        triplets = [
            {
                **link,
                "confidence": 0.4,
                "valid_time": {"start": "2020-01-01"},
                "evidence": {"text": "runbook"},
            },
            # A confidence of 1 is the default: nothing is lost without evidence.
            {**link, "confidence": 1},
        ]
        with LbbClient("http://h", graph="main", transport=transport) as client:
            client.commit({"triplets": triplets})
            self.assertEqual(len(seen), 2)
            self.assertIn("replace_owned", body_of(seen[1]))
            # With observed_at the second link would lose its start.
            with self.assertRaises(LbbError) as caught:
                client.commit(
                    {"triplets": triplets, "observed_at": "2026-11-01T00:00:00Z"}
                )
            self.assertIn("observed_at", str(caught.exception))

    def test_a_commit_without_a_key_uses_one_key_for_both_attempts(self) -> None:
        transport, seen = server("rdf_native")
        with LbbClient("http://h", transport=transport) as client:
            client.graph("kb").facts.create(FACTS)
            client.graph("other").facts.create(FACTS)
        self.assertEqual(len(seen), 4)
        for first, second in [(seen[0], seen[1]), (seen[2], seen[3])]:
            self.assertTrue(first.headers["idempotency-key"].startswith("facts.create:"))
            self.assertEqual(
                second.headers["idempotency-key"], first.headers["idempotency-key"]
            )
        self.assertNotEqual(
            seen[0].headers["idempotency-key"], seen[2].headers["idempotency-key"]
        )

    def test_preconditions_are_refused_before_an_owned_commit_is_sent(self) -> None:
        transport, seen = server("rdf_native")
        with LbbClient("http://h", graph="main", transport=transport) as client:
            with self.assertRaises(LbbError) as caught:
                client.commit({**FACTS, "preconditions": {"expected_head_commit_seq": 3}})
        self.assertEqual(caught.exception.code, "rdf_native_graph")
        self.assertIn("preconditions", str(caught.exception))
        self.assertEqual(len(seen), 1)


class RdfNativeRetractTests(unittest.TestCase):
    def test_retract_of_entities_needs_purge_entity_properties(self) -> None:
        transport, seen = server("rdf_native")
        with LbbClient("http://h", graph="main", transport=transport) as client:
            with self.assertRaises(LbbError) as caught:
                client.retract({"entities": [{"type": "SERVICE", "name": "api"}]})
        error = caught.exception
        self.assertEqual(error.code, "rdf_native_graph")
        self.assertIn("purge_entity_properties=True", str(error))
        self.assertIn("Nothing was removed", str(error))
        # Nothing but the refused request went out.
        self.assertEqual(len(seen), 1)
        self.assertEqual(seen[0].url.path, "/v1/graph/retract")

    def test_retract_with_purge_removes_whole_records_as_an_owned_commit(self) -> None:
        transport, seen = server("rdf_native")
        entities = [
            {"type": "SERVICE", "name": "api"},
            {"type": "SERVICE", "name": "worker"},
        ]
        with LbbClient("http://h", graph="main", transport=transport) as client:
            done = client.retract(
                {"entities": entities, "purge_entity_properties": True},
                idempotency_key="gone-1",
            )
        self.assertEqual(len(seen), 2)
        self.assertEqual(seen[1].url.path, "/v1/graph/commit")
        self.assertEqual(
            body_of(seen[1]),
            {
                "replace_owned": {
                    "subjects": [],
                    "relations": [],
                    "properties": [],
                    "retract_entities": entities,
                }
            },
        )
        self.assertEqual(seen[1].headers["idempotency-key"], "gone-1:rdf")
        self.assertEqual(done["commit_seq"], 7)
        self.assertEqual(done["visibility_token"], "v7")
        self.assertEqual(done["retracted_entities"], 2)
        self.assertEqual(done["purged_entities"], 2)

    def test_exact_links_retract_first_under_their_own_key(self) -> None:
        transport, seen = server("rdf_native")
        with LbbClient("http://h", transport=transport) as client:
            done = client.graph("main").retract_model(
                {
                    "edges": [EDGE],
                    "entities": [{"type": "SERVICE", "name": "worker"}],
                    "purge_entity_properties": True,
                },
                idempotency_key="gone-2",
            )
        self.assertIsInstance(done, GraphRetractResponse)
        # The refused request, a dry run of the records' commit (no key, nothing
        # written), the links, then the records.
        self.assertEqual(
            [
                (r.url.path, "dry_run" in r.url.params, r.headers.get("idempotency-key"))
                for r in seen
            ],
            [
                ("/v1/graph/retract", False, "gone-2"),
                ("/v1/graph/commit", True, None),
                ("/v1/graph/retract", False, "gone-2:edges"),
                ("/v1/graph/commit", False, "gone-2:rdf"),
            ],
        )
        self.assertEqual(body_of(seen[2]), {"edges": [EDGE], "entities": []})
        self.assertEqual(done.retracted_edges, 1)
        self.assertEqual(done.retracted_entities, 1)

    def test_a_retract_that_names_a_record_of_an_unknown_type_changes_nothing(self) -> None:
        transport, seen = server("rdf_native")
        with LbbClient("http://h", graph="main", transport=transport) as client:
            with self.assertRaises(LbbError) as caught:
                client.retract(
                    {
                        "edges": [EDGE],
                        "entities": [{"type": "Nope", "name": "x"}],
                        "purge_entity_properties": True,
                    }
                )
        self.assertEqual(caught.exception.code, "invalid_input")
        # The refused request and the dry run: no link was retracted and no
        # record was removed.
        self.assertEqual(
            [(r.url.path, "dry_run" in r.url.params) for r in seen],
            [("/v1/graph/retract", False), ("/v1/graph/commit", True)],
        )

    def test_a_retract_of_more_than_1000_records_is_refused_before_it_changes_anything(
        self,
    ) -> None:
        transport, seen = server("rdf_native")
        entities = [{"type": "SERVICE", "name": f"s{n}"} for n in range(1001)]
        with LbbClient("http://h", graph="main", transport=transport) as client:
            for body in (
                {"edges": [EDGE], "entities": entities, "purge_entity_properties": True},
                {"entities": entities, "purge_entity_properties": True},
            ):
                with self.assertRaises(LbbError) as caught:
                    client.retract(body)
                self.assertEqual(caught.exception.code, "rdf_native_graph")
                self.assertIn("1000", str(caught.exception))
                self.assertIn("batches", str(caught.exception))
        # Only plain retract requests: no dry run, no link, no commit.
        self.assertTrue(all(r.url.path == "/v1/graph/retract" for r in seen))

    def test_a_retract_names_the_collateral_on_neighbours(self) -> None:
        transport, _ = server("rdf_native")
        with LbbClient("http://h", graph="main", transport=transport) as client:
            with self.assertRaises(LbbError) as caught:
                client.retract({"entities": [{"type": "SERVICE", "name": "api"}]})
        self.assertIn(
            "left with no link and no field because of it loses its type and label too",
            str(caught.exception),
        )

    def test_a_remembered_graph_that_takes_records_is_not_refused_on_a_retract(
        self,
    ) -> None:
        transport, seen = server("rdf_native")
        with LbbClient("http://h", graph="main", transport=transport) as client:
            client.commit(FACTS)
            become(transport, "property_graph")
            seen.clear()
            done = client.retract({"entities": [{"type": "SERVICE", "name": "api"}]})
        self.assertEqual(done["commit_seq"], 6)
        self.assertEqual([r.url.path for r in seen], ["/v1/graph/retract"])

    def test_edges_alone_and_graphs_of_records_are_unchanged(self) -> None:
        transport, seen = server("rdf_native")
        with LbbClient("http://h", graph="main", transport=transport) as client:
            client.retract({"edges": [EDGE]})
        self.assertEqual(len(seen), 1)

        transport, seen = server("property_graph")
        with LbbClient("http://h", graph="main", transport=transport) as client:
            client.retract({"entities": [{"type": "SERVICE", "name": "api"}]})
        self.assertEqual(len(seen), 1)
        self.assertEqual(
            body_of(seen[0]), {"entities": [{"type": "SERVICE", "name": "api"}]}
        )


class RdfNativeImportJobTests(unittest.TestCase):
    def test_an_import_job_is_refused_before_the_upload(self) -> None:
        transport, seen = server("rdf_native")
        with LbbClient("http://h", graph="main", transport=transport) as client:
            for _ in range(2):
                with self.assertRaises(LbbError) as caught:
                    client.submit_import_ndjson([FACTS["triplets"][0]], idempotency_key="j-1")
                error = caught.exception
                self.assertEqual(error.status_code, 400)
                self.assertEqual(error.code, "rdf_native_graph")
                self.assertIn("import_rdf", str(error))
                self.assertIn("Nothing was uploaded", str(error))
        # The capability check and the dry run that learned the kind of graph. The
        # second call asks the server again: a remembered graph does not refuse
        # an upload alone.
        self.assertEqual(
            [r.url.path for r in seen], ["/version", "/v1/graph/commit", "/v1/graph/commit"]
        )
        self.assertEqual(seen[1].url.params["dry_run"], "true")
        # The probe is the commit no graph accepts, and it carries no key.
        self.assertEqual(body_of(seen[1]), _NATIVE_PROBE_BODY)
        self.assertNotIn("idempotency-key", seen[1].headers)

    def test_the_import_job_check_runs_before_the_source_is_read_and_closes_it(self) -> None:
        transport, _ = server("rdf_native")
        produced = []

        def lines() -> Any:
            try:
                produced.append(1)
                yield FACTS["triplets"][0]
            finally:
                produced.append("closed")

        source = lines()
        with LbbClient("http://h", graph="main", transport=transport) as client:
            with self.assertRaises(LbbError):
                client.submit_import_ndjson(source, idempotency_key="j-5")
        # Not one record was read, and the source was closed.
        self.assertEqual(produced, [])
        self.assertEqual(next(iter(source), "done"), "done")

    def test_the_dry_run_that_learns_the_kind_of_graph_tells_the_two_apart(self) -> None:
        transport, _ = server("rdf_native")
        with LbbClient("http://h", graph="main", transport=transport) as client:
            with self.assertRaises(LbbError) as caught:
                client._request(
                    "POST",
                    "/v1/graph/commit",
                    params={"dry_run": "true"},
                    body=_NATIVE_PROBE_BODY,
                )
        self.assertEqual(caught.exception.code, "rdf_native_graph")
        transport, _ = server("property_graph")
        with LbbClient("http://h", graph="main", transport=transport) as client:
            report = client._request(
                "POST",
                "/v1/graph/commit",
                params={"dry_run": "true"},
                body=_NATIVE_PROBE_BODY,
            )
        self.assertTrue(report["dry_run"])

    def test_an_import_job_goes_ahead_on_a_graph_of_records(self) -> None:
        transport, seen = server("property_graph")
        with LbbClient("http://h", graph="main", transport=transport) as client:
            accepted = client.submit_import_ndjson(
                [FACTS["triplets"][0]], idempotency_key="j-2"
            )
        self.assertEqual(accepted.job_id, "import:job-1")
        self.assertEqual(
            [r.url.path for r in seen],
            ["/version", "/v1/graph/commit", "/v1/graph/import-jobs"],
        )

    def test_the_async_client_refuses_an_import_job_the_same_way(self) -> None:
        async def scenario() -> None:
            transport, seen = server("rdf_native")
            async with AsyncLbbClient("http://h", graph="main", transport=transport) as client:
                with self.assertRaises(LbbError) as caught:
                    await client.submit_import_ndjson(
                        [FACTS["triplets"][0]], idempotency_key="j-3"
                    )
            self.assertEqual(caught.exception.code, "rdf_native_graph")
            self.assertEqual([r.url.path for r in seen], ["/version", "/v1/graph/commit"])

        asyncio.run(scenario())


class AsyncRdfNativeWriteTests(unittest.TestCase):
    def test_the_async_client_follows_the_same_flow(self) -> None:
        async def scenario() -> None:
            transport, seen = server("rdf_native")
            async with AsyncLbbClient("http://h", graph="main", transport=transport) as client:
                done = await client.commit(FACTS, idempotency_key="a-1")
                self.assertEqual(done["commit_seq"], 7)
                self.assertEqual(len(seen), 2)
                self.assertIn("replace_owned", body_of(seen[1]))
                seen.clear()
                typed = await client.graph("main").facts.create_model(FACTS)
                self.assertIsInstance(typed, GraphCommitResponse)
                self.assertEqual(len(seen), 1)
                seen.clear()
                gone = await client.graph("main").retract_model(
                    {
                        "entities": [{"type": "SERVICE", "name": "api"}],
                        "purge_entity_properties": True,
                    },
                    idempotency_key="a-2",
                )
                self.assertIsInstance(gone, GraphRetractResponse)
                self.assertEqual(gone.purged_entities, 1)
                # The graph is known to store RDF, so the retract goes straight to
                # the owned commit.
                self.assertEqual([r.url.path for r in seen], ["/v1/graph/commit"])
                with self.assertRaises(LbbError) as caught:
                    await client.commit(
                        {
                            **FACTS,
                            "entity_embeddings": [
                                {"type": "SERVICE", "name": "api", "vector": [0.1]}
                            ],
                        }
                    )
                self.assertEqual(caught.exception.code, "rdf_native_graph")
                self.assertIn("entity_embeddings", str(caught.exception))

        asyncio.run(scenario())

    def test_the_async_import_job_check_runs_before_the_source_is_read(self) -> None:
        async def scenario() -> None:
            transport, seen = server("rdf_native")
            events: list[str] = []

            async def lines() -> Any:
                try:
                    events.append("read")
                    yield FACTS["triplets"][0]
                finally:
                    events.append("closed")

            source = lines()
            async with AsyncLbbClient("http://h", graph="main", transport=transport) as client:
                with self.assertRaises(LbbError):
                    await client.submit_import_ndjson(source, idempotency_key="j-6")
            self.assertEqual(events, [])
            self.assertEqual([r.url.path for r in seen], ["/version", "/v1/graph/commit"])

        asyncio.run(scenario())

    def test_the_async_client_reports_another_refusal_unchanged(self) -> None:
        async def scenario() -> None:
            seen: list[httpx.Request] = []

            def handler(request: httpx.Request) -> httpx.Response:
                seen.append(request)
                return httpx.Response(
                    400, json={"error": {"code": "invalid_input", "message": "x"}}
                )

            async with AsyncLbbClient(
                "http://h", graph="main", transport=httpx.MockTransport(handler)
            ) as client:
                with self.assertRaises(LbbError) as caught:
                    await client.commit_dry_run(FACTS)
            self.assertEqual(caught.exception.code, "invalid_input")
            self.assertEqual(len(seen), 1)

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
