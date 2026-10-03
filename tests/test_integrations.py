"""``client.integrations`` against a fake transport (no server).

The integrations routes go to ``integrations_url`` with the stack key; the
suggestion helpers go to the data plane (``base_url``). The last test checks
every route against ``contracts/integrations-openapi.json``.
"""

from __future__ import annotations

import json
import re
import unittest
from pathlib import Path
from typing import Any

import httpx

from lbb import AsyncLbbClient, LbbClient, LbbError
from lbb.models import AddEntityTypeOp, OntologyChangeSuggestion, WorkflowTurn

DATA_PLANE = "https://0abc1def--production.db.eu.littlebigbrain.com"
API = "https://api.littlebigbrain.com"
KEY = "lbb_sk_test_example"
GRAPH = "c-5f1c9a0e3b7d2c4a8e6f1b0d"
GRAPH_KEY = {"tenant_id": "tenant", "graph_id": GRAPH, "branch_id": "main"}

Reply = dict[str, Any]


def fake_transport(
    seen: list[httpx.Request], replies: list[Reply] | None = None
) -> Any:
    """Record each request and answer from ``replies`` in order."""
    queue = list(replies or [])

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        reply = queue.pop(0) if queue else {}
        return httpx.Response(
            reply.get("status", 200),
            json=reply.get("json", {"ok": True}),
            headers=reply.get("headers", {}),
        )

    return handler


def sync_client(
    seen: list[httpx.Request], replies: list[Reply] | None = None, **extra: Any
) -> LbbClient:
    # A graph scope on the client must not reach the integrations API.
    return LbbClient(
        DATA_PLANE,
        api_key=KEY,
        graph="main",
        retry_delay=0,
        transport=httpx.MockTransport(fake_transport(seen, replies)),
        **extra,
    )


def async_client(
    seen: list[httpx.Request], replies: list[Reply] | None = None, **extra: Any
) -> AsyncLbbClient:
    return AsyncLbbClient(
        DATA_PLANE,
        api_key=KEY,
        graph="main",
        retry_delay=0,
        transport=httpx.MockTransport(fake_transport(seen, replies)),
        **extra,
    )


def body(request: httpx.Request) -> Any:
    return json.loads(request.content) if request.content else None


def suggestion(**overrides: Any) -> dict[str, Any]:
    return {
        "suggestion_id": "s-deal",
        "graph": GRAPH_KEY,
        "key": "integration:hubspot:deals",
        "status": "accepted",
        "title": "Add class Deal",
        "anchor": {"kind": "ontology"},
        "origin": {"kind": "integration", "id": "hubspot", "label": "HubSpot"},
        "change": [{"op": "add_entity_type", "name": "Deal"}],
        "revision": 2,
        "created_at": "2026-10-03T00:00:00Z",
        "updated_at": "2026-10-03T00:00:00Z",
        **overrides,
    }


TURN = {
    "attempt": 0,
    "created_at_ms": 1,
    "message": {"type": "sync"},
    "message_id": "sync-after-s-deal",
    "number": 7,
    "ready_at_ms": 1,
    "result": None,
    "sequence": 7,
    "state_after": None,
    "status": "queued",
    "steps": [],
    "updated_at_ms": 1,
    "version": "1",
    "workflow_id": "hubspot",
}

STATUS = {"kind": "starting", "detail": "The first sync starts soon."}


class SyncIntegrationsTests(unittest.TestCase):
    def test_create_uses_integrations_url_and_the_stack_key(self) -> None:
        seen: list[httpx.Request] = []
        created = {
            "ok": True,
            "id": "hubspot",
            "graph": GRAPH,
            "kind": "hubspot",
            "status": STATUS,
        }
        with sync_client(seen, [{"json": created}]) as client:
            self.assertEqual(client.integrations_url, API)
            answer = client.integrations.create(
                graph=GRAPH,
                id="hubspot",
                kind="hubspot",
                credentials={"HUBSPOT_TOKEN": "pat-eu1-example"},
            )
        self.assertEqual(answer["status"]["kind"], "starting")
        (request,) = seen
        self.assertEqual(request.method, "POST")
        self.assertEqual(str(request.url), f"{API}/v1/integrations/connections")
        self.assertEqual(request.headers["authorization"], f"Bearer {KEY}")
        self.assertEqual(
            body(request),
            {
                "graph": GRAPH,
                "id": "hubspot",
                "kind": "hubspot",
                "credentials": {"HUBSPOT_TOKEN": "pat-eu1-example"},
            },
        )

    def test_create_sends_every_option_in_the_contract_names(self) -> None:
        seen: list[httpx.Request] = []
        with sync_client(seen, integrations_url="http://127.0.0.1:8787/") as client:
            client.integrations.create(
                graph=GRAPH,
                id="linear",
                kind="linear",
                credentials={"LINEAR_API_KEY": "lin_api_example"},
                config={"teams": ["ENG"]},
                every_ms=None,
                ontology_mode="review",
                starter="skip",
                start=False,
            )
        (request,) = seen
        self.assertEqual(
            str(request.url), "http://127.0.0.1:8787/v1/integrations/connections"
        )
        self.assertEqual(
            body(request),
            {
                "graph": GRAPH,
                "id": "linear",
                "kind": "linear",
                "credentials": {"LINEAR_API_KEY": "lin_api_example"},
                "config": {"teams": ["ENG"]},
                "everyMs": None,
                "ontologyMode": "review",
                "starter": "skip",
                "start": False,
            },
        )

    def test_reads_put_the_graph_in_the_query_and_writes_in_the_body(self) -> None:
        seen: list[httpx.Request] = []
        with sync_client(seen) as client:
            client.integrations.list(graph=GRAPH)
            client.integrations.get("hubspot", graph=GRAPH)
            client.integrations.delete("hubspot", graph=GRAPH)
            client.integrations.set_credentials(
                "hubspot", graph=GRAPH, credentials={"HUBSPOT_TOKEN": "pat-eu1-new"}
            )
            client.integrations.set_settings(
                "hubspot", graph=GRAPH, config={"tickets": True}
            )
            client.integrations.pause("hubspot", graph=GRAPH)
            client.integrations.resume("hubspot", graph=GRAPH)
        self.assertEqual(
            [(r.method, r.url.path, dict(r.url.params), body(r)) for r in seen],
            [
                ("GET", "/v1/integrations/connections", {"graph": GRAPH}, None),
                ("GET", "/v1/integrations/connections/hubspot", {"graph": GRAPH}, None),
                (
                    "DELETE",
                    "/v1/integrations/connections/hubspot",
                    {"graph": GRAPH},
                    None,
                ),
                (
                    "PUT",
                    "/v1/integrations/connections/hubspot/credentials",
                    {},
                    {"graph": GRAPH, "credentials": {"HUBSPOT_TOKEN": "pat-eu1-new"}},
                ),
                (
                    "PUT",
                    "/v1/integrations/connections/hubspot/settings",
                    {},
                    {"graph": GRAPH, "config": {"tickets": True}},
                ),
                (
                    "POST",
                    "/v1/integrations/connections/hubspot/pause",
                    {},
                    {"graph": GRAPH},
                ),
                (
                    "POST",
                    "/v1/integrations/connections/hubspot/resume",
                    {},
                    {"graph": GRAPH},
                ),
            ],
        )
        for request in seen:
            self.assertEqual(request.url.host, "api.littlebigbrain.com")

    def test_sync_sends_the_callers_idempotency_key(self) -> None:
        seen: list[httpx.Request] = []
        turn = {"number": 3, "status": "queued", "message_id": "api-sync-nightly-1"}
        with sync_client(
            seen,
            [{"json": {"ok": True, "id": "hubspot", "graph": GRAPH, "turn": turn}}],
        ) as client:
            answer = client.integrations.sync(
                "hubspot", graph=GRAPH, full=True, idempotency_key="nightly-1"
            )
        self.assertEqual(answer["turn"], turn)
        (request,) = seen
        self.assertEqual(request.url.path, "/v1/integrations/connections/hubspot/sync")
        self.assertEqual(request.headers["idempotency-key"], "nightly-1")
        self.assertEqual(body(request), {"graph": GRAPH, "full": True})

    def test_sync_makes_one_key_per_call_and_retries_under_it(self) -> None:
        seen: list[httpx.Request] = []
        busy = {
            "status": 503,
            "headers": {"retry-after": "0"},
            "json": {"ok": False, "error": "briefly busy", "code": "data_plane_busy"},
        }
        with sync_client(seen, [busy]) as client:
            client.integrations.sync("hubspot", graph=GRAPH)
            client.integrations.sync("hubspot", graph=GRAPH)
        self.assertEqual(len(seen), 3)
        key = seen[0].headers["idempotency-key"]
        self.assertRegex(key, r"^[A-Za-z0-9_.-]{1,100}$")
        self.assertEqual(seen[1].headers["idempotency-key"], key)
        self.assertNotEqual(seen[2].headers["idempotency-key"], key)
        self.assertEqual(body(seen[0]), {"graph": GRAPH})

    def test_erase_names_the_graph_in_the_path_and_confirms_it(self) -> None:
        seen: list[httpx.Request] = []
        erased = {
            "ok": True,
            "graph": GRAPH,
            "connections_deleted": 1,
            "graph_deleted": True,
            "reclaims": [],
        }
        with sync_client(seen, [{"json": erased}]) as client:
            answer = client.integrations.erase(GRAPH, confirm=GRAPH)
        self.assertEqual(answer["connections_deleted"], 1)
        (request,) = seen
        self.assertEqual(request.method, "POST")
        self.assertEqual(
            str(request.url),
            f"{API}/v1/integrations/graphs/{GRAPH}/erase?confirm={GRAPH}",
        )
        self.assertEqual(request.content, b"")

    def test_an_error_carries_its_code_and_details(self) -> None:
        seen: list[httpx.Request] = []
        conflicts = [{"term": "Deal", "reason": "class Deal has another parent"}]
        refusal = {
            "status": 409,
            "json": {
                "ok": False,
                "error": "the graph holds a starter term differently",
                "code": "starter_conflict",
                "details": {"conflicts": conflicts},
            },
        }
        with sync_client(seen, [refusal]) as client, self.assertRaises(
            LbbError
        ) as raised:
            client.integrations.create(
                graph=GRAPH,
                id="hubspot",
                kind="hubspot",
                credentials={"HUBSPOT_TOKEN": "pat-eu1-example"},
            )
        error = raised.exception
        self.assertEqual(error.status_code, 409)
        self.assertEqual(error.code, "starter_conflict")
        self.assertEqual(str(error), "the graph holds a starter term differently")
        self.assertEqual(error.details, {"conflicts": conflicts})

    def test_a_429_carries_retry_after_seconds(self) -> None:
        seen: list[httpx.Request] = []
        limited = {
            "status": 429,
            "headers": {"Retry-After": "1800"},
            "json": {
                "ok": False,
                "error": "at most 20 connections are created per stack and hour",
                "code": "rate_limited",
            },
        }
        with sync_client(seen, [limited], retry_budget_ms=1000) as client:
            with self.assertRaises(LbbError) as raised:
                client.integrations.create(
                    graph=GRAPH,
                    id="hubspot",
                    kind="hubspot",
                    credentials={"HUBSPOT_TOKEN": "pat-eu1-example"},
                )
        error = raised.exception
        self.assertEqual(error.status_code, 429)
        self.assertEqual(error.code, "rate_limited")
        self.assertEqual(error.retry_after_seconds, 1800)
        # A wait past the retry budget is not slept: the error comes at once.
        self.assertEqual(len(seen), 1)

    def test_a_rate_limited_read_is_retried_after_retry_after(self) -> None:
        seen: list[httpx.Request] = []
        limited = {
            "status": 429,
            "headers": {"retry-after": "0"},
            "json": {"ok": False, "error": "too many requests", "code": "rate_limited"},
        }
        listed = {"ok": True, "graph": GRAPH, "connections": []}
        with sync_client(seen, [limited, {"json": listed}]) as client:
            answer = client.integrations.list(graph=GRAPH)
        self.assertEqual(answer["connections"], [])
        self.assertEqual(len(seen), 2)

    def test_a_data_plane_error_takes_retry_after_from_the_header_without_a_hint(
        self,
    ) -> None:
        seen: list[httpx.Request] = []
        busy = {
            "status": 429,
            "headers": {"Retry-After": "5"},
            "json": {"error": {"code": "ingest_busy", "message": "busy"}},
        }
        hinted = {
            "status": 429,
            "headers": {"Retry-After": "5"},
            "json": {"error": {"code": "ingest_busy", "retry_after_seconds": 2}},
        }
        with sync_client(seen, [busy, hinted], max_retries=0) as client:
            with self.assertRaises(LbbError) as header_only:
                client.raw_request("GET", "/v1/graph/status")
            with self.assertRaises(LbbError) as with_hint:
                client.raw_request("GET", "/v1/graph/status")
        self.assertEqual(header_only.exception.code, "ingest_busy")
        self.assertEqual(header_only.exception.retry_after_seconds, 5)
        self.assertEqual(with_hint.exception.retry_after_seconds, 2)
        # The data-plane request keeps the client's graph scope.
        self.assertEqual(seen[0].url.params["graph"], "main")
        self.assertEqual(
            seen[0].url.host, "0abc1def--production.db.eu.littlebigbrain.com"
        )

    def test_a_fault_of_the_service_has_no_code(self) -> None:
        seen: list[httpx.Request] = []
        fault = {"status": 500, "json": {"ok": False, "error": "internal error"}}
        with sync_client(seen, [fault], max_retries=0) as client:
            with self.assertRaises(LbbError) as raised:
                client.integrations.connectors()
        self.assertEqual(raised.exception.status_code, 500)
        self.assertIsNone(raised.exception.code)
        self.assertEqual(str(raised.exception), "internal error")

    def test_suggestions_are_read_from_the_data_plane_for_one_connection(self) -> None:
        seen: list[httpx.Request] = []
        listing = {
            "graph": GRAPH_KEY,
            "ontology_version": 4,
            "counts": {"open": 1, "accepted": 0, "dismissed": 0, "superseded": 0},
            "suggestions": [],
            "truncated": False,
        }
        with sync_client(seen, [{"json": listing}]) as client:
            client.integrations.suggestions("hubspot", graph=GRAPH, status="open")
        (request,) = seen
        self.assertEqual(request.method, "GET")
        self.assertEqual(f"{request.url.scheme}://{request.url.host}", DATA_PLANE)
        self.assertEqual(request.url.path, "/v1/ontology/suggestions")
        self.assertEqual(
            sorted(request.url.params.multi_items()),
            [
                ("graph", GRAPH),
                ("origin_id", "hubspot"),
                ("origin_kind", "integration"),
                ("status", "open"),
            ],
        )
        self.assertEqual(request.headers["authorization"], f"Bearer {KEY}")

    def test_accept_with_sync_sends_sync_after_the_suggestion_id(self) -> None:
        seen: list[httpx.Request] = []
        with sync_client(seen, [{"json": suggestion()}, {"json": TURN}]) as client:
            result = client.integrations.accept(
                "s-deal",
                graph=GRAPH,
                change=[AddEntityTypeOp(op="add_entity_type", name="Deal")],
                sync=True,
            )
        self.assertIsInstance(result.suggestion, OntologyChangeSuggestion)
        self.assertIsInstance(result.sync, WorkflowTurn)
        accept, message = seen
        self.assertEqual(
            accept.url.host, "0abc1def--production.db.eu.littlebigbrain.com"
        )
        self.assertEqual(accept.url.path, "/v1/ontology/suggestions/accept")
        self.assertEqual(
            dict(accept.url.params), {"graph": GRAPH, "suggestion_id": "s-deal"}
        )
        self.assertEqual(
            body(accept), {"change": [{"op": "add_entity_type", "name": "Deal"}]}
        )
        self.assertEqual(
            message.url.host, "0abc1def--production.db.eu.littlebigbrain.com"
        )
        self.assertEqual(message.url.path, "/v1/workflows/instances/message")
        self.assertEqual(dict(message.url.params), {"graph": GRAPH})
        self.assertEqual(
            body(message),
            {
                "workflow_id": "hubspot",
                "id": "sync-after-s-deal",
                "message": {"type": "sync"},
            },
        )

    def test_accept_sends_no_sync_without_sync_or_for_an_identity_link(self) -> None:
        seen: list[httpx.Request] = []
        identity = {
            "members": [
                {"type": "Organization", "key": "hubspot:companies:1", "name": "Acme"},
                {"type": "Organization", "key": "linear:teams:1", "name": "Acme"},
            ],
            "reason": "domain acme.test",
            "confidence": 1,
        }
        replies = [
            {"json": suggestion()},
            {"json": suggestion(proposed_identities=[identity])},
            {"json": suggestion(origin={"kind": "person", "id": "ada"})},
            {"json": suggestion(status="open")},
        ]
        with sync_client(seen, replies) as client:
            plain = client.integrations.accept(
                "s-deal", graph=GRAPH, comment="Fits our CRM.", author="ana"
            )
            link = client.integrations.accept("s-deal", graph=GRAPH, sync=True)
            person = client.integrations.accept("s-deal", graph=GRAPH, sync=True)
            still_open = client.integrations.accept("s-deal", graph=GRAPH, sync=True)
        for result in (plain, link, person, still_open):
            self.assertIsNone(result.sync)
        self.assertEqual(body(seen[0]), {"comment": "Fits our CRM.", "author": "ana"})
        self.assertEqual(
            [request.url.path for request in seen],
            ["/v1/ontology/suggestions/accept"] * 4,
        )

    def test_dismiss_sends_the_reason_to_the_data_plane(self) -> None:
        seen: list[httpx.Request] = []
        with sync_client(seen, [{"json": suggestion(status="dismissed")}]) as client:
            dismissed = client.integrations.dismiss(
                "s-deal", graph=GRAPH, reason="We track deals elsewhere.", author="ana"
            )
        self.assertIsInstance(dismissed, OntologyChangeSuggestion)
        (request,) = seen
        self.assertEqual(request.url.path, "/v1/ontology/suggestions/dismiss")
        self.assertEqual(
            dict(request.url.params), {"graph": GRAPH, "suggestion_id": "s-deal"}
        )
        self.assertEqual(
            body(request), {"reason": "We track deals elsewhere.", "author": "ana"}
        )


class AsyncIntegrationsTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_methods_send_the_same_requests(self) -> None:
        seen: list[httpx.Request] = []
        async with async_client(seen) as client:
            await client.integrations.connectors()
            await client.integrations.create(
                graph=GRAPH,
                id="hubspot",
                kind="hubspot",
                credentials={"HUBSPOT_TOKEN": "pat-eu1-example"},
            )
            await client.integrations.list(graph=GRAPH)
            await client.integrations.get("hubspot", graph=GRAPH)
            await client.integrations.set_credentials(
                "hubspot", graph=GRAPH, credentials={"HUBSPOT_TOKEN": "x"}
            )
            await client.integrations.set_settings("hubspot", graph=GRAPH, config={})
            await client.integrations.sync(
                "hubspot", graph=GRAPH, idempotency_key="k-1"
            )
            await client.integrations.pause("hubspot", graph=GRAPH)
            await client.integrations.resume("hubspot", graph=GRAPH)
            await client.integrations.delete("hubspot", graph=GRAPH)
            await client.integrations.erase(GRAPH, confirm=GRAPH)
        self.assertEqual(
            [(r.method, r.url.path) for r in seen],
            [
                ("GET", "/v1/integrations/connectors"),
                ("POST", "/v1/integrations/connections"),
                ("GET", "/v1/integrations/connections"),
                ("GET", "/v1/integrations/connections/hubspot"),
                ("PUT", "/v1/integrations/connections/hubspot/credentials"),
                ("PUT", "/v1/integrations/connections/hubspot/settings"),
                ("POST", "/v1/integrations/connections/hubspot/sync"),
                ("POST", "/v1/integrations/connections/hubspot/pause"),
                ("POST", "/v1/integrations/connections/hubspot/resume"),
                ("DELETE", "/v1/integrations/connections/hubspot"),
                ("POST", f"/v1/integrations/graphs/{GRAPH}/erase"),
            ],
        )
        for request in seen:
            self.assertEqual(request.url.host, "api.littlebigbrain.com")
            self.assertEqual(request.headers["authorization"], f"Bearer {KEY}")
            self.assertNotIn(("graph", "main"), request.url.params.multi_items())
        self.assertEqual(seen[6].headers["idempotency-key"], "k-1")

    async def test_async_accept_with_sync_sends_sync_after_the_suggestion_id(
        self,
    ) -> None:
        seen: list[httpx.Request] = []
        async with async_client(
            seen, [{"json": suggestion()}, {"json": TURN}]
        ) as client:
            result = await client.integrations.accept("s-deal", graph=GRAPH, sync=True)
        self.assertIsInstance(result.sync, WorkflowTurn)
        self.assertEqual(
            [request.url.path for request in seen],
            ["/v1/ontology/suggestions/accept", "/v1/workflows/instances/message"],
        )
        self.assertEqual(body(seen[1])["id"], "sync-after-s-deal")
        self.assertEqual(body(seen[1])["workflow_id"], "hubspot")

    async def test_async_suggestions_and_dismiss_use_the_data_plane(self) -> None:
        seen: list[httpx.Request] = []
        listing = {
            "graph": GRAPH_KEY,
            "ontology_version": 4,
            "counts": {"open": 1, "accepted": 0, "dismissed": 0, "superseded": 0},
            "suggestions": [],
            "truncated": False,
        }
        replies = [{"json": listing}, {"json": suggestion(status="dismissed")}]
        async with async_client(seen, replies) as client:
            await client.integrations.suggestions("hubspot", graph=GRAPH, status="open")
            dismissed = await client.integrations.dismiss(
                "s-deal", graph=GRAPH, reason="Not needed."
            )
        self.assertIsInstance(dismissed, OntologyChangeSuggestion)
        for request in seen:
            self.assertEqual(
                request.url.host, "0abc1def--production.db.eu.littlebigbrain.com"
            )
            self.assertEqual(request.url.params["graph"], GRAPH)

    async def test_async_error_carries_retry_after_seconds(self) -> None:
        seen: list[httpx.Request] = []
        limited = {
            "status": 429,
            "headers": {"Retry-After": "30"},
            "json": {"ok": False, "error": "too many requests", "code": "rate_limited"},
        }
        async with async_client(seen, [limited], max_retries=0) as client:
            with self.assertRaises(LbbError) as raised:
                await client.integrations.list(graph=GRAPH)
        self.assertEqual(raised.exception.code, "rate_limited")
        self.assertEqual(raised.exception.retry_after_seconds, 30)


def _find_contract() -> Path:
    for parent in Path(__file__).resolve().parents:
        candidate = parent / "contracts" / "integrations-openapi.json"
        if candidate.is_file():
            return candidate
    raise FileNotFoundError(
        "contracts/integrations-openapi.json not found in any ancestor"
    )


class IntegrationsContractTests(unittest.TestCase):
    """Every integrations route the client calls is a route of the contract,
    with the contract's query and body fields, and every route of the
    contract has a client method."""

    def test_every_route_matches_the_contract(self) -> None:
        contract = json.loads(_find_contract().read_text())
        operations = [
            (
                f"{method.upper()} {path}",
                method.upper(),
                re.compile("^" + re.sub(r"\{\w+\}", "[^/]+", path) + "$"),
                operation,
            )
            for path, methods in contract["paths"].items()
            for method, operation in methods.items()
        ]
        seen: list[httpx.Request] = []
        with sync_client(seen) as client:
            api = client.integrations
            api.connectors()
            api.create(
                graph=GRAPH,
                id="hubspot",
                kind="hubspot",
                credentials={"HUBSPOT_TOKEN": "x"},
                config={},
                every_ms=3_600_000,
                ontology_mode="auto",
                starter="apply",
                start=True,
            )
            api.list(graph=GRAPH)
            api.get("hubspot", graph=GRAPH)
            api.set_credentials(
                "hubspot", graph=GRAPH, credentials={"HUBSPOT_TOKEN": "x"}
            )
            api.set_settings("hubspot", graph=GRAPH, config={})
            api.sync("hubspot", graph=GRAPH, full=False)
            api.pause("hubspot", graph=GRAPH)
            api.resume("hubspot", graph=GRAPH)
            api.delete("hubspot", graph=GRAPH)
            api.erase(GRAPH, confirm=GRAPH)
        called: set[str] = set()
        for request in seen:
            matched = [
                (key, operation)
                for key, method, pattern, operation in operations
                if method == request.method and pattern.match(request.url.path)
            ]
            self.assertEqual(len(matched), 1, f"{request.method} {request.url.path}")
            key, operation = matched[0]
            called.add(key)
            query = sorted(
                parameter["name"]
                for parameter in operation.get("parameters", [])
                if parameter["in"] == "query"
            )
            self.assertEqual(sorted(request.url.params.keys()), query, key)
            schema_ref = (
                operation.get("requestBody", {})
                .get("content", {})
                .get("application/json", {})
                .get("schema", {})
                .get("$ref")
            )
            if schema_ref:
                schema = contract["components"]["schemas"][
                    schema_ref.rsplit("/", 1)[-1]
                ]
                self.assertEqual(
                    sorted(body(request)), sorted(schema["properties"]), key
                )
            else:
                self.assertIsNone(body(request), key)
        self.assertEqual(
            called, {key for key, _method, _pattern, _operation in operations}
        )


if __name__ == "__main__":
    unittest.main()
