"""Catalog paging and delegated token requests through both SDK transports."""

from __future__ import annotations

import asyncio
import json

import httpx

from lbb import AsyncLbbClient, LbbClient


def test_catalog_scopes_idempotency_and_opaque_cursor() -> None:
    seen: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={"items": [], "next_cursor": "opaque+/=", "scanned": 0})

    with LbbClient(
        base_url="https://example.test",
        graph="project",
        environment="prod",
        transport=httpx.MockTransport(handle),
    ) as client:
        client.catalog.create_namespace({"name": "customer"}, idempotency_key="create-customer")
        client.catalog.adopt_graph({"expected_graph_epoch": "3"}, idempotency_key="adopt-project")
        first = client.catalog.graphs(limit=1)
        assert first["items"] == []
        client.catalog.graphs(limit=1, cursor=first["next_cursor"])
        client.catalog.resolve("customer/projects")
        client.access.create_read_token({"grants": [{"kind": "graph", "graph_id": "project"}]})
        client.access.revoke_token("cred_stable")
    assert seen[0].headers["idempotency-key"] == "create-customer"
    assert seen[1].headers["idempotency-key"] == "adopt-project"
    assert json.loads(seen[1].content) == {"expected_graph_epoch": "3"}
    assert seen[3].url.params["cursor"] == "opaque+/="
    assert seen[4].url.params["path"] == "customer/projects"
    assert json.loads(seen[-1].content) == {"credential_id": "cred_stable"}
    for request in seen:
        assert request.url.params["graph"] == "project"
        assert request.url.params["stack"] == "prod"


def test_async_catalog_and_tokens_use_the_same_contract() -> None:
    seen: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={"ok": True})

    async def run() -> None:
        async with AsyncLbbClient(
            base_url="https://example.test", graph="project", transport=httpx.MockTransport(handle)
        ) as client:
            assert await client.catalog.root() == {"ok": True}
            await client.catalog.create_namespace({"name": "team"}, idempotency_key="team")
            await client.catalog.adopt_graph({"expected_graph_epoch": "2"}, idempotency_key="adopt")
            await client.access.create_read_token(
                {"grants": [{"kind": "namespace", "namespace_id": "ns_stable"}]}
            )
            await client.access.revoke_token("cred_stable")

    asyncio.run(run())
    assert [request.url.path for request in seen] == [
        "/v1/catalog/root",
        "/v1/catalog/namespaces",
        "/v1/catalog/graphs/adopt",
        "/v1/access/tokens",
        "/v1/access/tokens/revoke",
    ]


def test_catalog_recovery_and_reference_dataset_keep_exact_selection() -> None:
    seen: list[httpx.Request] = []
    reply = {
        "results": "{}",
        "sources": [{"graph_epoch": "18446744073709551615"}],
        "charged_read_bytes": 128,
    }
    body = {
        "query": "ASK {}",
        "expected_graph_epoch": "18446744073709551615",
        "references": [
            {
                "name": "urn:standards",
                "graph_id": "standards",
                "expected_graph_epoch": "9007199254740993",
            }
        ],
    }

    def handle(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json=reply)

    with LbbClient(
        base_url="https://example.test", graph="project", transport=httpx.MockTransport(handle)
    ) as client:
        client.catalog.graphs(
            owner_id="ns_customer", include_descendants=True, cursor="opaque+/=", limit=1
        )
        client.catalog.namespaces(parent_id="ns_customer", limit=1)
        client.catalog.pending_operations(cursor="opaque+/=", limit=2)
        client.catalog.repair_operation("op_exact")
        client.catalog.repair_indexes({"kind": "graphs", "limit": 2})
    with LbbClient(
        base_url="https://example.test",
        graph="project",
        transport=httpx.MockTransport(handle),
    ) as client:
        assert client.sparql_dataset(body) == reply
    assert seen[0].url.params["include_descendants"] == "true"
    assert seen[0].url.params["owner_id"] == "ns_customer"
    assert seen[0].url.params["cursor"] == "opaque+/="
    assert seen[1].url.params["parent_id"] == "ns_customer"
    assert json.loads(seen[3].content) == {"operation_id": "op_exact"}
    assert json.loads(seen[-1].content) == body

    async def run() -> None:
        async with AsyncLbbClient(
            base_url="https://example.test", transport=httpx.MockTransport(handle)
        ) as client:
            await client.catalog.repair_operation("op_exact")
            assert await client.sparql_dataset(body) == reply

    asyncio.run(run())


def test_owner_usage_preserves_profile_exact_timestamp_and_continuation_in_both_transports() -> (
    None
):
    seen: list[httpx.Request] = []
    cpu = "340282366920938463463374607431768211455"
    reply = {
        "profile": "raw-resource-receipts-v1",
        "status": "partial",
        "page_totals": {"cpu_nanos": cpu},
        "next_cursor": "opaque+/=",
        "complete_through_micros": None,
    }

    def handle(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        assert request.method == "GET"
        assert request.content == b""
        return httpx.Response(200, json=reply)

    with LbbClient(
        base_url="https://example.test",
        environment="prod",
        api_key="environment-key",
        transport=httpx.MockTransport(handle),
    ) as client:
        first = client.catalog.usage(
            owner_id="ns_customer",
            owner_incarnation="inc_original",
            period_start_micros=1800000000000000,
            limit=1,
        )
        assert first["page_totals"]["cpu_nanos"] == cpu

    async def run() -> None:
        async with AsyncLbbClient(
            base_url="https://example.test",
            environment="prod",
            api_key="environment-key",
            transport=httpx.MockTransport(handle),
        ) as client:
            assert (
                await client.catalog.usage(
                    owner_id="ns_customer",
                    owner_incarnation="inc_original",
                    period_start_micros=9007200000000000,
                    profile="raw-resource-receipts-v1",
                    cursor=first["next_cursor"],
                    limit=2,
                )
                == reply
            )

    asyncio.run(run())
    assert "profile" not in seen[0].url.params
    assert seen[0].url.params["period_start_micros"] == "1800000000000000"
    assert seen[1].url.params["period_start_micros"] == "9007200000000000"
    assert seen[1].url.params["profile"] == "raw-resource-receipts-v1"
    assert seen[1].url.params["cursor"] == "opaque+/="
    assert seen[1].url.params["limit"] == "2"
    for request in seen:
        assert request.url.path == "/v1/catalog/usage"
        assert request.url.params["owner_id"] == "ns_customer"
        assert request.url.params["owner_incarnation"] == "inc_original"
        assert request.url.params["stack"] == "prod"
        assert request.headers["authorization"] == "Bearer environment-key"


def test_completed_execution_reporting_handoff_refusal_is_not_replayed_by_either_transport() -> (
    None
):
    from lbb import LbbError

    seen: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        assert request.method == "POST"
        assert request.url.path == "/v1/query/dataset"
        return httpx.Response(
            503,
            json={
                "error": {
                    "code": "application_usage_handoff_unconfirmed",
                    "retryable": False,
                    "message": "Execution completed; check operation state before retry.",
                }
            },
        )

    body = {
        "query": "SELECT ?s WHERE { ?s ?p ?o } LIMIT 1",
        "expected_graph_epoch": "0",
        "references": [],
    }
    with LbbClient(
        base_url="https://example.test", transport=httpx.MockTransport(handle)
    ) as client:
        try:
            client.sparql_dataset(body)
        except LbbError as error:
            assert error.status_code == 503
            assert error.code == "application_usage_handoff_unconfirmed"
            assert error.retryable is False
        else:
            raise AssertionError("handoff refusal must be surfaced")
    assert len(seen) == 1

    async def run() -> None:
        async with AsyncLbbClient(
            base_url="https://example.test", transport=httpx.MockTransport(handle)
        ) as client:
            try:
                await client.sparql_dataset(body)
            except LbbError as error:
                assert error.status_code == 503
                assert error.code == "application_usage_handoff_unconfirmed"
                assert error.retryable is False
            else:
                raise AssertionError("handoff refusal must be surfaced")

    asyncio.run(run())
    assert len(seen) == 2, "each original request must execute once without automatic replay"


def test_saved_dataset_views_preserve_exact_epochs_in_sync_and_async_clients() -> None:
    seen: list[httpx.Request] = []
    definition = {
        "primary_graph_id": "project",
        "expected_graph_epoch": "18446744073709551615",
        "references": [],
    }
    revision = "a" * 64
    reply = {"revision": revision, "definition": definition}
    body = {
        "query": "ASK {}",
        "expected_graph_epoch": "18446744073709551615",
        "references": [],
        "view_revision": revision,
    }

    def handle(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json=reply)

    with LbbClient(
        base_url="https://example.test", graph="project", transport=httpx.MockTransport(handle)
    ) as client:
        assert client.save_dataset_view(definition) == reply
        assert client.get_dataset_view(revision) == reply
        client.sparql_dataset(body)

    async def run() -> None:
        async with AsyncLbbClient(
            base_url="https://example.test", graph="project", transport=httpx.MockTransport(handle)
        ) as client:
            assert await client.save_dataset_view(definition) == reply
            assert await client.get_dataset_view(revision) == reply
            await client.sparql_dataset(body)

    asyncio.run(run())
    for offset in (0, 3):
        assert seen[offset].url.path == "/v1/catalog/views"
        assert json.loads(seen[offset].content) == definition
        assert seen[offset + 1].url.params["revision"] == revision
        assert json.loads(seen[offset + 2].content) == body
        assert seen[offset + 2].url.params["graph"] == "project"
