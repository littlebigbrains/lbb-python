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
    reply = {"results": "{}", "sources": [{"graph_epoch": "18446744073709551615"}], "charged_read_bytes": 128}
    body = {
        "query": "ASK {}",
        "expected_graph_epoch": "18446744073709551615",
        "references": [
            {"name": "urn:standards", "graph_id": "standards", "expected_graph_epoch": "9007199254740993"}
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


def test_saved_dataset_views_preserve_exact_epochs_in_sync_and_async_clients() -> None:
    seen: list[httpx.Request] = []
    definition = {"primary_graph_id": "project", "expected_graph_epoch": "18446744073709551615", "references": []}
    revision = "a" * 64
    reply = {"revision": revision, "definition": definition}
    body = {"query": "ASK {}", "expected_graph_epoch": "18446744073709551615", "references": [], "view_revision": revision}

    def handle(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json=reply)

    with LbbClient(base_url="https://example.test", graph="project", transport=httpx.MockTransport(handle)) as client:
        assert client.save_dataset_view(definition) == reply
        assert client.get_dataset_view(revision) == reply
        client.sparql_dataset(body)

    async def run() -> None:
        async with AsyncLbbClient(base_url="https://example.test", graph="project", transport=httpx.MockTransport(handle)) as client:
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
