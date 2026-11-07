"""Namespace queries through the public sync and async HTTP transports."""

from __future__ import annotations

import asyncio
import json

import httpx
import pytest

from lbb import AsyncLbbClient, LbbClient, LbbError, models


def namespace_response() -> dict:
    return {
        "namespace": {"id": "ns_customer", "incarnation": "inc_customer", "authority_revision": 3},
        "selection_token": "selection_exact",
        "coverage": {"selected_graphs": 1, "queried_graphs": 1, "complete": True},
        "results": '{"head": {}, "boolean": false}',
        "sources": [
            {
                "name": "urn:lbb:graph:project",
                "graph_id": "project",
                "graph_epoch": "18446744073709551615",
                "commit_seq": 9007199254740993,
                "generation": 7,
                "snapshot_token": "snapshot_exact",
            }
        ],
        "charged_read_bytes": 128,
    }


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("typed", [False, True])
def test_namespace_query_keeps_environment_and_exact_versions(asynchronous: bool, typed: bool) -> None:
    seen: list[httpx.Request] = []
    reply = namespace_response()
    body = {
        "namespace_id": "ns_customer",
        "include_descendants": False,
        "default_graph": "empty",
        "query": "ASK { ?s ?p ?o }",
    }
    request_body = models.NamespaceQueryRequest.model_validate(body) if typed else body
    method = "sparql_namespace_model" if typed else "sparql_namespace"

    def handle(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json=reply)

    config = {
        "base_url": "https://example.test",
        "api_key": "environment-key",
        "environment": "production",
        "graph": "unrelated-default-graph",
        "default_consistency": "strong",
        "transport": httpx.MockTransport(handle),
    }
    options = {"headers": {"x-query-trace": "trace-exact"}, "timeout": 2.5}
    if asynchronous:
        async def run():
            async with AsyncLbbClient(**config) as client:
                result = await getattr(client, method)(request_body, options=options)
                await client.raw_request("GET", "/v1/metadata")
                return result

        result = asyncio.run(run())
    else:
        with LbbClient(**config) as client:
            result = getattr(client, method)(request_body, options=options)
            client.raw_request("GET", "/v1/metadata")

    if typed:
        assert isinstance(result, models.NamespaceQueryResponse)
        assert result.model_dump(mode="json") == reply
        assert result.sources[0].graph_epoch == "18446744073709551615"
        assert result.sources[0].commit_seq == 9007199254740993
    else:
        assert result == reply
    request, ordinary = seen
    assert request.method == "POST"
    assert request.url.path == "/v1/query/namespace"
    assert dict(request.url.params) == {"stack": "production"}
    assert request.headers["authorization"] == "Bearer environment-key"
    assert request.headers["x-query-trace"] == "trace-exact"
    assert request.extensions["timeout"]["read"] == 2.5
    assert json.loads(request.content) == body
    assert "idempotency-key" not in request.headers
    assert ordinary.url.params["graph"] == "unrelated-default-graph"
    assert ordinary.url.params["stack"] == "production"


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("typed", [False, True])
@pytest.mark.parametrize("failure", ["server", "network", "terminal-rate-limit"])
def test_namespace_execution_failures_are_not_replayed(
    asynchronous: bool, typed: bool, failure: str
) -> None:
    seen: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if failure == "network":
            raise httpx.ReadError("connection lost after sending query", request=request)
        return httpx.Response(
            429 if failure == "terminal-rate-limit" else 503,
            json={
                "error": {
                    "code": "quota_exhausted" if failure == "terminal-rate-limit" else "server_busy",
                    "message": "query refused",
                    "retryable": failure != "terminal-rate-limit",
                }
            },
        )

    config = {"transport": httpx.MockTransport(handle), "max_retries": 3, "retry_delay": 0}
    method = "sparql_namespace_model" if typed else "sparql_namespace"
    body = {"namespace_id": "ns_customer", "query": "ASK {}"}
    if asynchronous:
        async def run() -> None:
            async with AsyncLbbClient(**config) as client:
                await getattr(client, method)(body)

        def call() -> None:
            asyncio.run(run())
    else:
        def call() -> None:
            with LbbClient(**config) as client:
                getattr(client, method)(body)

    with pytest.raises(httpx.ReadError if failure == "network" else LbbError):
        call()
    assert len(seen) == 1


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("disable_retry", [False, True])
def test_namespace_retries_pre_execution_rate_limits_and_respects_options(
    asynchronous: bool, disable_retry: bool
) -> None:
    seen: list[httpx.Request] = []
    reply = namespace_response()

    def handle(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if len(seen) == 1:
            return httpx.Response(
                429,
                headers={"Retry-After": "0"},
                json={"error": {"code": "server_busy", "message": "busy", "retryable": True}},
            )
        return httpx.Response(200, json=reply)

    config = {"transport": httpx.MockTransport(handle), "max_retries": 2, "retry_delay": 0}
    options = {"retry": False} if disable_retry else {}
    body = {"namespace_id": "ns_customer", "query": "ASK {}"}
    if asynchronous:
        async def run():
            async with AsyncLbbClient(**config) as client:
                return await client.sparql_namespace(body, options=options)

        def call():
            return asyncio.run(run())
    else:
        def call():
            with LbbClient(**config) as client:
                return client.sparql_namespace(body, options=options)

    if disable_retry:
        with pytest.raises(LbbError):
            call()
        assert len(seen) == 1
    else:
        assert call() == reply
        assert len(seen) == 2
        assert seen[0].content == seen[1].content
