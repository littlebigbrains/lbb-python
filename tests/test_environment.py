"""Environment naming keeps the deployed routing and credential contract."""

import unittest

import httpx

from lbb import AsyncLbbClient, LbbClient


class EnvironmentTests(unittest.TestCase):
    def test_environment_scope_and_graph_override(self) -> None:
        seen: list[httpx.Request] = []

        def handle(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            return httpx.Response(200, json={})

        with LbbClient(
            "https://example.test",
            api_key="session",
            graph="main",
            environment="production",
            transport=httpx.MockTransport(handle),
        ) as client:
            client.workflow_starters()
            client.graph("second").workflow_starters()
            client.integrations.provider_apps()
            client.workflow_starters()

        self.assertEqual(
            [dict(r.url.params) for r in seen],
            [
                {"graph": "main", "stack": "production"},
                {"graph": "second", "stack": "production"},
                {},
                {"graph": "main", "stack": "production"},
            ],
        )
        self.assertEqual(seen[2].url.host, "api.littlebigbrain.com")
        for request in seen:
            self.assertEqual(request.headers["authorization"], "Bearer session")


class AsyncEnvironmentTests(unittest.IsolatedAsyncioTestCase):
    async def test_environment_scope_and_graph_override(self) -> None:
        seen: list[httpx.Request] = []

        def handle(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            return httpx.Response(200, json={})

        async with AsyncLbbClient(
            "https://example.test",
            api_key="session",
            graph="main",
            environment="production",
            transport=httpx.MockTransport(handle),
        ) as client:
            await client.workflow_starters()
            await client.graph("second").workflow_starters()
            await client.integrations.provider_apps()
            await client.workflow_starters()

        self.assertEqual(
            [dict(r.url.params) for r in seen],
            [
                {"graph": "main", "stack": "production"},
                {"graph": "second", "stack": "production"},
                {},
                {"graph": "main", "stack": "production"},
            ],
        )
        self.assertEqual(seen[2].url.host, "api.littlebigbrain.com")
        for request in seen:
            self.assertEqual(request.headers["authorization"], "Bearer session")
