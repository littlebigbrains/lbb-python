"""``client.workflows.named.send_events``: the events of a stream go to their
workflow's route, sync and async, with an event's ``type`` under its wire
name."""

from __future__ import annotations

import asyncio
import json
import unittest

import httpx

from lbb import AsyncLbbClient, LbbClient
from lbb.models import WorkflowEvent

RECEIPT = {
    "object": "workflow_events",
    "workflow": "orders",
    "trigger": "stream",
    "received": 2,
    "received_at": "2026-10-21T10:00:00.120000Z",
}
EVENTS = [
    WorkflowEvent(id="evt_1", key="customer-42", type="order.paid", data={"order": "A-1007"}),
    {"id": "evt_2", "key": "customer-42", "data": {}},
]


def transport(seen: list[httpx.Request]) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json=RECEIPT)

    return httpx.MockTransport(handler)


class SendEventsTests(unittest.TestCase):
    def check(self, seen: list[httpx.Request]) -> None:
        self.assertEqual(len(seen), 1)
        self.assertEqual((seen[0].method, seen[0].url.path), ("POST", "/v1/workflows/named/events"))
        self.assertEqual(dict(seen[0].url.params), {"graph": "crm", "name": "orders"})
        body = json.loads(seen[0].content)
        self.assertEqual(body["events"][0]["type"], "order.paid")
        self.assertEqual(body["events"][0]["id"], "evt_1")
        self.assertEqual(body["events"][1], {"id": "evt_2", "key": "customer-42", "data": {}})

    def test_events_go_to_the_workflows_route(self) -> None:
        seen: list[httpx.Request] = []
        with LbbClient("http://h", graph="crm", transport=transport(seen)) as client:
            receipt = client.workflows.named.send_events("orders", EVENTS)
        self.check(seen)
        self.assertEqual(receipt.received, 2)
        self.assertEqual(receipt.trigger, "stream")

    def test_the_async_client_sends_the_same_request(self) -> None:
        seen: list[httpx.Request] = []

        async def send() -> int:
            async with AsyncLbbClient("http://h", graph="crm", transport=transport(seen)) as client:
                receipt = await client.workflows.named.send_events("orders", EVENTS)
                return receipt.received

        self.assertEqual(asyncio.run(send()), 2)
        self.check(seen)


if __name__ == "__main__":
    unittest.main()
