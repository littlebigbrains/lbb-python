"""The tools for agents send the documented requests: ``query.names``
(``POST /v1/query/names``), ``query.describe`` (``POST
/v1/query/describe``), ``query.commit_at`` (``GET /v1/graph/commit-at``) and
``query.compare`` (``POST /v1/query/compare``)."""

from __future__ import annotations

import json
import unittest
from typing import Any

import httpx

from lbb import AsyncLbbClient, LbbClient, models


def transport(seen: list[httpx.Request], responses: list[dict[str, Any]]) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        item = responses.pop(0) if responses else {"json": {}}
        return httpx.Response(item.get("status", 200), json=item.get("json", {}))

    return httpx.MockTransport(handler)


KORN = {
    "text": "David Korn",
    "iri": "https://x.test/e/korn",
    "label": "David Korn",
    "class": "https://x.test/class/Person",
    "score": 1.0,
    "by": "exact",
}

COMPARED: dict[str, Any] = {
    "before": {"as_of_commit_seq": 1, "resolved_by": "commit_time", "rows": 1200, "total": 1200, "complete": True, "pages": 3, "ms": 40},
    "after": {"as_of_commit_seq": 2, "resolved_by": "latest", "rows": 1230, "total": 1230, "complete": True, "pages": 3, "ms": 41},
    "vars": ["c", "stage"],
    "key": ["c"],
    "added": [],
    "removed": [],
    "changed": [],
    "totals": {"added": 50, "removed": 20, "changed": 300, "unchanged": 880},
    "offset": 0,
    "next_cursor": "7b22",
    "ms": 90,
}


class QueryToolTests(unittest.TestCase):
    def test_names_describe_and_commit_at(self) -> None:
        seen: list[httpx.Request] = []
        responses = [
            {"json": {"candidates": [KORN], "index_ready": True, "index_names": 8, "commit_seq": 4, "ms": 3}},
            {"json": {"commit_seq": 4, "partial": False, "classes": [], "properties": [], "statements": [], "prefixes": {}, "text": "", "age_ms": 1}},
            {"json": {"moment": "2026-06-19T00:00:00Z", "as_of_commit_seq": 5, "resolved_by": "commit_time"}},
        ]
        with LbbClient("http://h", graph="crm", transport=transport(seen, responses)) as client:
            found = client.query.names("Summarize David Korn's deals", limit=3)
            described = client.query.describe(
                question="Which clubs are active?", classes=["https://x.test/class/Club"]
            )
            at = client.query.commit_at(date="2026-06-18")
            with self.assertRaises(ValueError):
                client.query.commit_at()
        self.assertEqual(found["candidates"][0]["iri"], KORN["iri"])
        self.assertEqual(described["commit_seq"], 4)
        self.assertEqual(at["as_of_commit_seq"], 5)
        self.assertEqual(
            [(r.method, r.url.path) for r in seen],
            [
                ("POST", "/v1/query/names"),
                ("POST", "/v1/query/describe"),
                ("GET", "/v1/graph/commit-at"),
            ],
        )
        self.assertEqual(json.loads(seen[0].content), {"text": "Summarize David Korn's deals", "limit": 3})
        self.assertEqual(
            json.loads(seen[1].content),
            {"question": "Which clubs are active?", "classes": ["https://x.test/class/Club"]},
        )
        self.assertEqual(seen[2].url.params["date"], "2026-06-18")
        self.assertEqual(seen[2].url.params["graph"], "crm")

    def test_compare_sends_the_points_and_the_cursor(self) -> None:
        seen: list[httpx.Request] = []
        with LbbClient(
            "http://h",
            transport=transport(seen, [{"json": COMPARED}, {"json": {**COMPARED, "offset": 100, "next_cursor": None}}]),
        ) as client:
            query = "SELECT ?c ?stage WHERE { ?c <https://x.test/p/stage> ?stage }"
            first = client.query.compare(
                query,
                before={"date": "2026-06-05"},
                after=models.QueryComparePoint(as_of_commit_seq=2),
                key=["c"],
                consistency="strong",
            )
            client.query.compare(query, before={"date": "2026-06-05"}, key=["c"], cursor=first["next_cursor"])
        self.assertEqual(first["totals"]["changed"], 300)
        self.assertEqual(seen[0].url.params["consistency"], "strong")
        self.assertEqual(
            json.loads(seen[0].content),
            {"query": query, "before": {"date": "2026-06-05"}, "after": {"as_of_commit_seq": 2}, "key": ["c"]},
        )
        self.assertEqual(json.loads(seen[1].content)["cursor"], "7b22")


class AsyncQueryToolTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_names_and_compare(self) -> None:
        seen: list[httpx.Request] = []
        responses = [
            {"json": {"candidates": [KORN], "index_ready": True, "commit_seq": 4, "ms": 3}},
            {"json": COMPARED},
        ]
        async with AsyncLbbClient("http://h", transport=transport(seen, responses)) as client:
            found = await client.query.names("David Korn")
            compared = await client.query.compare("SELECT ?c WHERE { ?c ?p ?o }", before={"as_of_commit_seq": 1})
        self.assertTrue(found["index_ready"])
        self.assertEqual(compared["after"]["as_of_commit_seq"], 2)
        self.assertEqual(json.loads(seen[0].content), {"text": "David Korn"})
