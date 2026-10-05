"""``query.rewrite_profile`` and ``query.set_rewrite_profile`` send the
documented requests (``GET`` and ``PUT /v1/query/rewrite/profile``)."""

from __future__ import annotations

import json
import unittest
from typing import Any

import httpx

from lbb import AsyncLbbClient, LbbClient, LbbError, models

STORED: dict[str, Any] = {
    "version": 2,
    "notes": "A deal's current stage is p:deal_stage.",
    "examples": [
        {
            "question": "My open deals",
            "sparql": "SELECT ?d WHERE { ?d a <https://x.test/Deal> }",
        }
    ],
    "updated_at": "2026-10-05T10:00:00.000Z",
}


def transport(seen: list[httpx.Request], responses: list[dict[str, Any]]) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        item = responses.pop(0) if responses else {"json": {}}
        return httpx.Response(item.get("status", 200), json=item.get("json", {}))

    return httpx.MockTransport(handler)


class RewriteProfileTests(unittest.TestCase):
    def test_read_and_write_with_the_version_read(self) -> None:
        seen: list[httpx.Request] = []
        with LbbClient(
            "http://h",
            graph="crm",
            transport=transport(seen, [{"json": STORED}, {"json": {**STORED, "version": 3}}]),
        ) as client:
            profile = client.query.rewrite_profile()
            written = client.query.set_rewrite_profile(
                notes=profile["notes"],
                examples=[
                    models.QueryRewriteExample(
                        question="My open deals",
                        sparql="SELECT ?d WHERE { ?d a <https://x.test/Deal> }",
                        note="Open means no close date.",
                    )
                ],
                expected_version=profile["version"],
            )
        self.assertEqual(written["version"], 3)
        self.assertEqual(seen[0].method, "GET")
        self.assertEqual(seen[0].url.path, "/v1/query/rewrite/profile")
        self.assertEqual(seen[0].url.params["graph"], "crm")
        self.assertEqual(seen[1].method, "PUT")
        self.assertNotIn("dry_run", seen[1].url.params)
        self.assertEqual(
            json.loads(seen[1].content),
            {
                "notes": STORED["notes"],
                "examples": [
                    {
                        "question": "My open deals",
                        "sparql": "SELECT ?d WHERE { ?d a <https://x.test/Deal> }",
                        "note": "Open means no close date.",
                    }
                ],
                "expected_version": 2,
            },
        )

    def test_a_preview_and_a_conflict(self) -> None:
        seen: list[httpx.Request] = []
        conflict = {
            "status": 409,
            "json": {"error": {"code": "conflict", "message": "at version 3, not 2"}},
        }
        with LbbClient(
            "http://h",
            max_retries=1,
            retry_delay=0,
            transport=transport(
                seen, [{"json": {**STORED, "version": 3, "dry_run": True}}, conflict]
            ),
        ) as client:
            preview = client.query.set_rewrite_profile(notes="x", dry_run=True)
            with self.assertRaises(LbbError) as raised:
                client.query.set_rewrite_profile(notes="x", expected_version=2)
        self.assertTrue(preview["dry_run"])
        self.assertEqual(seen[0].url.params["dry_run"], "true")
        self.assertEqual(json.loads(seen[0].content), {"notes": "x", "examples": []})
        self.assertEqual(raised.exception.code, "conflict")
        self.assertEqual(len(seen), 2, "a conflict is not retried")


class AsyncRewriteProfileTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_read_and_write(self) -> None:
        seen: list[httpx.Request] = []
        async with AsyncLbbClient(
            "http://h",
            transport=transport(seen, [{"json": STORED}, {"json": {**STORED, "version": 3}}]),
        ) as client:
            profile = await client.query.rewrite_profile()
            written = await client.query.set_rewrite_profile(
                notes="n", examples=[{"question": "q", "sparql": "ASK {}"}], expected_version=2
            )
        self.assertEqual(profile["version"], 2)
        self.assertEqual(written["version"], 3)
        self.assertEqual(
            json.loads(seen[1].content),
            {"notes": "n", "examples": [{"question": "q", "sparql": "ASK {}"}], "expected_version": 2},
        )
