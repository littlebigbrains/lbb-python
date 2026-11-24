"""Subject erasure (``/v1/erasures*``), sync and async, against a recorder of
the requests: the scope goes in the body, a preview is a dry run without a
key, and an erase always carries an ``Idempotency-Key``."""

from __future__ import annotations

import asyncio
import json
import unittest
from typing import Any

import httpx

from lbb import AsyncLbbClient, LbbClient

PERSON = "https://x.test/person/jana"
SPOKEN_BY = {"predicate": "https://x.test/spokenBy", "direction": "in"}


class Recorder:
    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []

    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self._handle)

    def _handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if request.method == "POST":
            body = json.loads(request.content)
            if body["dry_run"]:
                return httpx.Response(
                    200, json={"object": "erasure_preview", "counts": {"statements": 3}}
                )
            return httpx.Response(
                200, json={"id": "ers_1", "object": "erasure", "status": "masked"}
            )
        if request.url.path == "/v1/erasures/get":
            return httpx.Response(200, json={"id": request.url.params["id"], "object": "erasure"})
        return httpx.Response(200, json={"object": "list", "data": [], "has_more": False})

    def body(self, index: int) -> Any:
        return json.loads(self.requests[index].content)


class ErasuresSync(unittest.TestCase):
    def setUp(self) -> None:
        self.recorder = Recorder()
        self.client = LbbClient(
            "http://h", graph="interviews", retry_delay=0, transport=self.recorder.transport()
        )

    def test_preview_is_a_dry_run_without_a_key(self) -> None:
        preview = self.client.erasures.preview(subjects=[PERSON], follow=[SPOKEN_BY])
        self.assertEqual(preview["object"], "erasure_preview")
        request = self.recorder.requests[0]
        self.assertEqual((request.method, request.url.path), ("POST", "/v1/erasures"))
        self.assertEqual(request.url.params["graph"], "interviews")
        self.assertNotIn("idempotency-key", request.headers)
        self.assertEqual(
            self.recorder.body(0),
            {
                "subjects": [PERSON],
                "files": [],
                "follow": [SPOKEN_BY],
                "accept_shape_violations": False,
                "dry_run": True,
            },
        )

    def test_erase_sends_a_key(self) -> None:
        erasure = self.client.erasures.erase(subjects=[PERSON], follow=[SPOKEN_BY])
        self.assertEqual(erasure["id"], "ers_1")
        self.assertFalse(self.recorder.body(0)["dry_run"])
        self.assertTrue(self.recorder.requests[0].headers["idempotency-key"].startswith("erase"))
        self.client.erasures.erase(subjects=[PERSON], idempotency_key="withdraw-jana")
        self.assertEqual(self.recorder.requests[1].headers["idempotency-key"], "withdraw-jana")

    def test_get_and_list(self) -> None:
        self.assertEqual(self.client.erasures.get("ers_1")["id"], "ers_1")
        self.assertEqual(self.recorder.requests[0].url.path, "/v1/erasures/get")
        self.assertEqual(self.client.erasures.list()["object"], "list")
        self.assertEqual(self.recorder.requests[1].url.path, "/v1/erasures")


class ErasuresAsync(unittest.TestCase):
    def test_the_async_client_has_the_same_calls(self) -> None:
        recorder = Recorder()

        async def calls() -> None:
            client = AsyncLbbClient(
                "http://h", graph="interviews", retry_delay=0, transport=recorder.transport()
            )
            preview = await client.erasures.preview(subjects=[PERSON])
            self.assertEqual(preview["object"], "erasure_preview")
            erasure = await client.erasures.erase(subjects=[PERSON])
            self.assertEqual(erasure["status"], "masked")
            self.assertEqual((await client.erasures.get("ers_1"))["id"], "ers_1")
            self.assertEqual((await client.erasures.list())["data"], [])
            await client.aclose()

        asyncio.run(calls())
        self.assertIn("idempotency-key", recorder.requests[1].headers)
        self.assertNotIn("idempotency-key", recorder.requests[0].headers)


if __name__ == "__main__":
    unittest.main()
