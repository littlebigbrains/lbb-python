"""Derivations: ``check_derivation`` and ``record_derivation`` on the client,
``graph(name)`` and the async client."""

from __future__ import annotations

import asyncio
import json
import unittest
from typing import Any

import httpx

from lbb import AsyncLbbClient, LbbClient, LbbError
from lbb.models import (
    DerivationCheckResponse,
    DerivationRecordRequest,
    DerivationRecordResponse,
)

SHA = "ab" * 32
KEY = {
    "sha256": SHA,
    "workflow_type": "acme.parse",
    "version": "v1",
    "settings_digest": "none",
}
DERIVATION = {
    **KEY,
    "run_id": "starter-pdfs-1",
    "outputs": {"pages": 12},
    "recorded_at": "2026-10-14T10:00:00Z",
}


def capturing_transport(
    captured: list[httpx.Request], responses: list[tuple[int, dict[str, Any]]]
) -> httpx.MockTransport:
    queue = list(responses)

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        status, body = queue.pop(0) if queue else (200, {})
        return httpx.Response(status, json=body)

    return httpx.MockTransport(handler)


class DerivationTests(unittest.TestCase):
    def test_check_then_record_send_the_key_on_the_graph(self) -> None:
        seen: list[httpx.Request] = []
        with LbbClient(
            "http://h",
            graph="docs",
            transport=capturing_transport(
                seen,
                [
                    (200, {"exists": False}),
                    (200, {"recorded": True, "derivation": DERIVATION}),
                ],
            ),
        ) as client:
            checked = client.check_derivation(SHA, "acme.parse", "v1")
            recorded = client.graph("other").record_derivation(
                SHA,
                "acme.parse",
                "v1",
                run_id="starter-pdfs-1",
                outputs={"pages": 12},
            )
        self.assertFalse(DerivationCheckResponse.model_validate(checked).exists)
        record = DerivationRecordResponse.model_validate(recorded)
        self.assertTrue(record.recorded)
        self.assertEqual(record.derivation.outputs, {"pages": 12})
        self.assertEqual(seen[0].method, "POST")
        self.assertEqual(seen[0].url.path, "/v1/workflows/derivations/check")
        self.assertEqual(dict(seen[0].url.params), {"graph": "docs"})
        self.assertEqual(json.loads(seen[0].content), KEY)
        self.assertEqual(seen[1].url.path, "/v1/workflows/derivations/record")
        self.assertEqual(dict(seen[1].url.params), {"graph": "other"})
        self.assertEqual(
            json.loads(seen[1].content),
            {**KEY, "run_id": "starter-pdfs-1", "outputs": {"pages": 12}},
        )

    def test_a_check_is_retried_after_a_server_error(self) -> None:
        seen: list[httpx.Request] = []
        with LbbClient(
            "http://h",
            transport=capturing_transport(
                seen,
                [(503, {"error": {"message": "busy"}}), (200, {"exists": True, "derivation": DERIVATION})],
            ),
            max_retries=1,
            retry_delay=0,
        ) as client:
            checked = client.check_derivation(SHA, "acme.parse", "v1", "s-1")
        self.assertTrue(checked["exists"])
        self.assertEqual(len(seen), 2)
        self.assertEqual(json.loads(seen[1].content)["settings_digest"], "s-1")

    def test_the_async_client_awaits_both(self) -> None:
        seen: list[httpx.Request] = []

        async def run() -> tuple[Any, Any]:
            async with AsyncLbbClient(
                "http://h",
                graph="docs",
                transport=capturing_transport(
                    seen,
                    [
                        (200, {"exists": True, "derivation": DERIVATION}),
                        (200, {"recorded": False, "derivation": DERIVATION}),
                    ],
                ),
            ) as client:
                checked = await client.check_derivation(SHA, "acme.parse", "v1")
                recorded = await client.record_derivation(SHA, "acme.parse", "v1")
                return checked, recorded

        checked, recorded = asyncio.run(run())
        self.assertTrue(checked["exists"])
        self.assertFalse(recorded["recorded"])
        self.assertEqual(json.loads(seen[1].content), KEY)

    def test_a_record_names_the_upload_of_its_check(self) -> None:
        upload = "2026-10-15T09:00:00.123456+00:00"
        seen: list[httpx.Request] = []
        with LbbClient(
            "http://h",
            transport=capturing_transport(
                seen,
                [
                    (200, {"exists": False, "content_uploaded_at": upload}),
                    (
                        409,
                        {
                            "error": {
                                "code": "derivation_upload_changed",
                                "message": "the file was deleted",
                                "param": "content_uploaded_at",
                                "retryable": False,
                            }
                        },
                    ),
                ],
            ),
            max_retries=2,
            retry_delay=0,
        ) as client:
            checked = DerivationCheckResponse.model_validate(
                client.check_derivation(SHA, "acme.parse", "v1")
            )
            self.assertEqual(checked.content_uploaded_at, upload)
            with self.assertRaises(LbbError) as raised:
                client.record_derivation(
                    SHA,
                    "acme.parse",
                    "v1",
                    outputs={"pages": 1},
                    content_uploaded_at=checked.content_uploaded_at,
                )
        self.assertEqual(raised.exception.status_code, 409)
        self.assertEqual(raised.exception.code, "derivation_upload_changed")
        self.assertEqual(len(seen), 2, "a 409 is not retried")
        sent = json.loads(seen[1].content)
        self.assertEqual(
            sent, {**KEY, "outputs": {"pages": 1}, "content_uploaded_at": upload}
        )
        self.assertEqual(
            DerivationRecordRequest.model_validate(sent).content_uploaded_at, upload
        )


if __name__ == "__main__":
    unittest.main()
