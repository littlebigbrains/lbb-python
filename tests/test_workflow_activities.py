"""Workflow activities: ``reason_activity``, ``decide_activity`` and
``embed_activity`` on the client, ``graph(name)`` and the async client."""

from __future__ import annotations

import json
import unittest
from typing import Any

import httpx

from lbb import AsyncLbbClient, LbbClient, LbbError
from lbb.models import (
    DecideActivityResponse,
    EmbedActivityResponse,
    ReasonActivityResponse,
)

SCHEMA = {
    "type": "object",
    "properties": {"code": {"type": "string"}, "description": {"type": "string"}},
    "required": ["code", "description"],
}
REASONED = {
    "value": {"code": "P3", "description": "Waschtisch 55x45"},
    "model": "claude-sonnet-5-5",
    "tokens_in": 92,
    "tokens_out": 21,
    "cost_micro_usd": 591,
    "cached": False,
}
QUESTIONS = {
    "kind": {
        "instructions": "What is the document?",
        "criteria": {"invoice": "an invoice", "letter": "a letter"},
    }
}
DECIDED = {
    "answers": {
        "kind": {
            "choice": "invoice",
            "confidence": 0.97,
            "probabilities": {"invoice": 0.97, "letter": 0.03},
        }
    },
    "model": "jev-latest",
    "tokens_in": 214,
    "tokens_out": 12,
    "cost_micro_usd": 0,
    "cached": True,
}
EMBEDDED = {
    "vectors": [[0.1, 0.2]],
    "model": "openai/text-embedding-3-small",
    "dim": 2,
    "tokens_in": 5,
    "cost_micro_usd": 0,
    "cached": False,
}
EFFECT = "0f3c9a51e2d84b7aa1c6d2e9b0f47a13/define-p3"


def capturing_transport(
    captured: list[httpx.Request], responses: list[tuple[int, dict[str, Any]]]
) -> httpx.MockTransport:
    queue = list(responses)

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        status, body = queue.pop(0) if queue else (200, {})
        return httpx.Response(status, json=body)

    return httpx.MockTransport(handler)


def refusal(code: str, status: int) -> tuple[int, dict[str, Any]]:
    return status, {"error": {"code": code, "message": code, "retryable": False}}


class WorkflowActivityTests(unittest.TestCase):
    def test_reason_sends_schema_and_the_effect_id_as_given(self) -> None:
        seen: list[httpx.Request] = []
        with LbbClient(
            "http://h",
            graph="tender",
            transport=capturing_transport(seen, [(200, REASONED)]),
        ) as client:
            answer = client.reason_activity(
                'Which code does this row define? "P3 Waschtisch 55x45"',
                SCHEMA,
                system="You read equipment lists.",
                max_tokens=2_000,
                effect_id=EFFECT,
                label="define P3",
            )
        self.assertEqual(answer["value"]["code"], "P3")
        self.assertEqual(
            ReasonActivityResponse.model_validate(answer).cost_micro_usd, 591
        )
        self.assertEqual(seen[0].method, "POST")
        self.assertEqual(seen[0].url.path, "/v1/workflows/activities/reason")
        self.assertEqual(dict(seen[0].url.params), {"graph": "tender"})
        self.assertEqual(
            json.loads(seen[0].content),
            {
                "prompt": 'Which code does this row define? "P3 Waschtisch 55x45"',
                "schema": SCHEMA,
                "system": "You read equipment lists.",
                "max_tokens": 2_000,
                "effect_id": EFFECT,
                "label": "define P3",
            },
        )

    def test_a_call_without_an_effect_id_sends_neither_effect_id_nor_label(
        self,
    ) -> None:
        seen: list[httpx.Request] = []
        with LbbClient(
            "http://h",
            graph="tender",
            transport=capturing_transport(seen, [(200, DECIDED), (200, EMBEDDED)]),
        ) as client:
            decided = client.decide_activity({"text": "Invoice 2026-118"}, QUESTIONS)
            embedded = client.graph("other").embed_activity(["P3 Waschtisch 55x45"])
        self.assertEqual(
            DecideActivityResponse.model_validate(decided).answers["kind"].choice,
            "invoice",
        )
        self.assertEqual(EmbedActivityResponse.model_validate(embedded).dim, 2)
        self.assertEqual(
            json.loads(seen[0].content),
            {"state": {"text": "Invoice 2026-118"}, "questions": QUESTIONS},
        )
        self.assertEqual(seen[1].url.path, "/v1/workflows/activities/embed")
        self.assertEqual(dict(seen[1].url.params), {"graph": "other"})
        self.assertEqual(json.loads(seen[1].content), {"texts": ["P3 Waschtisch 55x45"]})

    def test_decide_and_embed_pass_the_effect_id_and_label(self) -> None:
        seen: list[httpx.Request] = []
        with LbbClient(
            "http://h",
            graph="tender",
            transport=capturing_transport(seen, [(200, DECIDED), (200, EMBEDDED)]),
        ) as client:
            client.graph("tender").decide_activity(
                "x", QUESTIONS, effect_id="abc/triage", label="triage"
            )
            client.embed_activity(["a"], effect_id="abc/triage")
        self.assertEqual(json.loads(seen[0].content)["effect_id"], "abc/triage")
        self.assertEqual(json.loads(seen[0].content)["label"], "triage")
        self.assertEqual(
            json.loads(seen[1].content), {"texts": ["a"], "effect_id": "abc/triage"}
        )

    def test_embed_refuses_one_string(self) -> None:
        with LbbClient("http://h", transport=capturing_transport([], [])) as client:
            with self.assertRaises(TypeError):
                client.embed_activity("one text")

    def test_a_refused_request_raises_at_once(self) -> None:
        seen: list[httpx.Request] = []
        with LbbClient(
            "http://h",
            transport=capturing_transport(
                seen, [refusal("activity_request_refused", 422)] * 2
            ),
            max_retries=2,
            retry_delay=0,
        ) as client:
            with self.assertRaises(LbbError) as raised:
                client.reason_activity("p", {"type": "object"}, effect_id="a/b")
        self.assertEqual(raised.exception.status_code, 422)
        self.assertEqual(raised.exception.code, "activity_request_refused")
        self.assertEqual(len(seen), 1)

    def test_a_5xx_is_retried_only_with_an_effect_id(self) -> None:
        answers = [(503, {"error": {"message": "busy"}}), (200, EMBEDDED)]
        kept: list[httpx.Request] = []
        with LbbClient(
            "http://h",
            transport=capturing_transport(kept, answers),
            max_retries=1,
            retry_delay=0,
        ) as client:
            client.embed_activity(["a"], effect_id="t/embed")
        self.assertEqual(len(kept), 2)

        unkept: list[httpx.Request] = []
        with LbbClient(
            "http://h",
            transport=capturing_transport(unkept, answers),
            max_retries=1,
            retry_delay=0,
        ) as client:
            with self.assertRaises(LbbError):
                client.embed_activity(["a"])
        self.assertEqual(len(unkept), 1)


class AsyncWorkflowActivityTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_activities_are_awaitable(self) -> None:
        seen: list[httpx.Request] = []
        async with AsyncLbbClient(
            "http://h",
            graph="tender",
            transport=capturing_transport(
                seen, [(200, REASONED), (200, DECIDED), (200, EMBEDDED)]
            ),
        ) as client:
            reasoned = await client.reason_activity(
                "p", SCHEMA, effect_id=EFFECT
            )
            decided = await client.graph("tender").decide_activity(
                {"text": "x"}, QUESTIONS, label="triage"
            )
            embedded = await client.embed_activity(["a", "b"], effect_id="e/1")
        self.assertEqual(reasoned["value"]["code"], "P3")
        self.assertTrue(decided["cached"])
        self.assertEqual(embedded["dim"], 2)
        self.assertEqual(
            [request.url.path for request in seen],
            [
                "/v1/workflows/activities/reason",
                "/v1/workflows/activities/decide",
                "/v1/workflows/activities/embed",
            ],
        )
        self.assertEqual(
            json.loads(seen[0].content),
            {"prompt": "p", "schema": SCHEMA, "effect_id": EFFECT},
        )
        self.assertEqual(json.loads(seen[1].content)["label"], "triage")


if __name__ == "__main__":
    unittest.main()
