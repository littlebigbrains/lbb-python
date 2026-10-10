"""Developer workflows (triggered workflows of ``workflow``): pause and
resume, commits with ``replace_owned``, and review items with ``facts``
(sync and async)."""

from __future__ import annotations

import json
import unittest
from typing import Any

import httpx

from lbb import AsyncLbbClient, LbbClient
from lbb.models import OntologyChangeSuggestion

GRAPH = {"tenant_id": "default", "graph_id": "crm", "branch_id": "main"}
TRIGGERED = {
    "starter": "workflow",
    "version": "v1",
    "name": "notes",
    "owns": {"properties": ["https://littlebigbrain.com/p/word_count"]},
    "status": {"state": "paused", "pending": 0},
}
OWNED = {"edges_retracted": 1, "fields_removed": 2, "entities_retracted": 0}
FACTS = {
    "triplets": [
        {
            "source": {"type": "Room", "name": "R 1.04"},
            "relation": "HAS_COMPONENT",
            "target": {"type": "Component", "name": "Fire door T30"},
            "confidence": 0.62,
            "evidence": {"text": "Tür T30 RS", "source_id": "doc:plan-104"},
        }
    ],
    "replace_owned": {
        "subjects": [{"type": "Room", "name": "R 1.04"}],
        "relations": ["HAS_COMPONENT"],
    },
    "summary": "One component of room R 1.04",
}
FACTS_COMMIT = {
    "commit_seq": 9,
    "request_id": "ontology-suggestion:sg_1:facts:r1",
    "owned": OWNED,
}
ACCEPTED = {
    "suggestion_id": "sg_1",
    "graph": GRAPH,
    "key": "starter:rooms/r-1-04",
    "status": "accepted",
    "title": "Link room R 1.04",
    "anchor": {"kind": "ontology"},
    "origin": {"kind": "agent", "id": "starter:rooms"},
    "change": [],
    "facts": FACTS,
    "facts_commit": FACTS_COMMIT,
    "revision": 1,
    "created_at": "2026-10-09T10:00:00Z",
    "updated_at": "2026-10-09T10:01:00Z",
}
COMMIT_BODY = {
    "entity_properties": [
        {"type": "Note", "name": "Kickoff", "properties": {"word_count": 8}}
    ],
    "replace_owned": {
        "subjects": [{"type": "Note", "name": "Kickoff"}],
        "properties": ["word_count"],
    },
}


def capturing_transport(
    captured: list[httpx.Request], responses: list[dict[str, Any]]
) -> httpx.MockTransport:
    queue = list(responses)

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(200, json=queue.pop(0) if queue else {})

    return httpx.MockTransport(handler)


class DeveloperWorkflowTests(unittest.TestCase):
    def test_pause_and_resume_post_the_flag_for_one_triggered_workflow(
        self,
    ) -> None:
        seen: list[httpx.Request] = []
        with LbbClient(
            "http://h",
            graph="crm",
            transport=capturing_transport(seen, [TRIGGERED, TRIGGERED, TRIGGERED]),
        ) as client:
            paused = client.pause_triggered_workflow("workflow", "notes")
            client.pause_triggered_workflow("workflow", "notes", paused=False)
            client.graph("sales").pause_triggered_workflow("workflow", "notes")
        self.assertEqual(paused["status"]["state"], "paused")
        self.assertEqual(
            [(request.method, request.url.path) for request in seen],
            [("POST", "/v1/workflows/triggered/pause")] * 3,
        )
        self.assertEqual(
            [dict(request.url.params) for request in seen],
            [
                {"graph": "crm", "starter": "workflow", "name": "notes"},
                {"graph": "crm", "starter": "workflow", "name": "notes"},
                {"graph": "sales", "starter": "workflow", "name": "notes"},
            ],
        )
        self.assertEqual(
            [json.loads(request.content) for request in seen],
            [{"paused": True}, {"paused": False}, {"paused": True}],
        )

    def test_a_failed_pause_is_retried(self) -> None:
        attempts: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            attempts.append(request)
            if len(attempts) == 1:
                return httpx.Response(503, json={"error": {"message": "busy"}})
            return httpx.Response(200, json=TRIGGERED)

        with LbbClient(
            "http://h",
            transport=httpx.MockTransport(handler),
            max_retries=1,
            retry_delay=0,
        ) as client:
            client.pause_triggered_workflow("workflow", "notes")
        self.assertEqual(len(attempts), 2)

    def test_a_commit_carries_replace_owned_and_returns_what_it_removed(
        self,
    ) -> None:
        seen: list[httpx.Request] = []
        answer = {"commit_seq": 42, "replayed": False, "owned": OWNED}
        with LbbClient(
            "http://h",
            graph="crm",
            transport=capturing_transport(seen, [answer, answer]),
        ) as client:
            written = client.commit(COMMIT_BODY, idempotency_key="step-1")
            client.graph("crm").facts.create(COMMIT_BODY, idempotency_key="step-2")
        self.assertEqual(written["owned"], OWNED)
        for request, key in zip(seen, ["step-1", "step-2"], strict=True):
            self.assertEqual(request.url.path, "/v1/graph/commit")
            self.assertEqual(request.headers["idempotency-key"], key)
            self.assertEqual(json.loads(request.content), COMMIT_BODY)

    def test_a_review_item_carries_facts_and_its_accept_reports_the_commit(
        self,
    ) -> None:
        seen: list[httpx.Request] = []
        with LbbClient(
            "http://h",
            graph="crm",
            transport=capturing_transport(
                seen, [{**ACCEPTED, "status": "open", "facts_commit": None}, ACCEPTED]
            ),
        ) as client:
            filed = client.ontology.suggestion_create(
                {
                    "key": "starter:rooms/r-1-04",
                    "title": "Link room R 1.04",
                    "origin": {"kind": "agent", "id": "starter:rooms"},
                    "change": [],
                    "facts": FACTS,
                }
            )
            accepted = client.ontology.suggestion_accept("sg_1")
        self.assertIsInstance(filed, OntologyChangeSuggestion)
        assert filed.facts is not None
        self.assertEqual(filed.facts.summary, "One component of room R 1.04")
        self.assertEqual(json.loads(seen[0].content)["facts"], FACTS)
        assert accepted.facts_commit is not None
        self.assertEqual(accepted.facts_commit.commit_seq, 9)
        self.assertEqual(
            accepted.facts_commit.request_id, "ontology-suggestion:sg_1:facts:r1"
        )
        assert accepted.facts_commit.owned is not None
        self.assertEqual(accepted.facts_commit.owned.fields_removed, 2)


class AsyncDeveloperWorkflowTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_pause_posts_the_flag(self) -> None:
        seen: list[httpx.Request] = []
        async with AsyncLbbClient(
            "http://h",
            graph="crm",
            transport=capturing_transport(seen, [TRIGGERED]),
        ) as client:
            resumed = await client.pause_triggered_workflow(
                "workflow", "notes", paused=False
            )
        self.assertEqual(resumed["name"], "notes")
        self.assertEqual(seen[0].url.path, "/v1/workflows/triggered/pause")
        self.assertEqual(
            dict(seen[0].url.params),
            {"graph": "crm", "starter": "workflow", "name": "notes"},
        )
        self.assertEqual(json.loads(seen[0].content), {"paused": False})

    async def test_async_commit_carries_replace_owned(self) -> None:
        seen: list[httpx.Request] = []
        async with AsyncLbbClient(
            "http://h",
            graph="crm",
            transport=capturing_transport(seen, [{"commit_seq": 3, "owned": OWNED}]),
        ) as client:
            written = await client.commit(COMMIT_BODY, idempotency_key="step-1")
        self.assertEqual(written["owned"], OWNED)
        self.assertEqual(json.loads(seen[0].content), COMMIT_BODY)


if __name__ == "__main__":
    unittest.main()
