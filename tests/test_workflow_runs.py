"""The runs of named workflows, their compatibility view by starter and
name, and the documents of the document workflows: each method's route and
query, on the client and on a ``graph(name)`` (sync and async)."""

from __future__ import annotations

import unittest
from typing import Any

import httpx

from lbb import AsyncLbbClient, LbbClient
from lbb.models import TriggeredDocumentPage, WorkflowRunDetail, WorkflowRunList, WorkflowRunStats

RUN = {
    "v": 1,
    "id": "1791967648726-starter.rooms-412",
    "workflow": "rooms",
    "identity": "starter.rooms",
    "turn": 412,
    "workflow_type": "acme.rooms",
    "version": "v1",
    "started_at_ms": 1791967648726,
    "ended_at_ms": 1791967650190,
    "outcome": "more",
    "attempts": 1,
    "error": None,
    "origin": {"kind": "commit", "trigger": "watch", "from_seq": 409, "to_seq": 412},
    "queue_before": 2,
    "queue_after": 1,
    "units": 4,
    "unit": "rooms",
    "sample": ["room 1"],
    "steps": [{"key": "assemble", "at_ms": 1791967649010}],
    "phases": [],
    "counters": {"rooms": 4},
    "usage": {"calls": 2, "errors": 0, "tokens_in": 900, "tokens_out": 80, "cost_micro_usd": 1200},
}
LINK = ("documents.link", "documents-link")
ROUTES = [
    ("GET", "/v1/workflows/named/runs"),
    ("GET", "/v1/workflows/named/runs"),
    ("GET", "/v1/workflows/named/runs/get"),
    ("GET", "/v1/workflows/named/runs/stats"),
    ("GET", "/v1/workflows/triggered/runs"),
    ("GET", "/v1/workflows/triggered/runs/get"),
    ("GET", "/v1/workflows/triggered/runs/stats"),
    ("GET", "/v1/workflows/triggered/documents"),
]


def capturing_transport(
    captured: list[httpx.Request], responses: list[dict[str, Any]]
) -> httpx.MockTransport:
    queue = list(responses)

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(200, json=queue.pop(0) if queue else {})

    return httpx.MockTransport(handler)


def expected_params(graph: str) -> list[dict[str, str]]:
    named = {"graph": graph, "name": "rooms"}
    link = {"graph": graph, "starter": LINK[0], "name": LINK[1]}
    return [
        named,
        {**named, "before": RUN["id"], "limit": "20", "outcome": "retried"},
        {**named, "run": RUN["id"]},
        {**named, "hours": "48"},
        {**link, "outcome": "failed"},
        {**link, "run": RUN["id"]},
        {**link, "hours": "2"},
        {**link, "q": "schedule", "sort": "recent", "after": "ab12", "limit": "10"},
    ]


ANSWERS: list[dict[str, Any]] = [
    {"runs": [RUN], "next_before": RUN["id"], "repaired": 1},
    {"runs": []},
    {"run": RUN, "calls": [], "calls_truncated": False},
    {"hours": [{"hour_start_ms": 1791964800000, "runs": 1, "calls": 2}]},
    {"runs": []},
    {"run": RUN, "calls": [], "calls_truncated": False},
    {"hours": []},
    {"documents": [{"sha256": "ab12", "name": "D17 schedule"}], "total": 1},
]


def call_each(target: Any) -> list[Any]:
    return [
        target.named_workflow_runs("rooms"),
        target.named_workflow_runs("rooms", before=RUN["id"], limit=20, outcome="retried"),
        target.named_workflow_run("rooms", RUN["id"]),
        target.named_workflow_run_stats("rooms", hours=48),
        target.triggered_workflow_runs(*LINK, outcome="failed"),
        target.triggered_workflow_run(*LINK, RUN["id"]),
        target.triggered_workflow_run_stats(*LINK, hours=2),
        target.triggered_workflow_documents(
            *LINK, q="schedule", sort="recent", after="ab12", limit=10
        ),
    ]


class WorkflowRunsTests(unittest.TestCase):
    def test_each_method_reads_its_route_with_its_query(self) -> None:
        for graph, scoped in (("crm", False), ("sales", True)):
            seen: list[httpx.Request] = []
            with LbbClient(
                "http://h", graph="crm", transport=capturing_transport(seen, ANSWERS)
            ) as client:
                target: Any = client.graph(graph) if scoped else client
                answers = call_each(target)
            self.assertEqual(
                [(request.method, request.url.path) for request in seen], ROUTES
            )
            self.assertEqual(
                [dict(request.url.params) for request in seen], expected_params(graph)
            )
            page = WorkflowRunList.model_validate(answers[0])
            self.assertEqual(page.runs[0].outcome.value, "more")
            self.assertEqual(page.runs[0].turn, 412)
            self.assertEqual(page.runs[0].usage.calls, 2)
            self.assertEqual(page.next_before, RUN["id"])
            self.assertEqual(page.repaired, 1)
            detail = WorkflowRunDetail.model_validate(answers[2])
            self.assertEqual(detail.run.identity, "starter.rooms")
            stats = WorkflowRunStats.model_validate(answers[3])
            self.assertEqual(stats.hours[0].calls, 2)
            documents = TriggeredDocumentPage.model_validate(answers[7])
            self.assertEqual(documents.total, 1)


class AsyncWorkflowRunsTests(unittest.IsolatedAsyncioTestCase):
    async def test_the_async_client_awaits_each_method(self) -> None:
        seen: list[httpx.Request] = []
        async with AsyncLbbClient(
            "http://h", graph="crm", transport=capturing_transport(seen, ANSWERS)
        ) as client:
            answers = [await answer for answer in call_each(client)]
        self.assertEqual([(request.method, request.url.path) for request in seen], ROUTES)
        self.assertEqual([dict(request.url.params) for request in seen], expected_params("crm"))
        self.assertEqual(answers[2]["run"]["id"], RUN["id"])


if __name__ == "__main__":
    unittest.main()
