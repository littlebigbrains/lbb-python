"""The runs of named workflows, their compatibility view by starter and
name, and the documents of the document workflows: each flat method's route
and query, on the client and on a ``graph(name)`` (sync and async), and the
``client.workflows.named`` namespace with its list envelope."""

from __future__ import annotations

import json
import unittest
from typing import Any

import httpx

from lbb import AsyncLbbClient, LbbClient
from lbb.models import (
    NamedWorkflow,
    NamedWorkflowList,
    NamedWorkflowQueue,
    TriggeredDocumentPage,
    WorkflowRevision,
    WorkflowRevisionList,
    WorkflowRunDetail,
    WorkflowRunList,
    WorkflowRunStats,
)

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
    {
        "object": "list",
        "data": [RUN],
        "has_more": True,
        "next_cursor": RUN["id"],
        "repaired": 1,
    },
    {"object": "list", "data": [], "has_more": False},
    {"run": RUN, "calls": [], "calls_truncated": False},
    {"hours": [{"hour_start_ms": 1791964800000, "runs": 1, "calls": 2}]},
    {"object": "list", "data": [], "has_more": False},
    {"run": RUN, "calls": [], "calls_truncated": False},
    {"hours": []},
    {
        "object": "list",
        "data": [{"sha256": "ab12", "name": "D17 schedule"}],
        "has_more": False,
        "total": 1,
    },
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
            self.assertEqual(page.data[0].outcome.value, "more")
            self.assertEqual(page.data[0].turn, 412)
            self.assertEqual(page.data[0].usage.calls, 2)
            self.assertEqual(page.next_cursor, RUN["id"])
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


WORKFLOW = {
    "object": "workflow",
    "name": "notes",
    "created": "2026-10-21T09:00:00Z",
    "starter": "workflow",
    "workflow": {"workflow_type": "word-count", "version": "v1"},
    "runs_in": "worker",
    "shards": 2,
    "window": 4,
    "owns": {},
    "triggers": [{"id": "watch", "kind": "watch", "watch": {"class": "urn:Note", "fields": []}}],
    "paused": False,
    "status": {"state": "ready", "pending": 0},
}
ITEM = {
    "iri": "https://littlebigbrain.com/e/aa",
    "reason": "changed",
    "shard": 1,
    "origin": {"kind": "commit", "trigger": "watch", "from_seq": 4, "to_seq": 5},
}
QUEUE_FIELDS = {"name": "notes", "trigger": "watch", "pending": 30}
NAMED_ANSWERS: list[dict[str, Any]] = [
    {"object": "list", "data": [WORKFLOW], "has_more": True, "next_cursor": "notes"},
    {"object": "list", "data": [{**WORKFLOW, "name": "sinks"}], "has_more": False},
    WORKFLOW,
    WORKFLOW,
    {"workflow": WORKFLOW, "queued": True},
    {**WORKFLOW, "paused": True},
    {"object": "list", "data": [ITEM], "has_more": True, "next_cursor": "25", **QUEUE_FIELDS},
    {"object": "list", "data": [RUN], "has_more": False},
    {"run": RUN, "calls": [], "calls_truncated": False},
    {"hours": []},
    {"object": "list", "data": [{"sha256": "ab12"}], "has_more": False, "total": 1},
    {"deleted": True},
]
NAMED_ROUTES = [
    ("GET", "/v1/workflows/named", {"graph": "crm", "limit": "1"}),
    ("GET", "/v1/workflows/named", {"graph": "crm", "limit": "1", "cursor": "notes"}),
    ("GET", "/v1/workflows/named", {"graph": "crm", "name": "notes"}),
    ("PUT", "/v1/workflows/named", {"graph": "crm"}),
    ("POST", "/v1/workflows/named/run", {"graph": "crm", "name": "notes"}),
    ("POST", "/v1/workflows/named/pause", {"graph": "crm", "name": "notes"}),
    (
        "GET",
        "/v1/workflows/named/queue",
        {"graph": "crm", "name": "notes", "trigger": "watch", "cursor": "0", "limit": "25"},
    ),
    (
        "GET",
        "/v1/workflows/named/runs",
        {"graph": "crm", "name": "notes", "limit": "20", "outcome": "failed"},
    ),
    ("GET", "/v1/workflows/named/runs/get", {"graph": "crm", "name": "notes", "run": RUN["id"]}),
    ("GET", "/v1/workflows/named/runs/stats", {"graph": "crm", "name": "notes", "hours": "48"}),
    (
        "GET",
        "/v1/workflows/named/documents",
        {"graph": "crm", "name": "plans", "q": "plan", "sort": "recent", "limit": "10"},
    ),
    ("DELETE", "/v1/workflows/named", {"graph": "crm", "name": "notes", "confirm": "notes"}),
]
REQUEST = {
    "name": "notes",
    "workflow": {"workflow_type": "word-count", "version": "v1"},
    "triggers": [{"watch": {"class": "Note"}}],
}
# A server before the list envelope: the earlier names, and a cursor it
# ignores (`cursor` is sent again as `before` and `after`).
EARLIER_ANSWERS: list[dict[str, Any]] = [
    {"workflows": [WORKFLOW]},
    {"runs": [RUN], "next_before": RUN["id"], "repaired": 1},
    {"runs": [RUN], "next_before": RUN["id"]},
    {"runs": []},
    {"items": [ITEM], "next_after": 25, **QUEUE_FIELDS},
    {"items": [ITEM], "next_after": 25, **QUEUE_FIELDS},
    {"items": [], "next_after": None, **QUEUE_FIELDS},
    {"documents": [{"sha256": "ab12"}], "next_after": "ab12", "total": 2},
]
EARLIER_ROUTES = [
    ("/v1/workflows/named", {"graph": "crm"}),
    ("/v1/workflows/named/runs", {"graph": "crm", "name": "notes"}),
    ("/v1/workflows/named/runs", {"graph": "crm", "name": "notes", "cursor": RUN["id"]}),
    ("/v1/workflows/named/runs", {"graph": "crm", "name": "notes", "before": RUN["id"]}),
    ("/v1/workflows/named/queue", {"graph": "crm", "name": "notes"}),
    ("/v1/workflows/named/queue", {"graph": "crm", "name": "notes", "cursor": "25"}),
    ("/v1/workflows/named/queue", {"graph": "crm", "name": "notes", "after": "25"}),
    ("/v1/workflows/named/documents", {"graph": "crm", "name": "plans"}),
]


class RunResultsTests(unittest.TestCase):
    def test_run_results_read_their_route_with_filters_and_every_page(self) -> None:
        def result(rid: str, outcome: str) -> dict[str, Any]:
            return {
                "id": rid,
                "object": "workflow_run_result",
                "created": "2026-10-21T10:00:00.000Z",
                "run": "1791967648726-starter.notes-3",
                "label": rid,
                "outcome": outcome,
                "values": [],
                "cost_micro_usd": 0,
            }

        answers = [
            {
                "object": "list",
                "data": [result("wrr_a", "review")],
                "has_more": False,
                "run_counts": {
                    "counts": {"written": 0, "review": 1, "no_value": 0, "failed": 0, "duplicate": 0},
                    "inputs": 1,
                    "listed": 1,
                },
            },
            {
                "object": "list",
                "data": [result("wrr_b", "failed")],
                "has_more": True,
                "next_cursor": "1:1791967648726-starter.notes-3",
            },
            {"object": "list", "data": [result("wrr_c", "review")], "has_more": False},
        ]
        seen: list[httpx.Request] = []
        with LbbClient(
            "http://h", graph="crm", transport=capturing_transport(seen, answers)
        ) as client:
            named = client.workflows.named
            one = named.run_results(
                "notes", run="1791967648726-starter.notes-3", outcome="review", limit=20
            )
            ids = [
                each.id
                for each in named.all_run_results(
                    "notes", outcome=["review", "failed"], iri="https://littlebigbrain.com/e/01"
                )
            ]
        assert one.run_counts is not None
        self.assertEqual(one.run_counts.inputs, 1)
        self.assertEqual(ids, ["wrr_b", "wrr_c"])
        self.assertEqual(
            [dict(request.url.params) for request in seen],
            [
                {
                    "graph": "crm",
                    "name": "notes",
                    "run": "1791967648726-starter.notes-3",
                    "outcome": "review",
                    "limit": "20",
                },
                {
                    "graph": "crm",
                    "name": "notes",
                    "outcome": "review,failed",
                    "iri": "https://littlebigbrain.com/e/01",
                },
                {
                    "graph": "crm",
                    "name": "notes",
                    "outcome": "review,failed",
                    "iri": "https://littlebigbrain.com/e/01",
                    "cursor": "1:1791967648726-starter.notes-3",
                },
            ],
        )
        self.assertTrue(all(r.url.path == "/v1/workflows/named/runs/results" for r in seen))


class NamedWorkflowsTests(unittest.TestCase):
    def test_each_method_reads_its_route_and_returns_its_model(self) -> None:
        seen: list[httpx.Request] = []
        with LbbClient(
            "http://h", graph="crm", transport=capturing_transport(seen, NAMED_ANSWERS)
        ) as client:
            named = client.workflows.named
            names = [workflow.name for workflow in named.all(limit=1)]
            one = named.get("notes")
            put = named.put(REQUEST)
            ran = named.run("notes")
            paused = named.pause("notes")
            queue = named.queue("notes", trigger="watch", cursor="0", limit=25)
            runs = named.runs("notes", limit=20, outcome="failed")
            detail = named.get_run("notes", RUN["id"])
            stats = named.run_stats("notes", hours=48)
            documents = named.documents("plans", q="plan", sort="recent", limit=10)
            deleted = named.delete("notes", confirm="notes")
        self.assertEqual(
            [(r.method, r.url.path, dict(r.url.params)) for r in seen], NAMED_ROUTES
        )
        self.assertEqual(seen[0].headers["lbb-version"], "2026-10-21")
        self.assertEqual(json.loads(seen[3].content), REQUEST)
        self.assertEqual(json.loads(seen[5].content), {"paused": True})
        self.assertEqual(names, ["notes", "sinks"])
        self.assertIsInstance(one, NamedWorkflow)
        self.assertEqual(one.object, "workflow")
        self.assertEqual(one.created, "2026-10-21T09:00:00Z")
        self.assertEqual(put.shards, 2)
        self.assertTrue(ran.queued)
        self.assertTrue(paused.paused)
        self.assertIsInstance(queue, NamedWorkflowQueue)
        self.assertEqual((queue.has_more, queue.next_cursor, queue.pending), (True, "25", 30))
        self.assertEqual(queue.data[0].shard, 1)
        self.assertEqual(runs.data[0].turn, 412)
        self.assertEqual(detail.run.identity, "starter.rooms")
        self.assertEqual(stats.hours, [])
        self.assertEqual(documents.total, 1)
        self.assertTrue(deleted.deleted)

    def test_lists_of_a_server_before_the_envelope_read_as_the_envelope(self) -> None:
        seen: list[httpx.Request] = []
        with LbbClient(
            "http://h", graph="crm", transport=capturing_transport(seen, EARLIER_ANSWERS)
        ) as client:
            named = client.workflows.named
            listed = named.list()
            first = named.runs("notes")
            second = named.runs("notes", cursor=first.next_cursor)
            queue = named.queue("notes")
            rest = named.queue("notes", cursor=queue.next_cursor)
            documents = named.documents("plans")
        self.assertEqual([(r.url.path, dict(r.url.params)) for r in seen], EARLIER_ROUTES)
        self.assertIsInstance(listed, NamedWorkflowList)
        self.assertEqual(
            (listed.object, listed.data[0].name, listed.has_more), ("list", "notes", False)
        )
        self.assertEqual((first.has_more, first.next_cursor, first.repaired), (True, RUN["id"], 1))
        self.assertEqual((second.data, second.has_more, second.next_cursor), ([], False, None))
        self.assertEqual((queue.next_cursor, queue.pending, queue.data[0].shard), ("25", 30, 1))
        self.assertEqual((rest.data, rest.has_more, rest.next_cursor), ([], False, None))
        self.assertEqual((documents.next_cursor, documents.total), ("ab12", 2))


    def test_requeue_sends_the_failed_instances_and_is_retried(self) -> None:
        seen: list[httpx.Request] = []
        answer = {
            "attempt": 1,
            "not_before": "2026-10-21T12:01:00.000000Z",
            "requeued": ["https://x.test/e/1"],
            "exhausted": [],
        }
        with LbbClient("http://h", graph="crm", transport=capturing_transport(seen, [answer])) as client:
            report = client.workflows.named.requeue(
                "notes",
                {
                    "workflow_id": "starter.notes",
                    "turn": 3,
                    "items": [{"instance": "https://x.test/e/1", "error": "timeout"}],
                },
            )
        self.assertEqual(report.requeued, ["https://x.test/e/1"])
        self.assertEqual(seen[0].url.path, "/v1/workflows/named/requeue")
        self.assertEqual(dict(seen[0].url.params), {"graph": "crm", "name": "notes"})
        self.assertEqual(
            json.loads(seen[0].content),
            {
                "workflow_id": "starter.notes",
                "turn": 3,
                "items": [{"instance": "https://x.test/e/1", "error": "timeout"}],
            },
        )


class AsyncNamedWorkflowsTests(unittest.IsolatedAsyncioTestCase):
    async def test_the_async_namespace_awaits_each_method(self) -> None:
        seen: list[httpx.Request] = []
        async with AsyncLbbClient(
            "http://h", graph="crm", transport=capturing_transport(seen, NAMED_ANSWERS)
        ) as client:
            named = client.workflows.named
            names = [workflow.name async for workflow in named.all(limit=1)]
            one = await named.get("notes")
            put = await named.put(REQUEST)
            ran = await named.run("notes")
            paused = await named.pause("notes")
            queue = await named.queue("notes", trigger="watch", cursor="0", limit=25)
            runs = await named.runs("notes", limit=20, outcome="failed")
            detail = await named.get_run("notes", RUN["id"])
            stats = await named.run_stats("notes", hours=48)
            documents = await named.documents("plans", q="plan", sort="recent", limit=10)
            deleted = await named.delete("notes", confirm="notes")
        self.assertEqual(
            [(r.method, r.url.path, dict(r.url.params)) for r in seen], NAMED_ROUTES
        )
        self.assertEqual(names, ["notes", "sinks"])
        self.assertEqual((one.name, put.shards, ran.queued, paused.paused), ("notes", 2, True, True))
        self.assertEqual(queue.next_cursor, "25")
        self.assertEqual(runs.data[0].turn, 412)
        self.assertEqual(detail.run.identity, "starter.rooms")
        self.assertEqual(stats.hours, [])
        self.assertEqual(documents.total, 1)
        self.assertTrue(deleted.deleted)

    async def test_async_lists_of_a_server_before_the_envelope(self) -> None:
        seen: list[httpx.Request] = []
        async with AsyncLbbClient(
            "http://h", graph="crm", transport=capturing_transport(seen, EARLIER_ANSWERS)
        ) as client:
            named = client.workflows.named
            listed = await named.list()
            runs = [run.id async for run in named.all_runs("notes")]
            queue = await named.queue("notes")
            rest = await named.queue("notes", cursor=queue.next_cursor)
            documents = await named.documents("plans")
        self.assertEqual([(r.url.path, dict(r.url.params)) for r in seen], EARLIER_ROUTES)
        self.assertEqual(listed.data[0].name, "notes")
        self.assertEqual(runs, [RUN["id"]])
        self.assertEqual((queue.next_cursor, rest.has_more), ("25", False))
        self.assertEqual(documents.next_cursor, "ab12")

if __name__ == "__main__":
    unittest.main()


FIRST = "wrev_00000000000000000000000000000001"
SECOND = "wrev_00000000000000000000000000000002"


def revision(id: str, activation: int, bound: bool = False) -> dict[str, Any]:
    return {
        "id": id,
        "object": "workflow_revision",
        "created": "2026-10-21T09:00:00Z",
        "workflow": "notes",
        "starter": "workflow",
        "available": True,
        "target": {"workflow_type": "word-count", "version": "v1"},
        "settings": {"batch": activation},
        "request": {"route": "named", "body": REQUEST},
        "activation": activation,
        "action": "change",
        "activated_at": "2026-10-21T09:00:00Z",
        "activated_by": "key:k1",
        "bound": bound,
    }


REVISION_ANSWERS: list[dict[str, Any]] = [
    {"object": "list", "data": [revision(SECOND, 2, True)], "has_more": True, "next_cursor": "2"},
    {"object": "list", "data": [revision(FIRST, 1)], "has_more": False},
    revision(FIRST, 1),
    {**WORKFLOW, "active_revision": FIRST},
]
REVISION_ROUTES = [
    ("GET", "/v1/workflows/named/revisions", {"graph": "crm", "name": "notes", "limit": "1"}),
    (
        "GET",
        "/v1/workflows/named/revisions",
        {"graph": "crm", "name": "notes", "cursor": "2", "limit": "1"},
    ),
    ("GET", "/v1/workflows/named/revisions/get", {"graph": "crm", "name": "notes", "id": FIRST}),
    ("POST", "/v1/workflows/named/revisions/activate", {"graph": "crm"}),
]


class WorkflowRevisionsTests(unittest.TestCase):
    def test_the_history_reads_page_by_page_and_an_activation_names_the_revision(self) -> None:
        seen: list[httpx.Request] = []
        with LbbClient(
            "http://h", graph="crm", transport=capturing_transport(seen, REVISION_ANSWERS)
        ) as client:
            named = client.workflows.named
            ids = [item.id for item in named.all_revisions("notes", limit=1)]
            one = named.get_revision("notes", FIRST)
            workflow = named.activate_revision("notes", FIRST, expected_revision=SECOND)
        self.assertEqual(
            [(r.method, r.url.path, dict(r.url.params)) for r in seen], REVISION_ROUTES
        )
        self.assertEqual(ids, [SECOND, FIRST])
        self.assertIsInstance(one, WorkflowRevision)
        self.assertEqual((one.activation, one.bound, one.request.route), (1, False, "named"))
        self.assertEqual(
            json.loads(seen[3].content),
            {"name": "notes", "id": FIRST, "expected_revision": SECOND},
        )
        self.assertEqual(workflow.active_revision, FIRST)


class AsyncWorkflowRevisionsTests(unittest.IsolatedAsyncioTestCase):
    async def test_the_async_namespace_awaits_each_revision_method(self) -> None:
        seen: list[httpx.Request] = []
        # The two pages, one page again, the revision, the workflow.
        answers = REVISION_ANSWERS[:2] + REVISION_ANSWERS[1:]
        async with AsyncLbbClient(
            "http://h", graph="crm", transport=capturing_transport(seen, answers)
        ) as client:
            named = client.workflows.named
            ids = [item.id async for item in named.all_revisions("notes", limit=1)]
            page = await named.revisions("notes", limit=1)
            one = await named.get_revision("notes", FIRST)
            workflow = await named.activate_revision("notes", FIRST)
        self.assertEqual(ids, [SECOND, FIRST])
        self.assertIsInstance(page, WorkflowRevisionList)
        self.assertEqual(one.id, FIRST)
        self.assertEqual(json.loads(seen[-1].content), {"name": "notes", "id": FIRST})
        self.assertEqual(workflow.active_revision, FIRST)
