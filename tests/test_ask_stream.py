"""``query.ask_stream``: the progress events of a streamed question."""

from __future__ import annotations

import contextlib
import json
import unittest
from collections.abc import AsyncIterator, Iterator
from typing import Any

import httpx

import lbb.models as model_module
from lbb import (
    AsyncLbbClient,
    LbbClient,
    LbbError,
    QueryAskResult,
    QueryAskStreamEvent,
)
from lbb._client_base import _SseParser

SPARQL = "SELECT ?name WHERE { ?s <http://www.w3.org/2000/01/rdf-schema#label> ?name }"
SERVICE = "https://x.test/e/billing"


def ask_response() -> dict[str, Any]:
    """The ``done`` payload: the same JSON as the response without a stream."""
    return {
        "route": {"kind": "lookup", "confidence": 0.92, "by": "router"},
        "query": {"sparql": SPARQL, "entailment": "none"},
        "rationale": "The rows of step 2 name the services in Zürich.",
        "attempts": 1,
        "grounding": {
            "commit_seq": 7,
            "classes": 3,
            "properties": 5,
            "embeddings": 0,
            "age_ms": 10,
        },
        "models": [],
        "timings": {
            "ground_ms": 1,
            "route_ms": 2,
            "rewrite_ms": 3,
            "run_ms": 4,
            "total_ms": 10,
        },
        "result": {
            "results": json.dumps(
                {
                    "head": {"vars": ["name"]},
                    "results": {"bindings": [{"name": {"type": "literal", "value": "Billing"}}]},
                }
            ),
            "row_page": {
                "returned": 1,
                "total": 1,
                "offset": 0,
                "limit": 100,
                "has_more": False,
            },
            "trace_id": "tr_1",
        },
        "answer": {"text": "One service: Billing.", "citations": [SERVICE]},
        "steps": [
            {"n": 1, "tool": "find_entities", "input": {"text": "Zürich"}, "ok": True, "ms": 12},
            {"n": 2, "tool": "sparql", "input": {"query": SPARQL}, "ok": True, "rows": 1, "ms": 85},
        ],
    }


def route_response() -> dict[str, Any]:
    """The ``done`` payload of ``mode="route"``: no query, no rows, no answer."""
    response = ask_response()
    for name in ("query", "result", "answer", "steps"):
        del response[name]
    response["attempts"] = 0
    response["rationale"] = "The router chose the route."
    return response


def frame(event: str, data: Any, line_end: str = "\n") -> str:
    return f"event: {event}{line_end}data: {json.dumps(data)}{line_end}{line_end}"


def server_stream() -> str:
    """The events of a question the loop answered in two tool calls, as the
    server sends them."""
    return "".join(
        [
            ": keep-alive\n\n",
            frame("grounding", {"cached": True, "age_ms": 5, "classes": 3}),
            frame("route", {"kind": "lookup", "confidence": 0.92, "by": "router"}),
            frame("answer.delta", {"text": "a later event"}),
            frame(
                "step",
                {"n": 1, "tool": "find_entities", "input": "Zürich", "ok": True},
                "\r\n",
            ),
            ": keep-alive\r\n\r\n",
            frame("query", {"sparql": "SELECT ?x", "entailment": "none", "attempt": 1}),
            frame("step", {"n": 2, "tool": "sparql", "input": SPARQL, "ok": True, "rows": 1}),
            frame("route", {"kind": "lookup", "confidence": 0.8, "by": "rewriter"}),
            frame("answer", {"text": "One service: Billing.", "citations": [SERVICE]}),
            frame("done", ask_response()),
        ]
    )


EVENT_NAMES = ["grounding", "route", "step", "step", "route", "answer", "done"]


def split(text: str, sizes: tuple[int, ...] = (1, 7, 3, 13, 2, 29)) -> list[bytes]:
    """Bytes cut at uneven offsets, so events, lines and characters break."""
    data = text.encode()
    chunks: list[bytes] = []
    offset = 0
    index = 0
    while offset < len(data):
        size = sizes[index % len(sizes)]
        chunks.append(data[offset : offset + size])
        offset += size
        index += 1
    return chunks


class ChunkStream(httpx.SyncByteStream, httpx.AsyncByteStream):
    """A response body that arrives in the given chunks and records its close."""

    def __init__(self, chunks: list[bytes]) -> None:
        self.chunks = chunks
        self.read = 0
        self.closed = False

    def __iter__(self) -> Iterator[bytes]:
        for chunk in self.chunks:
            self.read += 1
            yield chunk

    async def __aiter__(self) -> AsyncIterator[bytes]:
        for chunk in self.chunks:
            self.read += 1
            yield chunk

    def close(self) -> None:
        self.closed = True

    async def aclose(self) -> None:
        self.closed = True


def stream_transport(
    seen: list[httpx.Request], body: ChunkStream, *, status: int = 200
) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(
            status,
            headers={"content-type": "text/event-stream", "x-request-id": "req_1"},
            stream=body,
        )

    return httpx.MockTransport(handler)


class AskStreamTests(unittest.TestCase):
    def test_events_arrive_in_order_from_a_body_split_at_any_byte(self) -> None:
        seen: list[httpx.Request] = []
        body = ChunkStream(split(server_stream()))
        with LbbClient(
            "http://h",
            api_key="k",
            graph="main",
            transport=stream_transport(seen, body),
        ) as client:
            events = list(
                client.query.ask_stream(
                    "Which services exist?", limit=20, consistency="strong"
                )
            )
        self.assertEqual([event.event for event in events], EVENT_NAMES)
        self.assertTrue(all(isinstance(event, QueryAskStreamEvent) for event in events))
        self.assertEqual(events[0].data, {"cached": True, "age_ms": 5, "classes": 3})
        self.assertEqual(
            events[2].data, {"n": 1, "tool": "find_entities", "input": "Zürich", "ok": True}
        )
        self.assertEqual(events[3].data["rows"], 1)
        self.assertEqual(events[4].data["by"], "rewriter")
        self.assertEqual(
            events[5].data, {"text": "One service: Billing.", "citations": [SERVICE]}
        )
        self.assertEqual(events[6].data, ask_response())
        self.assertTrue(body.closed)

        request = seen[0]
        self.assertEqual(len(seen), 1)
        self.assertEqual(request.method, "POST")
        self.assertEqual(request.url.path, "/v1/query/ask")
        self.assertEqual(request.url.params["graph"], "main")
        self.assertEqual(request.url.params["consistency"], "strong")
        self.assertEqual(request.headers["accept"], "text/event-stream")
        self.assertEqual(request.headers["authorization"], "Bearer k")
        self.assertEqual(
            json.loads(request.content), {"question": "Which services exist?", "limit": 20}
        )

        # One byte per chunk: every multi-byte character arrives in pieces.
        with LbbClient(
            "http://h",
            transport=stream_transport([], ChunkStream(split(server_stream(), (1,)))),
        ) as client:
            bytewise = list(client.query.ask_stream("Which services exist?"))
        self.assertEqual(bytewise, events)

    def test_the_done_event_parses_as_ask_does_and_events_validate(self) -> None:
        with LbbClient(
            "http://h", transport=stream_transport([], ChunkStream(split(server_stream())))
        ) as client:
            events = list(client.query.ask_stream("Which services exist?"))
        answer = QueryAskResult.from_response(events[-1].data)
        self.assertEqual(answer.answer, "One service: Billing.")
        self.assertEqual(answer.citations, [SERVICE])
        self.assertEqual(answer.rows, [{"name": "Billing"}])
        self.assertEqual(answer.trace_id, "tr_1")
        self.assertEqual([step["tool"] for step in answer.steps], ["find_entities", "sparql"])
        route = events[1].model()
        self.assertIsInstance(route, model_module.QueryRewriteEventRoute)
        assert isinstance(route, model_module.QueryRewriteEventRoute)
        self.assertEqual(route.data.kind, model_module.QueryRoute.lookup)
        step = events[3].model()
        assert isinstance(step, model_module.QueryRewriteEventStep)
        self.assertEqual(step.data.tool, model_module.QueryAnswerTool.sparql)
        said = events[5].model()
        assert isinstance(said, model_module.QueryRewriteEventAnswer)
        self.assertEqual(said.data.text, "One service: Billing.")
        self.assertIsInstance(events[-1].model(), model_module.QueryRewriteEventDone)

    def test_route_mode_streams_grounding_route_and_done(self) -> None:
        text = (
            frame("grounding", {"cached": False, "age_ms": 0, "classes": 3})
            + frame("route", {"kind": "lookup", "confidence": 0.92, "by": "router"})
            + frame("done", route_response())
        )
        seen: list[httpx.Request] = []
        with LbbClient(
            "http://h", transport=stream_transport(seen, ChunkStream(split(text)))
        ) as client:
            events = list(
                client.query.ask_stream(
                    "Which services exist?", mode=model_module.QueryRewriteMode.route
                )
            )
        self.assertEqual([event.event for event in events], ["grounding", "route", "done"])
        self.assertEqual(
            json.loads(seen[0].content), {"question": "Which services exist?", "mode": "route"}
        )
        result = QueryAskResult.from_response(events[-1].data)
        self.assertEqual(result.route["kind"], "lookup")
        self.assertIsNone(result.answer)
        self.assertIsNone(result.query)
        self.assertEqual(result.rows, [])

    def test_an_error_event_raises_the_error_ask_raises(self) -> None:
        text = (
            frame("grounding", {"cached": False, "age_ms": 0, "classes": 3})
            + frame("route", {"kind": "lookup", "confidence": 0.9, "by": "router"})
            + frame("step", {"n": 1, "tool": "sparql", "input": SPARQL, "ok": False})
            + frame(
                "error",
                {
                    "status": 503,
                    "code": "rewrite_model_unavailable",
                    "message": "the query rewriter model did not answer; try again",
                },
            )
            + frame("done", ask_response())
        )
        body = ChunkStream(split(text))
        seen: list[str] = []
        with LbbClient("http://h", transport=stream_transport([], body)) as client:
            with self.assertRaises(LbbError) as raised:
                for event in client.query.ask_stream("q"):
                    seen.append(event.event)
        error = raised.exception
        self.assertEqual(seen, ["grounding", "route", "step"])
        self.assertEqual(error.status_code, 503)
        self.assertEqual(error.code, "rewrite_model_unavailable")
        self.assertEqual(str(error), "the query rewriter model did not answer; try again")
        self.assertEqual(error.type, "api_error")
        self.assertEqual(error.request_id, "req_1")
        self.assertTrue(body.closed)

    def test_a_json_error_before_the_stream_raises_as_ask_does_once(self) -> None:
        failure = {
            "error": {
                "type": "rate_limit_error",
                "code": "rewrite_limit",
                "message": "rewrite_limit: this stack asked its 200 questions of today",
                "retryable": False,
            }
        }
        seen: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            return httpx.Response(429, json=failure, headers={"retry-after": "60"})

        with LbbClient(
            "http://h", max_retries=3, retry_delay=0, transport=httpx.MockTransport(handler)
        ) as client:
            with self.assertRaises(LbbError) as streamed:
                list(client.query.ask_stream("q", options={"retry": True}))
            self.assertEqual(len(seen), 1, "a stream is never retried")
            with self.assertRaises(LbbError) as plain:
                client.query.ask("q")
        for name in ("status_code", "code", "type", "retryable", "retry_after_seconds", "body"):
            self.assertEqual(getattr(streamed.exception, name), getattr(plain.exception, name), name)
        self.assertEqual(str(streamed.exception), str(plain.exception))
        self.assertEqual(streamed.exception.status_code, 429)
        self.assertEqual(streamed.exception.code, "rewrite_limit")

    def test_a_body_that_ends_before_done_raises(self) -> None:
        text = frame("grounding", {"cached": True, "age_ms": 5, "classes": 3}) + (
            'event: done\ndata: {"attempts": 1}'
        )
        seen: list[str] = []
        with LbbClient(
            "http://h", transport=stream_transport([], ChunkStream(split(text)))
        ) as client:
            with self.assertRaises(httpx.RemoteProtocolError) as raised:
                for event in client.query.ask_stream("q"):
                    seen.append(event.event)
        self.assertEqual(seen, ["grounding"], "an unfinished event is dropped")
        self.assertIn("ended before its done or error event", str(raised.exception))

    def test_closing_the_generator_closes_the_response(self) -> None:
        body = ChunkStream(split(server_stream()))
        with LbbClient("http://h", transport=stream_transport([], body)) as client:
            events = client.query.ask_stream("q")
            self.assertEqual(next(events).event, "grounding")
            self.assertFalse(body.closed)
            events.close()
            self.assertTrue(body.closed)
            self.assertLess(body.read, len(body.chunks))

    def test_a_server_without_streams_yields_the_response_as_done(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=ask_response())

        with LbbClient("http://h", transport=httpx.MockTransport(handler)) as client:
            events = list(client.query.ask_stream("q"))
        self.assertEqual(events, [QueryAskStreamEvent("done", ask_response())])

    def test_the_parser_joins_data_lines_splits_every_line_end_and_bounds_an_event(
        self,
    ) -> None:
        parser = _SseParser(64)
        events = [
            *parser.feed("data: one\r"),
            *parser.feed("\ndata:two\rdata\r\r"),
            *parser.feed("event: x\nid: 1\nretry: 5\nunknown: y\ndata: \n\n"),
            *parser.feed("event: empty\n\n"),
        ]
        self.assertEqual(events, [("message", "one\ntwo\n"), ("x", "")])
        with self.assertRaisesRegex(ValueError, "larger than 64 characters"):
            parser.feed("data: " + "x" * 80)
        lines = _SseParser(64)
        with self.assertRaisesRegex(ValueError, "larger than 64 characters"):
            for _ in range(20):
                lines.feed("data: xxxx\n")


class AsyncAskStreamTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_events_arrive_in_order_and_close_the_response(self) -> None:
        seen: list[httpx.Request] = []
        body = ChunkStream(split(server_stream()))
        async with AsyncLbbClient(
            "http://h", default_consistency="eventual", transport=stream_transport(seen, body)
        ) as client:
            events = [
                event
                async for event in client.query.ask_stream("q", anchor=[SERVICE], mode="answer")
            ]
            self.assertEqual([event.event for event in events], EVENT_NAMES)
            self.assertEqual(events[-1].data, ask_response())
            self.assertTrue(body.closed)
            self.assertEqual(seen[0].url.path, "/v1/query/ask")
            self.assertEqual(seen[0].headers["accept"], "text/event-stream")
            self.assertEqual(seen[0].url.params["consistency"], "eventual")
            self.assertEqual(
                json.loads(seen[0].content),
                {"question": "q", "mode": "answer", "anchor": [SERVICE]},
            )

        early = ChunkStream(split(server_stream()))
        async with AsyncLbbClient("http://h", transport=stream_transport([], early)) as client:
            async with contextlib.aclosing(client.query.ask_stream("q")) as stream:
                async for event in stream:
                    if event.event == "route":
                        break
            self.assertTrue(early.closed)
            self.assertLess(early.read, len(early.chunks))

    async def test_async_error_event_and_early_end_raise(self) -> None:
        error = frame("error", {"status": 400, "code": "invalid_ask_request", "message": "bad"})
        async with AsyncLbbClient(
            "http://h", transport=stream_transport([], ChunkStream(split(error)))
        ) as client:
            with self.assertRaises(LbbError) as raised:
                [event async for event in client.query.ask_stream("q")]
        self.assertEqual(raised.exception.status_code, 400)
        self.assertEqual(raised.exception.code, "invalid_ask_request")
        self.assertEqual(raised.exception.type, "invalid_request_error")

        partial = frame("grounding", {"cached": True, "age_ms": 5, "classes": 3})
        async with AsyncLbbClient(
            "http://h", transport=stream_transport([], ChunkStream(split(partial)))
        ) as client:
            with self.assertRaises(httpx.RemoteProtocolError):
                [event async for event in client.query.ask_stream("q")]


if __name__ == "__main__":
    unittest.main()
