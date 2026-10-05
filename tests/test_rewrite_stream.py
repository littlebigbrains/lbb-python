"""``query.rewrite_stream``: the progress events of a streamed rewrite."""

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
    QueryRewriteStreamEvent,
)
from lbb._client_base import _SseParser

SPARQL = "SELECT ?name WHERE { ?s <http://www.w3.org/2000/01/rdf-schema#label> ?name }"


def rewrite_response() -> dict[str, Any]:
    """The ``done`` payload: the same JSON as the response without a stream."""
    return {
        "route": {"kind": "lookup", "confidence": 0.92, "by": "router"},
        "query": {"sparql": SPARQL, "entailment": "none"},
        "rationale": "The question names services in Zürich — by name.",
        "attempts": 2,
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
    }


def frame(event: str, data: Any, line_end: str = "\n") -> str:
    return f"event: {event}{line_end}data: {json.dumps(data)}{line_end}{line_end}"


def server_stream() -> str:
    """The events of a rewrite with one correction, as the server sends them."""
    return "".join(
        [
            ": keep-alive\n\n",
            frame("grounding", {"cached": True, "age_ms": 5, "classes": 3}),
            frame("route", {"kind": "lookup", "confidence": 0.92, "by": "router"}),
            frame("answer.delta", {"text": "a later event"}),
            frame("query", {"sparql": "SELECT ?x", "entailment": "none", "attempt": 1}),
            frame("run", {"as_of_commit_seq": None}, "\r\n"),
            ": keep-alive\r\n\r\n",
            frame("repair", {"error": "unknown prefix ex", "attempt": 2}, "\r\n"),
            frame("query", {"sparql": SPARQL, "entailment": "none", "attempt": 2}),
            frame("run", {"as_of_commit_seq": 7}),
            frame("rows", {"count": 1, "ms": 85}),
            frame("done", rewrite_response()),
        ]
    )


EVENT_NAMES = ["grounding", "route", "query", "run", "repair", "query", "run", "rows", "done"]


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


class RewriteStreamTests(unittest.TestCase):
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
                client.query.rewrite_stream(
                    "Which services exist?", run=True, consistency="strong"
                )
            )
        self.assertEqual([event.event for event in events], EVENT_NAMES)
        self.assertTrue(all(isinstance(event, QueryRewriteStreamEvent) for event in events))
        self.assertEqual(events[0].data, {"cached": True, "age_ms": 5, "classes": 3})
        self.assertEqual(events[4].data, {"error": "unknown prefix ex", "attempt": 2})
        self.assertEqual(events[7].data, {"count": 1, "ms": 85})
        self.assertEqual(events[8].data, rewrite_response())
        self.assertTrue(body.closed)

        request = seen[0]
        self.assertEqual(len(seen), 1)
        self.assertEqual(request.method, "POST")
        self.assertEqual(request.url.path, "/v1/query/rewrite")
        self.assertEqual(request.url.params["graph"], "main")
        self.assertEqual(request.url.params["consistency"], "strong")
        self.assertEqual(request.headers["accept"], "text/event-stream")
        self.assertEqual(request.headers["authorization"], "Bearer k")
        self.assertEqual(
            json.loads(request.content), {"question": "Which services exist?", "run": True}
        )

        # One byte per chunk: every multi-byte character arrives in pieces.
        with LbbClient(
            "http://h",
            transport=stream_transport([], ChunkStream(split(server_stream(), (1,)))),
        ) as client:
            bytewise = list(client.query.rewrite_stream("Which services exist?"))
        self.assertEqual(bytewise, events)

    def test_the_done_event_parses_as_rewrite_does_and_events_validate(self) -> None:
        with LbbClient(
            "http://h", transport=stream_transport([], ChunkStream(split(server_stream())))
        ) as client:
            events = list(client.query.rewrite_stream("Which services exist?", run=True))
        answer = QueryAskResult.from_response(events[-1].data)
        self.assertEqual(answer.rows, [{"name": "Billing"}])
        self.assertEqual(answer.trace_id, "tr_1")
        route = events[1].model()
        self.assertIsInstance(route, model_module.QueryRewriteEventRoute)
        assert isinstance(route, model_module.QueryRewriteEventRoute)
        self.assertEqual(route.data.kind, model_module.QueryRoute.lookup)
        self.assertIsInstance(events[-1].model(), model_module.QueryRewriteEventDone)

    def test_an_error_event_raises_the_error_rewrite_raises(self) -> None:
        text = (
            frame("grounding", {"cached": False, "age_ms": 0, "classes": 3})
            + frame("route", {"kind": "lookup", "confidence": 0.9, "by": "router"})
            + frame(
                "error",
                {
                    "status": 503,
                    "code": "rewrite_model_unavailable",
                    "message": "the query rewriter model did not answer; try again",
                },
            )
        )
        body = ChunkStream(split(text))
        seen: list[str] = []
        with LbbClient("http://h", transport=stream_transport([], body)) as client:
            with self.assertRaises(LbbError) as raised:
                for event in client.query.rewrite_stream("q"):
                    seen.append(event.event)
        error = raised.exception
        self.assertEqual(seen, ["grounding", "route"])
        self.assertEqual(error.status_code, 503)
        self.assertEqual(error.code, "rewrite_model_unavailable")
        self.assertEqual(str(error), "the query rewriter model did not answer; try again")
        self.assertEqual(error.type, "api_error")
        self.assertEqual(error.request_id, "req_1")
        self.assertTrue(body.closed)

    def test_a_json_error_before_the_stream_raises_as_rewrite_does_once(self) -> None:
        failure = {
            "error": {
                "type": "rate_limit_error",
                "code": "rewrite_limit",
                "message": "rewrite_limit: the stack used its 200 rewrites of the day",
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
                list(client.query.rewrite_stream("q", options={"retry": True}))
            self.assertEqual(len(seen), 1, "a stream is never retried")
            with self.assertRaises(LbbError) as plain:
                client.query.rewrite("q")
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
                for event in client.query.rewrite_stream("q"):
                    seen.append(event.event)
        self.assertEqual(seen, ["grounding"], "an unfinished event is dropped")
        self.assertIn("ended before its done or error event", str(raised.exception))

    def test_closing_the_generator_closes_the_response(self) -> None:
        body = ChunkStream(split(server_stream()))
        with LbbClient("http://h", transport=stream_transport([], body)) as client:
            events = client.query.rewrite_stream("q")
            self.assertEqual(next(events).event, "grounding")
            self.assertFalse(body.closed)
            events.close()
            self.assertTrue(body.closed)
            self.assertLess(body.read, len(body.chunks))

    def test_a_server_without_streams_yields_the_response_as_done(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=rewrite_response())

        with LbbClient("http://h", transport=httpx.MockTransport(handler)) as client:
            events = list(client.query.rewrite_stream("q"))
        self.assertEqual(events, [QueryRewriteStreamEvent("done", rewrite_response())])

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


class AsyncRewriteStreamTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_events_arrive_in_order_and_close_the_response(self) -> None:
        seen: list[httpx.Request] = []
        body = ChunkStream(split(server_stream()))
        async with AsyncLbbClient(
            "http://h", default_consistency="eventual", transport=stream_transport(seen, body)
        ) as client:
            events = [event async for event in client.query.rewrite_stream("q", run=True)]
            self.assertEqual([event.event for event in events], EVENT_NAMES)
            self.assertEqual(events[-1].data, rewrite_response())
            self.assertTrue(body.closed)
            self.assertEqual(seen[0].headers["accept"], "text/event-stream")
            self.assertEqual(seen[0].url.params["consistency"], "eventual")
            self.assertEqual(json.loads(seen[0].content), {"question": "q", "run": True})

        early = ChunkStream(split(server_stream()))
        async with AsyncLbbClient("http://h", transport=stream_transport([], early)) as client:
            async with contextlib.aclosing(client.query.rewrite_stream("q")) as stream:
                async for event in stream:
                    if event.event == "route":
                        break
            self.assertTrue(early.closed)
            self.assertLess(early.read, len(early.chunks))

    async def test_async_error_event_and_early_end_raise(self) -> None:
        error = frame("error", {"status": 400, "code": "invalid_request", "message": "bad"})
        async with AsyncLbbClient(
            "http://h", transport=stream_transport([], ChunkStream(split(error)))
        ) as client:
            with self.assertRaises(LbbError) as raised:
                [event async for event in client.query.rewrite_stream("q")]
        self.assertEqual(raised.exception.status_code, 400)
        self.assertEqual(raised.exception.code, "invalid_request")
        self.assertEqual(raised.exception.type, "invalid_request_error")

        partial = frame("grounding", {"cached": True, "age_ms": 5, "classes": 3})
        async with AsyncLbbClient(
            "http://h", transport=stream_transport([], ChunkStream(split(partial)))
        ) as client:
            with self.assertRaises(httpx.RemoteProtocolError):
                [event async for event in client.query.rewrite_stream("q")]


if __name__ == "__main__":
    unittest.main()
