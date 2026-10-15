"""Files of a graph (``/v1/files*``) and entity references by IRI, sync and
async, against a mock transport."""

from __future__ import annotations

import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest import mock

import httpx

from lbb import AsyncLbbClient, LbbClient, LbbError, entity_id_from_iri, entity_ref
from lbb.entities import ENTITY_ID_KEY_SENTINEL, ENTITY_IRI_NAMESPACE
from lbb.models import FileCounts, FileList, FilePages, FileUploadResponse

SHA = "ab" * 32
RECORD = {
    "sha256": SHA,
    "name": "room-book.pdf",
    "content_type": "application/pdf",
    "bytes": 4,
    "uploaded_at": "2026-10-10T08:00:00Z",
    "document": "https://littlebigbrain.com/e/0123456789abcdef0123456789abcdef",
}
PAGES = {
    "sha256": SHA,
    "parser": "poppler-26.03.0",
    "page_count": 3,
    "from": 1,
    "to": 2,
    "pages": [
        {
            "page": 1,
            "width": 595.22,
            "height": 842,
            "lines": [
                {
                    "id": "p2-l1",
                    "text": "S 1.1",
                    "box": [0.1, 0.2, 0.3, 0.4],
                    "words": [{"text": "S", "box": [0.1, 0.2, 0.15, 0.4]}],
                }
            ],
        }
    ],
}
PDF = b"%PDF\xff\x00\x80"
ENTRY_ID = "0123456789abcdef0123456789abcdef"
ENTRY_IRI = f"https://littlebigbrain.com/e/{ENTRY_ID}"


def transport(captured: list[httpx.Request], answers: list[httpx.Response]) -> httpx.MockTransport:
    queue = list(answers)

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return queue.pop(0) if queue else httpx.Response(200, json={})

    return httpx.MockTransport(handler)


def json_answer(body: Any, status: int = 200) -> httpx.Response:
    return httpx.Response(status, json=body)


class FilesSync(unittest.TestCase):
    def client(self, captured: list[httpx.Request], answers: list[httpx.Response]) -> LbbClient:
        return LbbClient(
            "http://h",
            graph="docs",
            retry_delay=0,
            transport=transport(captured, answers),
        )

    def test_upload_sends_the_bytes_under_the_name(self) -> None:
        captured: list[httpx.Request] = []
        lbb = self.client(
            captured,
            [json_answer({"file": RECORD, "created": True, "commit_seq": 12})],
        )
        answer = lbb.files.upload(PDF, name="room book.pdf", content_type="application/pdf")
        self.assertTrue(FileUploadResponse.model_validate(answer).created)
        request = captured[0]
        self.assertEqual(request.method, "POST")
        self.assertEqual(request.url.path, "/v1/files")
        self.assertEqual(dict(request.url.params), {"graph": "docs", "name": "room book.pdf"})
        self.assertEqual(request.content, PDF)
        self.assertEqual(request.headers["content-type"], "application/pdf")

    def test_upload_sends_the_language_and_schema_when_given(self) -> None:
        captured: list[httpx.Request] = []
        lbb = self.client(
            captured,
            [
                json_answer({"file": {**RECORD, "language": "de"}, "created": True}),
                json_answer({"file": RECORD, "created": False}),
            ],
        )
        lbb.files.upload(PDF, name="plan.pdf", language="de", schema="urn:x:plan")
        lbb.files.upload(PDF, name="plan.pdf")
        self.assertEqual(
            dict(captured[0].url.params),
            {"graph": "docs", "name": "plan.pdf", "language": "de", "schema": "urn:x:plan"},
        )
        self.assertEqual(dict(captured[1].url.params), {"graph": "docs", "name": "plan.pdf"})

    def test_upload_reads_a_path_and_retries_a_503(self) -> None:
        captured: list[httpx.Request] = []
        lbb = self.client(
            captured,
            [
                json_answer({"error": {"code": "server_busy", "message": "busy"}}, 503),
                json_answer({"file": RECORD, "created": False}),
                json_answer({"file": RECORD, "created": False}),
            ],
        )
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "room-book.pdf"
            path.write_bytes(PDF)
            lbb.files.upload(path)
            lbb.files.upload(str(path), name="other.bin", content_type="application/x-test")
        self.assertEqual(len(captured), 3, "the 503 was retried")
        self.assertEqual(captured[0].content, PDF)
        self.assertEqual(captured[1].content, PDF)
        self.assertEqual(captured[1].url.params["name"], "room-book.pdf")
        self.assertEqual(captured[1].headers["content-type"], "application/pdf")
        self.assertEqual(captured[2].url.params["name"], "other.bin")
        self.assertEqual(captured[2].headers["content-type"], "application/x-test")

    def test_upload_of_bytes_needs_a_name_and_defaults_to_octet_stream(self) -> None:
        captured: list[httpx.Request] = []
        lbb = self.client(captured, [json_answer({"file": RECORD, "created": True})])
        with self.assertRaises(ValueError):
            lbb.files.upload(b"x")
        lbb.files.upload(bytearray(b"x"), name="x.bin")
        self.assertEqual(len(captured), 1)
        self.assertEqual(captured[0].headers["content-type"], "application/octet-stream")

    def test_list_content_pages_and_delete(self) -> None:
        captured: list[httpx.Request] = []
        lbb = self.client(
            captured,
            [
                json_answer({"files": [RECORD]}),
                httpx.Response(200, content=PDF, headers={"content-type": "application/pdf"}),
                json_answer(PAGES),
                json_answer({"deleted": True, "sha256": SHA}),
            ],
        )
        self.assertEqual(lbb.files.list()["files"][0]["name"], "room-book.pdf")
        self.assertEqual(lbb.files.content(SHA.upper()), PDF)
        pages = FilePages.model_validate(lbb.files.pages(SHA, from_=1, to=2))
        self.assertEqual(pages.pages[0].lines[0].id, "p2-l1")
        self.assertEqual(pages.pages[0].lines[0].box, [0.1, 0.2, 0.3, 0.4])
        self.assertTrue(lbb.files.delete(f" {SHA} ")["deleted"])
        self.assertEqual(
            [(request.method, request.url.path, dict(request.url.params)) for request in captured],
            [
                ("GET", "/v1/files", {"graph": "docs"}),
                ("GET", "/v1/files/content", {"graph": "docs", "sha256": SHA}),
                (
                    "GET",
                    "/v1/files/pages",
                    {"graph": "docs", "sha256": SHA, "from": "1", "to": "2"},
                ),
                ("DELETE", "/v1/files", {"graph": "docs", "sha256": SHA, "confirm": SHA}),
            ],
        )

    def test_a_plain_text_file_stays_bytes(self) -> None:
        captured: list[httpx.Request] = []
        lbb = self.client(
            captured,
            [httpx.Response(200, content=b"{not json", headers={"content-type": "text/plain"})],
        )
        self.assertEqual(lbb.files.content(SHA), b"{not json")

    def test_bad_arguments_send_nothing(self) -> None:
        captured: list[httpx.Request] = []
        lbb = self.client(captured, [])
        for call in (
            lambda: lbb.files.content("abc"),
            lambda: lbb.files.pages(SHA, from_=3, to=1),
            lambda: lbb.files.pages(SHA, from_=-1),
            lambda: lbb.files.delete("z" * 64),
        ):
            with self.assertRaises(ValueError):
                call()
        self.assertEqual(captured, [])

    def test_a_missing_file_raises_the_server_error(self) -> None:
        captured: list[httpx.Request] = []
        lbb = self.client(
            captured,
            [
                json_answer(
                    {
                        "error": {
                            "type": "not_found_error",
                            "code": "file_not_parsed",
                            "message": "not parsed yet",
                        }
                    },
                    404,
                )
            ],
        )
        with self.assertRaises(LbbError) as raised:
            lbb.files.pages(SHA)
        self.assertEqual(raised.exception.status_code, 404)
        self.assertEqual(raised.exception.code, "file_not_parsed")

    def test_list_pages_with_limit_and_after_and_counts(self) -> None:
        captured: list[httpx.Request] = []
        lbb = self.client(
            captured,
            [
                json_answer({"files": [RECORD], "next": "6162.0123456789abcdef"}),
                json_answer({"files": []}),
                json_answer(
                    {
                        "files": 3,
                        "by_parse_status": {"parsed": 2, "waiting": 1},
                        "as_of_commit_seq": 41,
                    }
                ),
            ],
        )
        first = FileList.model_validate(lbb.files.list(limit=1))
        self.assertEqual(first.next, "6162.0123456789abcdef")
        rest = FileList.model_validate(lbb.files.list(limit=50, after=first.next))
        self.assertIsNone(rest.next)
        counts = FileCounts.model_validate(lbb.files.counts())
        self.assertEqual((counts.files, counts.as_of_commit_seq), (3, 41))
        self.assertEqual(counts.by_parse_status["parsed"], 2)
        self.assertEqual(dict(captured[0].url.params), {"graph": "docs", "limit": "1"})
        self.assertEqual(
            dict(captured[1].url.params),
            {"graph": "docs", "limit": "50", "after": "6162.0123456789abcdef"},
        )
        self.assertEqual(captured[2].url.path, "/v1/files/counts")
        for limit in (0, 1001, True):
            with self.assertRaises(ValueError):
                lbb.files.list(limit=limit)
        self.assertEqual(len(captured), 3)

    def test_get_reads_one_file_by_its_sha256(self) -> None:
        captured: list[httpx.Request] = []
        lbb = self.client(captured, [json_answer(RECORD)])
        self.assertEqual(lbb.files.get(SHA.upper())["name"], "room-book.pdf")
        self.assertEqual(captured[0].url.path, "/v1/files/record")
        self.assertEqual(dict(captured[0].url.params), {"graph": "docs", "sha256": SHA})
        with self.assertRaises(ValueError):
            lbb.files.get("xyz")
        self.assertEqual(len(captured), 1)

    def test_graph_namespace_scopes_the_files(self) -> None:
        captured: list[httpx.Request] = []
        lbb = self.client(captured, [json_answer({"files": []})])
        lbb.graph("other").files.list()
        self.assertEqual(captured[0].url.params["graph"], "other")

    def test_content_sends_one_byte_range(self) -> None:
        captured: list[httpx.Request] = []
        lbb = self.client(
            captured,
            [
                httpx.Response(206, content=b"789"),
                httpx.Response(206, content=b"9"),
                httpx.Response(206, content=b"23"),
            ],
        )
        self.assertEqual(lbb.files.content(SHA, range=(None, 3)), b"789")
        self.assertEqual(lbb.files.content(SHA, range=(9, None)), b"9")
        self.assertEqual(lbb.files.content(SHA, range=(2, 3)), b"23")
        self.assertEqual(
            [request.headers["range"] for request in captured],
            ["bytes=-3", "bytes=9-", "bytes=2-3"],
        )
        for bad in ((5, 2), (-1, None), (None, 0), (None, None), (True, None)):
            with self.assertRaises(ValueError, msg=str(bad)):
                lbb.files.content(SHA, range=bad)  # type: ignore[arg-type]
        self.assertEqual(len(captured), 3, "a bad range sends nothing")

    def test_a_range_with_no_byte_raises_416(self) -> None:
        captured: list[httpx.Request] = []
        lbb = self.client(
            captured,
            [
                json_answer(
                    {"error": {"code": "range_not_satisfiable", "message": "no byte"}},
                    416,
                )
            ],
        )
        with self.assertRaises(LbbError) as raised:
            lbb.files.content(SHA, range=(10, None))
        self.assertEqual(raised.exception.status_code, 416)
        self.assertEqual(raised.exception.code, "range_not_satisfiable")

    def test_download_streams_to_the_path_and_retries_a_503(self) -> None:
        captured: list[httpx.Request] = []
        body = bytes(range(256)) * 1000
        lbb = self.client(
            captured,
            [
                json_answer({"error": {"code": "server_busy", "message": "busy"}}, 503),
                httpx.Response(200, content=body),
                httpx.Response(206, content=body[-1:]),
            ],
        )
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "room-book.pdf"
            self.assertEqual(lbb.files.download(SHA, path), len(body))
            self.assertEqual(path.read_bytes(), body)
            self.assertEqual(lbb.files.download(SHA, str(path), range=(None, 1)), 1)
            self.assertEqual(path.read_bytes(), body[-1:])
            self.assertEqual(sorted(entry.name for entry in Path(folder).iterdir()), [path.name])
        self.assertEqual(len(captured), 3, "the 503 was retried")
        self.assertEqual(captured[2].headers["range"], "bytes=-1")

    def test_a_failed_download_leaves_the_path_alone(self) -> None:
        captured: list[httpx.Request] = []
        lbb = self.client(
            captured,
            [json_answer({"error": {"code": "file_not_found", "message": "no file"}}, 404)],
        )
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "kept.bin"
            path.write_bytes(b"before")
            with self.assertRaises(LbbError) as raised:
                lbb.files.download(SHA, path)
            self.assertEqual(raised.exception.code, "file_not_found")
            self.assertEqual(path.read_bytes(), b"before")
            self.assertEqual([entry.name for entry in Path(folder).iterdir()], ["kept.bin"])

    def test_head_answers_the_length_type_and_digest(self) -> None:
        captured: list[httpx.Request] = []
        lbb = self.client(
            captured,
            [
                httpx.Response(
                    200,
                    headers={
                        "content-type": "application/pdf",
                        "content-length": "18510",
                        "etag": f'"{SHA}"',
                    },
                )
            ],
        )
        self.assertEqual(
            lbb.files.head(SHA.upper()),
            {"bytes": 18510, "content_type": "application/pdf", "sha256": SHA},
        )
        self.assertEqual(captured[0].method, "HEAD")
        self.assertEqual(dict(captured[0].url.params), {"graph": "docs", "sha256": SHA})


class ChunkRecorder(httpx.BaseTransport):
    """A transport that reads each request body chunk by chunk, as the
    connection would send it, and keeps the chunks."""

    def __init__(self, answers: list[httpx.Response]) -> None:
        self.answers = list(answers)
        self.requests: list[tuple[httpx.Request, list[bytes]]] = []

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        assert isinstance(request.stream, httpx.SyncByteStream)
        chunks = [bytes(chunk) for chunk in request.stream]
        self.requests.append((request, chunks))
        return self.answers.pop(0)


class AsyncChunkRecorder(httpx.AsyncBaseTransport):
    def __init__(self, answers: list[httpx.Response]) -> None:
        self.answers = list(answers)
        self.requests: list[tuple[httpx.Request, list[bytes]]] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        assert isinstance(request.stream, httpx.AsyncByteStream)
        chunks = [bytes(chunk) async for chunk in request.stream]
        self.requests.append((request, chunks))
        return self.answers.pop(0)


class FilesUploadStreams(unittest.TestCase):
    """An upload from a path sends the file in chunks with its length, and a
    retry sends it again from the start."""

    BODY = bytes(range(256)) * 40

    def setUp(self) -> None:
        self.folder = tempfile.TemporaryDirectory()
        self.path = Path(self.folder.name) / "big.bin"
        self.path.write_bytes(self.BODY)
        patcher = mock.patch.multiple("lbb._client_base", _UPLOAD_CHUNK_BYTES=1000)
        patcher.start()
        self.addCleanup(patcher.stop)
        async_patcher = mock.patch.multiple("lbb._async_client", _UPLOAD_CHUNK_BYTES=1000)
        async_patcher.start()
        self.addCleanup(async_patcher.stop)

    def tearDown(self) -> None:
        self.folder.cleanup()

    def check(self, requests: list[tuple[httpx.Request, list[bytes]]]) -> None:
        self.assertEqual(len(requests), 2, "the 503 was retried")
        for request, chunks in requests:
            self.assertEqual(request.headers["content-length"], str(len(self.BODY)))
            self.assertNotIn("transfer-encoding", request.headers)
            self.assertEqual(request.headers["content-type"], "application/octet-stream")
            self.assertEqual(b"".join(chunks), self.BODY)
            self.assertGreater(len(chunks), 1)
            self.assertTrue(all(len(chunk) <= 1000 for chunk in chunks))

    def answers(self) -> list[httpx.Response]:
        return [
            json_answer({"error": {"code": "server_busy", "message": "busy"}}, 503),
            json_answer({"file": RECORD, "created": True}),
        ]

    def test_sync_upload_of_a_path_streams_the_file(self) -> None:
        recorder = ChunkRecorder(self.answers())
        lbb = LbbClient("http://h", graph="docs", retry_delay=0, transport=recorder)
        self.assertTrue(lbb.files.upload(self.path)["created"])
        self.check(recorder.requests)
        self.assertEqual(recorder.requests[0][0].url.params["name"], "big.bin")

    def test_async_upload_of_a_path_streams_the_file(self) -> None:
        recorder = AsyncChunkRecorder(self.answers())

        async def run() -> Any:
            async with AsyncLbbClient(
                "http://h", graph="docs", retry_delay=0, transport=recorder
            ) as lbb:
                return await lbb.files.upload(str(self.path))

        self.assertTrue(asyncio.run(run())["created"])
        self.check(recorder.requests)

    def test_bytes_still_go_whole(self) -> None:
        recorder = ChunkRecorder([json_answer({"file": RECORD, "created": True})])
        lbb = LbbClient("http://h", graph="docs", retry_delay=0, transport=recorder)
        lbb.files.upload(self.BODY, name="big.bin")
        request, chunks = recorder.requests[0]
        self.assertEqual(b"".join(chunks), self.BODY)
        self.assertEqual(request.headers["content-length"], str(len(self.BODY)))


class FilesAsync(unittest.TestCase):
    def test_async_files_await_their_answers(self) -> None:
        captured: list[httpx.Request] = []

        async def run() -> tuple[Any, bytes, Any]:
            async with AsyncLbbClient(
                "http://h",
                graph="docs",
                retry_delay=0,
                transport=transport(
                    captured,
                    [
                        json_answer({"file": RECORD, "created": True}),
                        httpx.Response(
                            200, content=PDF, headers={"content-type": "application/pdf"}
                        ),
                        json_answer(PAGES),
                    ],
                ),
            ) as lbb:
                uploaded = await lbb.files.upload(PDF, name="room-book.pdf")
                content = await lbb.files.content(SHA)
                pages = await lbb.graph("docs").files.pages(SHA, to=2)
                return uploaded, content, pages

        uploaded, content, pages = asyncio.run(run())
        self.assertTrue(uploaded["created"])
        self.assertEqual(content, PDF)
        self.assertEqual(pages["page_count"], 3)
        self.assertEqual(captured[0].content, PDF)
        self.assertEqual(dict(captured[2].url.params), {"graph": "docs", "sha256": SHA, "to": "2"})

    def test_async_range_download_and_head(self) -> None:
        captured: list[httpx.Request] = []
        body = bytes(range(256)) * 100

        async def run(folder: str) -> tuple[bytes, int, Any]:
            async with AsyncLbbClient(
                "http://h",
                graph="docs",
                retry_delay=0,
                transport=transport(
                    captured,
                    [
                        httpx.Response(206, content=b"9"),
                        json_answer({"error": {"code": "server_busy", "message": "busy"}}, 503),
                        httpx.Response(200, content=body),
                        httpx.Response(
                            200,
                            headers={
                                "content-type": "text/plain",
                                "content-length": "10",
                                "etag": f'"{SHA}"',
                            },
                        ),
                    ],
                ),
            ) as lbb:
                last = await lbb.files.content(SHA, range=(None, 1))
                written = await lbb.files.download(SHA, Path(folder) / "out.bin")
                head = await lbb.files.head(SHA)
                return last, written, head

        with tempfile.TemporaryDirectory() as folder:
            last, written, head = asyncio.run(run(folder))
            self.assertEqual((Path(folder) / "out.bin").read_bytes(), body)
        self.assertEqual(last, b"9")
        self.assertEqual(written, len(body))
        self.assertEqual(head, {"bytes": 10, "content_type": "text/plain", "sha256": SHA})
        self.assertEqual(captured[0].headers["range"], "bytes=-1")
        self.assertEqual(len(captured), 4, "the 503 before the download was retried")
        self.assertEqual(captured[3].method, "HEAD")


class EntityRefs(unittest.TestCase):
    def test_entity_ref_addresses_the_entity_by_its_id(self) -> None:
        self.assertEqual(ENTITY_IRI_NAMESPACE, "https://littlebigbrain.com/e/")
        self.assertEqual(ENTITY_ID_KEY_SENTINEL, "\u0000lbb-eid:")
        self.assertEqual(entity_id_from_iri(ENTRY_IRI), ENTRY_ID)
        self.assertEqual(entity_id_from_iri(ENTITY_IRI_NAMESPACE + ENTRY_ID.upper()), ENTRY_ID)
        self.assertEqual(
            entity_ref(ENTRY_IRI, "DocEntry"),
            {"type": "DocEntry", "name": ENTRY_ID, "key": f"\u0000lbb-eid:{ENTRY_ID}"},
        )
        self.assertEqual(entity_ref(ENTRY_IRI, "DocEntry", "S 1.1")["name"], "S 1.1")
        self.assertIn('"key": "\\u0000lbb-eid:', json.dumps(entity_ref(ENTRY_IRI, "DocEntry")))

    def test_entity_ref_refuses_other_iris(self) -> None:
        for iri in (
            "https://example.org/e/0123456789abcdef0123456789abcdef",
            "https://littlebigbrain.com/class/docentry",
            f"{ENTRY_IRI}0",
            "https://littlebigbrain.com/e/0123456789abcdef0123456789abcdeg",
        ):
            with self.assertRaises(ValueError, msg=iri):
                entity_ref(iri, "DocEntry")
        with self.assertRaises(ValueError):
            entity_ref(ENTRY_IRI, " ")

    def test_a_commit_carries_the_reference_as_a_subject(self) -> None:
        captured: list[httpx.Request] = []
        lbb = LbbClient("http://h", graph="docs", transport=transport(captured, []))
        lbb.commit(
            {
                "entity_properties": [
                    {**entity_ref(ENTRY_IRI, "DocEntry", "S 1.1"), "properties": {"width_cm": 60}}
                ],
                "replace_owned": {
                    "subjects": [entity_ref(ENTRY_IRI, "DocEntry")],
                    "properties": ["width_cm"],
                },
            }
        )
        body = json.loads(captured[0].content)
        self.assertEqual(
            body["replace_owned"]["subjects"][0]["key"], f"\u0000lbb-eid:{ENTRY_ID}"
        )
        self.assertEqual(body["entity_properties"][0]["name"], "S 1.1")


if __name__ == "__main__":
    unittest.main()
