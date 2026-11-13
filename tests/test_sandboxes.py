"""Sandboxes (``/v1/sandboxes*``), sync and async, against a stand-in for the
server.

The stand-in keeps sandboxes, commands and files in memory and answers the way
the server does for the calls these tests make. It runs no code: a ``run``
whose code holds ``sum(input)`` answers the sum of its input, and any other
code answers ``{"input", "config"}``. The server's own tests run the same
calls against a real guest agent.
"""

from __future__ import annotations

import asyncio
import json
import re
import textwrap
import unittest
from collections.abc import Callable
from pathlib import Path
from typing import Any

import httpx

from lbb import AsyncLbbClient, LbbClient, LbbError
from lbb.sandboxes import SandboxRunResult, SandboxWaitError

CODE = "def run(input, config):\n    return sum(input)"
CSV = "1,2\n3,4\n"
SBX = "sbx_1793174400000a1b2c3d4e5f6"

Responder = Callable[[httpx.Request], httpx.Response | None]


def answer(body: Any, status: int = 200) -> httpx.Response:
    return httpx.Response(status, json=body)


def error(code: str, status: int, message: str = "x") -> httpx.Response:
    return answer({"error": {"type": "invalid_request_error", "code": code, "message": message}}, status)


class Service:
    """The sandbox routes in memory, with hooks a test scripts first."""

    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []
        self.responders: list[Responder] = []
        self.sandboxes: dict[str, dict[str, Any]] = {}
        self.commands: dict[str, dict[str, Any]] = {}
        self.files: dict[str, bytes] = {}
        self._counter = 0

    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self._handle)

    def _next(self, prefix: str) -> str:
        self._counter += 1
        return f"{prefix}_{1_793_174_400_000 + self._counter:013d}a1b2c3d4e5f6"

    @staticmethod
    def _body(request: httpx.Request) -> dict[str, Any]:
        if request.content and request.headers.get("content-type", "").startswith("application/json"):
            parsed = json.loads(request.content)
            return parsed if isinstance(parsed, dict) else {}
        return {}

    def _command(self, command: str, sandbox: str, body: dict[str, Any]) -> dict[str, Any]:
        run = body.get("run")
        result: Any = None
        if run is not None:
            result = (
                sum(run.get("input") or [])
                if "sum(input)" in str(run.get("code", ""))
                else {"input": run.get("input"), "config": run.get("config")}
            )
        return {
            "id": command,
            "object": "sandbox_command",
            "sandbox": sandbox,
            "status": "exited",
            "exit_code": 0,
            "env_names": sorted(body.get("env") or {}),
            "stdin_bytes": 0,
            "timeout_ms": 60_000,
            "stdout": {"bytes": 0, "truncated_bytes": 0, "tail": ""},
            "stderr": {"bytes": 0, "truncated_bytes": 0, "tail": ""},
            "result": result,
        }

    def _handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        for responder in self.responders:
            response = responder(request)
            if response is not None:
                return response
        route = f"{request.method} {request.url.path}"
        params = request.url.params
        ident = params.get("id", "")
        sandbox = params.get("sandbox", "")
        body = self._body(request)
        if route == "POST /v1/sandboxes":
            created = {
                "id": self._next("sbx"),
                "object": "sandbox",
                "status": "running",
                "kind": "run" if body.get("run") else "sandbox",
                "template": {"id": "sbt_lbb_python", "name": body.get("template", "lbb/python")},
                "metadata": body.get("metadata", {}),
            }
            if body.get("run"):
                command = self._next("cmd")
                record = self._command(command, created["id"], body)
                created["status"] = "destroyed"
                created["run"] = {
                    "status": "exited",
                    "command": command,
                    "result": record["result"],
                    "exit_code": 0,
                }
            self.sandboxes[created["id"]] = created
            return answer(created)
        if route == "GET /v1/sandboxes/get":
            found = self.sandboxes.get(ident)
            return answer(found) if found else error("sandbox_not_found", 404)
        if route in {
            "DELETE /v1/sandboxes",
            "POST /v1/sandboxes/stop",
            "POST /v1/sandboxes/resume",
            "POST /v1/sandboxes/update",
        }:
            found = self.sandboxes.get(ident)
            if not found:
                return error("sandbox_not_found", 404)
            if route.endswith("stop"):
                found["status"] = "stopped"
            elif route.endswith("resume"):
                found["status"] = "running"
            elif route.endswith("update"):
                found["metadata"] = body.get("metadata", found["metadata"])
            else:
                found["status"] = "destroyed"
            return answer(found)
        if route == "GET /v1/sandboxes":
            return answer({"object": "list", "data": list(self.sandboxes.values()), "has_more": False})
        if route == "POST /v1/sandboxes/commands":
            command = self._next("cmd")
            record = self._command(command, sandbox, body)
            self.commands[command] = record
            return answer(record)
        if route == "GET /v1/sandboxes/commands/get":
            found = self.commands.get(ident)
            return answer(found) if found else error("sandbox_command_not_found", 404)
        if route == "GET /v1/sandboxes/commands":
            return answer({"object": "list", "data": list(self.commands.values()), "has_more": False})
        if route == "GET /v1/sandboxes/commands/logs":
            return answer({"object": "sandbox_log", "data": [], "next_offset": 0, "done": True})
        if route == "POST /v1/sandboxes/commands/cancel":
            found = self.commands.get(ident)
            if not found:
                return error("sandbox_command_not_found", 404)
            return answer({**found, "status": "cancelled", "reason": "request"})
        if route == "PUT /v1/sandboxes/files/content":
            path = params.get("path", "")
            self.files[f"{sandbox}:{path}"] = request.content
            return answer(
                {
                    "object": "sandbox_file",
                    "path": f"/workspace/{path}",
                    "type": "file",
                    "bytes": len(request.content),
                    "modified": 1_793_174_400,
                }
            )
        if route == "GET /v1/sandboxes/files/content":
            data = self.files.get(f"{sandbox}:{params.get('path', '')}")
            if data is None:
                return error("sandbox_file_not_found", 404)
            return httpx.Response(200, content=data, headers={"content-type": "application/octet-stream"})
        if route == "GET /v1/sandboxes/files":
            prefix = f"{sandbox}:"
            return answer(
                {
                    "object": "list",
                    "data": [
                        {
                            "object": "sandbox_file",
                            "path": f"/workspace/{key[len(prefix):]}",
                            "type": "file",
                            "bytes": 1,
                            "modified": 1_793_174_400,
                        }
                        for key in self.files
                        if key.startswith(prefix)
                    ],
                    "has_more": False,
                }
            )
        if route == "DELETE /v1/sandboxes/files":
            path = params.get("path", "")
            self.files.pop(f"{sandbox}:{path}", None)
            return answer({"object": "sandbox_file_deleted", "path": f"/workspace/{path}", "deleted": True})
        if route == "POST /v1/sandboxes/templates":
            return answer(
                {
                    "id": self._next("sbt"),
                    "object": "sandbox_template",
                    "name": body.get("name"),
                    "base": body.get("base", "lbb/python"),
                    "code": body.get("code"),
                    "timeout_ms": body.get("timeout_ms", 60_000),
                    "config": body.get("config"),
                }
            )
        if route == "GET /v1/sandboxes/templates":
            return answer({"object": "list", "data": [], "has_more": False})
        if route == "GET /v1/sandboxes/templates/get":
            return answer({"id": ident, "object": "sandbox_template", "name": "lbb/python"})
        return error("unknown_route", 404, route)

    def posts(self, path: str = "/v1/sandboxes") -> list[httpx.Request]:
        return [r for r in self.requests if r.method == "POST" and r.url.path == path]


def lost_run() -> Responder:
    def respond(request: httpx.Request) -> httpx.Response | None:
        if request.method == "POST" and request.url.path == "/v1/sandboxes":
            return answer({"id": SBX, "status": "lost", "run": {"status": "lost", "command": "cmd_1"}})
        return None

    return respond


def sync_client(service: Service) -> LbbClient:
    return LbbClient("http://h", graph="main", retry_delay=0, transport=service.transport())


def async_client(service: Service) -> AsyncLbbClient:
    return AsyncLbbClient("http://h", graph="main", retry_delay=0, transport=service.transport())


class RunSync(unittest.TestCase):
    def test_run_sends_one_create_with_the_code_the_input_and_a_key(self) -> None:
        service = Service()
        result = sync_client(service).sandboxes.run(code=CODE, input=[1, 2, 3])
        self.assertIsInstance(result, SandboxRunResult)
        self.assertEqual(result.result, 6)
        self.assertTrue(result.ok)
        self.assertEqual((result.status, result.exit_code), ("exited", 0))
        self.assertTrue(result.command.startswith("cmd_"))
        self.assertEqual(result.sandbox["status"], "destroyed")
        self.assertEqual(len(service.requests), 1)
        request = service.requests[0]
        self.assertEqual((request.method, request.url.path), ("POST", "/v1/sandboxes"))
        self.assertEqual(request.url.params["graph"], "main")
        self.assertRegex(request.headers["idempotency-key"], r"^sandboxes\.run:")
        self.assertEqual(
            json.loads(request.content),
            {"run": {"code": CODE, "input": [1, 2, 3]}, "wait_ms": 30_000},
        )

    def test_run_takes_a_key_a_template_limits_and_the_command_timeout(self) -> None:
        service = Service()
        sync_client(service).sandboxes.run(
            template="normalize",
            input={"file": "a.csv"},
            config={"unit": "mm"},
            limits={"vcpus": 2, "memory_mib": 512, "disk_mib": 1024},
            timeout_ms=5_000,
            wait_ms=10_000,
            expires_in_seconds=600,
            metadata={"job": "x"},
            idempotency_key="mine-1",
        )
        request = service.requests[0]
        self.assertEqual(request.headers["idempotency-key"], "mine-1")
        self.assertEqual(
            json.loads(request.content),
            {
                "template": "normalize",
                "limits": {"vcpus": 2, "memory_mib": 512, "disk_mib": 1024},
                "expires_in_seconds": 600,
                "metadata": {"job": "x"},
                "run": {
                    "input": {"file": "a.csv"},
                    "config": {"unit": "mm"},
                    "timeout_ms": 5_000,
                },
                "wait_ms": 10_000,
            },
        )

    def test_input_null_is_sent_and_an_input_left_out_is_not(self) -> None:
        service = Service()
        sync_client(service).sandboxes.run(code=CODE, input=None)
        sync_client(service).sandboxes.run(code=CODE)
        self.assertEqual(json.loads(service.requests[0].content)["run"], {"code": CODE, "input": None})
        self.assertEqual(json.loads(service.requests[1].content)["run"], {"code": CODE})

    def test_a_run_whose_code_failed_is_a_result_with_ok_false(self) -> None:
        service = Service()
        service.responders.append(
            lambda r: answer(
                {"id": SBX, "status": "destroyed", "run": {"status": "exited", "command": "cmd_1", "exit_code": 1}}
            )
            if r.method == "POST"
            else None
        )
        result = sync_client(service).sandboxes.run(code="def run(i, c):\n    raise ValueError()")
        self.assertFalse(result.ok)
        self.assertEqual(result.exit_code, 1)
        self.assertIsNone(result.result)

    def test_a_retry_after_a_503_sends_the_same_key(self) -> None:
        service = Service()
        failed: list[bool] = []

        def busy(request: httpx.Request) -> httpx.Response | None:
            if request.method == "POST" and not failed:
                failed.append(True)
                return error("sandbox_unavailable", 503, "no host")
            return None

        service.responders.append(busy)
        sync_client(service).sandboxes.run(code=CODE, input=[1])
        posts = service.posts()
        self.assertEqual(len(posts), 2)
        self.assertEqual(posts[0].headers["idempotency-key"], posts[1].headers["idempotency-key"])

    def test_a_run_that_outlives_its_wait_is_polled_by_the_sandbox(self) -> None:
        service = Service()
        reads: list[int] = []

        def slow(request: httpx.Request) -> httpx.Response | None:
            if request.method == "POST" and request.url.path == "/v1/sandboxes":
                return answer({"id": SBX, "status": "running", "run": {"status": "running", "command": "cmd_1"}})
            if request.method == "GET" and request.url.path == "/v1/sandboxes/get":
                reads.append(1)
                if len(reads) < 2:
                    return answer({"id": SBX, "status": "running", "run": {"status": "running", "command": "cmd_1"}})
                return answer(
                    {
                        "id": SBX,
                        "status": "destroyed",
                        "run": {"status": "exited", "command": "cmd_1", "exit_code": 0, "result": 42},
                    }
                )
            return None

        service.responders.append(slow)
        result = sync_client(service).sandboxes.run(code=CODE)
        self.assertEqual(result.result, 42)
        self.assertEqual(len(reads), 2)
        self.assertEqual(len(service.posts()), 1, "polling never creates a second sandbox")

    def test_a_run_that_never_ends_raises_the_wait_error_with_the_sandbox(self) -> None:
        service = Service()
        service.responders.append(
            lambda r: answer({"id": SBX, "status": "running", "run": {"status": "running", "command": "cmd_1"}})
            if r.url.path in ("/v1/sandboxes", "/v1/sandboxes/get")
            else None
        )
        with self.assertRaises(SandboxWaitError) as caught:
            sync_client(service).sandboxes.run(code=CODE, max_wait_ms=50)
        self.assertEqual(caught.exception.sandbox["id"], SBX)

    def test_a_lost_run_goes_again_under_the_next_key_twice_at_most(self) -> None:
        service = Service()
        lost: list[int] = [2]

        def sometimes_lost(request: httpx.Request) -> httpx.Response | None:
            if lost[0] > 0 and request.method == "POST" and request.url.path == "/v1/sandboxes":
                lost[0] -= 1
                return answer({"id": SBX, "status": "lost", "run": {"status": "lost", "command": "cmd_1"}})
            return None

        service.responders.append(sometimes_lost)
        result = sync_client(service).sandboxes.run(code=CODE, input=[4, 5], idempotency_key="job")
        self.assertEqual(result.result, 9)
        self.assertEqual([r.headers["idempotency-key"] for r in service.posts()], ["job", "job.2", "job.3"])

    def test_a_run_that_stays_lost_is_returned_lost_and_zero_retries_turn_them_off(self) -> None:
        stubborn = Service()
        stubborn.responders.append(lost_run())
        result = sync_client(stubborn).sandboxes.run(code=CODE)
        self.assertEqual((result.status, result.ok), ("lost", False))
        self.assertEqual(len(stubborn.requests), 3, "one run and two more")
        once = Service()
        once.responders.append(lost_run())
        sync_client(once).sandboxes.run(code=CODE, lost_retries=0)
        self.assertEqual(len(once.requests), 1)

    def test_an_error_of_the_server_is_an_lbb_error(self) -> None:
        service = Service()
        service.responders.append(lambda r: error("sandbox_not_enabled", 409) if r.method == "POST" else None)
        with self.assertRaises(LbbError) as caught:
            sync_client(service).sandboxes.run(code=CODE)
        self.assertEqual(caught.exception.code, "sandbox_not_enabled")

    def test_the_reads_of_the_wait_carry_the_options_of_the_call(self) -> None:
        service = Service()
        reads: list[httpx.Request] = []

        def slow(request: httpx.Request) -> httpx.Response | None:
            if request.method == "POST" and request.url.path == "/v1/sandboxes":
                return answer({"id": SBX, "status": "running", "run": {"status": "running", "command": "cmd_1"}})
            if request.method == "GET" and request.url.path == "/v1/sandboxes/get":
                reads.append(request)
                return answer(
                    {
                        "id": SBX,
                        "status": "destroyed",
                        "run": {"status": "exited", "command": "cmd_1", "exit_code": 0, "result": 1},
                    }
                )
            return None

        service.responders.append(slow)
        sync_client(service).sandboxes.run(code=CODE, options={"headers": {"x-trace": "t-1"}})
        self.assertEqual(len(reads), 1)
        self.assertEqual(service.posts()[0].headers["x-trace"], "t-1")
        self.assertEqual(reads[0].headers["x-trace"], "t-1", "the poll carries the headers of the call")

    def test_an_empty_key_is_no_key_and_the_sdk_makes_one(self) -> None:
        service = Service()
        client = sync_client(service)
        client.sandboxes.run(code=CODE, idempotency_key="")
        client.sandboxes.create(idempotency_key="")
        with client.sandboxes.scoped(idempotency_key=""):
            pass
        keys = [r.headers.get("idempotency-key") for r in service.requests if r.method == "POST"]
        self.assertEqual(len(keys), 3)
        for key in keys:
            self.assertTrue(key, "a key that is not empty")

    def test_lost_retries_is_zero_to_two_whatever_is_asked(self) -> None:
        for asked, runs in ((5, 3), (2, 3), (1, 2), (0, 1), (-4, 1)):
            service = Service()
            service.responders.append(lost_run())
            sync_client(service).sandboxes.run(code=CODE, lost_retries=asked)
            self.assertEqual(len(service.posts()), runs, f"lost_retries={asked}")


class ScopedAndHandleSync(unittest.TestCase):
    def test_scoped_makes_a_sandbox_hands_over_the_handle_and_destroys_it(self) -> None:
        service = Service()
        with sync_client(service).sandboxes.scoped({"template": "normalize", "expires_in_seconds": 600}) as sb:
            self.assertTrue(sb.id.startswith("sbx_"))
            sb.files.write("data.csv", CSV)
        routes = [(r.method, r.url.path) for r in service.requests]
        self.assertEqual(
            routes,
            [
                ("POST", "/v1/sandboxes"),
                ("PUT", "/v1/sandboxes/files/content"),
                ("DELETE", "/v1/sandboxes"),
            ],
        )
        self.assertRegex(service.requests[0].headers["idempotency-key"], r"^sandboxes\.create:")
        self.assertEqual(
            json.loads(service.requests[0].content),
            {"template": "normalize", "expires_in_seconds": 600},
        )

    def test_scoped_destroys_the_sandbox_after_an_error_and_the_error_stays(self) -> None:
        service = Service()
        service.responders.append(lambda r: error("sandbox_unavailable", 503) if r.method == "DELETE" else None)
        client = LbbClient(
            "http://h", graph="main", retry_delay=0, max_retries=0, transport=service.transport()
        )
        with self.assertRaises(ValueError):
            with client.sandboxes.scoped():
                raise ValueError("the work failed")
        self.assertEqual(len([r for r in service.requests if r.method == "DELETE"]), 1)

    def test_scoped_ignores_a_sandbox_that_is_gone_and_reports_another_failure(self) -> None:
        gone = Service()
        gone.responders.append(lambda r: error("sandbox_not_found", 404) if r.method == "DELETE" else None)
        with sync_client(gone).sandboxes.scoped():
            pass
        broken = Service()
        broken.responders.append(lambda r: error("forbidden", 403) if r.method == "DELETE" else None)
        with self.assertRaises(LbbError) as caught:
            with sync_client(broken).sandboxes.scoped():
                pass
        self.assertEqual(caught.exception.status_code, 403)

    def test_the_handle_reads_writes_lists_and_deletes_files(self) -> None:
        service = Service()
        sb = sync_client(service).sandboxes.create()
        written = sb.files.write("data/a.csv", CSV)
        self.assertEqual(written["bytes"], len(CSV))
        put = service.requests[-1]
        self.assertEqual((put.method, put.url.params["sandbox"], put.url.params["path"]), ("PUT", sb.id, "data/a.csv"))
        self.assertEqual(put.headers["content-type"], "application/octet-stream")
        self.assertEqual(put.content, CSV.encode())
        self.assertEqual(sb.files.read("data/a.csv"), CSV.encode())
        sb.files.read("data/a.csv", start=2, end=4)
        self.assertEqual(service.requests[-1].headers["range"], "bytes=2-4")
        listing = sb.files.list("/workspace", recursive=True, limit=10)
        self.assertEqual(listing["data"][0]["path"], "/workspace/data/a.csv")
        self.assertEqual(service.requests[-1].url.params["recursive"], "true")
        sb.files.delete("data", recursive=True)
        self.assertEqual(service.requests[-1].method, "DELETE")
        self.assertEqual(service.requests[-1].url.params["recursive"], "true")

    def test_the_handle_of_a_sandbox_that_exists_is_made_from_its_record_with_no_request(self) -> None:
        service = Service()
        client = sync_client(service)
        made = client.sandboxes.create()
        # A caller that stored the id reads the record, and makes the handle.
        record = client.sandboxes.retrieve(made.id)
        requests = len(service.requests)
        sb = client.sandboxes.handle(record)
        self.assertEqual(len(service.requests), requests, "the handle makes no request")
        self.assertEqual((sb.id, sb.record["id"]), (made.id, made.id))
        self.assertIsNot(sb.record, record, "the handle holds a copy of the record")
        # Its files and its commands work on the sandbox that exists.
        sb.files.write("data.csv", CSV)
        put = service.requests[-1]
        self.assertEqual((put.method, put.url.params["sandbox"]), ("PUT", made.id))
        self.assertEqual(sb.files.read("data.csv"), CSV.encode())
        self.assertEqual(sb.run(code=CODE, input=[1, 2])["result"], 3)
        self.assertEqual(service.requests[-1].url.params["sandbox"], made.id)
        self.assertEqual(sb.refresh()["id"], made.id)
        self.assertEqual(sb.delete()["status"], "destroyed")

    def test_read_text_decodes_a_file_as_utf8(self) -> None:
        service = Service()
        sb = sync_client(service).sandboxes.create()
        sb.files.write("words.txt", "caf\u00e9 \u00fcber\n")
        text = sb.files.read_text("words.txt")
        self.assertIsInstance(text, str)
        self.assertEqual(text, "caf\u00e9 \u00fcber\n")
        get = service.requests[-1]
        self.assertEqual((get.method, get.url.params["path"]), ("GET", "words.txt"))
        # The range of read applies, and a byte that is not UTF-8 reads as U+FFFD.
        sb.files.read_text("words.txt", start=1, end=3)
        self.assertEqual(service.requests[-1].headers["range"], "bytes=1-3")
        service.files[next(iter(service.files))] = b"ok \xff ok"
        self.assertEqual(sb.files.read_text("words.txt"), "ok \ufffd ok")

    def test_the_handle_runs_code_and_commands_with_keys(self) -> None:
        service = Service()
        sb = sync_client(service).sandboxes.create()
        rows = sb.run(code=CODE, input=[1, 2], wait_ms=5_000)
        self.assertEqual(rows["result"], 3)
        run = service.requests[-1]
        self.assertEqual(run.url.path, "/v1/sandboxes/commands")
        self.assertEqual(run.url.params["sandbox"], sb.id)
        self.assertRegex(run.headers["idempotency-key"], r"^sandboxes\.commands\.create:")
        self.assertEqual(json.loads(run.content), {"run": {"code": CODE, "input": [1, 2]}, "wait_ms": 5_000})
        command = sb.commands.create(
            argv=["python", "check.py"],
            cwd="/workspace",
            env={"MODE": "fast"},
            stdin="x",
            timeout_ms=2_000,
            idempotency_key="check-1",
        )
        sent = service.requests[-1]
        self.assertEqual(sent.headers["idempotency-key"], "check-1")
        self.assertEqual(
            json.loads(sent.content),
            {
                "argv": ["python", "check.py"],
                "cwd": "/workspace",
                "env": {"MODE": "fast"},
                "stdin": "x",
                "timeout_ms": 2_000,
                "wait_ms": 30_000,
            },
        )
        self.assertEqual(sb.commands.retrieve(command["id"])["id"], command["id"])
        self.assertGreaterEqual(len(sb.commands.list(status="exited", limit=5)["data"]), 2)
        self.assertEqual(service.requests[-1].url.params["status"], "exited")
        self.assertTrue(sb.commands.logs(command["id"], stream="stderr", offset=10, limit=20)["done"])
        self.assertEqual(service.requests[-1].url.params["stream"], "stderr")
        self.assertEqual(sb.commands.cancel(command["id"], grace_ms=100)["status"], "cancelled")
        self.assertEqual(json.loads(service.requests[-1].content), {"grace_ms": 100})

    def test_the_handle_stops_resumes_updates_reads_and_deletes_its_sandbox(self) -> None:
        service = Service()
        sb = sync_client(service).sandboxes.create(limits={"memory_mib": 256}, auto_stop_seconds=60)
        self.assertEqual(
            json.loads(service.requests[0].content),
            {"limits": {"memory_mib": 256}, "auto_stop_seconds": 60},
        )
        self.assertEqual(sb.stop()["status"], "stopped")
        self.assertEqual(sb.record["status"], "stopped")
        self.assertEqual(sb.resume()["status"], "running")
        self.assertEqual(sb.update(metadata={"a": "b"})["metadata"], {"a": "b"})
        self.assertEqual(sb.refresh()["id"], sb.id)
        self.assertEqual(sb.delete()["status"], "destroyed")

    def test_a_spec_with_an_unknown_field_is_refused_before_a_request(self) -> None:
        service = Service()
        with self.assertRaises(TypeError):
            sync_client(service).sandboxes.create({"stack": "x"})
        self.assertEqual(service.requests, [])

    def test_the_namespace_lists_retrieves_and_saves_templates(self) -> None:
        service = Service()
        client = sync_client(service)
        client.sandboxes.list(status="running", kind="run", created_by="key:a", limit=3)
        params = service.requests[-1].url.params
        self.assertEqual(
            (params["status"], params["kind"], params["created_by"], params["limit"]),
            ("running", "run", "key:a", "3"),
        )
        template = client.sandboxes.templates.create(
            name="normalize",
            base="lbb/python",
            code=CODE,
            limits={"memory_mib": 2048},
            timeout_ms=60_000,
            config={"unit": "cm"},
        )
        self.assertEqual(template["name"], "normalize")
        saved = service.requests[-1]
        self.assertRegex(saved.headers["idempotency-key"], r"^sandboxes\.templates\.create:")
        self.assertEqual(
            json.loads(saved.content),
            {
                "name": "normalize",
                "base": "lbb/python",
                "code": CODE,
                "limits": {"memory_mib": 2048},
                "timeout_ms": 60_000,
                "config": {"unit": "cm"},
            },
        )
        client.sandboxes.templates.list(limit=2)
        client.sandboxes.templates.retrieve("sbt_lbb_python")
        self.assertEqual(service.requests[-1].url.params["id"], "sbt_lbb_python")


class Async(unittest.TestCase):
    def test_run_makes_a_key_and_a_lost_run_goes_again(self) -> None:
        async def go() -> None:
            service = Service()
            lost: list[int] = [1]

            def sometimes_lost(request: httpx.Request) -> httpx.Response | None:
                if lost[0] > 0 and request.method == "POST" and request.url.path == "/v1/sandboxes":
                    lost[0] -= 1
                    return answer({"id": SBX, "status": "lost", "run": {"status": "lost", "command": "cmd_1"}})
                return None

            service.responders.append(sometimes_lost)
            result = await async_client(service).sandboxes.run(code=CODE, input=[1, 2, 3], idempotency_key="j")
            self.assertEqual((result.result, result.ok), (6, True))
            self.assertEqual([r.headers["idempotency-key"] for r in service.posts()], ["j", "j.2"])

        asyncio.run(go())

    def test_the_reads_of_the_wait_carry_the_options_and_lost_retries_stop_at_two(self) -> None:
        async def go() -> None:
            service = Service()
            reads: list[httpx.Request] = []

            def slow(request: httpx.Request) -> httpx.Response | None:
                if request.method == "POST" and request.url.path == "/v1/sandboxes":
                    return answer({"id": SBX, "status": "running", "run": {"status": "running", "command": "cmd_1"}})
                if request.method == "GET" and request.url.path == "/v1/sandboxes/get":
                    reads.append(request)
                    return answer(
                        {
                            "id": SBX,
                            "status": "destroyed",
                            "run": {"status": "exited", "command": "cmd_1", "exit_code": 0, "result": 1},
                        }
                    )
                return None

            service.responders.append(slow)
            await async_client(service).sandboxes.run(code=CODE, options={"headers": {"x-trace": "t-2"}})
            self.assertEqual(reads[0].headers["x-trace"], "t-2")

            stubborn = Service()
            stubborn.responders.append(lost_run())
            await async_client(stubborn).sandboxes.run(code=CODE, lost_retries=9)
            self.assertEqual(len(stubborn.posts()), 3, "one run and two more, whatever was asked")

            keyed = Service()
            await async_client(keyed).sandboxes.run(code=CODE, idempotency_key="")
            self.assertTrue(keyed.posts()[0].headers["idempotency-key"])

        asyncio.run(go())

    def test_run_polls_a_slow_run_and_raises_the_wait_error(self) -> None:
        async def go() -> None:
            service = Service()
            service.responders.append(
                lambda r: answer({"id": SBX, "status": "running", "run": {"status": "running", "command": "cmd_1"}})
                if r.url.path in ("/v1/sandboxes", "/v1/sandboxes/get")
                else None
            )
            with self.assertRaises(SandboxWaitError):
                await async_client(service).sandboxes.run(code=CODE, max_wait_ms=50)

        asyncio.run(go())

    def test_the_handle_of_a_sandbox_that_exists_and_read_text_work_with_await(self) -> None:
        async def go() -> None:
            service = Service()
            client = async_client(service)
            made = await client.sandboxes.create()
            record = await client.sandboxes.retrieve(made.id)
            requests = len(service.requests)
            # `handle` makes no request: a plain call, and no coroutine.
            sb = client.sandboxes.handle(record)
            self.assertFalse(asyncio.iscoroutine(sb))
            self.assertEqual(len(service.requests), requests, "the handle makes no request")
            self.assertEqual(sb.id, made.id)
            await sb.files.write("words.txt", "caf\u00e9\n")
            self.assertEqual(await sb.files.read_text("words.txt"), "caf\u00e9\n")
            self.assertEqual(service.requests[-1].url.params["sandbox"], made.id)
            service.files[next(iter(service.files))] = b"ok \xff ok"
            self.assertEqual(await sb.files.read_text("words.txt"), "ok \ufffd ok")
            self.assertEqual((await sb.delete())["status"], "destroyed")

        asyncio.run(go())

    def test_scoped_and_the_handle_work_with_await(self) -> None:
        async def go() -> None:
            service = Service()
            client = async_client(service)
            async with client.sandboxes.scoped({"template": "normalize"}) as sb:
                await sb.files.write("data.csv", CSV)
                self.assertEqual(await sb.files.read("data.csv"), CSV.encode())
                rows = await sb.run(code=CODE, input=[1, 2])
                self.assertEqual(rows["result"], 3)
                self.assertEqual((await sb.stop())["status"], "stopped")
                self.assertEqual((await sb.resume())["status"], "running")
            self.assertEqual(service.requests[-1].method, "DELETE")
            self.assertEqual(sb.record["status"], "destroyed")
            with self.assertRaises(ValueError):
                async with client.sandboxes.scoped():
                    raise ValueError("the work failed")
            self.assertEqual(service.requests[-1].method, "DELETE")
            kept = await client.sandboxes.create({"template": "normalize"})
            self.assertEqual((await kept.update(auto_stop_seconds=60))["id"], kept.id)
            self.assertEqual((await client.sandboxes.retrieve(kept.id))["id"], kept.id)
            self.assertEqual((await client.sandboxes.delete(kept.id))["status"], "destroyed")
            await client.sandboxes.templates.create(name="n", code=CODE)

        asyncio.run(go())


# ---- the examples of the guide, run ---------------------------------------------------

_GUIDE: Path | None = next(
    (
        parent / "docs" / "guides" / "sandboxes.md"
        for parent in Path(__file__).resolve().parents
        if (parent / "docs" / "guides" / "sandboxes.md").is_file()
    ),
    None,
)

#: The ladder's examples as the guide shows them. The guide holds each one as
#: written. ``async`` examples run inside a coroutine.
EXAMPLES: dict[str, tuple[bool, str]] = {
    "level 1": (False, "result = client.sandboxes.run(code=code, input=[1, 2, 3]).result"),
    "level 2": (
        False,
        'client.sandboxes.templates.create(\n'
        '    name="normalize",\n'
        '    base="lbb/python",\n'
        "    code=code,\n"
        '    limits={"memory_mib": 2048},\n'
        "    timeout_ms=60_000,\n"
        '    config={"unit": "cm"},\n'
        ")\n"
        'client.sandboxes.run(template="normalize", input=[1, 2, 3])',
    ),
    "level 3, scoped": (
        False,
        'with client.sandboxes.scoped({"template": "normalize"}) as sb:\n'
        '    sb.files.write("data.csv", csv)\n'
        '    rows = sb.run(input={"file": "data.csv"})\n'
        '    out = sb.commands.create(argv=["python", "check.py"], wait_ms=30_000)',
    ),
    "level 3, scoped, async": (
        True,
        'async with client.sandboxes.scoped({"template": "normalize"}) as sb:   # client = AsyncLbbClient(...)\n'
        '    await sb.files.write("data.csv", csv)\n'
        '    rows = await sb.run(input={"file": "data.csv"})\n'
        '    out = await sb.commands.create(argv=["python", "check.py"], wait_ms=30_000)',
    ),
    "level 3, a handle you keep": (
        False,
        'sb = client.sandboxes.create({"template": "normalize", "expires_in_seconds": 600})\n'
        'sb.files.write("data.csv", csv)\n'
        "sb.stop()\n"
        "sb.resume()\n"
        "sb.delete()",
    ),
}


class GuideExamples(unittest.TestCase):
    @unittest.skipIf(_GUIDE is None, "the guide is not in this checkout")
    def test_the_ladders_examples_are_in_the_guide_as_written_and_they_run(self) -> None:
        assert _GUIDE is not None
        text = _GUIDE.read_text(encoding="utf-8")
        for name, (is_async, source) in EXAMPLES.items():
            with self.subTest(example=name):
                self.assertIn(source, text, f"docs/guides/sandboxes.md holds the example {name!r}")
                service = Service()
                scope: dict[str, Any] = {"code": CODE, "csv": CSV}
                if is_async:
                    wrapped = "async def main(client, code, csv):\n" + textwrap.indent(source, "    ")
                    exec(compile(wrapped, f"<{name}>", "exec"), scope)
                    asyncio.run(scope["main"](async_client(service), CODE, CSV))
                else:
                    scope["client"] = sync_client(service)
                    exec(compile(source, f"<{name}>", "exec"), scope)
                self.assertTrue(service.requests, f"the example {name!r} made calls")
        self.assertTrue(re.search(r"`LBB_SANDBOX_BACKEND=docker`", text), "the guide names the backend")


if __name__ == "__main__":
    unittest.main()
