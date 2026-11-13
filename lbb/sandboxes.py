"""Sandboxes (``/v1/sandboxes*``): isolated code runs.

A sandbox is a VM-like workspace with a disk and no network. LBB keeps the
records of what ran, on which inputs, with which result.

One resource on three levels in this SDK, with the same records on each:

* **L1** ``sandboxes.run(code=..., input=...)``: one call; the sandbox is
  destroyed after the run.
* **L2** ``sandboxes.run(template="name", input=...)``: a saved template names
  the code and the limits.
* **L3** ``sandboxes.scoped(spec)`` and ``sandboxes.create(...)``: the
  resource API, with files, commands, stop and resume.

Level 4 (``ctx.sandboxes`` in a workflow handler) is not in this SDK yet.

Every call that runs code carries an ``Idempotency-Key``, which the SDK makes
when you give none, so a retry after a ``5xx`` or a lost connection answers the
same record and never runs the code twice.

``LbbClient.sandboxes`` has plain methods and ``with``; ``AsyncLbbClient.sandboxes``
has the same names with ``await`` and ``async with``.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from types import TracebackType
from typing import Any, Final

from ._client_base import LbbError, RequestOptions, _BaseLbbClient

#: The most ``wait_ms`` holds the answer of a create, and the SDK's default.
SANDBOX_WAIT_MS_MAX: Final = 30_000
#: The most a lost run goes again: the design says twice for the class ``fresh``.
LOST_RETRIES_MAX: Final = 2


def _lost_retries(requested: int) -> int:
    """How often a lost run goes again: 0 to 2, whatever was asked for."""
    return min(LOST_RETRIES_MAX, max(0, int(requested)))


class _Unset:
    """The type of :data:`UNSET`: an argument that was not given."""

    def __repr__(self) -> str:
        return "UNSET"


#: An argument left out, as opposed to ``None`` (JSON ``null``).
UNSET: Final = _Unset()


@dataclass(frozen=True)
class SandboxRunResult:
    """What an L1 or L2 call comes to."""

    #: What ``run(input, config)`` returned (JSON, 48 KiB at most); ``None`` when
    #: the run made no result.
    result: Any
    #: The command's status: ``exited`` with ``exit_code`` 0 is a run that worked.
    status: str
    exit_code: int | None
    #: The command's id (``cmd_``).
    command: str
    #: The run exited with code 0. Check it before you use ``result``.
    ok: bool
    #: The sandbox record of the run.
    sandbox: dict[str, Any]


class SandboxWaitError(RuntimeError):
    """The run did not end within ``max_wait_ms``. The sandbox and the command
    stay; read them by their ids."""

    def __init__(self, sandbox: dict[str, Any], waited_ms: float) -> None:
        run = sandbox.get("run") or {}
        super().__init__(
            f"sandbox {sandbox.get('id')} did not finish its run within "
            f"{int(waited_ms)} ms (status {run.get('status') or sandbox.get('status')})"
        )
        self.sandbox = sandbox
        self.waited_ms = waited_ms


def _drop_none(values: Mapping[str, Any]) -> dict[str, Any]:
    return {name: value for name, value in values.items() if value is not None}


def _given(value: Any) -> bool:
    return not isinstance(value, _Unset)


def _create_body(
    *,
    template: str | None,
    limits: Mapping[str, int] | None,
    auto_stop_seconds: int | None,
    expires_in_seconds: int | None,
    metadata: Mapping[str, str] | None,
) -> dict[str, Any]:
    return _drop_none(
        {
            "template": template,
            "limits": dict(limits) if limits is not None else None,
            "auto_stop_seconds": auto_stop_seconds,
            "expires_in_seconds": expires_in_seconds,
            "metadata": dict(metadata) if metadata is not None else None,
        }
    )


def _run_body(
    *,
    code: str | None | _Unset,
    input: Any,
    config: Any,
    timeout_ms: int | None,
) -> dict[str, Any]:
    body: dict[str, Any] = {}
    if code is not None and _given(code):
        body["code"] = code
    if _given(input):
        body["input"] = input
    if _given(config):
        body["config"] = config
    if timeout_ms is not None:
        body["timeout_ms"] = timeout_ms
    return body


def _result_of(sandbox: dict[str, Any]) -> SandboxRunResult:
    run = sandbox.get("run") or {}
    status = run.get("status") or "failed"
    exit_code = run.get("exit_code")
    return SandboxRunResult(
        result=run.get("result"),
        status=status,
        exit_code=exit_code,
        command=run.get("command") or "",
        ok=status == "exited" and exit_code == 0,
        sandbox=sandbox,
    )


def _run_is_final(sandbox: dict[str, Any]) -> bool:
    run = sandbox.get("run")
    return not run or run.get("status") not in ("queued", "running")


def _file_bytes(data: str | bytes | bytearray | memoryview) -> bytes:
    if isinstance(data, str):
        return data.encode("utf-8")
    return bytes(data)


def _text_of(data: bytes | bytearray | memoryview) -> str:
    """The text of file bytes, decoded as UTF-8. A byte that is not UTF-8 reads
    as U+FFFD, as the TypeScript SDK's ``readText`` does."""
    return bytes(data).decode("utf-8", errors="replace")


class _SandboxFiles:
    """The files of a sandbox's ``/workspace``. Each method returns the answer
    (an awaitable on :class:`AsyncLbbClient`)."""

    def __init__(self, client: _BaseLbbClient, sandbox: str) -> None:
        self._client = client
        self._sandbox = sandbox

    def read(
        self, path: str, *, start: int | None = None, end: int | None = None
    ) -> Any:
        """A file's bytes (``GET /v1/sandboxes/files/content``), at most 4 MiB;
        a larger file needs ``start`` (and ``end``, both included). A path is
        under ``/workspace``; a relative path is."""
        options: RequestOptions | None = None
        if start is not None:
            options = {"headers": {"range": f"bytes={start}-{'' if end is None else end}"}}
        return self._client._bytes_request(
            "GET",
            "/v1/sandboxes/files/content",
            params={"sandbox": self._sandbox, "path": path},
            options=options,
        )

    def write(self, path: str, data: str | bytes | bytearray | memoryview) -> Any:
        """Write a file (``PUT /v1/sandboxes/files/content``), 4 MiB at most,
        and the directories above it. The same bytes again change nothing, so
        a retry is safe. Answers the file entry."""
        return self._client._request(
            "PUT",
            "/v1/sandboxes/files/content",
            params={"sandbox": self._sandbox, "path": path},
            content=_file_bytes(data),
            content_type="application/octet-stream",
            options={"retry": True},
        )

    def list(
        self,
        path: str = "/workspace",
        *,
        recursive: bool = False,
        limit: int | None = None,
        cursor: str | None = None,
    ) -> Any:
        """One page of a directory (``GET /v1/sandboxes/files``), in path order."""
        return self._client._request(
            "GET",
            "/v1/sandboxes/files",
            params=_drop_none(
                {
                    "sandbox": self._sandbox,
                    "path": path,
                    "recursive": True if recursive else None,
                    "limit": limit,
                    "cursor": cursor,
                }
            ),
        )

    def delete(self, path: str, *, recursive: bool = False) -> Any:
        """Delete a file, or a directory with ``recursive``."""
        return self._client._request(
            "DELETE",
            "/v1/sandboxes/files",
            params=_drop_none(
                {
                    "sandbox": self._sandbox,
                    "path": path,
                    "recursive": True if recursive else None,
                }
            ),
            options={"retry": True},
        )


class _SyncSandboxFiles(_SandboxFiles):
    def read_text(self, path: str, *, start: int | None = None, end: int | None = None) -> str:
        """A file's text, decoded as UTF-8: :meth:`read`, decoded. The same size
        limit and the same ``start`` and ``end`` apply."""
        return _text_of(self.read(path, start=start, end=end))


class _AsyncSandboxFiles(_SandboxFiles):
    async def read_text(self, path: str, *, start: int | None = None, end: int | None = None) -> str:
        """A file's text, decoded as UTF-8: :meth:`read`, decoded. The same size
        limit and the same ``start`` and ``end`` apply."""
        return _text_of(await self.read(path, start=start, end=end))


class _SandboxCommands:
    """The commands of a sandbox. Each method returns the answer (an awaitable
    on :class:`AsyncLbbClient`)."""

    def __init__(self, client: _BaseLbbClient, sandbox: str) -> None:
        self._client = client
        self._sandbox = sandbox

    def create(
        self,
        *,
        argv: Sequence[str] | None = None,
        run: Mapping[str, Any] | None = None,
        cwd: str | None = None,
        env: Mapping[str, str] | None = None,
        stdin: str | None = None,
        timeout_ms: int | None = None,
        wait_ms: int = SANDBOX_WAIT_MS_MAX,
        idempotency_key: str | None = None,
        options: RequestOptions | None = None,
    ) -> Any:
        """Start a command (``POST /v1/sandboxes/commands``) and wait up to
        ``wait_ms`` for it to end. ``argv`` is the program and its arguments (a
        shell is explicit: ``["bash", "-lc", "..."]``); ``run`` is
        ``{"code", "input", "config"}`` for the template's harness. The call
        carries an ``Idempotency-Key`` (made when you give none): a retry
        answers the same command and never runs it twice. A command whose
        outcome cannot be settled ends ``lost`` and is not run again."""
        body = _drop_none(
            {
                "argv": list(argv) if argv is not None else None,
                "run": dict(run) if run is not None else None,
                "cwd": cwd,
                "env": dict(env) if env is not None else None,
                "stdin": stdin,
                "timeout_ms": timeout_ms,
                "wait_ms": wait_ms,
            }
        )
        return self._client._request(
            "POST",
            "/v1/sandboxes/commands",
            params={"sandbox": self._sandbox},
            body=body,
            idempotency_key=idempotency_key
            or self._client.idempotency_key("sandboxes.commands.create"),
            options=options,
        )

    def list(
        self,
        *,
        status: str | None = None,
        limit: int | None = None,
        cursor: str | None = None,
    ) -> Any:
        """One page of commands, the newest first."""
        return self._client._request(
            "GET",
            "/v1/sandboxes/commands",
            params=_drop_none(
                {
                    "sandbox": self._sandbox,
                    "status": status,
                    "limit": limit,
                    "cursor": cursor,
                }
            ),
        )

    def retrieve(self, id: str) -> Any:
        """One command: its status and, once it ended, its exit code, output
        tails and result."""
        return self._client._request(
            "GET",
            "/v1/sandboxes/commands/get",
            params={"sandbox": self._sandbox, "id": id},
        )

    def logs(
        self,
        id: str,
        *,
        stream: str | None = None,
        offset: int | None = None,
        limit: int | None = None,
    ) -> Any:
        """One page of a command's output by byte offset. The server keeps the
        first 256 KiB and the last 768 KiB of each stream; ``gap`` marks what
        lies between them."""
        return self._client._request(
            "GET",
            "/v1/sandboxes/commands/logs",
            params=_drop_none(
                {
                    "sandbox": self._sandbox,
                    "id": id,
                    "stream": stream,
                    "offset": offset,
                    "limit": limit,
                }
            ),
        )

    def cancel(self, id: str, *, grace_ms: int | None = None) -> Any:
        """End a command: ``SIGTERM`` to its processes, then ``SIGKILL`` after
        ``grace_ms`` (default 5,000). A repeat on an ended command answers its
        record."""
        return self._client._request(
            "POST",
            "/v1/sandboxes/commands/cancel",
            params={"sandbox": self._sandbox, "id": id},
            body={} if grace_ms is None else {"grace_ms": grace_ms},
            options={"retry": True},
        )


class _SandboxHandleBase:
    """A sandbox you hold: its files, its commands and its life."""

    #: The kind of ``files`` the handle has: the sync and the async handle each
    #: set their own (``read_text`` is a call or a coroutine).
    _files_type: type[_SandboxFiles] = _SandboxFiles

    def __init__(self, client: _BaseLbbClient, record: dict[str, Any]) -> None:
        self._client = client
        #: The last record the SDK read. :meth:`refresh` reads it again.
        self.record = record
        self.files = self._files_type(client, record["id"])
        self.commands = _SandboxCommands(client, record["id"])

    @property
    def id(self) -> str:
        """The sandbox id (``sbx_``)."""
        return str(self.record["id"])

    def run(
        self,
        *,
        code: str | None | _Unset = UNSET,
        input: Any = UNSET,
        config: Any = UNSET,
        timeout_ms: int | None = None,
        wait_ms: int = SANDBOX_WAIT_MS_MAX,
        cwd: str | None = None,
        idempotency_key: str | None = None,
        options: RequestOptions | None = None,
    ) -> Any:
        """Run ``run(input, config)`` on this sandbox: ``commands.create(run=...)``.
        Answers the command record, whose ``result`` is what ``run`` returned.
        The sandbox keeps its files between calls, so a later call sees what an
        earlier one wrote."""
        return self.commands.create(
            run=_run_body(code=code, input=input, config=config, timeout_ms=None),
            timeout_ms=timeout_ms,
            wait_ms=wait_ms,
            cwd=cwd,
            idempotency_key=idempotency_key,
            options=options,
        )

    def _params(self) -> dict[str, Any]:
        return {"id": self.id}


class _SyncSandboxHandle(_SandboxHandleBase):
    _files_type = _SyncSandboxFiles
    files: _SyncSandboxFiles

    def refresh(self) -> dict[str, Any]:
        """Read the sandbox again."""
        self.record = self._client._request("GET", "/v1/sandboxes/get", params=self._params())
        return self.record

    def stop(self) -> dict[str, Any]:
        """Stop the VM and keep the disk. A repeat answers the sandbox as it is."""
        self.record = self._client._request(
            "POST", "/v1/sandboxes/stop", params=self._params(), options={"retry": True}
        )
        return self.record

    def resume(self) -> dict[str, Any]:
        """Start a stopped sandbox from its disk."""
        self.record = self._client._request(
            "POST", "/v1/sandboxes/resume", params=self._params(), options={"retry": True}
        )
        return self.record

    def update(
        self,
        *,
        auto_stop_seconds: int | None = None,
        metadata: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        """Change ``auto_stop_seconds`` or the whole ``metadata``."""
        self.record = self._client._request(
            "POST",
            "/v1/sandboxes/update",
            params=self._params(),
            body=_drop_none(
                {
                    "auto_stop_seconds": auto_stop_seconds,
                    "metadata": dict(metadata) if metadata is not None else None,
                }
            ),
        )
        return self.record

    def delete(self) -> dict[str, Any]:
        """Destroy the VM and its disk. A repeat answers the sandbox as it is."""
        self.record = self._client._request(
            "DELETE", "/v1/sandboxes", params=self._params(), options={"retry": True}
        )
        return self.record


class _AsyncSandboxHandle(_SandboxHandleBase):
    _files_type = _AsyncSandboxFiles
    files: _AsyncSandboxFiles

    async def refresh(self) -> dict[str, Any]:
        """Read the sandbox again."""
        self.record = await self._client._request(
            "GET", "/v1/sandboxes/get", params=self._params()
        )
        return self.record

    async def stop(self) -> dict[str, Any]:
        """Stop the VM and keep the disk. A repeat answers the sandbox as it is."""
        self.record = await self._client._request(
            "POST", "/v1/sandboxes/stop", params=self._params(), options={"retry": True}
        )
        return self.record

    async def resume(self) -> dict[str, Any]:
        """Start a stopped sandbox from its disk."""
        self.record = await self._client._request(
            "POST", "/v1/sandboxes/resume", params=self._params(), options={"retry": True}
        )
        return self.record

    async def update(
        self,
        *,
        auto_stop_seconds: int | None = None,
        metadata: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        """Change ``auto_stop_seconds`` or the whole ``metadata``."""
        self.record = await self._client._request(
            "POST",
            "/v1/sandboxes/update",
            params=self._params(),
            body=_drop_none(
                {
                    "auto_stop_seconds": auto_stop_seconds,
                    "metadata": dict(metadata) if metadata is not None else None,
                }
            ),
        )
        return self.record

    async def delete(self) -> dict[str, Any]:
        """Destroy the VM and its disk. A repeat answers the sandbox as it is."""
        self.record = await self._client._request(
            "DELETE", "/v1/sandboxes", params=self._params(), options={"retry": True}
        )
        return self.record


class _SandboxTemplates:
    """Saved templates: each save of a name is a new immutable version."""

    def __init__(self, client: _BaseLbbClient) -> None:
        self._client = client

    def create(
        self,
        *,
        name: str,
        base: str | None = None,
        code: str | None = None,
        limits: Mapping[str, int] | None = None,
        timeout_ms: int | None = None,
        config: Any = UNSET,
        idempotency_key: str | None = None,
        options: RequestOptions | None = None,
    ) -> Any:
        """Save a new version of a template (``POST /v1/sandboxes/templates``).
        ``name`` is 1 to 64 of ``a-z``, ``0-9``, ``.``, ``_`` and ``-``;
        ``base`` is ``lbb/python`` (the default), ``lbb/node`` or ``lbb/shell``;
        ``limits`` has ``vcpus``, ``memory_mib`` and ``disk_mib``; ``timeout_ms``
        is the default command timeout and ``config`` the default config of a
        ``run``."""
        body = _drop_none(
            {
                "name": name,
                "base": base,
                "code": code,
                "limits": dict(limits) if limits is not None else None,
                "timeout_ms": timeout_ms,
            }
        )
        if _given(config):
            body["config"] = config
        return self._client._request(
            "POST",
            "/v1/sandboxes/templates",
            body=body,
            idempotency_key=idempotency_key
            or self._client.idempotency_key("sandboxes.templates.create"),
            options=options,
        )

    def list(self, *, limit: int | None = None, cursor: str | None = None) -> Any:
        """One page of saved templates, the newest version first."""
        return self._client._request(
            "GET",
            "/v1/sandboxes/templates",
            params=_drop_none({"limit": limit, "cursor": cursor}),
        )

    def retrieve(self, id: str) -> Any:
        """One version by its ``sbt_`` id, or a base template (``sbt_lbb_python``)."""
        return self._client._request("GET", "/v1/sandboxes/templates/get", params={"id": id})


class _SandboxesBase:
    """The sandboxes of the graph in scope."""

    def __init__(self, client: _BaseLbbClient) -> None:
        self._client = client
        #: Saved templates.
        self.templates = _SandboxTemplates(client)

    def _create_body(
        self,
        *,
        template: str | None,
        limits: Mapping[str, int] | None,
        auto_stop_seconds: int | None,
        expires_in_seconds: int | None,
        metadata: Mapping[str, str] | None,
    ) -> dict[str, Any]:
        return _create_body(
            template=template,
            limits=limits,
            auto_stop_seconds=auto_stop_seconds,
            expires_in_seconds=expires_in_seconds,
            metadata=metadata,
        )

    def _spec(
        self, spec: Mapping[str, Any] | None, overrides: Mapping[str, Any]
    ) -> dict[str, Any]:
        """A create body from a spec mapping (``template``, ``limits``,
        ``auto_stop_seconds``, ``expires_in_seconds``, ``metadata``) and
        keyword arguments of the same names."""
        merged = {**(spec or {}), **{k: v for k, v in overrides.items() if v is not None}}
        unknown = set(merged) - {
            "template",
            "limits",
            "auto_stop_seconds",
            "expires_in_seconds",
            "metadata",
        }
        if unknown:
            raise TypeError(f"unknown sandbox spec field(s): {', '.join(sorted(unknown))}")
        return self._create_body(
            template=merged.get("template"),
            limits=merged.get("limits"),
            auto_stop_seconds=merged.get("auto_stop_seconds"),
            expires_in_seconds=merged.get("expires_in_seconds"),
            metadata=merged.get("metadata"),
        )

    def retrieve(self, id: str, *, options: RequestOptions | None = None) -> Any:
        """One sandbox."""
        return self._client._request(
            "GET", "/v1/sandboxes/get", params={"id": id}, options=options
        )

    def list(
        self,
        *,
        status: str | None = None,
        template: str | None = None,
        workflow: str | None = None,
        created_by: str | None = None,
        kind: str | None = None,
        limit: int | None = None,
        cursor: str | None = None,
    ) -> Any:
        """One page of sandboxes, the newest first. ``kind`` is ``run`` for the
        sandboxes of an L1 call and ``sandbox`` for the others."""
        return self._client._request(
            "GET",
            "/v1/sandboxes",
            params=_drop_none(
                {
                    "status": status,
                    "template": template,
                    "workflow": workflow,
                    "created_by": created_by,
                    "kind": kind,
                    "limit": limit,
                    "cursor": cursor,
                }
            ),
        )

    def update(
        self,
        id: str,
        *,
        auto_stop_seconds: int | None = None,
        metadata: Mapping[str, str] | None = None,
    ) -> Any:
        """Change ``auto_stop_seconds`` or the whole ``metadata`` of a sandbox."""
        return self._client._request(
            "POST",
            "/v1/sandboxes/update",
            params={"id": id},
            body=_drop_none(
                {
                    "auto_stop_seconds": auto_stop_seconds,
                    "metadata": dict(metadata) if metadata is not None else None,
                }
            ),
        )

    def stop(self, id: str) -> Any:
        """Stop a sandbox's VM and keep its disk."""
        return self._client._request(
            "POST", "/v1/sandboxes/stop", params={"id": id}, options={"retry": True}
        )

    def resume(self, id: str) -> Any:
        """Start a stopped sandbox from its disk."""
        return self._client._request(
            "POST", "/v1/sandboxes/resume", params={"id": id}, options={"retry": True}
        )

    def delete(self, id: str) -> Any:
        """Destroy a sandbox's VM and disk."""
        return self._client._request(
            "DELETE", "/v1/sandboxes", params={"id": id}, options={"retry": True}
        )


class _SyncScoped:
    """``with client.sandboxes.scoped(spec) as sb``: the sandbox is destroyed
    when the block ends, also after an error."""

    def __init__(self, owner: _SyncSandboxes, body: dict[str, Any], key: str | None) -> None:
        self._owner = owner
        self._body = body
        self._key = key
        self._handle: _SyncSandboxHandle | None = None

    def __enter__(self) -> _SyncSandboxHandle:
        self._handle = self._owner._create_handle(self._body, self._key)
        return self._handle

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        handle = self._handle
        if handle is None:
            return
        try:
            handle.delete()
        except LbbError as error:
            # A failure to destroy never hides the error of the block; a
            # sandbox that is not destroyed ends at its ``expires_at``.
            if exc is None and error.status_code != 404:
                raise
        except Exception:
            if exc is None:
                raise


class _AsyncScoped:
    """``async with client.sandboxes.scoped(spec) as sb``: the sandbox is
    destroyed when the block ends, also after an error."""

    def __init__(self, owner: _AsyncSandboxes, body: dict[str, Any], key: str | None) -> None:
        self._owner = owner
        self._body = body
        self._key = key
        self._handle: _AsyncSandboxHandle | None = None

    async def __aenter__(self) -> _AsyncSandboxHandle:
        self._handle = await self._owner._create_handle(self._body, self._key)
        return self._handle

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        handle = self._handle
        if handle is None:
            return
        try:
            await handle.delete()
        except LbbError as error:
            if exc is None and error.status_code != 404:
                raise
        except Exception:
            if exc is None:
                raise


class _SyncSandboxes(_SandboxesBase):
    """Sandboxes: :meth:`run` (one call), :meth:`scoped` and :meth:`create` (a
    handle), and the resource calls."""

    def handle(self, record: Mapping[str, Any]) -> _SyncSandboxHandle:
        """The handle of a sandbox that exists, from its record (the answer of
        :meth:`retrieve`, or of a list). It makes no request: files, commands
        and the life of the sandbox are on the handle, as on the one
        :meth:`create` answers. The handle holds a copy of the record;
        :meth:`refresh <_SyncSandboxHandle.refresh>` reads it again."""
        return _SyncSandboxHandle(self._client, dict(record))

    def _create_handle(self, body: dict[str, Any], key: str | None) -> _SyncSandboxHandle:
        record = self._client._request(
            "POST",
            "/v1/sandboxes",
            body=body,
            idempotency_key=key or self._client.idempotency_key("sandboxes.create"),
        )
        return _SyncSandboxHandle(self._client, record)

    def create(
        self,
        spec: Mapping[str, Any] | None = None,
        *,
        template: str | None = None,
        limits: Mapping[str, int] | None = None,
        auto_stop_seconds: int | None = None,
        expires_in_seconds: int | None = None,
        metadata: Mapping[str, str] | None = None,
        idempotency_key: str | None = None,
    ) -> _SyncSandboxHandle:
        """Level 3: make a sandbox that you keep. Stop, resume, files and
        commands are on the handle. It lives until you delete it or it expires
        (``expires_in_seconds``, one hour by default)."""
        body = self._spec(
            spec,
            {
                "template": template,
                "limits": limits,
                "auto_stop_seconds": auto_stop_seconds,
                "expires_in_seconds": expires_in_seconds,
                "metadata": metadata,
            },
        )
        return self._create_handle(body, idempotency_key)

    def scoped(
        self,
        spec: Mapping[str, Any] | None = None,
        *,
        template: str | None = None,
        limits: Mapping[str, int] | None = None,
        auto_stop_seconds: int | None = None,
        expires_in_seconds: int | None = None,
        metadata: Mapping[str, str] | None = None,
        idempotency_key: str | None = None,
    ) -> _SyncScoped:
        """Level 3: ``with client.sandboxes.scoped({"template": "normalize"}) as
        sb:`` makes a sandbox, hands over its handle and destroys it when the
        block ends, also after an error. The create carries an
        ``Idempotency-Key``, so a retry makes one sandbox."""
        body = self._spec(
            spec,
            {
                "template": template,
                "limits": limits,
                "auto_stop_seconds": auto_stop_seconds,
                "expires_in_seconds": expires_in_seconds,
                "metadata": metadata,
            },
        )
        return _SyncScoped(self, body, idempotency_key)

    def run(
        self,
        *,
        code: str | None | _Unset = UNSET,
        input: Any = UNSET,
        config: Any = UNSET,
        template: str | None = None,
        limits: Mapping[str, int] | None = None,
        auto_stop_seconds: int | None = None,
        expires_in_seconds: int | None = None,
        metadata: Mapping[str, str] | None = None,
        timeout_ms: int | None = None,
        wait_ms: int = SANDBOX_WAIT_MS_MAX,
        max_wait_ms: int = 600_000,
        lost_retries: int = 2,
        idempotency_key: str | None = None,
        options: RequestOptions | None = None,
    ) -> SandboxRunResult:
        """Level 1 and 2: run code once. The call makes a sandbox, runs
        ``run(input, config)`` in it, and the sandbox is destroyed when the run
        ends. ``template`` names a saved template instead of passing ``code``.
        ``timeout_ms`` is the command's own timeout.

        The call sends an ``Idempotency-Key``: the one you give, else one the
        SDK makes. A retry after a ``5xx`` answers the same record. A run whose
        VM the host lost (status ``lost``) goes again in a new sandbox under the
        key ``<key>.2``, at most ``lost_retries`` times, because a lost VM took
        its effects with it. ``lost_retries`` is 0 to 2 (default 2, which is also
        the most: the design allows a fresh run twice again at most, so a larger
        number is 2). A run that failed in your code is not run again.

        ``max_wait_ms`` (default 10 minutes) is how long the SDK waits for a run
        that outlives ``wait_ms`` before it raises :class:`SandboxWaitError`. It
        is **per attempt**: a run that goes again gets a new wait.

        The reads of that wait carry your ``options`` (timeouts, headers), as the
        create does.

        Check ``ok`` before you use ``result``: a run whose code raised is a
        result with ``exit_code`` 1, not an error."""
        body = {
            **self._create_body(
                template=template,
                limits=limits,
                auto_stop_seconds=auto_stop_seconds,
                expires_in_seconds=expires_in_seconds,
                metadata=metadata,
            ),
            "run": _run_body(code=code, input=input, config=config, timeout_ms=timeout_ms),
            "wait_ms": wait_ms,
        }
        base_key = idempotency_key or self._client.idempotency_key("sandboxes.run")
        max_runs = 1 + _lost_retries(lost_retries)
        attempt = 1
        while True:
            key = base_key if attempt == 1 else f"{base_key}.{attempt}"
            sandbox: dict[str, Any] = self._client._request(
                "POST", "/v1/sandboxes", body=body, idempotency_key=key, options=options
            )
            began = time.monotonic()
            pause = 0.25
            while not _run_is_final(sandbox):
                waited_ms = (time.monotonic() - began) * 1000
                if waited_ms >= max_wait_ms:
                    raise SandboxWaitError(sandbox, waited_ms)
                time.sleep(pause)
                pause = min(pause * 2, 2.0)
                # The reads of the wait carry the caller's options (timeouts,
                # headers), as the create does.
                sandbox = self.retrieve(str(sandbox["id"]), options=options)
            if (sandbox.get("run") or {}).get("status") == "lost" and attempt < max_runs:
                attempt += 1
                continue
            return _result_of(sandbox)


class _AsyncSandboxes(_SandboxesBase):
    """Sandboxes: :meth:`run` (one call), :meth:`scoped` and :meth:`create` (a
    handle), and the resource calls. Every method returns an awaitable, except
    :meth:`handle` and :meth:`scoped`, which make no request."""

    def handle(self, record: Mapping[str, Any]) -> _AsyncSandboxHandle:
        """The handle of a sandbox that exists, from its record (the answer of
        :meth:`retrieve`, or of a list). It makes no request, so it is a plain
        call and no coroutine: files, commands and the life of the sandbox are on
        the handle, as on the one :meth:`create` answers. The handle holds a copy
        of the record; :meth:`refresh <_AsyncSandboxHandle.refresh>` reads it
        again."""
        return _AsyncSandboxHandle(self._client, dict(record))

    async def _create_handle(self, body: dict[str, Any], key: str | None) -> _AsyncSandboxHandle:
        record = await self._client._request(
            "POST",
            "/v1/sandboxes",
            body=body,
            idempotency_key=key or self._client.idempotency_key("sandboxes.create"),
        )
        return _AsyncSandboxHandle(self._client, record)

    async def create(
        self,
        spec: Mapping[str, Any] | None = None,
        *,
        template: str | None = None,
        limits: Mapping[str, int] | None = None,
        auto_stop_seconds: int | None = None,
        expires_in_seconds: int | None = None,
        metadata: Mapping[str, str] | None = None,
        idempotency_key: str | None = None,
    ) -> _AsyncSandboxHandle:
        """Level 3: make a sandbox that you keep. Stop, resume, files and
        commands are on the handle. It lives until you delete it or it expires
        (``expires_in_seconds``, one hour by default)."""
        body = self._spec(
            spec,
            {
                "template": template,
                "limits": limits,
                "auto_stop_seconds": auto_stop_seconds,
                "expires_in_seconds": expires_in_seconds,
                "metadata": metadata,
            },
        )
        return await self._create_handle(body, idempotency_key)

    def scoped(
        self,
        spec: Mapping[str, Any] | None = None,
        *,
        template: str | None = None,
        limits: Mapping[str, int] | None = None,
        auto_stop_seconds: int | None = None,
        expires_in_seconds: int | None = None,
        metadata: Mapping[str, str] | None = None,
        idempotency_key: str | None = None,
    ) -> _AsyncScoped:
        """Level 3: ``async with client.sandboxes.scoped({"template":
        "normalize"}) as sb:`` makes a sandbox, hands over its handle and
        destroys it when the block ends, also after an error. The create
        carries an ``Idempotency-Key``, so a retry makes one sandbox."""
        body = self._spec(
            spec,
            {
                "template": template,
                "limits": limits,
                "auto_stop_seconds": auto_stop_seconds,
                "expires_in_seconds": expires_in_seconds,
                "metadata": metadata,
            },
        )
        return _AsyncScoped(self, body, idempotency_key)

    async def run(
        self,
        *,
        code: str | None | _Unset = UNSET,
        input: Any = UNSET,
        config: Any = UNSET,
        template: str | None = None,
        limits: Mapping[str, int] | None = None,
        auto_stop_seconds: int | None = None,
        expires_in_seconds: int | None = None,
        metadata: Mapping[str, str] | None = None,
        timeout_ms: int | None = None,
        wait_ms: int = SANDBOX_WAIT_MS_MAX,
        max_wait_ms: int = 600_000,
        lost_retries: int = 2,
        idempotency_key: str | None = None,
        options: RequestOptions | None = None,
    ) -> SandboxRunResult:
        """Level 1 and 2: run code once, as the sync client's ``run`` does."""
        body = {
            **self._create_body(
                template=template,
                limits=limits,
                auto_stop_seconds=auto_stop_seconds,
                expires_in_seconds=expires_in_seconds,
                metadata=metadata,
            ),
            "run": _run_body(code=code, input=input, config=config, timeout_ms=timeout_ms),
            "wait_ms": wait_ms,
        }
        base_key = idempotency_key or self._client.idempotency_key("sandboxes.run")
        max_runs = 1 + _lost_retries(lost_retries)
        attempt = 1
        while True:
            key = base_key if attempt == 1 else f"{base_key}.{attempt}"
            sandbox: dict[str, Any] = await self._client._request(
                "POST", "/v1/sandboxes", body=body, idempotency_key=key, options=options
            )
            began = time.monotonic()
            pause = 0.25
            while not _run_is_final(sandbox):
                waited_ms = (time.monotonic() - began) * 1000
                if waited_ms >= max_wait_ms:
                    raise SandboxWaitError(sandbox, waited_ms)
                await asyncio.sleep(pause)
                pause = min(pause * 2, 2.0)
                sandbox = await self.retrieve(str(sandbox["id"]), options=options)
            if (sandbox.get("run") or {}).get("status") == "lost" and attempt < max_runs:
                attempt += 1
                continue
            return _result_of(sandbox)
