"""``client.workflows``: instances of a workflow definition, and the blobs of
the workflow engine (the TypeScript ``WorkflowNamespace``)."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Literal

from .. import models
from ._worker import WorkflowDefinition, _copy

if TYPE_CHECKING:
    from .._async_client import AsyncLbbClient

_BASE = "/v1/workflows"
_BLOB_MAX_BYTES = 8 * 1024 * 1024


class WorkflowHandle:
    """One workflow instance: its status, its inbox and its controls."""

    def __init__(self, client: AsyncLbbClient, id: str) -> None:
        self._client = client
        self.id = id

    async def status(self) -> models.WorkflowInstance:
        """The instance (``GET /v1/workflows/instances/get``)."""
        return await self._client._model_request(
            models.WorkflowInstance, "GET", f"{_BASE}/instances/get", params={"id": self.id}
        )

    async def send(
        self,
        message: Any,
        *,
        id: str,
        origin: models.WorkflowMessageOrigin | Mapping[str, Any] | None = None,
    ) -> models.WorkflowTurn:
        """Append ``message`` to the inbox (``POST /v1/workflows/instances/message``).

        Reuse ``id`` when you send the same message again, also after a
        restart: a repeated id answers the turn it admitted. ``origin`` says
        where the message came from; it is stored with the turn and shown in
        the inbox.
        """
        body = models.WorkflowMessageRequest(
            workflow_id=self.id,
            id=id,
            message=_copy(message, "message"),
            origin=models.WorkflowMessageOrigin.model_validate(origin) if origin else None,
        )
        return await self._client._model_request(
            models.WorkflowTurn,
            "POST",
            f"{_BASE}/instances/message",
            body=body,
            options={"retry": True},
        )

    async def signal(self, name: str, value: Any, *, turn: int, id: str) -> models.WorkflowTurn:
        """Deliver a signal to turn ``turn``, also while it waits
        (``POST /v1/workflows/turns/signal``). The same ``id`` again is
        delivered once."""
        body = models.WorkflowTurnSignalRequest(
            workflow_id=self.id, turn=turn, id=id, name=name, value=_copy(value, f'signal "{name}"')
        )
        return await self._client._model_request(
            models.WorkflowTurn, "POST", f"{_BASE}/turns/signal", body=body, options={"retry": True}
        )

    async def inbox(
        self, *, after: int | None = None, limit: int | None = None, payloads: bool = False
    ) -> models.WorkflowInboxResponse:
        """The messages that wait in the inbox, oldest first
        (``GET /v1/workflows/instances/inbox``); payloads only with
        ``payloads=True``."""
        return await self._client._model_request(
            models.WorkflowInboxResponse,
            "GET",
            f"{_BASE}/instances/inbox",
            params={
                "id": self.id,
                "after": after,
                "limit": limit,
                "payloads": "true" if payloads else None,
            },
        )

    async def history(
        self, *, after: int | None = None, limit: int | None = None
    ) -> models.WorkflowTurnHistoryResponse:
        """The finished turns (``GET /v1/workflows/instances/history``)."""
        return await self._client._model_request(
            models.WorkflowTurnHistoryResponse,
            "GET",
            f"{_BASE}/instances/history",
            params={"id": self.id, "after": after, "limit": limit},
        )

    async def _control(
        self, action: str, turn: int | None = None
    ) -> models.WorkflowInstance:
        # The turn number keeps a repeated command off the next turn.
        body = models.WorkflowInstanceControlRequest(
            workflow_id=self.id, action=models.WorkflowInstanceAction(action), turn=turn
        )
        return await self._client._model_request(
            models.WorkflowInstance,
            "POST",
            f"{_BASE}/instances/control",
            body=body,
            options={"retry": False},
        )

    async def pause(self) -> models.WorkflowInstance:
        """Stop claiming turns of this instance."""
        return await self._control("pause")

    async def resume(self) -> models.WorkflowInstance:
        """Claim turns again; a turn that waits after a failure runs now."""
        return await self._control("resume")

    async def retry(self, turn: int) -> models.WorkflowInstance:
        """Run the failed turn ``turn`` again."""
        return await self._control("retry", turn)

    async def skip(self, turn: int) -> models.WorkflowInstance:
        """Leave the failed turn ``turn`` in the history and go on."""
        return await self._control("skip", turn)

    async def cancel_turn(self, turn: int) -> models.WorkflowInstance:
        """Cancel the unfinished turn ``turn``."""
        return await self._control("cancel_turn", turn)

    async def wake(self) -> models.WorkflowInstance:
        """End a wait of the current turn that counts no attempt
        (``retry_at_ms``): a run now."""
        return await self._control("wake")


class WorkflowBlobsNamespace:
    """``client.workflows.blobs``: blobs of the workflow engine, large step
    outputs and shared reads, keyed by their SHA-256."""

    def __init__(self, client: AsyncLbbClient) -> None:
        self._client = client

    async def put(self, data: bytes | str, *, workflow_id: str) -> models.WorkflowBlob:
        """Store ``data`` (at most 8 MiB) as a blob of the identity
        ``workflow_id``'s workflow (``POST /v1/workflows/blobs``), and answer
        its key, ``<workflow>/<sha256>``.

        Name the key in a step's output, the state or a message: a blob stays
        while a live identity names it, and at least a day after its last
        write. The same bytes again answer the same key, so a failed call is
        retried.
        """
        content = data.encode() if isinstance(data, str) else bytes(data)
        if len(content) > _BLOB_MAX_BYTES:
            raise ValueError("a blob holds at most 8 MiB")
        return await self._client._model_request(
            models.WorkflowBlob,
            "POST",
            f"{_BASE}/blobs",
            params={"workflow_id": workflow_id},
            content=content,
            content_type="application/octet-stream",
            options={"retry": True},
        )

    async def get(self, key: str) -> bytes:
        """The bytes of blob ``key`` (``GET /v1/workflows/blobs``)."""
        return await self._client._bytes_request("GET", f"{_BASE}/blobs", params={"key": key})

    async def get_text(self, key: str) -> str:
        """The text of blob ``key``, decoded as UTF-8."""
        return (await self.get(key)).decode()

    async def put_json(self, value: Any, *, workflow_id: str) -> models.WorkflowBlob:
        """Store a JSON value as a blob: :meth:`put` of its text."""
        return await self.put(
            json.dumps(_copy(value), separators=(",", ":"), ensure_ascii=False),
            workflow_id=workflow_id,
        )

    async def get_json(self, key: str) -> Any:
        """The JSON value of blob ``key``."""
        return json.loads(await self.get_text(key))


class WorkflowNamespace:
    """``client.workflows`` of an :class:`~lbb.AsyncLbbClient`: start an
    instance of a workflow definition, address one by its id, and the blobs of
    the workflow engine."""

    def __init__(self, client: AsyncLbbClient) -> None:
        self._client = client
        #: Blobs: large step outputs and shared reads, keyed by their SHA-256.
        self.blobs = WorkflowBlobsNamespace(client)

    def handle(self, id: str) -> WorkflowHandle:
        """The instance ``id``."""
        return WorkflowHandle(self._client, id)

    async def start(
        self,
        definition: WorkflowDefinition[Any, Any],
        id: str,
        *,
        lease_ms: int = 60_000,
        max_attempts: int = 3,
        on_exhausted: models.WorkflowOnExhausted | Literal["block", "skip"] | None = None,
    ) -> WorkflowHandle:
        """Create the instance ``id`` of ``definition`` with its initial state
        (``POST /v1/workflows/instances``), and return its handle. The same
        request again answers the instance it created.

        ``on_exhausted`` says what a turn that failed for good does: ``block``
        (the default) holds the inbox until a person retries or skips it;
        ``skip`` keeps the failed turn in the history and goes on.
        """
        body = models.WorkflowInstanceCreateRequest(
            id=id,
            workflow_type=definition.name,
            version=definition.version,
            state=definition.initial_state,
            lease_ms=lease_ms,
            max_attempts=max_attempts,
            on_exhausted=models.WorkflowOnExhausted(on_exhausted) if on_exhausted else None,
        )
        await self._client._request(
            "POST", f"{_BASE}/instances", body=body, options={"retry": True}
        )
        return self.handle(id)


__all__ = ["WorkflowBlobsNamespace", "WorkflowHandle", "WorkflowNamespace"]
