"""Hosted integrations for a developer's end customers.

One graph per end customer in a developer stack, called from the developer's
backend with a stack API key. The routes live on the integrations API
(``integrations_url``, ``https://api.littlebigbrain.com`` by default); their
contract is ``contracts/integrations-openapi.json``. Ontology suggestions stay
on the data plane (``base_url``).

``LbbClient.integrations`` and ``AsyncLbbClient.integrations`` have the same
methods; the async ones are coroutines. Errors raise :class:`lbb.LbbError`
with ``status_code``, ``code``, ``details`` and, on a ``429``,
``retry_after_seconds``.
"""

from __future__ import annotations

import time
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any, Final, Literal, TypedDict, cast

from . import models
from ._client_base import Body, RequestOptions, _coerce_body

if TYPE_CHECKING:
    from ._client_base import _BaseLbbClient

IntegrationStatusKind = Literal[
    "syncing",
    "working",
    "starting",
    "delayed",
    "retrying",
    "attention",
    "review",
    "paused",
    "new",
    "current",
]


class IntegrationStatus(TypedDict):
    """One status per connection: ``kind`` for code, ``detail`` for a person."""

    kind: IntegrationStatusKind
    detail: str


class IntegrationRun(TypedDict):
    """The connection's last run."""

    status: Literal["idle", "running", "blocked"]
    started_at_ms: int | None
    finished_at_ms: int | None
    records: int
    deleted: int
    message: str | None


class IntegrationTotals(TypedDict):
    """Records written and deleted over every run."""

    records: int
    deleted: int
    runs: int


class IntegrationFitStreams(TypedDict):
    total: int
    mapped: int
    waiting: int
    ignored: int


class IntegrationFit(TypedDict):
    """How the source fits the graph's ontology."""

    checked_at_ms: int | None
    streams: IntegrationFitStreams
    open_suggestions: int


class IntegrationTurn(TypedDict):
    """The connection's current turn."""

    number: int
    type: str | None
    status: str
    attempt: int
    error: str | None
    ready_at_ms: int


class IntegrationConnection(TypedDict):
    """One connection in a list."""

    id: str
    kind: str
    label: str
    status: IntegrationStatus
    run: IntegrationRun
    totals: IntegrationTotals
    fit: IntegrationFit | None


class IntegrationConnectionDetail(IntegrationConnection):
    """One connection with its settings (never its credentials) and turn."""

    graph: str
    account: str | None
    ontology_mode: Literal["auto", "review"]
    every_ms: int | None
    config: dict[str, Any]
    paused: bool
    turn: IntegrationTurn | None


class IntegrationReclaim(TypedDict):
    """A reclaim pass of a deleted graph. ``apply`` deletes; ``plan`` only counts."""

    branch: str
    job_id: str
    mode: str
    not_before: str
    retired_epoch: int


class _IntegrationCredentialFieldBase(TypedDict):
    name: str
    label: str
    secret: bool


class IntegrationCredentialField(_IntegrationCredentialFieldBase, total=False):
    """A value an end customer enters to connect. ``name`` is its key in
    ``credentials``, for example ``HUBSPOT_TOKEN``."""

    help: str
    optional: bool


class IntegrationConnector(TypedDict):
    """A connector of the catalog. It holds no secret value."""

    kind: str
    label: str
    description: str
    credentialFields: list[IntegrationCredentialField]
    settings: list[dict[str, Any]]
    starter: dict[str, Any]


class IntegrationConnectorsAnswer(TypedDict):
    ok: Literal[True]
    connectors: list[IntegrationConnector]


class IntegrationCreateAnswer(TypedDict):
    ok: Literal[True]
    id: str
    graph: str
    kind: str
    status: IntegrationStatus


class IntegrationListAnswer(TypedDict):
    ok: Literal[True]
    graph: str
    connections: list[IntegrationConnection]


class IntegrationConnectionAnswer(TypedDict):
    ok: Literal[True]
    connection: IntegrationConnectionDetail


class IntegrationRefAnswer(TypedDict):
    ok: Literal[True]
    id: str
    graph: str


class IntegrationSyncTurn(TypedDict):
    number: int
    status: str
    message_id: str


class IntegrationSyncAnswer(TypedDict):
    ok: Literal[True]
    id: str
    graph: str
    turn: IntegrationSyncTurn


class IntegrationStatusAnswer(TypedDict):
    ok: Literal[True]
    id: str
    graph: str
    status: IntegrationStatus


class IntegrationDeleteAnswer(TypedDict):
    ok: Literal[True]
    id: str
    graph: str
    #: False when no connection had the id, or a retry finished the delete.
    deleted: bool
    target_removed: bool


class IntegrationEraseAnswer(TypedDict):
    ok: Literal[True]
    graph: str
    connections_deleted: int
    graph_deleted: bool
    reclaims: list[IntegrationReclaim]


@dataclass(frozen=True)
class IntegrationAcceptResult:
    """What :meth:`IntegrationsNamespace.accept` did."""

    suggestion: models.OntologyChangeSuggestion
    #: The sync turn ``sync=True`` queued. ``None`` without ``sync``, when the
    #: suggestion is not accepted, when no integration filed it, or when it
    #: links identities (records the graph holds already).
    sync: models.WorkflowTurn | None


class _Default(Enum):
    DEFAULT = "default"


#: Leave a field out of the request, so the server's default applies.
_DEFAULT: Final = _Default.DEFAULT

_BASE = "/v1/integrations"
_RETRY: Final[RequestOptions] = {"retry": True}
# A 429 is answered before any work, so it is safe to retry for every route.
_RETRY_RATE_LIMITED: Final[RequestOptions] = {"retry": "rate_limited"}


def _connection_path(connection_id: str) -> str:
    # Connection ids are letters, digits, `_`, `-` and `.`: no escaping needed.
    return f"{_BASE}/connections/{connection_id}"


def _sync_key() -> str:
    """``sync-<ms>-<random>``: an ``Idempotency-Key`` the route accepts."""
    return f"sync-{int(time.time() * 1000)}-{uuid.uuid4().hex[:12]}"


def _queues_sync(suggestion: models.OntologyChangeSuggestion) -> str | None:
    """The connection to send ``sync`` after an accept, as the console does:
    an accepted suggestion an integration filed, which links no identities."""
    origin = suggestion.origin
    links = bool(suggestion.identities) or bool(suggestion.proposed_identities)
    if (
        suggestion.status != models.OntologyChangeSuggestionStatus.accepted
        or links
        or origin.kind != models.SuggestionOriginKind.integration
        or not origin.id
    ):
        return None
    return origin.id


class IntegrationsNamespace:
    """``client.integrations``: connect your end customers' sources to their
    graphs. Each method names the end customer's ``graph``."""

    def __init__(self, client: _BaseLbbClient) -> None:
        self._client = client

    def connectors(self) -> IntegrationConnectorsAnswer:
        """The connector catalog: credential fields, settings and starter per kind."""
        return cast(
            IntegrationConnectorsAnswer,
            self._client._integrations_call("GET", f"{_BASE}/connectors"),
        )

    def create(
        self,
        *,
        graph: str,
        id: str,
        kind: str,
        credentials: Mapping[str, str],
        config: Mapping[str, Any] | None = None,
        every_ms: int | None | _Default = _DEFAULT,
        ontology_mode: Literal["auto", "review"] | None = None,
        starter: Literal["apply", "skip"] | None = None,
        start: bool | None = None,
    ) -> IntegrationCreateAnswer:
        """Create a connection: seal the credentials, apply the connector's
        starter ontology, and send the first sync.

        ``every_ms`` is the sync interval in ms, at least 60,000: one hour when
        left out, and ``None`` syncs only on request. The same request again
        answers success and queues one first sync, so a retry is safe.
        ``409 starter_conflict`` (with ``details["conflicts"]``) and
        ``409 integration_exists`` create nothing.
        """
        body: dict[str, Any] = {
            "graph": graph,
            "id": id,
            "kind": kind,
            "credentials": dict(credentials),
        }
        if config is not None:
            body["config"] = dict(config)
        if not isinstance(every_ms, _Default):
            body["everyMs"] = every_ms
        if ontology_mode is not None:
            body["ontologyMode"] = ontology_mode
        if starter is not None:
            body["starter"] = starter
        if start is not None:
            body["start"] = start
        return cast(
            IntegrationCreateAnswer,
            self._client._integrations_call(
                "POST", f"{_BASE}/connections", body=body, options=_RETRY
            ),
        )

    def list(self, *, graph: str) -> IntegrationListAnswer:
        """Every connection of the graph with its status, last run, totals and fit."""
        return cast(
            IntegrationListAnswer,
            self._client._integrations_call(
                "GET", f"{_BASE}/connections", params={"graph": graph}
            ),
        )

    def get(self, id: str, *, graph: str) -> IntegrationConnectionAnswer:
        """One connection: status, settings, current turn and its error, and fit."""
        return cast(
            IntegrationConnectionAnswer,
            self._client._integrations_call(
                "GET", _connection_path(id), params={"graph": graph}
            ),
        )

    def set_credentials(
        self, id: str, *, graph: str, credentials: Mapping[str, str]
    ) -> IntegrationRefAnswer:
        """Replace the credentials, then check them with a ``configure`` and ``check``."""
        return cast(
            IntegrationRefAnswer,
            self._client._integrations_call(
                "PUT",
                f"{_connection_path(id)}/credentials",
                body={"graph": graph, "credentials": dict(credentials)},
                options=_RETRY_RATE_LIMITED,
            ),
        )

    def set_settings(
        self, id: str, *, graph: str, config: Mapping[str, Any]
    ) -> IntegrationRefAnswer:
        """Replace the connector settings.

        ``409 credentials_required`` when the stored credentials do not open
        under them: replace the credentials first.
        """
        return cast(
            IntegrationRefAnswer,
            self._client._integrations_call(
                "PUT",
                f"{_connection_path(id)}/settings",
                body={"graph": graph, "config": dict(config)},
                options=_RETRY_RATE_LIMITED,
            ),
        )

    def sync(
        self,
        id: str,
        *,
        graph: str,
        full: bool | None = None,
        idempotency_key: str | None = None,
    ) -> IntegrationSyncAnswer:
        """Send the connection a sync. The answer names the queued turn.

        A request with the same ``idempotency_key`` (1 to 100 letters, digits,
        ``_``, ``-`` or ``.``) answers the same turn and sends nothing new. The
        client makes one per call when it is absent, so its own retries send
        one sync. ``full=True`` reads every stream in full.
        """
        body: dict[str, Any] = {"graph": graph}
        if full is not None:
            body["full"] = full
        return cast(
            IntegrationSyncAnswer,
            self._client._integrations_call(
                "POST",
                f"{_connection_path(id)}/sync",
                body=body,
                idempotency_key=idempotency_key or _sync_key(),
            ),
        )

    def pause(self, id: str, *, graph: str) -> IntegrationStatusAnswer:
        """Scheduled syncs and queued work wait until the connection resumes."""
        return cast(
            IntegrationStatusAnswer,
            self._client._integrations_call(
                "POST",
                f"{_connection_path(id)}/pause",
                body={"graph": graph},
                options=_RETRY,
            ),
        )

    def resume(self, id: str, *, graph: str) -> IntegrationStatusAnswer:
        """Queued work and the schedule continue."""
        return cast(
            IntegrationStatusAnswer,
            self._client._integrations_call(
                "POST",
                f"{_connection_path(id)}/resume",
                body={"graph": graph},
                options=_RETRY,
            ),
        )

    def delete(self, id: str, *, graph: str) -> IntegrationDeleteAnswer:
        """Delete the connection's state, sealed credentials and turn history.

        The records it wrote stay in the graph. Deleting again answers
        ``deleted: False``.
        """
        return cast(
            IntegrationDeleteAnswer,
            self._client._integrations_call(
                "DELETE", _connection_path(id), params={"graph": graph}, options=_RETRY
            ),
        )

    def erase(self, graph: str, *, confirm: str) -> IntegrationEraseAnswer:
        """Erase an end customer: delete every connection of the graph, then
        the graph itself. ``confirm`` repeats the graph id. Erasing again
        answers zero counts."""
        return cast(
            IntegrationEraseAnswer,
            self._client._integrations_call(
                "POST",
                f"{_BASE}/graphs/{graph}/erase",
                params={"confirm": confirm},
                options=_RETRY,
            ),
        )

    def suggestions(
        self,
        connection_id: str,
        *,
        graph: str,
        status: str | None = None,
        limit: int | None = None,
    ) -> models.OntologyChangeSuggestionList:
        """The ontology change suggestions a connection filed, from the data
        plane (``base_url``). Pass ``status="open"`` for the ones that wait."""
        return cast(
            models.OntologyChangeSuggestionList,
            self._client._model_request(
                models.OntologyChangeSuggestionList,
                "GET",
                "/v1/ontology/suggestions",
                params={
                    "graph": graph,
                    "origin_kind": "integration",
                    "origin_id": connection_id,
                    "status": status,
                    "limit": limit,
                },
            ),
        )

    def accept(
        self,
        suggestion_id: str,
        *,
        graph: str,
        change: Sequence[Body] | None = None,
        comment: str | None = None,
        author: str | None = None,
        sync: bool = False,
    ) -> IntegrationAcceptResult:
        """Accept a suggestion on the data plane, with an edited ``change``
        when given.

        With ``sync=True``, then send the connection that filed it a sync under
        the message id ``sync-after-<suggestion id>``, so the records that
        waited are written now. Both steps are safe to repeat: after an error,
        call ``accept`` again.
        """
        suggestion = cast(
            models.OntologyChangeSuggestion,
            self._client._model_request(
                models.OntologyChangeSuggestion,
                **_accept_request(suggestion_id, graph, change, comment, author),
            ),
        )
        connection = _queues_sync(suggestion) if sync else None
        if connection is None:
            return IntegrationAcceptResult(suggestion=suggestion, sync=None)
        turn = cast(
            models.WorkflowTurn,
            self._client._model_request(
                models.WorkflowTurn,
                **_sync_after_request(suggestion, connection, graph),
            ),
        )
        return IntegrationAcceptResult(suggestion=suggestion, sync=turn)

    def dismiss(
        self,
        suggestion_id: str,
        *,
        graph: str,
        reason: str,
        author: str | None = None,
    ) -> models.OntologyChangeSuggestion:
        """Dismiss a suggestion on the data plane with a reason."""
        body: dict[str, Any] = {"reason": reason}
        if author is not None:
            body["author"] = author
        return cast(
            models.OntologyChangeSuggestion,
            self._client._model_request(
                models.OntologyChangeSuggestion,
                "POST",
                "/v1/ontology/suggestions/dismiss",
                params={"graph": graph, "suggestion_id": suggestion_id},
                body=body,
                options=_RETRY,
            ),
        )


def _accept_request(
    suggestion_id: str,
    graph: str,
    change: Sequence[Body] | None,
    comment: str | None,
    author: str | None,
) -> dict[str, Any]:
    """The data-plane accept as ``_model_request`` keyword arguments."""
    body: dict[str, Any] = {}
    if change is not None:
        body["change"] = [_coerce_body(op) for op in change]
    if comment is not None:
        body["comment"] = comment
    if author is not None:
        body["author"] = author
    return {
        "method": "POST",
        "path": "/v1/ontology/suggestions/accept",
        "params": {"graph": graph, "suggestion_id": suggestion_id},
        "body": body,
        "options": _RETRY,
    }


def _sync_after_request(
    suggestion: models.OntologyChangeSuggestion, connection: str, graph: str
) -> dict[str, Any]:
    """The workflow message ``sync`` under ``sync-after-<suggestion id>``."""
    return {
        "method": "POST",
        "path": "/v1/workflows/instances/message",
        "params": {"graph": graph},
        "body": {
            "workflow_id": connection,
            "id": f"sync-after-{suggestion.suggestion_id}",
            "message": {"type": "sync"},
        },
        "options": _RETRY,
    }
