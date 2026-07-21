"""HTTP client for a little big brain graph server.

Talks to ``lbb-server`` over HTTP with a stack API key
(``lbb_sk_test_…`` / ``lbb_sk_live_…``) or
single-mode token as a bearer credential — the same surface the TypeScript SDK,
CLI, and MCP server use. Request/response shapes are available as Pydantic
models in :mod:`lbb.models` (generated from the committed OpenAPI spec); the
methods here accept either a model instance or a plain ``dict`` and return the
parsed JSON response. For stronger IDE/type-checker help without changing that
default, the ``*_model`` and ``*_page`` helpers validate selected responses into
the generated Pydantic models. Synchronous and asynchronous transports are provided by the public
:mod:`lbb.client` facade.

For the local ``lbb-testctl`` shell-out wrapper (tests, notebooks), see
:mod:`lbb.local`.
"""

from __future__ import annotations

import json
import random
import time
import uuid
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Generic, TypedDict, TypeVar

import httpx
from pydantic import BaseModel

from . import models
from ._version import __version__

DEFAULT_BASE_URL = "http://127.0.0.1:7400"
# Generous default: commits over a large corpus and synchronous index builds run
# well past a few seconds. Background index builds (index_run(background=True))
# return immediately regardless.
DEFAULT_TIMEOUT = 120.0
# Retry count is now a secondary safety ceiling; the binding limit is the
# deadline budget below. Raised 2 → 6 so a multi-second `Retry-After` sequence
# (WAL depth-scaled backpressure, breaker cooldown) fits inside the budget
# instead of exhausting a tiny count first.
DEFAULT_MAX_RETRIES = 6
# Deadline-based retry budget (ms): keep retrying an idempotent op until this
# much wall-clock has elapsed, so a server's advertised backpressure window is
# actually honored rather than truncated by the count cap. 60s matches the
# `Retry-After` safety cap.
DEFAULT_RETRY_BUDGET_MS = 60_000.0
# Upper bound on any single computed backoff, matching the server's Retry-After cap.
_RETRY_DELAY_CAP_SECONDS = 60.0

# A request body: a plain mapping, or anything with a Pydantic ``model_dump``.
Body = Mapping[str, Any] | Any
ModelT = TypeVar("ModelT", bound=BaseModel)
RowT = TypeVar("RowT", bound=BaseModel)


class RequestOptions(TypedDict, total=False):
    """Per-request transport overrides accepted by :meth:`raw_request`."""

    max_retries: int
    retry: bool
    retry_budget_ms: float
    timeout: float
    headers: Mapping[str, str]


def _read_options(options: RequestOptions | None = None) -> RequestOptions:
    """Mark a semantically read-only POST as retry-safe unless explicitly disabled."""
    return {"retry": True, **(options or {})}


class LbbError(RuntimeError):
    """Raised when the server responds with a non-2xx status."""

    def __init__(
        self, status_code: int, body: str, error: Mapping[str, Any] | None = None
    ) -> None:
        self.status_code = status_code
        self.body = body
        self.error = dict(error or {})
        self.type = self.error.get("type")
        self.code = self.error.get("code")
        self.param = self.error.get("param")
        self.request_id = self.error.get("request_id")
        self.doc_url = self.error.get("doc_url")
        self.retryable = self.error.get("retryable")
        self.retry_after_seconds = self.error.get("retry_after_seconds")
        self.endpoint_hint = _endpoint_migration_hint(self.code)
        super().__init__(
            self.error.get("message") or f"Little Big Brain {status_code}: {body}"
        )


def _endpoint_migration_hint(code: str | None) -> str | None:
    if code == "stack_endpoint_required":
        return "Copy endpoint_url from the stack's Connect page and use it as base_url."
    if code == "stack_endpoint_mismatch":
        return "Use the endpoint_url and API key from the same stack."
    return None


@dataclass(frozen=True)
class RawLbbResponse:
    data: Any
    status_code: int
    request_id: str | None
    version: str | None
    headers: httpx.Headers
    attempts: int
    retry_count: int
    elapsed_ms: float

    def model(self, model_cls: type[ModelT]) -> ModelT:
        """Validate this response's JSON payload as a generated Pydantic model."""
        return _parse_model(model_cls, self.data)


@dataclass(frozen=True)
class RetryEvent:
    """Passed to a client's ``on_retry`` callback immediately before each backoff
    sleep, so callers can observe the backpressure the retry loop is absorbing —
    the visibility the ergonomic methods (``commit``, ``search``, …) otherwise
    hide by returning only ``.data``. Fires once per retry; sum ``delay_seconds``
    for the total wait, and read the final :attr:`RawLbbResponse.attempts` for the
    count.
    """

    method: str
    path: str
    #: 1-based number of the attempt that just failed and triggered this retry.
    attempt: int
    #: HTTP status of the failed attempt, or ``None`` for a transport error.
    status_code: int | None
    #: Parsed ``error.code`` of the failed attempt, when the body carried one.
    error_code: str | None
    #: The backoff (seconds) about to be slept — Retry-After header, the server's
    #: body ``retry_after_seconds`` hint, or full-jitter exponential backoff.
    delay_seconds: float
    #: Inclusive wall-clock elapsed (ms) across attempts and waits so far.
    elapsed_ms: float


@dataclass(frozen=True)
class IndexLineageObservation:
    metadata: models.GraphMetadataResponse
    lineage: models.IndexLineage
    build_commit: str | None
    replica: str | None
    request_id: str | None
    attempts: int
    elapsed_ms: float


@dataclass(frozen=True)
class ListPage(Generic[RowT]):
    """Typed view of LBB's unified list envelope.

    The server returns ``{object, data, has_more, next_cursor, snapshot,
    total_count}`` for browsable collections. Existing SDK methods keep
    returning that envelope as a dict; ``*_page`` helpers return this wrapper
    with each row validated as a generated Pydantic model.
    """

    object: str
    data: list[RowT]
    has_more: bool
    next_cursor: str | None
    snapshot: models.SnapshotView
    total_count: int

    @classmethod
    def from_payload(
        cls, payload: Mapping[str, Any], row_model: type[RowT]
    ) -> ListPage[RowT]:
        return cls(
            object=str(payload.get("object", "list")),
            data=[_parse_model(row_model, row) for row in payload.get("data", [])],
            has_more=bool(payload.get("has_more", False)),
            next_cursor=payload.get("next_cursor"),
            snapshot=_parse_model(models.SnapshotView, payload["snapshot"]),
            total_count=int(payload.get("total_count", len(payload.get("data", [])))),
        )

    def __iter__(self) -> Iterator[RowT]:
        return iter(self.data)


@dataclass(frozen=True)
class SparqlResults:
    """Parsed SPARQL 1.1 Query Results, returned by :meth:`LbbClient.sparql`.

    Wraps the standard results document (``{"head": {"vars": …}, "results":
    {"bindings": …}}`` for SELECT, ``{"head": …, "boolean": …}`` for ASK) so the
    caller never has to parse the engine's serialized JSON by hand.

    - :attr:`vars` — the projected variable names (the result ``head``).
    - :attr:`boolean` — the ASK answer, or ``None`` for a SELECT.
    - :attr:`bindings` — the raw typed bindings: each row maps a variable to a
      ``{"type", "value", "datatype"/"xml:lang"}`` term object (unbound
      variables are omitted, per the spec).
    - :meth:`rows` — the bindings flattened to plain ``{var: lexical_value}``
      dicts, the form most callers want. Iterating a ``SparqlResults`` yields
      these rows.
    - :attr:`row_page` — the server's pagination envelope (``returned``,
      ``total``, ``has_more``, ``next_offset``), when present.
    """

    vars: list[str]
    bindings: list[dict[str, Any]]
    boolean: bool | None
    row_page: dict[str, Any] | None

    @classmethod
    def from_results_json(
        cls, doc: Mapping[str, Any], row_page: Mapping[str, Any] | None = None
    ) -> SparqlResults:
        """Build from a parsed SPARQL Results JSON document."""
        head = doc.get("head") or {}
        variables = list(head.get("vars") or [])
        page = dict(row_page) if row_page is not None else None
        if "boolean" in doc:
            return cls(
                vars=variables, bindings=[], boolean=bool(doc["boolean"]), row_page=page
            )
        results = doc.get("results") or {}
        bindings = [dict(binding) for binding in results.get("bindings") or []]
        return cls(vars=variables, bindings=bindings, boolean=None, row_page=page)

    @classmethod
    def from_envelope(cls, envelope: Mapping[str, Any]) -> SparqlResults:
        """Build from the ``/v1/query/sparql-text`` envelope.

        That route carries the results document as a JSON *string* in
        ``results`` plus a sibling ``row_page``; this unwraps both.
        """
        raw = envelope.get("results")
        doc = json.loads(raw) if isinstance(raw, str) else (raw or {})
        return cls.from_results_json(doc, row_page=envelope.get("row_page"))

    def rows(self) -> list[dict[str, Any]]:
        """The bindings as plain ``{variable: lexical_value}`` dicts."""
        return [
            {name: term.get("value") for name, term in binding.items()}
            for binding in self.bindings
        ]

    def __iter__(self) -> Iterator[dict[str, Any]]:
        return iter(self.rows())

    def __len__(self) -> int:
        return len(self.bindings)


def _coerce_body(body: Body | None) -> Any:
    if body is None:
        return None
    dump = getattr(body, "model_dump", None)
    if callable(dump):
        return dump(mode="json")
    return body


def _parse_model(model_cls: type[ModelT], data: Any) -> ModelT:
    return model_cls.model_validate(data)


def _first_pattern_variable(patterns: Sequence[Mapping[str, Any]]) -> str:
    for pattern in patterns:
        subject = pattern.get("subject") or {}
        subject_var = subject.get("var") if isinstance(subject, Mapping) else None
        if isinstance(subject_var, str):
            return subject_var
        obj = pattern.get("object") or {}
        object_var = obj.get("var") if isinstance(obj, Mapping) else None
        if isinstance(object_var, str):
            return object_var
    return "entity"


def _attribute_filter_value(value: Any) -> dict[str, Any]:
    if isinstance(value, bool):
        return {"bool": value}
    if isinstance(value, int):
        return {"i64": value}
    if isinstance(value, float):
        return {"f64": value}
    if isinstance(value, str):
        return {"str": value}
    if isinstance(value, Mapping):
        if "date_time" in value:
            return {"date_time": value["date_time"]}
        if "dateTime" in value:
            return {"date_time": value["dateTime"]}
        if "entity" in value:
            return {"entity": value["entity"]}
    raise TypeError(f"unsupported attribute filter value: {value!r}")


def _attribute_filter(where: Mapping[str, Any], default_var: str) -> dict[str, Any]:
    return {
        "compare": {
            "op": where.get("op", "eq"),
            "left": {
                "property": {
                    "var": where.get("var", default_var),
                    "field": where["field"],
                }
            },
            "right": {"value": _attribute_filter_value(where["value"])},
        }
    }


def _parse_error(status_code: int, body: str, request_id: str | None) -> LbbError:
    try:
        parsed = json.loads(body)
    except json.JSONDecodeError:
        parsed = {}
    error = parsed.get("error") if isinstance(parsed, dict) else None
    if isinstance(error, dict):
        if error.get("request_id") is None:
            error = {**error, "request_id": request_id}
        return LbbError(status_code, body, error)
    return LbbError(
        status_code,
        body,
        {
            "type": "api_error",
            "code": "unstructured_error",
            "message": body or f"Little Big Brain {status_code}",
            "request_id": request_id,
        },
    )


def _decode_response_data(response: httpx.Response) -> Any:
    if not response.content:
        return None
    content_type = response.headers.get("content-type", "").lower()
    if any(
        rdf_type in content_type
        for rdf_type in (
            "text/turtle",
            "application/n-triples",
            "application/trig",
            "application/n-quads",
        )
    ):
        return response.text
    return response.json()


def _retryable(status_code: int) -> bool:
    # A 429 or any 5xx is retryable by status alone. A naked LB `502/503/504`
    # with an HTML body (no parseable error envelope) is a transient
    # server_busy-equivalent and is retried here just like a typed overload.
    return status_code == 429 or status_code >= 500


def _retry_allowed(method: str, idempotency_key: str | None) -> bool:
    return method.upper() in {"GET", "HEAD", "OPTIONS"} or idempotency_key is not None


def _error_body_field(response: httpx.Response, name: str) -> Any:
    """Read ``error.<name>`` from a JSON error body, or ``None`` when the body is
    absent, naked (a bare LB 5xx), or not the standard error envelope."""
    try:
        parsed = json.loads(response.text)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None
    error = parsed.get("error") if isinstance(parsed, dict) else None
    if isinstance(error, dict):
        return error.get(name)
    return None


def _body_marks_terminal(response: httpx.Response) -> bool:
    """True iff the server explicitly marked this error non-retryable in the body
    (``error.retryable == false``) — a durable rejection (e.g. an exhausted quota)
    the client must surface immediately instead of spending its retry budget."""
    return _error_body_field(response, "retryable") is False


def _jittered_backoff(base_delay: float, attempt: int) -> float:
    """Full-jitter exponential backoff: ``uniform(0, base * 2**attempt)``, capped.

    Replaces the old linear ``base * (attempt + 1)`` so many clients recovering
    from one outage do not retry in lockstep (a thundering herd that re-triggers
    the overload).
    """
    ceiling = min(max(0.0, base_delay) * (2**attempt), _RETRY_DELAY_CAP_SECONDS)
    return random.uniform(0.0, ceiling)


def _parse_retry_after_header(value: str, now: datetime | None) -> float | None:
    """Parse a Retry-After delta-seconds or HTTP-date value into seconds (may be
    negative for a past date; ``None`` when unparseable)."""
    try:
        return float(value)
    except ValueError:
        try:
            retry_at = parsedate_to_datetime(value)
            if retry_at.tzinfo is None:
                retry_at = retry_at.replace(tzinfo=timezone.utc)
            return (retry_at - (now or datetime.now(timezone.utc))).total_seconds()
        except (TypeError, ValueError, OverflowError):
            return None


def _retry_delay_seconds(
    response: httpx.Response,
    base_delay: float,
    attempt: int,
    *,
    now: datetime | None = None,
) -> float:
    """The backoff before the next attempt, in seconds:

    1. the ``Retry-After`` header (delta-seconds or HTTP-date), capped at 60s;
    2. else the server's own body hint ``error.retry_after_seconds`` (the server
       advertises this even on the rare path where the header is absent), capped
       at 60s;
    3. else full-jitter exponential backoff (see :func:`_jittered_backoff`).
    """
    value = response.headers.get("retry-after")
    if value:
        seconds = _parse_retry_after_header(value, now)
        if seconds is not None and seconds >= 0:
            return min(seconds, _RETRY_DELAY_CAP_SECONDS)
    body_hint = _error_body_field(response, "retry_after_seconds")
    if (
        isinstance(body_hint, (int, float))
        and not isinstance(body_hint, bool)
        and body_hint >= 0
    ):
        return min(float(body_hint), _RETRY_DELAY_CAP_SECONDS)
    return _jittered_backoff(base_delay, attempt)


def _raw_response(
    response: httpx.Response, *, attempts: int = 1, elapsed_ms: float = 0.0
) -> RawLbbResponse:
    request_id = response.headers.get("x-request-id")
    if response.status_code // 100 != 2:
        raise _parse_error(response.status_code, response.text.strip(), request_id)
    try:
        data = _decode_response_data(response)
    except ValueError as error:
        request = f" (request {request_id})" if request_id else ""
        raise ValueError(
            f"Little Big Brain returned invalid JSON with HTTP {response.status_code}{request}"
        ) from error
    return RawLbbResponse(
        data=data,
        status_code=response.status_code,
        request_id=request_id,
        version=response.headers.get("lbb-version"),
        headers=response.headers,
        attempts=attempts,
        retry_count=max(0, attempts - 1),
        elapsed_ms=max(0.0, elapsed_ms),
    )


class _BaseLbbClient:
    """Shared configuration and the route method table.

    Each route method returns ``self._request(...)``; in :class:`LbbClient`
    that is a value, in :class:`AsyncLbbClient` it is an awaitable.
    """

    def __init__(
        self,
        base_url: str = DEFAULT_BASE_URL,
        *,
        api_key: str | None = None,
        graph: str | None = None,
        branch: str | None = None,
        api_version: str = "2026-06-22",
        max_retries: int = DEFAULT_MAX_RETRIES,
        retry_delay: float = 0.1,
        retry_budget_ms: float = DEFAULT_RETRY_BUDGET_MS,
        on_retry: Callable[[RetryEvent], None] | None = None,
        default_consistency: str | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._api_key = api_key
        self._graph = graph
        self._branch = branch
        self._api_version = api_version
        self._max_retries = max_retries
        self._retry_delay = retry_delay
        self._retry_budget_ms = retry_budget_ms
        self._on_retry = on_retry
        # A5: read consistency applied when a read omits its own value. Since A5
        # the server default is ``eventual``; set ``"strong"`` to keep reads
        # head-exact by default. A per-call ``consistency`` always wins.
        self._default_consistency = default_consistency
        self.search = _SearchNamespace(self)
        self.context = _ContextNamespace(self)
        self.indexes = _IndexNamespace(self)
        self.entities = _EntityNamespace(self)
        self.ontology = _OntologyNamespace(self)
        self.query = _QueryNamespace(self)
        self.schema = _SchemaNamespace(self)

    def _headers(self, idempotency_key: str | None = None) -> dict[str, str]:
        headers: dict[str, str] = {
            "lbb-version": self._api_version,
            "user-agent": f"littlebigbrain/{__version__}",
        }
        if self._api_key is not None:
            headers["authorization"] = f"Bearer {self._api_key}"
        if idempotency_key is not None:
            headers["idempotency-key"] = idempotency_key
        return headers

    def _params(self, extra: Mapping[str, Any] | None = None) -> dict[str, Any]:
        params: dict[str, Any] = {}
        if self._graph is not None:
            params["graph"] = self._graph
        if self._branch is not None:
            params["branch"] = self._branch
        if extra:
            for key, value in extra.items():
                if value is not None:
                    params[key] = value
        return params

    def _resolve_consistency(self, consistency: str | None) -> str | None:
        """A5: a per-call consistency wins over the client ``default_consistency``."""
        return consistency if consistency is not None else self._default_consistency

    def _consistency_params(
        self, consistency: str | None, min_indexed_seq: int | None
    ) -> dict[str, Any]:
        """A5: read-consistency options as URL query params (SPARQL-text, summary)."""
        params: dict[str, Any] = {}
        resolved = self._resolve_consistency(consistency)
        if resolved is not None:
            params["consistency"] = resolved
        if min_indexed_seq is not None:
            params["min_indexed_seq"] = min_indexed_seq
        return params

    def _with_consistency(
        self, body: Body, consistency: str | None, min_indexed_seq: int | None
    ) -> Body:
        """A5: fold read-consistency options into a request body's own
        ``consistency`` / ``min_indexed_seq`` fields (full-text, embedding,
        structured-SPARQL bodies). An explicit body field wins over both the
        per-call value and the client default."""
        resolved = self._resolve_consistency(consistency)
        if resolved is None and min_indexed_seq is None:
            return body
        merged = dict(body) if isinstance(body, Mapping) else body
        if isinstance(merged, dict):
            if resolved is not None and merged.get("consistency") is None:
                merged["consistency"] = resolved
            if min_indexed_seq is not None and merged.get("min_indexed_seq") is None:
                merged["min_indexed_seq"] = min_indexed_seq
        return merged

    def _request_kwargs(
        self,
        *,
        params: Mapping[str, Any] | None,
        body: Body | None,
        content: str | None,
        content_type: str | None,
        idempotency_key: str | None,
        headers: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        """Build identical request options for the sync and async transports."""
        request_headers = self._headers(idempotency_key)
        request_headers.update(headers or {})
        kwargs: dict[str, Any] = {
            "params": self._params(params),
            "headers": request_headers,
        }
        if content is not None:
            kwargs["content"] = content
            request_headers["content-type"] = content_type or "application/octet-stream"
        elif body is not None:
            kwargs["json"] = _coerce_body(body)
        return kwargs

    def _emit_retry(
        self,
        method: str,
        path: str,
        *,
        attempt: int,
        status_code: int | None,
        error_code: str | None,
        delay_seconds: float,
        elapsed_ms: float,
    ) -> None:
        """Fire the ``on_retry`` callback for one absorbed retry (no-op if unset)."""
        on_retry = self._on_retry
        if on_retry is None:
            return
        on_retry(
            RetryEvent(
                method=method.upper(),
                path=path,
                attempt=attempt,
                status_code=status_code,
                error_code=error_code,
                delay_seconds=delay_seconds,
                elapsed_ms=elapsed_ms,
            )
        )

    def idempotency_key(self, prefix: str = "request") -> str:
        return f"{prefix}:{int(time.time() * 1_000_000)}:{uuid.uuid4().hex}"

    def graph(self, name: str, *, branch: str | None = None) -> _GraphNamespace:
        return _GraphNamespace(self, name, branch)

    def raw_request(
        self,
        method: str,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        body: Body | None = None,
        content: str | None = None,
        content_type: str | None = None,
        idempotency_key: str | None = None,
        options: RequestOptions | None = None,
    ) -> Any:
        raise NotImplementedError

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        body: Body | None = None,
        content: str | None = None,
        content_type: str | None = None,
        idempotency_key: str | None = None,
        options: RequestOptions | None = None,
    ) -> Any:  # noqa: D401
        raise NotImplementedError

    # --- writes ---

    def create_graph(self) -> models.CreateGraphResponse:
        """Create the scoped graph and branch with the built-in ontology.

        To use a custom ontology, call :meth:`ontology.define` instead before
        the first commit; defining an ontology also creates the graph head.
        """
        return self._model_request(
            models.CreateGraphResponse, "POST", "/v1/graph/create"
        )

    def commit(self, body: Body, *, idempotency_key: str | None = None) -> Any:
        """Commit triplets and optional entity embeddings."""
        return self._request(
            "POST",
            "/v1/graph/commit",
            body=body,
            idempotency_key=idempotency_key or self.idempotency_key("facts.create"),
        )

    def commit_model(
        self, body: Body, *, idempotency_key: str | None = None
    ) -> models.GraphCommitResponse:
        """Commit and validate the response as ``GraphCommitResponse``."""
        return self._model_request(
            models.GraphCommitResponse,
            "POST",
            "/v1/graph/commit",
            body=body,
            idempotency_key=idempotency_key or self.idempotency_key("facts.create"),
        )

    def commit_dry_run(self, body: Body) -> Any:
        """Validate-only preflight: run the same ontology/schema validation a real
        commit would and report the would-be effect (``op_count``,
        ``written_properties``, ``schema_validation``) without writing. A rejected
        request fails exactly as a real commit would, so it is a safe check before
        mutating. No idempotency key is needed — nothing is persisted.
        """
        return self._request(
            "POST",
            "/v1/graph/commit",
            body=body,
            params={"dry_run": "true"},
        )

    def commit_dry_run_model(self, body: Body) -> models.GraphCommitDryRunResponse:
        """Validate-only preflight with a typed ``GraphCommitDryRunResponse``."""
        return self._model_request(
            models.GraphCommitDryRunResponse,
            "POST",
            "/v1/graph/commit",
            body=body,
            params={"dry_run": "true"},
        )

    def delete_graph(self, *, confirm: str) -> models.GraphDeleteResponse:
        """Delete the scoped graph, including all branches and graph-scoped jobs."""
        return self._model_request(
            models.GraphDeleteResponse,
            "POST",
            "/v1/graph/delete",
            params={"confirm": confirm},
            options={"retry": True},
        )

    def delete_branch(self, *, confirm: str) -> models.GraphBranchDeleteResponse:
        """Delete only the scoped branch; the final live branch is protected."""
        return self._model_request(
            models.GraphBranchDeleteResponse,
            "DELETE",
            "/v1/graph/branch",
            params={"confirm": confirm},
        )

    def fork_graph(self, src: str, dst: str) -> models.GraphForkResponse:
        """Fork a whole graph into a brand-new destination graph in the same tenant.

        The copy runs as a durable background job (``confirm`` is fixed to ``dst``,
        which the route requires to authorize the fork); the destination must not
        already exist, so the create-only CAS on the server side makes the call
        safe to retry. The response only acknowledges the enqueue — poll the
        destination graph's metadata (see ``response.poll``) to observe the fork
        completing: the destination becomes readable once its head is published.
        """
        return self._model_request(
            models.GraphForkResponse,
            "POST",
            "/v1/graph/fork",
            params={"src": src, "dst": dst, "confirm": dst},
            options={"retry": True},
        )

    def embedding_config(
        self, *, options: RequestOptions | None = None
    ) -> models.ManagedEmbeddingConfigResponse:
        """Read the scoped graph's managed embedding configuration."""
        return self._model_request(
            models.ManagedEmbeddingConfigResponse,
            "GET",
            "/v1/graph/embedding",
            options=options,
        )

    def embedding_models(
        self,
        *,
        options: RequestOptions | None = None,
    ) -> models.ManagedEmbeddingModelsResponse:
        """List the embedding models available on this deployment."""
        return self._model_request(
            models.ManagedEmbeddingModelsResponse,
            "GET",
            "/v1/graph/embedding/models",
            options=options,
        )

    def set_embedding_model(
        self, model_id: str, *, auto_embed_query: bool = True
    ) -> models.ManagedEmbeddingConfigResponse:
        """Choose the model used automatically for writes and vector queries."""
        return self.set_embedding_config(
            {
                "model_id": model_id,
                "service": "open_router",
                "auto_embed_query": auto_embed_query,
            }
        )

    def set_embedding_config(self, body: Body) -> models.ManagedEmbeddingConfigResponse:
        """Set advanced managed embedding configuration."""
        return self._model_request(
            models.ManagedEmbeddingConfigResponse,
            "POST",
            "/v1/graph/embedding",
            body=body,
        )

    def backfill_embeddings(
        self,
        *,
        batch_size: int | None = None,
        limit: int | None = None,
        full: bool | None = None,
        idempotency_key: str | None = None,
        timeout: float = 1800.0,
        poll_interval: float = 2.0,
    ) -> models.ManagedEmbeddingBackfillResponse:
        """Submit the durable backfill job and wait for its terminal result."""
        status = self.submit_embedding_backfill(
            {"batch_size": batch_size, "limit": limit, "full": bool(full)},
            idempotency_key=idempotency_key
            or self.idempotency_key("embedding-backfill"),
        )
        deadline = time.monotonic() + timeout
        while status.status in {"pending", "running"}:
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"embedding backfill {status.job_id} exceeded {timeout}s"
                )
            time.sleep(poll_interval)
            status = self.embedding_backfill_job(status.job_id)
        if status.status != "succeeded" or status.result is None:
            raise RuntimeError(
                status.terminal_error
                or f"embedding backfill {status.job_id} ended {status.status}"
            )
        return status.result

    def submit_embedding_backfill(
        self,
        body: Body,
        *,
        idempotency_key: str,
    ) -> models.ManagedEmbeddingBackfillJobStatusResponse:
        return self._model_request(
            models.ManagedEmbeddingBackfillJobStatusResponse,
            "POST",
            "/v1/graph/embedding/backfill-jobs",
            body=body,
            idempotency_key=idempotency_key,
        )

    def embedding_backfill_job(
        self, job_id: str
    ) -> models.ManagedEmbeddingBackfillJobStatusResponse:
        return self._model_request(
            models.ManagedEmbeddingBackfillJobStatusResponse,
            "GET",
            "/v1/graph/embedding/backfill-jobs",
            params={"job_id": job_id},
        )

    def cancel_embedding_backfill(
        self, job_id: str
    ) -> models.ManagedEmbeddingBackfillJobStatusResponse:
        return self._model_request(
            models.ManagedEmbeddingBackfillJobStatusResponse,
            "DELETE",
            "/v1/graph/embedding/backfill-jobs",
            params={"job_id": job_id},
        )

    def promote_embedding(
        self, *, run_id: str, allow_regression: bool | None = None
    ) -> models.ManagedEmbeddingPromoteResponse:
        """Promote a successful fine-tuned embedding run to the graph default."""
        return self._model_request(
            models.ManagedEmbeddingPromoteResponse,
            "POST",
            "/v1/graph/embedding/promote",
            params={"run_id": run_id, "allow_regression": allow_regression},
        )

    def import_ndjson(
        self,
        lines: Sequence[Mapping[str, Any]] | str,
        *,
        batch: int | None = None,
        strict: bool | None = None,
        observed_at: str | None = None,
        index: bool | None = None,
        idempotency_key: str | None = None,
    ) -> Any:
        """Bulk-ingest a dataset as NDJSON.

        ``lines`` is a sequence of triplet / entity-properties records (each
        serialized to one NDJSON line here) or a pre-built NDJSON string. Lines are
        batched into bounded internal commits server-side, so a whole dataset loads
        in one streamed request without a single oversized commit.

        Set ``index=True`` to run one full index build after the last batch, so the
        data is served from the persisted runs (not just the ephemeral snapshot
        fallback) by the time the call returns — the "bulk load, queryable on
        return" path for connector backfills. This replaces the anti-pattern of
        indexing per batch (which serializes builds and races the throttle): import
        the whole dataset, index once. The response's ``index`` object reports
        whether the build ran (``built``) or was skipped (``skipped_reason``).
        """
        ndjson = (
            lines
            if isinstance(lines, str)
            else "\n".join(json.dumps(_coerce_body(line)) for line in lines)
        )
        return self._request(
            "POST",
            "/v1/graph/import",
            params={
                "batch": batch,
                "strict": strict,
                "observed_at": observed_at,
                "index": index,
            },
            content=ndjson,
            content_type="application/x-ndjson",
            idempotency_key=idempotency_key or self.idempotency_key("import"),
        )

    def reload(
        self,
        lines: Sequence[Mapping[str, Any]] | str,
        *,
        confirm: str,
        dry_run: bool | None = None,
        strict: bool | None = None,
        observed_at: str | None = None,
        idempotency_key: str | None = None,
    ) -> models.GraphReloadResponse:
        """Declarative full-state replace: reconcile the scoped graph so its current
        state matches exactly the NDJSON payload.

        ``lines`` uses the same grammar as :meth:`import_ndjson` — triplet /
        entity-properties records passed as a sequence (serialized to one NDJSON
        line each here) or a pre-built NDJSON string. The whole reconciliation
        lands as one atomic cutover: payload records are upserted, and entities
        present at the pre-reload head but absent from the payload leave current
        state (retraction semantics — history is preserved, so an ``as_of`` read
        pinned before the cutover still sees the old state).

        ``confirm`` must equal the target graph id (reload is semi-destructive).
        ``dry_run=True`` previews the full delta with zero durable changes. The
        response carries ``prior_commit_seq`` / ``prior_snapshot_token`` as the
        rollback anchor — read them back with ``?as_of_commit_seq=<prior_commit_seq>``
        to see the pre-reload state. An idempotency key scopes the single cutover
        commit, so a retry replays rather than re-applying.
        """
        ndjson = (
            lines
            if isinstance(lines, str)
            else "\n".join(json.dumps(_coerce_body(line)) for line in lines)
        )
        return self._model_request(
            models.GraphReloadResponse,
            "POST",
            "/v1/graph/reload",
            params={
                "confirm": confirm,
                "dry_run": dry_run,
                "strict": strict,
                "observed_at": observed_at,
            },
            content=ndjson,
            content_type="application/x-ndjson",
            idempotency_key=idempotency_key or self.idempotency_key("reload"),
        )

    def import_rdf(
        self,
        rdf: str | None = None,
        *,
        ntriples: str | None = None,
        format: str = "ntriples",
        base_iri: str | None = None,
        graph_uri: str | None = None,
        blank_node_scope: str | None = None,
        batch: int | None = None,
        strict: bool | None = None,
        observed_at: str | None = None,
        resource_type: str | None = None,
        edge_idempotency: str | None = None,
        idempotency_key: str | None = None,
    ) -> Any:
        """Bulk-ingest N-Triples, Turtle, N-Quads, or TriG through the native RDF import endpoint.

        Statements are committed through the fixed ``RDF_TRIPLE`` relation;
        source RDF predicates and literal term details are preserved as edge
        metadata.
        """
        if rdf is not None and ntriples is not None:
            raise TypeError("pass exactly one of rdf= or the deprecated ntriples=")
        if rdf is None:
            rdf = ntriples
        if rdf is None:
            raise TypeError("import_rdf() requires RDF text via rdf= or ntriples=")
        if ntriples is not None and format != "ntriples":
            raise ValueError(
                "the deprecated ntriples= keyword requires format='ntriples'"
            )
        content_types = {
            "ntriples": "application/n-triples",
            "turtle": "text/turtle",
            "nquads": "application/n-quads",
            "trig": "application/trig",
        }
        if format not in content_types:
            raise ValueError("format must be 'ntriples', 'turtle', 'nquads', or 'trig'")
        return self._request(
            "POST",
            "/v1/graph/import/rdf",
            params={
                "batch": batch,
                "strict": strict,
                "observed_at": observed_at,
                "format": format,
                "base_iri": base_iri,
                "graph_uri": graph_uri,
                "blank_node_scope": blank_node_scope,
                "resource_type": resource_type,
                "edge_idempotency": edge_idempotency,
            },
            content=rdf,
            content_type=content_types[format],
            idempotency_key=idempotency_key or self.idempotency_key("import-rdf"),
        )

    def export_rdf(
        self,
        *,
        format: str = "turtle",
        max_triples: int | None = None,
        as_of_valid_time: str | None = None,
        as_of_commit_seq: int | None = None,
        entailment: str | None = None,
        reason: bool | None = None,
    ) -> Any:
        """Export the snapshot-visible RDF projection as Turtle, N-Triples, TriG, or N-Quads."""
        if format not in {"turtle", "ntriples", "trig", "nquads"}:
            raise ValueError("format must be 'turtle', 'ntriples', 'trig', or 'nquads'")
        return self._request(
            "GET",
            "/v1/graph/export/rdf",
            params={
                "format": "nt" if format == "ntriples" else format,
                "max_triples": max_triples,
                "as_of_valid_time": as_of_valid_time,
                "as_of_commit_seq": as_of_commit_seq,
                "entailment": entailment,
                "reason": reason,
            },
        )

    def export_rdf_preview(
        self,
        *,
        format: str = "turtle",
        max_triples: int = 100,
        as_of_valid_time: str | None = None,
        as_of_commit_seq: int | None = None,
        entailment: str | None = None,
        reason: bool | None = None,
    ) -> models.RdfExportPreviewResponse:
        """Return a deterministic bounded RDF slice plus truncation metadata."""
        if format not in {"turtle", "ntriples", "trig", "nquads"}:
            raise ValueError("format must be 'turtle', 'ntriples', 'trig', or 'nquads'")
        return self._model_request(
            models.RdfExportPreviewResponse,
            "GET",
            "/v1/graph/export/rdf",
            params={
                "format": "nt" if format == "ntriples" else format,
                "max_triples": max_triples,
                "truncate": "true",
                "as_of_valid_time": as_of_valid_time,
                "as_of_commit_seq": as_of_commit_seq,
                "entailment": entailment,
                "reason": reason,
            },
        )

    def retract(self, body: Body, *, idempotency_key: str | None = None) -> Any:
        """Retract specific edges and/or every edge touching given entities.

        Appends superseding retract events rather than deleting — history stays
        visible in an ``as_of`` read before the retraction, but the edges drop out
        of current-state reads. The surgical alternative to :meth:`delete_graph`.
        """
        return self._request(
            "POST",
            "/v1/graph/retract",
            body=body,
            idempotency_key=idempotency_key or self.idempotency_key("retract"),
        )

    def merge_branch(self, body: Body, *, idempotency_key: str | None = None) -> Any:
        """Validate-then-merge a child branch onto the scoped branch (its fork
        parent) as one commit (``POST /v1/graph/branch/merge``). Body:
        ``{"from_branch", "validate"?, "delete_source"?}``. A write — carries an
        Idempotency-Key so a retry replays instead of re-applying.
        """
        return self._request(
            "POST",
            "/v1/graph/branch/merge",
            body=body,
            idempotency_key=idempotency_key or self.idempotency_key("branch-merge"),
        )

    def observe(self, body: Body, *, idempotency_key: str | None = None) -> Any:
        """Observe (``POST /v1/memory/observe``): store a conversation
        episode verbatim as EPISODE evidence, anchor + gate extracted facts on an
        observe branch, and optionally auto-merge when validation is clean.
        Flag-gated server-side (``--enable-observe``).
        """
        return self._request(
            "POST",
            "/v1/memory/observe",
            body=body,
            idempotency_key=idempotency_key or self.idempotency_key("observe"),
        )

    # --- models as runs (training-run registry + eval machinery) ---

    def vocab_export(
        self, *, sections: list[str] | None = None, limit: int | None = None
    ) -> Any:
        """The graph's grounding vocabulary as byte-sorted, deduped string
        sections — decoder-side automaton input / export-bundle half
        (``GET /v1/search/vocab``)."""
        return self._request(
            "GET",
            "/v1/search/vocab",
            params={
                "sections": ",".join(sections) if sections else None,
                "limit": limit,
            },
        )

    def read_signals(
        self,
        *,
        from_seq: int | None = None,
        to_seq: int | None = None,
        limit: int | None = None,
    ) -> Any:
        """Captured signals by flush-seq range, oldest first — the model-training
        feed; ``seq`` is the temporal-split coordinate
        (``GET /v1/signals``)."""
        return self._request(
            "GET",
            "/v1/signals",
            params={"from": from_seq, "to": to_seq, "limit": limit},
        )

    def record_model_run(self, manifest: Body) -> Any:
        """Record one immutable model-as-run manifest (``POST
        /v1/models/record``); runs number sequentially per kind. Trainers MUST
        train on data <= ``trained_at_commit_seq`` and evaluate past it —
        ``model_split_audit`` verifies the recorded lineage."""
        return self._request("POST", "/v1/models/record", body=manifest)

    def promote_model_run(self, *, kind: str, run: int) -> Any:
        """CAS-promote a recorded run to CURRENT for its kind (``POST
        /v1/models/promote``); replay is a no-op."""
        return self._request(
            "POST", "/v1/models/promote", params={"kind": kind, "run": run}
        )

    def model_registry(self, *, kind: str) -> Any:
        """A kind's model runs, newest first, with effective promotion state
        (``GET /v1/models/registry``)."""
        return self._request("GET", "/v1/models/registry", params={"kind": kind})

    def model_registry_gc(self, *, kind: str, keep: int | None = None) -> Any:
        """GC run prefixes beyond the promoted run + the last ``keep``;
        reports deletions (``POST /v1/models/registry/gc``)."""
        return self._request(
            "POST", "/v1/models/registry/gc", params={"kind": kind, "keep": keep}
        )

    def model_split_audit(self, *, kind: str, run: int) -> Any:
        """Verify a run's temporal-split obligation from its recorded lineage
        (``GET /v1/models/split-audit``)."""
        return self._request(
            "GET", "/v1/models/split-audit", params={"kind": kind, "run": run}
        )

    def shadow_eval(self, body: Body) -> Any:
        """Champion vs challenger retrieval over one pinned snapshot (``POST
        /v1/models/shadow-eval``). Returns promotion evidence (hit-rate@k,
        latency, per-query overlap); never promotes."""
        return self._request("POST", "/v1/models/shadow-eval", body=body)

    def synthetic_eval(self, *, limit: int | None = None) -> Any:
        """Execution-verified QA probes generated from the graph's current
        edges — labels are the executed projections (``GET
        /v1/models/synthetic-eval``). Feeds ``shadow_eval`` directly."""
        return self._request(
            "GET", "/v1/models/synthetic-eval", params={"limit": limit}
        )

    def model_cadence(self, *, kind: str) -> Any:
        """The doubling retrain policy: is a retrain due for this kind?
        (``GET /v1/models/cadence``)."""
        return self._request("GET", "/v1/models/cadence", params={"kind": kind})

    def train_tick(self, body: Body) -> Any:
        """One deterministic trainer tick (``POST /v1/models/train-tick``):
        probe set (execution-verified synthetic pairs, or bring your own via
        ``probes``) -> bounded candidate search on the train slice -> held-out
        eval gate -> record the run either way -> CAS promote only when the
        gate passes. The same tick the ``auto_train`` cadence fires."""
        return self._request("POST", "/v1/models/train-tick", body=body)

    def train_submit(
        self, body: Body, *, idempotency_key: str
    ) -> models.TrainModelJobStatusResponse:
        """Submit a durable background trainer job with a reconnect-safe id."""
        return self._model_request(
            models.TrainModelJobStatusResponse,
            "POST",
            "/v1/models/train-jobs",
            body=body,
            idempotency_key=idempotency_key,
        )

    def train_job(self, job_id: str) -> models.TrainModelJobStatusResponse:
        """Read progress, terminal failure, or the complete gated result."""
        return self._model_request(
            models.TrainModelJobStatusResponse,
            "GET",
            "/v1/models/train-jobs",
            params={"job_id": job_id},
        )

    def training_config(self) -> Any:
        """The graph's automatic-training configuration (default: off)
        (``GET /v1/models/training-config``)."""
        return self._request("GET", "/v1/models/training-config")

    def set_training_config(self, body: Body) -> Any:
        """Set the automatic-training configuration — the ``auto_train``
        toggle + trainable kinds (``POST /v1/models/training-config``)."""
        return self._request("POST", "/v1/models/training-config", body=body)

    def ask(self, body: Body) -> Any:
        """Ground a natural-language question to the graph's real vocabulary,
        retrieve against the pinned snapshot, and answer with citations
        (``POST /v1/ask``). Body:
        ``{"question", "execute"?, "top_k"?, "as_of_commit_seq"?,
        "as_of_valid_time"?}``. The response carries ``mode``
        (``grounding_only`` | ``resident_planner``), ``answer``, ``citations``,
        ``grounding``, a per-call ``explain``, and an ``ask_id`` to join to
        :meth:`ask_feedback`. The typed response model is
        :class:`lbb.models.AskResponse`."""
        return self._request("POST", "/v1/ask", body=body)

    def ask_feedback(self, body: Body, *, idempotency_key: str | None = None) -> Any:
        """Verdict on an ask (``POST /v1/ask/feedback``): ``accepted`` |
        ``rejected`` | ``corrected`` (+ ``corrected_plan``), joined to the
        ask's trace by ``ask_id`` — the planner fine-tune's feedback capture.
        ``accepted: false`` means signal capture is off on this deployment."""
        validated = models.AskFeedbackRequest.model_validate(body)
        return self._request(
            "POST",
            "/v1/ask/feedback",
            body=validated.model_dump(mode="json", exclude_none=True),
            idempotency_key=idempotency_key or self.idempotency_key("ask-feedback"),
        )

    def ingest_signals(self, body: Body, *, idempotency_key: str | None = None) -> Any:
        """Write a typed supervision batch with a durable replay-safe receipt."""
        return self._request(
            "POST",
            "/v1/signals",
            body=body,
            idempotency_key=idempotency_key or self.idempotency_key("signals"),
        )

    def suggestion_shown(
        self, payload: Body, *, idempotency_key: str | None = None
    ) -> Any:
        """Ingest one ``SuggestionShownV1`` payload."""
        validated = models.SuggestionShownV1.model_validate(payload)
        return self.ingest_signals(
            {
                "signals": [
                    {
                        "kind": "suggestion_shown",
                        "payload": validated.model_dump(mode="json"),
                    }
                ]
            },
            idempotency_key=idempotency_key,
        )

    def suggestion_adopted(
        self, payload: Body, *, idempotency_key: str | None = None
    ) -> Any:
        """Ingest one trainable ``SuggestionAdoptedV1`` payload."""
        validated = models.SuggestionAdoptedV1.model_validate(payload)
        return self.ingest_signals(
            {
                "signals": [
                    {
                        "kind": "suggestion_adopted",
                        "payload": validated.model_dump(mode="json"),
                    }
                ]
            },
            idempotency_key=idempotency_key,
        )

    def external_planner_trace(
        self, payload: Body, *, idempotency_key: str | None = None
    ) -> Any:
        """Ingest one versioned external planner trace."""
        validated = models.ExternalPlannerTraceV1.model_validate(payload)
        encoded = validated.model_dump(mode="json", exclude_none=True)
        return self.ingest_signals(
            {
                "signals": [
                    {
                        "kind": "external_planner_trace",
                        "request_id": validated.ask_id,
                        "snapshot_token": validated.snapshot_token,
                        "payload": encoded,
                    }
                ]
            },
            idempotency_key=idempotency_key,
        )

    def planner_dataset(
        self, *, limit: int | None = None, split_seq: int | None = None
    ) -> Any:
        """The planner fine-tune's training feed (``GET
        /v1/models/planner-dataset``): feedback rows joined from signals ≤ the
        split pin + execution-verified synthetic plans."""
        return self._request(
            "GET",
            "/v1/models/planner-dataset",
            params={"limit": limit, "split_seq": split_seq},
        )

    def planner_preference_dataset(
        self, *, limit: int | None = None, split_seq: int | None = None
    ) -> Any:
        """The DPO pass's training feed (``GET
        /v1/models/planner-preference-dataset``): preference pairs from
        corrected verdicts, paired rejections, synthetic corrupted-slot
        pairs; unpaired rejections are counted."""
        return self._request(
            "GET",
            "/v1/models/planner-preference-dataset",
            params={"limit": limit, "split_seq": split_seq},
        )

    def suggest_dataset(
        self, *, limit: int | None = None, split_seq: int | None = None
    ) -> Any:
        """The suggest-ranker trainer's probe feed (``GET
        /v1/models/suggest-dataset``): ``suggestion_adopted`` signals ≤ the
        split pin + execution-verified synthetic vocabulary pairs."""
        return self._request(
            "GET",
            "/v1/models/suggest-dataset",
            params={"limit": limit, "split_seq": split_seq},
        )

    def extractor_dataset(
        self, *, limit: int | None = None, split_seq: int | None = None
    ) -> Any:
        """The extractor fine-tune's training feed (``GET
        /v1/models/extractor-dataset``): EPISODE transcripts joined to the
        facts the observe pipeline committed from them."""
        return self._request(
            "GET",
            "/v1/models/extractor-dataset",
            params={"limit": limit, "split_seq": split_seq},
        )

    def promote_extractor(
        self, *, run_id: str, allow_regression: bool | None = None
    ) -> Any:
        """Promote a finished ``extractor_lora`` run (``POST
        /v1/models/promote-extractor``): gated on held-out fact F1, recorded
        as a ``kind=extractor`` training run whose adapter resident extraction
        serves."""
        return self._request(
            "POST",
            "/v1/models/promote-extractor",
            params={"run_id": run_id, "allow_regression": allow_regression},
        )

    def promote_planner(
        self, *, run_id: str, allow_regression: bool | None = None
    ) -> Any:
        """Promote a finished ``planner_lora`` run (``POST
        /v1/models/promote-planner``): gated on held-out slot exactness,
        recorded as a ``kind=planner`` training run whose adapter ``/v1/ask``
        serves."""
        return self._request(
            "POST",
            "/v1/models/promote-planner",
            params={"run_id": run_id, "allow_regression": allow_regression},
        )

    # --- search ---

    def graph_search(self, body: Body, *, options: RequestOptions | None = None) -> Any:
        """Full semantic hybrid search from a request body."""
        return self._request(
            "POST", "/v1/graph/search", body=body, options=_read_options(options)
        )

    def multi_search(self, body: Body, *, options: RequestOptions | None = None) -> Any:
        """Reciprocal-rank-fusion across sub-queries."""
        return self._request(
            "POST", "/v1/search/multi", body=body, options=_read_options(options)
        )

    def full_text_search(
        self,
        body: Body,
        *,
        consistency: str | None = None,
        min_indexed_seq: int | None = None,
        options: RequestOptions | None = None,
    ) -> Any:
        """BM25 search. ``min_indexed_seq`` sets the A5 read-your-writes floor."""
        return self._request(
            "POST",
            "/v1/search/full-text",
            body=self._with_consistency(body, consistency, min_indexed_seq),
            options=_read_options(options),
        )

    def embedding_search(
        self,
        body: Body,
        *,
        consistency: str | None = None,
        min_indexed_seq: int | None = None,
        options: RequestOptions | None = None,
    ) -> Any:
        """ANN/vector search. ``min_indexed_seq`` sets the A5 read-your-writes floor."""
        return self._request(
            "POST",
            "/v1/search/embedding",
            body=self._with_consistency(body, consistency, min_indexed_seq),
            options=_read_options(options),
        )

    # --- search relevance feedback (training data) ---

    def search_feedback(self, body: Body, *, idempotency_key: str | None = None) -> Any:
        """Append relevance labels for a set of search results.

        How little big brain gathers customer-specific qrels: after a search,
        grade the results (``3`` ideal/good, ``1`` partially relevant, ``0``
        bad), referencing the ``search_id`` from the search response so the
        labels tie back to that exact ranking. Labels are stored apart from
        customer facts (in ``__lbb_feedback``) and exported via
        :meth:`search_feedback_export` as training/eval data for embedding
        fine-tuning. The body is a ``SearchFeedbackRequest`` (``query``,
        optional ``search_id``, and ``labels`` of
        ``{target, rank, score, grade, split}``).
        """
        return self._request(
            "POST", "/v1/search/feedback", body=body, idempotency_key=idempotency_key
        )

    def search_feedback_export(self) -> models.SearchFeedbackExportResponse:
        """Export labels as a typed ``SearchFeedbackExportResponse``."""
        return self._model_request(
            models.SearchFeedbackExportResponse, "GET", "/v1/search/feedback/export"
        )

    def search_feedback_summary(self) -> models.SearchFeedbackSummaryResponse:
        """Read constant-size feedback counts and promoted-model status."""
        return self._model_request(
            models.SearchFeedbackSummaryResponse, "GET", "/v1/search/feedback/summary"
        )

    # --- traversal ---

    def traverse(self, body: Body) -> Any:
        """Bounded k-hop graph traversal."""
        return self._request("POST", "/v1/graph/traverse", body=body)

    def semantic_traverse(self, body: Body) -> Any:
        """Resolve a query to seed entities, then return bounded paths."""
        return self._request("POST", "/v1/graph/semantic-traverse", body=body)

    # --- temporal / lineage / shapes ---

    def current_state(self, body: Body) -> Any:
        """Current state of an entity's relations, optionally as-of a timestamp."""
        return self._request("POST", "/v1/query/state", body=body)

    def history(self, body: Body) -> Any:
        """Full edge-event history for a relationship."""
        return self._request("POST", "/v1/query/history", body=body)

    def why(self, body: Body) -> Any:
        """Lineage and evidence for a single edge."""
        return self._request("POST", "/v1/query/why", body=body)

    def shacl(self, body: Body) -> Any:
        """SHACL-style shape/pattern query."""
        return self._request("POST", "/v1/query/shacl", body=body)

    # --- SPARQL ---

    def sparql_select(
        self,
        body: Body,
        *,
        consistency: str | None = None,
        min_indexed_seq: int | None = None,
    ) -> Any:
        """Structured SPARQL-subset SELECT/ASK/aggregate (``POST /v1/query/sparql``).

        Takes a ``SparqlSelectRequest`` (model or dict): conjunctive ``patterns``
        plus optional ``filters``, ``group_by``/``aggregates`` (COUNT/SUM/AVG/
        MIN/MAX), ``having``, ``order_by``, ``select``/``distinct``, ``ask``,
        ``limit``/``offset``, and the ``as_of_*`` snapshot pins. GROUP BY is not
        limited to entity identity: ``group_keys`` adds typed scalar keys — a
        property value or a calendar bucket of a datetime property
        (``{"date_bucket": {"var", "field", "granularity", "as"}}``) — so a
        per-category breakdown or a time series is one server-side query; scalar
        keys come back in each ``groups[].value_keys[<as>]``. Returns the typed
        ``SparqlSelectResponse`` shape. For raw SPARQL *text*, use :meth:`sparql`.

        ``consistency`` / ``min_indexed_seq`` select the A5 read mode and the
        read-your-writes floor (body fields on this structured route).
        """
        return self._request(
            "POST",
            "/v1/query/sparql",
            body=self._with_consistency(body, consistency, min_indexed_seq),
        )

    def sparql_select_model(self, body: Body) -> models.SparqlSelectResponse:
        """Structured SPARQL response validated as ``SparqlSelectResponse``."""
        return self._model_request(
            models.SparqlSelectResponse, "POST", "/v1/query/sparql", body=body
        )

    def analytics(self, body: Body) -> Any:
        """Basic-graph-pattern query with group-graph-pattern combinators.

        UNION / OPTIONAL / MINUS / EXISTS / NOT EXISTS folded over the base
        ``patterns``. The complement to :meth:`sparql_select`: this route carries
        the combinators (but not FILTER/aggregation), so reach for it when a
        query needs an optional/union/negated leg rather than a grouped aggregate.
        """
        return self._request("POST", "/v1/query/analytics", body=body)

    def governed_conflicts(
        self, body: Body
    ) -> models.GovernedConflictAggregationResponse:
        """Return ACL-first distinct-value conflicts without raw corpus transfer."""
        return self._model_request(
            models.GovernedConflictAggregationResponse,
            "POST",
            "/v1/query/conflicts",
            body=body,
            options=_read_options(None),
        )

    def _sparql_text_envelope(
        self,
        query: str,
        *,
        reason: bool | None = None,
        entailment: str | None = None,
        as_of_valid_time: str | None = None,
        as_of_commit_seq: int | None = None,
        limit: int | None = None,
        offset: int | None = None,
        consistency: str | None = None,
        min_indexed_seq: int | None = None,
    ) -> Any:
        """POST raw SPARQL text to ``/v1/query/sparql-text``; returns the envelope.

        Value in :class:`LbbClient`, awaitable in :class:`AsyncLbbClient`; the
        concrete :meth:`sparql` wrappers parse it into :class:`SparqlResults`.
        """
        body: dict[str, Any] = {"query": query}
        if reason is not None:
            body["reason"] = reason
        if entailment is not None:
            body["entailment"] = entailment
        if as_of_valid_time is not None:
            body["as_of_valid_time"] = as_of_valid_time
        if as_of_commit_seq is not None:
            body["as_of_commit_seq"] = as_of_commit_seq
        if limit is not None:
            body["limit"] = limit
        if offset is not None:
            body["offset"] = offset
        # A5: the text dialect carries consistency/floor on the URL, not the body.
        params = self._consistency_params(consistency, min_indexed_seq)
        return self._request(
            "POST", "/v1/query/sparql-text", body=body, params=params or None
        )

    # --- ontology ---

    def ontology_search(self, body: Body) -> Any:
        """Discover ontology concepts, terms, and relations."""
        return self._request("POST", "/v1/ontology/search", body=body)

    def ontology_resolve(self, body: Body) -> Any:
        """Resolve mentions to concepts/entities."""
        return self._request("POST", "/v1/ontology/resolve", body=body)

    def ontology_conformance(self) -> Any:
        """Audit the snapshot against the ontology's implied constraints.

        Derives SHACL shapes from the ontology's capped ``cardinality``
        (``-> sh:maxCount``) and validates the current snapshot against them,
        returning a ``SchemaAuditReport``. Whole-snapshot and never blocks a
        write; unlike the published-shapes schema audit it needs no activated
        shape bundle — the shapes come from the ontology itself.
        """
        return self._request("GET", "/v1/ontology/conformance")

    def ontology_conformance_model(self) -> models.SchemaAuditReport:
        """Ontology conformance report validated as ``SchemaAuditReport``."""
        return self._model_request(
            models.SchemaAuditReport, "GET", "/v1/ontology/conformance"
        )

    def ontology_view(self, *, counts: bool = False) -> Any:
        """The active ontology for the scoped graph: entity types and relations.

        Pass ``counts=True`` to include ``relation_defs[].edge_count`` — the
        number of current edges of each relation in the served snapshot — so
        you can see which declared relations are actually populated
        (``edge_count == 0`` is declared-but-unused). It is opt-in because the
        count costs a snapshot load; the field is omitted otherwise.
        """
        params = {"counts": "true"} if counts else None
        return self._request("GET", "/v1/ontology", params=params)

    def ontology_view_model(self, *, counts: bool = False) -> models.OntologyView:
        """Active ontology validated as ``OntologyView``."""
        params = {"counts": "true"} if counts else None
        return self._model_request(
            models.OntologyView, "GET", "/v1/ontology", params=params
        )

    # --- index lifecycle ---

    def index_build(self, *, background: bool = False) -> Any:
        """Build default ANN + BM25 indexes.

        With ``background=True`` the build runs detached on the server and the
        call returns immediately — use this for large corpora whose synchronous
        build would exceed a fronting gateway's timeout (a 504), then poll
        :meth:`metadata` (or :meth:`LbbClient.wait_for_index`) for completion.
        """
        return self._request(
            "POST",
            "/v1/index/build",
            params={"background": "true" if background else None},
        )

    def index_run(self, *, background: bool = False) -> Any:
        """Build BM25, ANN/vector, and adjacency index families.

        With ``background=True`` the build runs detached on the server and the
        call returns immediately — use this for large corpora whose synchronous
        build would exceed a fronting gateway's timeout, then poll
        :meth:`metadata` (or :meth:`LbbClient.wait_for_index`) for completion.
        """
        return self._request(
            "POST",
            "/v1/index/run",
            params={"background": "true" if background else None},
        )

    def index_submit(
        self, body: Body | None = None, *, idempotency_key: str
    ) -> models.SearchIndexJobStatusResponse:
        """Submit a durable full-index job with a reconnect-safe id."""
        return self._model_request(
            models.SearchIndexJobStatusResponse,
            "POST",
            "/v1/index/jobs",
            body=body or {},
            idempotency_key=idempotency_key,
        )

    def index_job(self, job_id: str) -> models.SearchIndexJobStatusResponse:
        """Poll per-family index progress or terminal failure."""
        return self._model_request(
            models.SearchIndexJobStatusResponse,
            "GET",
            "/v1/index/jobs",
            params={"job_id": job_id},
        )

    def cancel_index_job(self, job_id: str) -> models.SearchIndexJobStatusResponse:
        """Cancel a durable full-index job."""
        return self._model_request(
            models.SearchIndexJobStatusResponse,
            "DELETE",
            "/v1/index/jobs",
            params={"job_id": job_id},
        )

    def index_delta(self) -> Any:
        """Append a BM25 delta segment for the unindexed WAL tail."""
        return self._request("POST", "/v1/index/delta")

    def index_gc(
        self, *, keep_runs: int | None = None, dry_run: bool | None = None
    ) -> Any:
        """Preview or delete superseded persisted index runs."""
        return self._request(
            "POST", "/v1/index/gc", params={"keep_runs": keep_runs, "dry_run": dry_run}
        )

    def index_gc_submit(
        self, body: Body | None = None, *, idempotency_key: str
    ) -> models.IndexGcJobStatusResponse:
        """Submit durable index GC with reconnect-safe progress."""
        return self._model_request(
            models.IndexGcJobStatusResponse,
            "POST",
            "/v1/index/gc-jobs",
            body=body or {},
            idempotency_key=idempotency_key,
        )

    def index_gc_job(self, job_id: str) -> models.IndexGcJobStatusResponse:
        """Poll durable index-GC planning and deletion progress."""
        return self._model_request(
            models.IndexGcJobStatusResponse,
            "GET",
            "/v1/index/gc-jobs",
            params={"job_id": job_id},
        )

    def cancel_index_gc_job(self, job_id: str) -> models.IndexGcJobStatusResponse:
        """Cancel durable index garbage collection."""
        return self._model_request(
            models.IndexGcJobStatusResponse,
            "DELETE",
            "/v1/index/gc-jobs",
            params={"job_id": job_id},
        )

    def compact(
        self, *, min_tail_commits: int | None = None, max_segments: int | None = None
    ) -> Any:
        """Fold the WAL tail into snapshot segments."""
        return self._request(
            "POST",
            "/v1/graph/compact",
            params={"min_tail_commits": min_tail_commits, "max_segments": max_segments},
        )

    # --- inspection ---

    def status(self) -> Any:
        """Server, graph, and persisted-index status."""
        return self._request("GET", "/v1/status")

    def metadata(
        self,
        *,
        include_objects: bool | None = None,
        include_indexes: bool | None = None,
        include_temporal_coverage: bool | None = None,
    ) -> Any:
        """Graph footprint, WAL tail, and index coverage.

        Exact recursive object inventory is opt-in because its cost grows with
        object count.
        """
        return self._request(
            "GET",
            "/v1/graph/metadata",
            params={
                "include_objects": include_objects,
                "include_indexes": include_indexes,
                "include_temporal_coverage": include_temporal_coverage,
            },
        )

    def metadata_model(
        self,
        *,
        include_objects: bool | None = None,
        include_indexes: bool | None = None,
        include_temporal_coverage: bool | None = None,
    ) -> models.GraphMetadataResponse:
        """Graph metadata validated as ``GraphMetadataResponse``."""
        return self._model_request(
            models.GraphMetadataResponse,
            "GET",
            "/v1/graph/metadata",
            params={
                "include_objects": include_objects,
                "include_indexes": include_indexes,
                "include_temporal_coverage": include_temporal_coverage,
            },
        )

    def summary(
        self,
        *,
        consistency: str | None = None,
        min_indexed_seq: int | None = None,
    ) -> Any:
        """Graph counts and type/relation buckets. ``consistency`` /
        ``min_indexed_seq`` (A5) ride the URL query string."""
        return self._request(
            "GET",
            "/v1/graph/summary",
            params=self._consistency_params(consistency, min_indexed_seq) or None,
        )

    def summary_model(self) -> models.GraphSummaryResponse:
        """Graph counts validated as ``GraphSummaryResponse``."""
        return self._model_request(
            models.GraphSummaryResponse, "GET", "/v1/graph/summary"
        )

    def graph_edges(
        self,
        *,
        id: str | None = None,
        type: str | None = None,
        name: str | None = None,
        direction: str | None = None,
        relation: str | None = None,
        q: str | None = None,
        limit: int | None = None,
        cursor: str | int | None = None,
        offset: int | None = None,
        as_of: str | None = None,
        as_of_commit_seq: int | None = None,
    ) -> Any:
        """Paged edge listing in the unified list envelope.

        Scope to one node with ``id`` (or ``type`` + ``name``) and a
        ``direction`` (``out`` / ``in`` / ``both``) to walk **every** edge of a
        high-degree node. Returns ``{object, data, has_more, next_cursor,
        snapshot, total_count}`` — the same envelope as ``entities.list`` — so
        page by feeding ``next_cursor`` back as ``cursor`` (the legacy ``offset``
        is still accepted). Optional ``relation`` / ``q`` filters and an
        ``as_of`` / ``as_of_commit_seq`` snapshot pin. Each edge row carries
        ``valid_time``, so a page is enough to reconstruct a per-edge timeline.
        """
        return self._request(
            "GET",
            "/v1/graph/edges",
            params={
                "id": id,
                "type": type,
                "name": name,
                "direction": direction,
                "relation": relation,
                "q": q,
                "limit": limit,
                "cursor": cursor,
                "offset": offset,
                "as_of": as_of,
                "as_of_commit_seq": as_of_commit_seq,
            },
        )

    def graph_edges_page(self, **kwargs: Any) -> ListPage[models.GraphEdgeRow]:
        """Paged edges with rows validated as ``GraphEdgeRow``."""
        return self._page_request(models.GraphEdgeRow, self.graph_edges(**kwargs))

    def list_graphs(self) -> Any:
        """List the graphs (and branches) under the scoped tenant."""
        return self._request("GET", "/v1/graphs")

    def list_graphs_model(self) -> models.GraphListResponse:
        """List graphs with a typed ``GraphListResponse``."""
        return self._model_request(models.GraphListResponse, "GET", "/v1/graphs")

    def _model_request(
        self,
        model_cls: type[ModelT],
        method: str,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        body: Body | None = None,
        content: str | None = None,
        content_type: str | None = None,
        idempotency_key: str | None = None,
        options: RequestOptions | None = None,
    ) -> Any:
        raise NotImplementedError

    def _page_request(self, row_model: type[RowT], payload: Any) -> Any:
        raise NotImplementedError

    def _iter_entity_pages(self, **kwargs: Any) -> Any:
        raise NotImplementedError

    def _iter_entity_rows(self, **kwargs: Any) -> Any:
        raise NotImplementedError

    def sparql(
        self,
        query: str,
        *,
        reason: bool | None = None,
        entailment: str | None = None,
        as_of_valid_time: str | None = None,
        as_of_commit_seq: int | None = None,
        limit: int | None = None,
        offset: int | None = None,
        consistency: str | None = None,
        min_indexed_seq: int | None = None,
    ) -> Any:
        """Run SPARQL text; concrete transports return or await parsed results."""
        raise NotImplementedError


class _GraphNamespace:
    def __init__(self, client: _BaseLbbClient, graph: str, branch: str | None) -> None:
        self._client = client
        self._graph = graph
        self._branch = branch
        self.facts = _FactsNamespace(client, graph, branch)

    def delete(self, *, confirm: str) -> models.GraphDeleteResponse:
        """Delete and deregister this whole graph, including every branch."""
        return self._client._model_request(
            models.GraphDeleteResponse,
            "POST",
            "/v1/graph/delete",
            params={"graph": self._graph, "branch": self._branch, "confirm": confirm},
            options={"retry": True},
        )

    def delete_branch(self, *, confirm: str) -> models.GraphBranchDeleteResponse:
        """Delete this branch; the graph's final live branch is protected."""
        return self._client._model_request(
            models.GraphBranchDeleteResponse,
            "DELETE",
            "/v1/graph/branch",
            params={"graph": self._graph, "branch": self._branch, "confirm": confirm},
        )

    def embedding_config(
        self, *, options: RequestOptions | None = None
    ) -> models.ManagedEmbeddingConfigResponse:
        return self._client._model_request(
            models.ManagedEmbeddingConfigResponse,
            "GET",
            "/v1/graph/embedding",
            params={"graph": self._graph, "branch": self._branch},
            options=options,
        )

    def embedding_models(
        self,
        *,
        options: RequestOptions | None = None,
    ) -> models.ManagedEmbeddingModelsResponse:
        return self._client._model_request(
            models.ManagedEmbeddingModelsResponse,
            "GET",
            "/v1/graph/embedding/models",
            params={
                "graph": self._graph,
                "branch": self._branch,
            },
            options=options,
        )

    def set_embedding_model(
        self, model_id: str, *, auto_embed_query: bool = True
    ) -> models.ManagedEmbeddingConfigResponse:
        return self.set_embedding_config(
            {
                "model_id": model_id,
                "service": "open_router",
                "auto_embed_query": auto_embed_query,
            }
        )

    def set_embedding_config(self, body: Body) -> models.ManagedEmbeddingConfigResponse:
        return self._client._model_request(
            models.ManagedEmbeddingConfigResponse,
            "POST",
            "/v1/graph/embedding",
            params={"graph": self._graph, "branch": self._branch},
            body=body,
        )

    def backfill_embeddings(
        self,
        *,
        batch_size: int | None = None,
        limit: int | None = None,
        full: bool | None = None,
        idempotency_key: str | None = None,
        timeout: float = 1800.0,
        poll_interval: float = 2.0,
    ) -> models.ManagedEmbeddingBackfillResponse:
        status = self.submit_embedding_backfill(
            {"batch_size": batch_size, "limit": limit, "full": bool(full)},
            idempotency_key=idempotency_key
            or self._client.idempotency_key("embedding-backfill"),
        )
        deadline = time.monotonic() + timeout
        while status.status in {"pending", "running"}:
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"embedding backfill {status.job_id} exceeded {timeout}s"
                )
            time.sleep(poll_interval)
            status = self.embedding_backfill_job(status.job_id)
        if status.status != "succeeded" or status.result is None:
            raise RuntimeError(
                status.terminal_error
                or f"embedding backfill {status.job_id} ended {status.status}"
            )
        return status.result

    def submit_embedding_backfill(
        self, body: Body, *, idempotency_key: str
    ) -> models.ManagedEmbeddingBackfillJobStatusResponse:
        return self._client._model_request(
            models.ManagedEmbeddingBackfillJobStatusResponse,
            "POST",
            "/v1/graph/embedding/backfill-jobs",
            params={
                "graph": self._graph,
                "branch": self._branch,
            },
            body=body,
            idempotency_key=idempotency_key,
        )

    def embedding_backfill_job(
        self, job_id: str
    ) -> models.ManagedEmbeddingBackfillJobStatusResponse:
        return self._client._model_request(
            models.ManagedEmbeddingBackfillJobStatusResponse,
            "GET",
            "/v1/graph/embedding/backfill-jobs",
            params={
                "graph": self._graph,
                "branch": self._branch,
                "job_id": job_id,
            },
        )

    def promote_embedding(
        self, *, run_id: str, allow_regression: bool | None = None
    ) -> models.ManagedEmbeddingPromoteResponse:
        return self._client._model_request(
            models.ManagedEmbeddingPromoteResponse,
            "POST",
            "/v1/graph/embedding/promote",
            params={
                "graph": self._graph,
                "branch": self._branch,
                "run_id": run_id,
                "allow_regression": allow_regression,
            },
        )

    def retract(self, body: Body, *, idempotency_key: str | None = None) -> Any:
        """Retract edges/entities from the scoped graph. See :meth:`LbbClient.retract`."""
        return self._client._request(
            "POST",
            "/v1/graph/retract",
            params={"graph": self._graph, "branch": self._branch},
            body=body,
            idempotency_key=idempotency_key or self._client.idempotency_key("retract"),
        )

    def retract_model(
        self, body: Body, *, idempotency_key: str | None = None
    ) -> models.GraphRetractResponse:
        """Retract and validate the response as ``GraphRetractResponse``."""
        return self._client._model_request(
            models.GraphRetractResponse,
            "POST",
            "/v1/graph/retract",
            params={"graph": self._graph, "branch": self._branch},
            body=body,
            idempotency_key=idempotency_key or self._client.idempotency_key("retract"),
        )

    def export_rdf(
        self,
        *,
        format: str = "turtle",
        max_triples: int | None = None,
        as_of_valid_time: str | None = None,
        as_of_commit_seq: int | None = None,
        entailment: str | None = None,
        reason: bool | None = None,
    ) -> Any:
        """Export this graph's snapshot as Turtle, N-Triples, TriG, or N-Quads."""
        if format not in {"turtle", "ntriples", "trig", "nquads"}:
            raise ValueError("format must be 'turtle', 'ntriples', 'trig', or 'nquads'")
        return self._client._request(
            "GET",
            "/v1/graph/export/rdf",
            params={
                "graph": self._graph,
                "branch": self._branch,
                "format": "nt" if format == "ntriples" else format,
                "max_triples": max_triples,
                "as_of_valid_time": as_of_valid_time,
                "as_of_commit_seq": as_of_commit_seq,
                "entailment": entailment,
                "reason": reason,
            },
        )

    def export_rdf_preview(
        self,
        *,
        format: str = "turtle",
        max_triples: int = 100,
        as_of_valid_time: str | None = None,
        as_of_commit_seq: int | None = None,
        entailment: str | None = None,
        reason: bool | None = None,
    ) -> models.RdfExportPreviewResponse:
        """Return a bounded typed RDF preview for this explicit graph scope."""
        if format not in {"turtle", "ntriples", "trig", "nquads"}:
            raise ValueError("format must be 'turtle', 'ntriples', 'trig', or 'nquads'")
        return self._client._model_request(
            models.RdfExportPreviewResponse,
            "GET",
            "/v1/graph/export/rdf",
            params={
                "graph": self._graph,
                "branch": self._branch,
                "format": "nt" if format == "ntriples" else format,
                "max_triples": max_triples,
                "truncate": "true",
                "as_of_valid_time": as_of_valid_time,
                "as_of_commit_seq": as_of_commit_seq,
                "entailment": entailment,
                "reason": reason,
            },
        )


class _FactsNamespace:
    def __init__(self, client: _BaseLbbClient, graph: str, branch: str | None) -> None:
        self._client = client
        self._graph = graph
        self._branch = branch

    def create(self, body: Body, *, idempotency_key: str | None = None) -> Any:
        params = {"graph": self._graph, "branch": self._branch}
        return self._client._request(
            "POST",
            "/v1/graph/commit",
            params=params,
            body=body,
            idempotency_key=idempotency_key
            or self._client.idempotency_key("facts.create"),
        )

    def create_model(
        self, body: Body, *, idempotency_key: str | None = None
    ) -> models.GraphCommitResponse:
        """Create facts and validate the response as ``GraphCommitResponse``."""
        params = {"graph": self._graph, "branch": self._branch}
        return self._client._model_request(
            models.GraphCommitResponse,
            "POST",
            "/v1/graph/commit",
            params=params,
            body=body,
            idempotency_key=idempotency_key
            or self._client.idempotency_key("facts.create"),
        )

    def import_ndjson(
        self,
        lines: Sequence[Mapping[str, Any]] | str,
        *,
        batch: int | None = None,
        strict: bool | None = None,
        observed_at: str | None = None,
        index: bool | None = None,
        idempotency_key: str | None = None,
    ) -> Any:
        """Bulk-load a dataset as NDJSON. See :meth:`LbbClient.import_ndjson`."""
        ndjson = (
            lines
            if isinstance(lines, str)
            else "\n".join(json.dumps(_coerce_body(line)) for line in lines)
        )
        return self._client._request(
            "POST",
            "/v1/graph/import",
            params={
                "graph": self._graph,
                "branch": self._branch,
                "batch": batch,
                "strict": strict,
                "observed_at": observed_at,
                "index": index,
            },
            content=ndjson,
            content_type="application/x-ndjson",
            idempotency_key=idempotency_key or self._client.idempotency_key("import"),
        )

    def import_rdf(
        self,
        rdf: str | None = None,
        *,
        ntriples: str | None = None,
        format: str = "ntriples",
        base_iri: str | None = None,
        graph_uri: str | None = None,
        blank_node_scope: str | None = None,
        batch: int | None = None,
        strict: bool | None = None,
        observed_at: str | None = None,
        resource_type: str | None = None,
        edge_idempotency: str | None = None,
        idempotency_key: str | None = None,
    ) -> Any:
        """Bulk-load N-Triples, Turtle, N-Quads, or TriG. See :meth:`LbbClient.import_rdf`."""
        if rdf is not None and ntriples is not None:
            raise TypeError("pass exactly one of rdf= or the deprecated ntriples=")
        if rdf is None:
            rdf = ntriples
        if rdf is None:
            raise TypeError("import_rdf() requires RDF text via rdf= or ntriples=")
        if ntriples is not None and format != "ntriples":
            raise ValueError(
                "the deprecated ntriples= keyword requires format='ntriples'"
            )
        content_types = {
            "ntriples": "application/n-triples",
            "turtle": "text/turtle",
            "nquads": "application/n-quads",
            "trig": "application/trig",
        }
        if format not in content_types:
            raise ValueError("format must be 'ntriples', 'turtle', 'nquads', or 'trig'")
        return self._client._request(
            "POST",
            "/v1/graph/import/rdf",
            params={
                "graph": self._graph,
                "branch": self._branch,
                "batch": batch,
                "strict": strict,
                "observed_at": observed_at,
                "format": format,
                "base_iri": base_iri,
                "graph_uri": graph_uri,
                "blank_node_scope": blank_node_scope,
                "resource_type": resource_type,
                "edge_idempotency": edge_idempotency,
            },
            content=rdf,
            content_type=content_types[format],
            idempotency_key=idempotency_key
            or self._client.idempotency_key("import-rdf"),
        )


class _SearchNamespace:
    def __init__(self, client: _BaseLbbClient) -> None:
        self._client = client

    def __call__(
        self,
        query: str,
        *,
        top_k: int | None = None,
        source: str | None = None,
        consistency: str | None = None,
        min_indexed_seq: int | None = None,
    ) -> Any:
        """Quick semantic hybrid search while preserving ``client.search(...)``."""
        return self.hybrid(
            query,
            top_k=top_k,
            source=source,
            consistency=consistency,
            min_indexed_seq=min_indexed_seq,
        )

    def hybrid(self, query_or_body: str | Body, **kwargs: Any) -> Any:
        options = kwargs.get("options")
        # A5: resolve the client default consistency and thread the floor.
        consistency = self._client._resolve_consistency(kwargs.get("consistency"))
        min_indexed_seq = kwargs.get("min_indexed_seq")
        if isinstance(query_or_body, str):
            params = {
                "query": query_or_body,
                "top_k": kwargs.get("top_k"),
                "source": kwargs.get("source"),
                "consistency": consistency,
                "min_indexed_seq": min_indexed_seq,
                "targets": (
                    ",".join(kwargs["targets"]) if kwargs.get("targets") else None
                ),
                "profile": kwargs.get("profile"),
            }
            return self._client._request(
                "GET", "/v1/search", params=params, options=options
            )
        # Hybrid graph search carries consistency on the nested `search` options.
        body = query_or_body
        if (consistency is not None or min_indexed_seq is not None) and isinstance(
            body, Mapping
        ):
            merged = dict(body)
            search = dict(merged.get("search") or {})
            if consistency is not None and search.get("consistency") is None:
                search["consistency"] = consistency
            if min_indexed_seq is not None and search.get("min_indexed_seq") is None:
                search["min_indexed_seq"] = min_indexed_seq
            merged["search"] = search
            body = merged
        return self._client._request(
            "POST",
            "/v1/graph/search",
            body=body,
            options=_read_options(options),
        )


class _ContextNamespace:
    """Typed grounding, completion, decoding, and answer operations."""

    def __init__(self, client: _BaseLbbClient) -> None:
        self._client = client

    def ask(
        self, body: Body, *, options: RequestOptions | None = None
    ) -> models.AskResponse:
        return self._client._model_request(
            models.AskResponse,
            "POST",
            "/v1/ask",
            body=body,
            options=_read_options(options),
        )

    def suggest(
        self, body: Body, *, options: RequestOptions | None = None
    ) -> models.SearchSuggestResponse:
        return self._client._model_request(
            models.SearchSuggestResponse,
            "POST",
            "/v1/search/suggest",
            body=body,
            options=_read_options(options),
        )

    def resolve(
        self, body: Body, *, options: RequestOptions | None = None
    ) -> models.ResolveTermResponse:
        return self._client._model_request(
            models.ResolveTermResponse,
            "POST",
            "/v1/search/resolve-term",
            body=body,
            options=_read_options(options),
        )

    def decode(
        self, body: Body, *, options: RequestOptions | None = None
    ) -> models.DecodeResponse:
        return self._client._model_request(
            models.DecodeResponse,
            "POST",
            "/v1/decode",
            body=body,
            options=_read_options(options),
        )

    def groundability(
        self, *, sample: int | None = None, options: RequestOptions | None = None
    ) -> models.GroundabilityReport:
        return self._client._model_request(
            models.GroundabilityReport,
            "GET",
            "/v1/graph/groundability",
            params={"sample": sample},
            options=_read_options(options),
        )


class _OntologyNamespace:
    """Typed ontology discovery and lifecycle operations."""

    def __init__(self, client: _BaseLbbClient) -> None:
        self._client = client

    def view(
        self, *, counts: bool = False, options: RequestOptions | None = None
    ) -> models.OntologyView:
        return self._client._model_request(
            models.OntologyView,
            "GET",
            "/v1/ontology",
            params={"counts": True if counts else None},
            options=options,
        )

    def conformance(
        self, *, options: RequestOptions | None = None
    ) -> models.SchemaAuditReport:
        return self._client._model_request(
            models.SchemaAuditReport,
            "GET",
            "/v1/ontology/conformance",
            options=options,
        )

    def search(
        self, body: Body, *, options: RequestOptions | None = None
    ) -> models.OntologySearchResponse:
        return self._client._model_request(
            models.OntologySearchResponse,
            "POST",
            "/v1/ontology/search",
            body=body,
            options=_read_options(options),
        )

    def resolve(
        self, body: Body, *, options: RequestOptions | None = None
    ) -> models.OntologyResolveResponse:
        return self._client._model_request(
            models.OntologyResolveResponse,
            "POST",
            "/v1/ontology/resolve",
            body=body,
            options=_read_options(options),
        )

    def define(self, body: Body) -> models.OntologyDefineResponse:
        return self._client._model_request(
            models.OntologyDefineResponse, "POST", "/v1/ontology/define", body=body
        )

    def evolve(
        self, body: Body, *, dry_run: bool = False
    ) -> models.OntologyEvolveResponse:
        """Apply an ontology patch, or preview it without mutation."""
        return self._client._model_request(
            models.OntologyEvolveResponse,
            "POST",
            "/v1/ontology/evolve",
            params={"dry_run": "true" if dry_run else None},
            body=body,
        )

    def induce(
        self, body: Body, *, options: RequestOptions | None = None
    ) -> models.OntologyInduceResponse:
        """Suggest ontology changes from the current graph without mutating it."""
        return self._client._model_request(
            models.OntologyInduceResponse,
            "POST",
            "/v1/ontology/induce",
            body=body,
            options=_read_options(options),
        )

    def draft_create(self, body: Body) -> models.OntologyDraft:
        """Create a durable proposal from samples without ingesting them."""
        return self._client._model_request(
            models.OntologyDraft, "POST", "/v1/ontology/drafts", body=body
        )

    def draft_get(self, draft_id: str) -> models.OntologyDraft:
        return self._client._model_request(
            models.OntologyDraft,
            "GET",
            "/v1/ontology/drafts",
            params={"draft_id": draft_id},
        )

    def draft_validate(self, draft_id: str) -> models.OntologyDraft:
        return self._client._model_request(
            models.OntologyDraft,
            "POST",
            "/v1/ontology/drafts/validate",
            params={"draft_id": draft_id},
            options=_read_options(),
        )

    def draft_promote(
        self, draft_id: str, *, idempotency_key: str | None = None
    ) -> models.OntologyDraft:
        return self._client._model_request(
            models.OntologyDraft,
            "POST",
            "/v1/ontology/drafts/promote",
            params={"draft_id": draft_id},
            idempotency_key=idempotency_key
            or self._client.idempotency_key("ontology-draft-promote"),
        )

    def draft_reject(self, draft_id: str, reason: str) -> models.OntologyDraft:
        return self._client._model_request(
            models.OntologyDraft,
            "POST",
            "/v1/ontology/drafts/reject",
            params={"draft_id": draft_id, "reason": reason},
        )


class _QueryNamespace:
    """Typed structured, SPARQL-text, analytical, and reasoning queries."""

    def __init__(self, client: _BaseLbbClient) -> None:
        self._client = client

    def structured(
        self,
        body: Body,
        *,
        consistency: str | None = None,
        min_indexed_seq: int | None = None,
        options: RequestOptions | None = None,
    ) -> models.SparqlSelectResponse:
        return self._client._model_request(
            models.SparqlSelectResponse,
            "POST",
            "/v1/query/sparql",
            body=self._client._with_consistency(body, consistency, min_indexed_seq),
            options=_read_options(options),
        )

    def sparql(
        self,
        query: str,
        *,
        reason: bool | None = None,
        entailment: str | None = None,
        as_of_valid_time: str | None = None,
        as_of_commit_seq: int | None = None,
        limit: int | None = None,
        offset: int | None = None,
        consistency: str | None = None,
        min_indexed_seq: int | None = None,
    ) -> Any:
        return self._client.sparql(
            query,
            reason=reason,
            entailment=entailment,
            as_of_valid_time=as_of_valid_time,
            as_of_commit_seq=as_of_commit_seq,
            limit=limit,
            offset=offset,
            consistency=consistency,
            min_indexed_seq=min_indexed_seq,
        )

    def analytics(
        self, body: Body, *, options: RequestOptions | None = None
    ) -> models.AnalyticQueryResponse:
        return self._client._model_request(
            models.AnalyticQueryResponse,
            "POST",
            "/v1/query/analytics",
            body=body,
            options=_read_options(options),
        )

    def conflicts(
        self, body: Body, *, options: RequestOptions | None = None
    ) -> models.GovernedConflictAggregationResponse:
        return self._client._model_request(
            models.GovernedConflictAggregationResponse,
            "POST",
            "/v1/query/conflicts",
            body=body,
            options=_read_options(options),
        )

    def shacl(
        self, body: Body, *, options: RequestOptions | None = None
    ) -> models.ShaclQueryResponse:
        return self._client._model_request(
            models.ShaclQueryResponse,
            "POST",
            "/v1/query/shacl",
            body=body,
            options=_read_options(options),
        )

    def infer(
        self, body: Body, *, options: RequestOptions | None = None
    ) -> models.InferenceRunResponse:
        return self._client._model_request(
            models.InferenceRunResponse,
            "POST",
            "/v1/inference/run",
            body=body,
            options=_read_options(options),
        )

    def premises(
        self, body: Body, *, options: RequestOptions | None = None
    ) -> models.RetrievalPremiseResponse:
        return self._client._model_request(
            models.RetrievalPremiseResponse,
            "POST",
            "/v1/inference/retrieval-premises",
            body=body,
            options=_read_options(options),
        )


class _SchemaNamespace:
    def __init__(self, client: _BaseLbbClient) -> None:
        self._client = client

    def view(self, *, audit: bool = False) -> Any:
        """Active graph schema bundle: ontology plus activated SHACL shapes."""
        return self._client._request(
            "GET",
            "/v1/schema",
            params={"audit": "true" if audit else None},
        )

    def view_model(self, *, audit: bool = False) -> models.SchemaBundleView:
        """Active schema bundle validated as ``SchemaBundleView``."""
        return self._client._model_request(
            models.SchemaBundleView,
            "GET",
            "/v1/schema",
            params={"audit": "true" if audit else None},
        )

    def preview(self, body: Body) -> Any:
        """Preview a proposed RDF/SHACL schema bundle and audit current data."""
        return self._client._request("POST", "/v1/schema/preview", body=body)

    def preview_model(self, body: Body) -> models.SchemaPreviewResponse:
        """Schema preview validated as ``SchemaPreviewResponse``."""
        return self._client._model_request(
            models.SchemaPreviewResponse, "POST", "/v1/schema/preview", body=body
        )

    def publish(self, body: Body) -> Any:
        """Activate a previewed SHACL schema bundle for this graph branch."""
        return self._client._request("POST", "/v1/schema/publish", body=body)

    def publish_model(self, body: Body) -> models.SchemaPublishResponse:
        """Schema publish response validated as ``SchemaPublishResponse``."""
        return self._client._model_request(
            models.SchemaPublishResponse, "POST", "/v1/schema/publish", body=body
        )

    def audit(self) -> Any:
        """Audit current data against the active SHACL schema bundle."""
        return self._client._request("POST", "/v1/schema/audit")

    def audit_model(self) -> models.SchemaAuditReport:
        """Schema audit validated as ``SchemaAuditReport``."""
        return self._client._model_request(
            models.SchemaAuditReport, "POST", "/v1/schema/audit"
        )


class _IndexNamespace:
    def __init__(self, client: _BaseLbbClient) -> None:
        self._client = client

    def run(
        self,
        *,
        wait: bool = True,
        background: bool | None = None,
        body: Body | None = None,
    ) -> Any:
        run_background = (
            background if background is not None else (False if wait else True)
        )
        return self._client._request(
            "POST",
            "/v1/index/run",
            params={"background": "true" if run_background else None},
            body=body,
        )


class _EntityNamespace:
    def __init__(self, client: _BaseLbbClient) -> None:
        self._client = client

    def sample(
        self,
        *,
        type: str,  # noqa: A002 - mirrors the HTTP query param.
        limit: int | None = None,
        options: RequestOptions | None = None,
    ) -> models.EntityTypeSampleResponse:
        """Return exact type cardinality and a bounded ranged-index sample.

        The server returns ``index_busy`` instead of falling back to an
        exhaustive snapshot scan while the adjacency index is unavailable.
        """
        return self._client._model_request(
            models.EntityTypeSampleResponse,
            "GET",
            "/v1/graph/entities/sample",
            params={"type": type, "limit": limit},
            options=_read_options(options),
        )

    def list(
        self,
        *,
        type: str | None = None,  # noqa: A002 - mirrors the HTTP query param.
        limit: int | None = None,
        cursor: str | int | None = None,
        offset: int | None = None,
        query: str | None = None,
        fields: str | Sequence[str] | None = None,
        ids: str | Sequence[str] | None = None,
    ) -> Any:
        """Browse entities as the unified list envelope.

        Pass ``fields`` (names, or ``"*"`` for all) to inline each row's typed
        attributes as native JSON (under ``attributes``) — "list entities and
        their titles" in one call instead of a list plus N point lookups — or
        ``ids`` to bulk-fetch a specific set. Page with ``cursor`` from the
        previous ``next_cursor``.
        """

        def _csv(value: str | Sequence[str] | None) -> str | None:
            if value is None or isinstance(value, str):
                return value
            return ",".join(value)

        return self._client._request(
            "GET",
            "/v1/graph/entities",
            params={
                "type": type,
                "limit": limit,
                "cursor": cursor,
                "offset": offset,
                "q": query,
                "fields": _csv(fields),
                "ids": _csv(ids),
            },
        )

    def list_page(self, **kwargs: Any) -> Any:
        """Paged entities with rows validated as ``EntityExplorerRow``."""
        return self._client._page_request(models.EntityExplorerRow, self.list(**kwargs))

    def filter(
        self, body: Body, *, options: RequestOptions | None = None
    ) -> models.EntityFilterResponse:
        """Snapshot-pinned entity filtering with typed projected attributes."""
        return self._client._model_request(
            models.EntityFilterResponse,
            "POST",
            "/v1/graph/entities/filter",
            body=body,
            options=_read_options(options),
        )

    def pages(self, **kwargs: Any) -> Any:
        """Follow list cursors and yield typed :class:`ListPage` envelopes."""
        return self._client._iter_entity_pages(**kwargs)

    def iter(self, **kwargs: Any) -> Any:
        """Follow list cursors and yield typed ``EntityExplorerRow`` objects."""
        return self._client._iter_entity_rows(**kwargs)

    def filter_by_attributes(
        self,
        *,
        patterns: Sequence[Mapping[str, Any]],
        where: Mapping[str, Any] | Sequence[Mapping[str, Any]],
        filters: Sequence[Mapping[str, Any]] | None = None,
        select: Sequence[str] | None = None,
        limit: int | None = None,
        offset: int | None = None,
        as_of_valid_time: str | None = None,
        as_of_commit_seq: int | None = None,
        order_by: Sequence[Mapping[str, Any]] | None = None,
        reason: bool | None = None,
        max_solutions: int | None = None,
        max_object_reads: int | None = None,
        max_fetched_bytes: int | None = None,
    ) -> Any:
        """Filter relation-bound entities by typed attributes.

        Convenience wrapper over the structured SPARQL route: ``patterns`` bind
        variables through graph relations, then ``where`` compares ontology
        property fields on those variables without making callers write RDF IRIs.
        """
        return self._client._request(
            "POST",
            "/v1/query/sparql",
            body=self._filter_by_attributes_body(
                patterns=patterns,
                where=where,
                filters=filters,
                select=select,
                limit=limit,
                offset=offset,
                as_of_valid_time=as_of_valid_time,
                as_of_commit_seq=as_of_commit_seq,
                order_by=order_by,
                reason=reason,
                max_solutions=max_solutions,
                max_object_reads=max_object_reads,
                max_fetched_bytes=max_fetched_bytes,
            ),
        )

    def filter_by_attributes_model(self, **kwargs: Any) -> models.SparqlSelectResponse:
        """Relation-bound attribute query validated as ``SparqlSelectResponse``."""
        return self._client._model_request(
            models.SparqlSelectResponse,
            "POST",
            "/v1/query/sparql",
            body=self._filter_by_attributes_body(**kwargs),
        )

    def _filter_by_attributes_body(
        self,
        *,
        patterns: Sequence[Mapping[str, Any]],
        where: Mapping[str, Any] | Sequence[Mapping[str, Any]],
        filters: Sequence[Mapping[str, Any]] | None = None,
        select: Sequence[str] | None = None,
        limit: int | None = None,
        offset: int | None = None,
        as_of_valid_time: str | None = None,
        as_of_commit_seq: int | None = None,
        order_by: Sequence[Mapping[str, Any]] | None = None,
        reason: bool | None = None,
        max_solutions: int | None = None,
        max_object_reads: int | None = None,
        max_fetched_bytes: int | None = None,
    ) -> dict[str, Any]:
        where_items = [where] if isinstance(where, Mapping) else list(where)
        default_var = _first_pattern_variable(patterns)
        body = {
            "patterns": list(patterns),
            "filters": [
                *(list(filters) if filters is not None else []),
                *[_attribute_filter(item, default_var) for item in where_items],
            ],
            "select": list(select) if select is not None else None,
            "limit": limit,
            "offset": offset,
            "as_of_valid_time": as_of_valid_time,
            "as_of_commit_seq": as_of_commit_seq,
            "order_by": list(order_by) if order_by is not None else None,
            "reason": reason,
            "max_solutions": max_solutions,
            "max_object_reads": max_object_reads,
            "max_fetched_bytes": max_fetched_bytes,
        }
        return {key: value for key, value in body.items() if value is not None}
