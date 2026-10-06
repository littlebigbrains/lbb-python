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

import codecs
import enum
import json
import random
import re
import time
import uuid
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Final, Generic, Literal, TypedDict, TypeVar

import httpx
from pydantic import BaseModel, RootModel

from . import models
from ._version import __version__

DEFAULT_BASE_URL = "http://127.0.0.1:7400"
# The integrations API that ``client.integrations`` calls with the same key.
DEFAULT_INTEGRATIONS_URL = "https://api.littlebigbrain.com"
# Generous default: commits over a large corpus and long administration calls
# can run well past a few seconds.
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


class _Reset(enum.Enum):
    """The type of :data:`RESET`."""

    RESET = "RESET"

    def __repr__(self) -> str:
        return "RESET"


#: Pass as a setting to set it back to its default. The client sends it as
#: JSON ``null``; a setting left at ``None`` stays off the wire and keeps its
#: value. Example: ``client.embeddings.set_search_settings(blend=RESET)``.
RESET: Final = _Reset.RESET
ModelT = TypeVar("ModelT", bound=BaseModel)
RowT = TypeVar("RowT", bound=BaseModel)


class RequestOptions(TypedDict, total=False):
    """Per-request transport overrides accepted by :meth:`raw_request`.

    ``retry`` overrides the retry-safety classification: ``True`` retries a
    ``429``, a ``5xx`` and a transport failure; ``"rate_limited"`` retries only
    a retryable ``429``, which the server returns before it runs the request.
    """

    max_retries: int
    retry: bool | Literal["rate_limited"]
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
        # Per-item reasons for a refusal, e.g. ``conflicts`` of ``starter_conflict``.
        self.details = self.error.get("details")
        super().__init__(
            self.error.get("message") or f"Little Big Brain {status_code}: {body}"
        )


class LbbCapabilityError(RuntimeError):
    """Raised before upload when the server lacks a required additive capability."""

    def __init__(self, capability: str) -> None:
        self.capability = capability
        super().__init__(
            f"Little Big Brain server does not advertise {capability}; "
            "upgrade the server before using this SDK method"
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
    - :attr:`next_cursor` — pass to ``sparql(query, cursor=...)`` to continue the
      same snapshot; start with ``cursor=""`` and an indexed ordered LIMIT.
    - :attr:`snapshot` — served watermark and current head metadata, when present.
    - :attr:`profile` — what the server measured for the request (timings,
      reads, plan counters, the join order with estimates), when the call
      passed ``profile=True``.
    - :attr:`search` — how the search by meaning in the query ran (the plan,
      the hits asked for and bound, ``complete``, the lag of the vectors),
      when the query holds a ``search:similarTo`` triple.
    - :attr:`trace_id` — the eval trace the server recorded, when the call
      passed ``request``. Label the rows with ``evals.label(trace_id, …)``.
    """

    vars: list[str]
    bindings: list[dict[str, Any]]
    boolean: bool | None
    row_page: dict[str, Any] | None
    next_cursor: str | None = None
    snapshot: dict[str, Any] | None = None
    profile: dict[str, Any] | None = None
    search: dict[str, Any] | None = None
    trace_id: str | None = None

    @classmethod
    def from_results_json(
        cls,
        doc: Mapping[str, Any],
        row_page: Mapping[str, Any] | None = None,
        *,
        next_cursor: str | None = None,
        snapshot: Mapping[str, Any] | None = None,
        profile: Mapping[str, Any] | None = None,
        search: Mapping[str, Any] | None = None,
        trace_id: str | None = None,
    ) -> SparqlResults:
        """Build from a parsed SPARQL Results JSON document."""
        head = doc.get("head") or {}
        variables = list(head.get("vars") or [])
        page = dict(row_page) if row_page is not None else None
        retained = dict(snapshot) if snapshot is not None else None
        measured = dict(profile) if profile is not None else None
        searched = dict(search) if search is not None else None
        if "boolean" in doc:
            return cls(
                vars=variables,
                bindings=[],
                boolean=bool(doc["boolean"]),
                row_page=page,
                next_cursor=next_cursor,
                snapshot=retained,
                profile=measured,
                search=searched,
                trace_id=trace_id,
            )
        results = doc.get("results") or {}
        bindings = [dict(binding) for binding in results.get("bindings") or []]
        return cls(
            vars=variables,
            bindings=bindings,
            boolean=None,
            row_page=page,
            next_cursor=next_cursor,
            snapshot=retained,
            profile=measured,
            search=searched,
            trace_id=trace_id,
        )

    @classmethod
    def from_envelope(cls, envelope: Mapping[str, Any]) -> SparqlResults:
        """Build from the ``/v1/query/sparql-text`` envelope.

        That route carries the results document as a JSON *string* in
        ``results`` plus a sibling ``row_page``; this unwraps both.
        """
        raw = envelope.get("results")
        doc = json.loads(raw) if isinstance(raw, str) else (raw or {})
        return cls.from_results_json(
            doc,
            row_page=envelope.get("row_page"),
            next_cursor=envelope.get("next_cursor"),
            snapshot=envelope.get("snapshot"),
            profile=envelope.get("profile"),
            search=envelope.get("search"),
            trace_id=envelope.get("trace_id"),
        )

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


@dataclass(frozen=True)
class QueryAskResult:
    """What ``query.ask`` returns: the answer, the query that holds it, and its rows.

    - :attr:`answer` — the answer in plain words. ``None`` when the loop
      stopped before it answered (``error`` says why; ``rows`` then hold the
      best rows it read), and for ``mode="route"``.
    - :attr:`citations` — the IRIs the answer names. Each one appeared in the
      rows the loop read.
    - :attr:`steps` — the tool calls of the loop, in order (``n``, ``tool``,
      ``input``, ``ok``, ``rows``, ``error``, ``ms``).
    - :attr:`route` — the kind of question (``kind``), who chose it (``by``),
      and how sure the choice is (``confidence``).
    - :attr:`query` — the query whose rows hold the answer (``sparql``,
      ``entailment``, ``as_of_commit_seq``). For a comparison, the query at
      the later point. ``None`` for ``mode="route"``, and when no query
      answers the question.
    - :attr:`rationale` — one sentence: which rows answer the question, or
      why the loop stopped. For ``mode="route"``, who chose the route.
    - :attr:`rows` — the rows of :attr:`query` as ``{variable:
      lexical_value}`` dicts. Empty for a comparison and for ``mode="route"``.
    - :attr:`vars` — the projected variables.
    - :attr:`boolean` — the answer of an ``ASK`` query, ``None`` for a ``SELECT``.
    - :attr:`snapshot` — the snapshot the rows were read from, when the server
      names it.
    - :attr:`error` — why the loop stopped before it answered.
    - :attr:`trace_id` — the eval trace of the rows; label them with
      ``evals.label``.
    - :attr:`linked` — the names of the question the server linked to
      entities (``text``, ``iri``, ``label``, ``class``, ``score``, ``by``),
      for a "Did you mean …?"; empty when nothing linked.
    - :attr:`anchors` — what the server read about each anchored IRI
      (``iri``, ``found``, ``label``, ``types``, ``note``).
    - :attr:`history` — where a history answer read the graph: the date
      (``as_of_date``), its commit (``as_of_commit_seq``) and how the loop
      found it (``resolved_by``). For a comparison (``compare`` is ``True``):
      the rows ``added`` and ``removed``, and with a ``key`` the entities
      ``changed``, with ``totals``. ``None`` for other questions.
    - :attr:`response` — the whole ``POST /v1/query/ask`` response.
    """

    route: dict[str, Any]
    query: dict[str, Any] | None
    rationale: str
    rows: list[dict[str, Any]]
    vars: list[str]
    boolean: bool | None
    snapshot: dict[str, Any] | None
    error: str | None
    trace_id: str | None
    response: dict[str, Any]
    linked: list[dict[str, Any]] = dataclass_field(default_factory=list)
    anchors: list[dict[str, Any]] = dataclass_field(default_factory=list)
    history: dict[str, Any] | None = None
    answer: str | None = None
    citations: list[str] = dataclass_field(default_factory=list)
    steps: list[dict[str, Any]] = dataclass_field(default_factory=list)

    @classmethod
    def from_response(cls, response: Mapping[str, Any]) -> QueryAskResult:
        """Build from a ``POST /v1/query/ask`` response."""
        result = response.get("result")
        parsed = (
            SparqlResults.from_envelope(result) if isinstance(result, Mapping) else None
        )
        query = response.get("query")
        answer = response.get("answer")
        answer = answer if isinstance(answer, Mapping) else None
        return cls(
            route=dict(response.get("route") or {}),
            query=dict(query) if isinstance(query, Mapping) else None,
            rationale=str(response.get("rationale") or ""),
            rows=parsed.rows() if parsed is not None else [],
            vars=list(parsed.vars) if parsed is not None else [],
            boolean=parsed.boolean if parsed is not None else None,
            snapshot=parsed.snapshot if parsed is not None else None,
            error=response.get("error"),
            trace_id=result.get("trace_id") if isinstance(result, Mapping) else None,
            response=dict(response),
            linked=[dict(link) for link in response.get("linked") or []],
            anchors=[dict(anchor) for anchor in response.get("anchors") or []],
            history=(
                dict(response["history"])
                if isinstance(response.get("history"), Mapping)
                else None
            ),
            answer=str(answer["text"]) if answer is not None else None,
            citations=[str(iri) for iri in (answer or {}).get("citations") or []],
            steps=[dict(step) for step in response.get("steps") or []],
        )


#: The generated model of one event of a streamed question.
QueryAskEventModel = (
    models.QueryRewriteEventGrounding
    | models.QueryRewriteEventRoute
    | models.QueryRewriteEventStep
    | models.QueryRewriteEventAnswer
    | models.QueryRewriteEventDone
    | models.QueryRewriteEventError
)


@dataclass(frozen=True)
class QueryAskStreamEvent:
    """One event of ``query.ask_stream``.

    - :attr:`event` — the step: ``grounding``, ``route``, a ``step`` per tool
      call of the loop, ``answer``, and last ``done``. A second ``route``
      comes before ``answer`` when the loop chose another route.
      ``mode="route"`` sends ``grounding``, ``route`` and ``done``.
    - :attr:`data` — the event's JSON object as a dict. The ``data`` of
      ``done`` is the whole ``POST /v1/query/ask`` response.
      ``QueryAskResult.from_response(event.data)`` parses it as
      ``query.ask`` does.

    The stream raises an ``error`` event as :class:`LbbError`; it never
    yields one.
    """

    event: str
    data: dict[str, Any]

    def model(self) -> QueryAskEventModel:
        """Validate the event as its generated model, for example
        ``models.QueryRewriteEventRoute``."""
        return models.QueryRewriteEvent.model_validate(
            {"event": self.event, "data": self.data}
        ).root


def _coerce_body(body: Body | None) -> Any:
    if body is None:
        return None
    if isinstance(body, BaseModel):
        return body.model_dump(
            mode="json", exclude=_unset_optional_fields(body) or None
        )
    dump = getattr(body, "model_dump", None)
    if callable(dump):
        return dump(mode="json")
    return body


def _unset_optional_fields(value: Any) -> dict[Any, Any]:
    """The ``model_dump`` exclude spec for every ``None`` optional field in ``value``.

    The generated models declare an omitted field as ``X | None = None``, but
    the server decodes many of them as ``#[serde(default)] Vec<_>`` or
    ``bool``, which reject an explicit ``null`` (HTTP 400). No server field
    gives ``null`` a meaning apart from absence, so these fields are left off
    the wire. A required field keeps an explicit ``None``: a required JSON value
    such as ``WorkflowSignalRequest.value`` accepts ``null`` and rejects absence.
    """
    if isinstance(value, RootModel):
        return _unset_optional_fields(value.root)
    if isinstance(value, BaseModel):
        spec: dict[Any, Any] = {}
        for name, field in type(value).model_fields.items():
            item = getattr(value, name)
            if item is None:
                if not field.is_required():
                    spec[name] = True
            elif nested := _unset_optional_fields(item):
                spec[name] = nested
        return spec
    if isinstance(value, (list, tuple)):
        return {
            i: s for i, item in enumerate(value) if (s := _unset_optional_fields(item))
        }
    if isinstance(value, Mapping):
        return {
            k: s for k, item in value.items() if (s := _unset_optional_fields(item))
        }
    return {}


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


def _retry_after_header_seconds(value: str | None) -> int | float | None:
    """A ``Retry-After`` header in seconds, uncapped, or ``None`` when it is
    absent or unparseable. An HTTP date counts from now."""
    if not value:
        return None
    seconds = _parse_retry_after_header(value, None)
    if seconds is None:
        return None
    seconds = max(0.0, seconds)
    return int(seconds) if seconds.is_integer() else seconds


def _parse_error(
    status_code: int,
    body: str,
    request_id: str | None,
    retry_after: str | None = None,
) -> LbbError:
    """The :class:`LbbError` for a failed response.

    It reads the data plane's envelope ``{"error": {"code", "message", …}}``
    and the integrations API's ``{"ok": false, "error", "code", "details"}``.
    ``retry_after_seconds`` comes from the body's hint, else from the
    ``Retry-After`` header.
    """
    header_seconds = _retry_after_header_seconds(retry_after)
    try:
        parsed = json.loads(body)
    except json.JSONDecodeError:
        parsed = {}
    error = parsed.get("error") if isinstance(parsed, dict) else None
    if isinstance(error, str) and isinstance(parsed, dict):
        details = parsed.get("details")
        return LbbError(
            status_code,
            body,
            {
                "code": parsed.get("code") if isinstance(parsed.get("code"), str) else None,
                "message": error,
                "request_id": request_id,
                "retry_after_seconds": header_seconds,
                "details": details if isinstance(details, dict) else None,
            },
        )
    if isinstance(error, dict):
        if error.get("request_id") is None:
            error = {**error, "request_id": request_id}
        if error.get("retry_after_seconds") is None and header_seconds is not None:
            error = {**error, "retry_after_seconds": header_seconds}
        return LbbError(status_code, body, error)
    fallback: dict[str, Any] = {
        "type": "api_error",
        "code": "unstructured_error",
        "message": body or f"Little Big Brain {status_code}",
        "request_id": request_id,
    }
    if header_seconds is not None:
        fallback["retry_after_seconds"] = header_seconds
    return LbbError(status_code, body, fallback)


def _decode_response_data(response: httpx.Response) -> Any:
    content_type = response.headers.get("content-type", "").lower()
    if "application/x-ndjson" in content_type:
        # JSON lines: a list of the lines' values; an empty body is an empty list.
        return [json.loads(line) for line in response.text.splitlines() if line.strip()]
    if not response.content:
        return None
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


def _retries_transport_error(retry: bool | str) -> bool:
    """Whether a request's ``retry`` classification covers a transport failure.
    ``"rate_limited"`` does not: the request may have run on the server."""
    return retry != "rate_limited" and bool(retry)


def _retries_status(retry: bool | str, status_code: int) -> bool:
    """Whether a request's ``retry`` classification covers a retryable status.
    ``"rate_limited"`` covers only ``429``."""
    if retry == "rate_limited":
        return status_code == 429
    return bool(retry)


def _error_body_field(response: httpx.Response, name: str) -> Any:
    """Read ``error.<name>`` from a JSON error body, or ``None`` when the body is
    absent, naked (a bare LB 5xx), or not an error envelope. The integrations
    API's ``{"ok": false, "error": "…", "code": …}`` holds its fields at the
    top level."""
    try:
        parsed = json.loads(response.text)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None
    error = parsed.get("error") if isinstance(parsed, dict) else None
    if isinstance(error, dict):
        return error.get(name)
    if isinstance(error, str) and isinstance(parsed, dict):
        return parsed.get(name)
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
        raise _parse_error(
            response.status_code,
            response.text.strip(),
            request_id,
            response.headers.get("retry-after"),
        )
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
        api_version: str = "2026-07-23",
        max_retries: int = DEFAULT_MAX_RETRIES,
        retry_delay: float = 0.1,
        retry_budget_ms: float = DEFAULT_RETRY_BUDGET_MS,
        on_retry: Callable[[RetryEvent], None] | None = None,
        default_consistency: str | None = None,
        integrations_url: str = DEFAULT_INTEGRATIONS_URL,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._integrations_url = (
            integrations_url.strip() or DEFAULT_INTEGRATIONS_URL
        ).rstrip("/")
        self._api_key = api_key
        self._graph = graph
        self._api_version = api_version
        self._max_retries = max_retries
        self._retry_delay = retry_delay
        self._retry_budget_ms = retry_budget_ms
        self._on_retry = on_retry
        # A5: read consistency applied when a read omits its own value. Since A5
        # the server default is ``eventual``; set ``"strong"`` to keep reads
        # head-exact by default. A per-call ``consistency`` always wins.
        self._default_consistency = default_consistency
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

    @property
    def integrations_url(self) -> str:
        """The integrations API, ``https://api.littlebigbrain.com`` by default."""
        return self._integrations_url

    def _params(
        self, extra: Mapping[str, Any] | None = None, *, scoped: bool = True
    ) -> dict[str, Any]:
        params: dict[str, Any] = {}
        if scoped and self._graph is not None:
            params["graph"] = self._graph
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
        content: Any | None,
        content_type: str | None,
        idempotency_key: str | None,
        headers: Mapping[str, str] | None = None,
        scoped: bool = True,
    ) -> dict[str, Any]:
        """Build identical request options for the sync and async transports.

        ``scoped=False`` leaves out the client's graph: the integrations routes
        name their graph themselves."""
        request_headers = self._headers(idempotency_key)
        request_headers.update(headers or {})
        kwargs: dict[str, Any] = {
            "params": self._params(params, scoped=scoped),
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

    def graph(self, name: str) -> _GraphNamespace:
        return _GraphNamespace(self, name)

    def raw_request(
        self,
        method: str,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        body: Body | None = None,
        content: Any | None = None,
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
        content: Any | None = None,
        content_type: str | None = None,
        idempotency_key: str | None = None,
        options: RequestOptions | None = None,
    ) -> Any:  # noqa: D401
        raise NotImplementedError

    def _integrations_call(
        self,
        method: str,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        body: Body | None = None,
        idempotency_key: str | None = None,
        options: RequestOptions | None = None,
    ) -> Any:
        """A request to the integrations API (``integrations_url``) with the
        same key, retries and errors; the client's graph is not added."""
        raise NotImplementedError

    # --- writes ---

    def create_graph(self) -> models.CreateGraphResponse:
        """Create the scoped graph with an empty ontology.

        Call :meth:`ontology.define` before the first typed commit. Defining an
        ontology can also create the graph head when it does not exist yet.
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
        """Delete the scoped graph, including its graph-scoped jobs."""
        return self._model_request(
            models.GraphDeleteResponse,
            "POST",
            "/v1/graph/delete",
            params={"confirm": confirm},
            options={"retry": True},
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

    def import_ndjson(
        self,
        lines: Sequence[Mapping[str, Any]] | str,
        *,
        batch: int | None = None,
        strict: bool | None = None,
        observed_at: str | None = None,
        idempotency_key: str | None = None,
    ) -> Any:
        """Bulk-ingest a dataset as NDJSON.

        ``lines`` is a sequence of triplet / entity-properties records (each
        serialized to one NDJSON line here) or a pre-built NDJSON string. Lines are
        batched into bounded internal commits server-side, so a whole dataset loads
        in one streamed request without a single oversized commit.

        A successful import durably enqueues one complete published-generation
        build after the final batch. It does not wait for visibility;
        ``published_generation`` carries the durable job identity and due
        sequence to observe.
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
        rdf: str,
        *,
        format: str = "ntriples",
        base_iri: str | None = None,
        graph_uri: str | None = None,
        blank_node_scope: str | None = None,
        batch: int | None = None,
        strict: bool | None = None,
        observed_at: str | None = None,
        resource_type: str | None = None,
        edge_idempotency: str | None = None,
        build: bool | None = None,
        idempotency_key: str | None = None,
    ) -> Any:
        """Bulk-ingest N-Triples, Turtle, N-Quads, or TriG through the native RDF import endpoint.

        Statements are committed through the fixed ``RDF_TRIPLE`` relation;
        source RDF predicates and literal term details are preserved as edge
        metadata. Pass ``build=False`` on intermediate chunks to suppress the
        publication enqueue; every request still commits durable truth and an
        exact per-commit F3 delta. Omit it on the last chunk to advance one
        final coalesced publication fence.
        """
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
                "build": "false" if build is False else None,
            },
            content=rdf,
            content_type=content_types[format],
            idempotency_key=idempotency_key or self.idempotency_key("import-rdf"),
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

    # --- models as runs (training-run registry + eval machinery) ---

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
        """Compare champion and challenger retrieval over one pinned snapshot."""
        return self._request("POST", "/v1/models/shadow-eval", body=body)

    def synthetic_eval(self, *, limit: int | None = None) -> Any:
        """Execution-verified QA probes generated from the graph's current
        edges — labels are the executed projections (``GET
        /v1/models/synthetic-eval``)."""
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

    def suggest_dataset(
        self, *, limit: int | None = None, split_seq: int | None = None
    ) -> Any:
        return self._request(
            "GET",
            "/v1/models/suggest-dataset",
            params={"limit": limit, "split_seq": split_seq},
        )

    def extractor_dataset(
        self, *, limit: int | None = None, split_seq: int | None = None
    ) -> Any:
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

    # --- relevance feedback (training data) ---

    def search_feedback(self, body: Body, *, idempotency_key: str | None = None) -> Any:
        """Append relevance labels for a set of ranked results.

        How little big brain gathers customer-specific qrels: grade the results
        (``3`` ideal/good, ``1`` partially relevant, ``0`` bad), referencing the
        ranking's ``search_id`` so the labels tie back to that exact ranking.
        Labels are stored apart from
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
        and ``limit``/``offset``. GROUP BY is not
        limited to entity identity: ``group_keys`` adds typed scalar keys — a
        property value or a calendar bucket of a datetime property
        (``{"date_bucket": {"var", "field", "granularity", "as"}}``) — so a
        per-category breakdown or a time series is one server-side query; scalar
        keys come back in each ``groups[].value_keys[<as>]``. Returns the typed
        ``SparqlSelectResponse`` shape. For raw SPARQL *text*, use :meth:`sparql`.

        ``consistency`` / ``min_indexed_seq`` select the A5 read mode and the
        read-your-writes floor (body fields on this structured route). A floor
        with no explicit consistency implies a strong base-plus-delta read.
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

    def _sparql_text_envelope(
        self,
        query: str,
        *,
        cursor: str | None = None,
        reason: bool | None = None,
        entailment: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
        consistency: str | None = None,
        min_indexed_seq: int | None = None,
        as_of_commit_seq: int | None = None,
        profile: bool = False,
        request: str | None = None,
    ) -> Any:
        """POST raw SPARQL text to ``/v1/query/sparql-text``; returns the envelope.

        Value in :class:`LbbClient`, awaitable in :class:`AsyncLbbClient`; the
        concrete :meth:`sparql` wrappers parse it into :class:`SparqlResults`.

        The query is read-only, so a retryable ``429`` (for example
        ``read_your_writes_pending`` while publication catches up to
        ``min_indexed_seq``) is retried within the client's retry budget. A
        ``5xx`` is not retried: a query that timed out would run again.
        """
        body: dict[str, Any] = {"query": query}
        if as_of_commit_seq is not None:
            body["as_of_commit_seq"] = as_of_commit_seq
        if cursor is not None:
            body["cursor"] = cursor
        if reason is not None:
            body["reason"] = reason
        if entailment is not None:
            body["entailment"] = entailment
        if limit is not None:
            body["limit"] = limit
        if offset is not None:
            body["offset"] = offset
        if profile:
            body["profile"] = True
        if request is not None:
            body["request"] = request
        # A5: the text dialect carries consistency/floor on the URL, not the body.
        params = self._consistency_params(consistency, min_indexed_seq)
        return self._request(
            "POST",
            "/v1/query/sparql-text",
            body=body,
            params=params or None,
            options={"retry": "rate_limited"},
        )

    # --- ontology ---

    def ontology_search(self, body: Body) -> Any:
        """Discover ontology concepts, terms, and relations."""
        return self._request("POST", "/v1/ontology/search", body=body)

    def ontology_resolve(self, body: Body) -> Any:
        """Resolve mentions to concepts/entities."""
        return self._request("POST", "/v1/ontology/resolve", body=body)

    def ontology_conformance(self, *, consistency: str | None = None) -> Any:
        """Read the durable ontology-conformance report.

        ``eventual`` serves the report referenced by the published read root.
        ``strong`` requires its validation watermark and ontology/shapes
        provenance to match current head; it never runs validation inline.
        """
        params = self._consistency_params(consistency, None)
        return self._request("GET", "/v1/ontology/conformance", params=params or None)

    def ontology_conformance_model(
        self, *, consistency: str | None = None
    ) -> models.SchemaAuditReport:
        """Ontology conformance report validated as ``SchemaAuditReport``."""
        params = self._consistency_params(consistency, None)
        return self._model_request(
            models.SchemaAuditReport,
            "GET",
            "/v1/ontology/conformance",
            params=params or None,
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
        include_indexes: bool | None = None,
    ) -> Any:
        """Graph footprint, WAL tail, and published-index coverage."""
        return self._request(
            "GET",
            "/v1/graph/metadata",
            params={
                "include_indexes": include_indexes,
            },
        )

    def metadata_model(
        self,
        *,
        include_indexes: bool | None = None,
    ) -> models.GraphMetadataResponse:
        """Graph metadata validated as ``GraphMetadataResponse``."""
        return self._model_request(
            models.GraphMetadataResponse,
            "GET",
            "/v1/graph/metadata",
            params={
                "include_indexes": include_indexes,
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

    def read_snapshot(self) -> Any:
        """Pinned published read root and same-epoch query/conformance lag."""
        return self._request("GET", "/v1/graph/read-snapshot")

    def read_snapshot_model(self) -> models.PublishedReadStatusResponse:
        """Published read status validated as ``PublishedReadStatusResponse``."""
        return self._model_request(
            models.PublishedReadStatusResponse,
            "GET",
            "/v1/graph/read-snapshot",
        )

    def schema_summary(self) -> Any:
        """Compact observed RDF schema attached to the immutable published
        base: class populations, resource- and literal-valued predicate
        counts, and bounded OWL/RDFS statements. ``literal_predicate_counts``
        is ``None`` when the stored summary artifact predates the field; the
        next full summary rebuild fills it."""
        return self._request("GET", "/v1/graph/schema-summary")

    def schema_summary_model(self) -> models.RdfSchemaSummaryResponse:
        """Observed schema validated as ``RdfSchemaSummaryResponse``."""
        return self._model_request(
            models.RdfSchemaSummaryResponse,
            "GET",
            "/v1/graph/schema-summary",
        )

    def planner_stats(
        self, *, cursor: str | None = None, limit: int | None = None
    ) -> Any:
        """The SPARQL planner's statistics for the published generation latest
        reads use: per-predicate triple and distinct counts (by triple count,
        paged by ``cursor``/``limit``, 200 by default and 500 at most),
        key-histogram, pair-count and trigram sidecars, and value-order index
        coverage. ``served_at_seq`` is ``None`` when the graph has no published
        generation."""
        params: dict[str, Any] = {}
        if cursor is not None:
            params["cursor"] = cursor
        if limit is not None:
            params["limit"] = limit
        return self._request(
            "GET", "/v1/graph/planner-stats", params=params or None
        )

    def planner_stats_model(
        self, *, cursor: str | None = None, limit: int | None = None
    ) -> models.PlannerStatsResponse:
        """Planner statistics validated as ``PlannerStatsResponse``."""
        params: dict[str, Any] = {}
        if cursor is not None:
            params["cursor"] = cursor
        if limit is not None:
            params["limit"] = limit
        return self._model_request(
            models.PlannerStatsResponse,
            "GET",
            "/v1/graph/planner-stats",
            params=params or None,
        )

    def publication_status(self) -> Any:
        """Automatic publication lifecycle, including pre-first-generation state."""
        return self._request("GET", "/v1/graph/publication-status")

    def managed_models(self) -> Any:
        """The managed models the platform uses per role (embedding, judge, rewriter)."""
        return self._request("GET", "/v1/managed-models")

    def model_activity(self, month: str | None = None) -> Any:
        """What each managed model did for the stack in one month.

        ``month`` is ``yyyy-mm`` (UTC); the current month by default. The
        answer holds totals, by day and by graph per feature (``index``,
        ``search``, ``fit``, ``judge``, ``training``) and model, the months
        with activity, and the model each feature uses now. A stack read:
        the graph scope is not used.
        """
        params = {"month": month} if month is not None else None
        return self._request("GET", "/v1/models/activity", params=params)

    def model_activity_model(
        self, month: str | None = None
    ) -> models.ModelActivityResponse:
        """Model activity validated as ``ModelActivityResponse``."""
        params = {"month": month} if month is not None else None
        return self._model_request(
            models.ModelActivityResponse,
            "GET",
            "/v1/models/activity",
            params=params,
        )

    def publication_status_model(self) -> models.PublicationStatusResponse:
        """Publication lifecycle validated as ``PublicationStatusResponse``."""
        return self._model_request(
            models.PublicationStatusResponse,
            "GET",
            "/v1/graph/publication-status",
        )

    def activity(self) -> Any:
        """Background work on the graph: publication, compaction, query
        statistics, validation, embeddings, and imports, exports, forks and
        index upgrades. Needs the server capability ``graph_activity_v1``."""
        return self._request("GET", "/v1/graph/activity")

    def activity_model(self) -> models.GraphActivityResponse:
        """Background work validated as ``GraphActivityResponse``."""
        return self._model_request(
            models.GraphActivityResponse,
            "GET",
            "/v1/graph/activity",
        )

    def list_graphs(self) -> Any:
        """List the graphs under the scoped tenant."""
        return self._request("GET", "/v1/graphs")

    def list_graphs_model(self) -> models.GraphListResponse:
        """List graphs with a typed ``GraphListResponse``."""
        return self._model_request(models.GraphListResponse, "GET", "/v1/graphs")

    def workflow_delete_instance(
        self, workflow_id: str
    ) -> models.WorkflowInstanceDeleteResponse:
        """Delete a message workflow instance with its turns and history.

        ``deleted`` is false when no instance had the id, or when a retry
        finished an earlier delete. The id can be created again afterwards.
        """
        return self._model_request(
            models.WorkflowInstanceDeleteResponse,
            "POST",
            "/v1/workflows/instances/delete",
            body={"workflow_id": workflow_id},
            options={"retry": True},
        )

    def _model_request(
        self,
        model_cls: type[ModelT],
        method: str,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        body: Body | None = None,
        content: Any | None = None,
        content_type: str | None = None,
        idempotency_key: str | None = None,
        options: RequestOptions | None = None,
    ) -> Any:
        raise NotImplementedError

    def _page_request(self, row_model: type[RowT], payload: Any) -> Any:
        raise NotImplementedError

    def sparql(
        self,
        query: str,
        *,
        cursor: str | None = None,
        reason: bool | None = None,
        entailment: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
        consistency: str | None = None,
        min_indexed_seq: int | None = None,
        as_of_commit_seq: int | None = None,
        profile: bool = False,
        request: str | None = None,
    ) -> Any:
        """Run SPARQL text; concrete transports return or await parsed results."""
        raise NotImplementedError


class _GraphNamespace:
    def __init__(self, client: _BaseLbbClient, graph: str) -> None:
        self._client = client
        self._graph = graph
        self.facts = _FactsNamespace(client, graph)
        self.ontology = _OntologyNamespace(client, graph)

    def delete(self, *, confirm: str) -> models.GraphDeleteResponse:
        """Delete and deregister this whole graph."""
        return self._client._model_request(
            models.GraphDeleteResponse,
            "POST",
            "/v1/graph/delete",
            params={"graph": self._graph, "confirm": confirm},
            options={"retry": True},
        )

    def publication_status(self) -> Any:
        """Read server-managed publication progress for this graph."""
        return self._client._request(
            "GET",
            "/v1/graph/publication-status",
            params={"graph": self._graph},
        )

    def publication_status_model(self) -> models.PublicationStatusResponse:
        """Read typed publication progress for this graph."""
        return self._client._model_request(
            models.PublicationStatusResponse,
            "GET",
            "/v1/graph/publication-status",
            params={"graph": self._graph},
        )

    def activity(self) -> Any:
        """Read the background work on this graph."""
        return self._client._request(
            "GET",
            "/v1/graph/activity",
            params={"graph": self._graph},
        )

    def activity_model(self) -> models.GraphActivityResponse:
        """Read the typed background work on this graph."""
        return self._client._model_request(
            models.GraphActivityResponse,
            "GET",
            "/v1/graph/activity",
            params={"graph": self._graph},
        )

    def planner_stats(
        self, *, cursor: str | None = None, limit: int | None = None
    ) -> Any:
        """Read the SPARQL planner's statistics for this graph."""
        params: dict[str, Any] = {"graph": self._graph}
        if cursor is not None:
            params["cursor"] = cursor
        if limit is not None:
            params["limit"] = limit
        return self._client._request(
            "GET", "/v1/graph/planner-stats", params=params
        )

    def wait_for_published(
        self,
        target_seq: int,
        *,
        timeout: float = 30.0,
        poll_interval: float = 0.25,
    ) -> models.PublicationStatusResponse:
        """Wait for this graph to publish an exact target sequence."""
        if target_seq < 0:
            raise ValueError("target_seq must be non-negative")
        if timeout < 0 or poll_interval < 0:
            raise ValueError("timeout and poll_interval must be non-negative")
        deadline = time.monotonic() + timeout
        while True:
            status = self.publication_status_model()
            if status.state == models.PublicationState.blocked:
                raise RuntimeError(
                    f"publication blocked at {status.current_stage or 'unknown stage'}: "
                    f"{status.retry.message}"
                )
            if (
                status.state == models.PublicationState.current
                and status.published_seq >= target_seq
            ):
                return status
            now = time.monotonic()
            if now >= deadline:
                raise TimeoutError(
                    f"publication did not reach {target_seq} within {timeout}s "
                    f"(state={status.state.value}, head={status.head_seq}, "
                    f"target={status.target_seq}, published={status.published_seq}, "
                    f"stage={status.current_stage or 'unknown'})"
                )
            retry_after = status.retry.retry_after_ms / 1000
            time.sleep(min(max(poll_interval, retry_after), deadline - now))

    def retract(self, body: Body, *, idempotency_key: str | None = None) -> Any:
        """Retract edges/entities from the scoped graph. See :meth:`LbbClient.retract`."""
        return self._client._request(
            "POST",
            "/v1/graph/retract",
            params={"graph": self._graph},
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
            params={"graph": self._graph},
            body=body,
            idempotency_key=idempotency_key or self._client.idempotency_key("retract"),
        )


class _FactsNamespace:
    def __init__(self, client: _BaseLbbClient, graph: str) -> None:
        self._client = client
        self._graph = graph

    def create(self, body: Body, *, idempotency_key: str | None = None) -> Any:
        params = {"graph": self._graph}
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
        params = {"graph": self._graph}
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
                "batch": batch,
                "strict": strict,
                "observed_at": observed_at,
            },
            content=ndjson,
            content_type="application/x-ndjson",
            idempotency_key=idempotency_key or self._client.idempotency_key("import"),
        )

    def import_rdf(
        self,
        rdf: str,
        *,
        format: str = "ntriples",
        base_iri: str | None = None,
        graph_uri: str | None = None,
        blank_node_scope: str | None = None,
        batch: int | None = None,
        strict: bool | None = None,
        observed_at: str | None = None,
        resource_type: str | None = None,
        edge_idempotency: str | None = None,
        build: bool | None = None,
        idempotency_key: str | None = None,
    ) -> Any:
        """Bulk-load N-Triples, Turtle, N-Quads, or TriG. See :meth:`LbbClient.import_rdf`."""
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
                "batch": batch,
                "strict": strict,
                "observed_at": observed_at,
                "format": format,
                "base_iri": base_iri,
                "graph_uri": graph_uri,
                "blank_node_scope": blank_node_scope,
                "resource_type": resource_type,
                "edge_idempotency": edge_idempotency,
                "build": "false" if build is False else None,
            },
            content=rdf,
            content_type=content_types[format],
            idempotency_key=idempotency_key
            or self._client.idempotency_key("import-rdf"),
        )

    def import_rdf_many(
        self,
        documents: Sequence[str],
        *,
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
    ) -> dict[str, Any]:
        """Import RDF documents as truth-only chunks, then advance one final publication fence."""
        if not documents:
            raise ValueError("import_rdf_many requires at least one document")
        imports = []
        for index, document in enumerate(documents):
            imports.append(
                self.import_rdf(
                    document,
                    format=format,
                    base_iri=base_iri,
                    graph_uri=graph_uri,
                    blank_node_scope=blank_node_scope,
                    batch=batch,
                    strict=strict,
                    observed_at=observed_at,
                    resource_type=resource_type,
                    edge_idempotency=edge_idempotency,
                    build=index == len(documents) - 1,
                    idempotency_key=(
                        f"{idempotency_key}:{index + 1}" if idempotency_key else None
                    ),
                )
            )
        final = imports[-1]
        return {
            "imports": imports,
            "final_sequence": final.get("committed_commit_seq"),
            "publication": final.get("published_generation"),
        }


def _decision(reason: str, author: str | None) -> dict[str, Any]:
    body: dict[str, Any] = {"reason": reason}
    if author is not None:
        body["author"] = author
    return body


def _graph_scoped(graph: str | None, params: Mapping[str, Any] | None) -> dict[str, Any]:
    """Query parameters with ``graph`` set when a namespace is graph-scoped.

    The client adds its own default graph first, so this one wins."""
    scoped = dict(params or {})
    if graph is not None:
        scoped["graph"] = graph
    return scoped


class _OntologyStartersNamespace:
    """Ontology starters: versioned base ontologies a graph starts from.

    ``list`` and ``get`` answer for a graph that does not exist yet. The status
    compares a starter's terms with the graph's ontology.
    """

    def __init__(self, client: _BaseLbbClient, graph: str | None = None) -> None:
        self._client = client
        self._graph = graph

    def list(self) -> models.OntologyStarterList:
        """Every starter with its status on the graph."""
        return self._client._model_request(
            models.OntologyStarterList,
            "GET",
            "/v1/ontology/starters",
            params=_graph_scoped(self._graph, None),
        )

    def get(self, starter: str) -> models.OntologyStarterDetail:
        """One starter's document, status and ``missing_ops``."""
        return self._client._model_request(
            models.OntologyStarterDetail,
            "GET",
            "/v1/ontology/starters/detail",
            params=_graph_scoped(self._graph, {"starter": starter}),
        )

    def apply(
        self,
        starter: str,
        *,
        dry_run: bool = False,
        expected_ontology_version: int | None = None,
    ) -> models.OntologyStarterApplyResponse:
        """Add what the graph lacks of a starter in one ontology version.

        A relation the graph has is widened. Applying again answers
        ``no_op: True``, so a retry is safe. A term the graph holds differently
        fails with ``409 starter_conflict`` and writes nothing.
        """
        body: dict[str, Any] = {"starter": starter}
        if dry_run:
            body["dry_run"] = True
        if expected_ontology_version is not None:
            body["expected_ontology_version"] = expected_ontology_version
        return self._client._model_request(
            models.OntologyStarterApplyResponse,
            "POST",
            "/v1/ontology/starters/apply",
            params=_graph_scoped(self._graph, None),
            body=body,
            options={"retry": True},
        )

    def update(self, starter: str) -> models.OntologyStarterUpdateResponse:
        """File what the graph lacks of a starter as one change suggestion.

        The suggestion is keyed ``starter:<id>/<version>``: asking again
        returns the same one.
        """
        return self._client._model_request(
            models.OntologyStarterUpdateResponse,
            "POST",
            "/v1/ontology/starters/update",
            params=_graph_scoped(self._graph, None),
            body={"starter": starter},
            options={"retry": True},
        )


class _OntologyNamespace:
    """Typed ontology discovery and lifecycle operations."""

    def __init__(self, client: _BaseLbbClient, graph: str | None = None) -> None:
        self._client = client
        self._graph = graph
        self.starters = _OntologyStartersNamespace(client, graph)

    def _scope(self, params: Mapping[str, Any] | None = None) -> dict[str, Any]:
        """Query parameters, with this namespace's graph when it has one."""
        return _graph_scoped(self._graph, params)

    def view(
        self, *, counts: bool = False, options: RequestOptions | None = None
    ) -> models.OntologyView:
        return self._client._model_request(
            models.OntologyView,
            "GET",
            "/v1/ontology",
            params=self._scope({"counts": True if counts else None}),
            options=options,
        )

    def conformance(
        self,
        *,
        consistency: str | None = None,
        options: RequestOptions | None = None,
    ) -> models.SchemaAuditReport:
        return self._client._model_request(
            models.SchemaAuditReport,
            "GET",
            "/v1/ontology/conformance",
            params=self._scope(
                {"consistency": self._client._resolve_consistency(consistency)}
            ),
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
            params=self._scope(),
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
            params=self._scope(),
        )

    def define(self, body: Body) -> models.OntologyDefineResponse:
        """Put the scoped graph on an imported ontology.

        Creates the graph when it does not exist yet. Safe to repeat: an
        unchanged ontology answers ``changed: False`` without writing. An
        additive difference is applied, including a wider relation domain or
        range and a new property field, and ``changed`` reports what was
        written. A document that narrows or drops what the graph already
        defines, or states a change no additive operation expresses, is refused
        with ``ontology_restrictive_change``,
        ``ontology_identity_breaking_change``, or
        ``ontology_unsupported_change``, and writes nothing.
        """
        return self._client._model_request(
            models.OntologyDefineResponse,
            "POST",
            "/v1/ontology/define",
            body=body,
            params=self._scope(),
        )

    def evolve(
        self, body: Body, *, dry_run: bool = False
    ) -> models.OntologyEvolveResponse:
        """Apply an ontology patch, or preview it without mutation."""
        return self._client._model_request(
            models.OntologyEvolveResponse,
            "POST",
            "/v1/ontology/evolve",
            params=self._scope({"dry_run": "true" if dry_run else None}),
            body=body,
        )

    def draft_create(self, body: Body) -> models.OntologyDraft:
        """Create a durable proposal from samples without ingesting them."""
        return self._client._model_request(
            models.OntologyDraft,
            "POST",
            "/v1/ontology/drafts",
            body=body,
            params=self._scope(),
        )

    def draft_get(self, draft_id: str) -> models.OntologyDraft:
        return self._client._model_request(
            models.OntologyDraft,
            "GET",
            "/v1/ontology/drafts",
            params=self._scope({"draft_id": draft_id}),
        )

    def draft_validate(self, draft_id: str) -> models.OntologyDraft:
        return self._client._model_request(
            models.OntologyDraft,
            "POST",
            "/v1/ontology/drafts/validate",
            params=self._scope({"draft_id": draft_id}),
            options=_read_options(),
        )

    def draft_promote(
        self, draft_id: str, *, idempotency_key: str | None = None
    ) -> models.OntologyDraft:
        return self._client._model_request(
            models.OntologyDraft,
            "POST",
            "/v1/ontology/drafts/promote",
            params=self._scope({"draft_id": draft_id}),
            idempotency_key=idempotency_key
            or self._client.idempotency_key("ontology-draft-promote"),
        )

    def draft_reject(self, draft_id: str, reason: str) -> models.OntologyDraft:
        return self._client._model_request(
            models.OntologyDraft,
            "POST",
            "/v1/ontology/drafts/reject",
            params=self._scope({"draft_id": draft_id, "reason": reason}),
        )

    def suggestions(
        self,
        *,
        status: str | None = None,
        origin_kind: str | None = None,
        origin_id: str | None = None,
        anchor: str | None = None,
        key: str | None = None,
        limit: int | None = None,
    ) -> models.OntologyChangeSuggestionList:
        """List ontology change suggestions, newest update first."""
        return self._client._model_request(
            models.OntologyChangeSuggestionList,
            "GET",
            "/v1/ontology/suggestions",
            params=self._scope(
                {
                    "status": status,
                    "origin_kind": origin_kind,
                    "origin_id": origin_id,
                    "anchor": anchor,
                    "key": key,
                    "limit": limit,
                }
            ),
        )

    def suggestion_get(self, suggestion_id: str) -> models.OntologyChangeSuggestion:
        """One suggestion with its evidence, impact and discussion."""
        return self._client._model_request(
            models.OntologyChangeSuggestion,
            "GET",
            "/v1/ontology/suggestions/detail",
            params=self._scope({"suggestion_id": suggestion_id}),
        )

    def suggestion_create(self, body: Body) -> models.OntologyChangeSuggestion:
        """File a suggestion, or revise the one with the same ``key``.

        The server dry-runs the change and never changes the ontology here, so
        a retry is safe.
        """
        return self._client._model_request(
            models.OntologyChangeSuggestion,
            "POST",
            "/v1/ontology/suggestions",
            body=body,
            options=_read_options(),
            params=self._scope(),
        )

    def suggestion_validate(
        self, suggestion_id: str
    ) -> models.OntologyChangeSuggestion:
        """Dry-run the change against the current ontology."""
        return self._client._model_request(
            models.OntologyChangeSuggestion,
            "POST",
            "/v1/ontology/suggestions/validate",
            params=self._scope({"suggestion_id": suggestion_id}),
            options=_read_options(),
        )

    def suggestion_accept(
        self, suggestion_id: str, body: Body | None = None
    ) -> models.OntologyChangeSuggestion:
        """Apply the change (or an edited ``change``) to the current ontology."""
        return self._client._model_request(
            models.OntologyChangeSuggestion,
            "POST",
            "/v1/ontology/suggestions/accept",
            params=self._scope({"suggestion_id": suggestion_id}),
            body=body if body is not None else {},
            options=_read_options(),
        )

    def suggestion_dismiss(
        self, suggestion_id: str, reason: str, *, author: str | None = None
    ) -> models.OntologyChangeSuggestion:
        return self._client._model_request(
            models.OntologyChangeSuggestion,
            "POST",
            "/v1/ontology/suggestions/dismiss",
            params=self._scope({"suggestion_id": suggestion_id}),
            body=_decision(reason, author),
            options=_read_options(),
        )

    def suggestion_supersede(
        self, suggestion_id: str, reason: str, *, author: str | None = None
    ) -> models.OntologyChangeSuggestion:
        return self._client._model_request(
            models.OntologyChangeSuggestion,
            "POST",
            "/v1/ontology/suggestions/supersede",
            params=self._scope({"suggestion_id": suggestion_id}),
            body=_decision(reason, author),
            options=_read_options(),
        )

    def suggestion_comment(
        self, suggestion_id: str, text: str, *, author: str | None = None
    ) -> models.OntologyChangeSuggestion:
        body: dict[str, Any] = {"text": text}
        if author is not None:
            body["author"] = author
        return self._client._model_request(
            models.OntologyChangeSuggestion,
            "POST",
            "/v1/ontology/suggestions/comment",
            params=self._scope({"suggestion_id": suggestion_id}),
            body=body,
        )


class _QueryNamespace:
    """Typed structured and SPARQL-text queries."""

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
        cursor: str | None = None,
        reason: bool | None = None,
        entailment: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
        consistency: str | None = None,
        min_indexed_seq: int | None = None,
        as_of_commit_seq: int | None = None,
        profile: bool = False,
        request: str | None = None,
    ) -> Any:
        return self._client.sparql(
            query,
            cursor=cursor,
            reason=reason,
            entailment=entailment,
            limit=limit,
            offset=offset,
            consistency=consistency,
            min_indexed_seq=min_indexed_seq,
            as_of_commit_seq=as_of_commit_seq,
            profile=profile,
            request=request,
        )

    def update(
        self,
        update: str,
        *,
        idempotency_key: str | None = None,
        options: RequestOptions | None = None,
    ) -> Any:
        """Run a SPARQL 1.1 Update on the native ``/update`` endpoint.

        The server accepts ``INSERT DATA`` and answers every other form with
        400; one request is one commit. A graph whose first write comes
        through this route is RDF-native, and an RDF-native graph refuses the
        JSON write routes with ``400 rdf_native_graph``. Under ``reject``-mode
        SHACL shapes, a write that breaks them fails with 400 and writes
        nothing.

        The client sends an idempotency key (a new one per call unless you
        pass ``idempotency_key``), so a retry replays the write. The answer
        has no body: read the write back with a ``strong`` read.
        """
        return self._client._request(
            "POST",
            "/update",
            content=update.encode(),
            content_type="application/sparql-update",
            idempotency_key=idempotency_key
            or self._client.idempotency_key("sparql-update"),
            options=options,
        )

    def names(
        self,
        text: str,
        *,
        limit: int | None = None,
        options: RequestOptions | None = None,
    ) -> Any:
        """The entities a question or a list of names names
        (``POST /v1/query/names``).

        Each name gets up to ``limit`` candidates (default 5) in
        ``candidates``, with its ``class``, ``label``, ``score`` and ``by``
        (how it matched); the candidates of one name share its ``text``, the
        one to prefer first. Use it in your own agent before you write a
        query: put the IRI in the query instead of matching the name.
        ``index_ready`` is ``False`` while the name index builds; ask again in
        a few seconds. No model call.
        """
        body: dict[str, Any] = {"text": text}
        if limit is not None:
            body["limit"] = limit
        return self._client._request(
            "POST",
            "/v1/query/names",
            body=body,
            options={"retry": True, **(options or {})},
        )

    def describe(
        self,
        *,
        question: str | None = None,
        classes: Sequence[str] | None = None,
        properties: Sequence[str] | None = None,
        options: RequestOptions | None = None,
    ) -> Any:
        """The classes and properties a question needs, or the ones named by
        IRI (``POST /v1/query/describe``).

        Each class lists how many of its sampled instances hold each
        property (``instances`` of ``of``): a filter on a property that few
        instances hold returns few rows. A small class (at most 30
        instances: stages, statuses) lists its instances with their values.
        ``text`` holds the same description for a model's prompt.
        ``partial`` is ``True`` while the server still samples. No model
        call.
        """
        body: dict[str, Any] = {}
        if question is not None:
            body["question"] = question
        if classes:
            body["classes"] = list(classes)
        if properties:
            body["properties"] = list(properties)
        return self._client._request(
            "POST",
            "/v1/query/describe",
            body=body,
            options={"retry": True, **(options or {})},
        )

    def commit_at(
        self,
        *,
        date: str | None = None,
        moment: str | None = None,
        options: RequestOptions | None = None,
    ) -> Any:
        """The commit of a date or a moment (``GET /v1/graph/commit-at``).

        Pass exactly one: ``date`` (``YYYY-MM-DD``) finds the last commit
        written by the end of that day, UTC; ``moment`` (RFC 3339) the last
        commit written at or before it. Read the graph as it was with
        ``query.sparql(..., as_of_commit_seq=...)``. ``as_of_commit_seq`` is
        absent, with a ``note``, when the moment is before the first commit
        or the commits record no time.
        """
        if (date is None) == (moment is None):
            raise ValueError("pass exactly one of date and moment")
        params = {"date": date} if date is not None else {"moment": moment}
        return self._client._request(
            "GET", "/v1/graph/commit-at", params=params, options=options
        )

    def compare(
        self,
        query: str,
        *,
        before: Mapping[str, Any] | models.QueryComparePoint,
        after: Mapping[str, Any] | models.QueryComparePoint | None = None,
        key: Sequence[str] | None = None,
        entailment: str | None = None,
        max_rows: int | None = None,
        limit: int | None = None,
        cursor: str | None = None,
        consistency: str | None = None,
        options: RequestOptions | None = None,
    ) -> Any:
        """Run one ``SELECT`` at two points and pair the rows
        (``POST /v1/query/compare``).

        ``before`` and ``after`` are each ``{"as_of_commit_seq": n}``,
        ``{"date": "YYYY-MM-DD"}`` or ``{"moment": "<RFC 3339>"}``; ``after``
        defaults to the latest commit. With ``key`` (for example
        ``["contact"]``) the rows are paired by those variables into
        ``added``, ``removed`` and ``changed``; without it whole rows are
        compared. ``totals`` counts each list. The server reads up to
        ``max_rows`` rows per point (default 20,000) and pages the lists by
        ``limit`` (default 100): pass ``next_cursor`` back as ``cursor`` with
        the same arguments. ``truncated`` means a point was not read whole.
        """
        def point(value: Mapping[str, Any] | models.QueryComparePoint) -> Any:
            return dict(value) if isinstance(value, Mapping) else _coerce_body(value)

        body: dict[str, Any] = {"query": query, "before": point(before)}
        if after is not None:
            body["after"] = point(after)
        if key:
            body["key"] = list(key)
        for name, value in (
            ("entailment", entailment),
            ("max_rows", max_rows),
            ("limit", limit),
            ("cursor", cursor),
        ):
            if value is not None:
                body[name] = value
        params = self._client._consistency_params(consistency, None)
        return self._client._request(
            "POST",
            "/v1/query/compare",
            body=body,
            params=params or None,
            options={"retry": True, **(options or {})},
        )

    def rewrite_profile(self, *, options: RequestOptions | None = None) -> Any:
        """The graph's rewrite profile (``GET /v1/query/rewrite/profile``):
        the notes and worked examples the model of ``query.ask`` reads for
        every question of the graph. ``version`` is 0 when the graph has none."""
        return self._client._request(
            "GET", "/v1/query/rewrite/profile", options=options
        )

    def set_rewrite_profile(
        self,
        *,
        notes: str = "",
        examples: Sequence[Mapping[str, Any] | models.QueryRewriteExample] = (),
        expected_version: int | None = None,
        dry_run: bool = False,
        options: RequestOptions | None = None,
    ) -> Any:
        """Store the graph's rewrite profile (``PUT /v1/query/rewrite/profile``).

        ``notes`` (at most 8,000 characters) say what the data means: which
        property holds the current state, what "me" means. ``examples`` (at
        most 20) each hold a ``question`` and the ``sparql`` ``SELECT`` or
        ``ASK`` query that answers it, with an optional ``note``; the server
        parses each query. Pass the ``version`` you read as
        ``expected_version``: when another write came first, the call raises
        a ``409 conflict`` and stores nothing. ``dry_run=True`` checks the
        profile and stores nothing. Empty notes and no examples clear it.
        ``query.ask`` reads the profile for every question; a call's
        ``context`` still adds notes.
        """
        body: dict[str, Any] = {
            "notes": notes,
            "examples": [
                dict(example) if isinstance(example, Mapping) else _coerce_body(example)
                for example in examples
            ],
        }
        if expected_version is not None:
            body["expected_version"] = expected_version
        return self._client._request(
            "PUT",
            "/v1/query/rewrite/profile",
            body=body,
            params={"dry_run": "true"} if dry_run else None,
            options=options,
        )

    def _ask_response(
        self,
        question: str,
        *,
        context: str | None,
        route: str | models.QueryRoute | None,
        mode: str | models.QueryRewriteMode | None,
        limit: int | None,
        as_of_commit_seq: int | None,
        today: str | None,
        include_grounding: bool | None,
        anchor: Sequence[str] | None,
        timeline: Sequence[Mapping[str, Any] | models.QueryRewriteTimelinePoint] | None,
        consistency: str | None,
        options: RequestOptions | None,
    ) -> Any:
        """Send ``POST /v1/query/ask`` and return the response as it came.

        ``LbbClient`` returns the dict, and ``AsyncLbbClient`` an awaitable
        of it. The call uses model tokens, so a failed call is not retried
        unless ``options={"retry": True}``.
        """
        body = _ask_body(
            question,
            context=context,
            route=route,
            mode=mode,
            limit=limit,
            as_of_commit_seq=as_of_commit_seq,
            today=today,
            include_grounding=include_grounding,
            anchor=anchor,
            timeline=timeline,
        )
        params = self._client._consistency_params(consistency, None)
        return self._client._request(
            "POST",
            "/v1/query/ask",
            body=body,
            params=params or None,
            options={"retry": False, **(options or {})},
        )


def _ask_body(
    question: str,
    *,
    context: str | None,
    route: str | models.QueryRoute | None,
    mode: str | models.QueryRewriteMode | None,
    limit: int | None,
    as_of_commit_seq: int | None,
    today: str | None,
    include_grounding: bool | None = None,
    anchor: Sequence[str] | None = None,
    timeline: Sequence[Mapping[str, Any] | models.QueryRewriteTimelinePoint] | None = None,
) -> dict[str, Any]:
    """The ``QueryRewriteRequest`` body of ``POST /v1/query/ask``. A field
    the caller left at ``None`` stays off the wire: each one has a server
    default."""
    body: dict[str, Any] = {"question": question}
    if context is not None:
        body["context"] = context
    if route is not None:
        body["route"] = getattr(route, "value", route)
    if mode is not None:
        body["mode"] = getattr(mode, "value", mode)
    if limit is not None:
        body["limit"] = limit
    if as_of_commit_seq is not None:
        body["as_of_commit_seq"] = as_of_commit_seq
    if today is not None:
        body["today"] = today
    if include_grounding is not None:
        body["include_grounding"] = include_grounding
    if anchor:
        body["anchor"] = list(anchor)
    if timeline:
        body["timeline"] = [
            dict(point) if isinstance(point, Mapping) else _coerce_body(point)
            for point in timeline
        ]
    return body


#: The most text one server-sent event may hold before the parser stops: 64 MiB.
_MAX_SSE_EVENT_CHARS = 64 * 1024 * 1024
_SSE_LINE_END = re.compile(r"\r\n|\r|\n")


class _SseParser:
    """An incremental parser of a ``text/event-stream`` body, as the HTML
    standard defines it.

    Feed decoded text in pieces of any size; each call returns the
    ``(event, data)`` pairs that completed. A line ends with ``\\n``,
    ``\\r\\n`` or ``\\r``, also when ``\\r\\n`` arrives in two pieces.
    Comment lines (``:``) and unknown fields are skipped, and several
    ``data`` lines join with ``\\n``. One event may hold at most
    ``max_event_chars`` characters.
    """

    def __init__(self, max_event_chars: int = _MAX_SSE_EVENT_CHARS) -> None:
        self._max_event_chars = max_event_chars
        self._pending: list[str] = []
        self._pending_chars = 0
        self._skip_line_feed = False
        self._event = ""
        self._data: list[str] = []
        self._event_chars = 0

    def feed(self, text: str) -> list[tuple[str, str]]:
        events: list[tuple[str, str]] = []
        start = 0
        if self._skip_line_feed and text:
            self._skip_line_feed = False
            if text[0] == "\n":
                start = 1
        for match in _SSE_LINE_END.finditer(text, start):
            piece = text[start : match.start()]
            if self._pending:
                piece = "".join(self._pending) + piece
                self._pending = []
                self._pending_chars = 0
            self._line(piece, events)
            if match.group() == "\r" and match.end() == len(text):
                self._skip_line_feed = True
            start = match.end()
        if start < len(text):
            self._pending.append(text[start:])
            self._pending_chars += len(text) - start
            self._check_size()
        return events

    def _line(self, line: str, events: list[tuple[str, str]]) -> None:
        if not line:
            if self._data:
                events.append((self._event or "message", "\n".join(self._data)))
            self._event = ""
            self._data = []
            self._event_chars = 0
            return
        if line.startswith(":"):
            return
        field, colon, value = line.partition(":")
        if colon and value.startswith(" "):
            value = value[1:]
        if field == "event":
            self._event = value
        elif field == "data":
            self._event_chars += len(value) + 1
            self._check_size()
            self._data.append(value)
        # ``id``, ``retry`` and unknown fields do not change what a client reads.

    def _check_size(self) -> None:
        if self._event_chars + self._pending_chars > self._max_event_chars:
            raise ValueError(
                f"a server-sent event is larger than {self._max_event_chars} "
                "characters; the stream was stopped"
            )


def _sse_decoder() -> codecs.IncrementalDecoder:
    """The UTF-8 decoder of an event stream: bytes may split a character, a
    leading byte-order mark is dropped, and bad bytes become U+FFFD."""
    return codecs.getincrementaldecoder("utf-8-sig")(errors="replace")


#: The event names of a streamed question. A client skips any other name.
_ASK_STREAM_EVENTS: Final = frozenset(
    {"grounding", "route", "step", "answer", "done", "error"}
)
_ASK_STREAM_ENDED = (
    "Little Big Brain ask stream ended before its done or error event"
)


def _error_type_for_status(status_code: int) -> str:
    """The ``type`` the server's JSON error carries for a status."""
    if status_code == 400:
        return "invalid_request_error"
    if status_code in (401, 403):
        return "auth_error"
    if status_code == 404:
        return "not_found_error"
    if status_code == 409:
        return "conflict_error"
    if status_code == 429:
        return "rate_limit_error"
    return "api_error"


def _ask_stream_event(
    name: str, text: str, request_id: str | None
) -> QueryAskStreamEvent | None:
    """One server-sent event of a streamed question: ``None`` for an event
    name the client does not know, the event otherwise. An ``error`` event
    raises the :class:`LbbError` the same request without a stream gets."""
    if name not in _ASK_STREAM_EVENTS:
        return None
    try:
        data = json.loads(text)
    except json.JSONDecodeError as error:
        raise ValueError(f'Little Big Brain sent invalid JSON in a "{name}" event') from error
    if not isinstance(data, dict):
        raise ValueError(f'Little Big Brain sent a "{name}" event that is not a JSON object')
    if name == "error":
        status = data.get("status")
        status_code = status if isinstance(status, int) else 500
        envelope = {
            "error": {
                "type": _error_type_for_status(status_code),
                "code": data.get("code"),
                "message": data.get("message"),
            }
        }
        raise _parse_error(status_code, json.dumps(envelope), request_id)
    return QueryAskStreamEvent(event=name, data=data)


class _SchemaNamespace:
    """Active ontology/SHACL bundle metadata and atomic publication."""

    def __init__(self, client: _BaseLbbClient) -> None:
        self._client = client

    def view(self) -> Any:
        """Read active metadata without running request-time validation."""
        return self._client._request("GET", "/v1/schema")

    def view_model(self) -> models.SchemaBundleView:
        return self._client._model_request(models.SchemaBundleView, "GET", "/v1/schema")

    def publish(self, body: Body, *, idempotency_key: str | None = None) -> Any:
        """Atomically publish a bundle; conformance is produced asynchronously."""
        return self._client._request(
            "POST",
            "/v1/schema/publish",
            body=body,
            idempotency_key=idempotency_key
            or self._client.idempotency_key("schema-publish"),
        )

    def publish_model(
        self, body: Body, *, idempotency_key: str | None = None
    ) -> models.SchemaPublishResponse:
        return self._client._model_request(
            models.SchemaPublishResponse,
            "POST",
            "/v1/schema/publish",
            body=body,
            idempotency_key=idempotency_key
            or self._client.idempotency_key("schema-publish"),
        )


class _EvalsNamespace:
    """Managed evals: traces, labels (thumbs up or down), goldens, and runs."""

    def __init__(self, client: _BaseLbbClient) -> None:
        self._client = client

    def summary(self) -> Any:
        """Settings, golden counts, unlabeled traces, the latest run, and the score by commit."""
        return self._client._request("GET", "/v1/evals")

    def traces(self, *, limit: int | None = None, unlabeled: bool = False) -> Any:
        """Recent traces, newest first."""
        return self._client._request(
            "GET",
            "/v1/evals/traces",
            params={"limit": limit, "unlabeled": "true" if unlabeled else None},
        )

    def trace(self, trace_id: str) -> Any:
        """One trace: the request, its query, its results, and their labels."""
        return self._client._request("GET", "/v1/evals/trace", params={"id": trace_id})

    def label(
        self,
        trace_id: str,
        *,
        item: str | None = None,
        valid: bool | None = None,
        items: list[Mapping[str, Any]] | None = None,
        by: str | None = None,
        note: str | None = None,
        source: str | None = None,
        sparql: str | None = None,
    ) -> Any:
        """Thumbs up or down on a trace.

        One result as `item` + `valid`, or several as
        `items=[{"id": ..., "valid": ...}]`: each label judges one result, a
        citation of the answer.

        On a question's trace, `valid` alone judges the whole answer:
        `valid=True` makes the trace's query the golden query of the question;
        `valid=False` with `sparql` gives the right query, which becomes the
        golden query (the server checks and runs it once); `valid=False`
        alone marks the answer wrong."""
        body: dict[str, Any] = {}
        if item is not None:
            body["item"] = item
        if valid is not None:
            body["valid"] = valid
        if items is not None:
            body["items"] = [dict(entry) for entry in items]
        if by is not None:
            body["by"] = by
        if note is not None:
            body["note"] = note
        if source is not None:
            body["source"] = source
        if sparql is not None:
            body["sparql"] = sparql
        return self._client._request(
            "POST", "/v1/evals/label", params={"trace": trace_id}, body=body
        )

    def judge(self, *, trace_id: str | None = None, limit: int | None = None) -> Any:
        """Let the managed judge label the results of one trace, or of a batch of traces."""
        return self._client._request(
            "POST", "/v1/evals/judge", params={"trace": trace_id, "limit": limit}
        )

    def goldens(self) -> Any:
        return self._client._request("GET", "/v1/evals/goldens")

    def create_golden(
        self,
        sparql: str,
        *,
        request: str | None = None,
        entailment: str | None = None,
    ) -> Any:
        """Freeze a query: every result it returns now is relevant."""
        body: dict[str, Any] = {"sparql": sparql}
        if request is not None:
            body["request"] = request
        if entailment is not None:
            body["entailment"] = entailment
        return self._client._request("POST", "/v1/evals/goldens", body=body)

    def accept_golden(self, golden_id: str, *, consistency: str | None = None) -> Any:
        """Accept the rows a golden returns now as its new reference."""
        return self._client._request(
            "POST",
            "/v1/evals/goldens/accept",
            params={"id": golden_id, "consistency": consistency},
        )

    def delete_golden(self, golden_id: str) -> Any:
        return self._client._request(
            "DELETE", "/v1/evals/goldens", params={"id": golden_id}
        )

    def run(self, *, consistency: str | None = None) -> Any:
        """Replay every golden at the current commit."""
        return self._client._request(
            "POST", "/v1/evals/run", params={"consistency": consistency}
        )

    def results(self, *, limit: int | None = None) -> Any:
        return self._client._request(
            "GET", "/v1/evals/results", params={"limit": limit}
        )

    def settings(self) -> Any:
        return self._client._request("GET", "/v1/evals/settings")

    def set_settings(self, body: Body) -> Any:
        return self._client._request("PUT", "/v1/evals/settings", body=body)


def _enum_value(value: Any) -> Any:
    """The wire value of a generated enum member, or the value itself."""
    return getattr(value, "value", value)


class _ChecksNamespace:
    """Model checks: the log of the model calls LBB makes for its own work on
    the graph (rerank, route, rewrite, fit, propose, label, embed), the checks
    a judge model makes of a sample of them, and the reviews people make of
    the checks. A review is the call's ground truth.

    Reading calls and checks and reviewing a check use no model.
    :meth:`check_call` spends the platform's judge budget.
    """

    def __init__(self, client: _BaseLbbClient) -> None:
        self._client = client

    def summary(self, *, month: str | None = None) -> Any:
        """One month of checks (``yyyy-mm``, UTC; the current month by
        default): per job and model the checks, the mean score, right,
        partly and wrong, the reviews and corrections; the judge's agreement
        with people; today's budget; whether the server has a checker; the
        month's model calls."""
        return self._client._request(
            "GET", "/v1/models/checks/summary", params={"month": month}
        )

    def list(
        self,
        *,
        job: str | models.ModelJob | None = None,
        month: str | None = None,
        verdict: str | models.CheckVerdict | None = None,
        reviewed: bool | None = None,
        after: str | None = None,
        limit: int | None = None,
    ) -> Any:
        """The checks of a month, newest first (``GET /v1/models/checks``).

        ``verdict`` (``right``, ``partly`` or ``wrong``) filters the ground
        truth; ``reviewed=True`` keeps the checks a person reviewed,
        ``False`` the others. ``after`` is the ``next_after`` of the previous
        page; ``limit`` is 1 to 100 (default 50).
        """
        return self._client._request(
            "GET",
            "/v1/models/checks",
            params={
                "job": _enum_value(job),
                "month": month,
                "verdict": _enum_value(verdict),
                "reviewed": reviewed,
                "after": after,
                "limit": limit,
            },
        )

    def review(
        self,
        call_id: str,
        *,
        agree: bool,
        verdict: str | models.CheckVerdict | None = None,
        score: float | None = None,
        reference: Any | None = None,
        note: str | None = None,
    ) -> Any:
        """Agree with the judge, or correct it (``POST /v1/models/checks/review``).

        ``agree=True`` keeps the judge's verdict (with an optional ``note``).
        ``agree=False`` corrects it: ``verdict`` is required, ``score`` is 0
        to 1, ``reference`` is the right answer (for a rerank or label check
        ``{"grades": {"<hit id>": 0..3}}``), and ``note`` is at most 2,000
        characters. The review becomes the call's ground truth and replaces
        an earlier review, which moves to ``history``. Arguments left at
        ``None`` stay off the wire. Returns the check.
        """
        body: dict[str, Any] = {"agree": agree}
        if verdict is not None:
            body["verdict"] = _enum_value(verdict)
        if score is not None:
            body["score"] = score
        if reference is not None:
            body["reference"] = reference
        if note is not None:
            body["note"] = note
        return self._client._request(
            "POST", "/v1/models/checks/review", params={"id": call_id}, body=body
        )

    def export(
        self,
        *,
        job: str | models.ModelJob | None = None,
        month: str | None = None,
    ) -> Any:
        """The checks of a month as a list of parsed JSON lines, oldest first
        (``GET /v1/models/checks/export``): ``{"call": ..., "check": ...}``,
        ``call`` ``None`` when the log no longer holds it. At most 10,000
        lines."""
        return self._client._request(
            "GET",
            "/v1/models/checks/export",
            params={"job": _enum_value(job), "month": month},
        )

    def calls(
        self,
        *,
        job: str | models.ModelJob | None = None,
        day: str | None = None,
        checked: bool | None = None,
        after: str | None = None,
        limit: int | None = None,
    ) -> Any:
        """The call log, newest first (``GET /v1/models/calls``).

        ``day`` (``yyyy-mm-dd``, UTC) is the newest day of the page (default
        today, or the day of ``after``); a page reads back at most 31 days.
        ``checked=True`` keeps the calls that have a check, ``False`` those
        without one. ``after`` is the ``next_after`` of the previous page;
        ``limit`` is 1 to 100 (default 50).
        """
        return self._client._request(
            "GET",
            "/v1/models/calls",
            params={
                "job": _enum_value(job),
                "day": day,
                "checked": checked,
                "after": after,
                "limit": limit,
            },
        )

    def call(self, call_id: str) -> Any:
        """One call: its input and output, its groundings as text, and its check."""
        return self._client._request(
            "GET", "/v1/models/calls/get", params={"id": call_id}
        )

    def check_call(self, call_id: str, *, options: RequestOptions | None = None) -> Any:
        """Ask the judge to check one call now (``POST /v1/models/calls/check``).

        The call goes to the graph's checks workflow; a call that has a check
        is checked again, and its review stays. Returns ``{"queued": True}``.
        ``400 model_call_not_checkable`` for an ``embed`` call or a failed
        call; ``503`` without a checker. A check spends the platform's judge
        budget, so a failed call is not retried unless
        ``options={"retry": True}``.
        """
        return self._client._request(
            "POST",
            "/v1/models/calls/check",
            params={"id": call_id},
            options={"retry": False, **(options or {})},
        )


class _SearchTuningNamespace:
    """Search tuning: a session runs the graph's own searches with other
    search settings, grades the hits with the platform's judge, and proposes
    the settings that rank best. A person applies the proposal; a session
    never changes a setting by itself.
    """

    def __init__(self, client: _BaseLbbClient) -> None:
        self._client = client

    def start(
        self,
        *,
        queries: int | None = None,
        rounds: int | None = None,
        options: RequestOptions | None = None,
    ) -> Any:
        """Start a session (``POST /v1/search/tuning``).

        ``queries`` is 6 to 40 (default 40) and ``rounds`` 1 to 3 (default 3);
        an argument left at ``None`` stays off the wire. Returns the session,
        ``queued``: read it with :meth:`get` until its ``status`` is ``done``
        or ``failed``. One session per graph runs at a time
        (``409 tuning_running``). A session spends the platform's judge
        budget, so a failed call is not retried unless
        ``options={"retry": True}``.
        """
        body: dict[str, Any] = {}
        if queries is not None:
            body["queries"] = queries
        if rounds is not None:
            body["rounds"] = rounds
        return self._client._request(
            "POST",
            "/v1/search/tuning",
            body=body,
            options={"retry": False, **(options or {})},
        )

    def list(self, *, limit: int | None = None) -> Any:
        """The graph's sessions, newest first (``limit`` 1 to 50, default 10)."""
        return self._client._request("GET", "/v1/search/tuning", params={"limit": limit})

    def get(self, session_id: str) -> Any:
        """One session: its step, the baseline, the variants with their
        scores, the rounds with the judge's notes, and the proposal."""
        return self._client._request(
            "GET", "/v1/search/tuning/get", params={"id": session_id}
        )

    def apply(self, session_id: str) -> Any:
        """Set the graph's search settings to the session's proposal
        (``POST /v1/search/tuning/apply``): exactly the settings the session
        tested. A setting the proposal leaves unset goes back to its default.
        Returns the session with ``applied_at_ms`` and ``applied_by``;
        ``409 tuning_no_proposal`` when it has none. A retry sets the same
        settings, so it is safe."""
        return self._client._request(
            "POST",
            "/v1/search/tuning/apply",
            params={"id": session_id},
            options={"retry": True},
        )


class _EmbeddingsNamespace:
    """Search: embeddings declared on classes of the graph.

    The platform keeps the vectors in step with the published graph; a
    search checks every hit against one graph snapshot.
    """

    def __init__(self, client: _BaseLbbClient) -> None:
        self._client = client
        # Sessions that test search settings on the graph's own searches.
        self.search_tuning = _SearchTuningNamespace(client)

    def list(self) -> Any:
        """Every embedding of the graph with its status."""
        return self._client._request("GET", "/v1/embeddings")

    def get(self, name: str) -> Any:
        """One embedding: serving and building version, backfill, lag, recall."""
        return self._client._request("GET", "/v1/embeddings", params={"name": name})

    def declare(
        self,
        class_: str,
        *,
        name: str | None = None,
        from_: Sequence[str | Mapping[str, Any]] | None = None,
        exclude: Sequence[str] | None = None,
        title: str | None = None,
        model: str | None = None,
        dim: int | None = None,
    ) -> Any:
        """Declare or change the embedding of a class (``PUT /v1/embeddings``).

        ``from_`` (the wire field ``from``) are one-hop property paths
        (``"label"``, ``"description"``, ``"calls/label"``); without them the
        server picks the label, frequent text, and the names of linked
        entities. ``title`` is the field that names each hit
        (``"display_name"``); without it a new embedding takes the ontology's
        name property. On an existing embedding, only what you name changes:
        no ``from_`` keeps its fields, no ``title`` keeps its name, no
        ``model`` keeps its model. A new recipe builds as a new version while
        the old one serves.
        """
        body: dict[str, Any] = {"class": class_}
        if name is not None:
            body["name"] = name
        if from_ is not None:
            body["from"] = list(from_)
        if exclude is not None:
            body["exclude"] = list(exclude)
        if title is not None:
            body["title"] = title
        if model is not None:
            body["model"] = model
        if dim is not None:
            body["dim"] = dim
        return self._client._request("PUT", "/v1/embeddings", body=body)

    def preview(
        self,
        class_: str,
        *,
        name: str | None = None,
        from_: Sequence[str | Mapping[str, Any]] | None = None,
        exclude: Sequence[str] | None = None,
        title: str | None = None,
        model: str | None = None,
        dim: int | None = None,
        sample: int | None = None,
        iris: Sequence[str] | None = None,
    ) -> Any:
        """What a declaration would embed: the fields, every candidate fact
        of the class with its coverage and examples, and the exact text of
        sample entities. Stores nothing and calls no model."""
        body: dict[str, Any] = {"class": class_}
        if name is not None:
            body["name"] = name
        if from_ is not None:
            body["from"] = list(from_)
        if exclude is not None:
            body["exclude"] = list(exclude)
        if title is not None:
            body["title"] = title
        if model is not None:
            body["model"] = model
        if dim is not None:
            body["dim"] = dim
        if sample is not None:
            body["sample"] = sample
        if iris is not None:
            body["iris"] = list(iris)
        return self._client._request("POST", "/v1/embeddings/preview", body=body)

    def set_model(self, model: str, *, dim: int | None = None) -> Any:
        """Move every embedding of the graph to another model (``PUT /v1/embeddings/model``).

        A graph has one embedding model. Each embedding builds a new version
        with it; the graph switches at once when every embedding has it
        ready, so a search never mixes two models.
        """
        body: dict[str, Any] = {"model": model}
        if dim is not None:
            body["dim"] = dim
        return self._client._request("PUT", "/v1/embeddings/model", body=body)

    def refresh(self, name: str) -> Any:
        """Run one bounded step of the embed job now."""
        return self._client._request("POST", "/v1/embeddings/refresh", params={"name": name})

    def delete(self, name: str) -> Any:
        return self._client._request(
            "DELETE", "/v1/embeddings", params={"name": name, "confirm": name}
        )

    def search(
        self,
        text: str,
        *,
        embedding: str | None = None,
        filter_: Sequence[Mapping[str, Any]] | None = None,
        top_k: int | None = None,
        include: Sequence[str] | None = None,
        probe: int | None = None,
        request: str | None = None,
        explain: bool = False,
        rerank: bool | None = None,
    ) -> Any:
        """Search by meaning (``POST /v1/search``).

        The search covers every searchable class of the graph, or one
        ``embedding``. ``filter_`` is the list of conditions every hit must
        meet (the wire field ``filter``): ``{"class": iri}`` (or a list of
        IRIs; their subclasses too; one class condition per search) and
        ``{"via": "calls", "to": "payment-service", "direction": "out"}``
        (``to`` an IRI or a name, or a list). Every hit carries its ``class``
        and is checked against one graph snapshot; the response's ``filter``
        shows how each condition resolved. ``include=["text"]`` returns the
        embedded text of each hit; ``probe`` sets how many clusters are read;
        ``explain=True`` returns the plan without a model call. ``rerank=True``
        orders the best hits by the managed rerank model (each hit then
        carries its ``relevance``), ``rerank=False`` keeps the similarity
        order; without it the graph's search setting decides.
        """
        body: dict[str, Any] = {"text": text}
        if embedding is not None:
            body["embedding"] = embedding
        if filter_:
            body["filter"] = [dict(condition) for condition in filter_]
        if top_k is not None:
            body["top_k"] = top_k
        if include is not None:
            body["include"] = list(include)
        if probe is not None:
            body["probe"] = probe
        if request is not None:
            body["request"] = request
        if explain:
            body["explain"] = True
        if rerank is not None:
            body["rerank"] = rerank
        return self._client._request("POST", "/v1/search", body=body)

    def search_settings(self) -> Any:
        """The graph's search settings (``GET /v1/search/settings``): whether
        every search reranks its best hits, ``rerank_depth``, ``blend`` and
        ``probe_factor`` when they are set, and the rerank model the server
        has (``rerank_available``)."""
        return self._client._request("GET", "/v1/search/settings")

    def set_search_settings(
        self,
        *,
        rerank: bool | None = None,
        rerank_depth: int | _Reset | None = None,
        blend: float | _Reset | None = None,
        probe_factor: float | _Reset | None = None,
    ) -> Any:
        """Change the graph's search settings (``PUT /v1/search/settings``).

        ``rerank`` turns the rerank on or off for every search; a search's
        own ``rerank`` still decides for itself. ``rerank_depth`` (20 to 80)
        is the hits the rerank model reads; ``blend`` (0 to 1) mixes the
        rerank order and the similarity order; ``probe_factor`` (1 to 4)
        widens the vector search over big runs. A setting left at ``None``
        stays off the wire and keeps its value. Pass :data:`RESET` to set one
        back to its default (sent as JSON ``null``). Returns the settings
        after the change.
        """
        body: dict[str, Any] = {}
        if rerank is not None:
            body["rerank"] = rerank
        for name, value in (
            ("rerank_depth", rerank_depth),
            ("blend", blend),
            ("probe_factor", probe_factor),
        ):
            if value is RESET:
                body[name] = None
            elif value is not None:
                body[name] = value
        return self._client._request("PUT", "/v1/search/settings", body=body)


class _EntityNamespace:
    def __init__(self, client: _BaseLbbClient) -> None:
        self._client = client

    def detail(
        self,
        *,
        id: str | None = None,
        type: str | None = None,
        name: str | None = None,
        key: str | None = None,
        consistency: str | None = None,
        edges: int | None = None,
        as_of_commit_seq: int | None = None,
    ) -> Any:
        """One record's typed attributes and current links (``GET /v1/graph/entity``).

        Name the record by ``id``, by ``type`` and ``name``, or by ``type``
        and ``key``. ``edges`` caps the links read per direction (1 to
        10,000, default 1,000). ``consistency="strong"`` reads your own
        write at once; ``as_of_commit_seq`` reads the record at a retained
        commit. ``unavailable_sections`` names the sections this read
        does not fill.
        """
        return self._client._request(
            "GET",
            "/v1/graph/entity",
            params=self._detail_params(
                id, type, name, key, consistency, edges, as_of_commit_seq
            ),
        )

    def detail_model(
        self,
        *,
        id: str | None = None,
        type: str | None = None,
        name: str | None = None,
        key: str | None = None,
        consistency: str | None = None,
        edges: int | None = None,
        as_of_commit_seq: int | None = None,
    ) -> models.EntityDetailResponse:
        """One record validated as ``EntityDetailResponse``."""
        return self._client._model_request(
            models.EntityDetailResponse,
            "GET",
            "/v1/graph/entity",
            params=self._detail_params(
                id, type, name, key, consistency, edges, as_of_commit_seq
            ),
        )

    def _detail_params(
        self,
        id: str | None,
        type: str | None,
        name: str | None,
        key: str | None,
        consistency: str | None,
        edges: int | None,
        as_of_commit_seq: int | None,
    ) -> dict[str, Any]:
        params = {
            "id": id,
            "type": type,
            "name": name,
            "key": key,
            "consistency": self._client._resolve_consistency(consistency),
            "edges": edges,
            "as_of_commit_seq": as_of_commit_seq,
        }
        return {field: value for field, value in params.items() if value is not None}

    def filter_by_attributes(
        self,
        *,
        patterns: Sequence[Mapping[str, Any]],
        where: Mapping[str, Any] | Sequence[Mapping[str, Any]],
        filters: Sequence[Mapping[str, Any]] | None = None,
        select: Sequence[str] | None = None,
        limit: int | None = None,
        offset: int | None = None,
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
            "order_by": list(order_by) if order_by is not None else None,
            "reason": reason,
            "max_solutions": max_solutions,
            "max_object_reads": max_object_reads,
            "max_fetched_bytes": max_fetched_bytes,
        }
        return {key: value for key, value in body.items() if value is not None}
