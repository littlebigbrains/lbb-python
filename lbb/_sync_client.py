"""Synchronous transport for the little big brain Python SDK."""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping
from typing import Any, cast

import httpx

from . import models
from ._client_base import (
    DEFAULT_BASE_URL,
    DEFAULT_MAX_RETRIES,
    DEFAULT_RETRY_BUDGET_MS,
    DEFAULT_TIMEOUT,
    Body,
    IndexLineageObservation,
    ListPage,
    ModelT,
    RawLbbResponse,
    RequestOptions,
    RetryEvent,
    RowT,
    SparqlResults,
    _BaseLbbClient,
    _body_marks_terminal,
    _ContextNamespace,
    _EntityNamespace,
    _error_body_field,
    _jittered_backoff,
    _OntologyNamespace,
    _parse_model,
    _QueryNamespace,
    _raw_response,
    _retry_allowed,
    _retry_delay_seconds,
    _retryable,
    _SchemaNamespace,
)


class _SyncContextNamespace(_ContextNamespace):
    def suggest(
        self, body: Body, *, options: RequestOptions | None = None
    ) -> models.SearchSuggestResponse:
        return cast(models.SearchSuggestResponse, super().suggest(body, options=options))

    def resolve(
        self, body: Body, *, options: RequestOptions | None = None
    ) -> models.ResolveTermResponse:
        return cast(models.ResolveTermResponse, super().resolve(body, options=options))

    def decode(
        self, body: Body, *, options: RequestOptions | None = None
    ) -> models.DecodeResponse:
        return cast(models.DecodeResponse, super().decode(body, options=options))

    def groundability(
        self, *, sample: int | None = None, options: RequestOptions | None = None
    ) -> models.GroundabilityReport:
        return cast(
            models.GroundabilityReport,
            super().groundability(sample=sample, options=options),
        )

class _SyncOntologyNamespace(_OntologyNamespace):
    def view(
        self, *, counts: bool = False, options: RequestOptions | None = None
    ) -> models.OntologyView:
        return cast(models.OntologyView, super().view(counts=counts, options=options))

    def conformance(
        self,
        *,
        consistency: str | None = None,
        options: RequestOptions | None = None,
    ) -> models.SchemaAuditReport:
        return cast(
            models.SchemaAuditReport,
            super().conformance(consistency=consistency, options=options),
        )

    def search(
        self, body: Body, *, options: RequestOptions | None = None
    ) -> models.OntologySearchResponse:
        return cast(models.OntologySearchResponse, super().search(body, options=options))

    def resolve(
        self, body: Body, *, options: RequestOptions | None = None
    ) -> models.OntologyResolveResponse:
        return cast(models.OntologyResolveResponse, super().resolve(body, options=options))

    def define(self, body: Body) -> models.OntologyDefineResponse:
        return cast(models.OntologyDefineResponse, super().define(body))

    def evolve(
        self, body: Body, *, dry_run: bool = False
    ) -> models.OntologyEvolveResponse:
        return cast(
            models.OntologyEvolveResponse,
            super().evolve(body, dry_run=dry_run),
        )


class _SyncQueryNamespace(_QueryNamespace):
    def structured(
        self,
        body: Body,
        *,
        consistency: str | None = None,
        min_indexed_seq: int | None = None,
        options: RequestOptions | None = None,
    ) -> models.SparqlSelectResponse:
        return cast(
            models.SparqlSelectResponse,
            super().structured(
                body,
                consistency=consistency,
                min_indexed_seq=min_indexed_seq,
                options=options,
            ),
        )

    def sparql(
        self,
        query: str,
        *,
        reason: bool | None = None,
        entailment: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
        consistency: str | None = None,
        min_indexed_seq: int | None = None,
    ) -> SparqlResults:
        return cast(
            SparqlResults,
            super().sparql(
                query,
                reason=reason,
                entailment=entailment,
                limit=limit,
                offset=offset,
                consistency=consistency,
                min_indexed_seq=min_indexed_seq,
            ),
        )

    def analytics(
        self, body: Body, *, options: RequestOptions | None = None
    ) -> models.AnalyticQueryResponse:
        return cast(models.AnalyticQueryResponse, super().analytics(body, options=options))

class LbbClient(_BaseLbbClient):
    """Synchronous client. Usable as a context manager."""

    context: _SyncContextNamespace
    entities: _EntityNamespace
    ontology: _SyncOntologyNamespace
    query: _SyncQueryNamespace
    schema: _SchemaNamespace

    def __init__(
        self,
        base_url: str = DEFAULT_BASE_URL,
        *,
        api_key: str | None = None,
        graph: str | None = None,
        branch: str | None = None,
        api_version: str = "2026-07-23",
        max_retries: int = DEFAULT_MAX_RETRIES,
        retry_delay: float = 0.1,
        retry_budget_ms: float = DEFAULT_RETRY_BUDGET_MS,
        on_retry: Callable[[RetryEvent], None] | None = None,
        timeout: float = DEFAULT_TIMEOUT,
        transport: httpx.BaseTransport | None = None,
        event_hooks: Mapping[str, list[Callable[[Any], Any]]] | None = None,
        default_consistency: str | None = None,
    ) -> None:
        super().__init__(
            base_url,
            api_key=api_key,
            graph=graph,
            branch=branch,
            api_version=api_version,
            max_retries=max_retries,
            retry_delay=retry_delay,
            retry_budget_ms=retry_budget_ms,
            on_retry=on_retry,
            default_consistency=default_consistency,
        )
        self.context = _SyncContextNamespace(self)
        self.entities = _EntityNamespace(self)
        self.ontology = _SyncOntologyNamespace(self)
        self.query = _SyncQueryNamespace(self)
        self.schema = _SchemaNamespace(self)
        self._http = httpx.Client(timeout=timeout, transport=transport, event_hooks=event_hooks)

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
    ) -> RawLbbResponse:
        request_options = options or {}
        kwargs = self._request_kwargs(
            params=params,
            body=body,
            content=content,
            content_type=content_type,
            idempotency_key=idempotency_key,
            headers=request_options.get("headers"),
        )
        if "timeout" in request_options:
            kwargs["timeout"] = request_options["timeout"]
        response: httpx.Response | None = None
        can_retry = request_options.get("retry", _retry_allowed(method, idempotency_key))
        max_retries = request_options.get("max_retries", self._max_retries)
        if max_retries < 0:
            raise ValueError("max_retries must be non-negative")
        retry_budget_ms = request_options.get("retry_budget_ms", self._retry_budget_ms)
        started_at = time.monotonic()
        # Deadline is the binding limit; `max_retries` is a secondary safety cap.
        deadline = started_at + max(0.0, retry_budget_ms) / 1000.0
        attempts = 0
        for attempt in range(max_retries + 1):
            attempts = attempt + 1
            try:
                response = self._http.request(method, f"{self._base_url}{path}", **kwargs)
            except httpx.RequestError:
                if not (can_retry and attempt < max_retries):
                    raise
                delay = _jittered_backoff(self._retry_delay, attempt)
                if time.monotonic() + delay > deadline:
                    raise
                self._emit_retry(
                    method,
                    path,
                    attempt=attempts,
                    status_code=None,
                    error_code=None,
                    delay_seconds=delay,
                    elapsed_ms=(time.monotonic() - started_at) * 1000,
                )
                time.sleep(delay)
                continue
            if response.status_code // 100 == 2 or not _retryable(response.status_code):
                break
            if not can_retry or attempt >= max_retries:
                break
            # Honor the server's typed body verdict: a terminal error
            # (`retryable: false`, e.g. an exhausted quota) is surfaced at once
            # rather than retried to the budget.
            if _body_marks_terminal(response):
                break
            delay = _retry_delay_seconds(response, self._retry_delay, attempt)
            if time.monotonic() + delay > deadline:
                break
            self._emit_retry(
                method,
                path,
                attempt=attempts,
                status_code=response.status_code,
                error_code=_error_body_field(response, "code"),
                delay_seconds=delay,
                elapsed_ms=(time.monotonic() - started_at) * 1000,
            )
            time.sleep(delay)
        assert response is not None
        return _raw_response(
            response,
            attempts=attempts,
            elapsed_ms=(time.monotonic() - started_at) * 1000,
        )

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
    ) -> Any:
        return self.raw_request(
            method,
            path,
            params=params,
            body=body,
            content=content,
            content_type=content_type,
            idempotency_key=idempotency_key,
            options=options,
        ).data

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
    ) -> ModelT:
        return _parse_model(
            model_cls,
            self._request(
                method,
                path,
                params=params,
                body=body,
                content=content,
                content_type=content_type,
                idempotency_key=idempotency_key,
                options=options,
            ),
        )

    def _page_request(self, row_model: type[RowT], payload: Any) -> ListPage[RowT]:
        return ListPage.from_payload(payload, row_model)

    def wait_for_index_lineage(
        self,
        target_seq: int,
        *,
        timeout: float = 30.0,
        poll_interval: float = 0.25,
    ) -> IndexLineageObservation:
        """Wait until BM25, ANN, and adjacency all cover ``target_seq``.

        Returns typed lineage plus the build/replica headers from the exact
        observation that satisfied the gate, so a timeout or replica skew is
        diagnosable without a second request.
        """
        deadline = time.monotonic() + timeout
        last: RawLbbResponse | None = None
        while True:
            last = self.raw_request("GET", "/v1/graph/metadata")
            metadata = last.model(models.GraphMetadataResponse)
            lineage = metadata.index_lineage
            if (
                lineage is not None
                and lineage.bm25_indexed_commit_seq is not None
                and lineage.bm25_indexed_commit_seq.root >= target_seq
                and lineage.ann_indexed_commit_seq is not None
                and lineage.ann_indexed_commit_seq.root >= target_seq
                and lineage.adjacency_indexed_commit_seq is not None
                and lineage.adjacency_indexed_commit_seq.root >= target_seq
            ):
                return IndexLineageObservation(
                    metadata=metadata,
                    lineage=lineage,
                    build_commit=last.headers.get("lbb-build-commit"),
                    replica=last.headers.get("lbb-replica"),
                    request_id=last.request_id,
                    attempts=last.attempts,
                    elapsed_ms=last.elapsed_ms,
                )
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"index lineage did not reach {target_seq} within {timeout}s "
                    f"(build={last.headers.get('lbb-build-commit')}, "
                    f"replica={last.headers.get('lbb-replica')}, lineage={lineage})"
                )
            time.sleep(poll_interval)

    def sparql(
        self,
        query: str,
        *,
        reason: bool | None = None,
        entailment: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
        consistency: str | None = None,
        min_indexed_seq: int | None = None,
    ) -> SparqlResults:
        """Run a SPARQL 1.1 text query (SELECT or ASK) and return parsed results.

        The ergonomic entry point: pass query text, get a :class:`SparqlResults`
        with ``.rows()``, ``.vars``, and ``.boolean`` already parsed — no manual
        ``json.loads`` of a results string. Engine extensions map to query
        options: ``reason`` (fold rule-derived edges), ``entailment`` (``"none"``
        to disable the default ``rdfs:subClassOf`` closure), and
        ``limit``/``offset``.

        Note: this uses ``/v1/query/sparql-text``. A standalone stack also serves
        the native SPARQL 1.1 *Protocol* at ``/sparql`` for off-the-shelf SPARQL
        clients (YASGUI, Protégé, RDFLib) with ``Accept``-negotiated
        JSON/XML/CSV/TSV; this SDK method returns parsed JSON rows.
        """
        envelope = self._sparql_text_envelope(
            query,
            reason=reason,
            entailment=entailment,
            limit=limit,
            offset=offset,
            consistency=consistency,
            min_indexed_seq=min_indexed_seq,
        )
        return SparqlResults.from_envelope(envelope)

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> LbbClient:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()
