"""Synchronous transport for the little big brain Python SDK."""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from itertools import chain
from typing import Any, cast

import httpx

from . import models
from ._client_base import (
    DEFAULT_BASE_URL,
    DEFAULT_MAX_RETRIES,
    DEFAULT_RETRY_BUDGET_MS,
    DEFAULT_TIMEOUT,
    Body,
    LbbCapabilityError,
    ListPage,
    ModelT,
    RawLbbResponse,
    RequestOptions,
    RetryEvent,
    RowT,
    SparqlResults,
    _BaseLbbClient,
    _body_marks_terminal,
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

ImportItem = Mapping[str, Any] | str | bytes
ImportSource = Iterable[ImportItem] | str | bytes


def _iter_import_ndjson(lines: ImportSource) -> Iterator[bytes]:
    source: Iterable[ImportItem] = [lines] if isinstance(lines, (str, bytes)) else lines
    for line in source:
        if isinstance(line, bytes):
            encoded = line
        elif isinstance(line, str):
            encoded = line.encode()
        else:
            encoded = json.dumps(line, separators=(",", ":")).encode()
        yield encoded if encoded.endswith(b"\n") else encoded + b"\n"


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
        return cast(
            models.OntologySearchResponse, super().search(body, options=options)
        )

    def resolve(
        self, body: Body, *, options: RequestOptions | None = None
    ) -> models.OntologyResolveResponse:
        return cast(
            models.OntologyResolveResponse, super().resolve(body, options=options)
        )

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


class LbbClient(_BaseLbbClient):
    """Synchronous client. Usable as a context manager."""

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
        self.entities = _EntityNamespace(self)
        self.ontology = _SyncOntologyNamespace(self)
        self.query = _SyncQueryNamespace(self)
        self.schema = _SchemaNamespace(self)
        self._http = httpx.Client(
            timeout=timeout, transport=transport, event_hooks=event_hooks
        )
        self._capabilities: set[str] | None = None

    def _require_capability(self, capability: str) -> None:
        if self._capabilities is None:
            response = self.raw_request("GET", "/version").data
            advertised = (
                response.get("capabilities", [])
                if isinstance(response, Mapping)
                else []
            )
            self._capabilities = {str(item) for item in advertised}
        if capability not in self._capabilities:
            raise LbbCapabilityError(capability)

    def submit_import_ndjson(
        self,
        lines: ImportSource,
        *,
        idempotency_key: str,
        batch: int | None = None,
        strict: bool | None = None,
        observed_at: str | None = None,
    ) -> models.GraphImportJobAccepted:
        """Stream NDJSON once and enqueue a durable import job.

        Automatic transport retries are disabled because an arbitrary iterator
        may be one-shot. Reinvoke with a fresh iterable and the same explicit
        key for an idempotent replay.
        """
        if not idempotency_key.strip():
            raise ValueError(
                "submit_import_ndjson requires a non-empty idempotency_key"
            )
        self._require_capability("durable_import_jobs_v1")
        content = _iter_import_ndjson(lines)
        try:
            first = next(content)
        except StopIteration as error:
            raise ValueError(
                "submit_import_ndjson requires at least one NDJSON record or byte chunk"
            ) from error
        return self._model_request(
            models.GraphImportJobAccepted,
            "POST",
            "/v1/graph/import-jobs",
            params={"batch": batch, "strict": strict, "observed_at": observed_at},
            content=chain((first,), content),
            content_type="application/x-ndjson",
            idempotency_key=idempotency_key,
            options={"max_retries": 0, "retry": False},
        )

    def get_import_job(self, job_id: str) -> models.GraphImportJobStatus:
        self._require_capability("durable_import_jobs_v1")
        return self._model_request(
            models.GraphImportJobStatus,
            "GET",
            "/v1/graph/import-jobs",
            params={"job_id": job_id},
        )

    def cancel_import_job(self, job_id: str) -> models.GraphImportJobCancelResponse:
        self._require_capability("durable_import_jobs_v1")
        return self._model_request(
            models.GraphImportJobCancelResponse,
            "DELETE",
            "/v1/graph/import-jobs",
            params={"job_id": job_id},
        )

    def wait_for_import_job(
        self,
        job_id: str,
        *,
        timeout: float | None = None,
        poll_interval: float = 1.0,
    ) -> models.GraphImportJobStatus:
        if poll_interval < 0:
            raise ValueError("poll_interval must be non-negative")
        if timeout is not None and timeout < 0:
            raise ValueError("timeout must be non-negative")
        deadline = time.monotonic() + timeout if timeout is not None else None
        terminal = {
            models.GraphImportJobState.succeeded,
            models.GraphImportJobState.failed,
            models.GraphImportJobState.cancelled,
        }
        while True:
            status = self.get_import_job(job_id)
            if status.state in terminal:
                return status
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError(f"timed out waiting for durable import job {job_id}")
            time.sleep(poll_interval)

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
        can_retry = request_options.get(
            "retry", _retry_allowed(method, idempotency_key)
        )
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
                response = self._http.request(
                    method, f"{self._base_url}{path}", **kwargs
                )
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
        content: Any | None = None,
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
        content: Any | None = None,
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

    def wait_for_published(
        self,
        target_seq: int,
        *,
        timeout: float = 30.0,
        poll_interval: float = 0.25,
    ) -> models.PublicationStatusResponse:
        """Wait until reconciliation folds ``target_seq`` into the RDF base."""
        if target_seq < 0:
            raise ValueError("target_seq must be non-negative")
        if timeout < 0 or poll_interval < 0:
            raise ValueError("timeout and poll_interval must be non-negative")
        deadline = time.monotonic() + timeout
        while True:
            status = self._model_request(
                models.PublicationStatusResponse,
                "GET",
                "/v1/graph/publication-status",
                options={"max_retries": 0},
            )
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
        options: ``entailment`` (``"rdfs"`` for the query-time RDFS core,
        ``"subclass"`` for the class-only subset; the default ``"none"`` matches
        asserted triples only) and ``limit``/``offset``. ``reason`` is refused
        on the published surface (stored rules already run at publish time).

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
