"""Asynchronous transport for the little big brain Python SDK."""

from __future__ import annotations

import asyncio
import inspect
import json
from collections.abc import (
    AsyncIterable,
    AsyncIterator,
    Callable,
    Iterable,
    Mapping,
    Sequence,
)
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
    LbbCapabilityError,
    LbbError,
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
    _FactsNamespace,
    _GraphNamespace,
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

AsyncImportItem = Mapping[str, Any] | str | bytes
AsyncImportSource = (
    AsyncIterable[AsyncImportItem] | Iterable[AsyncImportItem] | str | bytes
)


def _import_bytes(line: AsyncImportItem) -> bytes:
    if isinstance(line, bytes):
        encoded = line
    elif isinstance(line, str):
        encoded = line.encode()
    else:
        encoded = json.dumps(line, separators=(",", ":")).encode()
    return encoded if encoded.endswith(b"\n") else encoded + b"\n"


async def _aiter_import_ndjson(lines: AsyncImportSource) -> AsyncIterator[bytes]:
    if isinstance(lines, (str, bytes)):
        yield _import_bytes(lines)
    elif isinstance(lines, AsyncIterable):
        async for line in lines:
            yield _import_bytes(line)
    else:
        for line in lines:
            yield _import_bytes(line)


class _AsyncOntologyNamespace(_OntologyNamespace):
    async def view(
        self, *, counts: bool = False, options: RequestOptions | None = None
    ) -> models.OntologyView:
        return cast(
            models.OntologyView, await super().view(counts=counts, options=options)
        )

    async def conformance(
        self,
        *,
        consistency: str | None = None,
        options: RequestOptions | None = None,
    ) -> models.SchemaAuditReport:
        return cast(
            models.SchemaAuditReport,
            await super().conformance(consistency=consistency, options=options),
        )

    async def search(
        self, body: Body, *, options: RequestOptions | None = None
    ) -> models.OntologySearchResponse:
        return cast(
            models.OntologySearchResponse,
            await super().search(body, options=options),
        )

    async def resolve(
        self, body: Body, *, options: RequestOptions | None = None
    ) -> models.OntologyResolveResponse:
        return cast(
            models.OntologyResolveResponse,
            await super().resolve(body, options=options),
        )

    async def define(self, body: Body) -> models.OntologyDefineResponse:
        return cast(models.OntologyDefineResponse, await super().define(body))

    async def evolve(
        self, body: Body, *, dry_run: bool = False
    ) -> models.OntologyEvolveResponse:
        return cast(
            models.OntologyEvolveResponse,
            await super().evolve(body, dry_run=dry_run),
        )

    async def induce(
        self, body: Body, *, options: RequestOptions | None = None
    ) -> models.OntologyInduceResponse:
        return cast(
            models.OntologyInduceResponse,
            await super().induce(body, options=options),
        )

    async def draft_create(self, body: Body) -> models.OntologyDraft:
        return cast(models.OntologyDraft, await super().draft_create(body))

    async def draft_get(self, draft_id: str) -> models.OntologyDraft:
        return cast(models.OntologyDraft, await super().draft_get(draft_id))

    async def draft_validate(self, draft_id: str) -> models.OntologyDraft:
        return cast(models.OntologyDraft, await super().draft_validate(draft_id))

    async def draft_promote(
        self, draft_id: str, *, idempotency_key: str | None = None
    ) -> models.OntologyDraft:
        return cast(
            models.OntologyDraft,
            await super().draft_promote(draft_id, idempotency_key=idempotency_key),
        )

    async def draft_reject(self, draft_id: str, reason: str) -> models.OntologyDraft:
        return cast(
            models.OntologyDraft,
            await super().draft_reject(draft_id, reason),
        )


class _AsyncQueryNamespace(_QueryNamespace):
    async def structured(
        self,
        body: Body,
        *,
        consistency: str | None = None,
        min_indexed_seq: int | None = None,
        options: RequestOptions | None = None,
    ) -> models.SparqlSelectResponse:
        return cast(
            models.SparqlSelectResponse,
            await super().structured(
                body,
                consistency=consistency,
                min_indexed_seq=min_indexed_seq,
                options=options,
            ),
        )

    async def sparql(
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
            await super().sparql(
                query,
                reason=reason,
                entailment=entailment,
                limit=limit,
                offset=offset,
                consistency=consistency,
                min_indexed_seq=min_indexed_seq,
            ),
        )

    async def conflicts(
        self, body: Body, *, options: RequestOptions | None = None
    ) -> models.GovernedConflictAggregationResponse:
        return cast(
            models.GovernedConflictAggregationResponse,
            await super().conflicts(body, options=options),
        )


class _AsyncFactsNamespace(_FactsNamespace):
    async def create_model(
        self, body: Body, *, idempotency_key: str | None = None
    ) -> models.GraphCommitResponse:
        return cast(
            models.GraphCommitResponse,
            await super().create_model(body, idempotency_key=idempotency_key),
        )


class _AsyncSchemaNamespace(_SchemaNamespace):
    async def view_model(self) -> models.SchemaBundleView:
        return cast(models.SchemaBundleView, await super().view_model())

    async def publish_model(
        self, body: Body, *, idempotency_key: str | None = None
    ) -> models.SchemaPublishResponse:
        return cast(
            models.SchemaPublishResponse,
            await super().publish_model(body, idempotency_key=idempotency_key),
        )


class _AsyncGraphNamespace(_GraphNamespace):
    facts: _AsyncFactsNamespace

    def __init__(self, client: _BaseLbbClient, graph: str, branch: str | None) -> None:
        super().__init__(client, graph, branch)
        self.facts = _AsyncFactsNamespace(client, graph, branch)

    async def delete(self, *, confirm: str) -> models.GraphDeleteResponse:
        return cast(models.GraphDeleteResponse, await super().delete(confirm=confirm))

    async def delete_branch(self, *, confirm: str) -> models.GraphBranchDeleteResponse:
        return cast(
            models.GraphBranchDeleteResponse,
            await super().delete_branch(confirm=confirm),
        )

    async def retract_model(
        self, body: Body, *, idempotency_key: str | None = None
    ) -> models.GraphRetractResponse:
        return cast(
            models.GraphRetractResponse,
            await super().retract_model(body, idempotency_key=idempotency_key),
        )


class _AsyncEntityNamespace(_EntityNamespace):
    async def sample(
        self,
        *,
        type: str,
        limit: int | None = None,
        options: RequestOptions | None = None,
    ) -> models.EntityTypeSampleResponse:
        return cast(
            models.EntityTypeSampleResponse,
            await super().sample(type=type, limit=limit, options=options),
        )

    async def filter_by_attributes_model(
        self, **kwargs: Any
    ) -> models.SparqlSelectResponse:
        return cast(
            models.SparqlSelectResponse,
            await super().filter_by_attributes_model(**kwargs),
        )


class AsyncLbbClient(_BaseLbbClient):
    """Asynchronous client. Usable as an async context manager."""

    entities: _AsyncEntityNamespace
    ontology: _AsyncOntologyNamespace
    query: _AsyncQueryNamespace
    schema: _AsyncSchemaNamespace

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
        transport: httpx.AsyncBaseTransport | None = None,
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
        self.entities = _AsyncEntityNamespace(self)
        self.ontology = _AsyncOntologyNamespace(self)
        self.query = _AsyncQueryNamespace(self)
        self.schema = _AsyncSchemaNamespace(self)
        self._http = httpx.AsyncClient(
            timeout=timeout, transport=transport, event_hooks=event_hooks
        )
        self._capabilities: set[str] | None = None

    async def _require_capability(self, capability: str) -> None:
        if self._capabilities is None:
            response = (await self.raw_request("GET", "/version")).data
            advertised = (
                response.get("capabilities", [])
                if isinstance(response, Mapping)
                else []
            )
            self._capabilities = {str(item) for item in advertised}
        if capability not in self._capabilities:
            raise LbbCapabilityError(capability)

    async def submit_import_ndjson(
        self,
        lines: AsyncImportSource,
        *,
        idempotency_key: str,
        batch: int | None = None,
        strict: bool | None = None,
        observed_at: str | None = None,
    ) -> models.GraphImportJobAccepted:
        """Stream NDJSON once and enqueue a durable import job."""
        if not idempotency_key.strip():
            raise ValueError(
                "submit_import_ndjson requires a non-empty idempotency_key"
            )
        await self._require_capability("durable_import_jobs_v1")
        content = _aiter_import_ndjson(lines)
        try:
            first = await anext(content)
        except StopAsyncIteration as error:
            raise ValueError(
                "submit_import_ndjson requires at least one NDJSON record or byte chunk"
            ) from error

        async def nonempty_content() -> AsyncIterator[bytes]:
            yield first
            async for chunk in content:
                yield chunk

        return await self._model_request(
            models.GraphImportJobAccepted,
            "POST",
            "/v1/graph/import-jobs",
            params={"batch": batch, "strict": strict, "observed_at": observed_at},
            content=nonempty_content(),
            content_type="application/x-ndjson",
            idempotency_key=idempotency_key,
            options={"max_retries": 0, "retry": False},
        )

    async def get_import_job(self, job_id: str) -> models.GraphImportJobStatus:
        await self._require_capability("durable_import_jobs_v1")
        return await self._model_request(
            models.GraphImportJobStatus,
            "GET",
            "/v1/graph/import-jobs",
            params={"job_id": job_id},
        )

    async def cancel_import_job(
        self, job_id: str
    ) -> models.GraphImportJobCancelResponse:
        await self._require_capability("durable_import_jobs_v1")
        return await self._model_request(
            models.GraphImportJobCancelResponse,
            "DELETE",
            "/v1/graph/import-jobs",
            params={"job_id": job_id},
        )

    async def wait_for_import_job(
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
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout if timeout is not None else None
        terminal = {
            models.GraphImportJobState.succeeded,
            models.GraphImportJobState.failed,
            models.GraphImportJobState.cancelled,
        }
        while True:
            status = await self.get_import_job(job_id)
            if status.state in terminal:
                return status
            if deadline is not None and loop.time() >= deadline:
                raise TimeoutError(f"timed out waiting for durable import job {job_id}")
            await asyncio.sleep(poll_interval)

    def graph(self, name: str, *, branch: str | None = None) -> _AsyncGraphNamespace:
        return _AsyncGraphNamespace(self, name, branch)

    async def create_graph(self) -> models.CreateGraphResponse:
        return cast(models.CreateGraphResponse, await super().create_graph())

    async def delete_graph(self, *, confirm: str) -> models.GraphDeleteResponse:
        return cast(
            models.GraphDeleteResponse, await super().delete_graph(confirm=confirm)
        )

    async def delete_branch(self, *, confirm: str) -> models.GraphBranchDeleteResponse:
        return cast(
            models.GraphBranchDeleteResponse,
            await super().delete_branch(confirm=confirm),
        )

    async def fork_graph(self, src: str, dst: str) -> models.GraphForkResponse:
        return cast(models.GraphForkResponse, await super().fork_graph(src, dst))

    async def reload(
        self,
        lines: Sequence[Mapping[str, Any]] | str,
        *,
        confirm: str,
        dry_run: bool | None = None,
        strict: bool | None = None,
        observed_at: str | None = None,
        idempotency_key: str | None = None,
    ) -> models.GraphReloadResponse:
        return cast(
            models.GraphReloadResponse,
            await super().reload(
                lines,
                confirm=confirm,
                dry_run=dry_run,
                strict=strict,
                observed_at=observed_at,
                idempotency_key=idempotency_key,
            ),
        )

    async def commit_model(
        self, body: Body, *, idempotency_key: str | None = None
    ) -> models.GraphCommitResponse:
        return cast(
            models.GraphCommitResponse,
            await super().commit_model(body, idempotency_key=idempotency_key),
        )

    async def commit_dry_run_model(
        self, body: Body
    ) -> models.GraphCommitDryRunResponse:
        return cast(
            models.GraphCommitDryRunResponse, await super().commit_dry_run_model(body)
        )

    async def train_submit(
        self, body: Body, *, idempotency_key: str
    ) -> models.TrainModelJobStatusResponse:
        return cast(
            models.TrainModelJobStatusResponse,
            await super().train_submit(body, idempotency_key=idempotency_key),
        )

    async def train_job(self, job_id: str) -> models.TrainModelJobStatusResponse:
        return cast(models.TrainModelJobStatusResponse, await super().train_job(job_id))

    async def search_feedback_export(self) -> models.SearchFeedbackExportResponse:
        return cast(
            models.SearchFeedbackExportResponse,
            await super().search_feedback_export(),
        )

    async def search_feedback_summary(self) -> models.SearchFeedbackSummaryResponse:
        return cast(
            models.SearchFeedbackSummaryResponse,
            await super().search_feedback_summary(),
        )

    async def sparql_select_model(self, body: Body) -> models.SparqlSelectResponse:
        return cast(
            models.SparqlSelectResponse, await super().sparql_select_model(body)
        )

    async def governed_conflicts(
        self, body: Body
    ) -> models.GovernedConflictAggregationResponse:
        return cast(
            models.GovernedConflictAggregationResponse,
            await super().governed_conflicts(body),
        )

    async def ontology_conformance_model(
        self, *, consistency: str | None = None
    ) -> models.SchemaAuditReport:
        return cast(
            models.SchemaAuditReport,
            await super().ontology_conformance_model(consistency=consistency),
        )

    async def ontology_view_model(self, *, counts: bool = False) -> models.OntologyView:
        return cast(
            models.OntologyView, await super().ontology_view_model(counts=counts)
        )

    async def metadata_model(self) -> models.GraphMetadataResponse:
        return cast(models.GraphMetadataResponse, await super().metadata_model())

    async def wait_for_index_lineage(
        self,
        target_seq: int,
        *,
        timeout: float = 30.0,
        poll_interval: float = 0.25,
    ) -> IndexLineageObservation:
        """Wait until one published generation covers ``target_seq``.

        Publication polling owns ``timeout`` directly and works for both the
        RDF-only and full-family deployment rosters.
        """
        deadline = asyncio.get_running_loop().time() + timeout
        last: RawLbbResponse | None = None
        last_error: Exception | None = None
        while True:
            try:
                # This method owns an explicit publication deadline. Do not
                # nest the generic request retry-count cap inside that poll.
                last = await self.raw_request(
                    "GET",
                    "/v1/graph/metadata",
                    options={"max_retries": 0},
                )
            except (LbbError, httpx.RequestError) as error:
                if isinstance(error, LbbError) and (
                    not _retryable(error.status_code) or error.retryable is False
                ):
                    raise
                last_error = error
                now = asyncio.get_running_loop().time()
                if now >= deadline:
                    raise TimeoutError(
                        f"index lineage did not reach {target_seq} within {timeout}s "
                        f"(last_error={error})"
                    ) from error
                retry_after = (
                    float(error.retry_after_seconds or 0)
                    if isinstance(error, LbbError)
                    else 0.0
                )
                await asyncio.sleep(
                    min(max(poll_interval, retry_after), deadline - now)
                )
                continue
            metadata = last.model(models.GraphMetadataResponse)
            lineage = metadata.index_lineage
            served_at_seq = metadata.snapshot.served_at_seq
            if (
                lineage is not None
                and served_at_seq is not None
                and served_at_seq.root >= target_seq
                and (
                    metadata.index_caught_up is True
                    or (
                        lineage.bm25_indexed_commit_seq is not None
                        and lineage.bm25_indexed_commit_seq.root >= target_seq
                        and lineage.ann_indexed_commit_seq is not None
                        and lineage.ann_indexed_commit_seq.root >= target_seq
                    )
                )
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
            if asyncio.get_running_loop().time() >= deadline:
                raise TimeoutError(
                    f"index lineage did not reach {target_seq} within {timeout}s "
                    f"(build={last.headers.get('lbb-build-commit')}, "
                    f"replica={last.headers.get('lbb-replica')}, lineage={lineage}, "
                    f"last_error={last_error})"
                )
            await asyncio.sleep(poll_interval)

    async def summary_model(self) -> models.GraphSummaryResponse:
        return cast(models.GraphSummaryResponse, await super().summary_model())

    async def read_snapshot_model(self) -> models.PublishedReadStatusResponse:
        return cast(
            models.PublishedReadStatusResponse,
            await super().read_snapshot_model(),
        )

    async def list_graphs_model(self) -> models.GraphListResponse:
        return cast(models.GraphListResponse, await super().list_graphs_model())

    async def raw_request(
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
        loop = asyncio.get_running_loop()
        started_at = loop.time()
        # Deadline is the binding limit; `max_retries` is a secondary safety cap.
        deadline = started_at + max(0.0, retry_budget_ms) / 1000.0
        attempts = 0
        for attempt in range(max_retries + 1):
            attempts = attempt + 1
            try:
                response = await self._http.request(
                    method, f"{self._base_url}{path}", **kwargs
                )
            except httpx.RequestError:
                if not (can_retry and attempt < max_retries):
                    raise
                delay = _jittered_backoff(self._retry_delay, attempt)
                if loop.time() + delay > deadline:
                    raise
                self._emit_retry(
                    method,
                    path,
                    attempt=attempts,
                    status_code=None,
                    error_code=None,
                    delay_seconds=delay,
                    elapsed_ms=(loop.time() - started_at) * 1000,
                )
                await asyncio.sleep(delay)
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
            if loop.time() + delay > deadline:
                break
            self._emit_retry(
                method,
                path,
                attempt=attempts,
                status_code=response.status_code,
                error_code=_error_body_field(response, "code"),
                delay_seconds=delay,
                elapsed_ms=(loop.time() - started_at) * 1000,
            )
            await asyncio.sleep(delay)
        assert response is not None
        return _raw_response(
            response,
            attempts=attempts,
            elapsed_ms=(loop.time() - started_at) * 1000,
        )

    async def _request(
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
        response = await self.raw_request(
            method,
            path,
            params=params,
            body=body,
            content=content,
            content_type=content_type,
            idempotency_key=idempotency_key,
            options=options,
        )
        return response.data

    async def _model_request(
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
            await self._request(
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

    async def _page_request(
        self, row_model: type[RowT], payload: Any
    ) -> ListPage[RowT]:
        if inspect.isawaitable(payload):
            payload = await payload
        return ListPage.from_payload(payload, row_model)

    async def sparql(
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
        """Async :meth:`LbbClient.sparql`: run SPARQL text, return parsed results."""
        envelope = await self._sparql_text_envelope(
            query,
            reason=reason,
            entailment=entailment,
            limit=limit,
            offset=offset,
            consistency=consistency,
            min_indexed_seq=min_indexed_seq,
        )
        return SparqlResults.from_envelope(envelope)

    async def aclose(self) -> None:
        await self._http.aclose()

    async def __aenter__(self) -> AsyncLbbClient:
        return self

    async def __aexit__(self, *_exc: object) -> None:
        await self.aclose()
