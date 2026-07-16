"""Asynchronous transport for the little big brain Python SDK."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import AsyncIterator, Callable, Mapping
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


class _AsyncContextNamespace(_ContextNamespace):
    async def ask(
        self, body: Body, *, options: RequestOptions | None = None
    ) -> models.AskResponse:
        return cast(models.AskResponse, await super().ask(body, options=options))

    async def suggest(
        self, body: Body, *, options: RequestOptions | None = None
    ) -> models.SearchSuggestResponse:
        return cast(
            models.SearchSuggestResponse, await super().suggest(body, options=options)
        )

    async def resolve(
        self, body: Body, *, options: RequestOptions | None = None
    ) -> models.ResolveTermResponse:
        return cast(
            models.ResolveTermResponse, await super().resolve(body, options=options)
        )

    async def decode(
        self, body: Body, *, options: RequestOptions | None = None
    ) -> models.DecodeResponse:
        return cast(models.DecodeResponse, await super().decode(body, options=options))

    async def groundability(
        self, *, sample: int | None = None, options: RequestOptions | None = None
    ) -> models.GroundabilityReport:
        return cast(
            models.GroundabilityReport,
            await super().groundability(sample=sample, options=options),
        )


class _AsyncOntologyNamespace(_OntologyNamespace):
    async def view(
        self, *, counts: bool = False, options: RequestOptions | None = None
    ) -> models.OntologyView:
        return cast(
            models.OntologyView, await super().view(counts=counts, options=options)
        )

    async def conformance(
        self, *, options: RequestOptions | None = None
    ) -> models.SchemaAuditReport:
        return cast(
            models.SchemaAuditReport, await super().conformance(options=options)
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
        self, body: Body, *, options: RequestOptions | None = None
    ) -> models.SparqlSelectResponse:
        return cast(
            models.SparqlSelectResponse,
            await super().structured(body, options=options),
        )

    async def sparql(
        self,
        query: str,
        *,
        reason: bool | None = None,
        entailment: str | None = None,
        as_of_valid_time: str | None = None,
        as_of_commit_seq: int | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> SparqlResults:
        return cast(
            SparqlResults,
            await super().sparql(
                query,
                reason=reason,
                entailment=entailment,
                as_of_valid_time=as_of_valid_time,
                as_of_commit_seq=as_of_commit_seq,
                limit=limit,
                offset=offset,
            ),
        )

    async def analytics(
        self, body: Body, *, options: RequestOptions | None = None
    ) -> models.AnalyticQueryResponse:
        return cast(
            models.AnalyticQueryResponse,
            await super().analytics(body, options=options),
        )

    async def shacl(
        self, body: Body, *, options: RequestOptions | None = None
    ) -> models.ShaclQueryResponse:
        return cast(
            models.ShaclQueryResponse, await super().shacl(body, options=options)
        )

    async def infer(
        self, body: Body, *, options: RequestOptions | None = None
    ) -> models.InferenceRunResponse:
        return cast(
            models.InferenceRunResponse, await super().infer(body, options=options)
        )

    async def premises(
        self, body: Body, *, options: RequestOptions | None = None
    ) -> models.RetrievalPremiseResponse:
        return cast(
            models.RetrievalPremiseResponse,
            await super().premises(body, options=options),
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

    async def embedding_models(
        self,
        *,
        options: RequestOptions | None = None,
    ) -> models.ManagedEmbeddingModelsResponse:
        return cast(
            models.ManagedEmbeddingModelsResponse,
            await super().embedding_models(options=options),
        )

    async def embedding_config(
        self, *, options: RequestOptions | None = None
    ) -> models.ManagedEmbeddingConfigResponse:
        return cast(
            models.ManagedEmbeddingConfigResponse,
            await super().embedding_config(options=options),
        )

    async def set_embedding_config(
        self, body: Body
    ) -> models.ManagedEmbeddingConfigResponse:
        return cast(
            models.ManagedEmbeddingConfigResponse,
            await super().set_embedding_config(body),
        )

    async def set_embedding_model(
        self, model_id: str, *, auto_embed_query: bool = True
    ) -> models.ManagedEmbeddingConfigResponse:
        return cast(
            models.ManagedEmbeddingConfigResponse,
            await super().set_embedding_model(
                model_id, auto_embed_query=auto_embed_query
            ),
        )

    async def backfill_embeddings(
        self,
        *,
        batch_size: int | None = None,
        limit: int | None = None,
        full: bool | None = None,
        idempotency_key: str | None = None,
        timeout: float = 1800.0,
        poll_interval: float = 2.0,
    ) -> models.ManagedEmbeddingBackfillResponse:
        status = await self._client._model_request(
            models.ManagedEmbeddingBackfillJobStatusResponse,
            "POST",
            "/v1/graph/embedding/backfill-jobs",
            params={"graph": self._graph, "branch": self._branch},
            body={"batch_size": batch_size, "limit": limit, "full": bool(full)},
            idempotency_key=idempotency_key
            or self._client.idempotency_key("embedding-backfill"),
        )
        deadline = asyncio.get_running_loop().time() + timeout
        while status.status in {"pending", "running"}:
            if asyncio.get_running_loop().time() >= deadline:
                raise TimeoutError(
                    f"embedding backfill {status.job_id} exceeded {timeout}s"
                )
            await asyncio.sleep(poll_interval)
            status = await self.embedding_backfill_job(status.job_id)
        if status.status != "succeeded" or status.result is None:
            raise RuntimeError(
                status.terminal_error
                or f"embedding backfill {status.job_id} ended {status.status}"
            )
        return cast(models.ManagedEmbeddingBackfillResponse, status.result)

    async def submit_embedding_backfill(
        self, body: Body, *, idempotency_key: str
    ) -> models.ManagedEmbeddingBackfillJobStatusResponse:
        return cast(
            models.ManagedEmbeddingBackfillJobStatusResponse,
            await super().submit_embedding_backfill(
                body, idempotency_key=idempotency_key
            ),
        )

    async def embedding_backfill_job(
        self, job_id: str
    ) -> models.ManagedEmbeddingBackfillJobStatusResponse:
        return cast(
            models.ManagedEmbeddingBackfillJobStatusResponse,
            await super().embedding_backfill_job(job_id),
        )

    async def promote_embedding(
        self, *, run_id: str, allow_regression: bool | None = None
    ) -> models.ManagedEmbeddingPromoteResponse:
        return cast(
            models.ManagedEmbeddingPromoteResponse,
            await super().promote_embedding(
                run_id=run_id, allow_regression=allow_regression
            ),
        )

    async def retract_model(
        self, body: Body, *, idempotency_key: str | None = None
    ) -> models.GraphRetractResponse:
        return cast(
            models.GraphRetractResponse,
            await super().retract_model(body, idempotency_key=idempotency_key),
        )

    async def export_rdf_preview(
        self,
        *,
        format: str = "turtle",
        max_triples: int = 100,
        as_of_valid_time: str | None = None,
        as_of_commit_seq: int | None = None,
        entailment: str | None = None,
        reason: bool | None = None,
    ) -> models.RdfExportPreviewResponse:
        return cast(
            models.RdfExportPreviewResponse,
            await super().export_rdf_preview(
                format=format,
                max_triples=max_triples,
                as_of_valid_time=as_of_valid_time,
                as_of_commit_seq=as_of_commit_seq,
                entailment=entailment,
                reason=reason,
            ),
        )


class _AsyncSchemaNamespace(_SchemaNamespace):
    async def view_model(self, *, audit: bool = False) -> models.SchemaBundleView:
        return cast(models.SchemaBundleView, await super().view_model(audit=audit))

    async def preview_model(self, body: Body) -> models.SchemaPreviewResponse:
        return cast(models.SchemaPreviewResponse, await super().preview_model(body))

    async def publish_model(self, body: Body) -> models.SchemaPublishResponse:
        return cast(models.SchemaPublishResponse, await super().publish_model(body))

    async def audit_model(self) -> models.SchemaAuditReport:
        return cast(models.SchemaAuditReport, await super().audit_model())


class _AsyncEntityNamespace(_EntityNamespace):
    async def list_page(self, **kwargs: Any) -> ListPage[models.EntityExplorerRow]:
        return cast(
            ListPage[models.EntityExplorerRow], await super().list_page(**kwargs)
        )

    async def filter(
        self, body: Body, *, options: RequestOptions | None = None
    ) -> models.EntityFilterResponse:
        return cast(
            models.EntityFilterResponse,
            await super().filter(body, options=options),
        )

    async def filter_by_attributes_model(
        self, **kwargs: Any
    ) -> models.SparqlSelectResponse:
        return cast(
            models.SparqlSelectResponse,
            await super().filter_by_attributes_model(**kwargs),
        )

    def pages(self, **kwargs: Any) -> AsyncIterator[ListPage[models.EntityExplorerRow]]:
        return cast(
            AsyncIterator[ListPage[models.EntityExplorerRow]], super().pages(**kwargs)
        )

    def iter(self, **kwargs: Any) -> AsyncIterator[models.EntityExplorerRow]:
        return cast(AsyncIterator[models.EntityExplorerRow], super().iter(**kwargs))


class AsyncLbbClient(_BaseLbbClient):
    """Asynchronous client. Usable as an async context manager."""

    context: _AsyncContextNamespace
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
        api_version: str = "2026-06-22",
        max_retries: int = DEFAULT_MAX_RETRIES,
        retry_delay: float = 0.1,
        retry_budget_ms: float = DEFAULT_RETRY_BUDGET_MS,
        on_retry: Callable[[RetryEvent], None] | None = None,
        timeout: float = DEFAULT_TIMEOUT,
        transport: httpx.AsyncBaseTransport | None = None,
        event_hooks: Mapping[str, list[Callable[[Any], Any]]] | None = None,
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
        )
        self.context = _AsyncContextNamespace(self)
        self.entities = _AsyncEntityNamespace(self)
        self.ontology = _AsyncOntologyNamespace(self)
        self.query = _AsyncQueryNamespace(self)
        self.schema = _AsyncSchemaNamespace(self)
        self._http = httpx.AsyncClient(
            timeout=timeout, transport=transport, event_hooks=event_hooks
        )

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

    async def embedding_models(
        self,
        *,
        options: RequestOptions | None = None,
    ) -> models.ManagedEmbeddingModelsResponse:
        return cast(
            models.ManagedEmbeddingModelsResponse,
            await super().embedding_models(options=options),
        )

    async def embedding_config(
        self, *, options: RequestOptions | None = None
    ) -> models.ManagedEmbeddingConfigResponse:
        return cast(
            models.ManagedEmbeddingConfigResponse,
            await super().embedding_config(options=options),
        )

    async def set_embedding_config(
        self, body: Body
    ) -> models.ManagedEmbeddingConfigResponse:
        return cast(
            models.ManagedEmbeddingConfigResponse,
            await super().set_embedding_config(body),
        )

    async def set_embedding_model(
        self, model_id: str, *, auto_embed_query: bool = True
    ) -> models.ManagedEmbeddingConfigResponse:
        return cast(
            models.ManagedEmbeddingConfigResponse,
            await super().set_embedding_model(
                model_id, auto_embed_query=auto_embed_query
            ),
        )

    async def backfill_embeddings(
        self,
        *,
        batch_size: int | None = None,
        limit: int | None = None,
        full: bool | None = None,
        idempotency_key: str | None = None,
        timeout: float = 1800.0,
        poll_interval: float = 2.0,
    ) -> models.ManagedEmbeddingBackfillResponse:
        status = await self._model_request(
            models.ManagedEmbeddingBackfillJobStatusResponse,
            "POST",
            "/v1/graph/embedding/backfill-jobs",
            body={"batch_size": batch_size, "limit": limit, "full": bool(full)},
            idempotency_key=idempotency_key
            or self.idempotency_key("embedding-backfill"),
        )
        deadline = asyncio.get_running_loop().time() + timeout
        while status.status in {"pending", "running"}:
            if asyncio.get_running_loop().time() >= deadline:
                raise TimeoutError(
                    f"embedding backfill {status.job_id} exceeded {timeout}s"
                )
            await asyncio.sleep(poll_interval)
            status = await self._model_request(
                models.ManagedEmbeddingBackfillJobStatusResponse,
                "GET",
                "/v1/graph/embedding/backfill-jobs",
                params={"job_id": status.job_id},
            )
        if status.status != "succeeded" or status.result is None:
            raise RuntimeError(
                status.terminal_error
                or f"embedding backfill {status.job_id} ended {status.status}"
            )
        return status.result

    async def submit_embedding_backfill(
        self, body: Body, *, idempotency_key: str
    ) -> models.ManagedEmbeddingBackfillJobStatusResponse:
        return cast(
            models.ManagedEmbeddingBackfillJobStatusResponse,
            await super().submit_embedding_backfill(
                body, idempotency_key=idempotency_key
            ),
        )

    async def embedding_backfill_job(
        self, job_id: str
    ) -> models.ManagedEmbeddingBackfillJobStatusResponse:
        return cast(
            models.ManagedEmbeddingBackfillJobStatusResponse,
            await super().embedding_backfill_job(job_id),
        )

    async def cancel_embedding_backfill(
        self, job_id: str
    ) -> models.ManagedEmbeddingBackfillJobStatusResponse:
        return cast(
            models.ManagedEmbeddingBackfillJobStatusResponse,
            await super().cancel_embedding_backfill(job_id),
        )

    async def promote_embedding(
        self, *, run_id: str, allow_regression: bool | None = None
    ) -> models.ManagedEmbeddingPromoteResponse:
        return cast(
            models.ManagedEmbeddingPromoteResponse,
            await super().promote_embedding(
                run_id=run_id, allow_regression=allow_regression
            ),
        )

    async def export_rdf_preview(
        self,
        *,
        format: str = "turtle",
        max_triples: int = 100,
        as_of_valid_time: str | None = None,
        as_of_commit_seq: int | None = None,
        entailment: str | None = None,
        reason: bool | None = None,
    ) -> models.RdfExportPreviewResponse:
        return cast(
            models.RdfExportPreviewResponse,
            await super().export_rdf_preview(
                format=format,
                max_triples=max_triples,
                as_of_valid_time=as_of_valid_time,
                as_of_commit_seq=as_of_commit_seq,
                entailment=entailment,
                reason=reason,
            ),
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

    async def ontology_conformance_model(self) -> models.SchemaAuditReport:
        return cast(
            models.SchemaAuditReport, await super().ontology_conformance_model()
        )

    async def ontology_view_model(self, *, counts: bool = False) -> models.OntologyView:
        return cast(
            models.OntologyView, await super().ontology_view_model(counts=counts)
        )

    async def index_submit(
        self, body: Body | None = None, *, idempotency_key: str
    ) -> models.SearchIndexJobStatusResponse:
        return cast(
            models.SearchIndexJobStatusResponse,
            await super().index_submit(body, idempotency_key=idempotency_key),
        )

    async def index_job(self, job_id: str) -> models.SearchIndexJobStatusResponse:
        return cast(
            models.SearchIndexJobStatusResponse, await super().index_job(job_id)
        )

    async def cancel_index_job(
        self, job_id: str
    ) -> models.SearchIndexJobStatusResponse:
        return cast(
            models.SearchIndexJobStatusResponse,
            await super().cancel_index_job(job_id),
        )

    async def index_gc_submit(
        self, body: Body | None = None, *, idempotency_key: str
    ) -> models.IndexGcJobStatusResponse:
        return cast(
            models.IndexGcJobStatusResponse,
            await super().index_gc_submit(body, idempotency_key=idempotency_key),
        )

    async def index_gc_job(self, job_id: str) -> models.IndexGcJobStatusResponse:
        return cast(models.IndexGcJobStatusResponse, await super().index_gc_job(job_id))

    async def cancel_index_gc_job(self, job_id: str) -> models.IndexGcJobStatusResponse:
        return cast(
            models.IndexGcJobStatusResponse,
            await super().cancel_index_gc_job(job_id),
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
        deadline = asyncio.get_running_loop().time() + timeout
        last: RawLbbResponse | None = None
        while True:
            last = await self.raw_request("GET", "/v1/graph/metadata")
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
            if asyncio.get_running_loop().time() >= deadline:
                raise TimeoutError(
                    f"index lineage did not reach {target_seq} within {timeout}s "
                    f"(build={last.headers.get('lbb-build-commit')}, "
                    f"replica={last.headers.get('lbb-replica')}, lineage={lineage})"
                )
            await asyncio.sleep(poll_interval)

    async def summary_model(self) -> models.GraphSummaryResponse:
        return cast(models.GraphSummaryResponse, await super().summary_model())

    async def graph_edges_page(self, **kwargs: Any) -> ListPage[models.GraphEdgeRow]:
        return cast(
            ListPage[models.GraphEdgeRow], await super().graph_edges_page(**kwargs)
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
        content: str | None = None,
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
        content: str | None = None,
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

    async def _iter_entity_pages(
        self, **kwargs: Any
    ) -> AsyncIterator[ListPage[models.EntityExplorerRow]]:
        cursor = kwargs.pop("cursor", None)
        initial_offset = kwargs.pop("offset", None)
        seen: set[str] = set()
        while True:
            page = await self.entities.list_page(
                **kwargs,
                cursor=cursor,
                offset=initial_offset if cursor is None else None,
            )
            yield page
            if not page.has_more or page.next_cursor is None:
                return
            if page.next_cursor in seen:
                raise RuntimeError(
                    f"entity pagination cursor repeated: {page.next_cursor}"
                )
            seen.add(page.next_cursor)
            cursor = page.next_cursor

    async def _iter_entity_rows(
        self, **kwargs: Any
    ) -> AsyncIterator[models.EntityExplorerRow]:
        async for page in self._iter_entity_pages(**kwargs):
            for row in page:
                yield row

    async def sparql(
        self,
        query: str,
        *,
        reason: bool | None = None,
        entailment: str | None = None,
        as_of_valid_time: str | None = None,
        as_of_commit_seq: int | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> SparqlResults:
        """Async :meth:`LbbClient.sparql`: run SPARQL text, return parsed results."""
        envelope = await self._sparql_text_envelope(
            query,
            reason=reason,
            entailment=entailment,
            as_of_valid_time=as_of_valid_time,
            as_of_commit_seq=as_of_commit_seq,
            limit=limit,
            offset=offset,
        )
        return SparqlResults.from_envelope(envelope)

    async def aclose(self) -> None:
        await self._http.aclose()

    async def __aenter__(self) -> AsyncLbbClient:
        return self

    async def __aexit__(self, *_exc: object) -> None:
        await self.aclose()
