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
from typing import Any, Literal, cast

import httpx

from . import models
from ._client_base import (
    DEFAULT_BASE_URL,
    DEFAULT_INTEGRATIONS_URL,
    DEFAULT_MAX_RETRIES,
    DEFAULT_RETRY_BUDGET_MS,
    DEFAULT_TIMEOUT,
    Body,
    LbbCapabilityError,
    ListPage,
    ModelT,
    QueryAskResult,
    RawLbbResponse,
    RequestOptions,
    RetryEvent,
    RowT,
    SparqlResults,
    _BaseLbbClient,
    _body_marks_terminal,
    _ChecksNamespace,
    _EmbeddingsNamespace,
    _EntityNamespace,
    _error_body_field,
    _EvalsNamespace,
    _FactsNamespace,
    _GraphNamespace,
    _jittered_backoff,
    _OntologyNamespace,
    _OntologyStartersNamespace,
    _parse_model,
    _QueryNamespace,
    _raw_response,
    _retries_status,
    _retries_transport_error,
    _retry_allowed,
    _retry_delay_seconds,
    _retryable,
    _SchemaNamespace,
)
from .integrations import (
    _DEFAULT,
    IntegrationAcceptResult,
    IntegrationConnectionAnswer,
    IntegrationConnectorsAnswer,
    IntegrationCreateAnswer,
    IntegrationDeleteAnswer,
    IntegrationEraseAnswer,
    IntegrationListAnswer,
    IntegrationRefAnswer,
    IntegrationsNamespace,
    IntegrationStatusAnswer,
    IntegrationSyncAnswer,
    _accept_request,
    _Default,
    _queues_sync,
    _sync_after_request,
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


class _AsyncOntologyStartersNamespace(_OntologyStartersNamespace):
    async def list(self) -> models.OntologyStarterList:
        return cast(models.OntologyStarterList, await super().list())

    async def get(self, starter: str) -> models.OntologyStarterDetail:
        return cast(models.OntologyStarterDetail, await super().get(starter))

    async def apply(
        self,
        starter: str,
        *,
        dry_run: bool = False,
        expected_ontology_version: int | None = None,
    ) -> models.OntologyStarterApplyResponse:
        return cast(
            models.OntologyStarterApplyResponse,
            await super().apply(
                starter,
                dry_run=dry_run,
                expected_ontology_version=expected_ontology_version,
            ),
        )

    async def update(self, starter: str) -> models.OntologyStarterUpdateResponse:
        return cast(models.OntologyStarterUpdateResponse, await super().update(starter))


class _AsyncOntologyNamespace(_OntologyNamespace):
    starters: _AsyncOntologyStartersNamespace

    def __init__(self, client: _BaseLbbClient, graph: str | None = None) -> None:
        super().__init__(client, graph)
        self.starters = _AsyncOntologyStartersNamespace(client, graph)

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

    async def suggestions(
        self,
        *,
        status: str | None = None,
        origin_kind: str | None = None,
        origin_id: str | None = None,
        anchor: str | None = None,
        key: str | None = None,
        limit: int | None = None,
    ) -> models.OntologyChangeSuggestionList:
        return cast(
            models.OntologyChangeSuggestionList,
            await super().suggestions(
                status=status,
                origin_kind=origin_kind,
                origin_id=origin_id,
                anchor=anchor,
                key=key,
                limit=limit,
            ),
        )

    async def suggestion_get(
        self, suggestion_id: str
    ) -> models.OntologyChangeSuggestion:
        return cast(
            models.OntologyChangeSuggestion,
            await super().suggestion_get(suggestion_id),
        )

    async def suggestion_create(self, body: Body) -> models.OntologyChangeSuggestion:
        return cast(
            models.OntologyChangeSuggestion, await super().suggestion_create(body)
        )

    async def suggestion_validate(
        self, suggestion_id: str
    ) -> models.OntologyChangeSuggestion:
        return cast(
            models.OntologyChangeSuggestion,
            await super().suggestion_validate(suggestion_id),
        )

    async def suggestion_accept(
        self, suggestion_id: str, body: Body | None = None
    ) -> models.OntologyChangeSuggestion:
        return cast(
            models.OntologyChangeSuggestion,
            await super().suggestion_accept(suggestion_id, body),
        )

    async def suggestion_dismiss(
        self, suggestion_id: str, reason: str, *, author: str | None = None
    ) -> models.OntologyChangeSuggestion:
        return cast(
            models.OntologyChangeSuggestion,
            await super().suggestion_dismiss(suggestion_id, reason, author=author),
        )

    async def suggestion_supersede(
        self, suggestion_id: str, reason: str, *, author: str | None = None
    ) -> models.OntologyChangeSuggestion:
        return cast(
            models.OntologyChangeSuggestion,
            await super().suggestion_supersede(suggestion_id, reason, author=author),
        )

    async def suggestion_comment(
        self, suggestion_id: str, text: str, *, author: str | None = None
    ) -> models.OntologyChangeSuggestion:
        return cast(
            models.OntologyChangeSuggestion,
            await super().suggestion_comment(suggestion_id, text, author=author),
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
    ) -> SparqlResults:
        return cast(
            SparqlResults,
            await super().sparql(
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
            ),
        )

    async def rewrite(
        self,
        question: str,
        *,
        context: str | None = None,
        previous: Sequence[Mapping[str, Any] | models.QueryRewriteStep] | None = None,
        route: str | models.QueryRoute | None = None,
        mode: str | models.QueryRewriteMode | None = None,
        run: bool | None = None,
        limit: int | None = None,
        as_of_commit_seq: int | None = None,
        today: str | None = None,
        include_grounding: bool | None = None,
        consistency: str | None = None,
        options: RequestOptions | None = None,
    ) -> Any:
        return await super().rewrite(
            question,
            context=context,
            previous=previous,
            route=route,
            mode=mode,
            run=run,
            limit=limit,
            as_of_commit_seq=as_of_commit_seq,
            today=today,
            include_grounding=include_grounding,
            consistency=consistency,
            options=options,
        )

    async def ask(
        self,
        question: str,
        *,
        context: str | None = None,
        previous: Sequence[Mapping[str, Any] | models.QueryRewriteStep] | None = None,
        route: str | models.QueryRoute | None = None,
        limit: int | None = None,
        as_of_commit_seq: int | None = None,
        today: str | None = None,
        consistency: str | None = None,
        options: RequestOptions | None = None,
    ) -> QueryAskResult:
        """Async :meth:`LbbClient.query.ask`: rewrite the question, run the
        query, and return its rows."""
        response = await self.rewrite(
            question,
            context=context,
            previous=previous,
            route=route,
            run=True,
            limit=limit,
            as_of_commit_seq=as_of_commit_seq,
            today=today,
            consistency=consistency,
            options=options,
        )
        return QueryAskResult.from_response(response)

    async def update(
        self,
        update: str,
        *,
        idempotency_key: str | None = None,
        options: RequestOptions | None = None,
    ) -> None:
        await super().update(
            update, idempotency_key=idempotency_key, options=options
        )



class _AsyncFactsNamespace(_FactsNamespace):
    async def create_model(
        self, body: Body, *, idempotency_key: str | None = None
    ) -> models.GraphCommitResponse:
        return cast(
            models.GraphCommitResponse,
            await super().create_model(body, idempotency_key=idempotency_key),
        )

    async def import_rdf_many(
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
        return await _import_rdf_many(
            self,
            documents,
            format=format,
            base_iri=base_iri,
            graph_uri=graph_uri,
            blank_node_scope=blank_node_scope,
            batch=batch,
            strict=strict,
            observed_at=observed_at,
            resource_type=resource_type,
            edge_idempotency=edge_idempotency,
            idempotency_key=idempotency_key,
        )


async def _import_rdf_many(
    target: Any,
    documents: Sequence[str],
    *,
    format: str,
    base_iri: str | None,
    graph_uri: str | None,
    blank_node_scope: str | None,
    batch: int | None,
    strict: bool | None,
    observed_at: str | None,
    resource_type: str | None,
    edge_idempotency: str | None,
    idempotency_key: str | None,
) -> dict[str, Any]:
    if not documents:
        raise ValueError("import_rdf_many requires at least one document")
    imports = []
    for index, document in enumerate(documents):
        imports.append(
            await target.import_rdf(
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
    ontology: _AsyncOntologyNamespace

    def __init__(self, client: _BaseLbbClient, graph: str) -> None:
        super().__init__(client, graph)
        self.facts = _AsyncFactsNamespace(client, graph)
        self.ontology = _AsyncOntologyNamespace(client, graph)

    async def delete(self, *, confirm: str) -> models.GraphDeleteResponse:
        return cast(models.GraphDeleteResponse, await super().delete(confirm=confirm))

    async def publication_status(self) -> Any:
        return await super().publication_status()

    async def publication_status_model(self) -> models.PublicationStatusResponse:
        return cast(
            models.PublicationStatusResponse,
            await super().publication_status_model(),
        )

    async def activity(self) -> Any:
        return await super().activity()

    async def activity_model(self) -> models.GraphActivityResponse:
        return cast(models.GraphActivityResponse, await super().activity_model())

    async def wait_for_published(
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
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while True:
            status = await self.publication_status_model()
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
            now = loop.time()
            if now >= deadline:
                raise TimeoutError(
                    f"publication did not reach {target_seq} within {timeout}s "
                    f"(state={status.state.value}, head={status.head_seq}, "
                    f"target={status.target_seq}, published={status.published_seq}, "
                    f"stage={status.current_stage or 'unknown'})"
                )
            retry_after = status.retry.retry_after_ms / 1000
            await asyncio.sleep(min(max(poll_interval, retry_after), deadline - now))

    async def retract_model(
        self, body: Body, *, idempotency_key: str | None = None
    ) -> models.GraphRetractResponse:
        return cast(
            models.GraphRetractResponse,
            await super().retract_model(body, idempotency_key=idempotency_key),
        )


class _AsyncEntityNamespace(_EntityNamespace):
    async def detail_model(
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
        return cast(
            models.EntityDetailResponse,
            await super().detail_model(
                id=id,
                type=type,
                name=name,
                key=key,
                consistency=consistency,
                edges=edges,
                as_of_commit_seq=as_of_commit_seq,
            ),
        )

    async def filter_by_attributes_model(
        self, **kwargs: Any
    ) -> models.SparqlSelectResponse:
        return cast(
            models.SparqlSelectResponse,
            await super().filter_by_attributes_model(**kwargs),
        )


class _AsyncIntegrationsNamespace(IntegrationsNamespace):
    """Async :class:`lbb.integrations.IntegrationsNamespace`: the same methods,
    as coroutines."""

    async def connectors(self) -> IntegrationConnectorsAnswer:
        return cast(IntegrationConnectorsAnswer, await super().connectors())

    async def create(
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
        return cast(
            IntegrationCreateAnswer,
            await super().create(
                graph=graph,
                id=id,
                kind=kind,
                credentials=credentials,
                config=config,
                every_ms=every_ms,
                ontology_mode=ontology_mode,
                starter=starter,
                start=start,
            ),
        )

    async def list(self, *, graph: str) -> IntegrationListAnswer:
        return cast(IntegrationListAnswer, await super().list(graph=graph))

    async def get(self, id: str, *, graph: str) -> IntegrationConnectionAnswer:
        return cast(IntegrationConnectionAnswer, await super().get(id, graph=graph))

    async def set_credentials(
        self, id: str, *, graph: str, credentials: Mapping[str, str]
    ) -> IntegrationRefAnswer:
        return cast(
            IntegrationRefAnswer,
            await super().set_credentials(id, graph=graph, credentials=credentials),
        )

    async def set_settings(
        self, id: str, *, graph: str, config: Mapping[str, Any]
    ) -> IntegrationRefAnswer:
        return cast(
            IntegrationRefAnswer,
            await super().set_settings(id, graph=graph, config=config),
        )

    async def sync(
        self,
        id: str,
        *,
        graph: str,
        full: bool | None = None,
        idempotency_key: str | None = None,
    ) -> IntegrationSyncAnswer:
        return cast(
            IntegrationSyncAnswer,
            await super().sync(
                id, graph=graph, full=full, idempotency_key=idempotency_key
            ),
        )

    async def pause(self, id: str, *, graph: str) -> IntegrationStatusAnswer:
        return cast(IntegrationStatusAnswer, await super().pause(id, graph=graph))

    async def resume(self, id: str, *, graph: str) -> IntegrationStatusAnswer:
        return cast(IntegrationStatusAnswer, await super().resume(id, graph=graph))

    async def delete(self, id: str, *, graph: str) -> IntegrationDeleteAnswer:
        return cast(IntegrationDeleteAnswer, await super().delete(id, graph=graph))

    async def erase(self, graph: str, *, confirm: str) -> IntegrationEraseAnswer:
        return cast(IntegrationEraseAnswer, await super().erase(graph, confirm=confirm))

    async def suggestions(
        self,
        connection_id: str,
        *,
        graph: str,
        status: str | None = None,
        limit: int | None = None,
    ) -> models.OntologyChangeSuggestionList:
        return cast(
            models.OntologyChangeSuggestionList,
            await super().suggestions(
                connection_id, graph=graph, status=status, limit=limit
            ),
        )

    async def accept(
        self,
        suggestion_id: str,
        *,
        graph: str,
        change: Sequence[Body] | None = None,
        comment: str | None = None,
        author: str | None = None,
        sync: bool = False,
    ) -> IntegrationAcceptResult:
        suggestion = await self._client._model_request(
            models.OntologyChangeSuggestion,
            **_accept_request(suggestion_id, graph, change, comment, author),
        )
        connection = _queues_sync(suggestion) if sync else None
        if connection is None:
            return IntegrationAcceptResult(suggestion=suggestion, sync=None)
        turn = await self._client._model_request(
            models.WorkflowTurn, **_sync_after_request(suggestion, connection, graph)
        )
        return IntegrationAcceptResult(suggestion=suggestion, sync=turn)

    async def dismiss(
        self,
        suggestion_id: str,
        *,
        graph: str,
        reason: str,
        author: str | None = None,
    ) -> models.OntologyChangeSuggestion:
        return cast(
            models.OntologyChangeSuggestion,
            await super().dismiss(
                suggestion_id, graph=graph, reason=reason, author=author
            ),
        )


class AsyncLbbClient(_BaseLbbClient):
    """Asynchronous client. Usable as an async context manager."""

    entities: _AsyncEntityNamespace
    ontology: _AsyncOntologyNamespace
    integrations: _AsyncIntegrationsNamespace
    query: _AsyncQueryNamespace
    schema: _AsyncSchemaNamespace

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
        timeout: float = DEFAULT_TIMEOUT,
        transport: httpx.AsyncBaseTransport | None = None,
        event_hooks: Mapping[str, list[Callable[[Any], Any]]] | None = None,
        default_consistency: str | None = None,
        integrations_url: str = DEFAULT_INTEGRATIONS_URL,
    ) -> None:
        super().__init__(
            base_url,
            api_key=api_key,
            graph=graph,
            api_version=api_version,
            max_retries=max_retries,
            retry_delay=retry_delay,
            retry_budget_ms=retry_budget_ms,
            on_retry=on_retry,
            default_consistency=default_consistency,
            integrations_url=integrations_url,
        )
        self.entities = _AsyncEntityNamespace(self)
        self.ontology = _AsyncOntologyNamespace(self)
        self.query = _AsyncQueryNamespace(self)
        self.schema = _AsyncSchemaNamespace(self)
        self.evals = _EvalsNamespace(self)
        # Model checks: the call log, the judge's checks, and reviews. Each
        # method returns an awaitable here.
        self.checks = _ChecksNamespace(self)
        self.embeddings = _EmbeddingsNamespace(self)
        # Hosted integrations for your end customers, at ``integrations_url``.
        self.integrations = _AsyncIntegrationsNamespace(self)
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

    def graph(self, name: str) -> _AsyncGraphNamespace:
        return _AsyncGraphNamespace(self, name)

    async def create_graph(self) -> models.CreateGraphResponse:
        return cast(models.CreateGraphResponse, await super().create_graph())

    async def delete_graph(self, *, confirm: str) -> models.GraphDeleteResponse:
        return cast(
            models.GraphDeleteResponse, await super().delete_graph(confirm=confirm)
        )

    async def workflow_delete_instance(
        self, workflow_id: str
    ) -> models.WorkflowInstanceDeleteResponse:
        return cast(
            models.WorkflowInstanceDeleteResponse,
            await super().workflow_delete_instance(workflow_id),
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

    async def publication_status_model(self) -> models.PublicationStatusResponse:
        return cast(
            models.PublicationStatusResponse,
            await super().publication_status_model(),
        )

    async def activity_model(self) -> models.GraphActivityResponse:
        return cast(models.GraphActivityResponse, await super().activity_model())

    async def wait_for_published(
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
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while True:
            status = await self._model_request(
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
            now = loop.time()
            if now >= deadline:
                raise TimeoutError(
                    f"publication did not reach {target_seq} within {timeout}s "
                    f"(state={status.state.value}, head={status.head_seq}, "
                    f"target={status.target_seq}, published={status.published_seq}, "
                    f"stage={status.current_stage or 'unknown'})"
                )
            retry_after = status.retry.retry_after_ms / 1000
            await asyncio.sleep(min(max(poll_interval, retry_after), deadline - now))

    async def import_rdf_many(
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
        return await _import_rdf_many(
            self,
            documents,
            format=format,
            base_iri=base_iri,
            graph_uri=graph_uri,
            blank_node_scope=blank_node_scope,
            batch=batch,
            strict=strict,
            observed_at=observed_at,
            resource_type=resource_type,
            edge_idempotency=edge_idempotency,
            idempotency_key=idempotency_key,
        )

    async def summary_model(self) -> models.GraphSummaryResponse:
        return cast(models.GraphSummaryResponse, await super().summary_model())

    async def read_snapshot_model(self) -> models.PublishedReadStatusResponse:
        return cast(
            models.PublishedReadStatusResponse,
            await super().read_snapshot_model(),
        )

    async def schema_summary_model(self) -> models.RdfSchemaSummaryResponse:
        return cast(
            models.RdfSchemaSummaryResponse,
            await super().schema_summary_model(),
        )

    async def planner_stats_model(
        self, *, cursor: str | None = None, limit: int | None = None
    ) -> models.PlannerStatsResponse:
        return cast(
            models.PlannerStatsResponse,
            await super().planner_stats_model(cursor=cursor, limit=limit),
        )

    async def list_graphs_model(self) -> models.GraphListResponse:
        return cast(models.GraphListResponse, await super().list_graphs_model())

    async def model_activity(self, month: str | None = None) -> Any:
        return await super().model_activity(month)

    async def model_activity_model(
        self, month: str | None = None
    ) -> models.ModelActivityResponse:
        return cast(
            models.ModelActivityResponse, await super().model_activity_model(month)
        )

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
        return await self._send(
            method,
            path,
            f"{self._base_url}{path}",
            params=params,
            body=body,
            content=content,
            content_type=content_type,
            idempotency_key=idempotency_key,
            options=options,
        )

    async def _integrations_call(
        self,
        method: str,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        body: Body | None = None,
        idempotency_key: str | None = None,
        options: RequestOptions | None = None,
    ) -> Any:
        response = await self._send(
            method,
            path,
            f"{self._integrations_url}{path}",
            params=params,
            body=body,
            idempotency_key=idempotency_key,
            options=options,
            scoped=False,
        )
        return response.data

    async def _send(
        self,
        method: str,
        path: str,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
        body: Body | None = None,
        content: Any | None = None,
        content_type: str | None = None,
        idempotency_key: str | None = None,
        options: RequestOptions | None = None,
        scoped: bool = True,
    ) -> RawLbbResponse:
        request_options = options or {}
        kwargs = self._request_kwargs(
            params=params,
            body=body,
            content=content,
            content_type=content_type,
            idempotency_key=idempotency_key,
            headers=request_options.get("headers"),
            scoped=scoped,
        )
        if "timeout" in request_options:
            kwargs["timeout"] = request_options["timeout"]
        response: httpx.Response | None = None
        retry = request_options.get("retry", _retry_allowed(method, idempotency_key))
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
                response = await self._http.request(method, url, **kwargs)
            except httpx.RequestError:
                if not (_retries_transport_error(retry) and attempt < max_retries):
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
            if (
                not _retries_status(retry, response.status_code)
                or attempt >= max_retries
            ):
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
    ) -> SparqlResults:
        """Async :meth:`LbbClient.sparql`: run SPARQL text, return parsed results."""
        envelope = await self._sparql_text_envelope(
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
        return SparqlResults.from_envelope(envelope)

    async def aclose(self) -> None:
        await self._http.aclose()

    async def __aenter__(self) -> AsyncLbbClient:
        return self

    async def __aexit__(self, *_exc: object) -> None:
        await self.aclose()
