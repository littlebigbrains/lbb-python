"""Asynchronous transport for the little big brain Python SDK."""

from __future__ import annotations

import asyncio
import inspect
import json
import os
from collections.abc import (
    AsyncGenerator,
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
from ._catalog import AccessNamespace, CatalogNamespace
from ._client_base import (
    _ASK_STREAM_ENDED,
    _UPLOAD_CHUNK_BYTES,
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
    QueryAskStreamEvent,
    RawLbbResponse,
    RequestOptions,
    RetryEvent,
    RowT,
    SparqlResults,
    TrialCandidate,
    _ask_body,
    _ask_stream_event,
    _BaseLbbClient,
    _body_marks_terminal,
    _ChecksNamespace,
    _discard_download,
    _EmbeddingsNamespace,
    _EntityNamespace,
    _error_body_field,
    _EvalsNamespace,
    _FactsNamespace,
    _file_head,
    _FilesNamespace,
    _GraphNamespace,
    _jittered_backoff,
    _ModelSwitchesNamespace,
    _ModelTrialsNamespace,
    _OntologyNamespace,
    _OntologyStartersNamespace,
    _open_download,
    _parse_model,
    _QueryNamespace,
    _raw_response,
    _retries_status,
    _retries_transport_error,
    _retry_allowed,
    _retry_delay_seconds,
    _retryable,
    _SchemaNamespace,
    _sse_decoder,
    _SseParser,
    _StarterWorkflowMethods,
    _trial_settled,
    _trial_timeout,
)
from ._workflow_execution import AsyncWorkflowTurnReader
from .integrations import (
    _DEFAULT,
    IntegrationAcceptResult,
    IntegrationCdcAction,
    IntegrationCdcAlertsAnswer,
    IntegrationCdcControlAnswer,
    IntegrationCdcDiscoveryAnswer,
    IntegrationCdcDiscoveryCreateAnswer,
    IntegrationCdcOverview,
    IntegrationCdcReviewAnswer,
    IntegrationCdcStatusAnswer,
    IntegrationConnectionAnswer,
    IntegrationConnectorsAnswer,
    IntegrationConnectSessionAnswer,
    IntegrationConnectSessionCreateAnswer,
    IntegrationCreateAnswer,
    IntegrationDeleteAnswer,
    IntegrationEraseAnswer,
    IntegrationListAnswer,
    IntegrationProviderAppsAnswer,
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

    async def suggestion_patterns(
        self,
        *,
        status: str | None = None,
        origin_id: str | None = None,
        kind: str | None = None,
    ) -> models.OntologySuggestionPatternList:
        return cast(
            models.OntologySuggestionPatternList,
            await super().suggestion_patterns(
                status=status, origin_id=origin_id, kind=kind
            ),
        )

    async def suggestions_accept_batch(
        self, body: Body
    ) -> models.OntologySuggestionBatchResponse:
        return cast(
            models.OntologySuggestionBatchResponse,
            await super().suggestions_accept_batch(body),
        )

    async def suggestions_dismiss_batch(
        self, body: Body
    ) -> models.OntologySuggestionBatchResponse:
        return cast(
            models.OntologySuggestionBatchResponse,
            await super().suggestions_dismiss_batch(body),
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

    async def ask(
        self,
        question: str,
        *,
        context: str | None = None,
        route: str | models.QueryRoute | None = None,
        mode: str | models.QueryRewriteMode | None = None,
        limit: int | None = None,
        as_of_commit_seq: int | None = None,
        today: str | None = None,
        include_grounding: bool | None = None,
        anchor: Sequence[str] | None = None,
        timeline: Sequence[Mapping[str, Any] | models.QueryRewriteTimelinePoint] | None = None,
        consistency: str | None = None,
        options: RequestOptions | None = None,
    ) -> QueryAskResult:
        """Async :meth:`LbbClient.query.ask`: answer a question about the
        graph in plain words (``POST /v1/query/ask``). ``mode="route"``
        returns only the kind of question. A failed call is not retried
        unless ``options={"retry": True}``."""
        response = await self._ask_response(
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
            consistency=consistency,
            options=options,
        )
        return QueryAskResult.from_response(response)

    def ask_stream(
        self,
        question: str,
        *,
        context: str | None = None,
        route: str | models.QueryRoute | None = None,
        mode: str | models.QueryRewriteMode | None = None,
        limit: int | None = None,
        as_of_commit_seq: int | None = None,
        today: str | None = None,
        include_grounding: bool | None = None,
        anchor: Sequence[str] | None = None,
        timeline: Sequence[Mapping[str, Any] | models.QueryRewriteTimelinePoint] | None = None,
        consistency: str | None = None,
        options: RequestOptions | None = None,
    ) -> AsyncGenerator[QueryAskStreamEvent, None]:
        """Async :meth:`LbbClient.query.ask_stream`: an async generator of
        the events. ``await events.aclose()`` (or ``contextlib.aclosing``)
        closes the response at once."""
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
        client = cast("AsyncLbbClient", self._client)
        return client._ask_events(body, params or None, options)

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


class _AsyncStarterWorkflowMethods(_StarterWorkflowMethods):
    """The starter workflow, triggered workflow and workflow activity
    methods, awaited."""

    async def reason_activity(
        self,
        prompt: str,
        schema: Mapping[str, Any],
        *,
        system: str | None = None,
        max_tokens: int | None = None,
        effect_id: str | None = None,
        label: str | None = None,
    ) -> Any:
        return await super().reason_activity(
            prompt,
            schema,
            system=system,
            max_tokens=max_tokens,
            effect_id=effect_id,
            label=label,
        )

    async def decide_activity(
        self,
        state: Any,
        questions: Mapping[str, Mapping[str, Any]],
        *,
        effect_id: str | None = None,
        label: str | None = None,
    ) -> Any:
        return await super().decide_activity(
            state, questions, effect_id=effect_id, label=label
        )

    async def embed_activity(
        self,
        texts: Sequence[str],
        *,
        effect_id: str | None = None,
        label: str | None = None,
    ) -> Any:
        return await super().embed_activity(texts, effect_id=effect_id, label=label)

    async def check_derivation(
        self,
        sha256: str,
        workflow_type: str,
        version: str,
        settings_digest: str = "none",
    ) -> Any:
        return await super().check_derivation(
            sha256, workflow_type, version, settings_digest
        )

    async def record_derivation(
        self,
        sha256: str,
        workflow_type: str,
        version: str,
        settings_digest: str = "none",
        *,
        run_id: str | None = None,
        outputs: Any = None,
        content_uploaded_at: str | None = None,
    ) -> Any:
        return await super().record_derivation(
            sha256,
            workflow_type,
            version,
            settings_digest,
            run_id=run_id,
            outputs=outputs,
            content_uploaded_at=content_uploaded_at,
        )

    async def workflow_starters(self) -> Any:
        return await super().workflow_starters()

    async def try_workflow_starter(
        self, starter: str, *, params: Mapping[str, Any] | None = None
    ) -> Any:
        return await super().try_workflow_starter(starter, params=params)

    async def use_workflow_starter(
        self,
        starter: str,
        *,
        name: str | None = None,
        class_: str | None = None,
        fields: Sequence[str | Mapping[str, Any]] | None = None,
        exclude: Sequence[str] | None = None,
        params: Mapping[str, Any] | None = None,
        gate: float | None = None,
        budget_usd_per_month: float | None = None,
        workflow: Mapping[str, Any] | None = None,
        owns: Mapping[str, Any] | None = None,
        batch: int | None = None,
    ) -> Any:
        return await super().use_workflow_starter(
            starter,
            name=name,
            class_=class_,
            fields=fields,
            exclude=exclude,
            params=params,
            gate=gate,
            budget_usd_per_month=budget_usd_per_month,
            workflow=workflow,
            owns=owns,
            batch=batch,
        )

    async def triggered_workflows(self) -> Any:
        return await super().triggered_workflows()

    async def triggered_workflow(self, starter: str, name: str) -> Any:
        return await super().triggered_workflow(starter, name)

    async def preview_triggered_workflow(
        self,
        starter: str,
        *,
        name: str | None = None,
        class_: str | None = None,
        fields: Sequence[str | Mapping[str, Any]] | None = None,
        exclude: Sequence[str] | None = None,
        params: Mapping[str, Any] | None = None,
        gate: float | None = None,
        budget_usd_per_month: float | None = None,
        workflow: Mapping[str, Any] | None = None,
        owns: Mapping[str, Any] | None = None,
        batch: int | None = None,
        sample: int | None = None,
        iris: Sequence[str] | None = None,
        propose: bool = False,
    ) -> Any:
        return await super().preview_triggered_workflow(
            starter,
            name=name,
            class_=class_,
            fields=fields,
            exclude=exclude,
            params=params,
            gate=gate,
            budget_usd_per_month=budget_usd_per_month,
            workflow=workflow,
            owns=owns,
            batch=batch,
            sample=sample,
            iris=iris,
            propose=propose,
        )

    async def refresh_triggered_workflow(self, starter: str, name: str) -> Any:
        return await super().refresh_triggered_workflow(starter, name)

    async def pause_triggered_workflow(
        self, starter: str, name: str, paused: bool = True
    ) -> Any:
        return await super().pause_triggered_workflow(starter, name, paused)

    async def triggered_workflow_runs(
        self,
        starter: str,
        name: str,
        *,
        before: str | None = None,
        limit: int | None = None,
        outcome: str | models.WorkflowRunOutcome | None = None,
    ) -> Any:
        return await super().triggered_workflow_runs(
            starter, name, before=before, limit=limit, outcome=outcome
        )

    async def triggered_workflow_run(self, starter: str, name: str, run: str) -> Any:
        return await super().triggered_workflow_run(starter, name, run)

    async def triggered_workflow_run_stats(
        self, starter: str, name: str, *, hours: int | None = None
    ) -> Any:
        return await super().triggered_workflow_run_stats(starter, name, hours=hours)

    async def named_workflow_runs(
        self,
        name: str,
        *,
        before: str | None = None,
        limit: int | None = None,
        outcome: str | models.WorkflowRunOutcome | None = None,
    ) -> Any:
        return await super().named_workflow_runs(
            name, before=before, limit=limit, outcome=outcome
        )

    async def named_workflow_run(self, name: str, run: str) -> Any:
        return await super().named_workflow_run(name, run)

    async def named_workflow_run_stats(self, name: str, *, hours: int | None = None) -> Any:
        return await super().named_workflow_run_stats(name, hours=hours)

    async def triggered_workflow_documents(
        self,
        starter: str,
        name: str,
        *,
        q: str | None = None,
        sort: str | models.TriggeredDocumentSort | None = None,
        after: str | None = None,
        limit: int | None = None,
    ) -> Any:
        return await super().triggered_workflow_documents(
            starter, name, q=q, sort=sort, after=after, limit=limit
        )

    async def delete_triggered_workflow(self, starter: str, name: str) -> Any:
        return await super().delete_triggered_workflow(starter, name)


class _AsyncGraphNamespace(_AsyncStarterWorkflowMethods, _GraphNamespace):
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
        self, body: Body, *, idempotency_key: str | None = None,
        expected_graph_epoch: str | None = None,
    ) -> models.GraphRetractResponse:
        return cast(
            models.GraphRetractResponse,
            await super().retract_model(
                body, idempotency_key=idempotency_key,
                expected_graph_epoch=expected_graph_epoch,
            ),
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
        evidence: int | None = None,
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
                evidence=evidence,
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

    async def provider_apps(self) -> IntegrationProviderAppsAnswer:
        return cast(IntegrationProviderAppsAnswer, await super().provider_apps())

    async def create_connect_session(
        self, *, app_id: str, graph: str, return_url: str,
        connection_id: str | None = None,
    ) -> IntegrationConnectSessionCreateAnswer:
        return cast(
            IntegrationConnectSessionCreateAnswer,
            await super().create_connect_session(
                app_id=app_id, graph=graph, return_url=return_url,
                connection_id=connection_id,
            ),
        )

    async def get_connect_session(self, id: str) -> IntegrationConnectSessionAnswer:
        return cast(IntegrationConnectSessionAnswer, await super().get_connect_session(id))

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

    async def cdc_status(self, id: str, *, graph: str) -> IntegrationCdcStatusAnswer:
        return cast(IntegrationCdcStatusAnswer, await super().cdc_status(id, graph=graph))

    async def cdc_overview(self, *, graph: str) -> IntegrationCdcOverview:
        return cast(IntegrationCdcOverview, await super().cdc_overview(graph=graph))

    async def cdc_discovery(self, id: str, *, graph: str) -> IntegrationCdcDiscoveryAnswer:
        return cast(IntegrationCdcDiscoveryAnswer, await super().cdc_discovery(id, graph=graph))

    async def cdc_discover(
        self, id: str, *, graph: str, job_id: str, expected_revision: int,
        source: Body, credentials: Mapping[str, str],
    ) -> IntegrationCdcDiscoveryCreateAnswer:
        return cast(IntegrationCdcDiscoveryCreateAnswer, await super().cdc_discover(
            id, graph=graph, job_id=job_id, expected_revision=expected_revision, source=source, credentials=credentials,
        ))

    async def cdc_review_discovery(
        self, id: str, *, graph: str, job_id: str, graph_epoch: int, expected_revision: int,
        catalog_digest: str, mapping: Body, max_capture_bytes: int,
    ) -> IntegrationCdcReviewAnswer:
        return cast(IntegrationCdcReviewAnswer, await super().cdc_review_discovery(
            id, graph=graph, job_id=job_id, graph_epoch=graph_epoch, expected_revision=expected_revision,
            catalog_digest=catalog_digest, mapping=mapping, max_capture_bytes=max_capture_bytes,
        ))

    async def cdc_approve_discovery(
        self, id: str, *, graph: str, job_id: str, graph_epoch: int, expected_revision: int,
        catalog_digest: str, mapping: Body, max_capture_bytes: int,
    ) -> IntegrationCdcDiscoveryAnswer:
        return cast(IntegrationCdcDiscoveryAnswer, await super().cdc_approve_discovery(
            id, graph=graph, job_id=job_id, graph_epoch=graph_epoch, expected_revision=expected_revision,
            catalog_digest=catalog_digest, mapping=mapping, max_capture_bytes=max_capture_bytes,
        ))

    async def cdc_cancel_discovery(
        self, id: str, *, graph: str, job_id: str, graph_epoch: int, expected_revision: int,
    ) -> IntegrationCdcDiscoveryAnswer:
        return cast(IntegrationCdcDiscoveryAnswer, await super().cdc_cancel_discovery(
            id, graph=graph, job_id=job_id, graph_epoch=graph_epoch, expected_revision=expected_revision,
        ))

    async def cdc_mute_alerts(self, id: str, *, graph: str, expected_revision: int, muted: bool) -> IntegrationCdcAlertsAnswer:
        return cast(IntegrationCdcAlertsAnswer, await super().cdc_mute_alerts(
            id, graph=graph, expected_revision=expected_revision, muted=muted,
        ))

    async def cdc_control(
        self, id: str, *, graph: str, operation_id: str,
        action: IntegrationCdcAction, confirm: str | None = None,
    ) -> IntegrationCdcControlAnswer:
        return cast(
            IntegrationCdcControlAnswer,
            await super().cdc_control(
                id, graph=graph, operation_id=operation_id, action=action, confirm=confirm,
            ),
        )

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


class _AsyncModelTrialsNamespace(_ModelTrialsNamespace):
    __doc__ = _ModelTrialsNamespace.__doc__

    async def options(self) -> models.ModelTrialOptionsResponse:
        return cast(models.ModelTrialOptionsResponse, await super().options())

    async def create(
        self,
        *,
        candidate: TrialCandidate,
        jobs: Sequence[str | models.ModelJob] | None = None,
        target: int | None = None,
        days: int | None = None,
    ) -> models.ModelTrialStartResponse:
        return cast(
            models.ModelTrialStartResponse,
            await super().create(candidate=candidate, jobs=jobs, target=target, days=days),
        )

    async def list(
        self, *, job: str | models.ModelJob | None = None, limit: int | None = None
    ) -> models.ModelTrialListResponse:
        return cast(models.ModelTrialListResponse, await super().list(job=job, limit=limit))

    async def get(self, trial_id: str) -> models.ModelTrial:
        return cast(models.ModelTrial, await super().get(trial_id))

    async def call(self, trial_id: str, call_id: str) -> models.ModelTrialCallResponse:
        return cast(models.ModelTrialCallResponse, await super().call(trial_id, call_id))

    async def stop(self, trial_id: str) -> models.ModelTrial:
        return cast(models.ModelTrial, await super().stop(trial_id))

    async def wait(
        self,
        trial_id: str,
        *,
        until: str = "compared",
        timeout: float | None = None,
        poll_interval: float = 5.0,
        on_update: Callable[[models.ModelTrial], None] | None = None,
    ) -> models.ModelTrial:
        """Read the trial until it compared what it can (``until="compared"``,
        the default: it no longer runs) or until it ended (``until="ended"``:
        ``done``, ``stopped`` or ``failed``), and return it.

        ``on_update`` gets the trial after each read, e.g. to show
        ``report.calls``. ``timeout`` (seconds) raises :class:`TimeoutError`.
        """
        if poll_interval < 0:
            raise ValueError("poll_interval must be non-negative")
        if timeout is not None and timeout < 0:
            raise ValueError("timeout must be non-negative")
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout if timeout is not None else None
        while True:
            trial = await self.get(trial_id)
            if on_update is not None:
                on_update(trial)
            if _trial_settled(trial, until):
                return trial
            if deadline is not None and loop.time() >= deadline:
                raise _trial_timeout(trial_id, trial)
            await asyncio.sleep(poll_interval)


class _AsyncModelSwitchesNamespace(_ModelSwitchesNamespace):
    __doc__ = _ModelSwitchesNamespace.__doc__

    async def list(self) -> models.ModelSwitchListResponse:
        return cast(models.ModelSwitchListResponse, await super().list())

    async def create(self, *, trial: str) -> models.ModelSwitch:
        return cast(models.ModelSwitch, await super().create(trial=trial))

    async def revert(self, job: str | models.ModelJob) -> models.ModelSwitchListResponse:
        return cast(models.ModelSwitchListResponse, await super().revert(job))


class _AsyncModelsNamespace:
    """The models of the graph's uses: trials of other models
    (:attr:`trials`), and switches (:attr:`switches`)."""

    def __init__(self, client: _BaseLbbClient) -> None:
        self.trials = _AsyncModelTrialsNamespace(client)
        self.switches = _AsyncModelSwitchesNamespace(client)


class _AsyncFileChunks:
    """A file's bytes in chunks for the async client, read off the event loop
    and again from the start each time they are iterated (a retry sends the
    file again)."""

    def __init__(self, path: str) -> None:
        self._path = path

    def __aiter__(self) -> AsyncIterator[bytes]:
        return self._chunks()

    async def _chunks(self) -> AsyncIterator[bytes]:
        handle = await asyncio.to_thread(open, self._path, "rb")
        try:
            while chunk := await asyncio.to_thread(handle.read, _UPLOAD_CHUNK_BYTES):
                yield chunk
        finally:
            handle.close()


class AsyncLbbClient(_AsyncStarterWorkflowMethods, _BaseLbbClient):
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
        environment: str | None = None,
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
            environment=environment,
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
        # Model trials of other models on the graph's uses, and switches.
        self.models = _AsyncModelsNamespace(self)
        self.embeddings = _EmbeddingsNamespace(self)
        # Files of the graph. Each method returns an awaitable here.
        self.files = _FilesNamespace(self)
        self.catalog = CatalogNamespace(self)
        self.access = AccessNamespace(self)
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

    def workflow_turn_reader(self, task: Body) -> AsyncWorkflowTurnReader:
        """Bind SELECT/ASK to a task claimed with include_execution_credential.

        Requires its issued execution credential; never falls back to this
        client's API key for reads. Keep heartbeats/checkpoints on this client.
        """
        return AsyncWorkflowTurnReader(self, task)

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
        binary: bool = False,
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
            binary=binary,
        )

    async def _event_stream(
        self,
        method: str,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        body: Body | None = None,
        options: RequestOptions | None = None,
        json_event: str | None = None,
        on_open: Callable[[httpx.Response], None] | None = None,
    ) -> AsyncGenerator[tuple[str, str], None]:
        """Async :meth:`LbbClient._event_stream`: one request, never
        retried, its server-sent events as ``(event, data)``."""
        request_options = options or {}
        kwargs = self._request_kwargs(
            params=params,
            body=body,
            content=None,
            content_type=None,
            idempotency_key=None,
            headers={"accept": "text/event-stream", **request_options.get("headers", {})},
        )
        if "timeout" in request_options:
            kwargs["timeout"] = request_options["timeout"]
        async with self._http.stream(method, f"{self._base_url}{path}", **kwargs) as response:
            if on_open is not None:
                on_open(response)
            content_type = response.headers.get("content-type", "").lower()
            if response.status_code // 100 != 2 or "text/event-stream" not in content_type:
                await response.aread()
                _raw_response(response)
                if json_event is not None and response.text.strip():
                    yield json_event, response.text
                return
            parser = _SseParser()
            decoder = _sse_decoder()
            async for chunk in response.aiter_bytes():
                for item in parser.feed(decoder.decode(chunk)):
                    yield item
            for item in parser.feed(decoder.decode(b"", final=True)):
                yield item

    async def _ask_events(
        self,
        body: dict[str, Any],
        params: Mapping[str, Any] | None,
        options: RequestOptions | None,
    ) -> AsyncGenerator[QueryAskStreamEvent, None]:
        """The events of ``POST /v1/query/ask`` as an async stream."""
        opened: list[httpx.Response] = []
        stream = self._event_stream(
            "POST",
            "/v1/query/ask",
            params=params,
            body=body,
            options=options,
            # A server without streams answers with the response itself.
            json_event="done",
            on_open=opened.append,
        )
        try:
            async for name, text in stream:
                request_id = opened[0].headers.get("x-request-id") if opened else None
                event = _ask_stream_event(name, text, request_id)
                if event is None:
                    continue
                yield event
                if event.event == "done":
                    return
        finally:
            await stream.aclose()
        raise httpx.RemoteProtocolError(
            _ASK_STREAM_ENDED, request=opened[0].request if opened else None
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

    async def _bytes_request(
        self,
        method: str,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        options: RequestOptions | None = None,
    ) -> bytes:
        response = await self._send(
            method,
            path,
            f"{self._base_url}{path}",
            params=params,
            options=options,
            binary=True,
        )
        return cast(bytes, response.data)

    def _path_content(self, path: str) -> Any:
        return _AsyncFileChunks(path)

    async def _download_request(
        self,
        path: str,
        dest: str | os.PathLike[str],
        *,
        params: Mapping[str, Any] | None = None,
        options: RequestOptions | None = None,
    ) -> int:
        request_options = options or {}
        kwargs = self._request_kwargs(
            params=params,
            body=None,
            content=None,
            content_type=None,
            idempotency_key=None,
            headers=request_options.get("headers"),
        )
        if "timeout" in request_options:
            kwargs["timeout"] = request_options["timeout"]
        max_retries = request_options.get("max_retries", self._max_retries)
        if max_retries < 0:
            raise ValueError("max_retries must be non-negative")
        retry_budget_ms = request_options.get("retry_budget_ms", self._retry_budget_ms)
        loop = asyncio.get_running_loop()
        started_at = loop.time()
        deadline = started_at + max(0.0, retry_budget_ms) / 1000.0
        url = f"{self._base_url}{path}"
        attempt = 0
        while True:
            delay: float | None = None
            answered = False
            try:
                async with self._http.stream("GET", url, **kwargs) as response:
                    answered = True
                    if response.status_code // 100 == 2:
                        handle, temp = _open_download(dest)
                        written = 0
                        try:
                            with handle:
                                async for chunk in response.aiter_bytes():
                                    handle.write(chunk)
                                    written += len(chunk)
                            os.replace(temp, os.fspath(dest))
                        except BaseException:
                            _discard_download(temp)
                            raise
                        return written
                    await response.aread()
                    if (
                        _retryable(response.status_code)
                        and attempt < max_retries
                        and not _body_marks_terminal(response)
                    ):
                        delay = _retry_delay_seconds(response, self._retry_delay, attempt)
                        if loop.time() + delay > deadline:
                            delay = None
                    if delay is None:
                        _raw_response(response)
                        raise AssertionError("a non-2xx answer raises")
                    self._emit_retry(
                        "GET",
                        path,
                        attempt=attempt + 1,
                        status_code=response.status_code,
                        error_code=_error_body_field(response, "code"),
                        delay_seconds=delay,
                        elapsed_ms=(loop.time() - started_at) * 1000,
                    )
            except httpx.TransportError:
                # Only a failure before the answer starts is retried. A
                # transfer that broke raises, and leaves nothing at ``dest``.
                if answered or attempt >= max_retries:
                    raise
                delay = _jittered_backoff(self._retry_delay, attempt)
                if loop.time() + delay > deadline:
                    raise
            assert delay is not None
            await asyncio.sleep(delay)
            attempt += 1

    async def _file_head_request(
        self,
        path: str,
        sha256: str,
        *,
        params: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        response = await self._send(
            "HEAD",
            path,
            f"{self._base_url}{path}",
            params=params,
            binary=True,
        )
        return _file_head(response.headers, sha256)

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
