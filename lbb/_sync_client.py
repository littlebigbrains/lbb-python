"""Synchronous transport for the little big brain Python SDK."""

from __future__ import annotations

import json
import os
import time
from collections.abc import Callable, Generator, Iterable, Iterator, Mapping, Sequence
from itertools import chain
from typing import Any, cast

import httpx

from . import models
from . import models as _models  # for a function whose `models` names the models a test runs
from ._catalog import AccessNamespace, CatalogNamespace
from ._client_base import (
    _ASK_STREAM_ENDED,
    _WORKFLOW_TEST_WAIT_TIMEOUT,
    DEFAULT_BASE_URL,
    DEFAULT_INTEGRATIONS_URL,
    DEFAULT_MAX_RETRIES,
    DEFAULT_RETRY_BUDGET_MS,
    DEFAULT_TIMEOUT,
    REVIEWS_DETAIL_PATH,
    Body,
    LbbCapabilityError,
    LbbError,
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
    _earlier_params,
    _EarlierList,
    _EmbeddingsNamespace,
    _EntityNamespace,
    _error_body_field,
    _EvalDatasetsNamespace,
    _EvalsNamespace,
    _file_head,
    _FilesNamespace,
    _is_earlier_list,
    _jittered_backoff,
    _list_envelope,
    _missing_route,
    _ModelChoicesNamespace,
    _ModelSwitchesNamespace,
    _ModelTrialsNamespace,
    _NamedWorkflowsNamespace,
    _OntologyNamespace,
    _parse_model,
    _QueryNamespace,
    _raw_response,
    _retries_status,
    _retries_transport_error,
    _retry_allowed,
    _retry_delay_seconds,
    _retryable,
    _ReviewsNamespace,
    _SchemaNamespace,
    _sse_decoder,
    _SseParser,
    _trial_settled,
    _trial_timeout,
    _workflow_test_open,
    _workflow_test_timeout,
    _WorkflowTestsNamespace,
    _write_chunks,
)
from ._workflow_execution import WorkflowTurnReader
from .integrations import IntegrationsNamespace

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
            super().sparql(
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

    def ask(
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
        """Answer a question about the graph in plain words
        (``POST /v1/query/ask``).

        The server's model runs a bounded loop of tool calls: it runs
        queries, looks up names, reads the graph description, finds the
        commit of a date, and compares two points. Then it answers.
        ``answer`` holds the text, ``citations`` the IRIs it names, and
        ``steps`` the tool calls. ``query`` and ``rows`` hold the query
        whose rows hold the answer. ``limit`` caps the rows each query of
        the loop returns (default 100).

        ``mode="route"`` returns only the kind of question, from the router
        model alone. ``route`` fixes the kind instead of asking the router.
        ``context`` adds notes for this call. ``anchor`` holds up to 10
        entity IRIs the user picked; the loop uses them directly.
        ``timeline`` (``[{"date", "as_of_commit_seq", "label"}]``, at most
        200) names the commit of each date, for graphs whose commits stand
        for other dates. ``include_grounding=True`` returns the graph
        description the models read in ``response["grounding"]["text"]``.

        Each call uses model tokens, so a failed call is not retried unless
        ``options={"retry": True}``. A ``429 rewrite_limit`` means the environment
        used its questions of the day.
        """
        return QueryAskResult.from_response(
            self._ask_response(
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
        )

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
    ) -> Generator[QueryAskStreamEvent, None, None]:
        """:meth:`ask` with progress: yield one event per step.

        The server sends ``grounding``, ``route``, a ``step`` per tool call
        of the loop, ``answer``, and last ``done``. ``done`` holds the same
        response as ``ask``: ``QueryAskResult.from_response(event.data)``
        parses it. A second ``route`` comes before ``answer`` when the loop
        chose another route. ``mode="route"`` sends ``grounding``, ``route``
        and ``done``. The client skips event names it does not know.

        An ``error`` event raises :class:`LbbError` with the status, code
        and message that ``ask`` raises. An error before the stream starts
        (a 400, a ``429 rewrite_limit``) raises as ``ask`` does. The call is
        never retried. Close the generator, or leave its loop, to close the
        response; the server then stops its work. A body that ends before
        ``done`` raises ``httpx.RemoteProtocolError``. ``options["timeout"]``
        bounds each network read, not the whole stream.
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
        client = cast("LbbClient", self._client)
        return client._ask_events(body, params or None, options)

    def update(
        self,
        update: str,
        *,
        idempotency_key: str | None = None,
        options: RequestOptions | None = None,
    ) -> None:
        super().update(update, idempotency_key=idempotency_key, options=options)


class _SyncModelTrialsNamespace(_ModelTrialsNamespace):
    __doc__ = _ModelTrialsNamespace.__doc__

    def options(self) -> models.ModelTrialOptionsResponse:
        return cast(models.ModelTrialOptionsResponse, super().options())

    def create(
        self,
        *,
        candidate: TrialCandidate,
        jobs: Sequence[str | models.ModelJob] | None = None,
        target: int | None = None,
        days: int | None = None,
    ) -> models.ModelTrialStartResponse:
        return cast(
            models.ModelTrialStartResponse,
            super().create(candidate=candidate, jobs=jobs, target=target, days=days),
        )

    def list(
        self, *, job: str | models.ModelJob | None = None, limit: int | None = None
    ) -> models.ModelTrialListResponse:
        return cast(models.ModelTrialListResponse, super().list(job=job, limit=limit))

    def get(self, trial_id: str) -> models.ModelTrial:
        return cast(models.ModelTrial, super().get(trial_id))

    def call(self, trial_id: str, call_id: str) -> models.ModelTrialCallResponse:
        return cast(models.ModelTrialCallResponse, super().call(trial_id, call_id))

    def stop(self, trial_id: str) -> models.ModelTrial:
        return cast(models.ModelTrial, super().stop(trial_id))

    def wait(
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
        A trial of question answers takes about 10 to 30 s per call; one of
        a decision or an order, about 1 s.
        """
        if poll_interval < 0:
            raise ValueError("poll_interval must be non-negative")
        if timeout is not None and timeout < 0:
            raise ValueError("timeout must be non-negative")
        deadline = time.monotonic() + timeout if timeout is not None else None
        while True:
            trial = self.get(trial_id)
            if on_update is not None:
                on_update(trial)
            if _trial_settled(trial, until):
                return trial
            if deadline is not None and time.monotonic() >= deadline:
                raise _trial_timeout(trial_id, trial)
            time.sleep(poll_interval)


class _SyncModelSwitchesNamespace(_ModelSwitchesNamespace):
    __doc__ = _ModelSwitchesNamespace.__doc__

    def list(self) -> models.ModelSwitchListResponse:
        return cast(models.ModelSwitchListResponse, super().list())

    def create(self, *, trial: str) -> models.ModelSwitch:
        return cast(models.ModelSwitch, super().create(trial=trial))

    def revert(self, job: str | models.ModelJob) -> models.ModelSwitchListResponse:
        return cast(models.ModelSwitchListResponse, super().revert(job))


class _SyncModelChoicesNamespace(_ModelChoicesNamespace):
    __doc__ = _ModelChoicesNamespace.__doc__

    def list(
        self,
        *,
        workflow: str | None = None,
        use: str | models.ModelUse | None = None,
        limit: int | None = None,
        cursor: str | None = None,
    ) -> models.ModelChoiceList:
        return cast(
            models.ModelChoiceList,
            super().list(workflow=workflow, use=use, limit=limit, cursor=cursor),
        )

    def all(
        self,
        *,
        workflow: str | None = None,
        use: str | models.ModelUse | None = None,
        limit: int | None = None,
    ) -> Iterator[models.ModelChoice]:
        """Every row: :meth:`list` page by page, with the same filters."""
        return _every(
            lambda cursor: self.list(workflow=workflow, use=use, limit=limit, cursor=cursor)
        )

    def put(
        self,
        *,
        use: str | models.ModelUse,
        model: TrialCandidate,
        workflow: str | None = None,
        idempotency_key: str | None = None,
    ) -> models.ModelChoice:
        return cast(
            models.ModelChoice,
            super().put(use=use, model=model, workflow=workflow, idempotency_key=idempotency_key),
        )

    def delete(
        self,
        *,
        use: str | models.ModelUse,
        workflow: str | None = None,
        idempotency_key: str | None = None,
    ) -> models.ModelChoiceDeleted:
        return cast(
            models.ModelChoiceDeleted,
            super().delete(use=use, workflow=workflow, idempotency_key=idempotency_key),
        )


class _SyncModelsNamespace:
    """The models of the graph's uses: trials of other models
    (:attr:`trials`), switches (:attr:`switches`), and the model choices of
    the workflow uses (:attr:`choices`)."""

    def __init__(self, client: _BaseLbbClient) -> None:
        self.trials = _SyncModelTrialsNamespace(client)
        self.switches = _SyncModelSwitchesNamespace(client)
        self.choices = _SyncModelChoicesNamespace(client)


def _every(fetch: Callable[[str | None], Any]) -> Iterator[Any]:
    """Every item of a list in the list envelope: ``fetch(cursor)`` page by
    page, until ``has_more`` is false."""
    cursor: str | None = None
    while True:
        page = fetch(cursor)
        yield from page.data
        if not page.has_more or not page.next_cursor:
            return
        cursor = page.next_cursor


class _SyncWorkflowTestsNamespace(_WorkflowTestsNamespace):
    __doc__ = _WorkflowTestsNamespace.__doc__

    def create(
        self,
        *,
        name: str | None = None,
        install: Mapping[str, Any] | None = None,
        revision: str | None = None,
        models: Mapping[str, TrialCandidate] | None = None,
        iris: Sequence[str] | None = None,
        sample: int | None = None,
        idempotency_key: str | None = None,
    ) -> _models.WorkflowTest:
        return cast(
            _models.WorkflowTest,
            super().create(
                name=name,
                install=install,
                revision=revision,
                models=models,
                iris=iris,
                sample=sample,
                idempotency_key=idempotency_key,
            ),
        )

    def get(self, test_id: str) -> models.WorkflowTest:
        return cast(models.WorkflowTest, super().get(test_id))

    def list(
        self,
        *,
        name: str | None = None,
        limit: int | None = None,
        cursor: str | None = None,
    ) -> models.WorkflowTestList:
        return cast(
            models.WorkflowTestList,
            super().list(name=name, limit=limit, cursor=cursor),
        )

    def all(
        self, *, name: str | None = None, limit: int | None = None
    ) -> Iterator[models.WorkflowTest]:
        """Every test, or every test of the workflow ``name``, newest first:
        :meth:`list` page after page (``limit`` tests per page)."""
        return _every(lambda cursor: self.list(name=name, limit=limit, cursor=cursor))

    def answer(
        self,
        test_id: str,
        iri: str,
        property: str,
        answer: Mapping[str, Any] | None,
        *,
        idempotency_key: str | None = None,
    ) -> models.WorkflowTest:
        return cast(
            models.WorkflowTest,
            super().answer(test_id, iri, property, answer, idempotency_key=idempotency_key),
        )

    def cancel(self, test_id: str) -> models.WorkflowTest:
        return cast(models.WorkflowTest, super().cancel(test_id))

    def wait(
        self,
        test_id: str,
        *,
        timeout: float | None = _WORKFLOW_TEST_WAIT_TIMEOUT,
        poll_interval: float = 1.0,
    ) -> models.WorkflowTest:
        """Read the test until it ends (``done``, ``failed`` or
        ``cancelled``) and return it. Raises :class:`TimeoutError` when the
        test is still open after ``timeout`` seconds (default 600; ``None``
        waits as long as it takes)."""
        if poll_interval < 0:
            raise ValueError("poll_interval must be non-negative")
        if timeout is not None and timeout < 0:
            raise ValueError("timeout must be non-negative")
        deadline = time.monotonic() + timeout if timeout is not None else None
        while True:
            test = self.get(test_id)
            if not _workflow_test_open(test):
                return test
            if deadline is not None and time.monotonic() >= deadline:
                raise _workflow_test_timeout(test_id, test)
            time.sleep(poll_interval)


class _SyncNamedWorkflowsNamespace(_NamedWorkflowsNamespace):
    __doc__ = _NamedWorkflowsNamespace.__doc__

    def _list(
        self,
        model_cls: type[ModelT],
        path: str,
        params: Mapping[str, Any],
        earlier: _EarlierList,
    ) -> ModelT:
        payload = self._client._request("GET", path, params=params)
        if _is_earlier_list(payload, earlier):
            again = _earlier_params(params, earlier)
            if again is not None:
                payload = self._client._request("GET", path, params=again)
            if _is_earlier_list(payload, earlier):
                payload = _list_envelope(payload, earlier)
        return _parse_model(model_cls, payload)

    def all(self, *, limit: int | None = None) -> Iterator[models.NamedWorkflow]:
        """Every workflow of the graph, by name: :meth:`list` page by page."""
        return _every(lambda cursor: self.list(limit=limit, cursor=cursor))

    def all_runs(
        self,
        name: str,
        *,
        limit: int | None = None,
        outcome: str | models.WorkflowRunOutcome | None = None,
    ) -> Iterator[models.WorkflowRunSummary]:
        """Every run of the workflow kept, newest first: :meth:`runs` page by
        page."""
        return _every(
            lambda cursor: self.runs(name, cursor=cursor, limit=limit, outcome=outcome)
        )

    def all_revisions(
        self, name: str, *, limit: int | None = None
    ) -> Iterator[models.WorkflowRevision]:
        """Every kept activation of the workflow's settings, newest first:
        :meth:`revisions` page by page."""
        return _every(lambda cursor: self.revisions(name, cursor=cursor, limit=limit))

    def all_run_results(
        self,
        name: str,
        *,
        run: str | None = None,
        outcome: str | Sequence[str] | None = None,
        iri: str | None = None,
        limit: int | None = None,
    ) -> Iterator[models.WorkflowRunResult]:
        """Every result of the workflow's runs kept, newest run first:
        :meth:`run_results` page by page."""
        return _every(
            lambda cursor: self.run_results(
                name, run=run, outcome=outcome, iri=iri, cursor=cursor, limit=limit
            )
        )

    def all_documents(
        self,
        name: str,
        *,
        q: str | None = None,
        sort: str | models.TriggeredDocumentSort | None = None,
        limit: int | None = None,
    ) -> Iterator[Any]:
        """Every document of a ``documents.link`` or ``documents.map``
        workflow: :meth:`documents` page by page."""
        return _every(
            lambda cursor: self.documents(name, q=q, sort=sort, cursor=cursor, limit=limit)
        )


class _SyncEvalDatasetsNamespace(_EvalDatasetsNamespace):
    __doc__ = _EvalDatasetsNamespace.__doc__

    def all(self, *, limit: int | None = None) -> Iterator[models.EvalDataset]:
        """Every dataset, newest first, without their items: :meth:`list`
        page after page (``limit`` datasets per page)."""
        return _every(lambda cursor: self.list(limit=limit, cursor=cursor))


class _SyncEvalsNamespace(_EvalsNamespace):
    __doc__ = _EvalsNamespace.__doc__
    datasets: _SyncEvalDatasetsNamespace

    def __init__(self, client: _BaseLbbClient) -> None:
        super().__init__(client)
        self.datasets = _SyncEvalDatasetsNamespace(client)


class _SyncReviewsNamespace(_ReviewsNamespace):
    __doc__ = _ReviewsNamespace.__doc__

    def all(
        self,
        *,
        status: str | None = None,
        kind: str | Sequence[str] | None = None,
        scope: str | None = None,
        origin_id: str | None = None,
        workflow: str | None = None,
        run: str | None = None,
        limit: int | None = None,
        key: str | Sequence[str] | None = None,
        eval_id: str | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Every item, the latest update first: :meth:`list` page by page,
        with the same filters (``limit`` items a page)."""
        cursor: str | None = None
        while True:
            page = self.list(
                status=status,
                kind=kind,
                scope=scope,
                origin_id=origin_id,
                workflow=workflow,
                run=run,
                limit=limit,
                cursor=cursor,
                key=key,
                eval_id=eval_id,
            )
            yield from page["data"]
            if not page.get("has_more") or not page.get("next_cursor"):
                return
            cursor = page["next_cursor"]

    def get(self, id: str) -> Any:
        try:
            return super().get(id)
        except LbbError as error:
            if not _missing_route(error):
                raise
            return self._client._request("GET", REVIEWS_DETAIL_PATH, params={"id": id})


class _SyncWorkflowsNamespace:
    """Workflows: the workflows of the graph by name (:attr:`named`), and
    tests of a workflow on chosen inputs (:attr:`tests`)."""

    def __init__(self, client: _BaseLbbClient) -> None:
        #: The workflows of the graph by name: list, read, change, run,
        #: pause, delete, and their queue, runs and documents.
        self.named = _SyncNamedWorkflowsNamespace(client)
        self.tests = _SyncWorkflowTestsNamespace(client)


class LbbClient(_BaseLbbClient):
    """Synchronous client. Usable as a context manager."""

    entities: _EntityNamespace
    ontology: _SyncOntologyNamespace
    query: _SyncQueryNamespace
    schema: _SchemaNamespace
    evals: _SyncEvalsNamespace
    checks: _ChecksNamespace
    embeddings: _EmbeddingsNamespace
    integrations: IntegrationsNamespace

    def __init__(
        self,
        base_url: str = DEFAULT_BASE_URL,
        *,
        api_key: str | None = None,
        graph: str | None = None,
        environment: str | None = None,
        api_version: str = "2026-10-21",
        max_retries: int = DEFAULT_MAX_RETRIES,
        retry_delay: float = 0.1,
        retry_budget_ms: float = DEFAULT_RETRY_BUDGET_MS,
        on_retry: Callable[[RetryEvent], None] | None = None,
        timeout: float = DEFAULT_TIMEOUT,
        transport: httpx.BaseTransport | None = None,
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
        self.entities = _EntityNamespace(self)
        self.ontology = _SyncOntologyNamespace(self)
        self.query = _SyncQueryNamespace(self)
        self.schema = _SchemaNamespace(self)
        self.evals = _SyncEvalsNamespace(self)
        # Model checks: the call log, the judge's checks, and reviews.
        self.checks = _ChecksNamespace(self)
        # Review: questions for a person, and their answers.
        self.reviews = _SyncReviewsNamespace(self)
        # Model trials of other models on the graph's uses, and switches.
        self.models = _SyncModelsNamespace(self)
        # Tests of a workflow on chosen inputs, which write nothing.
        self.workflows = _SyncWorkflowsNamespace(self)
        self.embeddings = _EmbeddingsNamespace(self)
        # Files of the graph: upload, list, bytes, parsed pages, delete.
        self.files = _FilesNamespace(self)
        self.catalog = CatalogNamespace(self)
        self.access = AccessNamespace(self)
        # Hosted integrations for your end customers, at ``integrations_url``.
        self.integrations = IntegrationsNamespace(self)
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

    def workflow_turn_reader(self, task: Body) -> WorkflowTurnReader:
        """Bind SELECT/ASK to a task claimed with include_execution_credential.

        Requires its issued execution credential; never falls back to this
        client's API key for reads. Keep heartbeats/checkpoints on this client.
        """
        return WorkflowTurnReader(self, task)

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
        return self._send(
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
        return self._send(
            method,
            path,
            f"{self._integrations_url}{path}",
            params=params,
            body=body,
            idempotency_key=idempotency_key,
            options=options,
            scoped=False,
        ).data

    def _send(
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
        started_at = time.monotonic()
        # Deadline is the binding limit; `max_retries` is a secondary safety cap.
        deadline = started_at + max(0.0, retry_budget_ms) / 1000.0
        attempts = 0
        for attempt in range(max_retries + 1):
            attempts = attempt + 1
            try:
                response = self._http.request(method, url, **kwargs)
            except httpx.RequestError:
                if not (_retries_transport_error(retry) and attempt < max_retries):
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
            binary=binary,
        )

    def _event_stream(
        self,
        method: str,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        body: Body | None = None,
        options: RequestOptions | None = None,
        json_event: str | None = None,
        on_open: Callable[[httpx.Response], None] | None = None,
    ) -> Generator[tuple[str, str], None, None]:
        """Send one request with ``Accept: text/event-stream`` and yield its
        server-sent events as ``(event, data)`` while they arrive.

        The request is sent once and never retried. A non-2xx answer raises
        :class:`LbbError` as :meth:`raw_request` does. A 2xx answer that is
        not an event stream (a server without streams) becomes one event
        named ``json_event`` with the whole body as its data. Closing the
        generator closes the response.
        """
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
        with self._http.stream(method, f"{self._base_url}{path}", **kwargs) as response:
            if on_open is not None:
                on_open(response)
            content_type = response.headers.get("content-type", "").lower()
            if response.status_code // 100 != 2 or "text/event-stream" not in content_type:
                response.read()
                _raw_response(response)
                if json_event is not None and response.text.strip():
                    yield json_event, response.text
                return
            parser = _SseParser()
            decoder = _sse_decoder()
            for chunk in response.iter_bytes():
                yield from parser.feed(decoder.decode(chunk))
            yield from parser.feed(decoder.decode(b"", final=True))

    def _ask_events(
        self,
        body: dict[str, Any],
        params: Mapping[str, Any] | None,
        options: RequestOptions | None,
    ) -> Generator[QueryAskStreamEvent, None, None]:
        """The events of ``POST /v1/query/ask`` as a stream."""
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
            for name, text in stream:
                request_id = opened[0].headers.get("x-request-id") if opened else None
                event = _ask_stream_event(name, text, request_id)
                if event is None:
                    continue
                yield event
                if event.event == "done":
                    return
        finally:
            stream.close()
        raise httpx.RemoteProtocolError(
            _ASK_STREAM_ENDED, request=opened[0].request if opened else None
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

    def _bytes_request(
        self,
        method: str,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        options: RequestOptions | None = None,
    ) -> bytes:
        response = self._send(
            method,
            path,
            f"{self._base_url}{path}",
            params=params,
            options=options,
            binary=True,
        )
        return cast(bytes, response.data)

    def _download_request(
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
        started_at = time.monotonic()
        deadline = started_at + max(0.0, retry_budget_ms) / 1000.0
        url = f"{self._base_url}{path}"
        attempt = 0
        while True:
            delay: float | None = None
            answered = False
            try:
                with self._http.stream("GET", url, **kwargs) as response:
                    answered = True
                    if response.status_code // 100 == 2:
                        return _write_chunks(response.iter_bytes(), dest)
                    response.read()
                    if (
                        _retryable(response.status_code)
                        and attempt < max_retries
                        and not _body_marks_terminal(response)
                    ):
                        delay = _retry_delay_seconds(response, self._retry_delay, attempt)
                        if time.monotonic() + delay > deadline:
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
                        elapsed_ms=(time.monotonic() - started_at) * 1000,
                    )
            except httpx.TransportError:
                # Only a failure before the answer starts is retried. A
                # transfer that broke raises, and leaves nothing at ``dest``.
                if answered or attempt >= max_retries:
                    raise
                delay = _jittered_backoff(self._retry_delay, attempt)
                if time.monotonic() + delay > deadline:
                    raise
            assert delay is not None
            time.sleep(delay)
            attempt += 1

    def _file_head_request(
        self,
        path: str,
        sha256: str,
        *,
        params: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        response = self._send(
            "HEAD",
            path,
            f"{self._base_url}{path}",
            params=params,
            binary=True,
        )
        return _file_head(response.headers, sha256)

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
        """Run a SPARQL 1.1 text query (SELECT or ASK) and return parsed results.

        The ergonomic entry point: pass query text, get a :class:`SparqlResults`
        with ``.rows()``, ``.vars``, and ``.boolean`` already parsed — no manual
        ``json.loads`` of a results string. Engine extensions map to query
        options: ``entailment`` (``"rdfs"`` for the query-time RDFS core,
        ``"subclass"`` for the class-only subset; the default ``"none"`` matches
        asserted triples only) and ``limit``/``offset``. ``reason`` is refused
        on the published surface (stored rules already run at publish time).

        ``as_of_commit_seq`` reads the retained published generation of that
        exact commit. The result's ``snapshot`` names the commit the rows were
        read from (``served_at_seq``) for an eventual or pinned read, and is
        ``None`` for a plain strong read. A retryable ``429``, such as
        ``read_your_writes_pending`` under a ``min_indexed_seq`` floor, is
        retried within the retry budget.

        ``request`` is the user's words behind the query: the server records
        an eval trace and the result's ``trace_id`` names it. A query with a
        ``search:similarTo`` triple searches by meaning inside the query, and
        the result's ``search`` reports how that search ran.

        Note: this uses ``/v1/query/sparql-text``. A standalone server also serves
        the native SPARQL 1.1 *Protocol* at ``/sparql`` for off-the-shelf SPARQL
        clients (YASGUI, Protégé, RDFLib) with ``Accept``-negotiated
        JSON/XML/CSV/TSV; this SDK method returns parsed JSON rows.
        """
        envelope = self._sparql_text_envelope(
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

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> LbbClient:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()
