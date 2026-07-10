"""Synchronous transport for the Little Big Brain Python SDK."""

from __future__ import annotations

import time
from collections.abc import Callable, Iterator, Mapping
from typing import Any, cast

import httpx

from . import models
from ._client_base import (
    DEFAULT_BASE_URL,
    DEFAULT_TIMEOUT,
    Body,
    ListPage,
    ModelT,
    RawLbbResponse,
    RequestOptions,
    RowT,
    SparqlResults,
    _BaseLbbClient,
    _ContextNamespace,
    _EntityNamespace,
    _OntologyNamespace,
    _parse_model,
    _QueryNamespace,
    _raw_response,
    _retry_allowed,
    _retry_delay_seconds,
    _retryable,
)


class _SyncContextNamespace(_ContextNamespace):
    def ask(self, body: Body, *, options: RequestOptions | None = None) -> models.AskResponse:
        return cast(models.AskResponse, super().ask(body, options=options))

    def suggest(
        self, body: Body, *, options: RequestOptions | None = None
    ) -> models.SearchSuggestResponse:
        return cast(models.SearchSuggestResponse, super().suggest(body, options=options))

    def resolve(
        self, body: Body, *, options: RequestOptions | None = None
    ) -> models.ResolveTermResponse:
        return cast(models.ResolveTermResponse, super().resolve(body, options=options))

    def decode(self, body: Body, *, options: RequestOptions | None = None) -> models.DecodeResponse:
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

    def conformance(self, *, options: RequestOptions | None = None) -> models.SchemaAuditReport:
        return cast(models.SchemaAuditReport, super().conformance(options=options))

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

    def evolve(self, body: Body) -> models.OntologyEvolveResponse:
        return cast(models.OntologyEvolveResponse, super().evolve(body))


class _SyncQueryNamespace(_QueryNamespace):
    def structured(
        self, body: Body, *, options: RequestOptions | None = None
    ) -> models.SparqlSelectResponse:
        return cast(models.SparqlSelectResponse, super().structured(body, options=options))

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
    ) -> SparqlResults:
        return cast(
            SparqlResults,
            super().sparql(
                query,
                reason=reason,
                entailment=entailment,
                as_of_valid_time=as_of_valid_time,
                as_of_commit_seq=as_of_commit_seq,
                limit=limit,
                offset=offset,
            ),
        )

    def analytics(
        self, body: Body, *, options: RequestOptions | None = None
    ) -> models.AnalyticQueryResponse:
        return cast(models.AnalyticQueryResponse, super().analytics(body, options=options))

    def shacl(
        self, body: Body, *, options: RequestOptions | None = None
    ) -> models.ShaclQueryResponse:
        return cast(models.ShaclQueryResponse, super().shacl(body, options=options))

    def infer(
        self, body: Body, *, options: RequestOptions | None = None
    ) -> models.InferenceRunResponse:
        return cast(models.InferenceRunResponse, super().infer(body, options=options))

    def premises(
        self, body: Body, *, options: RequestOptions | None = None
    ) -> models.RetrievalPremiseResponse:
        return cast(models.RetrievalPremiseResponse, super().premises(body, options=options))


class _SyncEntityNamespace(_EntityNamespace):
    def list_page(self, **kwargs: Any) -> ListPage[models.EntityExplorerRow]:
        return cast(ListPage[models.EntityExplorerRow], super().list_page(**kwargs))

    def pages(self, **kwargs: Any) -> Iterator[ListPage[models.EntityExplorerRow]]:
        return cast(Iterator[ListPage[models.EntityExplorerRow]], super().pages(**kwargs))

    def iter(self, **kwargs: Any) -> Iterator[models.EntityExplorerRow]:
        return cast(Iterator[models.EntityExplorerRow], super().iter(**kwargs))


class LbbClient(_BaseLbbClient):
    """Synchronous client. Usable as a context manager."""

    context: _SyncContextNamespace
    entities: _SyncEntityNamespace
    ontology: _SyncOntologyNamespace
    query: _SyncQueryNamespace

    def __init__(
        self,
        base_url: str = DEFAULT_BASE_URL,
        *,
        api_key: str | None = None,
        graph: str | None = None,
        branch: str | None = None,
        api_version: str = "2026-06-22",
        max_retries: int = 2,
        retry_delay: float = 0.1,
        timeout: float = DEFAULT_TIMEOUT,
        transport: httpx.BaseTransport | None = None,
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
        )
        self.context = _SyncContextNamespace(self)
        self.entities = _SyncEntityNamespace(self)
        self.ontology = _SyncOntologyNamespace(self)
        self.query = _SyncQueryNamespace(self)
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
        started_at = time.monotonic()
        attempts = 0
        for attempt in range(max_retries + 1):
            attempts = attempt + 1
            try:
                response = self._http.request(method, f"{self._base_url}{path}", **kwargs)
            except httpx.RequestError:
                if can_retry and attempt < max_retries:
                    time.sleep(self._retry_delay * (attempt + 1))
                    continue
                raise
            if response.status_code // 100 == 2 or not _retryable(response.status_code):
                break
            if not can_retry:
                break
            if attempt < max_retries:
                time.sleep(_retry_delay_seconds(response, self._retry_delay, attempt))
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

    def _iter_entity_pages(self, **kwargs: Any) -> Iterator[ListPage[models.EntityExplorerRow]]:
        cursor = kwargs.pop("cursor", None)
        initial_offset = kwargs.pop("offset", None)
        seen: set[str] = set()
        while True:
            page = self.entities.list_page(
                **kwargs,
                cursor=cursor,
                offset=initial_offset if cursor is None else None,
            )
            yield page
            if not page.has_more or page.next_cursor is None:
                return
            if page.next_cursor in seen:
                raise RuntimeError(f"entity pagination cursor repeated: {page.next_cursor}")
            seen.add(page.next_cursor)
            cursor = page.next_cursor

    def _iter_entity_rows(self, **kwargs: Any) -> Iterator[models.EntityExplorerRow]:
        for page in self._iter_entity_pages(**kwargs):
            yield from page

    def wait_for_index(
        self, *, timeout: float = 600.0, poll_interval: float = 2.0
    ) -> dict[str, Any]:
        """Block until the persisted index has caught up with the WAL head.

        Pairs with ``index_run(background=True)``: polls :meth:`metadata` until the
        persisted index has caught up, or until ``timeout`` seconds elapse (raises
        ``TimeoutError``). Returns the final metadata. Uses the server's
        authoritative ``index_caught_up`` signal when present, falling back to the
        BM25/ANN/tail predicate for older servers.
        """
        deadline = time.monotonic() + timeout
        while True:
            meta = cast(dict[str, Any], self.metadata())
            caught_up = meta.get("index_caught_up")
            if caught_up is None:
                caught_up = (
                    meta.get("bm25_indexed_commit_seq") is not None
                    and meta.get("ann_indexed_commit_seq") is not None
                    and meta.get("unindexed_tail_commits", 1) == 0
                )
            if caught_up:
                return meta
            if time.monotonic() >= deadline:
                raise TimeoutError(f"index did not catch up within {timeout}s (last: {meta})")
            time.sleep(poll_interval)

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
    ) -> SparqlResults:
        """Run a SPARQL 1.1 text query (SELECT or ASK) and return parsed results.

        The ergonomic entry point: pass query text, get a :class:`SparqlResults`
        with ``.rows()``, ``.vars``, and ``.boolean`` already parsed — no manual
        ``json.loads`` of a results string. Engine extensions map to query
        options: ``reason`` (fold rule-derived edges), ``entailment`` (``"none"``
        to disable the default ``rdfs:subClassOf`` closure), the ``as_of_*``
        snapshot pins, and ``limit``/``offset``.

        Note: this uses ``/v1/query/sparql-text``. A standalone stack also serves
        the native SPARQL 1.1 *Protocol* at ``/sparql`` for off-the-shelf SPARQL
        clients (YASGUI, Protégé, RDFLib) with ``Accept``-negotiated
        JSON/XML/CSV/TSV; this SDK method returns parsed JSON rows.
        """
        envelope = self._sparql_text_envelope(
            query,
            reason=reason,
            entailment=entailment,
            as_of_valid_time=as_of_valid_time,
            as_of_commit_seq=as_of_commit_seq,
            limit=limit,
            offset=offset,
        )
        return SparqlResults.from_envelope(envelope)

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> LbbClient:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()
