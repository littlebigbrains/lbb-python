"""Strict records commits over either transport; no graph activation."""

from __future__ import annotations

import inspect
import json
import re
from typing import TYPE_CHECKING, Any, cast

import httpx

from . import models
from ._client_base import _coerce_body

if TYPE_CHECKING:
    from ._client_base import Body, RequestOptions, _BaseLbbClient


class LbbRecordCommitOutcomeError(RuntimeError):
    """The POST may have committed; its response did not prove acceptance."""

    outcome = "unknown"

    def __init__(self, idempotency_key: str) -> None:
        self.idempotency_key = idempotency_key
        super().__init__(
            "Record commit outcome is unknown; the request may have committed. "
            "Retry the original body with the same idempotency key "
            f"({idempotency_key}) to recover its receipt."
        )


_CAPABILITY = "records_commit_v1"
_U64_MAX = 18_446_744_073_709_551_615
_DECIMAL = re.compile(r"^(0|[1-9][0-9]*)$")


def _text(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _counter(value: Any) -> bool:
    return type(value) is int and 0 <= value <= _U64_MAX


def _ids(value: Any) -> bool:
    return isinstance(value, list) and all(_text(item) for item in value)


def _checked(payload: Any, key: str, *, typed: bool) -> Any:
    if inspect.isawaitable(payload):

        async def checked() -> Any:
            try:
                result = await payload
            except httpx.RequestError as error:
                raise LbbRecordCommitOutcomeError(key) from error
            except ValueError as error:
                if str(error).startswith(
                    "Little Big Brain returned invalid JSON with HTTP 2"
                ):
                    raise LbbRecordCommitOutcomeError(key) from error
                raise
            return _checked(result, key, typed=typed)

        return checked()
    valid = isinstance(payload, dict)
    if valid:
        valid = (
            payload.get("object") == "record_commit"
            and payload.get("version") == "records/1"
            and type(payload.get("idempotent_replay")) is bool
            and isinstance(payload.get("receipt"), dict)
        )
    if valid:
        r = payload["receipt"]
        epoch = r.get("epoch")
        scope, ids, projection = (
            r.get("scope"),
            r.get("resolved_ids"),
            r.get("projection"),
        )
        valid = (
            isinstance(epoch, str)
            and len(epoch) <= 20
            and _DECIMAL.fullmatch(epoch) is not None
            and int(epoch) <= _U64_MAX
            and _counter(r.get("commit_seq"))
            and _text(r.get("visibility_token"))
            and _text(r.get("schema_revision"))
            and type(r.get("no_op")) is bool
            and isinstance(scope, dict)
            and _text(scope.get("owner"))
            and _text(scope.get("scope"))
            and isinstance(ids, dict)
            and _ids(ids.get("entities"))
            and _ids(ids.get("relationships"))
            and isinstance(projection, dict)
            and _counter(projection.get("added"))
            and _counter(projection.get("removed"))
            and (not r["no_op"] or (projection["added"] == projection["removed"] == 0))
        )
    if not valid:
        raise LbbRecordCommitOutcomeError(key)
    if typed:
        try:
            return models.RecordCommitResult.model_validate(payload)
        except (ValueError, TypeError, AttributeError) as error:
            raise LbbRecordCommitOutcomeError(key) from error
    return payload


class RecordsNamespace:
    """Exact records on an existing support-enabled graph, never activation.

    Explicit keys and the original body must be retained after an unknown
    outcome. A receipt proves acceptance, separately from publication/search.
    """

    def __init__(self, client: _BaseLbbClient, graph: str | None = None) -> None:
        self._client = client
        self._graph = graph

    def commit(
        self,
        body: Body,
        *,
        idempotency_key: str,
        expected_graph_epoch: str | None = None,
        options: RequestOptions | None = None,
    ) -> Any:
        return self._submit(
            body, idempotency_key, expected_graph_epoch, options, typed=False
        )

    def commit_model(
        self,
        body: Body,
        *,
        idempotency_key: str,
        expected_graph_epoch: str | None = None,
        options: RequestOptions | None = None,
    ) -> models.RecordCommitResult:
        return cast(
            models.RecordCommitResult,
            self._submit(
                body,
                idempotency_key,
                expected_graph_epoch,
                options,
                typed=True,
            ),
        )

    def _submit(
        self,
        body: Body,
        key: str,
        epoch: str | None,
        options: RequestOptions | None,
        *,
        typed: bool,
    ) -> Any:
        value = _coerce_body(body)
        if not isinstance(value, dict) or value.get("version") != "records/1":
            raise ValueError("records.commit requires explicit version records/1")
        if not isinstance(key, str) or not key.strip():
            raise ValueError("records.commit requires an explicit idempotency_key")
        captured = cast("RequestOptions", dict(options or {}))
        headers = dict(captured.get("headers") or {})
        if any(name.strip().lower() == "idempotency-key" for name in headers):
            raise ValueError(
                "use idempotency_key, not an Idempotency-Key header override"
            )
        captured["headers"] = headers
        if epoch is not None and (
            not isinstance(epoch, str)
            or len(epoch) > 20
            or not _DECIMAL.fullmatch(epoch)
            or int(epoch) > _U64_MAX
        ):
            raise ValueError("expected_graph_epoch must be a decimal u64 string")
        # Freeze nested data before invoking the possibly asynchronous capability
        # gate. httpx receives immutable bytes, never a mapping re-encoded on retry.
        content = json.dumps(
            value, ensure_ascii=False, allow_nan=False, separators=(",", ":")
        ).encode("utf-8")
        encoded = json.loads(content)
        if not isinstance(encoded, dict) or encoded.get("version") != "records/1":
            raise ValueError(
                "encoded records.commit body must retain version records/1"
            )
        params: dict[str, Any] = {}
        if self._graph is not None:
            params["graph"] = self._graph
        if epoch is not None:
            params["expected_graph_epoch"] = epoch
        capability = self._client._require_capability(_CAPABILITY)

        def send() -> Any:
            try:
                payload = self._client._request(
                    "POST",
                    "/v1/graph/commit",
                    params=params,
                    content=content,
                    content_type="application/json",
                    idempotency_key=key,
                    options=captured,
                )
            except httpx.RequestError as error:
                raise LbbRecordCommitOutcomeError(key) from error
            except ValueError as error:
                if str(error).startswith(
                    "Little Big Brain returned invalid JSON with HTTP 2"
                ):
                    raise LbbRecordCommitOutcomeError(key) from error
                raise
            return _checked(payload, key, typed=typed)

        if inspect.isawaitable(capability):

            async def checked_send() -> Any:
                await capability
                return await send()

            return checked_send()
        return send()
