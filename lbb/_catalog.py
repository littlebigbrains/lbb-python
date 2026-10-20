"""Optional catalog and delegated read access, using either SDK transport."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    from ._client_base import Body, RequestOptions, _BaseLbbClient


CatalogUsageProfile = Literal["registered-request-resource-v1", "raw-resource-receipts-v1"]


class CatalogNamespace:
    """Environment-admin catalog operations. Async clients return awaitables."""

    def __init__(self, client: _BaseLbbClient) -> None:
        self._client = client

    def root(self, *, options: RequestOptions | None = None) -> Any:
        return self._client._request("GET", "/v1/catalog/root", options=options)

    def create_namespace(
        self, body: Body, *, idempotency_key: str, options: RequestOptions | None = None
    ) -> Any:
        return self._client._request(
            "POST",
            "/v1/catalog/namespaces",
            body=body,
            idempotency_key=idempotency_key,
            options=options,
        )

    def namespace(self, namespace_id: str, *, options: RequestOptions | None = None) -> Any:
        return self._client._request(
            "GET", "/v1/catalog/namespace", params={"id": namespace_id}, options=options
        )

    def resolve(self, path: str, *, options: RequestOptions | None = None) -> Any:
        return self._client._request(
            "GET", "/v1/catalog/namespaces/resolve", params={"path": path}, options=options
        )

    def namespaces(
        self,
        *,
        parent_id: str | None = None,
        cursor: str | None = None,
        limit: int | None = None,
        options: RequestOptions | None = None,
    ) -> Any:
        return self._client._request(
            "GET",
            "/v1/catalog/namespaces",
            params={"cursor": cursor, "limit": limit, "parent_id": parent_id},
            options=options,
        )

    def usage(
        self,
        *,
        owner_id: str,
        owner_incarnation: str,
        period_start_micros: int,
        profile: CatalogUsageProfile | None = None,
        cursor: str | None = None,
        limit: int | None = None,
        options: RequestOptions | None = None,
    ) -> Any:
        """One owner page; async clients return an awaitable. Never add profiles together."""
        return self._client._request(
            "GET",
            "/v1/catalog/usage",
            params={
                "owner_id": owner_id,
                "owner_incarnation": owner_incarnation,
                "period_start_micros": period_start_micros,
                "profile": profile,
                "cursor": cursor,
                "limit": limit,
            },
            options=options,
        )

    def adopt_graph(
        self, body: Body, *, idempotency_key: str, options: RequestOptions | None = None
    ) -> Any:
        """Adopt the client's existing graph, pinned to expected_graph_epoch."""
        return self._client._request(
            "POST",
            "/v1/catalog/graphs/adopt",
            body=body,
            idempotency_key=idempotency_key,
            options=options,
        )

    def graph(self, *, options: RequestOptions | None = None) -> Any:
        return self._client._request("GET", "/v1/catalog/graph", options=options)

    def graphs(
        self,
        *,
        owner_id: str | None = None,
        include_descendants: bool | None = None,
        cursor: str | None = None,
        limit: int | None = None,
        options: RequestOptions | None = None,
    ) -> Any:
        """One bounded page; continue even when empty until next_cursor is null."""
        return self._client._request(
            "GET",
            "/v1/catalog/graphs",
            params={
                "cursor": cursor,
                "limit": limit,
                "owner_id": owner_id,
                "include_descendants": include_descendants,
            },
            options=options,
        )

    def pending_operations(
        self,
        *,
        cursor: str | None = None,
        limit: int | None = None,
        options: RequestOptions | None = None,
    ) -> Any:
        return self._client._request(
            "GET",
            "/v1/catalog/operations",
            params={"cursor": cursor, "limit": limit},
            options=options,
        )

    def repair_operation(self, operation_id: str, *, options: RequestOptions | None = None) -> Any:
        return self._client._request(
            "POST",
            "/v1/catalog/operations/repair",
            body={"operation_id": operation_id},
            options={"retry": True, **(options or {})},
        )

    def repair_indexes(self, body: Body, *, options: RequestOptions | None = None) -> Any:
        return self._client._request(
            "POST",
            "/v1/catalog/indexes/repair",
            body=body,
            options={"retry": True, **(options or {})},
        )


class AccessNamespace:
    """Mint and revoke read-only credentials as an environment administrator."""

    def __init__(self, client: _BaseLbbClient) -> None:
        self._client = client

    def create_read_token(self, body: Body, *, options: RequestOptions | None = None) -> Any:
        return self._client._request("POST", "/v1/access/tokens", body=body, options=options)

    def revoke_token(self, credential_id: str, *, options: RequestOptions | None = None) -> Any:
        return self._client._request(
            "POST",
            "/v1/access/tokens/revoke",
            body={"credential_id": credential_id},
            options={"retry": True, **(options or {})},
        )
