"""Static-only contracts for the preferred typed SDK surface."""

from __future__ import annotations

from typing import TYPE_CHECKING

from lbb import AsyncLbbClient, LbbClient
from lbb.models import EntityTypeSampleResponse, OntologyView, SparqlSelectResponse

if TYPE_CHECKING:
    from typing import assert_type

    def sync_dx_types(client: LbbClient) -> None:
        assert_type(client.ontology.view(counts=True), OntologyView)
        assert_type(
            client.query.structured({"patterns": [], "select": []}),
            SparqlSelectResponse,
        )
        assert_type(
            client.entities.sample(type="SERVICE", limit=20),
            EntityTypeSampleResponse,
        )

    async def async_dx_types(client: AsyncLbbClient) -> None:
        assert_type(await client.ontology.view(counts=True), OntologyView)
        assert_type(
            await client.query.structured({"patterns": [], "select": []}),
            SparqlSelectResponse,
        )
        assert_type(
            await client.entities.sample(type="SERVICE", limit=20),
            EntityTypeSampleResponse,
        )
