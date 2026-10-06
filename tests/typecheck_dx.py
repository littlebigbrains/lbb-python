"""Static-only contracts for the preferred typed SDK surface."""

from __future__ import annotations

from collections.abc import AsyncGenerator, Generator
from typing import TYPE_CHECKING

from lbb import AsyncLbbClient, LbbClient, QueryAskResult, QueryAskStreamEvent
from lbb.models import OntologyView, SparqlSelectResponse

if TYPE_CHECKING:
    from typing import assert_type

    def sync_dx_types(client: LbbClient) -> None:
        assert_type(client.ontology.view(counts=True), OntologyView)
        assert_type(
            client.query.structured({"patterns": [], "select": []}),
            SparqlSelectResponse,
        )
        assert_type(client.query.ask("Which services exist?"), QueryAskResult)
        assert_type(
            client.query.ask("Which services exist?", mode="route"), QueryAskResult
        )
        assert_type(
            client.query.ask_stream("Which services exist?"),
            Generator[QueryAskStreamEvent, None, None],
        )

    async def async_dx_types(client: AsyncLbbClient) -> None:
        assert_type(await client.ontology.view(counts=True), OntologyView)
        assert_type(
            await client.query.structured({"patterns": [], "select": []}),
            SparqlSelectResponse,
        )
        assert_type(await client.query.ask("Which services exist?"), QueryAskResult)
        assert_type(
            client.query.ask_stream("Which services exist?"),
            AsyncGenerator[QueryAskStreamEvent, None],
        )
