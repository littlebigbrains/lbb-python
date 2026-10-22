"""The samples of values-extract, for ``python -m values_extract try``: six
invented product data sheets of office furniture and small appliances
(``samples/records.json``), committed as instances of the class the settings
name, each with its text.
The same samples as the TypeScript template's."""

from __future__ import annotations

import json
from pathlib import Path

from lbb import AsyncLbbClient
from lbb.workflows import lbb_activities

from .workflow import DEFAULTS, ontology

SAMPLES = Path(__file__).resolve().parent / "samples"


async def load_samples(client: AsyncLbbClient, folder: Path = SAMPLES) -> int:
    """Add the class and its properties, then commit the sample records to the
    client's graph. The commit's idempotency key makes a second load a replay."""
    records = json.loads((folder / "records.json").read_text(encoding="utf-8"))
    await lbb_activities(client).evolve(ontology())
    await client.commit(
        {
            "entity_properties": [
                {
                    "type": DEFAULTS.class_name,
                    "name": record["name"],
                    "properties": {DEFAULTS.text_field: record["text"]},
                }
                for record in records
            ]
        },
        idempotency_key="values-extract-samples",
    )
    return len(records)
