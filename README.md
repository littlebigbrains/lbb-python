# littlebigbrain Python SDK

Python client for [little big brain](https://littlebigbrain.com), a search
platform for AI applications such as chatbots, search tools, and agents.
Load facts, query their relationships, and keep the data version behind an answer
so you can check it later.

The package supports Python 3.10+ and provides synchronous and asynchronous
clients built on `httpx`. Generated Pydantic models are available in `lbb.models`.

[Documentation](https://docs.littlebigbrain.com/sdks/python/) ·
[Quickstart](https://docs.littlebigbrain.com/start/quickstart/) ·
[Issues](https://github.com/littlebigbrains/lbb-python/issues)

## Install

```sh
pip install littlebigbrain
```

The package is installed as `littlebigbrain` and imported as `lbb`.

## Load facts and run a query

Create a stack in the [console](https://cloud.littlebigbrain.com) and open
**Connect**. Copy its complete endpoint and a stack API key:

```sh
export LBB_URL="https://<your-complete-stack-host>"
export LBB_API_KEY="<your-stack-api-key>"
```

This example creates a graph named `quickstart` on its first write. It stores
three facts: a service writes to a database, and each has a label. The data uses
Resource Description Framework (RDF), where each line names a subject, a
relationship, and a value or another record. SPARQL is the query language for
those facts.

Save as `quickstart.py`, then run `python3 quickstart.py`:

```python
import os

from lbb import LbbClient

facts = """
<https://example.org/auth-service> <https://example.org/writesTo> <https://example.org/user-db> .
<https://example.org/auth-service> <http://www.w3.org/2000/01/rdf-schema#label> "Auth Service" .
<https://example.org/user-db> <http://www.w3.org/2000/01/rdf-schema#label> "User Database" .
"""

query = """
    SELECT ?service ?database WHERE {
        ?s <https://example.org/writesTo> ?db .
        ?s <http://www.w3.org/2000/01/rdf-schema#label> ?service .
        ?db <http://www.w3.org/2000/01/rdf-schema#label> ?database .
    } ORDER BY ?service ?database LIMIT 10
"""

with LbbClient(
    os.environ["LBB_URL"],
    api_key=os.environ["LBB_API_KEY"],
    graph="quickstart",
) as lbb:
    imported = lbb.graph("quickstart").facts.import_rdf(
        facts, format="ntriples", idempotency_key="sdk-quickstart-v1"
    )

    results = lbb.sparql(
        query,
        consistency="strong",
        min_indexed_seq=imported["committed_commit_seq"],
    )
    for row in results:
        print(f"{row['service']} -> {row['database']}")
```

On a fresh graph, this prints:

```text
Auth Service -> User Database
```

The query follows the stored relationship between the service and database.
`consistency="strong"` makes the new facts available to this read without
waiting for a background index job. Reads default to eventual consistency, so
omit this option only when an earlier version is acceptable.

The idempotency key makes repeating the same import safe. Use a new key if you
change the data.

## Async client

`AsyncLbbClient` provides the same methods with `await`. After running the
quickstart, this script reads its data:

```python
import asyncio
import os

from lbb import AsyncLbbClient


async def main():
    async with AsyncLbbClient(
        os.environ["LBB_URL"],
        api_key=os.environ["LBB_API_KEY"],
        graph="quickstart",
    ) as lbb:
        result = await lbb.sparql(
            "ASK { ?s <https://example.org/writesTo> ?db }",
            consistency="strong",
        )
        print(result.boolean)  # True


asyncio.run(main())
```

## Next steps

- [Search by meaning](https://docs.littlebigbrain.com/guides/search-by-meaning/): choose which facts to embed and find records from a text description.
- [Load your own RDF](https://docs.littlebigbrain.com/guides/load-rdf/): import Turtle, N-Triples, N-Quads, or TriG.
- [Work with JSON records](https://docs.littlebigbrain.com/guides/without-rdf/): define a schema and write records without writing RDF.
- [Validate writes](https://docs.littlebigbrain.com/guides/sparql-and-shacl/): define constraints with the Shapes Constraint Language (SHACL).
- [Read the same version again](https://docs.littlebigbrain.com/guides/time-travel-audit/): save a query and its commit sequence, then replay it through the HTTP API.

The RDF and JSON guides use different write workflows. Choose one when creating
a graph; a graph first written through RDF import does not accept
`facts.create` or JSON record imports.

## Errors and retries

Failed HTTP requests raise `LbbError`, with a status, error code, message, and
request ID. Use `raw_request()` when you also need response headers or timing.

Safe reads and writes with an idempotency key retry rate limits, retryable server
errors, and network failures. Retries respect `Retry-After` and use a 60-second
budget by default. See the [client reference](https://docs.littlebigbrain.com/sdks/python/)
for timeout and retry options.

## Development

From a clone of this repository:

```sh
python3 -m venv .venv
.venv/bin/pip install -e ".[dev]"
.venv/bin/python -m ruff check lbb tests
.venv/bin/python -m mypy lbb
.venv/bin/python -m pytest tests
```

`lbb/models.py` is generated from the API contract. See
[CONTRIBUTING.md](CONTRIBUTING.md) for changes to generated models.

## License

[Apache-2.0](LICENSE).
