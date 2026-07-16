# littlebigbrain — Python SDK

The Python client for [Little Big Brain](https://littlebigbrain.com) — write graph facts, build indexes, and run hybrid search over one snapshot. Built on `httpx` + `pydantic`; ships sync and async clients.

```sh
pip install littlebigbrain   # imports as `lbb`
```

## Quickstart

```python
from lbb import LbbClient

with LbbClient("https://db.eu.littlebigbrain.com", api_key="lbb_sk_live_...") as lbb:
    graph = lbb.graph("main")

    # 1. Write a fact.
    graph.facts.create({
        "triplets": [{
            "source": {"type": "CONCEPT", "name": "handbook", "key": "doc:42"},
            "relation": "RELATED_TO",
            "target": {"type": "CONCEPT", "name": "vacation policy", "key": "passage:42:1"},
            "evidence": "Employees receive 25 days of annual leave.",
        }],
    }, idempotency_key="doc:42:v1")

    # 2. Build persisted BM25 + vector + adjacency indexes and wait.
    lbb.indexes.run(wait=True)

    # 3. Hybrid search over the snapshot.
    results = lbb.search.hybrid("how much annual leave do employees get?", top_k=5)
    for hit in results.get("assertions", []):
        print(hit["relation"]["name"], hit["score"])
```

Facts are graph-scoped (`lbb.graph("main").facts`); indexes and search are client-level (`lbb.indexes`, `lbb.search`) and use the stack's default graph.

## Examples

**Search with filters.** Use the request body to filter before ranking — here, only facts an ACL principal may see:

```python
results = lbb.graph_search({
    "query": "incident response runbook",
    "targets": ["entities"],
    "search": {
        "filters": {
            "op": "overlaps",
            "field": "acl",
            "values": ["user:rino@example.com", "group:engineering"],
        },
    },
    "top_k": 20,
})
```

**Bulk import.** Load many records as NDJSON in one call:

```python
lbb.graph("main").facts.import_ndjson(
    [
        {"source": {"type": "DOC", "name": "handbook", "key": "doc:42"},
         "relation": "HAS_PASSAGE",
         "target": {"type": "PASSAGE", "name": "leave-policy", "key": "p:42:1"}},
        # …one record per line
    ],
    idempotency_key="handbook-batch-1",
)
```

**Time-travel read.** Pin a SPARQL query to a past instant — results reflect the graph as it was then:

```python
results = lbb.sparql(
    "SELECT ?s ?p ?o WHERE { ?s ?p ?o } LIMIT 10",
    as_of_valid_time="2026-01-01T00:00:00Z",
)
print(results.vars)
for row in results:           # iterates flat {var: value} dicts
    print(row)
```

The async client mirrors every method — `async with AsyncLbbClient(...) as lbb:` and `await` each call.

## Errors & retries

Methods return parsed dictionaries and raise `LbbError` (with `status_code`, `code`, `param`, `request_id`, and `doc_url`) on any non-2xx response. Safe reads and idempotency-keyed writes retry `429`/`5xx` and transport failures with full-jitter backoff, bounded by a retry budget (`retry_budget_ms`, default 60s) rather than a fixed count, and honor `Retry-After` — a terminal error the server marks non-retryable surfaces immediately. Use `raw_request(...)` for response headers, request id, and retry/timing metadata.

## More

Beyond the quickstart: `entities.iter(...)` for cursor-safe iteration, `context.ask(...)` for grounded answers, `ontology`/`schema` for the SHACL lifecycle, durable index and training jobs (`index_submit`/`index_job`), managed embeddings, traversal, and temporal history. Typed Pydantic responses are available via the matching `*_model` / `*_page` helpers; generated models live in `lbb.models`.

Full reference and guides: [docs.littlebigbrain.com/sdks/python](https://docs.littlebigbrain.com/sdks/python/).

## Develop

```sh
python3 -m venv .venv
.venv/bin/pip install -e ".[dev]"
ruff check lbb tests
mypy lbb
pytest tests
```

`lbb/models.py` is generated from the API contract — change the Rust API types and regenerate rather than editing it by hand.
