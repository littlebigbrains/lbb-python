# littlebigbrain — Python SDK

The Python client for [Little Big Brain](https://littlebigbrain.com) — write graph facts and query one immutable published snapshot. Built on `httpx` + `pydantic`; ships sync and async clients.

```sh
pip install littlebigbrain   # imports as `lbb`
```

## Quickstart

```python
from lbb import LbbClient

with LbbClient(
    "https://0abc1def--production.db.eu.littlebigbrain.com",
    api_key="lbb_sk_live_...",
    graph="main",
) as lbb:
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

    # 2. Publication is automatic. Inspect one coherent watermark when needed.
    published = lbb.read_snapshot_model()
    print(published.snapshot.served_at_seq, published.query_lag_commits)

    # 3. Query the snapshot with SPARQL.
    rows = lbb.sparql_select(
        "SELECT ?s ?o WHERE { ?s <policy:annual_leave> ?o } LIMIT 5"
    )
    for row in rows:
        print(row["s"], row["o"])
```

For hosted use, pass the exact `endpoint_url` shown on the stack's Connect
page. Omitting `base_url` retains the loopback default for local/self-hosted
development only; graph and branch remain ordinary client scope parameters.

Facts are graph-scoped (`lbb.graph("main").facts`); search and published-snapshot
inspection use the client's active graph/branch scope.

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

For large or long-running loads, submit a streamed durable job:

```python
accepted = lbb.submit_import_ndjson(
    records(),
    idempotency_key="hubspot:portal-42:run-2026-07-29",
)
completed = lbb.wait_for_import_job(accepted.job_id)
print(completed.state, completed.committed_commit_seq)
if completed.committed_commit_seq is not None:
    lbb.wait_for_index_lineage(completed.committed_commit_seq.root)
```

The async client accepts an async iterable as well. Success means all grouped
commits are durable and final publication was enqueued; it does not mean
published indexes have already reached `committed_commit_seq`. Wait once after
the final commit, not after each source row or chunk. The lineage waiter polls
normal `index_caught_up=false` metadata until its own deadline, including on an
RDF-only deployment. Empty iterables are rejected locally before an import POST
is sent.

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
`wait_for_index_lineage(...)` is a separate deadline-bounded poller, so the
generic request retry-count cap cannot end publication waiting early.

## More

Beyond the quickstart: `entities.sample(type=..., limit=...)` for a bounded
published-generation sample and `entities.filter_by_attributes(...)` for
relation-bound structured SPARQL; and `ontology`/`schema` for ontology
inspection and atomic schema publication. SPARQL is the one query language on
the API. Typed Pydantic responses are exposed by
matching `*_model` helpers; generated models live in `lbb.models`.

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
