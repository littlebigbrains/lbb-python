# little big brain Python SDK

The Python client for little big brain. Use it to ingest records, build BM25 +
vector + graph indexes, search with authorization filters, traverse the graph,
and turn retrieval feedback into training data.

```sh
pip install littlebigbrain
```

## Five-minute start

```python
from lbb import LbbClient

with LbbClient(
    "https://db.eu.littlebigbrain.com",
    api_key="lbb_sk_live_...",
    graph="main",
) as lbb:
    graph = lbb.graph("main")

    graph.facts.create({
        "triplets": [{
            "source": {"type": "CONCEPT", "name": "handbook", "key": "doc:42"},
            "relation": "RELATED_TO",
            "target": {"type": "CONCEPT", "name": "vacation policy", "key": "passage:42:1"},
            "evidence": "Employees receive 25 days of annual leave.",
        }],
    }, idempotency_key="doc:42:v1")

    lbb.indexes.run(wait=True)
    results = lbb.graph_search({
        "query": "how much annual leave do employees get?",
        "targets": ["entities"],
        "top_k": 10,
    })
```

Stack keys belong on a server, worker, or secret-backed notebook—not in a
browser bundle. Safe reads and idempotency-keyed writes retry transient errors
and honor `Retry-After`.

## Enterprise-search integration

For an enterprise-search retrieval adapter, keep the application database for
users, connectors, tasks, and migration cursors. Put searchable documents,
passages, graph facts, embeddings, BM25/ANN/adjacency indexes, ontology review,
and retrieval feedback in LBB.

The production sequence is:

1. map a connector batch to stable-keyed document, passage, provenance, and edge records;
2. call `graph.facts.import_ndjson(..., index=False, idempotency_key=...)`;
3. submit one durable index job with `index_submit`, then reconnect with `index_job`;
4. translate the product's ACL/scope filter into native set `overlaps` filters;
5. call `graph_search` with projected fields and hydrate only the final top-k;
6. submit grade-3 feedback for sources cited in the grounded answer;
7. evolve ontology changes through draft → validate → promote/reject.

For an LLM query planner, use `lbb.context.suggest(...)` to fill grounded
schema/value prefixes and `lbb.context.resolve(...)` to snap free-text guesses
onto real vocabulary. `resolve` uses the graph's managed embeddings when
configured. Record adopted suggestions and accepted/rejected/corrected plans so
the feedback can train a smaller planner and suggest ranker.

See the complete [enterprise-search integration guide](https://docs.littlebigbrain.com/guides/enterprise-search/)
for the record model, migration plan, acceptance gates, and capability mapping.

## Useful surfaces

```python
# Bulk ingestion (flat or generated typed property values, including sets).
graph.facts.import_ndjson(records, strict=True, index=False,
                          idempotency_key="connector:batch:17")

# Durable indexing and training.
job = lbb.index_submit({}, idempotency_key="index:head:147")
status = lbb.index_job(job.job_id)
train = lbb.train_submit({"kind": "fusion", "force": True},
                         idempotency_key="fusion:gate:7")
progress = lbb.train_job(train.job_id).progress

# Typed namespaces.
answer = lbb.context.ask({"question": "what changed?"})
ontology = lbb.ontology.view(counts=True)
rows = lbb.query.sparql({"query": "SELECT ?s WHERE { ?s ?p ?o }"})

# Cursor-safe iteration.
for entity in lbb.entities.iter(fields=["text", "acl"]):
    print(entity.name, entity.attributes)
```

`LbbClient` and `AsyncLbbClient` expose the same capabilities. Preferred
namespaces return generated Pydantic models; compatibility helpers return
parsed dictionaries. `LbbError` includes HTTP status, structured code,
parameter, request ID, and documentation URL. `raw_request(...)` exposes
attempt count, elapsed time, response headers, build commit, and replica.

## Major capability areas

- `graph(...).facts`: commit, dry-run, retract, NDJSON/RDF import
- `search` / `graph_search`: lexical, BM25, vector, hybrid, filters, facets
- `indexes`: full build, durable submit/status, delta, garbage collection
- `entities`: projected reads, native filtering, cursor-safe iteration
- `ontology` / `schema`: define, evolve, induce, draft review, SHACL lifecycle
- `query`: SPARQL, structured query, analytics, SHACL, inference, conflicts
- `context`: grounded ask, suggest, resolve, decode, groundability
- feedback/training: labels, export/summary, durable trainer jobs and progress;
  typed suggestion/planner supervision helpers with validation before transport,
  automatic idempotency keys, and durable receipt/trainability acknowledgements
- temporal graph: traversal, state, history, lineage, snapshot pins

Generated models come from the bundled OpenAPI contract and are available in
`lbb.models`.

## Develop

```sh
python3 -m venv .venv
.venv/bin/pip install -e ".[dev]" httpx pydantic
ruff check lbb tests
mypy lbb
pytest tests
```

`lbb/models.py` is generated. Change the Rust API types and regenerate clients
instead of editing it by hand.
