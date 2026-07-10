# little big brain Python SDK

`lbb` is the Python client for a [little big brain](https://littlebigbrain.com) graph server. The
HTTP client (`LbbClient` / `AsyncLbbClient`) is the integration path for
applications; it talks to `lbb-server` with a stack API key (`lbb_sk_test_…`
or `lbb_sk_live_…`) or
single-mode token as a bearer credential — the same surface the TypeScript SDK,
CLI, and MCP server use.

```sh
pip install littlebigbrain   # imports as `lbb` (httpx + pydantic)
```

## HTTP client

```python
from lbb import LbbClient, LbbError

with LbbClient(
    "https://db.eu.littlebigbrain.com",
    api_key="lbb_sk_live_...",
    graph="main",
) as lbb:
    lbb.graph("main").facts.create({
        "triplets": [{
            "source": {"type": "SERVICE", "name": "auth-service"},
            "relation": "WRITES_TO",
            "target": {"type": "DATABASE", "name": "user-db"},
            "confidence": 0.93,
            "evidence": "auth-service writes identity records to user-db",
        }],
    }, idempotency_key="import-2026-06-13")
    lbb.indexes.run(wait=True)
    results = lbb.search.hybrid(
        "which systems store customer identity data",
        top_k=5,
        source="persisted",
        consistency="strong",
        targets=["entities", "assertions"],
    )
    for assertion in results.get("assertions", []):
        print(assertion["relation"]["name"], assertion["score"])
```

Use `client.graph("name")` to scope writes. The regular methods return parsed
JSON dictionaries and raise `LbbError` with `status_code`, `type`, `code`,
`param`, `request_id`, and `doc_url` on a non-2xx response. Use
`raw_request(...)` when you need response metadata. An async client mirrors the
same methods:

```python
from lbb import AsyncLbbClient

async with AsyncLbbClient("https://db.eu.littlebigbrain.com", api_key="lbb_sk_live_...") as lbb:
    state = await lbb.current_state({"entity": {"entity_type": "SERVICE", "name": "auth-service"}})
```

The default per-attempt timeout is 120 seconds (`timeout=...` configures it).
Safe reads and idempotency-keyed writes retry `429`/`5xx` and transport failures
up to twice, honor `Retry-After` with a one-minute cap, and attach generated
idempotency keys to NDJSON/RDF imports unless you provide one.

## Preferred typed surface

The `context`, `ontology`, and `query` namespaces return generated Pydantic
models by default, while the existing flat methods keep their dict-returning
behavior for compatibility:

```python
answer = lbb.context.ask({"question": "what stores identity data?"})
print(answer.answer, answer.citations)

ontology = lbb.ontology.view(counts=True)
rows = lbb.query.structured({"patterns": [], "select": []})

for entity in lbb.entities.iter(fields=["owner", "status"]):
    print(entity.name, entity.attributes or {})
```

`entities.pages(...)` yields typed `ListPage` envelopes; `entities.iter(...)`
yields `EntityExplorerRow` objects directly. Both forms also work with
`AsyncLbbClient` using `async for`.

`raw_request(..., options={...})` accepts per-request `max_retries`, `timeout`,
and `headers`. Its response exposes `attempts`, `retry_count`, and `elapsed_ms`.
Typed `context`, `ontology`, and `query` reads accept the same `options=` keyword
and automatically classify read-only POSTs as retry-safe.
Pass native `httpx` `event_hooks` to the client constructor for request/response
instrumentation.

Primary methods: `graph("main").facts.create`; `search.hybrid`;
`indexes.run`; `entities.list`; `entities.filter_by_attributes`;
`context.ask` / `context.suggest` / `context.resolve` / `context.decode` /
`context.groundability`; `ontology.view` / `ontology.search` /
`ontology.resolve`; `query.sparql` / `query.structured` / `query.analytics` /
`query.shacl` / `query.infer` / `query.premises`;
`schema.view` / `schema.preview` /
`schema.publish` / `schema.audit`; plus lower-level `graph_search`,
`multi_search`, `full_text_search`, `embedding_search`, `traverse`,
`semantic_traverse`, `current_state`, `history`, `why`, `shacl`,
`sparql` (SPARQL text → parsed results), `sparql_select` (structured
SELECT/ASK/aggregate — GROUP BY entity, property, or date bucket), `analytics`,
`ontology_view` (pass `counts=True` for per-relation `edge_count`),
`ontology_search`, `ontology_resolve`, `search_feedback` (grade results 3/1/0 →
customer qrels for embedding fine-tuning) and `search_feedback_export`,
`graph_edges`, `index_build`, `index_run`, `index_delta`, `index_gc`, `compact`,
`status`, `metadata`, and `summary`.

## Typed responses

The dict-returning methods are stable for scripts and notebooks. For app code
that wants generated Pydantic models and IDE autocompletion, use the matching
`*_model` or `*_page` helper:

```python
from lbb.models import GraphSummaryResponse, SchemaBundleView

summary: GraphSummaryResponse = lbb.summary_model()
schema: SchemaBundleView = lbb.schema.view_model()

entities = lbb.entities.list_page(fields="*")
for row in entities.data:
    print(row.name, row.attributes or {})

raw = lbb.raw_request("GET", "/v1/graph/summary")
typed = raw.model(GraphSummaryResponse)
```

The preferred namespaces above return models directly. Additional typed helpers
cover the remaining high-use surfaces: `commit_model`, `commit_dry_run_model`, `graph("main").facts.create_model`,
`graph("main").retract_model`, `summary_model`, `metadata_model`,
`list_graphs_model`, `ontology_view_model`, `ontology_conformance_model`,
`sparql_select_model`, `schema.view_model`, `schema.preview_model`,
`schema.publish_model`, `schema.audit_model`, `entities.list_page`,
`entities.filter_by_attributes_model`, and `graph_edges_page`.

Async clients expose the same helpers as awaitables:

```python
summary = await lbb.summary_model()
page = await lbb.entities.list_page(fields=["title", "status"])
```

## SPARQL

`client.sparql(query)` runs SPARQL 1.1 text (SELECT or ASK) through the
conformant engine and returns a parsed `SparqlResults` — no manual
`json.loads` of a results string:

```python
results = lbb.sparql("""
    SELECT ?service ?db WHERE {
        ?service <https://littlebigbrain.com/r/writes_to> ?db
    } LIMIT 10
""")

print(results.vars)           # ['service', 'db']
for row in results:           # iterates flat {var: value} dicts
    print(row["service"], "->", row["db"])

answer = lbb.sparql("ASK { ?s ?p ?o }").boolean   # True / False
```

`SparqlResults` exposes `.vars`, `.boolean` (for ASK), `.bindings` (the raw
typed term objects), `.rows()` (flattened `{var: lexical_value}` dicts, also
what iteration yields), and `.row_page`. Engine extensions are keyword args:
`reason=True` folds rule-derived edges, `entailment="none"` disables the
default `rdfs:subClassOf` closure, and `as_of_valid_time` / `as_of_commit_seq`
pin a snapshot. For the structured BGP form, `client.sparql_select(body)` posts
a `SparqlSelectRequest` and returns the typed `vars`/`solutions`/`groups`
response.

For app code that already has relation patterns but just needs typed attribute
predicates, `client.entities.filter_by_attributes(...)` builds the structured
SPARQL filter body without exposing RDF property IRIs:

```python
lbb.entities.filter_by_attributes(
    patterns=[{"subject": {"var": "service"}, "predicate": "WRITES_TO", "object": {"var": "db"}}],
    where=[{"field": "slo", "op": "ge", "value": 0.99}, {"var": "db", "field": "tier", "value": "prod"}],
    select=["service"],
)
```

A standalone stack also serves the **native SPARQL 1.1 Protocol** at `/sparql`
(`GET ?query=`, `POST` form or `application/sparql-query` body,
`Accept`-negotiated JSON/XML/CSV/TSV) so off-the-shelf SPARQL clients — YASGUI,
Protégé, RDFLib's `SPARQLWrapper` — connect directly; `client.sparql()` returns
parsed JSON rows for in-process use.

Bodies may be plain `dict`s or instances of the generated Pydantic models in
`lbb.models` (e.g. `lbb.models.SemanticGraphSearchRequest`), which are
generated from the committed [`contracts/openapi.json`](https://github.com/littlebigbrains/lbb-python/blob/main/contracts/openapi.json).

## Local client (`lbb.local`)

`LbbLocalClient` shells out to `lbb-testctl` and operates directly on object
storage — for tests, notebooks, and local demos, not application integration.
It requires a full little big brain **engine** checkout (`lbb-testctl` is
compiled with cargo), not just this SDK repository; point `repo_root` at that
checkout. It is re-exported as `from lbb import LbbLocalClient`.

```python
from lbb import LbbLocalClient

client = LbbLocalClient(root="/private/tmp/lbb-python", tenant="acme", graph="main", branch="main",
                          repo_root="/path/to/lbb")  # engine checkout, not this repo
client.create_graph()
client.commit_triplets_file("/path/to/lbb/database/examples/triplets/semantic_graph.json")
```

It also exposes `build_embedding_index`, `embedding_index_inspect`,
`embedding_search`, `ontology_search`, `ontology_resolve`, `semantic_search`,
`traverse`, `current_state`, and `relationship_history`. See
[`examples/embedding_search.py`](examples/embedding_search.py).

## Develop

```sh
python3 -m venv .venv && .venv/bin/pip install -e ".[dev]" httpx pydantic
ruff check lbb tests
mypy lbb
pytest tests --cov=lbb
python -m build && twine check dist/*
```

`lbb/models.py` is generated; the `contracts` CI job regenerates it (and the
OpenAPI spec and TS types) and fails if the committed output drifts. The wheel
ships `py.typed`; generated models and all hand-written modules pass strict
mypy before release. Do not edit `models.py` by hand; maintainers regenerate it
from the canonical private monorepo and sync the result with the OpenAPI contract.
