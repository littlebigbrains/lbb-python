# Changelog

All notable changes to the `littlebigbrain` Python SDK are documented here.

## 0.13.0 (2026-08-31)

Additive release covering the schema-observability surface that landed since
0.12.0.

- New `schema_summary()` / `schema_summary_model()`: the compact observed RDF schema attached to the
  immutable published base (`GET /v1/graph/schema-summary`), with class
  populations, resource- and literal-valued predicate counts
  (`resource_predicate_counts` / `literal_predicate_counts`; the literal field
  is `null` until a summary artifact written by a current server exists), and
  bounded OWL/RDFS statements.
- New `publication_status()` / `publication_status_model()` and `wait_for_published(target_seq)`: the automatic
  RDF publication lifecycle (`PublicationStatusResponse`), available before
  the first generation exists, and a bounded poll until background
  reconciliation folds a commit into the published base.
- New `import_rdf_many()`: multi-document RDF import in one call.
- Ontology and schema views carry each class's frozen `stable_id`, canonical
  query `iri`, and direct `super_types`; the evolve surface gains
  `AddSuperTypesOp` model; ontology define accepts `dry_run`.
- Server side, defining Turtle/RDF/JSON-LD ontologies now imports
  `owl:DatatypeProperty` declarations as typed property fields instead of
  relations spanning every class. No client change is needed; `property_defs`
  simply carries the imported fields.

## 0.12.0 (2026-08-24)

Breaking removal of request-time SHACL models that had no supported client or
server operation.

- Remove `ShaclQueryRequest`, `ShaclNodeShape`, `ShaclValidationReport`,
  `ShaclViolation`, and the other retired `Shacl*` generated model classes.
- Publish RDF SHACL shapes with `schema.publish`, then inspect the durable audit
  with `ontology.conformance`. There is no one-shot `/v1/query/shacl` route.

## 0.11.1 (2026-08-22)

- `create_graph` now creates the scoped graph with an empty ontology. The
  built-in AI-context vocabulary is opt-in through `ontology.define` with
  `merge_default=True`.
- `ontology.define` is safe to rerun on an existing graph: identical
  definitions are no-ops, additive differences are applied, and its response
  reports `graph_created`, `changed`, and the applied `changes`.
- Treat an absent first published generation as normal asynchronous build
  progress instead of retrying the metadata request until the generic retry
  budget is exhausted.
- Give synchronous and asynchronous publication waiters their own explicit
  deadline and continue through retryable metadata responses, including `429`,
  without multiplying nested retry loops.
- Determine readiness from the published generation and served RDF watermark,
  so RDF-only production deployments do not wait for removed search families.

## 0.11.0 (2026-08-21)

Breaking removal of every non-SPARQL query surface. The server now serves
SPARQL as its only query surface, so the client keeps only the SPARQL methods.

- Remove the `client.search` namespace, including the callable
  `client.search(...)` shortcut and `client.search.hybrid(...)`.
- Remove the `client.context` namespace (`suggest`, `resolve`, `decode`,
  `groundability`) from the sync and async clients.
- Remove `graph_search`, `multi_search`, `full_text_search`,
  `embedding_search`, `vocab_export`, and `analytics` from the client, plus
  `query.analytics` from the query namespace.
- Remove the managed embedding family from the client and from
  `client.graph(...)`: `embedding_config`, `embedding_models`,
  `set_embedding_model`, `set_embedding_config`, `backfill_embeddings`,
  `submit_embedding_backfill`, `embedding_backfill_job`,
  `cancel_embedding_backfill`, and `promote_embedding`.
- The removed methods took their generated request/response models with them,
  since the routes left the contract.
- Keep `sparql`, `sparql_select`, `sparql_select_model`, `query.structured`,
  and `query.sparql`. Keep the temporal reads (`current_state`, `history`,
  `why`), the entity reads, relevance feedback, and every write, ontology,
  schema, branch, and operations surface.

## 0.10.0 (2026-08-21)

Breaking removal of the standalone graph-traversal surface.

- Remove sync, async, and local `traverse` / `semantic_traverse` methods and
  their request/response models.
- Entity neighborhoods and class samples now read the published Base family.
- Use SPARQL 1.1 property paths for exact multi-hop graph queries; semantic
  search continues to expose bounded graph-path evidence internally.

RDF import.

- `import_rdf`'s server-side `batch` default changed from 1,000 statements to
  the 1,000,000 cap — one internal commit per fully-buffered request. Pass an
  explicit `batch` to opt back into smaller internal commits.
- `import_rdf` accepts `build`; pass `build=False` on every chunk except the
  last of a chunked bulk stream to defer the published-generation enqueue so the
  derived families build once at the final head.
- Drop the phantom `publish` query param from the generated import operations —
  the server never read it.

## 0.9.1

- Sync and async durable import submissions now reject an empty iterable before
  issuing the import POST.
- The one-record preflight preserves streaming and one-shot iterator semantics.

## 0.9.0

Durable, asynchronous NDJSON imports.

- Sync and async clients add `submit_import_ndjson`, `get_import_job`,
  `cancel_import_job`, and `wait_for_import_job`.
- Submissions consume iterable/async-iterable input as a streaming HTTP body,
  require an explicit idempotency key, and never fall back to the synchronous
  import route.
- Durable methods fail clearly unless the server advertises
  `durable_import_jobs_v1`.

## 0.8.1

Adjacency-backed Explorer reads now report the coherent adjacency coverage
watermark instead of failing while a published run trails graph head. The
generated `SnapshotView` model documents `stale_reason="adjacency_coverage"`
and the append-safe WAL-prefix semantics.

## 0.8.0

Eventual-by-default read consistency and the read-your-writes floor.

### ⚠️ Behavior change — default read consistency is now `eventual`

The server's default read consistency flipped from `strong` to `eventual`
(server-side change; this SDK forwards `consistency` unchanged). A read that
does not specify `consistency` now serves the last **published** index/dataset
state at its watermark (surfaced on `snapshot.served_at_seq` with
`stale_reason="eventual_consistency"`) rather than folding the un-indexed WAL
tail up to head. **Code that relied on the implicit `strong` default for
read-after-write must either pass `consistency="strong"` or — preferably — use
the new `min_indexed_seq` floor below.**

### Read-your-writes floor (`min_indexed_seq`)

- Read methods on the search / SPARQL / summary surfaces accept `consistency=`
  and `min_indexed_seq=` keyword arguments. Take the committed sequence a write
  returned and read with `min_indexed_seq` set to it:

  ```python
  commit_seq = client.commit(triplets)["commit_seq"]
  rows = client.sparql(query, min_indexed_seq=commit_seq)
  ```

  Under the eventual default, a floor not yet covered by published state raises a
  retryable `read_your_writes_pending` `429` (with `Retry-After`) so a sync
  pipeline can poll for its own write instead of reading a stale answer.
- **Client-level default.** `LbbClient(…, default_consistency="strong")` sets the
  consistency used when a call omits it; a per-call `consistency=` still wins.

## 0.6.1

Composite stack endpoints: hosted stacks are addressed by their own
`endpoint_url`, and a misroute is surfaced with actionable guidance instead of
being retried away.

### Endpoints

- **Hosted `base_url` is the stack `endpoint_url`.** Pass the exact value shown
  on the stack's Connect page
  (`https://<tenant-short-id>--<stack-slug>.db.eu.littlebigbrain.com`). Omitting
  `base_url` still retains the loopback default for local/self-hosted
  development; graph and branch stay ordinary client scope parameters.
- **Actionable routing hints.** `LbbError.endpoint_hint` carries copy-paste
  guidance for the composite-endpoint error codes `stack_endpoint_required`
  (HTTP `421`) and `stack_endpoint_mismatch` (HTTP `403`).

### Retry behavior

- **`421`/`403` are terminal.** Misdirection (`421`) and authorization (`403`)
  failures surface immediately — they were never retryable by status (only
  `429`/`5xx` are), and a test now pins that so the actionable `endpoint_hint`
  is never masked by retries.

## 0.6.0

Honest, deadline-bounded retries — so server-side backpressure stays invisible
to your code under sustained overload, not just a single blip.

### Server contract

- **Pressure ⇒ 429.** The server now returns `429` for every retryable
  pressure/throttle class, including the graph-scoped `ingest_busy` code (WAL
  backpressure, commit contention, busy full build) that previously came back as
  `503`. `storage_degraded` (a genuine storage-dependency outage) stays `503`.
  The SDK already retried both `429` and `5xx`, so this is **not wire-breaking** —
  existing retry behavior is unchanged; the class is just tidier.

### Retry behavior

- **Honors the server's typed body verdict.** A terminal error marked
  `retryable: false` in the body (e.g. an exhausted quota) is now surfaced
  immediately instead of being retried, and the body's `retry_after_seconds`
  hint is used for the backoff when no `Retry-After` header is present.
- **Full-jitter exponential backoff** replaces the old linear delay, so many
  clients recovering from one outage no longer retry in lockstep.
- **Deadline-based retry budget.** New `retry_budget_ms` (default `60_000`) is
  the binding limit: idempotent operations keep retrying until the budget
  elapses, so a multi-second advertised `Retry-After` window is actually
  honored. `max_retries` remains a secondary safety cap and its default is
  raised `2 → 6` so a Retry-After sequence fits inside the budget.
- **Naked load-balancer `5xx`** (a bare `502/503/504` with an HTML body and no
  error envelope) is explicitly treated as a transient, retryable
  server-busy-equivalent with backoff.
- **Absorbed retries are observable.** New optional `on_retry` client callback
  receives a `RetryEvent` (`attempt`, `status_code`, `error_code`,
  `delay_seconds`, `elapsed_ms`) before each backoff sleep; `RawLbbResponse`
  continues to carry `attempts` / `retry_count` / `elapsed_ms`.

All additions are backward-compatible: new optional keyword arguments
(`retry_budget_ms`, `on_retry`) and a new exported `RetryEvent` type.
