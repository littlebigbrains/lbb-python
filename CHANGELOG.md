# Changelog

All notable changes to the `littlebigbrain` Python SDK are documented here.

## Unreleased

- `SparqlResults.search` gains `rerank` and `timings.relevance_ms`, for a
  query with `search:rerank true`.

## 0.19.0 (2026-10-04)

Adds the server features the SDK did not cover yet, on `LbbClient` and
`AsyncLbbClient`, questions in plain words (the server turns a question into
a SPARQL query), and the search rerank.

- `sparql()` and `query.sparql()` take `request`, the user's words behind the
  query. The server records an eval trace, and `SparqlResults.trace_id` names
  it.
- `SparqlResults.search` reports how a `search:similarTo` pattern in the query
  ran: the plan, the hits asked for and bound, `complete` and the lag of the
  vectors. It is `None` for a query without a search.
- Add `query.update(text, idempotency_key=None)` for SPARQL Update on the
  `/update` endpoint. The server accepts `INSERT DATA`. The client sends an
  idempotency key, so a retry replays the write.
- Add `entities.detail()` and `entities.detail_model()` for
  `GET /v1/graph/entity`: one record's typed attributes and current links, by
  `id`, by `type` and `name`, or by `type` and `key`. `as_of_commit_seq`
  reads the record at a retained commit.
- `embeddings.search()` takes `rerank`: `True` orders the best hits by the
  managed rerank model (TypeSafe's Jev), and each hit carries its
  `relevance`; `False` keeps the similarity order. Without it the graph's
  search setting decides. The response has a `rerank` report.
- Add `embeddings.search_settings()` and `set_search_settings(rerank=…)` for
  `GET` and `PUT /v1/search/settings`: rerank every search of the graph, or
  none.
- Add `query.rewrite(question, ...)` for `POST /v1/query/rewrite`, on
  `LbbClient` and `AsyncLbbClient`. A router model selects the kind of query,
  and a rewriter model writes it from a description of the graph. With
  `run=True` the server also runs the query and returns the rows in `result`.
  Each call uses model tokens, so the client does not retry a failed call
  unless `options={"retry": True}`. Arguments left at `None` stay off the
  wire.
- Add `query.ask(question, ...)`. It calls `rewrite` with `run=True` and
  returns a `QueryAskResult`: the route, the query, the rationale, the parsed
  `rows` and `vars`, the `boolean` of an `ASK` query, the `snapshot`, the
  `error`, the eval `trace_id`, and the whole `rewrite` response.
- Add `checks` for the model checks of a graph, on `LbbClient` and
  `AsyncLbbClient`: `calls()` and `call(call_id)` read the log of the model
  calls LBB makes for its own work, `check_call(call_id)` asks the judge to
  check one call now, `list()` reads a month of checks,
  `review(call_id, agree=…)` agrees with the judge or corrects it,
  `summary()` sums a month per job and model, and `export()` returns a month
  of checks as a list of parsed JSON lines. `check_call` spends the
  platform's judge budget, so the client does not retry a failed call unless
  `options={"retry": True}`. Arguments left at `None` stay off the wire.
- A response of type `application/x-ndjson` decodes to a list of the lines'
  values.
- `embeddings.set_search_settings()` takes `rerank_depth`, `blend` and
  `probe_factor` next to `rerank`, and every argument is optional. A setting
  left at `None` stays off the wire and keeps its value. Pass the new
  `lbb.RESET` to set one back to its default: the client sends JSON `null`.
- Add `embeddings.search_tuning` with `start()`, `list()`, `get(session_id)`
  and `apply(session_id)` for the `/v1/search/tuning` routes. A session runs
  the graph's own searches with other settings and proposes the best.
  `start()` spends the judge budget and is not retried unless
  `options={"retry": True}`; `apply()` sets the same settings again on a
  retry.

## 0.18.0 (2026-10-03)

Adds `client.integrations`: hosted integrations for a developer's end
customers, one graph per customer, with Google Drive among the connectors.

- Add the `integrations_url` argument, `https://api.littlebigbrain.com` by
  default, on `LbbClient` and `AsyncLbbClient`. The integrations routes take
  the client's `api_key`.
- Add `integrations.connectors()`, `create()`, `list()`, `get()`,
  `set_credentials()`, `set_settings()`, `sync()`, `pause()`, `resume()`,
  `delete()` and `erase()` for the `/v1/integrations/*` routes of
  `contracts/integrations-openapi.json`. Answers are typed dicts from
  `lbb.integrations`. `sync()` sends an `Idempotency-Key`; without
  `idempotency_key` it makes one per call, so its retries queue one sync.
- Add `integrations.suggestions()`, `accept()` and `dismiss()` for a
  connection's ontology suggestions on the stack endpoint. `accept()` with
  `sync=True` then sends the connection a sync under the message id
  `sync-after-<suggestion id>`, and returns an `IntegrationAcceptResult`.
- `LbbError` reads the integrations API's error body: `code`, the message and
  `details`. `retry_after_seconds` comes from the `Retry-After` header when
  the body gives no wait.
- Add `lbb.google_drive` with `authorize_url()`, `exchange_code()` and
  `exchange_code_async()`: the Google consent URL and the code exchange a
  developer's server runs before it creates a `google_drive` connection. The
  exchange returns a `Grant` whose `credentials` hold `GOOGLE_CLIENT_ID`,
  `GOOGLE_CLIENT_SECRET` and `GOOGLE_REFRESH_TOKEN`, and raises
  `GoogleOAuthError`.

## 0.17.0 (2026-10-02)

Adds the ontology starters (`crm`, `documents`, `work`).

- Add `ontology.starters` with `list()`, `get(starter)`,
  `apply(starter, dry_run=False, expected_ontology_version=None)` and
  `update(starter)` for the `/v1/ontology/starters` routes, on `LbbClient` and
  `AsyncLbbClient`. A starter is a versioned base ontology (`crm`,
  `documents`, `work`); `list` and `get` answer for a graph that does not
  exist yet. Add the generated `OntologyStarter*` models.
- `graph(name).ontology` is the ontology namespace scoped to that graph:
  `lbb.graph("main").ontology.starters.apply("crm")`.
- Add `lbb.starters`: the starters' classes, properties, relations and
  competency questions as typed constants with their SPARQL IRIs, for example
  `crm.classes.Organization.iri` and `crm.relations.WORKS_AT.inverse_iri`.
- A suggestion's `change` holds up to 128 operations (was 64).
- `LbbError.details` holds the per-item reasons of a refusal, for example
  the `conflicts` of `409 starter_conflict`.

## 0.16.0 (2026-10-02)

Breaking removal of `ontology.induce()`, whose route is removed from the
server. Model request bodies no longer send `null` for unset optional fields.

- Fix: a Pydantic model request body no longer sends `null` for an optional
  field that is not set. The server rejected `null` for list and boolean
  fields with HTTP 400, for example `WidenRelationOp.add_range` and
  `OntologyEvolveRequest.allow_data_conflicts` on `POST /v1/ontology/evolve`.
  A required field set to `None` still sends `null`, for example
  `WorkflowSignalRequest.value`. Plain `dict` bodies are sent as given.
- Add `model_activity(month=None)` and `model_activity_model(month=None)` on
  `LbbClient` and `AsyncLbbClient` for `GET /v1/models/activity`. They read
  what each managed model did for the stack in one month (`yyyy-mm`, UTC;
  the current month by default). Add the generated `ModelActivityResponse`
  models.
- `WorkflowInstance` gains `history_pruned_through`: turns up to it were
  removed by the server's history retention. Reading one answers 404, and a
  message id whose turn was removed is admitted again as a new message.
- Add `workflow_delete_instance(workflow_id)` on `LbbClient` and
  `AsyncLbbClient` for `POST /v1/workflows/instances/delete`. It deletes a
  message workflow instance with its turns and history and returns
  `WorkflowInstanceDeleteResponse`.
- Add `profile=True` to `sparql()` on `LbbClient`, `AsyncLbbClient` and their
  `query` namespaces. `SparqlResults.profile` holds the server's measurements:
  timings, reads, plan counters and the join order with estimates.
- Add `planner_stats(cursor=None, limit=None)`, `planner_stats_model()` and
  `graph(name).planner_stats()` for `GET /v1/graph/planner-stats`, and the
  generated `PlannerStatsResponse` and `SparqlQueryProfile` models.
- Add `ontology.suggestions()`, `suggestion_get()`, `suggestion_create()`,
  `suggestion_validate()`, `suggestion_accept()`, `suggestion_dismiss()`,
  `suggestion_supersede()` and `suggestion_comment()` for the
  `/v1/ontology/suggestions` routes, on `LbbClient` and `AsyncLbbClient`.
- Remove `ontology.induce()` from `LbbClient` and `AsyncLbbClient`, and the
  induction models. The route answered `429` on every graph and is removed
  from the server.

## 0.15.0 (2026-09-26)

Breaking removal of the Base-family reads. Their routes answered
`429 ingest_busy` on every graph. No publication job writes the Base read root
they need. The server now answers 404 and names the replacement.

- Remove `current_state()`, `history()` and `why()` from `LbbClient` and
  `AsyncLbbClient`. Read the graph at a past commit with SPARQL and
  `as_of_commit_seq`.
- Remove `governed_conflicts()` and `query.conflicts()`.
- Remove `entities.sample()`. Page class members with SPARQL.
- Remove the generated models of those routes. Also remove the models of
  `/v1/graph/changes`, `/v1/graph/entity/metadata` and
  `/v1/graph/entity/neighborhood`, which are gone too.
- `LbbLocalClient.current_state()` and `relationship_history()` stay. They run
  the local `lbb-testctl` commands.

Added:

- Add `as_of_commit_seq` to `sparql()` and `query.sparql()`, sync and async.
  The query reads the retained state of that exact commit, and
  `SparqlResults.snapshot` echoes the pin.
- `sparql()` retries a retryable `429` within the retry budget. A read right
  after a write with `min_indexed_seq` now waits for `read_your_writes_pending`
  to clear. A `5xx` or a transport failure is not retried, because the query
  could run twice.
- `RequestOptions.retry` accepts `"rate_limited"`, which retries only a
  retryable `429`.
- The generated `EntityPropertiesInput.properties` accepts the flat
  `{ field: value }` map as well as a list of `PropertyInput`. The server always
  decoded both shapes, but a `TripletCommitFile` or `EntityPropertiesInput`
  model built with a flat map raised a validation error.
- The generated `SchemaBundleView` has a new `shapes` field: the active SHACL
  shapes as the validator parsed them. New models: `SchemaShapeView`,
  `SchemaShapeTarget`, and `SchemaShapeConstraint`.

## 0.14.0 (2026-09-25)

- Remove branches. Every graph now has one line of history.
- Remove the `branch` argument from `LbbClient`, `AsyncLbbClient`, `graph()`
  and `LbbLocalClient`. Remove `delete_branch()` and `merge_branch()`.
- Remove `observe()`.
- Remove `planner_dataset()`, `planner_preference_dataset()` and
  `promote_planner()`. The server no longer trains the planner.
- Remove the generated planner training models and the `planner` field of
  `ModelServingDefaults`.
- Add `cursor` to `sparql()` and `next_cursor` plus `snapshot` to its results,
  for snapshot-bound pagination of an indexed, ordered `LIMIT` query.

## 0.13.1 (2026-09-24)

- Add the `embeddings` namespace to preview and configure embeddings, change
  models, and search by meaning with class and relationship filters.
- Add the `evals` namespace for query traces, result labels, saved evaluation
  queries, and evaluation runs. Expose managed model settings with
  `managed_models()`.
- Refresh generated Pydantic models and schema preview support.
- Rewrite the README with a complete RDF import and relationship query,
  expected output, an async example, and links to the current guides.

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
