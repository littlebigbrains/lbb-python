# Changelog

All notable changes to the `littlebigbrain` Python SDK are documented here.

## Unreleased

- `files.list(*, parse_status=, name=)` filters the list: `parse_status`
  (`parsed`, `unsupported`, `failed`, or `waiting` for a file without one)
  and `name` (a part of the name, in any case). The server reads the
  `Document` facts of the published generation for the page, and
  `FileList.as_of_commit_seq` names the commit it read. `limit` and
  `after` page it as they page the plain list.
- `SuggestionLinkSummary.document_name` names the file of a link review:
  the `Document`'s name for a `document` IRI, the name itself for evidence
  written before typed content.
- An upload without a type (or with `application/octet-stream`) gets the
  type the server detects: PNG, JPEG, GIF, WebP, the Office types of
  `.xlsx`, `.docx` and `.pptx`, and `text/csv`, `text/tab-separated-values`,
  `text/markdown`, `application/json` and `text/plain` for UTF-8 text with
  those names, beside the PDF it detected before.
- The starter workflow catalog lists `rows` and `sections`, the templates
  your worker runs on files. `StarterWorkflow.media_types` names the media
  types of their content trigger, and their `watch` is `none`. A request to
  create one through the triggered workflow routes answers `400
  template_runs_in_worker`: create its workflow on the named workflow
  routes, as the template's `trigger()` does.
- `files.get(sha256, deleted=True)` names the facts that cite a live file
  too (`cited_by`, at most 20): what `delete(sha256, purge=True)` would
  retract.
- A deleted file's bytes, pages and derivations are freed after a grace
  (30 days by default), unless a fact's evidence still names the file.
  `files.delete(sha256, purge=True)` also retracts those facts and answers
  `purged_edges`. `files.get(sha256, deleted=True)` answers a deleted file
  until its content is freed, with `deleted_at` and `cited_by` (the facts
  that cite it, `FileCitation`). An upload of the same bytes during the
  reclaim raises `LbbError` 429 `file_reclaim_in_progress`; retry it.
- `files.list(*, limit=, after=)` reads one page of the graph's files,
  the newest upload first: `limit` is 1 to 1,000 (default 1,000), and the
  answer's `next` (`FileList.next`) is the `after` of the next page. A page
  can hold fewer than `limit` files while `next` is set. `files.counts()`
  (`GET /v1/files/counts`) counts the files by parse status over the
  `Document` entities of the published generation, with the commit it
  counted. The models gain `FileCounts`.
- `files.get(sha256)` (`GET /v1/files/record`) reads one file's record from
  one object; a SHA-256 the graph has no file for, or a deleted file,
  answers `404 file_not_found`.
- Files of up to 1 GiB. `files.content(sha256, range=)` reads one byte
  range: `(start, end)` with both bytes included, `(start, None)` to the
  end, or `(None, n)` for the last `n` bytes. `files.download(sha256, path,
  range=)` writes the bytes to `path` as they arrive and returns how many
  it wrote, so a large file is never held whole. `files.head(sha256)`
  returns `{"bytes", "content_type", "sha256"}` without the bytes. A range
  with no byte of the file raises `LbbError` 416 `range_not_satisfiable`;
  an upload above the server's limit raises 413 `file_too_large`. On the
  client, `graph(name)` and the async client.
- `files.upload(path)` sends the file in chunks of 1 MiB with its
  `Content-Length`, so a file of 1 GiB is never held in memory; a retry
  sends it again from the start. Bytes are sent as before.
- `files.upload(data, name=, content_type=, language=, schema=)`: an
  upload can state the content's `language` (the natural language of the
  content, a BCP 47 tag such as `en` or `de-CH`, at most 64 characters) and
  `schema` (at most 256), which the file's `Document` stores. `FileRecord`
  gains `language` and `schema_` (`schema` in JSON). An upload also writes the file's
  `Content` (`content:<sha256>`) and the edge `HAS_CONTENT`, and the
  ontology gains `REPRESENTS`. `SuggestionLinkSummary.document` is the
  `Document`'s IRI for a link written since, and the file's name for one
  written before.
- `named_workflow_runs(name, *, before=, limit=, outcome=)`,
  `named_workflow_run(name, run)` and `named_workflow_run_stats(name, *,
  hours=)` read the run summaries of a named workflow
  (`GET /v1/workflows/named/runs*`); `triggered_workflow_runs`,
  `triggered_workflow_run` and `triggered_workflow_run_stats` read the same
  by starter and name, and `triggered_workflow_documents` pages the
  documents of `documents.link` and `documents.map`. The models are
  `WorkflowRunList`, `WorkflowRunDetail`, `WorkflowRunStats` and
  `TriggeredDocumentPage`.
- `check_derivation(sha256, workflow_type, version, settings_digest)` and
  `record_derivation(..., run_id=, outputs=)` on the client, `graph(name)`
  and the async client (`/v1/workflows/derivations/*`): what one version of
  a workflow made of one content with one set of settings.
- The models gain `DerivationKey`, `Derivation`, `DerivationCheckResponse`,
  `DerivationRecordRequest`, `DerivationRecordResponse` and
  `WorkflowContentTrigger`; `WorkflowTrigger` and `WorkflowTriggerResolved`
  gain `content`; `WorkflowMessageOrigin` gains `sha256`, `media_type` and
  `uploaded_at_ms`.

## 0.26.0 (2026-10-14)

- Models for workflows with triggers (`/v1/workflows/named`):
  `NamedWorkflow`, `NamedWorkflowRequest`, `NamedWorkflowList`,
  `NamedWorkflowShard`, `NamedWorkflowQueue`, `NamedWorkflowRunResponse`,
  `NamedWorkflowPauseRequest`, `NamedWorkflowDeleteResponse`,
  `WorkflowTrigger`, `WorkflowTriggerResolved`, and the queue models
  `WorkflowQueueItem`, `WorkflowQueueShard`, `WorkflowQueuePass` and
  `WorkflowQueueInFlight`. The Python SDK has no methods for these routes
  yet.

## 0.25.0 (2026-10-13)

- `evals.run_answers(answers, runner=, consistency=)` checks your own
  agent's answers to the question goldens (`POST /v1/evals/run/answers`).
  `evals.results(runner=)` reads one agent's runs, and
  `evals.label(..., route=)` names the route a question expects.
- The models gain `route` on `SparqlTextRequest`, `EvalLabelRequest`,
  `EvalTrace` and `Golden`, `route_check` on a verdict, `runner`,
  `route_checked` and `route_right` on `EvalResults`, and `EvalVia.app`.
- `embeddings.search(..., shown=)` sends the app's cut for the eval trace
  of a search with `request`; the models gain `SearchShown`, `EvalShown`,
  `rerank` and `shown` on `EvalTrace`, and `shown` on `Golden` and a verdict.
- Models for the workflow progress convention and the inbox routes:
  `WorkflowProgress`, `WorkflowMessageOrigin`, `WorkflowInboxMessage`,
  `WorkflowInboxResponse`, `WorkflowTypeInboxEntry` and
  `WorkflowTypeInboxResponse`. `WorkflowInstance` and `WorkflowTurn` gain
  `progress`, `WorkflowTurn` and `WorkflowMessageRequest` gain `origin`.
  The Python SDK has no workflow worker.

## 0.24.0 (2026-10-12)

- The starter workflow Values from text is `values.extract` (formerly
  `infer.llm`; the server still accepts the former id and answers with
  `values.extract`).
- `ModelJob` gains `map` (the decisions of `documents.map`) and
  `ModelActivityFeature` gains `workflows`.
- Review items group by instance (`review` workflow): `SuggestedFacts`
  gains `required`, the rows an accept must keep (the type of a new
  instance); the accept request gains `facts`, the subset of the proposed
  facts to keep; a suggestion gains `proposed_facts` and `facts_written`.

## 0.23.0 (2026-10-11)

- `try_workflow_starter(starter, *, params=None)` runs Try on samples
  (`POST /v1/workflows/starters/try`, SaaS mode) and is sent once.
- Workflow activities on `LbbClient`, `AsyncLbbClient` and `graph(name)`:
  `reason_activity(prompt, schema, system=, max_tokens=, effect_id=,
  label=)`, `decide_activity(state, questions, effect_id=, label=)` and
  `embed_activity(texts, effect_id=, label=)` call
  `POST /v1/workflows/activities/{reason,decide,embed}`. `schema` goes on
  the wire as `schema` (the generated `ReasonActivityRequest` names the
  field `schema_`). With `effect_id` the server keeps the first answer and
  returns it again with `cached: True`, so a `5xx` and a transport failure
  are retried; without one only a `429` the server returns before it runs
  the request is. `reason_activity` waits up to 5 minutes per attempt.
  Each returns the JSON answer (an awaitable on `AsyncLbbClient`);
  validate it with `models.ReasonActivityResponse`,
  `models.DecideActivityResponse` or `models.EmbedActivityResponse`.
- Starter workflows and triggered workflows on `LbbClient`,
  `AsyncLbbClient` and `graph(name)`. A starter workflow is a template. A
  triggered workflow is a workflow created from one on a graph, and it
  follows a class of the graph. Embeddings on classes (`search.embed`) and
  fit sources (`ontology.fit`) are triggered workflows.
  `workflow_starters()` calls `GET /v1/workflows/starters`.
  `use_workflow_starter(starter, class_=..., fields=..., params=...)`
  creates or changes a triggered workflow. `triggered_workflows()` and
  `triggered_workflow(starter, name)` read them.
  `preview_triggered_workflow(..., sample=, iris=, propose=)`,
  `refresh_triggered_workflow(starter, name)` and
  `delete_triggered_workflow(starter, name)` call the other
  `/v1/workflows/triggered*` routes. Each returns the JSON answer (an
  awaitable on `AsyncLbbClient`). `use_workflow_starter` and a preview
  without `propose` retry a failed call. `delete_triggered_workflow`
  repeats the name as `confirm`. Ontology starters stay
  `ontology.starters`.
- Developer workflows (the `workflow@v1` starter workflow):
  `use_workflow_starter("workflow", ...)` takes `workflow` (`workflow_type`,
  `version`), `owns` and `batch`.
  `pause_triggered_workflow(starter, name, paused=True)` calls
  `POST /v1/workflows/triggered/pause` (`paused=False` resumes) and retries
  a failed call. `commit` and `graph(name).facts.create` send
  `replace_owned` as given and return `owned`. `ontology.suggestion_create`
  sends `facts`, and the accepted suggestion's `facts_commit` names the
  commit.
- New `files` on `LbbClient`, `AsyncLbbClient` and `graph(name)` for the
  files of a graph: `upload(data, name=, content_type=)` sends the bytes,
  or the file at a path (`str` or a path object; the name and the content
  type default to the path's), as the raw body of `POST /v1/files` and
  retries a failed call (the same bytes again change nothing); `list()`,
  `content(sha256)` (the bytes), `pages(sha256, from_=, to=)` (the parsed
  lines and words with their boxes) and `delete(sha256)` (repeats the
  SHA-256 as `confirm`) call `/v1/files*`. A SHA-256 that is not 64
  hexadecimal characters or a bad page range raises `ValueError` before a
  request.
- New `entity_ref(iri, type, name=None)` and `entity_id_from_iri(iri)`
  (also `lbb.entities.ENTITY_IRI_NAMESPACE` and `ENTITY_ID_KEY_SENTINEL`):
  the commit reference of an entity you know by its IRI. Its `key` is the
  id sentinel and the entity's 32-hex id, so a commit or a `replace_owned`
  subject addresses that entity directly, a keyed one included. `name`
  (default: the id) is the display name a triplet endpoint or a field row
  writes; `replace_owned` reads only the id.
- `query.names` responses carry `index_complete`, `index_classes_read` and
  `index_gaps` (new models `QueryNamesIndexGap` and `QueryNamesGapReason`):
  whether the name index read every class in full, and the classes it did
  not, with the reason. The index reads people and organizations first.

## 0.22.0 (2026-10-08)

- The `ModelJob` type gains `ask`: the checked answer of one question
  (model checks and the call log, #1107).
- New `models.trials` and `models.switches` on `LbbClient` and
  `AsyncLbbClient`: test another model on a use of a model on the graph
  (`ask`, `route`, `rerank`, `fit`, `label`) against the ground truth of its
  checked calls, then switch the use to it. `trials.options`, `create`,
  `list`, `get`, `call` and `stop` call `/v1/models/trials*` and return
  typed models (`ModelTrial`, `ModelTrialStartResponse`, ...);
  `trials.wait(trial_id, until=, timeout=, on_update=)` reads the trial
  until it compared the checked calls (or ended). `switches.create(trial=)`,
  `list` and `revert(job)` call `/v1/models/switches*`.
- New `ontology.fit_sources` for fit from text: `list`, `get`, `declare`,
  `preview`, `refresh` and `delete` call `/v1/ontology/fit-sources*`. A fit
  source names the properties of a class that hold text (transcripts,
  documents); the server reads every instance and files ontology change
  suggestions with verified quotes. `preview(..., propose=True)` runs the
  models on up to 3 instances without filing anything. New models
  `FitSource*` and `FitProposal*`.
- `QueryAskResult.chart`: how to draw `rows` (`kind` `bar`, `line`,
  `scatter` or `table`, and the columns `x`, `y` and `series`), or `None`.
  The server checked the hint against the rows. New models
  `QueryAnswerChart` and `QueryAnswerChartKind`; `QueryAnswer` has `chart`,
  and so does the stream's `answer` event.

## 0.21.0 (2026-10-06)

This release breaks the question API. The server removed the one-shot
rewrite and renamed its route to `POST /v1/query/ask`.

What breaks:

- `query.rewrite()` and `query.rewrite_stream()` are removed, on `LbbClient`
  and `AsyncLbbClient`. `POST /v1/query/rewrite` answers 404; its
  `did_you_mean` names `/v1/query/ask`. `GET` and
  `PUT /v1/query/rewrite/profile` stay, and so do `query.rewrite_profile()`
  and `query.set_rewrite_profile()`.
- `query.ask()` sends `POST /v1/query/ask`. It answers in plain words by
  default: the old `mode="answer"`. It no longer runs one written query.
- `query.ask()` takes no `previous`. The server answers
  `400 invalid_ask_request` to `previous`, `run` and `mode: "rewrite"`. The
  loop runs and corrects its own queries. Put what the app knows in
  `context`.
- `QueryAskResult.rewrite` is renamed `response`. It holds the whole
  `POST /v1/query/ask` response.
- `QueryAskResult.history` has no `before` and `after`. A comparison has no
  `result`, so `rows` is empty; `history` holds what differs.
- `QueryRewriteStreamEvent` is renamed `QueryAskStreamEvent`.
- The stream events `query`, `run`, `rows` and `repair` are removed. The
  stream sends `grounding`, `route`, a `step` per tool call, `answer` and
  `done`. The client skips the old names if a server still sends them.
- The error code `invalid_rewrite_request` is now `invalid_ask_request`.
  `rewrite_limit` and `rewrite_model_unavailable` keep their names.
- The generated models drop `QueryRewriteStep`, `QueryRewritePoint`,
  `QueryRewriteQueryEvent`, `QueryRewriteRunEvent`, `QueryRewriteRowsEvent`,
  `QueryRewriteRepairEvent` and their `QueryRewriteEvent*` wrappers.
  `QueryRewriteRequest` drops `run` and `previous`. `QueryRewriteMode` drops
  `rewrite`.

What to use instead:

- To answer a question, call `query.ask(question)`. Read `answer`,
  `citations`, `rows` and `steps` of the `QueryAskResult`.
- To show progress, call `query.ask_stream(question)`. It takes the
  arguments of `query.ask`. `LbbClient` returns a generator, and
  `AsyncLbbClient` an async generator. `QueryAskResult.from_response()`
  parses the data of `done`.
- For the kind of question alone, call `query.ask(question, mode="route")`.
  The router model answers alone; `answer` and `query` are `None`.
- When your app runs its own agent loop, call the tools: `query.names()`,
  `query.describe()`, `query.commit_at()`, `query.compare()` and
  `query.sparql(..., as_of_commit_seq=...)`. None calls a model.

Other changes:

- `query.ask()` and `query.ask_stream()` take the same keyword arguments on
  both clients: `context`, `route`, `mode`, `limit`, `as_of_commit_seq`,
  `today`, `include_grounding`, `anchor`, `timeline`, `consistency` and
  `options`. `limit` caps the rows of each query of the loop. A failed call
  is not retried unless `options={"retry": True}`.
- The loop has a `compare` tool. `QueryAnswerTool` gains `compare`, and a
  `compare` step's `rows` counts the entries that differ.
- Add four tools for an app's own agent, none of which calls a model:
  `query.names(text, limit=...)` (`POST /v1/query/names`) finds the entities
  a text names, with the candidates of each name, the one to prefer first;
  `query.describe(question=..., classes=..., properties=...)`
  (`POST /v1/query/describe`) describes the classes and properties a
  question needs, with how many sampled instances hold each property and the
  values of small classes; `query.commit_at(date=...)` or
  `query.commit_at(moment=...)` (`GET /v1/graph/commit-at`) finds the commit
  of a date; and `query.compare(query, before=..., after=..., key=...)`
  (`POST /v1/query/compare`) runs one `SELECT` at two points and pairs the
  rows into `added`, `removed` and `changed`, with `totals` and a `cursor`
  for the next page. The models gain the request and response types.
- `QueryRewriteHistory` gains `key`, `changed` and `totals`. With a `key`,
  the rows of a comparison are paired by those variables.
- `query.ask()` answers in plain words. The model runs queries and reads
  their rows (at most 4 rounds) before it answers. `QueryAskResult` gains
  `answer` (the text, or `None` when the loop stopped first), `citations`
  (IRIs from the rows the loop read) and `steps`. `rows` hold the rows of
  the query the answer stands on.
- New models: `QueryAnswer`, `QueryAnswerStep`, `QueryAnswerTool`,
  `QueryAnswerStepEvent`, `QueryRewriteEventStep`, `QueryRewriteEventAnswer`.
  `QueryRewriteMode` gains `answer`, and `ModelJob` gains `answer`.
- Evals ask a question again through the answer loop. The model
  `EvalAskInput` drops `previous` and gains `timeline`. `EvalTrace` and
  `EvalVerdictDetail` gain `reply`: the loop's answer in words and the IRIs
  it cites (`QueryAnswer`).

## 0.20.0 (2026-10-05)

- `SparqlResults.search` gains `rerank` and `timings.relevance_ms`, for a
  query with `search:rerank true`.
- Add `query.rewrite_stream(question, ...)` with the keyword arguments of
  `query.rewrite`. It sends `POST /v1/query/rewrite` with
  `Accept: text/event-stream` and yields one `QueryRewriteStreamEvent` per
  step: `grounding`, `route`, `query`, `run`, `rows` and `repair`. The last
  event, `done`, holds the same dict as `query.rewrite` returns.
- `LbbClient` returns a generator, and `AsyncLbbClient` an async generator.
  Closing it closes the response, and the server then stops its work.
- An `error` event raises the same `LbbError` as `query.rewrite`. An error
  before the stream starts raises as `query.rewrite` does. The client never
  retries a stream, and it skips event names it does not know.
- A body that ends before `done` or `error` raises
  `httpx.RemoteProtocolError`. A server without streams answers with JSON,
  and the stream then yields only `done`.
- `QueryRewriteStreamEvent.model()` validates an event as its generated
  model, for example `models.QueryRewriteEventRoute`.
- `query.rewrite()` and `query.ask()` take `anchor`: entity IRIs the user
  picked, at most 10. The server reads each one, and the query uses the IRIs
  directly instead of matching their names. The async client takes it too.
- `QueryAskResult` gains `linked`, the names of the question the server
  linked to entities (`text`, `iri`, `label`, `class`, `score`, `by`), and
  `anchors`, what the server read about each anchored IRI.
- The generated models add `QueryRewriteRequest.anchor`,
  `QueryRewriteResponse.linked` and `anchors`, `QueryRewriteLink`,
  `QueryRewriteAnchor`, `QueryLinkMethod`, `grounding.names`, and
  `timings.link_ms` and `anchor_ms`.
- Add `query.rewrite_profile()` and `query.set_rewrite_profile(notes=,
  examples=, expected_version=, dry_run=)` for the graph's rewrite profile:
  notes and up to 20 worked examples the rewriter reads for every question.
  Pass the `version` you read as `expected_version`; when another write came
  first, the call raises a `409 conflict`.
- The `grounding` of a rewrite gains `focus_classes`, `focus_properties` and
  `profile_version`.
- `SparqlResults.search` gains `rerank` and `timings.relevance_ms`, for a
  query with `search:rerank true`.
- `query.rewrite()`, `query.ask()` and `query.rewrite_stream()` take
  `timeline`: dated points that stand for the graph's commits
  (`{"date", "as_of_commit_seq", "label"}`, at most 200). A history question
  about a date then reads the commit of the latest point on or before it.
  Without a timeline the server reads the last commit written by the end of
  that day. The async client takes it too.
- `QueryAskResult` gains `history`: the date, the commit it resolved to
  (`as_of_commit_seq`) and how (`resolved_by`). A question that asks what
  changed runs at both points: `before` and `after` hold the two runs, and
  `added` and `removed` the rows that differ.
- The generated models add `QueryRewriteRequest.timeline`,
  `QueryRewriteTimelinePoint`, the new `QueryRewriteHistory` fields,
  `QueryHistoryResolution`, `QueryRewriteTerm`, and `point` on the `run`
  event (`QueryRewritePoint`).

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
