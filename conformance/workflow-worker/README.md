# Workflow worker conformance

The scenarios in `scenarios/` pin the rules every workflow worker of LBB
follows: the TypeScript `WorkflowWorker` (`sdk/typescript/src/workflows.ts`)
and the Python `lbb.workflows.Worker` (`sdk/python/lbb/workflows`). Each
scenario is a scripted exchange with the server and the requests the worker
must send. Both test suites run every scenario:

- TypeScript: `sdk/typescript/src/worker-conformance.test.ts` (`npm test -w sdk/typescript`)
- Python: `sdk/python/tests/test_worker_conformance.py` (`pytest sdk/python`)

A rule changes in one place: change the scenario, then both workers.

## The rules

| Route | Rule |
| --- | --- |
| `turns/claim` | Not retried: a lost answer must not claim twice. |
| `turns/heartbeat` | Every third of the lease, not retried. A failed heartbeat ends the attempt here, and nothing more is sent for the turn. |
| `turns/operation`, `turns/progress`, `turns/checkpoint`, `turns/wait-signal`, `turns/complete` | Retried: the same request again changes nothing more. A retried completion sends the same body. |
| `turns/fail` | Not retried. A `WorkflowWait` or a `429 workflow_budget_exceeded` fails with `retry_at_ms` (at least a second ahead) and counts no attempt. |
| Any turn route | A `409 turn_lease_lost` (the token no longer holds the turn), any other `409` (an older server answers a lost lease `409 conflict`) or a `404` (instance gone) stops the turn: nothing more is sent. |
| `POST /v1/graph/commit` | A fenced write that meets `409 workflow_fence_lost` stops the turn. |
| `POST /v1/workflows/activities/*` | A model call from a turn names the turn's run (`workflow`). It names the step (`workflow.step`, the key) whose effect id is the call's, or starts the call's followed by `.` or `:`. A call with no such step names no step. The calls' usage goes with the completion. |
| `POST /v1/reviews` | `waitForReview` / `wait_for_review` files the item in the step `<key>.ask`, with the run as the one that waits. It waits for the signal the item names in `<key>.answer`. On a timeout, the step `<key>.read` reads the item again. A trigger shard (`starter.<name>`) is refused: the turn fails for good. |
| Run results | When the claim names `results_max`, `turns/complete` and `turns/fail` carry `results` (`{inputs, counts}`): each subject of an owned write `written` with its fields and edges (`no_value` when the write sets nothing on it), each subject of a review item `review` with the item's id (`waitForReview`: with what the wait ended with), each requeued instance `failed` with its error, and, at a completion only, the trigger message's other instances not in the continuation `no_value`. The report is at most 512 KiB (UTF-8 of its JSON); a `413` sends the turn route again without `results`. A step that reported inputs saves `{lbb_step_results: 1, run, results, value}` and its code sees `value`; a replay in the same run reports the saved inputs as they were, in another run its writes as `duplicate`. Without `results_max`, nothing is sent and nothing is saved. |
| Captured activities | Model calls carry `test: {workflow}`. An owned write is checked with the commit's dry run and kept: no commit, no fence, no idempotency key. A refusal of the dry run is the write's error. Reviews are kept and filed nowhere, and a wait ends at once unanswered. A wait on a trigger shard is refused as the live one is. Reads of review items run. |

Steps replay by key and kind, in order; each step's effect id is
`<effect_prefix>/<key>`, the same on every attempt.

## The format

A scenario is one JSON object:

```json
{
  "name": "replay-reuses-saved-steps",
  "rule": "One sentence: what the scenario pins.",
  "workflow": { "type": "plain", "name": "counter", "version": "v1", "initial_state": {} },
  "handler": [{ "step": "increment", "returns": 99 }, { "return": { "state": { "count": { "$ref": "increment" } } } }],
  "task": { "...": "the claimed task that claim responses start from" },
  "runs": 1,
  "exchanges": [
    { "request": { "method": "POST", "path": "/v1/workflows/turns/claim" }, "response": { "claim": {} } }
  ],
  "expect": { "outcomes": ["claimed"], "ran": [], "settle_ms": 0 }
}
```

### `workflow`

- `type`: `plain` (a `workflow()` definition) or `triggered` (a
  `triggeredWorkflow()` / `triggered_workflow()` definition).
- `name`, `version`, `initial_state`.
- `worker`: optional worker options: `execution_reads`, `progress_every_ms`.
- The client names the graph `client_graph` (default `main`).
- `capture`: optional, `{"workflow": name}`. The program's activities are then
  a capture of the client's (`captureActivities` / `capture_activities`, with
  that workflow name). Without it, they are the client's (`lbbActivities` /
  `lbb_activities`).

### The handler program

`handler` is a list of operations that the harness runs in order with the
SDK's context. A triggered workflow has `on_instances` and, optionally,
`on_fan_in` instead.

| Operation | Does |
| --- | --- |
| `{"step": key, "returns": v, "do": [...], "kind", "timeout_ms", "heartbeat_timeout_ms"}` | `ctx.step(key, fn)`; `fn` runs the actions in `do`, then returns `v` |
| `{"sleep": key, "ms": n}` | `ctx.sleep(key, n)` |
| `{"wait_for_signal": key, "name": s, "timeout_ms": n}` | `ctx.waitForSignal` / `ctx.wait_for_signal`; its output is `{received, value}` |
| `{"write_owned": key, "write": {...}, "graph": g}` | the activities' `writeOwned` / `write_owned` with the SDK's owned-write fields |
| `{"requeue": key, "failed": [{instance, error}], "graph": g}` | the activities' `requeue` |
| `{"wait_for_review": key, "request": {...}, "timeout_ms": n}` | `waitForReview` / `wait_for_review`; its output is `{answered, answer, item}` |
| `{"review": key, "request": {...}}` | the activities' `review`; its output is the item |
| `{"review_items": key, "keys": [...]}` | `reviewItems` / `review_items`; its output is the list of items |
| `{"wait": retry_at_ms}` | raise a `WorkflowWait` |
| `{"raise": "error" \| "workflow_error", "message": m}` | raise a plain error, or a `WorkflowError` (permanent) |
| `{"return": {"state": v, "result": v}}` | return the turn's result |
| `{"continue": {"state": v, "message": v}}` | return `ctx.continue(state, message)` |
| `{"keep": true}` | a triggered handler returns nothing: the state stays |

Actions in a step's `do`:

| Action | Does |
| --- | --- |
| `{"heartbeat": details, "checkpoint": v}` | the step's heartbeat, with a resume checkpoint when `checkpoint` is present |
| `{"progress": phase, "done": n, "total": n, "unit": u}` | the step's coalesced progress |
| `{"sleep_ms": n}` | wait `n` ms of real time |
| `{"block": true}` | wait until the attempt ends (a step that never returns) |
| `{"raise": "error" \| "workflow_error"}` | raise inside the step |
| `{"reason": {prompt, schema}, "effect_id": true}` | the activities' `reason`, with the step's effect id when `effect_id` is true, or with that id and the text after it when `effect_id` is text (`".2"`) |
| `{"blob_put": text}` | put `text` as a blob of the turn's identity |

A value anywhere in the program can be `{"$ref": "key.path"}` (the output of
step `key`, then the dotted path into it), `{"$message": "path"}` or
`{"$state": "path"}` (the turn's message or state; `""` is the whole value),
`{"$repeat": n, "of": template}` (a list of `n` copies of the template, each
text's `{i}` replaced by its index) or `{"$chars": n, "char": c}` (a text of
`n` times `c`, default `q`). A step's `do` can also hold `{"review": request}`:
the activities' `review` inside the step.

### `task` and the exchanges

`task` is the claimed task. A claim response `{"claim": overrides}` answers
`{"task": <task deep-merged with overrides>}`; `{"claim": null}` answers
`{"task": null}`. Deep merge: objects merge key by key, anything else
replaces.

`exchanges` is the server's script, in the order the requests arrive. Each
request the worker sends must match the next exchange's `request`:

- `method` and `path` exactly;
- `query`: these query parameters (others may be present);
- `headers`: these headers (lowercase names; `null` means the header is absent);
- `body`: a subset match (an object matches when each listed key matches;
  arrays match item by item and in length; other values are equal);
- `raw`: the body's text exactly;
- `same_body_as`: the index of an earlier exchange whose body this request's
  body equals exactly (a retried request).

Matchers in `body`: `{"$absent": true}` (the key is absent),
`{"$any": true}` (present, any value), `{"$at_least_now_plus_ms": n}` (a
number at least the time the request arrived plus `n`, minus 100 ms),
`{"$length": n}` (a list of `n` items), `{"$json_bytes_at_most": n}` (a value
whose compact JSON is at most `n` bytes of UTF-8) and `{"$all": [m, …]}` (every
matcher holds).

`response`: `{"status": 200, "body": {...}}` (status 200 and body `{}` by
default), `{"claim": ...}`, `{"transport_error": true}` (the connection
fails), and `delay_ms` (answer that much later).

### `expect`

- `outcomes`: per run, `claimed` (a turn was claimed and ran), `idle` (no
  turn), or `error` (the run raised). `runs` is the number of runs, each one
  `runOnce()` / `run_once()`.
- `ran`: the keys of the step functions that ran, in order.
- `effect_ids`: the effect ids those step functions saw, in order.
- `settle_ms`: after the last run, wait this long; no more requests may come.
- `captured`: what the capture kept, matched as a `body` is. It is
  `{"writes": [...], "reviews": [...], "results": [...]}`. A write is `{step_key, graph, body,
  check, errors, report}`, without `graph` and `report` when it has none. A
  review is `{request}`.

Every exchange must be used, and no other request may come.
