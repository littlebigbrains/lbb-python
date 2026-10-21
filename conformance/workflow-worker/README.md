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
| Any turn route | A `409` (stale lease) or `404` (instance gone) stops the turn: nothing more is sent. |
| `POST /v1/graph/commit` | A fenced write that meets `409 workflow_fence_lost` stops the turn. |

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

### The handler program

`handler` is a list of operations that the harness runs in order with the
SDK's context. A triggered workflow has `on_instances` and, optionally,
`on_fan_in` instead.

| Operation | Does |
| --- | --- |
| `{"step": key, "returns": v, "do": [...], "kind", "timeout_ms", "heartbeat_timeout_ms"}` | `ctx.step(key, fn)`; `fn` runs the actions in `do`, then returns `v` |
| `{"sleep": key, "ms": n}` | `ctx.sleep(key, n)` |
| `{"wait_for_signal": key, "name": s, "timeout_ms": n}` | `ctx.waitForSignal` / `ctx.wait_for_signal`; its output is `{received, value}` |
| `{"write_owned": key, "write": {...}, "graph": g}` | `writeOwned` / `write_owned` with the SDK's owned-write fields |
| `{"requeue": key, "failed": [{instance, error}], "graph": g}` | `requeueFailed` / `requeue_failed` |
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
| `{"reason": {prompt, schema}, "effect_id": true}` | the `reason` activity, with the step's effect id when `effect_id` is true |
| `{"blob_put": text}` | put `text` as a blob of the turn's identity |

A value anywhere in the program can be `{"$ref": "key.path"}` (the output of
step `key`, then the dotted path into it), `{"$message": "path"}` or
`{"$state": "path"}` (the turn's message or state; `""` is the whole value).

### `task` and the exchanges

`task` is the claimed task. A claim response `{"claim": overrides}` answers
`{"task": <task deep-merged with overrides>}`; `{"claim": null}` answers
`{"task": null}`. Deep merge: objects merge key by key, anything else
replaces.

`exchanges` is the server's script, in the order the requests arrive. Each
request the worker sends must match the next exchange's `request`:

- `method` and `path` exactly;
- `query`: these query parameters (others may be present);
- `headers`: these headers (lowercase names);
- `body`: a subset match (an object matches when each listed key matches;
  arrays match item by item and in length; other values are equal);
- `raw`: the body's text exactly;
- `same_body_as`: the index of an earlier exchange whose body this request's
  body equals exactly (a retried request).

Matchers in `body`: `{"$absent": true}` (the key is absent),
`{"$any": true}` (present, any value), `{"$at_least_now_plus_ms": n}` (a
number at least the time the request arrived plus `n`, minus 100 ms).

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

Every exchange must be used, and no other request may come.
