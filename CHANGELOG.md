# Changelog

All notable changes to the `littlebigbrain` Python SDK are documented here.

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
