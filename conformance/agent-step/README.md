# Agent step conformance

The scenarios in `scenarios/` pin the crash and replay rules of the agent
step (`docs/architecture/agent-step.md`, section 10.3). Each scenario is a
scripted conversation with a model, a small world of files and entities, and
what a run must leave behind. A fake engine saves the output of each step by
key, as the turn record does, and ends the attempt at a step boundary. The
TypeScript interpreter runs every scenario:

- TypeScript: `sdk/typescript/src/agent-conformance.test.ts` (`npm test -w sdk/typescript`)
- Python: adopts the same scenarios after hosted v1.

A rule changes in one place: change the scenario, then the interpreters.

## The kill matrix

Each scenario runs to the end once, then again for every step the run began.
For each step the attempt ends twice:

- `after_save`: the step's output was saved, and the worker is gone.
- `before_save`: the step's effect ran, and the worker is gone before the
  output was saved. This is the commit that landed with its reply lost.

A second attempt resumes on the same engine, world and spec. It must end with
the same result as the run without a kill, and these invariants must hold:

| Invariant | Check |
| --- | --- |
| One commit | The world applied the commit body once. A commit sent twice under one key answers the first. |
| One Review item per key | The world holds the same Review keys as the run without a kill. |
| No second model call for a saved decision | A step saved before the kill makes no model call again. |
| Cost counted once | The result's cost is the cost the models billed, each effect id once. |
| The pin is checked first | See `pin_change`. |

## The format

A scenario is one JSON object:

```json
{
  "name": "grounded-value-is-written",
  "rule": "One sentence: what the scenario pins.",
  "spec": { "instructions": "…", "owns": { "properties": ["width_cm"] }, "input": { "file": "<sha256>", "instances": ["<iri>"] } },
  "world": { "files": {}, "entities": {} },
  "model": [{ "tool": "files.read", "arguments": {}, "cost_micro_usd": 1000 }],
  "events": [{ "after": "agent.i02.tool", "do": "change_file", "file": "<sha256>" }],
  "pin_change": { "instructions": "…" },
  "expect": { "outcome": "finished", "counts": { "written": 1, "review": 0, "failed": 0, "dropped": 0 } }
}
```

### `spec`

The spec of the step, with the SDK's names in snake case: `instructions`,
`tools`, `max_iterations`, `max_cost_micro_usd`, `max_wall_ms`, `max_read_ms`,
`gate`, `max_proposals`, `max_review_items`, `pin_wait_ms`,
`owns.properties`, `input.file`, `input.instances`, `message`. The step key is
`agent` and the workflow is named `doors`. The workflow identity is `wf-1`
and the message `msg-1`, so an effect id is `wf-1/msg-1/<step key>`.

### `world`

- `commit_seq`, `epoch`: the graph's head commit and epoch (defaults 41 and
  `epoch-1`).
- `files`: files by SHA-256, each with `uploaded_at`, `content_type`,
  `document` (the `Document` IRI), `generated`, and `text` or `pages`.
- `entities`: entity IRIs with the `type` and `name` the graph describes.
- `reads`: answers to `graph.read`. A read answers with the first entry whose
  `match` the query contains: `vars` and `rows`.
- `ground`: verdicts by quote. A quote that no entry names is read from the
  file: it is `ok` when its white-space-folded words occur in the folded
  words of the place, `quote_not_found` when they do not, `upload_changed`
  when the file's `uploaded_at` is not the one the proposal read, and
  `locator_invalid` when the place does not exist.
- `support`: the support the decision model gives, by property name or
  `default`.
- `answers`: a person's answer on the item of a pair, by `<subject IRI>|<property
  name>`. A decision alone (`"reject"`), or an object: `decision` (`accept`,
  `correct` or `reject`), `asked` (the value the item asked about), `value` (the
  value a person gave), `evidence_hash` (the mark of the evidence the item was
  asked on), `commit_error` (the graph refused to write the answer) and
  `reopened` (the item is open again, asked on new evidence, with the answer as
  its previous one). An answer holds for its value and its evidence: the same
  value, or the same evidence, is not asked again; another value on other
  evidence is asked again and waits in Review; a rejected value is skipped only
  for the same value; an item asked again holds the value.
- `commit_error`: the commit is refused with this `status` and `code`. A `400`,
  `403`, `404`, `413` or `422` wrote nothing: the values go to Review with the
  graph's reason (`commit_refused`; `graph_epoch_changed` and
  `evidence_not_verified` are `source_changed`). A `workflow_fence_lost` or
  `write_outside_owns` ends the attempt with the error. Any other `4xx` (an
  idempotency conflict) may have landed: the run fails as
  `agent_effect_uncertain`. A `5xx` is thrown for the turn to retry.

### `model`

The decisions of the scripted model, in order, one for each iteration. A
decision has the `tool`, its `arguments`, a `summary` and the `cost_micro_usd`
it is billed. A reply with `raw` is returned as the model's answer without the
action shape, for an invalid action. After the last entry the model finishes.
A model call is cached by its effect id: the same call again returns the
first answer and bills nothing.

### `events`

Something that happens in the world once, after a step is first saved:
`change_file` (the file is uploaded again, so its `uploaded_at` changes) and
`bump_epoch` (the graph is deleted and created again).

### `pin_change`

Spec fields to change on a resume. After the attempt ends at each step
boundary, a resume with the changed spec must wait (`WorkflowWait`) and call
nothing: no host call, no model call, no step begun. The resume with the
original spec then finishes with the same result.

### `expect`

Every key is checked, and an unknown key fails the scenario.

| Key | Meaning |
| --- | --- |
| `outcome`, `limit`, `code` | The result's outcome, cap and failure code. |
| `counts` | `written`, `review`, `failed`, `dropped` of the result. |
| `iterations`, `cost_micro_usd` | The result's iterations and cost. |
| `commits` | Distinct commits the world holds. |
| `review_items` | Distinct Review keys the world holds. |
| `model_calls` | Distinct effect ids billed. |
| `steps` | The saved step keys, in the order they were saved. |
| `verdicts` | The verdict code of each proposal, by property name. |
| `reviews_with_evidence`, `reviews_without_evidence` | Review items that show a verified quote, and those that show none. |
| `subject_properties` | The `replace_owned.subject_properties` of the commit: `id` (the entity id) and `properties`. |
