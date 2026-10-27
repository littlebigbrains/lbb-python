# values-extract (Python)

The Python port of the workflow template `values-extract`
(`sdk/templates/values-extract` in the LBB repository). It fills properties of
a class from the text of its instances. Each value comes with the words of the
text that state it: `width_cm` 120 and `depth_cm` 80 from `120 x 80 cm`.

The rules are those of the TypeScript template and of LBB's own
`values.extract@v1`: the same prompts, checks, review items and counters. On
the samples with the mock models of its test, the port writes the values of
the answer key (`samples/expected.json`). It gives a person the four values
it cannot confirm: two sizes in another unit, one without a unit, and one
below the gate.

A value between 0.5 and the gate becomes a `value` review item, one per
property of an instance, under LBB's key (`value_item_key`). A person's
answer wins over the model, for the evidence it was given on
(`evidence_of`: the quote and the unit the text states). An accepted or
corrected value is written as answered; a rejected one, or one the graph
refused, is not written again. New evidence with another value asks again,
and the answered value and its source link stay until a new answer. A value
an earlier version filed as a suggestion (`legacy_value_key`) keeps working.

With `Settings(repair=1)`, an instance whose first answer fails a check is
asked once more. A check fails for one of these reasons:

- the answer has the wrong shape;
- a field is missing;
- the property is unknown;
- the value is outside its type or unit;
- the quote is not in the text.

The retry runs in the same step. It uses the effect id `<effect id>.repair`
and `attempt=2`. It sends the same input plus the failed checks
(`failed_checks`). It never sends a label. A property whose first value passed
keeps it. Every retry value for that property is a `duplicate`. Every other
property takes the retry's value (`merge_attempts`). The values then go through
the support call and the gate. A retry that fails leaves the first answer. A
spent budget waits as in any step. `repair=0` (the default) makes the same
calls as before.

## Run it

From `sdk/python/examples`, with the Python SDK installed
(`pip install littlebigbrain`):

```sh
LBB_URL=http://127.0.0.1:7400 LBB_API_KEY=lbb_sk_test_local python -m values_extract try
LBB_URL=http://127.0.0.1:7400 LBB_API_KEY=lbb_sk_test_local LBB_GRAPH=try-values-extract python -m values_extract start
```

| Command | Does |
| --- | --- |
| `python -m values_extract try` | Creates a graph (`LBB_GRAPH`, default `try-values-extract`), puts the six sample records on it, and creates the workflow there |
| `python -m values_extract setup` | Creates the workflow on your graph (`LBB_GRAPH`, default `main`) |
| `python -m values_extract start` | Serves the workflow until SIGTERM or Ctrl-C. `LBB_WORKER_ID` names the worker, `LBB_CONCURRENCY` sets how many turns run at once |

In the LBB repository, `npm run dev:stack -- --worker python:values-extract`
starts a local stack with mock models and runs `try` and `start` on it.

## The files

| File | Holds |
| --- | --- |
| `workflow.py` | The settings, the trigger, the ontology, the checks and the workflow |
| `samples.py` | The loader of the samples |
| `samples/` | The sample records and their answer key, the template's |
| `__main__.py` | The commands |

The test of the port is `tests/test_values_extract_example.py` of the Python
SDK. It runs the workflow with the real worker against a fake LBB, and checks
that a worker stopped during its write replays its steps and writes once.
