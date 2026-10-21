# values-extract (Python)

The Python port of the workflow template `values-extract`
(`sdk/templates/values-extract` in the LBB repository). It fills properties of
a class from the text of its instances. Each value comes with the words of the
text that state it: `width_cm` 60 and `depth_cm` 40 from `ca. 60x40 cm`.

The rules are those of the TypeScript template and of LBB's own
`values.extract@v1`: the same prompts, checks, review items and counters. On
the samples with the mock models of its test, the port writes the values of
the answer key (`samples/expected.json`).

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
