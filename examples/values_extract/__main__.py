"""The commands of values-extract, as the TypeScript template project has
them:

    python -m values_extract try     # a graph with the samples and the workflow
    python -m values_extract setup   # the workflow on your graph
    python -m values_extract start   # serve it: claim and run its turns

They read ``LBB_URL`` (your stack's endpoint, default
``http://127.0.0.1:7400``), ``LBB_API_KEY`` (a key of the stack) and
``LBB_GRAPH`` (``try`` uses ``try-values-extract`` by default, the others
``main``). ``start`` also reads ``LBB_WORKER_ID`` and ``LBB_CONCURRENCY``.
"""

from __future__ import annotations

import asyncio
import os
import sys

from lbb import AsyncLbbClient, LbbError
from lbb.workflows import WorkflowWorker, lbb_activities

from .samples import load_samples
from .workflow import create_workflow, ontology, trigger


def client(default_graph: str) -> AsyncLbbClient:
    return AsyncLbbClient(
        os.environ.get("LBB_URL", "http://127.0.0.1:7400"),
        api_key=os.environ.get("LBB_API_KEY"),
        graph=os.environ.get("LBB_GRAPH", default_graph),
    )


async def try_samples() -> None:
    """Create a graph, put the samples on it, and create the workflow there.
    Then serve it with ``start`` and the same ``LBB_GRAPH``."""
    async with client("try-values-extract") as lbb_client:
        graph = lbb_client._graph
        try:
            await lbb_client.create_graph()
            print(f"Created the graph {graph}")
        except LbbError as error:
            # 409: the graph exists already.
            if error.status_code != 409:
                raise
        count = await load_samples(lbb_client)
        print(f"Committed {count} sample records")
        created = await lbb_client.use_workflow_starter(**trigger())
        print(f"Created {created['name']} on the graph {graph}: {created['status']['state']}.")
        print(f"Serve it with LBB_GRAPH={graph} python -m values_extract start")


async def set_up() -> None:
    """Add the classes and properties the workflow writes, then create the
    triggered workflow. Each call is safe to repeat."""
    async with client("main") as lbb_client:
        await lbb_activities(lbb_client).evolve(ontology())
        created = await lbb_client.use_workflow_starter(**trigger())
        graph = lbb_client._graph
        print(f"Created {created['name']} on the graph {graph}: {created['status']['state']}.")


def start() -> None:
    """Serve the workflow until SIGTERM or Ctrl-C."""
    lbb_client = client("main")
    workflow = create_workflow(lbb_activities(lbb_client))
    worker = WorkflowWorker(
        lbb_client,
        [workflow],
        worker=os.environ.get("LBB_WORKER_ID", f"{workflow.name}-py-1"),
        concurrency=int(os.environ.get("LBB_CONCURRENCY", "1")),
    )
    print(
        f"Serving {workflow.name}@{workflow.version} for the graph {lbb_client._graph}. "
        "Stop with Ctrl-C.",
        flush=True,
    )
    worker.run()


def main(argv: list[str]) -> int:
    command = argv[0] if argv else ""
    if command == "try":
        asyncio.run(try_samples())
    elif command == "setup":
        asyncio.run(set_up())
    elif command == "start":
        start()
    else:
        print(__doc__)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
