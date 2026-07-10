"""Embedding-index demo driven through ``LbbLocalClient``.

Requires a full Little Big Brain **engine** checkout (``lbb-testctl`` is
compiled with cargo), not just this SDK repository. Set ``LBB_REPO_ROOT`` to
the engine checkout, or run the script from inside one.
"""

import os
from pathlib import Path
from tempfile import mkdtemp

from lbb import LbbLocalClient


def main() -> None:
    repo = Path(os.environ.get("LBB_REPO_ROOT") or Path(__file__).resolve().parents[3])
    root = Path(mkdtemp(prefix="lbb-python-ann-"))
    client = LbbLocalClient(
        root=root,
        tenant="acme",
        graph="main",
        branch="main",
        repo_root=repo,
    )

    client.create_graph()
    client.commit_triplets_file(repo / "database/examples/triplets/semantic_graph.json")
    build = client.build_embedding_index(
        {
            "targets": ["entity", "assertion", "observation", "neighborhood"],
            "dim": 64,
            "max_clusters": 8,
            "include_clusters": False,
        }
    )
    print(f"manifest: {build['manifest_key']}")

    inspect = client.embedding_index_inspect(
        {
            "targets": ["entity", "assertion", "observation", "neighborhood"],
            "source": "persisted",
            "dim": 64,
            "max_clusters": 8,
            "include_clusters": True,
        }
    )
    print(f"spaces: {len(inspect['spaces'])}")

    search = client.embedding_search(
        {
            "query": "database storing customer identity records",
            "targets": ["entity", "assertion", "observation", "neighborhood"],
            "source": "persisted",
            "top_k": 5,
            "probe_count": 4,
            "dim": 64,
            "max_clusters": 8,
            "explain": True,
        }
    )
    for result in search["results"]:
        print(f"{result['score']:.3f} {result['target_kind']} {result['label']}")


if __name__ == "__main__":
    main()
