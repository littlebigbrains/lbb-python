"""0.33 explicit local search retirement; ordinary local calls still work."""

from __future__ import annotations

import json
import subprocess
from collections.abc import Iterator, Mapping, Sequence
from pathlib import Path
from typing import Any
from unittest import mock

import pytest

import lbb.local
from lbb import LbbLocalClient, LbbLocalSearchRetiredError


class UnreadableRequest(Mapping[str, Any]):
    def __getitem__(self, key: str) -> Any:
        raise AssertionError("retired request must not be inspected")

    def __iter__(self) -> Iterator[str]:
        raise AssertionError("retired request must not be serialized")

    def __len__(self) -> int:
        raise AssertionError("retired request must not be inspected")


@pytest.mark.parametrize(
    "method",
    [
        "semantic_search",
        "build_embedding_index",
        "embedding_index_inspect",
        "inspect_embedding_index",
        "embedding_search",
    ],
)
def test_retired_methods_keep_signatures_but_never_create_a_file_or_spawn(method: str) -> None:
    client = LbbLocalClient(
        root="PRIVATE-ROOT",
        tenant="tenant",
        graph="graph",
        testctl_bin="PRIVATE-BINARY",
        env={"PRIVATE-KEY": "PRIVATE-CREDENTIAL"},
    )
    with mock.patch.object(lbb.local.tempfile, "NamedTemporaryFile") as request_file:
        with mock.patch.object(lbb.local.subprocess, "run") as run:
            with pytest.raises(LbbLocalSearchRetiredError) as caught:
                getattr(client, method)(UnreadableRequest())
    request_file.assert_not_called()
    run.assert_not_called()
    error = caught.value
    assert error.code == "legacy_local_search_retired"
    assert error.retired_in == "0.33.0"
    assert error.method == method
    assert "RDF/SPARQL" in str(error)
    assert "class-declared embeddings" in str(error)
    assert "no automatic conversion" in str(error)
    assert "PRIVATE" not in str(error)


def test_ordinary_local_ontology_request_keeps_existing_subprocess_and_cleanup(
    tmp_path: Path,
) -> None:
    client = LbbLocalClient(root=tmp_path, tenant="tenant", graph="graph", testctl_bin="testctl")
    body = {"query": "ontology term"}
    paths: list[Path] = []

    def execute(command: Sequence[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        assert list(command[:3]) == ["testctl", "ontology", "search"]
        assert "--root" in command and "--tenant" in command and "--graph" in command
        path = Path(command[command.index("--file") + 1])
        assert json.loads(path.read_text()) == body
        paths.append(path)
        assert kwargs["check"] is False
        return subprocess.CompletedProcess(command, 0, '{"matches": []}', "")

    with mock.patch.object(lbb.local.subprocess, "run", side_effect=execute) as run:
        assert client.ontology_search(body) == {"matches": []}
    assert run.call_count == 1
    assert len(paths) == 1 and not paths[0].exists()
