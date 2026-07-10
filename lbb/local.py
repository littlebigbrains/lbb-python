from __future__ import annotations

import json
import os
import subprocess
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast


class LbbCommandError(RuntimeError):
    def __init__(
        self,
        args: Sequence[str],
        returncode: int,
        stdout: str,
        stderr: str,
    ) -> None:
        self.args_list = list(args)
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr
        super().__init__(
            f"lbb command failed with exit code {returncode}: {' '.join(args)}\n{stderr}"
        )


@dataclass(frozen=True)
class LbbLocalClient:
    root: str | Path
    tenant: str
    graph: str
    branch: str = "main"
    repo_root: str | Path | None = None
    testctl_bin: str | Path | None = None
    # May carry credentials (e.g. object-storage keys for `storage
    # conformance`) — kept out of repr so logs and tracebacks never print it.
    env: Mapping[str, str] | None = field(default=None, repr=False)

    def health(self) -> dict[str, Any]:
        return self._run("health")

    def storage_conformance(
        self,
        prefix: str = "lbb/test-runs/python-sdk/conformance",
    ) -> dict[str, Any]:
        return self._run(
            "storage",
            "conformance",
            "--root",
            str(self.root),
            "--prefix",
            prefix,
        )

    def create_graph(self) -> dict[str, Any]:
        return self._run("graph", "create", *self._scope_args())

    def commit_triplets_file(
        self,
        path: str | Path,
        request_id: str | None = None,
        *,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        args = [
            "graph",
            "triplets",
            "commit",
            *self._scope_args(),
            "--file",
            str(path),
        ]
        key = idempotency_key if idempotency_key is not None else request_id
        if key is not None:
            args.extend(["--idempotency-key", key])
        return self._run(*args)

    def ontology_search(self, request: Mapping[str, Any]) -> dict[str, Any]:
        return self._request_file(("ontology", "search", *self._scope_args()), request)

    def ontology_resolve(self, request: Mapping[str, Any]) -> dict[str, Any]:
        return self._request_file(("ontology", "resolve", *self._scope_args()), request)

    def semantic_search(self, request: Mapping[str, Any]) -> dict[str, Any]:
        return self._request_file(("graph", "search", *self._scope_args()), request)

    def build_embedding_index(self, request: Mapping[str, Any]) -> dict[str, Any]:
        return self._request_file(
            ("graph", "embeddings", "build", *self._scope_args()),
            request,
        )

    def embedding_index_inspect(self, request: Mapping[str, Any]) -> dict[str, Any]:
        return self._request_file(
            ("graph", "embeddings", "inspect", *self._scope_args()),
            request,
        )

    def inspect_embedding_index(self, request: Mapping[str, Any]) -> dict[str, Any]:
        return self.embedding_index_inspect(request)

    def embedding_search(self, request: Mapping[str, Any]) -> dict[str, Any]:
        return self._request_file(
            ("graph", "embeddings", "search", *self._scope_args()),
            request,
        )

    def traverse(self, request: Mapping[str, Any]) -> dict[str, Any]:
        return self._request_file(("query", "traverse", *self._scope_args()), request)

    def current_state(self, request: Mapping[str, Any]) -> dict[str, Any]:
        return self._request_file(("query", "state", *self._scope_args()), request)

    def relationship_history(self, request: Mapping[str, Any]) -> dict[str, Any]:
        return self._request_file(("query", "history", *self._scope_args()), request)

    def _scope_args(self) -> tuple[str, ...]:
        return (
            "--root",
            str(self.root),
            "--tenant",
            self.tenant,
            "--graph",
            self.graph,
            "--branch",
            self.branch,
        )

    def _request_file(
        self,
        command: Sequence[str],
        request: Mapping[str, Any],
    ) -> dict[str, Any]:
        with tempfile.NamedTemporaryFile(
            "w",
            encoding="utf-8",
            suffix=".json",
            delete=False,
        ) as handle:
            json.dump(request, handle)
            handle.write("\n")
            request_path = Path(handle.name)
        try:
            return self._run(*command, "--file", str(request_path))
        finally:
            request_path.unlink(missing_ok=True)

    def _base_command(self) -> list[str]:
        if self.testctl_bin is not None:
            return [str(self.testctl_bin)]
        return ["cargo", "run", "-p", "lbb-testctl", "--"]

    def _run(self, *args: str) -> dict[str, Any]:
        command = [*self._base_command(), *args]
        env = os.environ.copy()
        if self.env is not None:
            env.update(self.env)
        result = subprocess.run(
            command,
            cwd=str(self.repo_root) if self.repo_root is not None else None,
            env=env,
            text=True,
            capture_output=True,
            check=False,
        )
        if result.returncode != 0:
            raise LbbCommandError(command, result.returncode, result.stdout, result.stderr)
        output = result.stdout.strip()
        if not output:
            return {}
        return cast(dict[str, Any], json.loads(output))
