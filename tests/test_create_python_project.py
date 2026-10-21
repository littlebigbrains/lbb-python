"""``npm create @littlebigbrain -- --template values-extract --lang python``
makes a project whose own test passes. The generator of ``sdk/create`` writes
the project into a temporary folder, and the project's ``pytest`` runs there
in a subprocess, on the SDK of this tree (``PYTHONPATH``)."""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from lbb._version import __version__

SDK = Path(__file__).resolve().parents[1]
CREATE = SDK.parent / "create" / "bin" / "create-littlebigbrain.mjs"
NODE = shutil.which("node")

pytestmark = [
    pytest.mark.skipif(NODE is None, reason="node is not installed"),
    pytest.mark.skipif(not CREATE.is_file(), reason="sdk/create is not in this tree"),
]


def test_a_values_extract_project_in_python_passes_its_own_test(tmp_path: Path) -> None:
    assert NODE is not None
    project = tmp_path / "my-values"
    created = subprocess.run(
        [NODE, str(CREATE), "--template", "values-extract", "--lang", "python", str(project)],
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert created.returncode == 0, created.stderr
    assert "python -m values_extract try" in created.stdout

    pyproject = (project / "pyproject.toml").read_text(encoding="utf-8")
    assert re.search(r'^name = "my-values"$', pyproject, re.M), pyproject
    assert f'dependencies = ["littlebigbrain>={__version__}"]' in pyproject

    # The project's pytest, on this tree's SDK. The parent run's options and
    # its coverage settings stay out.
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("PYTEST_", "COV_CORE_", "COVERAGE_"))
    }
    env["PYTHONPATH"] = os.pathsep.join([str(SDK), env.get("PYTHONPATH", "")]).rstrip(os.pathsep)
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    tested = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider"],
        cwd=project,
        env=env,
        capture_output=True,
        text=True,
        timeout=300,
    )
    assert tested.returncode == 0, tested.stdout + tested.stderr
    assert re.search(r"\b2 passed\b", tested.stdout), tested.stdout
