"""Characterization tests for the import surface users install from PyPI."""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version

import lbb
import lbb.client


def test_public_exports_are_explicit_and_stable() -> None:
    assert lbb.__all__ == [
        "LbbClient",
        "AsyncLbbClient",
        "LbbError",
        "IndexLineageObservation",
        "ListPage",
        "RawLbbResponse",
        "RequestOptions",
        "RetryEvent",
        "SparqlResults",
        "LbbLocalClient",
        "LbbCommandError",
    ]


def test_package_version_and_primary_clients_are_available() -> None:
    assert lbb.__version__ == "0.8.1"
    try:
        distribution_version = version("littlebigbrain")
    except PackageNotFoundError:
        # A source-tree-only test run has no installed distribution metadata;
        # CI installs the package and exercises the equality below.
        distribution_version = lbb.__version__
    assert distribution_version == lbb.__version__
    assert lbb.LbbClient.__name__ == "LbbClient"
    assert lbb.AsyncLbbClient.__name__ == "AsyncLbbClient"
    assert lbb.RequestOptions.__name__ == "RequestOptions"


def test_client_module_keeps_the_documented_import_surface() -> None:
    assert lbb.client.LbbClient is lbb.LbbClient
    assert lbb.client.AsyncLbbClient is lbb.AsyncLbbClient
    assert lbb.client.LbbError is lbb.LbbError
    assert lbb.client.ListPage is lbb.ListPage
    assert lbb.client.RawLbbResponse is lbb.RawLbbResponse
    assert lbb.client.RequestOptions is lbb.RequestOptions
    assert lbb.client.SparqlResults is lbb.SparqlResults
