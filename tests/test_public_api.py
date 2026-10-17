"""Characterization tests for the import surface users install from PyPI."""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version

import lbb
import lbb.client
from lbb import models


def test_public_exports_are_explicit_and_stable() -> None:
    assert lbb.__all__ == [
        "LbbClient",
        "WorkflowTurnReader",
        "AsyncWorkflowTurnReader",
        "WorkflowExecutionError",
        "AsyncLbbClient",
        "LbbError",
        "LbbCapabilityError",
        "ListPage",
        "RawLbbResponse",
        "RequestOptions",
        "RetryEvent",
        "SparqlResults",
        "QueryAskResult",
        "QueryAskStreamEvent",
        "RESET",
        "entity_ref",
        "entity_id_from_iri",
        "LbbLocalClient",
        "LbbCommandError",
    ]


def test_package_version_and_primary_clients_are_available() -> None:
    assert lbb.__version__ == "0.28.0"
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
    assert lbb.client.LbbCapabilityError is lbb.LbbCapabilityError
    assert lbb.client.ListPage is lbb.ListPage
    assert lbb.client.RawLbbResponse is lbb.RawLbbResponse
    assert lbb.client.RequestOptions is lbb.RequestOptions
    assert lbb.client.SparqlResults is lbb.SparqlResults
    assert lbb.client.QueryAskResult is lbb.QueryAskResult
    assert lbb.client.QueryAskStreamEvent is lbb.QueryAskStreamEvent
    assert lbb.client.RESET is lbb.RESET
    assert repr(lbb.RESET) == "RESET"


def test_feedback_kind_enum_names_survive_new_application_schemas() -> None:
    # These generated names were already importable before application grants.
    # A new anonymous `kind` enum must not silently change their meanings.
    existing = (
        (models.Kind14, "entity", models.SearchFeedbackTarget1),
        (models.Kind15, "assertion", models.SearchFeedbackTarget2),
        (models.Kind16, "observation", models.SearchFeedbackTarget3),
        (models.Kind17, "concept", models.SearchFeedbackTarget4),
    )
    for enum_type, value, target in existing:
        assert enum_type(value).value == value
        assert target.model_fields["kind"].annotation is enum_type
