"""Python SDK for little big brain.

The HTTP client (:class:`LbbClient`, :class:`AsyncLbbClient`) is the primary
integration path. The legacy :class:`LbbLocalClient` shells out to
``lbb-testctl`` for local tests/notebooks and lives in :mod:`lbb.local`.
Generated request/response types are in :mod:`lbb.models`; selected client
methods also expose ``*_model`` and ``*_page`` helpers for validated Pydantic
responses.
"""

from ._catalog import CatalogUsageProfile
from ._version import __version__ as __version__
from ._workflow_execution import AsyncWorkflowTurnReader, WorkflowExecutionError, WorkflowTurnReader
from .client import (
    RESET,
    AsyncLbbClient,
    LbbCapabilityError,
    LbbClient,
    LbbError,
    ListPage,
    QueryAskResult,
    QueryAskStreamEvent,
    RawLbbResponse,
    RequestOptions,
    RetryEvent,
    SparqlResults,
)
from .entities import entity_id_from_iri, entity_ref
from .local import LbbCommandError, LbbLocalClient

__all__ = [
    "LbbClient",
    "CatalogUsageProfile",
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
