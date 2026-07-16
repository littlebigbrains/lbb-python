"""Python SDK for little big brain.

The HTTP client (:class:`LbbClient`, :class:`AsyncLbbClient`) is the primary
integration path. The legacy :class:`LbbLocalClient` shells out to
``lbb-testctl`` for local tests/notebooks and lives in :mod:`lbb.local`.
Generated request/response types are in :mod:`lbb.models`; selected client
methods also expose ``*_model`` and ``*_page`` helpers for validated Pydantic
responses.
"""

from ._version import __version__ as __version__
from .client import (
    AsyncLbbClient,
    IndexLineageObservation,
    LbbClient,
    LbbError,
    ListPage,
    RawLbbResponse,
    RequestOptions,
    RetryEvent,
    SparqlResults,
)
from .local import LbbCommandError, LbbLocalClient

__all__ = [
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
