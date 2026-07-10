"""Python SDK for Little Big Brain.

The HTTP client (:class:`LbbClient`, :class:`AsyncLbbClient`) is the primary
integration path. The legacy :class:`LbbLocalClient` shells out to
``lbb-testctl`` for local tests/notebooks and lives in :mod:`lbb.local`.
Generated request/response types are in :mod:`lbb.models`; selected client
methods also expose ``*_model`` and ``*_page`` helpers for validated Pydantic
responses.
"""

from .client import (
    AsyncLbbClient,
    LbbClient,
    LbbError,
    ListPage,
    RawLbbResponse,
    RequestOptions,
    SparqlResults,
)
from .local import LbbCommandError, LbbLocalClient

__all__ = [
    "LbbClient",
    "AsyncLbbClient",
    "LbbError",
    "ListPage",
    "RawLbbResponse",
    "RequestOptions",
    "SparqlResults",
    "LbbLocalClient",
    "LbbCommandError",
]
__version__ = "0.1.0"
