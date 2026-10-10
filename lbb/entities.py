"""Commit references of entities known by their IRI.

A starter message names its instances by IRI, and so does a SPARQL answer.
An entity's IRI is ``ENTITY_IRI_NAMESPACE`` and the entity's 32-hex id. A
commit endpoint, a field row or a ``replace_owned`` subject whose ``key`` is
``ENTITY_ID_KEY_SENTINEL`` followed by that id addresses the entity
directly, a keyed one included.
"""

from __future__ import annotations

import re
from typing import Any

ENTITY_IRI_NAMESPACE = "https://littlebigbrain.com/e/"
"""The namespace of entity IRIs: an entity's IRI is this and its 32-hex id."""

ENTITY_ID_KEY_SENTINEL = "\u0000lbb-eid:"
"""A commit ``key`` that starts with this addresses the entity by its id."""

_ENTITY_ID = re.compile(r"[0-9a-f]{32}")


def entity_id_from_iri(iri: str) -> str:
    """The 32-hex id of an entity IRI (``https://littlebigbrain.com/e/<id>``),
    in lower case.

    Raises ``ValueError`` for an IRI of another namespace or a malformed id.
    """
    if not isinstance(iri, str) or not iri.startswith(ENTITY_IRI_NAMESPACE):
        raise ValueError(
            f"not an entity IRI (expected {ENTITY_IRI_NAMESPACE}<32 hex>): {iri!r}"
        )
    entity_id = iri[len(ENTITY_IRI_NAMESPACE) :].lower()
    if not _ENTITY_ID.fullmatch(entity_id):
        raise ValueError(f"the id of an entity IRI is 32 hexadecimal characters: {iri!r}")
    return entity_id


def entity_ref(iri: str, type: str, name: str | None = None) -> dict[str, Any]:
    """The commit reference (``NamedEntityInput``) of the entity an IRI names.

    Its ``key`` is ``ENTITY_ID_KEY_SENTINEL`` and the entity's id, so the
    commit addresses that entity directly. ``type`` is the entity's class.

    ``name`` is the entity's display name; the default is its id. In
    ``replace_owned`` (``subjects``, ``retract_entities``) only the id
    counts. A triplet endpoint or a field row writes its name as the
    entity's display name, so pass the entity's current name there::

        subjects = [entity_ref(iri, "DocEntry") for iri in message["instances"]]
    """
    entity_id = entity_id_from_iri(iri)
    if not isinstance(type, str) or not type.strip():
        raise ValueError("entity_ref needs the entity's class as its type")
    return {
        "type": type,
        "name": name if name and name.strip() else entity_id,
        "key": f"{ENTITY_ID_KEY_SENTINEL}{entity_id}",
    }


__all__ = [
    "ENTITY_ID_KEY_SENTINEL",
    "ENTITY_IRI_NAMESPACE",
    "entity_id_from_iri",
    "entity_ref",
]
