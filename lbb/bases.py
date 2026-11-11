"""The terms of a developer's base ontology, with the IRIs SPARQL uses for them.

A base lives in the environment, not in the SDK, so its constants come from its
stored document: ``client.ontology.bases.terms("acme")``. The terms are keyed by
local name. ``name``, ``super_types``, ``domain``, ``range`` and ``inverse`` hold
the qualified names (``acme.Agreement``) and ``iri`` the IRI of the qualified
name. ``database/scripts/gen-starter-terms.mjs --base`` writes the same terms as
a module with attribute access. One fixed document pins both forms
(``contracts/ontology-bases/acme.json``).
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from .starters import (
    StarterClass,
    StarterProperty,
    StarterQuestion,
    StarterRelation,
)

__all__ = ["StarterTerms", "base_terms", "iri_segment", "qualified_name"]

_CLASS_NS = "https://littlebigbrain.com/class/"
_PROPERTY_NS = "https://littlebigbrain.com/p/"
_RELATION_NS = "https://littlebigbrain.com/r/"
_UNRESERVED = frozenset("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_.~")


@dataclass(frozen=True)
class StarterTerms:
    """The terms of one base, keyed by local name."""

    id: str
    version: str
    label: str
    classes: Mapping[str, StarterClass]
    properties: Mapping[str, StarterProperty]
    relations: Mapping[str, StarterRelation]
    questions: tuple[StarterQuestion, ...]


def iri_segment(name: str) -> str:
    """The IRI local part the RDF projection mints for a name.

    The name is trimmed, lowercased and its inner whitespace collapsed; every
    byte outside ``A-Z a-z 0-9 - _ . ~`` is percent-encoded.
    """
    normalized = " ".join(name.strip().lower().split())
    return "".join(
        char if char in _UNRESERVED else "".join(f"%{byte:02X}" for byte in char.encode())
        for char in normalized
    )


def qualified_name(prefix: str, local: str) -> str:
    """The qualified name of a base's term: ``acme.Agreement``."""
    return f"{prefix}.{local}"


def _get(item: Any, key: str, default: Any = None) -> Any:
    if isinstance(item, Mapping):
        return item.get(key, default)
    return getattr(item, key, default)


def base_terms(document: Any) -> StarterTerms:
    """The terms of a base document as stored (local names).

    ``document`` is the ``document`` of ``bases.get`` (a mapping or the
    ``OntologyStarterDocument`` model). Its ``id`` is the prefix.
    """
    prefix = _get(document, "id")

    def q(local: str) -> str:
        return qualified_name(prefix, local)

    classes: dict[str, StarterClass] = {}
    for cls in _get(document, "classes", []):
        name = _get(cls, "name")
        supers = _get(cls, "superTypes") or _get(cls, "super_types") or []
        classes[name] = StarterClass(
            name=q(name),
            iri=_CLASS_NS + iri_segment(q(name)),
            super_types=tuple(q(parent) for parent in supers),
        )
    properties: dict[str, StarterProperty] = {}
    for prop in _get(document, "properties", []):
        name = _get(prop, "name")
        value_type = _get(prop, "type")
        properties[name] = StarterProperty(
            name=q(name),
            iri=_PROPERTY_NS + iri_segment(q(name)),
            value_type=str(getattr(value_type, "value", value_type)),
        )
    relations: dict[str, StarterRelation] = {}
    for relation in _get(document, "relations", []):
        name = _get(relation, "name")
        inverse = _get(relation, "inverse")
        relations[name] = StarterRelation(
            name=q(name),
            iri=_RELATION_NS + iri_segment(q(name)),
            domain=tuple(q(item) for item in _get(relation, "domain", [])),
            range=tuple(q(item) for item in _get(relation, "range", [])),
            cardinality=str(_get(relation, "cardinality")),
            inverse=q(inverse) if inverse else None,
            inverse_iri=_RELATION_NS + iri_segment(q(inverse)) if inverse else None,
        )
    questions = tuple(
        StarterQuestion(
            id=_get(question, "id"),
            text=_get(question, "text"),
            sparql=_get(question, "sparql"),
        )
        for question in _get(document, "questions", [])
    )
    return StarterTerms(
        id=prefix,
        version=_get(document, "version"),
        label=_get(document, "label"),
        classes=classes,
        properties=properties,
        relations=relations,
        questions=questions,
    )
