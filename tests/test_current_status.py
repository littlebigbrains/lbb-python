"""Explicit current status refuses silently ignored response versions."""

import httpx
import pytest

from lbb import models
from lbb.client import AsyncLbbClient, LbbCapabilityError, LbbClient


def current_payload():
    return {
        "object": "status",
        "version": "status/1",
        "ok": True,
        "complete": False,
        "service": "lbb-server",
        "mode": "single",
        "uptime_seconds": 1,
        "bind": "127.0.0.1:7400",
        "storage": "local-fs",
        "auth_required": False,
        "graph": {"tenant_id": "acme", "graph_id": "research", "branch_id": "main"},
        "graph_status": {
            "epoch": 3,
            "complete": False,
            "rdf": {
                "availability": "available",
                "issue": None,
                "snapshot": {"commit_seq": 7, "compacted_seq": 0, "served_at_seq": 5},
                "publication_caught_up": False,
                "published_lag_commits": 2,
            },
            "class_embeddings": {
                "availability": "unavailable",
                "issue": "missing",
                "complete": False,
                "configured_count": None,
                "attempted_count": 0,
                "inspected_count": 0,
                "model_id": None,
                "next_model_id": None,
                "embeddings": [],
                "max_embeddings": 16,
                "metadata_budget_bytes": 16777216,
            },
        },
        "snapshot_cache": {
            "entries": 0,
            "total_bytes": 0,
            "pinned_bytes": 0,
            "pinned_ceiling_bytes": 1,
            "budget_bytes": 1,
        },
    }


def transport(payload, seen):
    def respond(request):
        seen.append(request)
        return httpx.Response(200, json=payload)

    return httpx.MockTransport(respond)


def test_current_status_preserves_unknown_coverage_and_typed_model():
    payload = current_payload()
    seen = []
    with LbbClient(
        "http://h", graph="research", transport=transport(payload, seen)
    ) as client:
        assert client.status(version="status/1") == payload
        typed = client.status_model()
    assert isinstance(typed, models.CurrentStatusResponse)
    assert typed.graph_status.class_embeddings.configured_count is None
    assert typed.graph_status.rdf.publication_caught_up is False
    assert seen[0].url.params["version"] == "status/1"
    assert seen[0].url.params["graph"] == "research"


@pytest.mark.parametrize(
    "payload",
    [
        {"ok": True, "graph_status": {}, "persisted_index": {}},
        {"object": "status", "version": "status/2"},
    ],
)
def test_current_status_refuses_older_or_wrong_version(payload):
    with LbbClient("http://h", transport=transport(payload, [])) as client:
        with pytest.raises(LbbCapabilityError):
            client.status(version="status/1")


def test_current_status_rejects_malformed_current_shape():
    with LbbClient(
        "http://h", transport=transport({"object": "status", "version": "status/1"}, [])
    ) as client:
        with pytest.raises(LbbCapabilityError):
            client.status(version="status/1")


def test_legacy_status_is_explicit_and_unversioned_behavior_is_retained():
    payload = {"ok": True, "graph_status": {}, "persisted_index": {}}
    seen = []
    with LbbClient("http://h", transport=transport(payload, seen)) as client:
        assert client.status() == payload
        assert client.status(version="legacy") == payload
        with pytest.raises(ValueError):
            client.status(version="unsupported")
    assert "version" not in seen[0].url.params
    assert seen[1].url.params["version"] == "legacy"
    with LbbClient("http://h", transport=transport(current_payload(), [])) as client:
        with pytest.raises(LbbCapabilityError):
            client.status(version="legacy")


def test_async_current_status_guards_version_and_validates_model():
    import asyncio

    async def check():
        payload = current_payload()
        async with AsyncLbbClient(
            "http://h", transport=transport(payload, [])
        ) as client:
            assert await client.status(version="status/1") == payload
            assert isinstance(await client.status_model(), models.CurrentStatusResponse)
        async with AsyncLbbClient(
            "http://h", transport=transport({"ok": True}, [])
        ) as client:
            with pytest.raises(LbbCapabilityError):
                await client.status(version="status/1")

    asyncio.run(check())


def populated_payload():
    import copy

    base = current_payload()
    fields = [{"path": ["https://example.org/label"], "name": "label", "max": 1}]
    serving = {
        "version": 1,
        "state": "ready",
        "model_id": "current-model",
        "dim": 2,
        "fields": fields,
        "entries": 0,
        "runs": 0,
        "stored_bytes": 0,
        "embedded_through_seq": 5,
        "usage": {"texts": 0, "tokens_estimate": 0, "cost_usd_estimate": 0},
    }
    building = {
        **copy.deepcopy(serving),
        "version": 2,
        "state": "backfilling",
        "model_id": "next-model",
        "backfill": {
            "target_seq": 5,
            "scanned": 0,
            "embedded": 0,
            "reused": 0,
            "removed": 0,
            "rescan": False,
        },
    }
    base["complete"] = base["graph_status"]["complete"] = True
    base["graph_status"]["class_embeddings"] = {
        "availability": "available",
        "issue": None,
        "complete": True,
        "configured_count": 1,
        "attempted_count": 1,
        "inspected_count": 1,
        "model_id": "current-model",
        "next_model_id": "next-model",
        "max_embeddings": 16,
        "metadata_budget_bytes": 16777216,
        "embeddings": [
            {
                "name": "things",
                "availability": "available",
                "issue": None,
                "status": {
                    "name": "things",
                    "class": "https://example.org/Thing",
                    "created_at": "now",
                    "updated_at": "now",
                    "head_seq": 7,
                    "published_seq": 5,
                    "lag_commits": 2,
                    "recipe": {
                        "class": "https://example.org/Thing",
                        "fields": copy.deepcopy(fields),
                        "model_id": "next-model",
                        "dim": 2,
                        "metric": "cosine",
                        "title": ["https://example.org/label"],
                    },
                    "serving": serving,
                    "building": building,
                },
            }
        ],
    }
    return base


def test_current_status_nested_versions_and_partial_diagnostics_remain_truthful():
    payload = populated_payload()
    with LbbClient("http://h", transport=transport(payload, [])) as client:
        assert client.status(version="status/1") == payload
        typed = client.status_model()
        assert typed.complete is True
        assert typed.graph_status.rdf.publication_caught_up is False
        assert (
            typed.graph_status.class_embeddings.embeddings[0].status.building.model_id
            == "next-model"
        )
    payload["complete"] = payload["graph_status"]["complete"] = False
    roster = payload["graph_status"]["class_embeddings"]
    roster.update(
        complete=False,
        configured_count=2,
        attempted_count=2,
        inspected_count=1,
        issue="missing",
    )
    roster["embeddings"].append(
        {
            "name": "unavailable",
            "availability": "unavailable",
            "issue": "missing",
            "status": None,
        }
    )
    with LbbClient("http://h", transport=transport(payload, [])) as client:
        assert client.status(version="status/1") == payload


_REMOVE = object()
_STATUS_PREFIX = ["graph_status", "class_embeddings", "embeddings", 0, "status"]
_STATUS_MUTATIONS = [
    (["ok"], False),
    (["snapshot_cache", "entries"], True),
    (["graph_status", "epoch"], _REMOVE),
    (["graph_status", "rdf", "issue"], "invalid_issue"),
    (["graph_status", "rdf", "issue"], _REMOVE),
    (["graph_status", "rdf", "publication_caught_up"], True),
    (["graph_status", "rdf", "snapshot", "stale"], None),
    (["graph_status", "class_embeddings", "configured_count"], _REMOVE),
    (["graph_status", "class_embeddings", "attempted_count"], True),
    (["graph_status", "class_embeddings", "inspected_count"], 0),
    (["complete"], False),
    *[
        (_STATUS_PREFIX + path, value)
        for path, value in [
            (["created_at"], _REMOVE),
            (["recipe", "dim"], "2"),
            (["recipe", "metric"], "invalid"),
            (["recipe", "title"], None),
            (["recipe", "fields", 0, "path"], [4]),
            (["serving", "usage", "texts"], True),
            (["serving", "state"], "invalid"),
            (["serving", "stored_bytes"], None),
            (["building", "backfill", "rescan"], None),
        ]
    ],
]


@pytest.mark.parametrize(
    "path,replacement",
    _STATUS_MUTATIONS,
    ids=[".".join(map(str, path)) for path, _ in _STATUS_MUTATIONS],
)
def test_current_status_refuses_coercion_omission_invalid_enums_and_nested_shapes(
    path, replacement
):
    payload = populated_payload()
    owner = payload
    for key in path[:-1]:
        owner = owner[key]
    if replacement is _REMOVE:
        del owner[path[-1]]
    else:
        owner[path[-1]] = replacement
    with LbbClient("http://h", transport=transport(payload, [])) as client:
        with pytest.raises(LbbCapabilityError):
            client.status(version="status/1")
        with pytest.raises(LbbCapabilityError):
            client.status_model()


def test_async_status_uses_the_same_strict_raw_response_guard():
    import asyncio

    async def check():
        payload = current_payload()
        payload["graph_status"]["class_embeddings"]["attempted_count"] = True
        async with AsyncLbbClient(
            "http://h", transport=transport(payload, [])
        ) as client:
            with pytest.raises(LbbCapabilityError):
                await client.status(version="status/1")
            with pytest.raises(LbbCapabilityError):
                await client.status_model()

    asyncio.run(check())


@pytest.mark.parametrize(
    "value",
    [float("nan"), float("inf"), -float("inf")],
    ids=["nan", "positive-infinity", "negative-infinity"],
)
def test_status_refuses_non_finite_numbers_from_a_permissive_json_decoder(value):
    import json

    payload = populated_payload()
    payload["graph_status"]["class_embeddings"]["embeddings"][0]["status"]["serving"][
        "usage"
    ]["cost_usd_estimate"] = value

    # Python's JSON decoder can accept these tokens; bypass httpx's strict
    # json= encoder so the SDK guard must refuse the received observation.
    def respond(_request):
        return httpx.Response(
            200,
            content=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json"},
        )

    with LbbClient("http://h", transport=httpx.MockTransport(respond)) as client:
        with pytest.raises(LbbCapabilityError):
            client.status(version="status/1")
        with pytest.raises(LbbCapabilityError):
            client.status_model()


@pytest.mark.parametrize("api_version", [None, "2026-10-21", "2026-10-27"])
def test_prepared_source_api_default_and_explicit_headers_keep_query_selection(
    api_version,
):
    options = {} if api_version is None else {"api_version": api_version}
    for version in ["status/1", "legacy"]:
        payload = (
            current_payload()
            if version == "status/1"
            else {"ok": True, "graph_status": {}, "persisted_index": {}}
        )
        seen = []
        with LbbClient(
            "http://h", transport=transport(payload, seen), **options
        ) as client:
            assert client.status(version=version) == payload
        assert len(seen) == 1
        assert seen[0].headers["lbb-version"] == (api_version or "2026-10-27")
        assert seen[0].url.params["version"] == version


@pytest.mark.parametrize("api_version", [None, "2026-10-21", "2026-10-27"])
def test_async_prepared_source_api_default_preserves_explicit_header_override(
    api_version,
):
    import asyncio

    async def check():
        options = {} if api_version is None else {"api_version": api_version}
        seen = []
        async with AsyncLbbClient(
            "http://h", transport=transport(current_payload(), seen), **options
        ) as client:
            assert await client.status(version="status/1") == current_payload()
        assert len(seen) == 1
        assert seen[0].headers["lbb-version"] == (api_version or "2026-10-27")
        assert seen[0].url.params["version"] == "status/1"

    asyncio.run(check())
