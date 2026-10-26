"""Version, exact-result and one-snapshot guards for the read-only demo helper."""

import asyncio
import json

import httpx
import pytest

from lbb import models
from lbb.client import AsyncLbbClient, LbbCapabilityError, LbbClient


def current():
    cases = []
    for identity in ("identity-store", "auth-outage", "sso-customer"):
        variable = "database" if identity == "identity-store" else "customer"
        iri = f"https://example.org/{identity}"
        cases.append(
            {
                "id": identity,
                "label": identity,
                "query_kind": "sparql_select",
                "query": f"SELECT DISTINCT ?{variable} WHERE {{}} LIMIT 2",
                "variable": variable,
                "expected": [{"@id": iri}],
                "epoch": 4,
                "served_at_seq": 7,
                "outcome": "passed",
                "issue": None,
                "actual_result": {
                    "format": "application/sparql-results+json",
                    "document": json.dumps(
                        {
                            "head": {"vars": [variable]},
                            "results": {
                                "bindings": [{variable: {"type": "uri", "value": iri}}]
                            },
                        }
                    ),
                },
            }
        )
    return {
        "object": "demo_run",
        "version": "demo/1",
        "graph": {"tenant_id": "acme", "graph_id": "research", "branch_id": "main"},
        "dataset_id": "semantic_graph",
        "epoch": 4,
        "served_at_seq": 7,
        "outcome": "passed",
        "passed": True,
        "complete": True,
        "expected_case_count": 3,
        "completed_case_count": 3,
        "case_payloads": "available",
        "cases": cases,
        "issue": None,
        "class_embedding_checks": "not_requested",
        "limits": {
            "max_cases": 3,
            "max_rows_per_case": 2,
            "max_query_bytes": 8192,
            "max_result_bytes": 8192,
            "max_ontology_bytes": 1048576,
            "deadline_millis": 5000,
        },
    }


def transport(payload, seen):
    def respond(request):
        seen.append(request)
        return httpx.Response(
            200,
            json=(
                {"capabilities": ["rdf_demo_checks_v1"]}
                if request.url.path == "/version"
                else payload
            ),
        )

    return httpx.MockTransport(respond)


def test_demo_run_sends_explicit_version_and_supports_named_models():
    payload = current()
    seen = []
    with LbbClient(
        "http://h", graph="research", transport=transport(payload, seen)
    ) as client:
        assert client.demo_run() == payload
        typed = client.demo_run_model()
        assert client.demo_run(dataset=None) == payload
    assert isinstance(typed, models.DemoCurrentRunResponse)
    assert (
        typed.cases[0].actual_result.document
        == payload["cases"][0]["actual_result"]["document"]
    )
    assert seen[0].method == "GET"
    assert seen[1].method == "POST"
    assert dict(seen[1].url.params) == {
        "graph": "research",
        "version": "demo/1",
        "dataset": "semantic_graph",
    }


def invalid_payloads():
    yield {"dataset": {"id": "semantic_graph"}, "passed": True, "cases": []}
    value = current()
    value["version"] = "demo/2"
    yield value
    for path, replacement in [
        (("cases", 1, "epoch"), 5),
        (("cases", 1, "served_at_seq"), 8),
        (("completed_case_count",), 2),
        (("cases", 0, "expected"), []),
    ]:
        value = current()
        target = value
        for key in path[:-1]:
            target = target[key]
        target[path[-1]] = replacement
        yield value
    value = current()
    value["cases"] = []
    value["completed_case_count"] = 0
    yield value
    value = current()
    value["cases"][1]["id"] = value["cases"][0]["id"]
    yield value
    value = current()
    value["cases"][0]["actual_result"]["document"] = value["cases"][0]["actual_result"][
        "document"
    ].replace(
        "https://example.org/identity-store", "https://example.org/identity-store-decoy"
    )
    yield value
    value = current()
    value["cases"][0]["actual_result"]["document"] = "not JSON"
    yield value


@pytest.mark.parametrize("payload", list(invalid_payloads()))
def test_demo_run_refuses_legacy_or_inconsistent_success(payload):
    with LbbClient("http://h", transport=transport(payload, [])) as client:
        with pytest.raises(LbbCapabilityError):
            client.demo_run()


def test_partial_and_late_timeout_cannot_pass_and_literals_stay_lexical():
    payload = current()
    payload.update(
        outcome="unavailable",
        passed=False,
        complete=False,
        issue="timeout",
        cases=[],
        completed_case_count=None,
        case_payloads="withheld",
        epoch=None,
        served_at_seq=None,
    )
    with LbbClient("http://h", transport=transport(payload, [])) as client:
        assert client.demo_run() == payload
    retired = current()
    retired.update(
        outcome="unavailable",
        passed=False,
        complete=False,
        issue="snapshot_changed",
        case_payloads="withheld",
    )
    with LbbClient("http://h", transport=transport(retired, [])) as client:
        with pytest.raises(LbbCapabilityError):
            client.demo_run()

    payload = current()
    payload.update(outcome="unavailable", passed=False, complete=False)
    payload["cases"][0].update(outcome="unavailable", issue="unexpected_term_shape")
    payload["cases"][0]["actual_result"]["document"] = json.dumps(
        {
            "head": {"vars": ["database"]},
            "results": {
                "bindings": [
                    {
                        "database": {
                            "type": "literal",
                            "value": "9007199254740993",
                            "datatype": "http://www.w3.org/2001/XMLSchema#integer",
                        }
                    }
                ]
            },
        }
    )
    with LbbClient("http://h", transport=transport(payload, [])) as client:
        assert (
            client.demo_run_model().cases[0].actual_result.document
            == payload["cases"][0]["actual_result"]["document"]
        )


def test_legacy_selection_is_explicit_and_versions_validate_before_request():
    payload = {"dataset": {"id": "semantic_graph"}, "passed": False, "cases": []}
    seen = []
    with LbbClient("http://h", transport=transport(payload, seen)) as client:
        assert client.demo_run(version="legacy") == payload
        with pytest.raises(ValueError):
            client.demo_run(version="wrong")
    assert seen[0].url.params["version"] == "legacy"
    with LbbClient("http://h", transport=transport(current(), [])) as client:
        with pytest.raises(LbbCapabilityError):
            client.demo_run(version="legacy")


def test_async_demo_guards_and_named_model_match_sync():
    async def check():
        payload = current()
        async with AsyncLbbClient(
            "http://h", transport=transport(payload, [])
        ) as client:
            assert await client.demo_run() == payload
            assert isinstance(
                await client.demo_run_model(), models.DemoCurrentRunResponse
            )
        async with AsyncLbbClient(
            "http://h", transport=transport({"passed": True}, [])
        ) as client:
            with pytest.raises(LbbCapabilityError):
                await client.demo_run()

    asyncio.run(check())


def test_missing_capability_sends_no_demo_or_mutation_post_sync_and_async():
    seen = []

    def old(request):
        seen.append(request)
        return httpx.Response(200, json={"capabilities": []})

    with LbbClient("http://old", transport=httpx.MockTransport(old)) as client:
        with pytest.raises(LbbCapabilityError):
            client.demo_run()
    assert len(seen) == 1 and seen[0].url.path == "/version" and seen[0].method == "GET"
    seen.clear()

    async def check():
        async with AsyncLbbClient(
            "http://old", transport=httpx.MockTransport(old)
        ) as client:
            with pytest.raises(LbbCapabilityError):
                await client.demo_run_model()

    asyncio.run(check())
    assert len(seen) == 1 and seen[0].url.path == "/version" and seen[0].method == "GET"


def test_demo_response_must_match_exact_requested_dataset_including_empty():
    for dataset in ("basic", ""):
        seen = []
        with LbbClient("http://h", transport=transport(current(), seen)) as client:
            with pytest.raises(LbbCapabilityError):
                client.demo_run(dataset=dataset)
        posts = [request for request in seen if request.method == "POST"]
        assert len(posts) == 1 and posts[0].url.params["dataset"] == dataset
