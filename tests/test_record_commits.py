"""Strict version selection, exact retry identity and original record receipts."""

import asyncio
import copy
import json

import httpx
import pytest

from lbb import (
    AsyncLbbClient,
    LbbCapabilityError,
    LbbClient,
    LbbError,
    LbbRecordCommitOutcomeError,
    models,
)


def request_body():
    return {
        "version": "records/1",
        "schema_revision": "immutable",
        "entities": [
            {
                "id": "urn:entity:雪",
                "set": {
                    "urn:integer": 1,
                    "urn:double": 1.0,
                    "urn:bool": True,
                    "urn:iri": {"@id": "urn:target"},
                    "urn:array": [1, 1.0, True, {"@id": "urn:target"}],
                    "urn:large": {
                        "@value": "9007199254740993",
                        "@type": "http://www.w3.org/2001/XMLSchema#integer",
                    },
                    "urn:decimal": {
                        "@value": "1.0000000000000001",
                        "@type": "http://www.w3.org/2001/XMLSchema#decimal",
                    },
                    "urn:time": {
                        "@value": "2026-10-26T10:00:00+02:00",
                        "@type": "http://www.w3.org/2001/XMLSchema#dateTime",
                    },
                    "urn:language": {"@value": "Bonjour", "@language": "fr"},
                },
            }
        ],
        "relationships": [],
    }


def receipt():
    return {
        "object": "record_commit",
        "version": "records/1",
        "idempotent_replay": False,
        "receipt": {
            "epoch": "18446744073709551615",
            "commit_seq": 7,
            "visibility_token": "opaque",
            "schema_revision": "immutable",
            "scope": {"owner": "application", "scope": "default"},
            "resolved_ids": {
                "entities": ["urn:entity:雪"],
                "relationships": ["urn:claim"],
            },
            "no_op": False,
            "projection": {"added": 2, "removed": 0},
        },
    }


CAPABILITY = {"capabilities": ["records_commit_v1"]}


def transport(seen, result=None):
    def respond(request):
        seen.append(request)
        return httpx.Response(
            200, json=CAPABILITY if request.url.path == "/version" else result
        )

    return httpx.MockTransport(respond)


def test_record_commits_exact_terms_models_graph_scope_and_original_replay():
    seen = []
    expected = receipt()
    expected["idempotent_replay"] = True
    with LbbClient(
        "http://h", graph="default", transport=transport(seen, expected)
    ) as client:
        result = client.graph("other").records.commit_model(
            models.RecordCommitRequest.model_validate(request_body()),
            idempotency_key="records:stable",
            expected_graph_epoch="18446744073709551615",
        )
        assert result.model_dump(mode="json") == expected
    sent = seen[1]
    assert sent.url.path == "/v1/graph/commit"
    assert dict(sent.url.params) == {
        "graph": "other",
        "expected_graph_epoch": "18446744073709551615",
    }
    assert sent.headers["idempotency-key"] == "records:stable"
    assert sent.headers["content-type"] == "application/json"
    body = json.loads(sent.content)
    assert body["entities"][0]["set"] == request_body()["entities"][0]["set"]
    assert "owner" not in body and "scope" not in body
    assert body["relationships"] == []


def test_generated_models_preserve_integer_float_bool_and_unsafe_integer_tokens():
    value = request_body()
    value["entities"][0]["set"]["urn:unsafe"] = 9_007_199_254_740_993
    model = models.RecordCommitRequest.model_validate(value)
    seen = []
    with LbbClient("http://h", transport=transport(seen, receipt())) as client:
        client.records.commit(model, idempotency_key="typed")
    # An unsafe integer stays exact on the wire for the server's typed refusal;
    # the SDK must never silently round it or convert an integer into1.0.
    fields = json.loads(seen[-1].content)["entities"][0]["set"]
    assert type(fields["urn:integer"]) is int and fields["urn:integer"] == 1
    assert type(fields["urn:double"]) is float and fields["urn:double"] == 1.0
    assert type(fields["urn:bool"]) is bool and fields["urn:bool"] is True
    assert (
        type(fields["urn:unsafe"]) is int
        and fields["urn:unsafe"] == 9_007_199_254_740_993
    )
    assert fields["urn:large"]["@value"] == "9007199254740993"
    assert fields["urn:iri"] == {"@id": "urn:target"}
    assert [type(item) for item in fields["urn:array"]] == [int, float, bool, dict]
    assert fields["urn:array"] == [1, 1.0, True, {"@id": "urn:target"}]


@pytest.mark.parametrize(
    "advertised",
    [
        {},
        None,
        {"capabilities": None},
        {"capabilities": "records_commit_v1"},
        {"capabilities": {"records_commit_v1": True}},
        {"capabilities": []},
    ],
)
def test_old_or_malformed_capabilities_send_no_post_sync_and_async(advertised):
    def respond(request):
        assert request.method == "GET" and request.url.path == "/version"
        return httpx.Response(200, json=advertised)

    with LbbClient("http://h", transport=httpx.MockTransport(respond)) as client:
        with pytest.raises(LbbCapabilityError):
            client.records.commit(request_body(), idempotency_key="stable")

    async def run():
        async with AsyncLbbClient(
            "http://h", transport=httpx.MockTransport(respond)
        ) as client:
            with pytest.raises(LbbCapabilityError):
                await client.records.commit(request_body(), idempotency_key="stable")

    asyncio.run(run())


@pytest.mark.parametrize(
    "value",
    [{}, {"version": None}, {"version": "legacy"}, [], {"version": "records/2"}],
)
def test_raw_runtime_version_is_required_before_any_request(value):
    def no_request(request):
        raise AssertionError("invalid version sent a request")

    with LbbClient("http://h", transport=httpx.MockTransport(no_request)) as client:
        with pytest.raises(ValueError):
            client.records.commit(value, idempotency_key="stable")

    async def run():
        async with AsyncLbbClient(
            "http://h", transport=httpx.MockTransport(no_request)
        ) as client:
            with pytest.raises(ValueError):
                await client.records.commit(value, idempotency_key="stable")

    asyncio.run(run())


def test_explicit_key_and_header_override_refuse_before_io():
    def no_request(request):
        raise AssertionError("invalid identity sent a request")

    with LbbClient("http://h", transport=httpx.MockTransport(no_request)) as client:
        with pytest.raises(ValueError):
            client.records.commit(request_body(), idempotency_key="")
        with pytest.raises(ValueError):
            client.records.commit(
                request_body(),
                idempotency_key="stable",
                options={"headers": {"IDEMPOTENCY-KEY": "other"}},
            )


def test_sync_retries_freeze_nested_body_key_and_options_before_capability_and_between_attempts():
    value = request_body()
    original = copy.deepcopy(value)
    options = {"headers": {"x-test": "original"}, "max_retries": 2}
    posts = []

    def respond(request):
        if request.url.path == "/version":
            value["entities"] = []
            options["headers"]["x-test"] = "changed"
            return httpx.Response(200, json=CAPABILITY)
        posts.append(request)
        if len(posts) == 1:
            value["relationships"] = [{"id": "changed"}]
            raise httpx.ReadError("lost reply", request=request)
        if len(posts) == 2:
            return httpx.Response(503, json={"error": {"message": "busy"}})
        return httpx.Response(200, json={**receipt(), "idempotent_replay": True})

    with LbbClient(
        "http://h", retry_delay=0, transport=httpx.MockTransport(respond)
    ) as client:
        result = client.records.commit(
            value, idempotency_key="original", expected_graph_epoch="9", options=options
        )
    assert result["idempotent_replay"] is True
    assert len(posts) == 3
    assert all(post.content == posts[0].content for post in posts)
    for post in posts:
        assert json.loads(post.content) == original
        assert post.headers["idempotency-key"] == "original"
        assert post.headers["x-test"] == "original"
        assert post.url.params["expected_graph_epoch"] == "9"


def test_async_freezes_before_await_and_returns_typed_original_result():
    async def run():
        value = request_body()
        original = copy.deepcopy(value)
        options = {"headers": {"x-test": "original"}, "max_retries": 1}
        posts = []

        async def respond(request):
            if request.url.path == "/version":
                return httpx.Response(200, json=CAPABILITY)
            posts.append(request)
            if len(posts) == 1:
                value["relationships"] = [{"id": "changed"}]
                raise httpx.ReadError("lost reply", request=request)
            return httpx.Response(200, json=receipt())

        async with AsyncLbbClient(
            "http://h",
            graph="default",
            retry_delay=0,
            transport=httpx.MockTransport(respond),
        ) as client:
            pending = client.graph("other").records.commit_model(
                value,
                idempotency_key="original",
                expected_graph_epoch="9",
                options=options,
            )
            value["entities"] = []
            options["headers"]["x-test"] = "changed"
            result = await pending
        assert isinstance(result, models.RecordCommitResult)
        assert result.model_dump(mode="json") == receipt()
        assert len(posts) == 2 and posts[0].content == posts[1].content
        for post in posts:
            assert json.loads(post.content) == original
            assert post.url.params["graph"] == "other"
            assert post.headers["x-test"] == "original"

    asyncio.run(run())


def test_typed_graph_refusal_is_preserved_and_legacy_success_is_never_accepted():
    for status, result, error in [
        (
            400,
            {
                "error": {
                    "message": "unsupported graph",
                    "code": "rdf_mutation_not_enabled",
                }
            },
            LbbError,
        ),
        (200, {"commit_seq": 0}, LbbRecordCommitOutcomeError),
    ]:

        def respond(request, status=status, result=result):
            return (
                httpx.Response(200, json=CAPABILITY)
                if request.url.path == "/version"
                else httpx.Response(status, json=result)
            )

        with LbbClient("http://h", transport=httpx.MockTransport(respond)) as client:
            with pytest.raises(error) as captured:
                client.records.commit(request_body(), idempotency_key="stable")
            if isinstance(captured.value, LbbError):
                assert captured.value.code == "rdf_mutation_not_enabled"


@pytest.mark.parametrize("bad", [True, -1, 1.5, "1", None, 2**64])
def test_invalid_receipt_counts_are_not_coerced_by_generated_models(bad):
    for field in ("commit_seq", "added", "removed"):
        result = receipt()
        if field == "commit_seq":
            result["receipt"][field] = bad
        else:
            result["receipt"]["projection"][field] = bad
        with LbbClient("http://h", transport=transport([], result)) as client:
            with pytest.raises(LbbRecordCommitOutcomeError):
                client.records.commit_model(request_body(), idempotency_key="stable")


def test_encoded_version_is_checked_after_custom_mapping_serialization():
    class Misleading(dict):
        def get(self, key, default=None):
            return "records/1" if key == "version" else super().get(key, default)

    def no_request(request):
        raise AssertionError("serializer removed version but a request was sent")

    with LbbClient("http://h", transport=httpx.MockTransport(no_request)) as client:
        with pytest.raises(ValueError):
            client.records.commit(
                Misleading({"triplets": []}), idempotency_key="stable"
            )


def test_async_plain_receipt_and_graph_refusals_keep_the_original_contract():
    async def run():
        for status, result in [
            (200, receipt()),
            (200, {"commit_seq": 0}),
            (
                400,
                {
                    "error": {
                        "code": "rdf_mutation_not_enabled",
                        "message": "unsupported graph",
                    }
                },
            ),
        ]:

            async def respond(request, status=status, result=result):
                if request.url.path == "/version":
                    return httpx.Response(200, json=CAPABILITY)
                return httpx.Response(status, json=result)

            async with AsyncLbbClient(
                "http://h", transport=httpx.MockTransport(respond)
            ) as client:
                if status == 400:
                    with pytest.raises(LbbError) as captured:
                        await client.records.commit(
                            request_body(), idempotency_key="stable"
                        )
                    assert captured.value.code == "rdf_mutation_not_enabled"
                elif "receipt" not in result:
                    with pytest.raises(LbbRecordCommitOutcomeError):
                        await client.records.commit(
                            request_body(), idempotency_key="stable"
                        )
                else:
                    assert (
                        await client.records.commit(
                            request_body(), idempotency_key="stable"
                        )
                        == result
                    )

    asyncio.run(run())


def test_malformed_receipt_is_unknown_with_original_key_after_exactly_one_post():
    for reply in ({"commit_seq": 0}, {**receipt(), "receipt": None}):
        seen = []
        with LbbClient("http://h", transport=transport(seen, reply)) as client:
            with pytest.raises(LbbRecordCommitOutcomeError) as captured:
                client.records.commit(request_body(), idempotency_key="original")
        assert captured.value.outcome == "unknown"
        assert captured.value.idempotency_key == "original"
        assert "may have committed" in str(captured.value)
        assert "upgrade" not in str(captured.value)
        assert len([request for request in seen if request.method == "POST"]) == 1


def test_typed_model_parse_failure_is_also_unknown_and_preserves_key(monkeypatch):
    def fail(*args, **kwargs):
        raise ValueError("incompatible typed result")

    monkeypatch.setattr(models.RecordCommitResult, "model_validate", fail)
    seen = []
    with LbbClient("http://h", transport=transport(seen, receipt())) as client:
        with pytest.raises(LbbRecordCommitOutcomeError) as captured:
            client.records.commit_model(request_body(), idempotency_key="original")
    assert captured.value.idempotency_key == "original"
    assert isinstance(captured.value.__cause__, ValueError)
    assert len([request for request in seen if request.method == "POST"]) == 1


def test_invalid_json_success_retains_unknown_outcome_and_key_sync_and_async():
    seen = []

    def respond(request):
        seen.append(request)
        return (
            httpx.Response(200, json=CAPABILITY)
            if request.url.path == "/version"
            else httpx.Response(
                200, content=b"not json", headers={"content-type": "application/json"}
            )
        )

    with LbbClient("http://h", transport=httpx.MockTransport(respond)) as client:
        with pytest.raises(LbbRecordCommitOutcomeError) as captured:
            client.records.commit(request_body(), idempotency_key="original")
    assert captured.value.idempotency_key == "original"
    assert isinstance(captured.value.__cause__, ValueError)
    assert len([request for request in seen if request.method == "POST"]) == 1
    seen.clear()

    async def run():
        async with AsyncLbbClient(
            "http://h", transport=httpx.MockTransport(respond)
        ) as client:
            with pytest.raises(LbbRecordCommitOutcomeError) as captured:
                await client.records.commit_model(
                    request_body(), idempotency_key="original"
                )
        assert captured.value.idempotency_key == "original"
        assert isinstance(captured.value.__cause__, ValueError)
        assert len([request for request in seen if request.method == "POST"]) == 1

    asyncio.run(run())


@pytest.mark.parametrize("max_retries", [0, 2])
def test_exhausted_post_transport_error_keeps_original_cause_and_key_sync_and_async(
    max_retries,
):
    cause = httpx.ReadTimeout("lost final response")
    seen = []

    def respond(request):
        seen.append(request)
        if request.url.path == "/version":
            return httpx.Response(200, json=CAPABILITY)
        raise cause

    with LbbClient(
        "http://h",
        max_retries=max_retries,
        retry_delay=0,
        transport=httpx.MockTransport(respond),
    ) as client:
        with pytest.raises(LbbRecordCommitOutcomeError) as captured:
            client.records.commit(request_body(), idempotency_key="original")
    assert captured.value.__cause__ is cause
    assert captured.value.idempotency_key == "original"
    assert len([r for r in seen if r.method == "POST"]) == max_retries + 1
    seen.clear()

    async def run():
        async with AsyncLbbClient(
            "http://h",
            max_retries=max_retries,
            retry_delay=0,
            transport=httpx.MockTransport(respond),
        ) as client:
            with pytest.raises(LbbRecordCommitOutcomeError) as captured:
                await client.records.commit_model(
                    request_body(), idempotency_key="original"
                )
        assert captured.value.__cause__ is cause
        assert captured.value.idempotency_key == "original"
        assert len([r for r in seen if r.method == "POST"]) == max_retries + 1

    asyncio.run(run())


def test_capability_transport_loss_is_not_a_record_unknown_outcome_sync_and_async():
    cause = httpx.ConnectError("capability endpoint offline")
    seen = []

    def respond(request):
        seen.append(request)
        assert request.method == "GET"
        raise cause

    with LbbClient(
        "http://h", max_retries=0, transport=httpx.MockTransport(respond)
    ) as client:
        with pytest.raises(httpx.ConnectError) as captured:
            client.records.commit(request_body(), idempotency_key="original")
    assert captured.value is cause and len(seen) == 1
    seen.clear()

    async def run():
        async with AsyncLbbClient(
            "http://h", max_retries=0, transport=httpx.MockTransport(respond)
        ) as client:
            with pytest.raises(httpx.ConnectError) as captured:
                await client.records.commit(request_body(), idempotency_key="original")
        assert captured.value is cause and len(seen) == 1

    asyncio.run(run())


def test_caller_task_cancellation_keeps_cancellation_semantics_after_post():
    seen = []

    async def respond(request):
        seen.append(request)
        if request.url.path == "/version":
            return httpx.Response(200, json=CAPABILITY)
        raise asyncio.CancelledError()

    async def run():
        async with AsyncLbbClient(
            "http://h", transport=httpx.MockTransport(respond)
        ) as client:
            with pytest.raises(asyncio.CancelledError):
                await client.records.commit(request_body(), idempotency_key="original")
        assert len([r for r in seen if r.method == "POST"]) == 1

    asyncio.run(run())
