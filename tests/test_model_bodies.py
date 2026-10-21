"""A request body given as a generated model goes on the wire under the
model's wire names, without its unset optional fields."""

from __future__ import annotations

import httpx

from lbb import LbbClient, LbbError, models


def test_a_model_body_uses_the_wire_names_of_its_fields() -> None:
    seen: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={})

    with LbbClient("https://example.test", transport=httpx.MockTransport(handle)) as client:
        client.raw_request(
            "POST",
            "/v1/workflows/activities/reason",
            body=models.ReasonActivityRequest(prompt="p", schema={"type": "object"}),
        )
    # `schema_` is the Python name of the wire field `schema`.
    assert seen[0].content == b'{"prompt":"p","schema":{"type":"object"}}'


def test_a_budget_refusal_names_when_to_run_again() -> None:
    def handle(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            429,
            json={
                "error": {
                    "type": "rate_limit_error",
                    "code": "workflow_budget_exceeded",
                    "message": "spent",
                    "retryable": False,
                    "retry_at_ms": 1_800_000_000_000,
                }
            },
        )

    with LbbClient("https://example.test", transport=httpx.MockTransport(handle)) as client:
        try:
            client.raw_request("GET", "/v1/graph/status")
        except LbbError as error:
            assert error.code == "workflow_budget_exceeded"
            assert error.retry_at_ms == 1_800_000_000_000
        else:
            raise AssertionError("expected a 429")
