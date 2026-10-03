"""The Google Drive OAuth helpers for a developer's server."""

from __future__ import annotations

import asyncio
import unittest
from typing import Any
from urllib.parse import parse_qs, urlsplit

import httpx

from lbb import google_drive

SCOPE = "https://www.googleapis.com/auth/drive.readonly"
INPUT: dict[str, Any] = {
    "client_id": "client-1.apps.googleusercontent.com",
    "client_secret": "secret-1",
    "redirect_uri": "https://app.example.com/google/callback",
    "code": "code-1",
}


def token_endpoint(
    answers: list[tuple[int, Any]], seen: list[httpx.Request]
) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        status, body = answers.pop(0)
        if isinstance(body, str):
            return httpx.Response(status, text=body)
        return httpx.Response(status, json=body)

    return httpx.MockTransport(handler)


class GoogleDriveTests(unittest.TestCase):
    def test_the_consent_url_asks_for_offline_read_access_to_drive(self) -> None:
        url = google_drive.authorize_url(
            client_id=INPUT["client_id"],
            redirect_uri=INPUT["redirect_uri"],
            state="state-1",
            login_hint="ana@example.com",
            code_challenge="challenge-1",
        )
        parts = urlsplit(url)
        self.assertEqual(
            f"{parts.scheme}://{parts.netloc}{parts.path}",
            "https://accounts.google.com/o/oauth2/v2/auth",
        )
        self.assertEqual(
            {key: values[0] for key, values in parse_qs(parts.query).items()},
            {
                "client_id": INPUT["client_id"],
                "redirect_uri": INPUT["redirect_uri"],
                "response_type": "code",
                "scope": SCOPE,
                "access_type": "offline",
                "prompt": "consent",
                "include_granted_scopes": "true",
                "state": "state-1",
                "login_hint": "ana@example.com",
                "code_challenge": "challenge-1",
                "code_challenge_method": "S256",
            },
        )

    def test_the_code_becomes_a_google_drive_connections_credentials(self) -> None:
        seen: list[httpx.Request] = []
        answers: list[tuple[int, Any]] = [
            (200, {"access_token": "a", "refresh_token": "r-1", "scope": f"openid {SCOPE}"})
        ]
        with httpx.Client(transport=token_endpoint(answers, seen)) as http:
            grant = google_drive.exchange_code(**INPUT, code_verifier="verifier-1", http=http)
        self.assertEqual(
            grant.credentials,
            {
                "GOOGLE_CLIENT_ID": INPUT["client_id"],
                "GOOGLE_CLIENT_SECRET": "secret-1",
                "GOOGLE_REFRESH_TOKEN": "r-1",
            },
        )
        self.assertEqual(grant.scope, f"openid {SCOPE}")
        (request,) = seen
        self.assertEqual(str(request.url), "https://oauth2.googleapis.com/token")
        self.assertEqual(
            {key: values[0] for key, values in parse_qs(request.content.decode()).items()},
            {
                "grant_type": "authorization_code",
                "code": "code-1",
                "redirect_uri": INPUT["redirect_uri"],
                "client_id": INPUT["client_id"],
                "client_secret": "secret-1",
                "code_verifier": "verifier-1",
            },
        )

    def test_failures_carry_their_code(self) -> None:
        seen: list[httpx.Request] = []
        answers: list[tuple[int, Any]] = [
            (400, {"error": "invalid_grant", "error_description": "Bad Request"}),
            (500, "<html>oops</html>"),
            (200, {"access_token": "a", "scope": SCOPE}),
            (200, {"access_token": "a", "refresh_token": "r", "scope": "openid"}),
        ]
        expected = [
            ("invalid_grant", "Google refused the code (400): Bad Request"),
            ("http_error", "Google refused the code (500)"),
            ("no_refresh_token", "Google returned no refresh token"),
            ("scope_not_granted", "The person did not grant read access"),
        ]
        with httpx.Client(transport=token_endpoint(answers, seen)) as http:
            for code, message in expected:
                with self.assertRaises(google_drive.GoogleOAuthError) as caught:
                    google_drive.exchange_code(**INPUT, http=http)
                self.assertEqual(caught.exception.code, code)
                self.assertIn(message, str(caught.exception))

    def test_the_async_exchange_matches(self) -> None:
        seen: list[httpx.Request] = []
        answers: list[tuple[int, Any]] = [
            (200, {"access_token": "a", "refresh_token": "r-2", "scope": SCOPE})
        ]

        async def run() -> google_drive.Grant:
            async with httpx.AsyncClient(transport=token_endpoint(answers, seen)) as http:
                return await google_drive.exchange_code_async(**INPUT, http=http)

        grant = asyncio.run(run())
        self.assertEqual(grant.credentials["GOOGLE_REFRESH_TOKEN"], "r-2")


if __name__ == "__main__":
    unittest.main()
