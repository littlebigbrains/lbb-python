"""Google Drive for a developer's end customers.

The two OAuth steps a developer's server runs with its own Google app, before
it creates a ``google_drive`` connection with ``integrations.create``::

    from lbb import google_drive

    url = google_drive.authorize_url(
        client_id=client_id, redirect_uri=redirect_uri, state=state
    )
    # On the callback, after ``state`` matches:
    grant = google_drive.exchange_code(
        client_id=client_id,
        client_secret=client_secret,
        redirect_uri=redirect_uri,
        code=code,
    )
    client.integrations.create(
        graph=graph, id="google-drive", kind="google_drive",
        credentials=grant.credentials,
    )

The connection renews its access token from the refresh token before each
sync. Nothing here talks to LBB.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Final
from urllib.parse import urlencode

import httpx

SCOPE: Final = "https://www.googleapis.com/auth/drive.readonly"
"""Read access to every file the person can read: the scope the connector needs."""
AUTHORIZE_URL: Final = "https://accounts.google.com/o/oauth2/v2/auth"
TOKEN_URL: Final = "https://oauth2.googleapis.com/token"


class GoogleOAuthError(Exception):
    """The exchange failed.

    ``code`` is Google's OAuth error (``invalid_grant`` for a used or expired
    code), or ``no_refresh_token`` or ``scope_not_granted``.
    """

    def __init__(self, code: str, message: str, status: int | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.status = status


@dataclass(frozen=True)
class Grant:
    """What :func:`exchange_code` returns."""

    credentials: dict[str, str]
    """Send these to ``integrations.create`` with ``kind="google_drive"``."""
    scope: str
    """The scopes the person granted, space-separated."""


def authorize_url(
    *,
    client_id: str,
    redirect_uri: str,
    state: str,
    login_hint: str | None = None,
    code_challenge: str | None = None,
) -> str:
    """The Google consent URL for one person.

    It asks for ``drive.readonly`` with offline access and a consent prompt,
    so the code always gives a refresh token, also to a person who granted
    access before. ``state`` is an unguessable value you keep with the
    person's session and check on the callback; ``code_challenge`` is a PKCE
    S256 challenge.
    """
    params = {
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "response_type": "code",
        "scope": SCOPE,
        "access_type": "offline",
        "prompt": "consent",
        "include_granted_scopes": "true",
        "state": state,
    }
    if login_hint:
        params["login_hint"] = login_hint
    if code_challenge:
        params["code_challenge"] = code_challenge
        params["code_challenge_method"] = "S256"
    return f"{AUTHORIZE_URL}?{urlencode(params)}"


def _form(
    client_id: str,
    client_secret: str,
    redirect_uri: str,
    code: str,
    code_verifier: str | None,
) -> dict[str, str]:
    form = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": redirect_uri,
        "client_id": client_id,
        "client_secret": client_secret,
    }
    if code_verifier:
        form["code_verifier"] = code_verifier
    return form


def _grant(response: httpx.Response, client_id: str, client_secret: str) -> Grant:
    try:
        body: Any = response.json()
    except ValueError:
        body = {}
    if not isinstance(body, dict):
        body = {}
    if response.status_code >= 400:
        code = body.get("error") if isinstance(body.get("error"), str) else "http_error"
        said = body.get("error_description")
        detail = f": {said}" if isinstance(said, str) and said else ""
        raise GoogleOAuthError(
            code,
            f"Google refused the code ({response.status_code}){detail}",
            response.status_code,
        )
    scope = body.get("scope") if isinstance(body.get("scope"), str) else ""
    if SCOPE not in scope.split():
        raise GoogleOAuthError(
            "scope_not_granted", "The person did not grant read access to Google Drive"
        )
    refresh = body.get("refresh_token")
    if not isinstance(refresh, str) or not refresh:
        raise GoogleOAuthError(
            "no_refresh_token",
            "Google returned no refresh token: ask with access_type=offline and prompt=consent",
        )
    return Grant(
        credentials={
            "GOOGLE_CLIENT_ID": client_id,
            "GOOGLE_CLIENT_SECRET": client_secret,
            "GOOGLE_REFRESH_TOKEN": refresh,
        },
        scope=scope,
    )


def exchange_code(
    *,
    client_id: str,
    client_secret: str,
    redirect_uri: str,
    code: str,
    code_verifier: str | None = None,
    http: httpx.Client | None = None,
) -> Grant:
    """Exchange the callback's code for the connection's credentials.

    Raises :class:`GoogleOAuthError` when Google refuses the code, returns no
    refresh token, or the person did not grant ``drive.readonly``.
    """
    form = _form(client_id, client_secret, redirect_uri, code, code_verifier)
    headers = {"accept": "application/json"}
    if http is not None:
        response = http.post(TOKEN_URL, data=form, headers=headers)
    else:
        with httpx.Client(timeout=30) as owned:
            response = owned.post(TOKEN_URL, data=form, headers=headers)
    return _grant(response, client_id, client_secret)


async def exchange_code_async(
    *,
    client_id: str,
    client_secret: str,
    redirect_uri: str,
    code: str,
    code_verifier: str | None = None,
    http: httpx.AsyncClient | None = None,
) -> Grant:
    """:func:`exchange_code` as a coroutine."""
    form = _form(client_id, client_secret, redirect_uri, code, code_verifier)
    headers = {"accept": "application/json"}
    if http is not None:
        response = await http.post(TOKEN_URL, data=form, headers=headers)
    else:
        async with httpx.AsyncClient(timeout=30) as owned:
            response = await owned.post(TOKEN_URL, data=form, headers=headers)
    return _grant(response, client_id, client_secret)
