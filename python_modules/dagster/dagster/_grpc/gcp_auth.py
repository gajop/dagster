from functools import cache
from threading import Lock

from google.auth.credentials import Credentials, TokenState
from google.auth.transport.requests import Request
from google.oauth2.id_token import fetch_id_token_credentials

import dagster._check as check


class _IdentityTokenProvider:
    def __init__(self, audience: str):
        self._audience = audience
        self._lock = Lock()
        self._request = Request()
        self._credentials: Credentials | None = None

    def get_token(self) -> str:
        with self._lock:
            if self._credentials is None:
                self._credentials = fetch_id_token_credentials(
                    self._audience, request=self._request
                )
            if self._credentials.token_state is not TokenState.FRESH:
                self._credentials.refresh(self._request)
            return check.not_none(self._credentials.token)


@cache
def gcp_id_token_provider(audience: str) -> _IdentityTokenProvider:
    # Workspace refreshes and gRPC channels are short-lived. Keep the token per audience.
    return _IdentityTokenProvider(audience)
