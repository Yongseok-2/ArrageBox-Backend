import pytest
from fastapi import HTTPException
from app.api.email import _resolve_access_token


def test_resolve_access_token_prefers_body() -> None:
    assert _resolve_access_token("body-token", "cookie-token") == "body-token"


def test_resolve_access_token_uses_cookie_when_body_missing() -> None:
    assert _resolve_access_token(None, "cookie-token") == "cookie-token"


def test_resolve_access_token_rejects_missing_token() -> None:
    with pytest.raises(HTTPException):
        _resolve_access_token(None, None)
