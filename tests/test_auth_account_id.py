from datetime import datetime, UTC

from app.api.auth import _to_public_token_response


def test_to_public_token_response_includes_account_id() -> None:
    response = _to_public_token_response(
        {
            "expires_in": 3600,
            "scope": "scope",
            "token_type": "Bearer",
            "expires_at": datetime.now(UTC),
            "refreshed": False,
            "account_id": "user@example.com",
        }
    )

    assert response.account_id == "user@example.com"
