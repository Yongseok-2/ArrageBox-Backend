import os
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import urlencode

import httpx
from fastapi import HTTPException, status

GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
GMAIL_SCOPE = "https://www.googleapis.com/auth/gmail.modify"
PEOPLE_SCOPE = "https://www.googleapis.com/auth/contacts.readonly"
DEFAULT_SCOPES = f"{GMAIL_SCOPE} {PEOPLE_SCOPE}"


class GoogleOAuthService:
    """Google OAuth 토큰 발급/갱신을 담당하는 서비스."""

    def __init__(self) -> None:
        """환경변수 기반 OAuth 클라이언트 설정을 초기화한다."""
        self.token_url = os.getenv("GOOGLE_TOKEN_URL", GOOGLE_TOKEN_URL)
        self.auth_url = os.getenv("GOOGLE_AUTH_URL", GOOGLE_AUTH_URL)
        self.client_id = os.getenv("GOOGLE_CLIENT_ID", "")
        self.client_secret = os.getenv("GOOGLE_CLIENT_SECRET", "")
        raw_redirect_uri = os.getenv("GOOGLE_REDIRECT_URI", "")
        self.redirect_uris: list[str] = [raw_redirect_uri] if raw_redirect_uri else []

    def _validate_config(self) -> None:
        """필수 OAuth 환경변수 존재 여부를 검증한다."""
        if not self.client_id or not self.client_secret:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Google OAuth settings are missing. Check GOOGLE_CLIENT_ID / GOOGLE_CLIENT_SECRET.",
            )

    def _resolve_redirect_uri(self, redirect_uri: str | None) -> str:
        """요청값 또는 기본 설정에서 사용할 redirect URI를 결정한다."""
        if redirect_uri:
            return redirect_uri
        if self.redirect_uris:
            return self.redirect_uris[0]
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Redirect URI is required. Provide it in request or GOOGLE_REDIRECT_URI.",
        )

    def build_authorization_url(
        self, redirect_uri: str | None, state: str | None = None
    ) -> str:
        """Google 동의 화면 URL을 생성한다."""
        self._validate_config()
        query_params = {
            "client_id": self.client_id,
            "redirect_uri": self._resolve_redirect_uri(redirect_uri),
            "response_type": "code",
            "scope": DEFAULT_SCOPES,
            "access_type": "offline",
            "prompt": "consent",
            "include_granted_scopes": "true",
        }
        if state:
            query_params["state"] = state
        return f"{self.auth_url}?{urlencode(query_params)}"

    def _append_expiry(self, token_data: dict[str, Any], refreshed: bool) -> dict[str, Any]:
        """토큰 응답에 expires_at/refreshed 메타데이터를 추가한다."""
        expires_in = int(token_data.get("expires_in", 0))
        expires_at = datetime.now(UTC) + timedelta(seconds=expires_in)
        token_data["expires_at"] = expires_at
        token_data["refreshed"] = refreshed
        return token_data

    async def exchange_code_for_tokens(
        self, code: str, redirect_uri: str | None
    ) -> dict[str, Any]:
        """인가 코드를 access/refresh token으로 교환한다."""
        self._validate_config()
        payload = {
            "code": code,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "redirect_uri": self._resolve_redirect_uri(redirect_uri),
            "grant_type": "authorization_code",
        }
        token_data = await self._request_token(payload)
        return self._append_expiry(token_data, refreshed=False)

    async def refresh_access_token(self, refresh_token: str) -> dict[str, Any]:
        """refresh token으로 새 access token을 발급받는다."""
        self._validate_config()
        payload = {
            "refresh_token": refresh_token,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "refresh_token",
        }
        token_data = await self._request_token(payload)
        if "refresh_token" not in token_data:
            token_data["refresh_token"] = refresh_token
        return self._append_expiry(token_data, refreshed=True)

    async def ensure_valid_access_token(
        self,
        access_token: str | None,
        refresh_token: str,
        expires_at: datetime | None,
    ) -> dict[str, Any]:
        """현재 access token이 유효하면 그대로 반환하고, 아니면 자동 갱신한다."""
        now = datetime.now(UTC)
        if (
            access_token
            and expires_at
            and expires_at.astimezone(UTC) > now + timedelta(seconds=60)
        ):
            return {
                "access_token": access_token,
                "refresh_token": refresh_token,
                "expires_in": int((expires_at.astimezone(UTC) - now).total_seconds()),
                "scope": DEFAULT_SCOPES,
                "token_type": "Bearer",
                "expires_at": expires_at.astimezone(UTC),
                "refreshed": False,
            }
        return await self.refresh_access_token(refresh_token)

    async def _request_token(self, payload: dict[str, str]) -> dict[str, Any]:
        """Google OAuth 토큰 엔드포인트에 요청을 보내고 결과를 반환한다."""
        try:
            async with httpx.AsyncClient(timeout=20) as client:
                response = await client.post(self.token_url, data=payload)
        except httpx.HTTPError as exc:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"Failed to connect Google OAuth server: {exc}",
            ) from exc

        if response.status_code >= 400:
            try:
                google_response = response.json()
            except ValueError:
                google_response = response.text
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    "message": "Google OAuth token request failed",
                    "google_response": google_response,
                },
            )

        token_data: dict[str, Any] = response.json()
        return token_data


google_oauth_service = GoogleOAuthService()