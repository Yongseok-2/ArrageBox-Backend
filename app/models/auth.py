# -*- coding: utf-8 -*-
from datetime import datetime

from pydantic import BaseModel, Field


class GoogleAuthorizeResponse(BaseModel):
    authorization_url: str


class TokenExchangeRequest(BaseModel):
    code: str = Field(..., description="Google OAuth2 인가 코드")
    redirect_uri: str | None = Field(
        default=None,
        description="OAuth 동의 시 사용한 Redirect URI (없으면 GOOGLE_REDIRECT_URI 사용)",
    )


class TokenRefreshRequest(BaseModel):
    refresh_token: str = Field(..., description="Google OAuth2 refresh token")


class GoogleTokenResponse(BaseModel):
    access_token: str
    expires_in: int
    scope: str | None = None
    token_type: str
    refresh_token: str | None = None
    id_token: str | None = None


class ManagedGoogleTokenResponse(GoogleTokenResponse):
    expires_at: datetime
    refreshed: bool = False


class EnsureTokenRequest(BaseModel):
    access_token: str | None = Field(
        default=None,
        description="현재 access token (있는 경우)",
    )
    refresh_token: str = Field(..., description="Google OAuth2 refresh token")
    expires_at: datetime | None = Field(
        default=None,
        description="access token 만료 시각(ISO-8601, UTC 권장)",
    )