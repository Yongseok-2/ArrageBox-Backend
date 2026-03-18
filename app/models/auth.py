# -*- coding: utf-8 -*-
from datetime import datetime

from pydantic import BaseModel, Field


class GoogleAuthorizeResponse(BaseModel):
    authorization_url: str


class TokenExchangeRequest(BaseModel):
    code: str = Field(..., description="Google OAuth2 인가 코드")
    redirect_uri: str | None = Field(
        default=None,
        description="OAuth 인증 시 사용된 Redirect URI (생략 시 GOOGLE_REDIRECT_URI 설정값 사용)",
    )


class TokenRefreshRequest(BaseModel):
    refresh_token: str | None = Field(
        default=None,
        description="Google OAuth2 리프레시 토큰 (생략 시 HttpOnly 쿠키 값 사용)",
    )


class ManagedGoogleTokenResponse(BaseModel):
    expires_in: int
    scope: str | None = None
    token_type: str
    expires_at: datetime
    refreshed: bool = False


class EnsureTokenRequest(BaseModel):
    access_token: str | None = Field(
        default=None,
        description="현재 액세스 토큰 (생략 시 HttpOnly 쿠키 값 사용)",
    )
    refresh_token: str | None = Field(
        default=None,
        description="Google OAuth2 리프레시 토큰 (생략 시 HttpOnly 쿠키 값 사용)",
    )
    expires_at: datetime | None = Field(
        default=None,
        description="액세스 토큰 만료 일시 (ISO-8601, UTC 기준, 생략 시 쿠키 값 확인)",
    )