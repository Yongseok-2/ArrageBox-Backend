# -*- coding: utf-8 -*-
from datetime import datetime

from pydantic import BaseModel, Field


class GoogleAuthorizeResponse(BaseModel):
    authorization_url: str


class TokenExchangeRequest(BaseModel):
    code: str = Field(..., description="Google OAuth2 authorization code")
    redirect_uri: str | None = Field(
        default=None,
        description="Optional redirect URI used for OAuth authorization",
    )


class TokenRefreshRequest(BaseModel):
    refresh_token: str | None = Field(
        default=None,
        description="Google OAuth2 refresh token (optional; HttpOnly cookie preferred)",
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
        description="Current access token (optional; HttpOnly cookie preferred)",
    )
    refresh_token: str | None = Field(
        default=None,
        description="Google OAuth2 refresh token (optional; HttpOnly cookie preferred)",
    )
    expires_at: datetime | None = Field(
        default=None,
        description="Current token expiration time (ISO-8601, optional)",
    )


class LogoutResponse(BaseModel):
    message: str
