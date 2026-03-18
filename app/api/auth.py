# -*- coding: utf-8 -*-
from datetime import datetime

from fastapi import APIRouter, Body, Cookie, HTTPException, Query, Response, status

from app.core.settings import settings
from app.models.auth import (
    EnsureTokenRequest,
    GoogleAuthorizeResponse,
    ManagedGoogleTokenResponse,
    TokenExchangeRequest,
    TokenRefreshRequest,
)
from app.services.auth import google_oauth_service

router = APIRouter(prefix="/auth", tags=["auth"])


def _parse_expires_at(raw: str | None) -> datetime | None:
    if not raw:
        return None
    try:
        normalized = raw.replace("Z", "+00:00")
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def _set_auth_cookies(response: Response, token_data: dict) -> None:
    cookie_common = {
        "httponly": True,
        "secure": settings.auth_cookie_secure,
        "samesite": settings.auth_cookie_samesite,
        "path": "/",
    }
    if settings.auth_cookie_domain:
        cookie_common["domain"] = settings.auth_cookie_domain

    response.set_cookie(
        key=settings.auth_access_cookie_name,
        value=token_data["access_token"],
        max_age=settings.auth_access_cookie_max_age,
        **cookie_common,
    )
    response.set_cookie(
        key=settings.auth_refresh_cookie_name,
        value=token_data["refresh_token"],
        max_age=settings.auth_refresh_cookie_max_age,
        **cookie_common,
    )
    response.set_cookie(
        key=settings.auth_expires_cookie_name,
        value=token_data["expires_at"].isoformat(),
        max_age=settings.auth_access_cookie_max_age,
        **cookie_common,
    )


def _to_public_token_response(token_data: dict) -> ManagedGoogleTokenResponse:
    return ManagedGoogleTokenResponse(
        expires_in=token_data["expires_in"],
        scope=token_data.get("scope"),
        token_type=token_data["token_type"],
        expires_at=token_data["expires_at"],
        refreshed=token_data.get("refreshed", False),
    )


@router.get(
    "/google/authorize",
    response_model=GoogleAuthorizeResponse,
    summary="Google OAuth 인증 URL 생성",
)
async def get_google_authorization_url(
    redirect_uri: str | None = Query(default=None),
    state: str | None = Query(default=None),
) -> GoogleAuthorizeResponse:
    """Google OAuth2 인증을 위한 URL을 생성하여 반환합니다."""
    auth_url = google_oauth_service.build_authorization_url(
        redirect_uri=redirect_uri,
        state=state,
    )
    return GoogleAuthorizeResponse(authorization_url=auth_url)


@router.post(
    "/google/token",
    response_model=ManagedGoogleTokenResponse,
    summary="인증 코드로 토큰 발급",
    description="Google 인가 코드(authorization code)를 사용해 토큰을 발급받고 HttpOnly 쿠키에 저장합니다.",
)
async def exchange_google_token(
    payload: TokenExchangeRequest,
    response: Response,
) -> ManagedGoogleTokenResponse:
    """인가 코드를 통해 액세스 토큰과 리프레시 토큰을 교환합니다."""
    token_data = await google_oauth_service.exchange_code_for_tokens(
        code=payload.code,
        redirect_uri=payload.redirect_uri,
    )
    _set_auth_cookies(response, token_data)
    return _to_public_token_response(token_data)


@router.post(
    "/google/refresh",
    response_model=ManagedGoogleTokenResponse,
    summary="Access Token 갱신",
    description="Refresh Token을 사용하여 새로운 Access Token을 발급받고 HttpOnly 쿠키에 저장합니다.",
)
async def refresh_google_token(
    response: Response,
    payload: TokenRefreshRequest = Body(default_factory=TokenRefreshRequest),
    refresh_cookie: str | None = Cookie(default=None, alias=settings.auth_refresh_cookie_name),
) -> ManagedGoogleTokenResponse:
    """Refresh Token을 사용하여 Access Token을 갱신합니다."""
    refresh_token = payload.refresh_token or refresh_cookie
    if not refresh_token:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Refresh token is required. Provide body value or send auth cookie.",
        )

    token_data = await google_oauth_service.refresh_access_token(refresh_token=refresh_token)
    _set_auth_cookies(response, token_data)
    return _to_public_token_response(token_data)


@router.post(
    "/google/token/ensure",
    response_model=ManagedGoogleTokenResponse,
    summary="유효한 토큰 보장",
    description="현재 토큰의 만료 여부를 확인하고, 만료되었거나 만료가 임박한 경우 자동으로 갱신하여 반환합니다.",
)
async def ensure_google_token(
    response: Response,
    payload: EnsureTokenRequest = Body(default_factory=EnsureTokenRequest),
    access_cookie: str | None = Cookie(default=None, alias=settings.auth_access_cookie_name),
    refresh_cookie: str | None = Cookie(default=None, alias=settings.auth_refresh_cookie_name),
    expires_cookie: str | None = Cookie(default=None, alias=settings.auth_expires_cookie_name),
) -> ManagedGoogleTokenResponse:
    """토큰의 유효성을 검증하고 필요시 갱신된 토큰을 제공합니다."""
    access_token = payload.access_token or access_cookie
    refresh_token = payload.refresh_token or refresh_cookie
    expires_at = payload.expires_at or _parse_expires_at(expires_cookie)

    if not refresh_token:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Refresh token is required. Provide body value or send auth cookie.",
        )

    token_data = await google_oauth_service.ensure_valid_access_token(
        access_token=access_token,
        refresh_token=refresh_token,
        expires_at=expires_at,
    )
    _set_auth_cookies(response, token_data)
    return _to_public_token_response(token_data)

@router.post("/logout", status_code=status.HTTP_204_NO_CONTENT, summary="????")
async def logout(response: Response) -> None:
    """인증 쿠키를 삭제하여 로그아웃 처리를 수행합니다."""
    cookie_common = {"path": "/"}
    if settings.auth_cookie_domain:
        cookie_common["domain"] = settings.auth_cookie_domain

    response.delete_cookie(key=settings.auth_access_cookie_name, **cookie_common)
    response.delete_cookie(key=settings.auth_refresh_cookie_name, **cookie_common)
    response.delete_cookie(key=settings.auth_expires_cookie_name, **cookie_common)

    return None
