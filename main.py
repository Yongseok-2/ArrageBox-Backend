# -*- coding: utf-8 -*-
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.analysis import router as analysis_router
from app.api.auth import router as auth_router
from app.api.email import router as email_router
from app.core.db import close_db_pool, init_db_pool
from app.services.kafka_producer import kafka_email_producer


@asynccontextmanager
async def lifespan(_: FastAPI):
    """앱 시작/종료 시 Kafka와 DB 리소스를 관리합니다."""
    await init_db_pool()
    try:
        await kafka_email_producer.start()
    except Exception:
        # Kafka가 불가해도 인증 API는 동작하도록 유지
        pass
    try:
        yield
    finally:
        await kafka_email_producer.stop()
        await close_db_pool()


app = FastAPI(
    title="InboxZero AI API",
    version="0.3.0",
    description="InboxZero AI 백엔드 API (인증, 메일 분석, 일괄 처리)",
    lifespan=lifespan,
)
app.include_router(auth_router)
app.include_router(email_router)
app.include_router(analysis_router)

# 허용할 Origin(출처) 목록 정의
origins = [
    "http://localhost:5173",  # 리액트 개발 서버 포트
    "http://127.0.0.1:5173",
]

# CORSMiddleware 추가
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,             # 허용할 도메인 목록
    allow_credentials=True,            # 쿠키나 인증 헤더 허용 여부 (OAuth에 필수!)
    allow_methods=["*"],               # 모든 HTTP 메서드(GET, POST 등) 허용
    allow_headers=["*"],               # 모든 HTTP 헤더 허용
)


@app.get("/health", summary="헬스체크")
async def health_check() -> dict[str, str]:
    """애플리케이션 상태를 반환합니다."""
    return {"status": "ok"}
