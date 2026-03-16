# -*- coding: utf-8 -*-
from contextlib import asynccontextmanager

from fastapi import FastAPI

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


@app.get("/health", summary="헬스체크")
async def health_check() -> dict[str, str]:
    """애플리케이션 상태를 반환합니다."""
    return {"status": "ok"}
