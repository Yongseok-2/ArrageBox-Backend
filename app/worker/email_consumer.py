import asyncio
import json
import logging
from datetime import UTC, datetime
from typing import Any

import asyncpg
from aiokafka import AIOKafkaConsumer

from app.core.redis_store import redis_temp_body_store
from app.core.settings import settings
from app.services.email_analyzer import email_analyzer

UPSERT_EMAIL_SQL = """
INSERT INTO email_analysis (
    account_id,
    gmail_message_id,
    gmail_thread_id,
    subject,
    from_email,
    to_email,
    date_header,
    snippet,
    internal_date,
    label_ids,
    payload_json,
    processed_at,
    sender_email,
    category,
    urgency_score,
    summary,
    keywords,
    confidence_score,
    analysis_source,
    review_required,
    draft_reply_context,
    analyzed_at
)
VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11::jsonb, $12,
    $13, $14, $15, $16, $17::jsonb, $18, $19, $20, $21, $22
)
ON CONFLICT (gmail_message_id)
DO UPDATE SET
    account_id = EXCLUDED.account_id,
    gmail_thread_id = EXCLUDED.gmail_thread_id,
    subject = EXCLUDED.subject,
    from_email = EXCLUDED.from_email,
    to_email = EXCLUDED.to_email,
    date_header = EXCLUDED.date_header,
    snippet = EXCLUDED.snippet,
    internal_date = EXCLUDED.internal_date,
    processed_at = EXCLUDED.processed_at,
    sender_email = EXCLUDED.sender_email,
    category = EXCLUDED.category,
    urgency_score = EXCLUDED.urgency_score,
    summary = EXCLUDED.summary,
    keywords = EXCLUDED.keywords,
    confidence_score = EXCLUDED.confidence_score,
    analysis_source = EXCLUDED.analysis_source,
    review_required = EXCLUDED.review_required,
    draft_reply_context = EXCLUDED.draft_reply_context,
    analyzed_at = EXCLUDED.analyzed_at;
"""

logger = logging.getLogger(__name__)


def safe_deserialize(value: bytes) -> dict[str, Any] | None:
    """Deserialize Kafka bytes safely; skip invalid payload."""
    try:
        return json.loads(value.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        logger.warning("Skip invalid Kafka payload")
        return None


def has_valid_account_id(email_payload: dict[str, Any]) -> bool:
    """멀티 사용자 분리를 위해 유효한 account_id 여부를 확인합니다."""
    account_id = str(email_payload.get("account_id", "")).strip()
    return bool(account_id and account_id != "unknown")


def _build_row_values(email: dict[str, Any], analysis: dict[str, Any] | None) -> tuple[Any, ...]:
    analysis = analysis or {}
    now = datetime.now(UTC)
    payload_json = email.get("raw", {})
    return (
        email.get("account_id"),
        email.get("gmail_message_id"),
        email.get("gmail_thread_id"),
        email.get("subject"),
        email.get("from_email"),
        email.get("to_email"),
        email.get("date_header"),
        email.get("snippet"),
        email.get("internal_date"),
        json.dumps(email.get("label_ids", [])),
        json.dumps(payload_json),
        now,
        analysis.get("sender_email", email.get("from_email")),
        analysis.get("category", "other"),
        analysis.get("urgency_score", 0),
        analysis.get("summary", ""),
        json.dumps(analysis.get("keywords", [])),
        analysis.get("confidence_score", 0.0),
        analysis.get("analysis_source", "rules"),
        analysis.get("review_required", False),
        analysis.get("draft_reply_context"),
        analysis.get("analyzed_at", now),
    )


async def upsert_email(pool: asyncpg.Pool, email: dict[str, Any], analysis: dict[str, Any] | None = None) -> None:
    """Insert or update the combined email record."""
    async with pool.acquire() as conn:
        await conn.execute(UPSERT_EMAIL_SQL, *_build_row_values(email, analysis))


async def store_temp_email_body(email: dict[str, Any]) -> str:
    """Store the raw payload in short-lived storage and return its key."""
    key = f"email-body:{email.get('account_id')}:{email.get('gmail_message_id')}"
    await redis_temp_body_store.set_json(
        key,
        email.get("raw", {}),
        ttl_seconds=settings.email_body_ttl_seconds,
    )
    return key


async def run_consumer() -> None:
    """Consume Kafka messages and persist combined email records."""
    pool = await asyncpg.create_pool(dsn=settings.postgres_dsn, min_size=1, max_size=5)
    consumer = AIOKafkaConsumer(
        settings.email_raw_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=settings.kafka_group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=safe_deserialize,
    )
    await consumer.start()
    try:
        async for msg in consumer:
            email_payload = msg.value
            if not email_payload:
                continue
            if not has_valid_account_id(email_payload):
                logger.warning("Skip payload without valid account_id")
                continue

            temp_key = await store_temp_email_body(email_payload)
            try:
                await upsert_email(pool, email_payload, analysis=None)
                analysis_payload = await email_analyzer.analyze_email(email_payload)
                await upsert_email(pool, email_payload, analysis=analysis_payload)
            finally:
                await redis_temp_body_store.delete(temp_key)
    finally:
        await consumer.stop()
        await pool.close()


if __name__ == "__main__":
    asyncio.run(run_consumer())
