import asyncio
import json
import logging
from datetime import UTC, datetime
from typing import Any

import asyncpg
from aiokafka import AIOKafkaConsumer

from app.core.settings import settings
from app.services.email_analyzer import email_analyzer

INSERT_RAW_SQL = """
INSERT INTO emails_raw (
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
    processed_at
)
VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11::jsonb, $12
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
    label_ids = EXCLUDED.label_ids,
    payload_json = EXCLUDED.payload_json,
    processed_at = EXCLUDED.processed_at;
"""

INSERT_ANALYSIS_SQL = """
INSERT INTO email_analysis (
    account_id,
    gmail_message_id,
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
    $1, $2, $3, $4, $5, $6, $7::jsonb, $8, $9, $10, $11, $12
)
ON CONFLICT (gmail_message_id)
DO UPDATE SET
    account_id = EXCLUDED.account_id,
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


async def save_email(pool: asyncpg.Pool, email: dict[str, Any]) -> None:
    """Save or update raw email payload."""
    async with pool.acquire() as conn:
        await conn.execute(
            INSERT_RAW_SQL,
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
            json.dumps(email.get("raw", {})),
            datetime.now(UTC),
        )


async def save_email_analysis(
    pool: asyncpg.Pool,
    account_id: str,
    analysis: dict[str, Any],
) -> None:
    """Save or update email analysis result."""
    async with pool.acquire() as conn:
        await conn.execute(
            INSERT_ANALYSIS_SQL,
            account_id,
            analysis["gmail_message_id"],
            analysis.get("sender_email"),
            analysis["category"],
            analysis["urgency_score"],
            analysis["summary"],
            json.dumps(analysis.get("keywords", [])),
            analysis["confidence_score"],
            analysis["analysis_source"],
            analysis["review_required"],
            analysis.get("draft_reply_context"),
            analysis["analyzed_at"],
        )


async def run_consumer() -> None:
    """Consume Kafka messages and persist raw + analyzed records."""
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
            account_id = str(email_payload.get("account_id", "")).strip()
            # 멀티 사용자 분리를 위해 account_id가 없는 payload는 저장하지 않는다.
            if not account_id or account_id == "unknown":
                logger.warning("Skip payload without valid account_id")
                continue
            await save_email(pool, email_payload)
            analysis_payload = await email_analyzer.analyze_email(email_payload)
            await save_email_analysis(pool, account_id=account_id, analysis=analysis_payload)
    finally:
        await consumer.stop()
        await pool.close()


if __name__ == "__main__":
    asyncio.run(run_consumer())
