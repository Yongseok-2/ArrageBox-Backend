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

CONSUMER_BATCH_SIZE = 100
CONSUMER_POLL_TIMEOUT_MS = 1000


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


def _chunk_items(items: list[Any], chunk_size: int) -> list[list[Any]]:
    if chunk_size <= 0:
        return [items]
    return [items[i : i + chunk_size] for i in range(0, len(items), chunk_size)]


async def process_email_batch(pool: asyncpg.Pool, emails: list[dict[str, Any]]) -> None:
    """Persist a batch of emails after rule analysis and optional Gemini batch fallback."""
    if not emails:
        return

    temp_keys: list[str] = []
    try:
        for email_payload in emails:
            temp_key = await store_temp_email_body(email_payload)
            temp_keys.append(temp_key)
            await upsert_email(pool, email_payload, analysis=None)

        analyses: dict[str, dict[str, Any]] = {}
        other_emails: list[dict[str, Any]] = []
        other_fallbacks: list[dict[str, Any]] = []

        for email_payload in emails:
            rule_result = email_analyzer.analyze_email_rules(email_payload)
            message_id = str(email_payload.get("gmail_message_id", ""))
            analyses[message_id] = rule_result
            if rule_result.get("category") == "other":
                other_emails.append(email_payload)
                other_fallbacks.append(rule_result)

        if settings.gemini_enabled and other_emails:
            for email_chunk, fallback_chunk in zip(
                _chunk_items(other_emails, settings.gemini_batch_size),
                _chunk_items(other_fallbacks, settings.gemini_batch_size),
            ):
                gemini_results = await email_analyzer.analyze_other_emails_with_gemini(
                    emails=email_chunk,
                    fallbacks=fallback_chunk,
                )
                analyses.update(gemini_results)

        for email_payload in emails:
            message_id = str(email_payload.get("gmail_message_id", ""))
            analysis_payload = analyses.get(message_id)
            await upsert_email(pool, email_payload, analysis=analysis_payload)
    finally:
        for temp_key in temp_keys:
            await redis_temp_body_store.delete(temp_key)


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
        while True:
            records = await consumer.getmany(
                timeout_ms=CONSUMER_POLL_TIMEOUT_MS,
                max_records=CONSUMER_BATCH_SIZE,
            )
            batch_payloads: list[dict[str, Any]] = []
            for topic_partition_records in records.values():
                for msg in topic_partition_records:
                    email_payload = msg.value
                    if not email_payload:
                        continue
                    if not has_valid_account_id(email_payload):
                        logger.warning("Skip payload without valid account_id")
                        continue
                    batch_payloads.append(email_payload)

            if not batch_payloads:
                continue

            await process_email_batch(pool, batch_payloads)
    finally:
        await consumer.stop()
        await pool.close()


if __name__ == "__main__":
    asyncio.run(run_consumer())
