from email.utils import parseaddr

import asyncpg
from fastapi import APIRouter

from app.core.settings import settings
from app.models.email import (
    BucketSummaryItem,
    BulkActionRequest,
    BulkActionResponse,
    CategorySummaryItem,
    EmailSyncRequest,
    EmailSyncResponse,
    TriageGroupItem,
    TriagePreviewDbRequest,
    TriagePreviewRequest,
    TriagePreviewResponse,
)
from app.services.email_analyzer import email_analyzer
from app.services.gmail import gmail_service
from app.services.kafka_producer import kafka_email_producer

router = APIRouter(prefix="/emails", tags=["emails"])


@router.post(
    "/sync",
    response_model=EmailSyncResponse,
    summary="읽지 않은 메일 동기화",
)
async def sync_unread_emails(payload: EmailSyncRequest) -> EmailSyncResponse:
    """읽지 않은 메일을 Gmail에서 가져와 Kafka 토픽으로 발행합니다."""
    unread_emails = await gmail_service.fetch_unread_emails(
        access_token=payload.access_token,
        user_id=payload.user_id,
        max_results=payload.max_results,
    )

    published_count = 0
    for email in unread_emails:
        # 멀티 사용자 분리를 위해 account_id를 payload에 포함한다.
        email["account_id"] = payload.account_id
        await kafka_email_producer.publish_email(email)
        published_count += 1

    return EmailSyncResponse(
        fetched_count=len(unread_emails),
        published_count=published_count,
        topic=kafka_email_producer.topic,
    )


@router.post(
    "/triage/preview",
    response_model=TriagePreviewResponse,
    summary="일괄 처리 미리보기",
)
async def preview_triage_groups(payload: TriagePreviewRequest) -> TriagePreviewResponse:
    """Gmail 실시간 데이터를 기준으로 unread/stale 그룹 미리보기를 생성합니다."""
    triage_data = await gmail_service.fetch_triage_emails(
        access_token=payload.access_token,
        user_id=payload.user_id,
        max_unread=payload.max_unread,
        max_stale=payload.max_stale,
    )

    groups: dict[tuple[str, str, str], dict[str, object]] = {}

    for bucket, items in [("unread", triage_data["unread"]), ("stale", triage_data["stale"])]:
        for email in items:
            analysis = await email_analyzer.analyze_email(email)
            sender_display = _extract_sender_display(email.get("from_email", ""))
            sender_key = _sender_group_key(email.get("from_email", ""))
            category = analysis["category"]
            key = (bucket, sender_key, category)

            if key not in groups:
                groups[key] = {
                    "group_id": f"{bucket}|{sender_key}|{category}",
                    "bucket": bucket,
                    "sender": sender_display,
                    "category": category,
                    "count": 0,
                    "confidence_sum": 0.0,
                    "review_required_count": 0,
                    "message_ids": [],
                    "sample_subjects": [],
                }

            group = groups[key]
            group["count"] = int(group["count"]) + 1
            group["confidence_sum"] = float(group["confidence_sum"]) + float(
                analysis.get("confidence_score", 0.0)
            )
            if analysis.get("review_required", False):
                group["review_required_count"] = int(group["review_required_count"]) + 1
            group["message_ids"].append(email.get("gmail_message_id", ""))
            if len(group["sample_subjects"]) < 3:
                group["sample_subjects"].append(email.get("subject", ""))

    return _build_triage_response(groups=groups)


@router.post(
    "/triage/preview-db",
    response_model=TriagePreviewResponse,
    summary="DB 기반 일괄 처리 미리보기",
)
async def preview_triage_groups_from_db(payload: TriagePreviewDbRequest) -> TriagePreviewResponse:
    """DB에 저장된 메일/분석 결과를 기준으로 unread/stale 그룹 미리보기를 생성합니다."""
    query = """
    WITH source AS (
        SELECT
            r.gmail_message_id,
            r.from_email,
            r.subject,
            r.label_ids,
            CASE
                WHEN COALESCE(r.internal_date, '') ~ '^[0-9]+$'
                THEN to_timestamp((r.internal_date::bigint) / 1000.0)
                ELSE NULL
            END AS internal_ts,
            a.category,
            a.confidence_score,
            a.review_required
        FROM emails_raw r
        JOIN email_analysis a ON a.gmail_message_id = r.gmail_message_id
        WHERE r.account_id = $4
          AND NOT (r.label_ids ?| ARRAY['TRASH', 'SPAM'])
    ),
    unread_rows AS (
        SELECT *, 'unread'::text AS bucket
        FROM source
        WHERE (label_ids ? 'UNREAD')
        ORDER BY internal_ts DESC NULLS LAST
        LIMIT $1
    ),
    stale_rows AS (
        SELECT *, 'stale'::text AS bucket
        FROM source
        WHERE NOT (label_ids ? 'UNREAD')
          AND internal_ts IS NOT NULL
          AND internal_ts < (NOW() - make_interval(months => $2::int))
        ORDER BY internal_ts ASC
        LIMIT $3
    )
    SELECT * FROM unread_rows
    UNION ALL
    SELECT * FROM stale_rows
    """

    pool = await asyncpg.create_pool(dsn=settings.postgres_dsn, min_size=1, max_size=3)
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                query,
                payload.max_unread,
                payload.stale_months,
                payload.max_stale,
                payload.account_id,
            )
    finally:
        await pool.close()

    groups: dict[tuple[str, str, str], dict[str, object]] = {}

    for row in rows:
        from_email = row["from_email"] or ""
        sender_display = _extract_sender_display(from_email)
        sender_key = _sender_group_key(from_email)
        bucket = str(row["bucket"])
        category = str(row["category"])
        key = (bucket, sender_key, category)

        if key not in groups:
            groups[key] = {
                "group_id": f"{bucket}|{sender_key}|{category}",
                "bucket": bucket,
                "sender": sender_display,
                "category": category,
                "count": 0,
                "confidence_sum": 0.0,
                "review_required_count": 0,
                "message_ids": [],
                "sample_subjects": [],
            }

        group = groups[key]
        group["count"] = int(group["count"]) + 1
        group["confidence_sum"] = float(group["confidence_sum"]) + float(row["confidence_score"] or 0.0)
        if bool(row["review_required"]):
            group["review_required_count"] = int(group["review_required_count"]) + 1

        message_id = str(row["gmail_message_id"])
        group["message_ids"].append(message_id)

        subject = str(row["subject"] or "")
        if subject and len(group["sample_subjects"]) < 3:
            group["sample_subjects"].append(subject)

    return _build_triage_response(groups=groups)


@router.post(
    "/triage/action",
    response_model=BulkActionResponse,
    summary="일괄 액션 실행",
)
async def apply_triage_action(payload: BulkActionRequest) -> BulkActionResponse:
    """선택한 message_ids에 대해 archive 또는 trash를 수행합니다."""
    result = await gmail_service.apply_bulk_action(
        access_token=payload.access_token,
        action=payload.action,
        message_ids=payload.message_ids,
        user_id=payload.user_id,
    )

    if payload.action == "trash":
        failed_set = set(result["failed_ids"])
        deleted_ids = [mid for mid in payload.message_ids if mid not in failed_set]
        await _delete_messages_from_db(account_id=payload.account_id, message_ids=deleted_ids)

    return BulkActionResponse(
        action=payload.action,
        processed_count=result["processed_count"],
        failed_ids=result["failed_ids"],
    )


async def _delete_messages_from_db(account_id: str, message_ids: list[str]) -> None:
    """Gmail 휴지통 이동 성공 메일을 DB에서도 삭제합니다."""
    if not message_ids:
        return

    query = "DELETE FROM emails_raw WHERE account_id = $1 AND gmail_message_id = ANY($2::text[])"
    pool = await asyncpg.create_pool(dsn=settings.postgres_dsn, min_size=1, max_size=3)
    try:
        async with pool.acquire() as conn:
            await conn.execute(query, account_id, message_ids)
    finally:
        await pool.close()


def _build_triage_response(groups: dict[tuple[str, str, str], dict[str, object]]) -> TriagePreviewResponse:
    """그룹 집계 데이터를 API 응답 모델 형태로 변환합니다."""
    items: list[TriageGroupItem] = []
    for group in groups.values():
        count = int(group["count"])
        confidence_sum = float(group["confidence_sum"])
        items.append(
            TriageGroupItem(
                group_id=str(group["group_id"]),
                bucket=str(group["bucket"]),
                sender=str(group["sender"]),
                category=str(group["category"]),
                count=count,
                avg_confidence_score=round(confidence_sum / count, 4) if count else 0.0,
                review_required_count=int(group["review_required_count"]),
                message_ids=[str(mid) for mid in group["message_ids"] if mid],
                sample_subjects=[str(subject) for subject in group["sample_subjects"]],
            )
        )

    sorted_groups = sorted(items, key=lambda item: item.count, reverse=True)
    bucket_summary = _build_bucket_summary(sorted_groups)
    category_summary = _build_category_summary(sorted_groups)

    return TriagePreviewResponse(
        total_unread=sum(g.count for g in sorted_groups if g.bucket == "unread"),
        total_stale=sum(g.count for g in sorted_groups if g.bucket == "stale"),
        groups=sorted_groups,
        bucket_summary=bucket_summary,
        category_summary=category_summary,
    )


def _extract_sender_display(raw_from: str) -> str:
    """From 헤더에서 화면 표시용 발신자명을 추출합니다."""
    name, email_addr = parseaddr((raw_from or "").strip())
    if name:
        return name.strip()
    if email_addr:
        local = email_addr.split("@", 1)[0].strip()
        return local or email_addr.strip().lower()
    return (raw_from or "").strip()


def _sender_group_key(raw_from: str) -> str:
    """그룹 내부 식별용 키를 생성합니다(가능하면 이메일 주소 사용)."""
    name, email_addr = parseaddr((raw_from or "").strip())
    if email_addr:
        return email_addr.strip().lower()
    if name:
        return name.strip().lower()
    return (raw_from or "").strip().lower()


def _build_bucket_summary(groups: list[TriageGroupItem]) -> list[BucketSummaryItem]:
    """버킷별 메일 수와 그룹 수를 계산합니다."""
    bucket_counts: dict[str, int] = {"unread": 0, "stale": 0}
    group_counts: dict[str, int] = {"unread": 0, "stale": 0}

    for group in groups:
        bucket_counts[group.bucket] += group.count
        group_counts[group.bucket] += 1

    return [
        BucketSummaryItem(bucket="unread", count=bucket_counts["unread"], group_count=group_counts["unread"]),
        BucketSummaryItem(bucket="stale", count=bucket_counts["stale"], group_count=group_counts["stale"]),
    ]


def _build_category_summary(groups: list[TriageGroupItem]) -> list[CategorySummaryItem]:
    """카테고리별 메일 수를 계산합니다."""
    counts: dict[str, int] = {}
    for group in groups:
        counts[group.category] = counts.get(group.category, 0) + group.count

    sorted_counts = sorted(counts.items(), key=lambda item: item[1], reverse=True)
    return [CategorySummaryItem(category=category, count=count) for category, count in sorted_counts]

