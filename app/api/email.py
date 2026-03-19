from email.utils import parseaddr
from typing import Any

from fastapi import APIRouter

from app.core.db import get_db_pool
from app.models.email import (
    BulkActionRequest,
    BulkActionResponse,
    BucketGroupItem,
    EmailSyncRequest,
    EmailSyncResponse,
    LabelCreateRequest,
    LabelCreateResponse,
    LabelGroupItem,
    LabelUpdateRequest,
    LabelUpdateResponse,
    SenderGroupItem,
    TriagePreviewDbRequest,
    TriagePreviewRequest,
    TriagePreviewResponse,
    TriageGroupItem,
)
from app.services.email_analyzer import email_analyzer
from app.services.gmail import gmail_service
from app.services.kafka_producer import kafka_email_producer

router = APIRouter(prefix="/emails", tags=["emails"])


@router.post(
    "/sync",
    response_model=EmailSyncResponse,
    summary="받은편지함 전체 동기화",
)
async def sync_unread_emails(payload: EmailSyncRequest) -> EmailSyncResponse:
    """받은편지함 전체 메일을 Gmail에서 가져와 Kafka 토픽으로 발행합니다."""
    inbox_emails = await gmail_service.fetch_unread_emails(
        access_token=payload.access_token,
        user_id=payload.user_id,
        max_results=payload.max_results,
    )

    published_count = 0
    for email in inbox_emails:
        # 멀티 사용자 분리를 위해 account_id를 payload에 포함한다.
        email["account_id"] = payload.account_id
        await kafka_email_producer.publish_email(email)
        published_count += 1

    return EmailSyncResponse(
        fetched_count=len(inbox_emails),
        published_count=published_count,
        topic=kafka_email_producer.topic,
    )


@router.post(
    "/triage/preview",
    response_model=TriagePreviewResponse,
    summary="일괄 처리 미리보기",
)
async def preview_triage_groups(payload: TriagePreviewRequest) -> TriagePreviewResponse:
    """Gmail 실시간 데이터를 기준으로 unread/read 그룹 미리보기를 생성합니다."""
    triage_data = await gmail_service.fetch_triage_emails(
        access_token=payload.access_token,
        user_id=payload.user_id,
        max_unread=payload.max_unread,
        max_read=payload.max_read,
    )

    groups: dict[tuple[str, str, str, str], dict[str, Any]] = {}

    for bucket, items in [("unread", triage_data["unread"]), ("read", triage_data["read"])]:
        for email in items:
            analysis = await email_analyzer.analyze_email(email)
            sender_display = _extract_sender_display(email.get("from_email", ""))
            sender_key = _sender_group_key(email.get("from_email", ""))
            category = analysis["category"]
            label_group = _detect_label_group(email.get("label_ids", []))
            key = (bucket, label_group, sender_key, category)

            if key not in groups:
                groups[key] = {
                    "group_id": f"{bucket}|{label_group}|{sender_key}|{category}",
                    "bucket": bucket,
                    "label_group": label_group,
                    "sender": sender_display,
                    "category": category,
                    "count": 0,
                    "confidence_sum": 0.0,
                    "review_required_count": 0,
                    "message_ids": [],
                    "sample_subjects": [],
                }

            group = groups[key]
            group["count"] += 1
            group["confidence_sum"] += float(analysis.get("confidence_score", 0.0))
            if analysis.get("review_required", False):
                group["review_required_count"] += 1
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
    """DB에 저장된 메일/분석 결과를 기준으로 unread/read 그룹 미리보기를 생성합니다."""
    date_filter_clause, date_filter_params = _build_date_filter_clause(payload)
    query = f"""
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
          {date_filter_clause}
    ),
    unread_rows AS (
        SELECT *, 'unread'::text AS bucket
        FROM source
        WHERE (label_ids ? 'UNREAD')
        ORDER BY internal_ts DESC NULLS LAST
        LIMIT $1
    ),
    read_rows AS (
        SELECT *, 'read'::text AS bucket
        FROM source
        WHERE NOT (label_ids ? 'UNREAD')
        AND internal_ts IS NOT NULL
        ORDER BY internal_ts DESC NULLS LAST
        LIMIT $2
    )
    SELECT * FROM unread_rows
    UNION ALL
    SELECT * FROM read_rows
    """

    pool = get_db_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            query,
            payload.max_unread,
            payload.max_read,
            payload.account_id,
            *date_filter_params,
        )

    groups: dict[tuple[str, str, str, str], dict[str, Any]] = {}

    for row in rows:
        from_email = row["from_email"] or ""
        sender_display = _extract_sender_display(from_email)
        sender_key = _sender_group_key(from_email)
        bucket = str(row["bucket"])
        category = str(row["category"])
        label_group = _detect_label_group(row["label_ids"] or [])
        key = (bucket, label_group, sender_key, category)

        if key not in groups:
            groups[key] = {
                "group_id": f"{bucket}|{label_group}|{sender_key}|{category}",
                "bucket": bucket,
                "label_group": label_group,
                "sender": sender_display,
                "category": category,
                "count": 0,
                "confidence_sum": 0.0,
                "review_required_count": 0,
                "message_ids": [],
                "sample_subjects": [],
            }

        group = groups[key]
        group["count"] += 1
        group["confidence_sum"] += float(row["confidence_score"] or 0.0)
        if bool(row["review_required"]):
            group["review_required_count"] += 1

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
    """선택한 message_ids에 대해 inbox 해제 또는 trash를 수행합니다."""
    result = await gmail_service.apply_bulk_action(
        access_token=payload.access_token,
        action=payload.action,
        message_ids=payload.message_ids,
        user_id=payload.user_id,
    )

    failed_ids = result["failed_ids"]
    failed_set = set(failed_ids)
    success_ids = [mid for mid in payload.message_ids if mid not in failed_set]

    if payload.action == "trash":
        await _delete_messages_from_db(account_id=payload.account_id, message_ids=success_ids)

    return BulkActionResponse(
        action=payload.action,
        processed_count=result["processed_count"],
        failed_count=len(failed_ids),
        partial_failed=bool(failed_ids),
        success_ids=success_ids,
        failed_ids=failed_ids,
    )


@router.post(
    "/labels",
    response_model=LabelUpdateResponse,
    summary="라벨 변경 동기화",
)
async def update_labels(payload: LabelUpdateRequest) -> LabelUpdateResponse:
    """Gmail 라벨 변경과 DB label_ids 동기화를 함께 수행합니다."""
    remove_label_ids = _normalize_remove_label_ids(payload.remove_label_ids)
    result = await gmail_service.apply_label_updates(
        access_token=payload.access_token,
        user_id=payload.user_id,
        message_ids=payload.message_ids,
        add_label_ids=payload.add_label_ids,
        remove_label_ids=remove_label_ids,
    )

    failed_ids = result["failed_ids"]
    failed_set = set(failed_ids)
    success_ids = [mid for mid in payload.message_ids if mid not in failed_set]

    if success_ids:
        await _sync_label_ids_in_db(
            account_id=payload.account_id,
            message_ids=success_ids,
            add_label_ids=payload.add_label_ids,
            remove_label_ids=remove_label_ids,
        )

    return LabelUpdateResponse(
        processed_count=result["processed_count"],
        failed_count=len(failed_ids),
        partial_failed=bool(failed_ids),
        success_ids=success_ids,
        failed_ids=failed_ids,
    )


@router.post(
    "/labels/create",
    response_model=LabelCreateResponse,
    summary="사용자 라벨 생성",
)
async def create_label(payload: LabelCreateRequest) -> LabelCreateResponse:
    """Gmail 사용자 라벨을 생성하고 DB에 메타데이터를 저장합니다."""
    created_label = await gmail_service.create_label(
        access_token=payload.access_token,
        user_id=payload.user_id,
        name=payload.name,
    )

    await _upsert_gmail_label_metadata(
        account_id=payload.account_id,
        gmail_label_id=str(created_label["gmail_label_id"]),
        name=str(created_label["name"]),
        label_type=str(created_label["label_type"]),
    )

    return LabelCreateResponse(
        gmail_label_id=str(created_label["gmail_label_id"]),
        name=str(created_label["name"]),
        label_type=str(created_label["label_type"]),
    )


async def _delete_messages_from_db(account_id: str, message_ids: list[str]) -> None:
    """Gmail 휴지통 이동 성공 메일을 DB에서도 삭제합니다."""
    if not message_ids:
        return

    query = "DELETE FROM emails_raw WHERE account_id = $1 AND gmail_message_id = ANY($2::text[])"
    pool = get_db_pool()
    async with pool.acquire() as conn:
        await conn.execute(query, account_id, message_ids)


async def _sync_label_ids_in_db(
    account_id: str,
    message_ids: list[str],
    add_label_ids: list[str],
    remove_label_ids: list[str],
) -> None:
    """DB emails_raw.label_ids를 Gmail 변경 결과에 맞춰 동기화합니다."""
    if not message_ids:
        return

    pool = get_db_pool()
    select_sql = """
    SELECT gmail_message_id, label_ids
    FROM emails_raw
    WHERE account_id = $1 AND gmail_message_id = ANY($2::text[])
    """
    update_sql = """
    UPDATE emails_raw
    SET label_ids = $3::jsonb
    WHERE account_id = $1 AND gmail_message_id = $2
    """

    async with pool.acquire() as conn:
        rows = await conn.fetch(select_sql, account_id, message_ids)
        for row in rows:
            current = row["label_ids"] or []
            labels = {str(item).upper() for item in current}
            for label in remove_label_ids:
                labels.discard(str(label).upper())
            for label in add_label_ids:
                labels.add(str(label).upper())
            await conn.execute(update_sql, account_id, row["gmail_message_id"], list(labels))


async def _upsert_gmail_label_metadata(
    account_id: str,
    gmail_label_id: str,
    name: str,
    label_type: str,
) -> None:
    """생성된 Gmail 사용자 라벨의 메타데이터를 DB에 저장합니다."""
    pool = get_db_pool()
    query = """
    INSERT INTO gmail_labels (account_id, gmail_label_id, label_name, label_type, updated_at)
    VALUES ($1, $2, $3, $4, NOW())
    ON CONFLICT (account_id, gmail_label_id)
    DO UPDATE SET
        label_name = EXCLUDED.label_name,
        label_type = EXCLUDED.label_type,
        updated_at = EXCLUDED.updated_at
    """
    async with pool.acquire() as conn:
        await conn.execute(query, account_id, gmail_label_id, name, label_type)


def _normalize_remove_label_ids(remove_label_ids: list[str]) -> list[str]:
    """Gmail 라벨 변경과 DB 반영에서 받은편지함 해제를 일관되게 유지한다."""
    normalized = [str(label).upper() for label in remove_label_ids if str(label).strip()]
    if "INBOX" not in normalized:
        normalized.append("INBOX")
    return list(dict.fromkeys(normalized))


def _build_triage_response(groups: dict[tuple[str, str, str, str], dict[str, Any]]) -> TriagePreviewResponse:
    """그룹 집계 데이터를 API 응답 모델 형태로 변환합니다."""
    nested: dict[str, dict[str, dict[str, dict[str, Any]]]] = {"unread": {}, "read": {}}
    total_count = 0

    for group in groups.values():
        count = int(group["count"])
        confidence_sum = float(group["confidence_sum"])
        bucket = str(group["bucket"])
        label_group = str(group["label_group"])
        sender = str(group["sender"])
        category = str(group["category"])
        category_item = {
            "category": category,
            "count": count,
            "avg_confidence_score": round(confidence_sum / count, 4) if count else 0.0,
            "review_required_count": int(group["review_required_count"]),
            "message_ids": [str(mid) for mid in group["message_ids"] if mid],
            "message_links": [_build_gmail_message_link(str(mid)) for mid in group["message_ids"] if mid],
            "sample_subjects": [str(subject) for subject in group["sample_subjects"]],
        }

        bucket_map = nested[bucket]
        label_map = bucket_map.setdefault(label_group, {})
        sender_map = label_map.setdefault(sender, {})
        sender_map[category] = category_item

        total_count += count

    buckets: list[BucketGroupItem] = []
    for bucket_name in ["unread", "read"]:
        label_items: list[LabelGroupItem] = []
        bucket_total = 0
        for label_group, senders in sorted(
            nested[bucket_name].items(),
            key=lambda item: sum(cat["count"] for sender in item[1].values() for cat in sender.values()),
            reverse=True,
        ):
            sender_items: list[SenderGroupItem] = []
            label_total = 0
            for sender, categories in sorted(
                senders.items(),
                key=lambda item: sum(cat["count"] for cat in item[1].values()),
                reverse=True,
            ):
                category_items = [
                    TriageGroupItem(
                        category=category,
                        count=category_data["count"],
                        avg_confidence_score=category_data["avg_confidence_score"],
                        review_required_count=category_data["review_required_count"],
                        message_ids=category_data["message_ids"],
                        message_links=category_data["message_links"],
                        sample_subjects=category_data["sample_subjects"],
                    )
                    for category, category_data in sorted(categories.items(), key=lambda item: item[1]["count"], reverse=True)
                ]
                sender_count = sum(item.count for item in category_items)
                label_total += sender_count
                sender_items.append(SenderGroupItem(sender=sender, count=sender_count, categories=category_items))

            sender_items.sort(key=lambda item: item.count, reverse=True)
            label_items.append(LabelGroupItem(label_group=label_group, count=label_total, senders=sender_items))
            bucket_total += label_total

        label_items.sort(key=lambda item: item.count, reverse=True)
        buckets.append(BucketGroupItem(bucket=bucket_name, count=bucket_total, label_groups=label_items))

    buckets.sort(key=lambda item: item.count, reverse=True)
    return TriagePreviewResponse(total_count=total_count, buckets=buckets)


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


def _detect_label_group(label_ids: list[str] | object) -> str:
    """label_ids를 기반으로 화면 표시용 라벨 그룹을 계산합니다."""
    if not isinstance(label_ids, list):
        return "normal"

    labels = {str(item).upper() for item in label_ids}
    if "IMPORTANT" in labels:
        return "important"
    if "STARRED" in labels:
        return "starred"

    system_labels = {
        "INBOX",
        "UNREAD",
        "SPAM",
        "TRASH",
        "SENT",
        "DRAFT",
        "IMPORTANT",
        "STARRED",
        "CATEGORY_PERSONAL",
        "CATEGORY_SOCIAL",
        "CATEGORY_PROMOTIONS",
        "CATEGORY_UPDATES",
        "CATEGORY_FORUMS",
    }
    if any(label not in system_labels for label in labels):
        return "user_labeled"
    return "normal"


def _build_gmail_message_link(message_id: str) -> str:
    """gmail_message_id를 Gmail 웹 링크로 변환합니다."""
    return f"https://mail.google.com/mail/u/0/#inbox/{message_id}"


def _build_date_filter_clause(payload: TriagePreviewDbRequest) -> tuple[str, list[Any]]:
    """DB 조회용 날짜 필터 SQL 조각과 파라미터를 만든다."""
    if payload.date_filter == "all":
        return "", []

    if payload.date_filter == "range":
        if not payload.start_date or not payload.end_date:
            return "", []
        return (
            " AND to_timestamp((r.internal_date::bigint) / 1000.0) BETWEEN $5::date AND ($6::date + INTERVAL '1 day' - INTERVAL '1 second')",
            [payload.start_date, payload.end_date],
        )

    months_map = {"1m": 1, "3m": 3, "6m": 6}
    months = months_map.get(payload.date_filter)
    if months is None:
        return "", []

    return (
        " AND to_timestamp((r.internal_date::bigint) / 1000.0) <= NOW() - make_interval(months => $5::int)",
        [months],
    )

