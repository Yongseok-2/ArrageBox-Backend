from email.utils import parseaddr
from datetime import UTC, datetime
from typing import Any
import json

from fastapi import APIRouter, Cookie, HTTPException, status

from app.core.db import get_db_pool
from app.core.settings import settings
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


def _resolve_access_token(body_token: str | None, cookie_token: str | None) -> str:
    token = (body_token or cookie_token or "").strip()
    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Access token is required. Sign in again to refresh auth cookies.",
        )
    return token


@router.post("/sync", response_model=EmailSyncResponse, summary="????? ?? ???")
async def sync_unread_emails(payload: EmailSyncRequest, access_cookie: str | None = Cookie(default=None, alias=settings.auth_access_cookie_name)) -> EmailSyncResponse:
    access_token = _resolve_access_token(payload.access_token, access_cookie)
    inbox_emails = await gmail_service.fetch_unread_emails(access_token=access_token, user_id=payload.user_id, max_results=payload.max_results)

    published_count = 0
    for email in inbox_emails:
        email["account_id"] = payload.account_id
        await kafka_email_producer.publish_email(email)
        published_count += 1

    return EmailSyncResponse(fetched_count=len(inbox_emails), published_count=published_count, topic=kafka_email_producer.topic)


@router.post("/triage/preview", response_model=TriagePreviewResponse, summary="?? ?? ????")
async def preview_triage_groups(payload: TriagePreviewRequest, access_cookie: str | None = Cookie(default=None, alias=settings.auth_access_cookie_name)) -> TriagePreviewResponse:
    access_token = _resolve_access_token(payload.access_token, access_cookie)
    triage_data = await gmail_service.fetch_triage_emails(access_token=access_token, user_id=payload.user_id, max_unread=payload.max_unread, max_read=payload.max_read)

    groups: dict[tuple[str, str, str], dict[str, Any]] = {}
    for source_bucket, items in [("unread", triage_data["unread"]), ("read", triage_data["read"])]:
        for email in items:
            analysis = await email_analyzer.analyze_email(email)
            sender_display = _extract_sender_display(email.get("from_email", ""))
            sender_key = _sender_group_key(email.get("from_email", ""))
            category = analysis["category"]
            label_groups = _detect_label_groups(email.get("label_ids", []))
            message_date = _parse_internal_date_to_iso(email.get("internal_date"))
            for bucket in _triage_buckets_for_email(source_bucket=source_bucket, label_groups=label_groups):
                key = (bucket, sender_key, category)
                if key not in groups:
                    groups[key] = {"group_id": f"{bucket}|{sender_key}|{category}", "bucket": bucket, "sender": sender_display, "category": category, "count": 0, "confidence_sum": 0.0, "review_required_count": 0, "message_ids": [], "message_dates": [], "sample_subjects": []}
                group = groups[key]
                group["count"] += 1
                group["confidence_sum"] += float(analysis.get("confidence_score", 0.0))
                if analysis.get("review_required", False):
                    group["review_required_count"] += 1
                message_id = str(email.get("gmail_message_id", ""))
                group["message_ids"].append(message_id)
                if message_date:
                    group["message_dates"].append(message_date)
                if len(group["sample_subjects"]) < 3:
                    group["sample_subjects"].append(email.get("subject", ""))

    return _build_triage_response(groups=groups)


@router.post("/triage/preview-db", response_model=TriagePreviewResponse, summary="DB ?? ?? ?? ????")
async def preview_triage_groups_from_db(payload: TriagePreviewDbRequest) -> TriagePreviewResponse:
    date_filter_clause, date_filter_params = _build_date_filter_clause(payload)
    query = f"""
    WITH source AS (
        SELECT a.gmail_message_id, a.from_email, a.subject, a.label_ids,
            CASE WHEN COALESCE(a.internal_date, '') ~ '^[0-9]+$' THEN to_timestamp((a.internal_date::bigint) / 1000.0) ELSE NULL END AS internal_ts,
            a.internal_date,
            a.category, a.confidence_score, a.review_required
        FROM email_analysis a
        WHERE a.account_id = $3 AND NOT (a.label_ids ?| ARRAY['TRASH', 'SPAM']) {date_filter_clause}
    ), unread_rows AS (
        SELECT *, 'unread'::text AS bucket FROM source WHERE (label_ids ? 'UNREAD') ORDER BY internal_ts DESC NULLS LAST LIMIT $1
    ), read_rows AS (
        SELECT *, 'read'::text AS bucket FROM source WHERE NOT (label_ids ? 'UNREAD') AND internal_ts IS NOT NULL ORDER BY internal_ts DESC NULLS LAST LIMIT $2
    )
    SELECT * FROM unread_rows
    UNION ALL
    SELECT * FROM read_rows
    """

    pool = get_db_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(query, payload.max_unread, payload.max_read, payload.account_id, *date_filter_params)

    groups: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in rows:
        from_email = row["from_email"] or ""
        sender_display = _extract_sender_display(from_email)
        sender_key = _sender_group_key(from_email)
        source_bucket = str(row["bucket"])
        category = str(row["category"])
        label_groups = _detect_label_groups(row["label_ids"] or [])
        message_date = _parse_internal_date_to_iso(row["internal_date"])
        for bucket in _triage_buckets_for_email(source_bucket=source_bucket, label_groups=label_groups):
            key = (bucket, sender_key, category)
            if key not in groups:
                groups[key] = {"group_id": f"{bucket}|{sender_key}|{category}", "bucket": bucket, "sender": sender_display, "category": category, "count": 0, "confidence_sum": 0.0, "review_required_count": 0, "message_ids": [], "message_dates": [], "sample_subjects": []}
            group = groups[key]
            group["count"] += 1
            group["confidence_sum"] += float(row["confidence_score"] or 0.0)
            if bool(row["review_required"]):
                group["review_required_count"] += 1
            message_id = str(row["gmail_message_id"])
            group["message_ids"].append(message_id)
            if message_date:
                group["message_dates"].append(message_date)
            subject = str(row["subject"] or "")
            if subject and len(group["sample_subjects"]) < 3:
                group["sample_subjects"].append(subject)

    return _build_triage_response(groups=groups)


@router.post("/triage/action", response_model=BulkActionResponse, summary="?? ?? ??")
async def apply_triage_action(payload: BulkActionRequest, access_cookie: str | None = Cookie(default=None, alias=settings.auth_access_cookie_name)) -> BulkActionResponse:
    access_token = _resolve_access_token(payload.access_token, access_cookie)
    result = await gmail_service.apply_bulk_action(access_token=access_token, action=payload.action, message_ids=payload.message_ids, user_id=payload.user_id)
    failed_ids = result["failed_ids"]
    failed_set = set(failed_ids)
    success_ids = [mid for mid in payload.message_ids if mid not in failed_set]
    if success_ids and payload.action in {"archive", "inbox_unlabel"}:
        await _sync_label_ids_in_db(account_id=payload.account_id, message_ids=success_ids, add_label_ids=[], remove_label_ids=["INBOX"])
    if payload.action == "trash":
        await _delete_messages_from_db(account_id=payload.account_id, message_ids=success_ids)
    return BulkActionResponse(action=payload.action, processed_count=result["processed_count"], failed_count=len(failed_ids), partial_failed=bool(failed_ids), success_ids=success_ids, failed_ids=failed_ids)


@router.post("/labels", response_model=LabelUpdateResponse, summary="?? ?? ???")
async def update_labels(payload: LabelUpdateRequest, access_cookie: str | None = Cookie(default=None, alias=settings.auth_access_cookie_name)) -> LabelUpdateResponse:
    access_token = _resolve_access_token(payload.access_token, access_cookie)
    remove_label_ids = _normalize_remove_label_ids(payload.remove_label_ids)
    result = await gmail_service.apply_label_updates(access_token=access_token, user_id=payload.user_id, message_ids=payload.message_ids, add_label_ids=payload.add_label_ids, remove_label_ids=remove_label_ids)
    failed_ids = result["failed_ids"]
    failed_set = set(failed_ids)
    success_ids = [mid for mid in payload.message_ids if mid not in failed_set]
    if success_ids:
        await _sync_label_ids_in_db(account_id=payload.account_id, message_ids=success_ids, add_label_ids=payload.add_label_ids, remove_label_ids=remove_label_ids)
    return LabelUpdateResponse(processed_count=result["processed_count"], failed_count=len(failed_ids), partial_failed=bool(failed_ids), success_ids=success_ids, failed_ids=failed_ids)


@router.post("/labels/create", response_model=LabelCreateResponse, summary="??? ?? ??")
async def create_label(payload: LabelCreateRequest, access_cookie: str | None = Cookie(default=None, alias=settings.auth_access_cookie_name)) -> LabelCreateResponse:
    access_token = _resolve_access_token(payload.access_token, access_cookie)
    created_label = await gmail_service.create_label(access_token=access_token, user_id=payload.user_id, name=payload.name)
    await _upsert_gmail_label_metadata(account_id=payload.account_id, gmail_label_id=str(created_label["gmail_label_id"]), name=str(created_label["name"]), label_type=str(created_label["label_type"]))
    return LabelCreateResponse(gmail_label_id=str(created_label["gmail_label_id"]), name=str(created_label["name"]), label_type=str(created_label["label_type"]))


async def _delete_messages_from_db(account_id: str, message_ids: list[str]) -> None:
    if not message_ids:
        return
    query = "DELETE FROM email_analysis WHERE account_id = $1 AND gmail_message_id = ANY($2::text[])"
    pool = get_db_pool()
    async with pool.acquire() as conn:
        await conn.execute(query, account_id, message_ids)


async def _sync_label_ids_in_db(account_id: str, message_ids: list[str], add_label_ids: list[str], remove_label_ids: list[str]) -> None:
    if not message_ids:
        return
    pool = get_db_pool()
    select_sql = """
    SELECT gmail_message_id, label_ids
    FROM email_analysis
    WHERE account_id = $1 AND gmail_message_id = ANY($2::text[])
    """
    update_sql = """
    UPDATE email_analysis
    SET label_ids = $3::jsonb
    WHERE account_id = $1 AND gmail_message_id = $2
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(select_sql, account_id, message_ids)
        for row in rows:
            current = _normalize_label_ids(row["label_ids"] or [])
            labels = {str(item).upper() for item in current}
            for label in remove_label_ids:
                labels.discard(str(label).upper())
            for label in add_label_ids:
                labels.add(str(label).upper())
            labels_json_str = json.dumps(sorted(labels))
            await conn.execute(update_sql, account_id, row["gmail_message_id"], labels_json_str)


async def _upsert_gmail_label_metadata(account_id: str, gmail_label_id: str, name: str, label_type: str) -> None:
    pool = get_db_pool()
    query = """
    INSERT INTO gmail_labels (account_id, gmail_label_id, label_name, label_type, updated_at)
    VALUES ($1, $2, $3, $4, NOW())
    ON CONFLICT (account_id, gmail_label_id)
    DO UPDATE SET label_name = EXCLUDED.label_name, label_type = EXCLUDED.label_type, updated_at = EXCLUDED.updated_at
    """
    async with pool.acquire() as conn:
        await conn.execute(query, account_id, gmail_label_id, name, label_type)


def _normalize_remove_label_ids(remove_label_ids: list[str]) -> list[str]:
    normalized = [str(label).upper() for label in remove_label_ids if str(label).strip()]
    if "INBOX" not in normalized:
        normalized.append("INBOX")
    return list(dict.fromkeys(normalized))


def _build_triage_response(groups: dict[tuple[str, str, str], dict[str, Any]]) -> TriagePreviewResponse:
    bucket_order = ["unread", "read", "important", "starred", "label"]
    nested: dict[str, dict[str, dict[str, Any]]] = {bucket: {} for bucket in bucket_order}
    total_count = 0
    for group in groups.values():
        count = int(group["count"])
        confidence_sum = float(group["confidence_sum"])
        bucket = str(group["bucket"])
        sender = str(group["sender"])
        category = str(group["category"])
        category_item = {"category": category, "count": count, "avg_confidence_score": round(confidence_sum / count, 4) if count else 0.0, "review_required_count": int(group["review_required_count"]), "message_ids": [str(mid) for mid in group["message_ids"] if mid], "message_dates": [str(item) for item in group.get("message_dates", []) if item], "message_links": [_build_gmail_message_link(str(mid)) for mid in group["message_ids"] if mid], "sample_subjects": [str(subject) for subject in group["sample_subjects"]]}
        sender_map = nested[bucket].setdefault(sender, {})
        sender_map[category] = category_item
        total_count += count
    buckets: list[BucketGroupItem] = []
    for bucket_name in bucket_order:
        sender_map = nested[bucket_name]
        sender_items: list[SenderGroupItem] = []
        bucket_total = 0
        for sender, categories in sorted(sender_map.items(), key=lambda item: sum(cat["count"] for cat in item[1].values()), reverse=True):
            category_items = [TriageGroupItem(category=category, count=category_data["count"], avg_confidence_score=category_data["avg_confidence_score"], review_required_count=category_data["review_required_count"], message_ids=category_data["message_ids"], message_dates=category_data["message_dates"], message_links=category_data["message_links"], sample_subjects=category_data["sample_subjects"]) for category, category_data in sorted(categories.items(), key=lambda item: item[1]["count"], reverse=True)]
            sender_count = sum(item.count for item in category_items)
            bucket_total += sender_count
            sender_items.append(SenderGroupItem(sender=sender, count=sender_count, categories=category_items))
        sender_items.sort(key=lambda item: item.count, reverse=True)
        buckets.append(BucketGroupItem(bucket=bucket_name, count=bucket_total, label_groups=[LabelGroupItem(label_group="normal", count=bucket_total, senders=sender_items)] if sender_items else []))
    buckets.sort(key=lambda item: item.count, reverse=True)
    return TriagePreviewResponse(total_count=total_count, buckets=buckets)


def _extract_sender_display(raw_from: str) -> str:
    name, email_addr = parseaddr((raw_from or "").strip())
    if name:
        return name.strip()
    if email_addr:
        local = email_addr.split("@", 1)[0].strip()
        return local or email_addr.strip().lower()
    return (raw_from or "").strip()


def _sender_group_key(raw_from: str) -> str:
    name, email_addr = parseaddr((raw_from or "").strip())
    if email_addr:
        return email_addr.strip().lower()
    if name:
        return name.strip().lower()
    return (raw_from or "").strip().lower()


def _detect_label_groups(label_ids: list[str] | object) -> list[str]:
    labels = {str(item).upper() for item in _normalize_label_ids(label_ids)}
    groups: list[str] = []
    if "IMPORTANT" in labels:
        groups.append("important")
    if "STARRED" in labels:
        groups.append("starred")
    system_labels = {"INBOX", "UNREAD", "SPAM", "TRASH", "SENT", "DRAFT", "IMPORTANT", "STARRED", "CATEGORY_PERSONAL", "CATEGORY_SOCIAL", "CATEGORY_PROMOTIONS", "CATEGORY_UPDATES", "CATEGORY_FORUMS"}
    if any(label not in system_labels for label in labels):
        groups.append("label")
    if not groups:
        groups.append("normal")
    return groups


def _normalize_label_ids(label_ids: list[str] | tuple[str, ...] | str | object) -> list[str]:
    if isinstance(label_ids, list):
        return [str(item).strip() for item in label_ids if str(item).strip()]
    if isinstance(label_ids, tuple):
        return [str(item).strip() for item in label_ids if str(item).strip()]
    if isinstance(label_ids, str):
        raw = label_ids.strip()
        if not raw:
            return []
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return [raw]
        if isinstance(parsed, list):
            return [str(item).strip() for item in parsed if str(item).strip()]
        return [str(parsed).strip()] if str(parsed).strip() else []
    return []


def _parse_internal_date_to_iso(internal_date: Any) -> str | None:
    if internal_date is None:
        return None
    raw = str(internal_date).strip()
    if not raw.isdigit():
        return None
    return datetime.fromtimestamp(int(raw) / 1000.0, tz=UTC).isoformat()


def _triage_buckets_for_email(source_bucket: str, label_groups: list[str]) -> list[str]:
    buckets: list[str] = []
    if source_bucket in {"unread", "read"}:
        buckets.append(source_bucket)
    if "important" in label_groups:
        buckets.append("important")
    if "starred" in label_groups:
        buckets.append("starred")
    if "label" in label_groups:
        buckets.append("label")
    return list(dict.fromkeys(buckets))


def _build_gmail_message_link(message_id: str) -> str:
    return f"https://mail.google.com/mail/u/0/#inbox/{message_id}"


def _build_date_filter_clause(payload: TriagePreviewDbRequest) -> tuple[str, list[Any]]:
    if payload.date_filter == "all":
        return "", []
    if payload.date_filter == "range":
        if not payload.start_date or not payload.end_date:
            return "", []
        return (" AND to_timestamp((r.internal_date::bigint) / 1000.0) BETWEEN $4::date AND ($5::date + INTERVAL '1 day' - INTERVAL '1 second')", [payload.start_date, payload.end_date])
    months_map = {"1m": 1, "3m": 3, "6m": 6}
    months = months_map.get(payload.date_filter)
    if months is None:
        return "", []
    return (" AND to_timestamp((r.internal_date::bigint) / 1000.0) <= NOW() - make_interval(months => $4::int)", [months])
