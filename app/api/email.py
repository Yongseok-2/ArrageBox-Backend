# -*- coding: utf-8 -*-
from email.utils import parseaddr

from fastapi import APIRouter

from app.models.email import (
    BucketSummaryItem,
    BulkActionRequest,
    BulkActionResponse,
    CategorySummaryItem,
    EmailSyncRequest,
    EmailSyncResponse,
    TriageGroupItem,
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
    description="읽지 않은 메일을 Gmail에서 가져와 Kafka 토픽으로 발행합니다.",
)
async def sync_unread_emails(payload: EmailSyncRequest) -> EmailSyncResponse:
    """읽지 않은 메일을 Kafka로 발행한다."""
    unread_emails = await gmail_service.fetch_unread_emails(
        access_token=payload.access_token,
        user_id=payload.user_id,
        max_results=payload.max_results,
    )

    published_count = 0
    for email in unread_emails:
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
    description="안읽음/오래된 메일을 발신자 표시명+카테고리로 그룹화해 일괄 처리 후보를 반환합니다.",
)
async def preview_triage_groups(payload: TriagePreviewRequest) -> TriagePreviewResponse:
    """일괄 처리 화면용 그룹 데이터를 생성한다."""
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
        total_unread=len(triage_data["unread"]),
        total_stale=len(triage_data["stale"]),
        groups=sorted_groups,
        bucket_summary=bucket_summary,
        category_summary=category_summary,
    )


@router.post(
    "/triage/action",
    response_model=BulkActionResponse,
    summary="일괄 액션 실행",
    description="선택한 message_ids에 대해 archive 또는 trash를 실행합니다.",
)
async def apply_triage_action(payload: BulkActionRequest) -> BulkActionResponse:
    """선택한 메일에 대해 일괄 보관/삭제를 수행한다."""
    result = await gmail_service.apply_bulk_action(
        access_token=payload.access_token,
        action=payload.action,
        message_ids=payload.message_ids,
        user_id=payload.user_id,
    )
    return BulkActionResponse(
        action=payload.action,
        processed_count=result["processed_count"],
        failed_ids=result["failed_ids"],
    )


def _extract_sender_display(raw_from: str) -> str:
    """From 헤더에서 표시 이름(예: Instagram)을 우선 추출한다."""
    name, email_addr = parseaddr((raw_from or "").strip())
    if name:
        return name.strip()
    if email_addr:
        local = email_addr.split("@", 1)[0].strip()
        return local or email_addr.strip().lower()
    return (raw_from or "").strip()


def _sender_group_key(raw_from: str) -> str:
    """내부 그룹 식별용 키(가능하면 이메일 주소)를 만든다."""
    name, email_addr = parseaddr((raw_from or "").strip())
    if email_addr:
        return email_addr.strip().lower()
    if name:
        return name.strip().lower()
    return (raw_from or "").strip().lower()


def _build_bucket_summary(groups: list[TriageGroupItem]) -> list[BucketSummaryItem]:
    """버킷별(안읽음/오래된 메일) 총 건수와 그룹 수를 계산한다."""
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
    """카테고리별 누적 메일 건수를 계산한다."""
    counts: dict[str, int] = {}
    for group in groups:
        counts[group.category] = counts.get(group.category, 0) + group.count

    sorted_counts = sorted(counts.items(), key=lambda item: item[1], reverse=True)
    return [CategorySummaryItem(category=category, count=count) for category, count in sorted_counts]