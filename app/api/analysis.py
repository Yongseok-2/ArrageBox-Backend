from fastapi import APIRouter, Query
import asyncpg

from app.core.settings import settings
from app.models.analysis import EmailAnalysisItem, EmailAnalysisListResponse

router = APIRouter(prefix="/analysis", tags=["analysis"])


@router.get(
    "/recent",
    response_model=EmailAnalysisListResponse,
    summary="최근 분석 결과 조회",
)
async def get_recent_analysis(
    account_id: str = Query(..., min_length=2, max_length=200),
    limit: int = Query(default=20, ge=1, le=200),
) -> EmailAnalysisListResponse:
    """지정한 account_id의 최근 메일 분석 결과 목록을 반환합니다."""
    query = """
    SELECT
        a.account_id,
        a.gmail_message_id,
        r.subject,
        r.from_email,
        a.category,
        a.urgency_score,
        a.summary,
        a.keywords,
        a.confidence_score,
        a.analysis_source,
        a.review_required,
        a.analyzed_at
    FROM email_analysis a
    LEFT JOIN emails_raw r ON r.gmail_message_id = a.gmail_message_id
    WHERE a.account_id = $1
    ORDER BY a.analyzed_at DESC
    LIMIT $2
    """

    pool = await asyncpg.create_pool(dsn=settings.postgres_dsn, min_size=1, max_size=3)
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(query, account_id, limit)
    finally:
        await pool.close()

    items = [
        EmailAnalysisItem(
            account_id=row["account_id"],
            gmail_message_id=row["gmail_message_id"],
            subject=row["subject"],
            from_email=row["from_email"],
            category=row["category"],
            urgency_score=row["urgency_score"],
            summary=row["summary"],
            keywords=row["keywords"] or [],
            confidence_score=float(row["confidence_score"]),
            analysis_source=row["analysis_source"],
            review_required=bool(row["review_required"]),
            analyzed_at=row["analyzed_at"],
        )
        for row in rows
    ]
    return EmailAnalysisListResponse(items=items, count=len(items))
