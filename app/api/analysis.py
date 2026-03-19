from fastapi import APIRouter, Query

from app.core.db import get_db_pool
from app.models.analysis import EmailAnalysisItem, EmailAnalysisListResponse, EmailAnalysisRecentRequest

router = APIRouter(prefix="/analysis", tags=["analysis"])


@router.get(
    "/recent",
    response_model=EmailAnalysisListResponse,
    summary="최근 분석 결과 조회",
)
async def get_recent_analysis(
    account_id: str = Query(..., min_length=2, max_length=200),
    limit: int = Query(default=20, ge=1, le=200),
    date_filter: str = Query(default="all"),
    start_date: str | None = Query(default=None),
    end_date: str | None = Query(default=None),
) -> EmailAnalysisListResponse:
    """지정한 account_id의 메일 분석 결과 목록을 반환합니다."""
    request = EmailAnalysisRecentRequest(
        account_id=account_id,
        limit=limit,
        date_filter=date_filter,
        start_date=start_date,
        end_date=end_date,
    )
    date_filter_clause, params = _build_analysis_date_filter_clause(request)
    limit_placeholder = 2 + len(params)
    query = f"""
    SELECT
        a.account_id,
        a.gmail_message_id,
        a.subject,
        a.from_email,
        a.category,
        a.urgency_score,
        a.summary,
        a.keywords,
        a.confidence_score,
        a.analysis_source,
        a.review_required,
        a.analyzed_at
    FROM email_analysis a
    WHERE a.account_id = $1
      {date_filter_clause}
    ORDER BY a.analyzed_at DESC
    LIMIT ${limit_placeholder}
    """

    pool = get_db_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(query, account_id, *params, limit)

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


def _build_analysis_date_filter_clause(payload: EmailAnalysisRecentRequest) -> tuple[str, list[str]]:
    if payload.date_filter == "all":
        return "", []

    if payload.date_filter == "range":
        if not payload.start_date or not payload.end_date:
            return "", []
        return (" AND a.internal_date <> '' AND to_timestamp((a.internal_date::bigint) / 1000.0) BETWEEN $2::date AND ($3::date + INTERVAL '1 day' - INTERVAL '1 second')", [payload.start_date, payload.end_date])

    months_map = {"1m": 1, "3m": 3, "6m": 6}
    months = months_map.get(payload.date_filter)
    if months is None:
        return "", []

    return (
        " AND a.internal_date <> '' AND to_timestamp((a.internal_date::bigint) / 1000.0) <= NOW() - make_interval(months => $2::int)",
        [str(months)],
    )
