from datetime import datetime

from pydantic import BaseModel, Field


class EmailAnalysisItem(BaseModel):
    account_id: str
    gmail_message_id: str
    subject: str | None = None
    from_email: str | None = None
    internal_date: str | None = None
    received_at: datetime | None = None
    category: str
    urgency_score: int = Field(..., ge=0, le=100)
    summary: str
    keywords: list[str] = Field(default_factory=list)
    confidence_score: float = Field(..., ge=0.0, le=1.0)
    analysis_source: str
    review_required: bool
    analyzed_at: datetime


class EmailAnalysisListResponse(BaseModel):
    items: list[EmailAnalysisItem]
    count: int


class EmailAnalysisRecentRequest(BaseModel):
    account_id: str = Field(..., min_length=2, max_length=200)
    limit: int = Field(default=20, ge=1, le=200)
    date_filter: str = Field(default="all", description="받은 지 N개월 지난 메일 필터(all, 1m, 3m, 6m, range)")
    start_date: str | None = Field(default=None, description="직접 날짜 범위 시작일(YYYY-MM-DD)")
    end_date: str | None = Field(default=None, description="직접 날짜 범위 종료일(YYYY-MM-DD)")
