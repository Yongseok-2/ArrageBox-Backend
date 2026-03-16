from datetime import datetime

from pydantic import BaseModel, Field


class EmailAnalysisItem(BaseModel):
    account_id: str
    gmail_message_id: str
    subject: str | None = None
    from_email: str | None = None
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
