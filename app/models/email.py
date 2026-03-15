# -*- coding: utf-8 -*-
from typing import Literal

from pydantic import BaseModel, Field


class EmailSyncRequest(BaseModel):
    access_token: str = Field(..., description="Google OAuth2 access token")
    user_id: str = Field(default="me", description="Gmail 사용자 ID (일반적으로 me)")
    max_results: int = Field(default=20, ge=1, le=100, description="읽지 않은 메일 조회 개수")


class EmailSyncResponse(BaseModel):
    fetched_count: int
    published_count: int
    topic: str


class TriagePreviewRequest(BaseModel):
    access_token: str = Field(..., description="Google OAuth2 access token")
    user_id: str = Field(default="me", description="Gmail 사용자 ID (일반적으로 me)")
    max_unread: int = Field(default=100, ge=1, le=500, description="안읽음 메일 최대 조회 수")
    max_stale: int = Field(default=100, ge=1, le=500, description="6개월 이상 메일 최대 조회 수")


class TriageGroupItem(BaseModel):
    group_id: str
    bucket: Literal["unread", "stale"]
    sender: str = Field(..., description="화면 표시용 발신자명")
    category: str
    count: int
    avg_confidence_score: float = Field(..., ge=0.0, le=1.0)
    review_required_count: int = Field(default=0, ge=0)
    message_ids: list[str] = Field(default_factory=list)
    sample_subjects: list[str] = Field(default_factory=list)


class BucketSummaryItem(BaseModel):
    bucket: Literal["unread", "stale"]
    count: int
    group_count: int


class CategorySummaryItem(BaseModel):
    category: str
    count: int


class TriagePreviewResponse(BaseModel):
    total_unread: int
    total_stale: int
    groups: list[TriageGroupItem]
    bucket_summary: list[BucketSummaryItem]
    category_summary: list[CategorySummaryItem]


class BulkActionRequest(BaseModel):
    access_token: str = Field(..., description="Google OAuth2 access token")
    user_id: str = Field(default="me", description="Gmail 사용자 ID (일반적으로 me)")
    action: Literal["archive", "trash"]
    message_ids: list[str] = Field(..., min_length=1, max_length=5000, description="일괄 처리할 Gmail message ID 목록")


class BulkActionResponse(BaseModel):
    action: Literal["archive", "trash"]
    processed_count: int
    failed_ids: list[str] = Field(default_factory=list)