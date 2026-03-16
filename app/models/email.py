from typing import Literal

from pydantic import BaseModel, Field


class EmailSyncRequest(BaseModel):
    access_token: str = Field(..., description="Google OAuth2 access token")
    account_id: str = Field(..., min_length=2, max_length=200, description="서비스 내 사용자 식별자(예: Google 이메일)")
    user_id: str = Field(default="me", description="Gmail 사용자 ID (일반적으로 me)")
    max_results: int = Field(default=20, ge=1, le=100, description="읽지 않은 메일 조회 개수")


class EmailSyncResponse(BaseModel):
    fetched_count: int
    published_count: int
    topic: str


class TriagePreviewRequest(BaseModel):
    access_token: str = Field(..., description="Google OAuth2 access token")
    user_id: str = Field(default="me", description="Gmail 사용자 ID (일반적으로 me)")
    max_unread: int = Field(default=100, ge=1, le=500, description="읽지 않은 메일 최대 조회 수")
    max_stale: int = Field(default=100, ge=1, le=500, description="6개월 이상 메일 최대 조회 수")


class TriagePreviewDbRequest(BaseModel):
    account_id: str = Field(..., min_length=2, max_length=200, description="조회할 사용자 식별자")
    max_unread: int = Field(default=100, ge=1, le=500, description="DB unread 메일 최대 조회 수")
    max_stale: int = Field(default=100, ge=1, le=500, description="DB stale 메일 최대 조회 수")
    stale_months: int = Field(default=6, ge=1, le=36, description="stale 판정 기준 개월 수")


class TriageGroupItem(BaseModel):
    group_id: str
    bucket: Literal["unread", "stale"]
    label_group: Literal["important", "starred", "user_labeled", "normal"]
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


class LabelSummaryItem(BaseModel):
    label_group: Literal["important", "starred", "user_labeled", "normal"]
    count: int


class TriagePreviewResponse(BaseModel):
    total_unread: int
    total_stale: int
    groups: list[TriageGroupItem]
    bucket_summary: list[BucketSummaryItem]
    category_summary: list[CategorySummaryItem]
    label_summary: list[LabelSummaryItem]


class BulkActionRequest(BaseModel):
    access_token: str = Field(..., description="Google OAuth2 access token")
    account_id: str = Field(..., min_length=2, max_length=200, description="서비스 내 사용자 식별자(예: Google 이메일)")
    user_id: str = Field(default="me", description="Gmail 사용자 ID (일반적으로 me)")
    action: Literal["archive", "trash"]
    message_ids: list[str] = Field(..., min_length=1, max_length=5000, description="일괄 처리할 Gmail message ID 목록")


class BulkActionResponse(BaseModel):
    action: Literal["archive", "trash"]
    processed_count: int
    failed_count: int = 0
    partial_failed: bool = False
    success_ids: list[str] = Field(default_factory=list)
    failed_ids: list[str] = Field(default_factory=list)


class LabelUpdateRequest(BaseModel):
    access_token: str = Field(..., description="Google OAuth2 access token")
    account_id: str = Field(..., min_length=2, max_length=200, description="서비스 내 사용자 식별자(예: Google 이메일)")
    user_id: str = Field(default="me", description="Gmail 사용자 ID (일반적으로 me)")
    message_ids: list[str] = Field(..., min_length=1, max_length=5000, description="라벨을 변경할 Gmail message ID 목록")
    add_label_ids: list[str] = Field(default_factory=list, description="추가할 Gmail 라벨 ID 목록")
    remove_label_ids: list[str] = Field(default_factory=list, description="제거할 Gmail 라벨 ID 목록")


class LabelUpdateResponse(BaseModel):
    processed_count: int
    failed_count: int = 0
    partial_failed: bool = False
    success_ids: list[str] = Field(default_factory=list)
    failed_ids: list[str] = Field(default_factory=list)
