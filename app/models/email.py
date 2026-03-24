from typing import Literal

from pydantic import BaseModel, Field


class EmailSyncRequest(BaseModel):
    access_token: str | None = Field(default=None, description="Google OAuth2 access token (deprecated; cookie preferred)")
    account_id: str = Field(..., min_length=2, max_length=200, description="서비스 내 사용자 식별자(예: Google 이메일)")
    user_id: str = Field(default="me", description="Gmail 사용자 ID (일반적으로 me)")
    max_results: int = Field(default=500, ge=1, le=500, description="받은편지함 전체 동기화 시 페이지당 조회 개수")


class EmailSyncResponse(BaseModel):
    fetched_count: int
    published_count: int
    topic: str


class TriagePreviewRequest(BaseModel):
    access_token: str | None = Field(default=None, description="Google OAuth2 access token (deprecated; cookie preferred)")
    user_id: str = Field(default="me", description="Gmail 사용자 ID (일반적으로 me)")
    max_unread: int = Field(default=100, ge=1, le=500, description="읽지 않은 메일 최대 조회 수")
    max_read: int = Field(default=100, ge=1, le=500, description="읽은 메일 최대 조회 수")


class TriagePreviewDbRequest(BaseModel):
    account_id: str = Field(..., min_length=2, max_length=200, description="조회할 사용자 식별자")
    max_unread: int = Field(default=100, ge=1, le=500, description="DB unread 메일 최대 조회 수")
    max_read: int = Field(default=100, ge=1, le=500, description="DB read 메일 최대 조회 수")
    date_filter: Literal["all", "1m", "3m", "6m", "range"] = Field(default="all", description="받은 지 N개월 지난 메일 필터")
    start_date: str | None = Field(default=None, description="직접 날짜 범위 시작일(YYYY-MM-DD)")
    end_date: str | None = Field(default=None, description="직접 날짜 범위 종료일(YYYY-MM-DD)")


class TriageGroupItem(BaseModel):
    category: str
    count: int
    avg_confidence_score: float = Field(..., ge=0.0, le=1.0)
    review_required_count: int = Field(default=0, ge=0)
    message_ids: list[str] = Field(default_factory=list)
    message_dates: list[str] = Field(default_factory=list)
    message_links: list[str] = Field(default_factory=list)
    sample_subjects: list[str] = Field(default_factory=list)


class SenderGroupItem(BaseModel):
    sender: str = Field(..., description="화면 표시용 발신자명")
    count: int
    categories: list[TriageGroupItem] = Field(default_factory=list)


class LabelGroupItem(BaseModel):
    label_group: Literal["normal"]
    count: int
    senders: list[SenderGroupItem] = Field(default_factory=list)


class BucketGroupItem(BaseModel):
    bucket: Literal["unread", "read", "important", "starred", "label"]
    count: int
    label_groups: list[LabelGroupItem] = Field(default_factory=list)


class TriagePreviewResponse(BaseModel):
    total_count: int
    buckets: list[BucketGroupItem]


class BulkActionRequest(BaseModel):
    access_token: str | None = Field(default=None, description="Google OAuth2 access token (deprecated; cookie preferred)")
    account_id: str = Field(..., min_length=2, max_length=200, description="서비스 내 사용자 식별자(예: Google 이메일)")
    user_id: str = Field(default="me", description="Gmail 사용자 ID (일반적으로 me)")
    action: Literal["archive", "inbox_unlabel", "trash"]
    message_ids: list[str] = Field(..., min_length=1, max_length=5000, description="일괄 처리할 Gmail message ID 목록")


class BulkActionResponse(BaseModel):
    action: Literal["archive", "inbox_unlabel", "trash"]
    processed_count: int
    failed_count: int = 0
    partial_failed: bool = False
    success_ids: list[str] = Field(default_factory=list)
    failed_ids: list[str] = Field(default_factory=list)


class LabelUpdateRequest(BaseModel):
    access_token: str | None = Field(default=None, description="Google OAuth2 access token (deprecated; cookie preferred)")
    account_id: str = Field(..., min_length=2, max_length=200, description="서비스 내 사용자 식별자(예: Google 이메일)")
    user_id: str = Field(default="me", description="Gmail 사용자 ID (일반적으로 me)")
    message_ids: list[str] = Field(..., min_length=1, max_length=5000, description="라벨을 변경할 Gmail message ID 목록")
    add_label_ids: list[str] = Field(default_factory=list, description="추가할 Gmail 라벨 ID 목록")
    remove_label_ids: list[str] = Field(default_factory=list, description="제거할 Gmail 라벨 ID 목록")


class LabelCreateRequest(BaseModel):
    access_token: str | None = Field(default=None, description="Google OAuth2 access token (deprecated; cookie preferred)")
    account_id: str = Field(..., min_length=2, max_length=200, description="서비스 내 사용자 식별자(예: Google 이메일)")
    user_id: str = Field(default="me", description="Gmail 사용자 ID (일반적으로 me)")
    name: str = Field(..., min_length=1, max_length=100, description="생성할 Gmail 사용자 라벨 이름")


class LabelCreateResponse(BaseModel):
    gmail_label_id: str
    name: str
    label_type: str


class LabelUpdateResponse(BaseModel):
    processed_count: int
    failed_count: int = 0
    partial_failed: bool = False
    success_ids: list[str] = Field(default_factory=list)
    failed_ids: list[str] = Field(default_factory=list)
