import asyncio

from app.api.email import _normalize_remove_label_ids
from app.services.gmail import GmailService


def test_normalize_remove_label_ids_always_includes_inbox() -> None:
    assert _normalize_remove_label_ids([]) == ["INBOX"]


def test_normalize_remove_label_ids_preserves_existing_labels() -> None:
    assert _normalize_remove_label_ids(["important", "STARRED"]) == ["IMPORTANT", "STARRED", "INBOX"]


def test_apply_bulk_action_accepts_inbox_unlabel(monkeypatch) -> None:
    service = GmailService()
    called = {"value": False}

    async def fake_batch_unarchive(*args, **kwargs) -> None:
        called["value"] = True

    monkeypatch.setattr(service, "_batch_unarchive", fake_batch_unarchive)

    result = asyncio.run(
        service.apply_bulk_action(
            access_token="token",
            action="inbox_unlabel",
            message_ids=["m1", "m2"],
            user_id="me",
        )
    )

    assert called["value"] is True
    assert result["processed_count"] == 2
    assert result["failed_ids"] == []
    assert result["missing_ids"] == []
