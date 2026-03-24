import asyncio

from app.api import email as email_api


class _FakeConn:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[object, ...]]] = []
        self.rows: list[dict[str, object]] = [
            {"gmail_message_id": "m1", "label_ids": ["INBOX", "UNREAD"]},
            {"gmail_message_id": "m2", "label_ids": ["INBOX"]},
        ]

    async def __aenter__(self) -> "_FakeConn":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None

    async def execute(self, query: str, *args: object) -> None:
        self.calls.append((query, args))

    async def fetch(self, query: str, *args: object):
        return self.rows


class _FakePool:
    def __init__(self) -> None:
        self.conn = _FakeConn()

    def acquire(self) -> _FakeConn:
        return self.conn


def test_apply_triage_action_updates_db_for_archive(monkeypatch) -> None:
    fake_pool = _FakePool()

    async def fake_apply_bulk_action(*args, **kwargs):
        return {"processed_count": 2, "failed_ids": []}

    monkeypatch.setattr(email_api.gmail_service, "apply_bulk_action", fake_apply_bulk_action)
    monkeypatch.setattr(email_api, "get_db_pool", lambda: fake_pool)

    result = asyncio.run(
        email_api.apply_triage_action(
            email_api.BulkActionRequest(
                access_token="token",
                account_id="user@example.com",
                action="archive",
                message_ids=["m1", "m2"],
            )
        )
    )

    assert result.success_ids == ["m1", "m2"]
    assert fake_pool.conn.calls
    update_calls = [call for call in fake_pool.conn.calls if "UPDATE email_analysis" in call[0]]
    assert update_calls
    _, args = update_calls[0]
    assert args[0] == "user@example.com"
    assert args[2] == '["UNREAD"]'


def test_apply_triage_action_deletes_db_for_trash(monkeypatch) -> None:
    fake_pool = _FakePool()

    async def fake_apply_bulk_action(*args, **kwargs):
        return {"processed_count": 1, "failed_ids": []}

    monkeypatch.setattr(email_api.gmail_service, "apply_bulk_action", fake_apply_bulk_action)
    monkeypatch.setattr(email_api, "get_db_pool", lambda: fake_pool)

    result = asyncio.run(
        email_api.apply_triage_action(
            email_api.BulkActionRequest(
                access_token="token",
                account_id="user@example.com",
                action="trash",
                message_ids=["m1"],
            )
        )
    )

    assert result.success_ids == ["m1"]
    query, args = fake_pool.conn.calls[0]
    assert "DELETE FROM email_analysis" in query
    assert args[0] == "user@example.com"
