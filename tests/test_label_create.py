import asyncio

from app.api import email as email_api


class _FakeConn:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    async def __aenter__(self) -> "_FakeConn":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None

    async def execute(self, query: str, *args: object) -> None:
        self.calls.append((query, args))


class _FakePool:
    def __init__(self) -> None:
        self.conn = _FakeConn()

    def acquire(self) -> _FakeConn:
        return self.conn


def test_create_label_persists_metadata(monkeypatch) -> None:
    fake_pool = _FakePool()

    async def fake_create_label(*args, **kwargs):
        return {"gmail_label_id": "Label_123", "name": "Work", "label_type": "user"}

    monkeypatch.setattr(email_api.gmail_service, "create_label", fake_create_label)
    monkeypatch.setattr(email_api, "get_db_pool", lambda: fake_pool)

    result = asyncio.run(
        email_api.create_label(
            email_api.LabelCreateRequest(
                access_token="token",
                account_id="user@example.com",
                name="Work",
            )
        )
    )

    assert result.gmail_label_id == "Label_123"
    assert result.name == "Work"
    assert result.label_type == "user"
    assert fake_pool.conn.calls
    query, args = fake_pool.conn.calls[0]
    assert "INSERT INTO gmail_labels" in query
    assert args == ("user@example.com", "Label_123", "Work", "user")
