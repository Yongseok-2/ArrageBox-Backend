import asyncio

from app.worker import email_consumer
from app.worker.email_consumer import has_valid_account_id


def test_has_valid_account_id_true() -> None:
    assert has_valid_account_id({"account_id": "user@example.com"}) is True


def test_has_valid_account_id_unknown_false() -> None:
    assert has_valid_account_id({"account_id": "unknown"}) is False


def test_has_valid_account_id_empty_false() -> None:
    assert has_valid_account_id({}) is False


def test_upsert_email_persists_combined_payload() -> None:
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

    fake_pool = _FakePool()
    email = {
        "account_id": "user@example.com",
        "gmail_message_id": "msg-1",
        "gmail_thread_id": "thread-1",
        "subject": "Hello",
        "from_email": "a@example.com",
        "to_email": "b@example.com",
        "date_header": "Wed, 19 Mar 2026 00:00:00 +0000",
        "snippet": "snippet",
        "internal_date": "1234567890",
        "label_ids": ["IMPORTANT", "STARRED"],
        "raw": {"payload": {"body": "secret"}},
    }

    asyncio.run(email_consumer.upsert_email(fake_pool, email))

    _, args = fake_pool.conn.calls[0]
    assert args[0] == "user@example.com"
    assert args[1] == "msg-1"
    assert args[9] == '["IMPORTANT", "STARRED"]'
    assert args[10] == '{"payload": {"body": "secret"}}'
    assert args[13] == "other"
