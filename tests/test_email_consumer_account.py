from app.worker.email_consumer import has_valid_account_id


def test_has_valid_account_id_true() -> None:
    assert has_valid_account_id({"account_id": "user@example.com"}) is True


def test_has_valid_account_id_unknown_false() -> None:
    assert has_valid_account_id({"account_id": "unknown"}) is False


def test_has_valid_account_id_empty_false() -> None:
    assert has_valid_account_id({}) is False
