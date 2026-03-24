from app.api.email import _detect_label_groups, _normalize_label_ids


def test_detect_label_group_important_first() -> None:
    assert _detect_label_groups(["INBOX", "IMPORTANT", "STARRED"]) == ["important", "starred"]


def test_detect_label_group_starred() -> None:
    assert _detect_label_groups(["INBOX", "STARRED"]) == ["starred"]


def test_detect_label_group_user_labeled() -> None:
    assert _detect_label_groups(["INBOX", "my-custom-tag"]) == ["label"]


def test_detect_label_group_normal() -> None:
    assert _detect_label_groups(["INBOX", "UNREAD"]) == ["normal"]


def test_normalize_label_ids_parses_json_string() -> None:
    assert _normalize_label_ids('["INBOX", "STARRED"]') == ["INBOX", "STARRED"]
