from app.api.email import _detect_label_group


def test_detect_label_group_important_first() -> None:
    assert _detect_label_group(["INBOX", "IMPORTANT", "STARRED"]) == "important"


def test_detect_label_group_starred() -> None:
    assert _detect_label_group(["INBOX", "STARRED"]) == "starred"


def test_detect_label_group_user_labeled() -> None:
    assert _detect_label_group(["INBOX", "my-custom-tag"]) == "user_labeled"


def test_detect_label_group_normal() -> None:
    assert _detect_label_group(["INBOX", "UNREAD"]) == "normal"
