from app.api.email import _triage_buckets_for_email


def test_triage_buckets_unread_starred_overlap() -> None:
    assert _triage_buckets_for_email("unread", ["starred"]) == ["unread", "starred"]


def test_triage_buckets_read_important_overlap() -> None:
    assert _triage_buckets_for_email("read", ["important"]) == ["read", "important"]


def test_triage_buckets_custom_label_overlap() -> None:
    assert _triage_buckets_for_email("unread", ["label"]) == ["unread", "label"]


def test_triage_buckets_important_and_starred_overlap() -> None:
    assert _triage_buckets_for_email("unread", ["important", "starred"]) == ["unread", "important", "starred"]
