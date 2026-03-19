from app.services.email_analyzer import email_analyzer


def test_build_summary_is_short() -> None:
    summary = email_analyzer._build_summary(
        subject="Subject",
        snippet="x" * 500,
    )
    assert len(summary) <= 120
