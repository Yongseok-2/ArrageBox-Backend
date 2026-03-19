from app.api.analysis import _build_analysis_date_filter_clause
from app.api.email import _build_date_filter_clause
from app.models.analysis import EmailAnalysisRecentRequest
from app.models.email import TriagePreviewDbRequest


def test_build_date_filter_clause_all() -> None:
    clause, params = _build_date_filter_clause(
        TriagePreviewDbRequest(account_id="user@example.com")
    )
    assert clause == ""
    assert params == []


def test_build_date_filter_clause_range() -> None:
    clause, params = _build_date_filter_clause(
        TriagePreviewDbRequest(
            account_id="user@example.com",
            date_filter="range",
            start_date="2026-01-01",
            end_date="2026-01-31",
        )
    )
    assert "BETWEEN $5::date AND ($6::date" in clause
    assert params == ["2026-01-01", "2026-01-31"]


def test_build_analysis_date_filter_clause_3m() -> None:
    clause, params = _build_analysis_date_filter_clause(
        EmailAnalysisRecentRequest(account_id="user@example.com", date_filter="3m")
    )
    assert "<= NOW() - make_interval(months => $2::int)" in clause
    assert params == ["3"]
