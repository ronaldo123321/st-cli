"""Tests for the ``st snapshot`` command."""

import json
from pathlib import Path

from click.testing import CliRunner

from st_cli.cli import cli
from st_cli.commands import snapshot_cmd
from st_cli.pipeline import PipelineFailure, PipelineSuccess


class _Cred:
    def __init__(self) -> None:
        self.cookies = {"session": "ok"}


class _ClientContext:
    def __enter__(self):
        return object()

    def __exit__(self, exc_type, exc, tb) -> None:
        return None


def _sample_snapshot_payload(raw_query: str) -> dict:
    return {
        "input": {"raw": raw_query, "search_term_used": raw_query},
        "selected": {
            "name": "Duolingo",
            "app_id": "123",
            "unified_app_id": 456,
        },
        "unified_app_id": 456,
        "first_release_date_us": "2013-05-29T00:00:00Z",
        "snapshot_window": {
            "start_date": "2026-01-01",
            "end_date": "2026-01-31",
            "comparison_start_date": "2025-12-01",
            "comparison_end_date": "2025-12-31",
        },
        "snapshot": {
            "revenue_usd": 1200.0,
            "revenue_previous_window_usd": 1000.0,
            "revenue_growth_vs_previous_window_percent": 20.0,
            "downloads_absolute": 3400.0,
            "downloads_previous_window_absolute": 3200.0,
            "downloads_growth_vs_previous_window_percent": 6.25,
            "mau_absolute": 5600.0,
            "mau_previous_window_absolute": 5300.0,
            "mau_growth_vs_previous_window_percent": 5.660377358490567,
            "wau_absolute": 2200.0,
            "wau_previous_window_absolute": 2000.0,
            "wau_growth_vs_previous_window_percent": 10.0,
        },
        "market_share_in_window": {
            "share_percent": 12.0,
            "market_revenue_total_proxy_usd": 10000.0,
            "top_apps_limit": 100,
            "category": 6014,
            "category_candidates": [6014],
            "start_date": "2026-01-01",
            "end_date": "2026-01-31",
        },
        "comments": [
            {
                "id": "c1",
                "content": "Very useful app",
                "rating": 5,
                "sentiment": "happy",
                "created_at": "2026-01-15",
            }
        ],
        "versions": [
            {
                "time": "2026-01-20T00:00:00Z",
                "version": "5.200.0",
                "featured_user_feedback": "Stability improvements.",
            }
        ],
        "version_timeline": {
            "country": "US",
            "max_age_days": 30,
            "reference_end_date": "2026-01-31",
            "platform": "ios",
        },
        "warnings": ["using_ios_store_id_from_url"],
    }


def test_snapshot_single_query_outputs_raw_shape(monkeypatch):
    runner = CliRunner()

    monkeypatch.setattr(snapshot_cmd, "get_credential", lambda: _Cred())
    monkeypatch.setattr(snapshot_cmd, "create_st_client", lambda cookies: _ClientContext())
    monkeypatch.setattr(
        snapshot_cmd,
        "run_snapshot_pipeline",
        lambda client, raw_query, **kwargs: PipelineSuccess(payload=_sample_snapshot_payload(raw_query)),
    )

    result = runner.invoke(
        cli,
        [
            "snapshot",
            "https://apps.apple.com/us/app/duolingo/id570060128",
            "--start-date",
            "2026-01-01",
            "--end-date",
            "2026-01-31",
            "--json",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["ok"] is True
    assert payload["data"]["source"]["shape"] == "raw"
    item = payload["data"]["raw"]["items"][0]
    assert item["query"] == "https://apps.apple.com/us/app/duolingo/id570060128"
    assert item["snapshot_window"]["start_date"] == "2026-01-01"
    assert item["snapshot"]["wau_absolute"] == 2200.0
    assert item["snapshot"]["revenue_growth_vs_previous_window_percent"] == 20.0
    assert item["market_share_in_window"]["share_percent"] == 12.0
    assert item["comments"][0]["content"] == "Very useful app"
    assert item["versions"][0]["version"] == "5.200.0"
    assert item["version_timeline"]["reference_end_date"] == "2026-01-31"


def test_snapshot_competitors_file_outputs_both_shapes(monkeypatch, tmp_path: Path):
    runner = CliRunner()
    competitors_file = tmp_path / "competitors.txt"
    competitors_file.write_text(
        "Duolingo\thttps://apps.apple.com/us/app/duolingo/id570060128\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(snapshot_cmd, "get_credential", lambda: _Cred())
    monkeypatch.setattr(snapshot_cmd, "create_st_client", lambda cookies: _ClientContext())
    monkeypatch.setattr(
        snapshot_cmd,
        "run_snapshot_pipeline",
        lambda client, raw_query, **kwargs: PipelineSuccess(payload=_sample_snapshot_payload(raw_query)),
    )

    result = runner.invoke(
        cli,
        [
            "snapshot",
            "--competitors-file",
            str(competitors_file),
            "--start-date",
            "2026-01-01",
            "--end-date",
            "2026-01-31",
            "--shape",
            "both",
            "--json",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["data"]["source"]["shape"] == "both"
    assert payload["data"]["raw"]["items"][0]["snapshot"]["revenue_usd"] == 1200.0
    competitor = payload["data"]["landscape"]["competitors"][0]
    assert competitor["name"] == "Duolingo"
    assert competitor["st"]["snapshot_window"]["end_date"] == "2026-01-31"
    assert competitor["st"]["revenue_growth_vs_previous_window_percent"] == 20.0
    assert competitor["st"]["wau_in_window"]["wau_absolute"] == 2200.0
    assert competitor["st"]["wau_in_window"]["growth_vs_previous_window_percent"] == 10.0
    assert competitor["st"]["market_share_in_window"]["share_percent"] == 12.0
    assert competitor["st"]["reviews_in_window"][0]["content"] == "Very useful app"
    assert competitor["st"]["versions"][0]["version"] == "5.200.0"


def test_snapshot_rejects_end_date_before_start_date():
    runner = CliRunner()

    result = runner.invoke(
        cli,
        [
            "snapshot",
            "Duolingo",
            "--start-date",
            "2026-02-01",
            "--end-date",
            "2026-01-31",
            "--json",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["ok"] is False
    assert payload["error"]["code"] == "bad_request"
    assert "start_date" in payload["error"]["message"]


def test_snapshot_competitors_file_falls_back_to_name_when_url_lookup_fails(
    monkeypatch, tmp_path: Path
):
    runner = CliRunner()
    competitors_file = tmp_path / "competitors.txt"
    competitors_file.write_text(
        "Xero\thttps://apps.apple.com/us/app/xero-accounting/id422067011\n",
        encoding="utf-8",
    )
    seen_queries: list[str] = []
    seen_match_queries: list[str | None] = []

    def fake_run_snapshot_pipeline(client, raw_query, **kwargs):
        del client
        seen_queries.append(raw_query)
        seen_match_queries.append(kwargs.get("match_query"))
        if raw_query.startswith("https://"):
            return PipelineFailure(
                code="not_found",
                message="No apps returned from autocomplete",
                details={"term": raw_query},
            )
        return PipelineSuccess(payload=_sample_snapshot_payload(raw_query))

    monkeypatch.setattr(snapshot_cmd, "get_credential", lambda: _Cred())
    monkeypatch.setattr(snapshot_cmd, "create_st_client", lambda cookies: _ClientContext())
    monkeypatch.setattr(snapshot_cmd, "run_snapshot_pipeline", fake_run_snapshot_pipeline)

    result = runner.invoke(
        cli,
        [
            "snapshot",
            "--competitors-file",
            str(competitors_file),
            "--start-date",
            "2026-01-01",
            "--end-date",
            "2026-01-31",
            "--shape",
            "both",
            "--json",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert seen_queries == [
        "https://apps.apple.com/us/app/xero-accounting/id422067011",
        "Xero",
    ]
    assert seen_match_queries == [None, "https://apps.apple.com/us/app/xero-accounting/id422067011"]
    assert payload["data"]["raw"]["items"][0]["query"] == "Xero"
    competitor = payload["data"]["landscape"]["competitors"][0]
    assert competitor["name"] == "Xero"
    assert competitor["error"] is None
