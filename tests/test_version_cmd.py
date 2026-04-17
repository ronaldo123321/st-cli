"""Tests for ``st version``."""

import json
import pytest
from datetime import datetime, timedelta, timezone

from click.testing import CliRunner

from st_cli.cli import cli
from st_cli.commands import version_cmd
from st_cli.st_api import filter_timeline_entries_within_days


class _Cred:
    def __init__(self) -> None:
        self.cookies = {"session": "ok"}


class _ClientContext:
    def __enter__(self):
        return object()

    def __exit__(self, exc_type, exc, tb) -> None:
        return None


def test_version_ios_numeric_skips_autocomplete(monkeypatch):
    runner = CliRunner()
    monkeypatch.setattr(version_cmd, "get_credential", lambda: _Cred())
    monkeypatch.setattr(version_cmd, "create_st_client", lambda cookies: _ClientContext())
    monkeypatch.setattr(version_cmd, "get_csrf_token_for_top_apps_page", lambda client: "csrf-test")
    calls: list[tuple[str, str]] = []

    ref = datetime(2026, 4, 17, 12, 0, 0, tzinfo=timezone.utc)
    recent = (ref - timedelta(days=10)).strftime("%Y-%m-%dT00:00:00Z")

    def fake_ios(client, *, app_id, country, csrf_token):
        calls.append((str(app_id), country))
        return {
            "update_data": [
                [
                    "2011-06-05T00:00:00Z",
                    {
                        "version": {"before": "1.8", "after": "1.8.5", "version_summary": ""},
                        "featured_user_feedback": None,
                    },
                ],
                [
                    recent,
                    {
                        "version": {
                            "before": "276.1",
                            "after": "277.0",
                            "version_summary": "Bug fixes.",
                        },
                        "featured_user_feedback": None,
                    },
                ],
            ]
        }

    monkeypatch.setattr(version_cmd, "get_ios_app_update_history", fake_ios)
    def _fixed_ref(slim, days=30, reference=None):
        return filter_timeline_entries_within_days(slim, days=days, reference=ref)

    monkeypatch.setattr(version_cmd, "filter_timeline_entries_within_days", _fixed_ref)

    monkeypatch.setattr(version_cmd, "autocomplete_search", lambda *a, **k: (_ for _ in ()).throw(AssertionError("no autocomplete")))

    result = runner.invoke(cli, ["version", "389801252", "--json"])
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert data["ok"] is True
    assert data["data"]["platform"] == "ios"
    assert data["data"]["app_id"] == "389801252"
    assert data["data"]["max_age_days"] == 30
    assert len(data["data"]["versions"]) == 1
    assert data["data"]["versions"][0]["time"] == recent
    assert data["data"]["versions"][0]["version"]["after"] == "277.0"
    assert calls == [("389801252", "US")]


def test_version_needs_disambiguation(monkeypatch):
    runner = CliRunner()
    monkeypatch.setattr(version_cmd, "get_credential", lambda: _Cred())
    monkeypatch.setattr(version_cmd, "create_st_client", lambda cookies: _ClientContext())
    monkeypatch.setattr(version_cmd, "get_csrf_token_for_top_apps_page", lambda client: None)

    monkeypatch.setattr(version_cmd, "autocomplete_search", lambda client, term, limit=20: [{"name": "A"}, {"name": "B"}])
    monkeypatch.setattr(
        version_cmd,
        "_choose_candidate_heuristic",
        lambda raw_query, candidates, warnings: None,
    )

    def boom(*a, **k):
        raise AssertionError("history should not be called")

    monkeypatch.setattr(version_cmd, "get_ios_app_update_history", boom)
    monkeypatch.setattr(version_cmd, "get_android_app_update_history", boom)

    result = runner.invoke(cli, ["version", "vague name", "--pick-strategy", "fail", "--json"])
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert data["data"]["needs_disambiguation"] is True
    assert data["data"]["comments"] == []


def test_version_play_url_uses_android_fast_path(monkeypatch):
    runner = CliRunner()
    monkeypatch.setattr(version_cmd, "get_credential", lambda: _Cred())
    monkeypatch.setattr(version_cmd, "create_st_client", lambda cookies: _ClientContext())
    monkeypatch.setattr(version_cmd, "get_csrf_token_for_top_apps_page", lambda client: None)
    calls: list[str] = []

    def fake_android(client, *, app_id, country, csrf_token):
        calls.append(str(app_id))
        return {"update_data": []}

    monkeypatch.setattr(version_cmd, "get_android_app_update_history", fake_android)
    monkeypatch.setattr(version_cmd, "autocomplete_search", lambda *a, **k: (_ for _ in ()).throw(AssertionError("no autocomplete")))

    url = "https://play.google.com/store/apps/details?id=com.instagram.android"
    result = runner.invoke(cli, ["version", url, "--json"])
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert data["data"]["platform"] == "android"
    assert data["data"]["app_id"] == "com.instagram.android"
    assert data["data"]["versions"] == []
    assert calls == ["com.instagram.android"]


def test_slim_app_update_timeline_nested_update_history():
    from st_cli.st_api import slim_app_update_timeline_entries

    raw = {
        "update_history": {
            "update_data": [
                [
                    "2024-01-01T00:00:00Z",
                    {
                        "version": None,
                        "featured_user_feedback": {"rating": 5},
                        "description": "strip",
                    },
                ]
            ]
        }
    }
    assert slim_app_update_timeline_entries(raw) == [
        {
            "time": "2024-01-01T00:00:00Z",
            "version": None,
            "featured_user_feedback": {"rating": 5},
        }
    ]


def test_filter_timeline_entries_within_days():
    ref = datetime(2026, 4, 17, tzinfo=timezone.utc)
    entries = [
        {"time": "2011-06-05T00:00:00Z", "version": {}, "featured_user_feedback": None},
        {"time": "2026-01-01T00:00:00Z", "version": {}, "featured_user_feedback": None},
    ]
    got = filter_timeline_entries_within_days(entries, days=365, reference=ref)
    assert len(got) == 1
    assert got[0]["time"] == "2026-01-01T00:00:00Z"


def test_filter_timeline_entries_within_days_rejects_negative():
    with pytest.raises(ValueError, match="non-negative"):
        filter_timeline_entries_within_days([], days=-1)


def test_version_rejects_negative_max_age_days(monkeypatch):
    runner = CliRunner()
    monkeypatch.setattr(version_cmd, "get_credential", lambda: _Cred())
    result = runner.invoke(cli, ["version", "1", "--max-age-days", "-1", "--json"])
    assert result.exit_code == 1
    data = json.loads(result.output)
    assert data["ok"] is False
