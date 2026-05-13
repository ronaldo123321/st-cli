"""Tests for ``st aso-keywords``."""

import json

from click.testing import CliRunner
import httpx

from st_cli.cli import cli
from st_cli.commands import aso_keywords_cmd
from st_cli.st_api import download_aso_keywords_csv, lookup_aso_keywords


class _Cred:
    def __init__(self) -> None:
        self.cookies = {"session": "ok"}


class _ClientContext:
    def __enter__(self):
        return object()

    def __exit__(self, exc_type, exc, tb) -> None:
        return None


def test_aso_keywords_add_calls_lookup(monkeypatch):
    runner = CliRunner()
    calls = []

    monkeypatch.setattr(aso_keywords_cmd, "get_credential", lambda: _Cred())
    monkeypatch.setattr(aso_keywords_cmd, "create_st_client", lambda cookies: _ClientContext())
    monkeypatch.setattr(aso_keywords_cmd, "_csrf_token", lambda client: "csrf-test")

    def fake_lookup(client, *, os, user_app_id, country, terms, csrf_token):
        calls.append(
            {
                "os": os,
                "user_app_id": user_app_id,
                "country": country,
                "terms": terms,
                "csrf_token": csrf_token,
            }
        )
        return {"keywords": terms}

    monkeypatch.setattr(aso_keywords_cmd, "lookup_aso_keywords", fake_lookup)

    result = runner.invoke(
        cli,
        [
            "aso-keywords",
            "add",
            "--user-app-id",
            "view-1",
            "--country",
            "US",
            "--keyword",
            "bible note, sermon notes",
            "--keyword",
            "bible note",
            "--json",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["ok"] is True
    assert payload["data"]["terms"] == ["bible note", "sermon notes"]
    assert calls == [
        {
            "os": "ios",
            "user_app_id": "view-1",
            "country": "US",
            "terms": ["bible note", "sermon notes"],
            "csrf_token": "csrf-test",
        }
    ]


def test_aso_keywords_export_writes_csv(monkeypatch, tmp_path):
    runner = CliRunner()
    out_file = tmp_path / "aso.csv"

    monkeypatch.setattr(aso_keywords_cmd, "get_credential", lambda: _Cred())
    monkeypatch.setattr(aso_keywords_cmd, "create_st_client", lambda cookies: _ClientContext())
    monkeypatch.setattr(aso_keywords_cmd, "_csrf_token", lambda client: "csrf-test")
    monkeypatch.setattr(aso_keywords_cmd, "download_aso_keywords_csv", lambda *args, **kwargs: b"Keyword,Rank\nx,1\n")

    result = runner.invoke(
        cli,
        [
            "aso-keywords",
            "export",
            "--keyword-view-id",
            "view-1",
            "--start",
            "2026-02-12",
            "--end",
            "2026-05-12",
            "--comparison-start",
            "2025-11-14",
            "--comparison-end",
            "2026-02-11",
            "--out",
            str(out_file),
            "--json",
        ],
    )

    assert result.exit_code == 0
    assert out_file.read_text() == "Keyword,Rank\nx,1\n"
    payload = json.loads(result.output)
    assert payload["data"]["out_file"] == str(out_file)
    assert payload["data"]["bytes"] == 17


def test_aso_keywords_export_resolves_app_id(monkeypatch, tmp_path):
    runner = CliRunner()
    out_file = tmp_path / "aso.csv"
    calls = []

    monkeypatch.setattr(aso_keywords_cmd, "get_credential", lambda: _Cred())
    monkeypatch.setattr(aso_keywords_cmd, "create_st_client", lambda cookies: _ClientContext())
    monkeypatch.setattr(aso_keywords_cmd, "_csrf_token", lambda client: "csrf-test")
    monkeypatch.setattr(
        aso_keywords_cmd,
        "get_app_intel_user_apps",
        lambda client, csrf_token=None: {
            "user_apps": [
                {
                    "id": "view-1",
                    "appId": 6477533581,
                    "appName": "AI Remodel — Interior Design",
                    "os": "ios",
                    "country": "US",
                    "numTerms": 85,
                }
            ]
        },
    )

    def fake_download(client, **kwargs):
        calls.append(kwargs)
        return b"Keyword,Rank\nx,1\n"

    monkeypatch.setattr(aso_keywords_cmd, "download_aso_keywords_csv", fake_download)

    result = runner.invoke(
        cli,
        [
            "aso-keywords",
            "export",
            "--app-id",
            "6477533581",
            "--start",
            "2026-02-12",
            "--end",
            "2026-05-12",
            "--out",
            str(out_file),
            "--json",
        ],
    )

    assert result.exit_code == 0
    assert calls[0]["keyword_view_id"] == "view-1"
    assert calls[0]["comparison_start_date"] == "2025-11-14"
    assert calls[0]["comparison_end_date"] == "2026-02-11"
    payload = json.loads(result.output)
    assert payload["data"]["keyword_view_id"] == "view-1"
    assert payload["data"]["matched_app"] == {
        "id": "view-1",
        "appId": 6477533581,
        "appName": "AI Remodel — Interior Design",
        "os": "ios",
        "country": "US",
        "numTerms": 85,
    }


def test_resolve_user_app_id_from_app_name():
    rows = [
        {
            "id": "view-1",
            "appId": 6477533581,
            "appName": "AI Remodel — Interior Design",
            "os": "ios",
            "country": "US",
        }
    ]

    resolved, matched = aso_keywords_cmd._resolve_user_app_id_from_rows(
        rows,
        user_app_id=None,
        app_id=None,
        app_name="ai remodel",
        os_name="ios",
        country="US",
    )

    assert resolved == "view-1"
    assert matched == rows[0]


def test_lookup_aso_keywords_posts_expected_body():
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        seen["headers"] = dict(request.headers)
        seen["body"] = json.loads(request.content.decode())
        return httpx.Response(200, json={"ok": True})

    client = httpx.Client(base_url="https://app.sensortower.com", transport=httpx.MockTransport(handler))

    data = lookup_aso_keywords(
        client,
        os="ios",
        user_app_id="view-1",
        country="us",
        terms=[" bible ", ""],
        csrf_token="csrf-test",
    )

    assert data == {"ok": True}
    assert seen["url"] == "https://app.sensortower.com/api/ios/keywords/lookup"
    assert seen["headers"]["x-csrf-token"] == "csrf-test"
    assert seen["body"] == {"user_app_id": "view-1", "country": "US", "terms": [" bible "]}


def test_download_aso_keywords_csv_posts_expected_body():
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        seen["headers"] = dict(request.headers)
        seen["body"] = json.loads(request.content.decode())
        return httpx.Response(200, content=b"Keyword,Rank\n")

    client = httpx.Client(base_url="https://app.sensortower.com", transport=httpx.MockTransport(handler))

    data = download_aso_keywords_csv(
        client,
        keyword_view_id="view-1",
        os="ios",
        country="us",
        device="iphone",
        start_date="2026-02-12",
        end_date="2026-05-12",
        comparison_start_date="2025-11-14",
        comparison_end_date="2026-02-11",
        csrf_token="csrf-test",
    )

    assert data == b"Keyword,Rank\n"
    assert seen["url"] == "https://app.sensortower.com/api/v2/apps/facets.csv"
    assert seen["headers"]["x-csrf-token"] == "csrf-test"
    assert seen["body"]["filters"]["keyword_view_id"] == "view-1"
    assert seen["body"]["filters"]["regions"] == ["US"]
    assert seen["body"]["filters"]["devices"] == ["iphone"]
    assert seen["body"]["filters"]["keywords"]["in"]["filters"]["start_date"] == "2026-05-05"
    assert seen["body"]["facets"][0] == {"facet": "keyword", "alias": "Keyword"}
