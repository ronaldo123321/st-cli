"""Tests for search term preparation (no browser)."""

import httpx
from datetime import date

from st_cli.pipeline import PipelineSuccess, prepare_search_term, run_snapshot_pipeline


def test_prepare_plain_name():
    term, w = prepare_search_term("  Duolingo  ")
    assert term == "Duolingo"
    assert w == []


def test_prepare_ios_store_url_uses_slug_for_autocomplete():
    term, w = prepare_search_term("https://apps.apple.com/us/app/duolingo/id570060128")
    assert term == "570060128"
    assert "using_ios_store_id_from_url" in w


def test_prepare_play_store_url():
    term, w = prepare_search_term(
        "https://play.google.com/store/apps/details?id=com.duolingo"
    )
    assert term == "com.duolingo"
    assert "using_android_package_from_url" in w


def test_snapshot_pipeline_falls_back_to_ios_slug_when_id_returns_no_candidates(monkeypatch):
    seen_terms: list[str] = []

    def fake_autocomplete_search(client: httpx.Client, term: str, limit: int = 20):
        del client, limit
        seen_terms.append(term)
        if term == "422067011":
            return []
        if term == "xero-accounting":
            return [
                {
                    "name": "Xero Accounting",
                    "ios_apps": [{"app_id": 422067011}],
                    "android_apps": [],
                }
            ]
        return []

    monkeypatch.setattr("st_cli.pipeline.autocomplete_search", fake_autocomplete_search)
    monkeypatch.setattr("st_cli.pipeline.get_csrf_token_for_top_apps_page", lambda client: None)
    monkeypatch.setattr(
        "st_cli.pipeline.apps_facets_v2_month_slice",
        lambda client, app_ids, start_date, end_date, comparison_start, comparison_end, csrf_token=None: [
            {
                "appId": None,
                "revenueAbsolute": 123400,
                "downloadsAbsolute": 22,
                "activeUsersWAUAbsolute": 33,
                "releaseDate": "2020/01/01",
                "unifiedAppId": 99,
            }
        ],
    )
    monkeypatch.setattr(
        "st_cli.pipeline.get_app_comments",
        lambda client, ios_app_id, android_app_id, start_date, end_date, limit=20, csrf_token=None: [],
    )

    with httpx.Client(base_url="https://example.com") as client:
        result = run_snapshot_pipeline(
            client,
            "https://apps.apple.com/us/app/xero-accounting/id422067011",
            start_date=__import__("datetime").date(2026, 3, 9),
            end_date=__import__("datetime").date(2026, 3, 22),
        )

    assert isinstance(result, PipelineSuccess)
    assert seen_terms == ["422067011", "xero-accounting"]
    assert result.payload["snapshot"]["revenue_usd"] == 1234.0
    assert "using_ios_slug_from_url" in result.payload["warnings"]


def test_snapshot_pipeline_uses_url_slug_for_heuristic_disambiguation(monkeypatch):
    def fake_autocomplete_search(client: httpx.Client, term: str, limit: int = 20):
        del client, limit
        assert term == "Xero"
        return [
            {
                "name": "Xero Accounting for business",
                "humanized_name": "Xero Accounting",
                "publisher_name": "Xero",
                "ios_apps": [{"app_id": 441880705}],
                "android_apps": [{"app_id": "com.xero.touch"}],
                "active": True,
            },
            {
                "name": "Xero Verify",
                "humanized_name": "Xero Verify",
                "publisher_name": "Xero",
                "ios_apps": [{"app_id": 1510862201}],
                "android_apps": [{"app_id": "com.xero.authenticator"}],
                "active": True,
            },
        ]

    monkeypatch.setattr("st_cli.pipeline.autocomplete_search", fake_autocomplete_search)
    monkeypatch.setattr("st_cli.pipeline.get_csrf_token_for_top_apps_page", lambda client: None)
    monkeypatch.setattr(
        "st_cli.pipeline.apps_facets_v2_month_slice",
        lambda client, app_ids, start_date, end_date, comparison_start, comparison_end, csrf_token=None: [
            {
                "appId": None,
                "revenueAbsolute": 123400,
                "downloadsAbsolute": 22,
                "activeUsersWAUAbsolute": 33,
                "releaseDate": "2020/01/01",
                "unifiedAppId": 99,
            }
        ],
    )
    monkeypatch.setattr(
        "st_cli.pipeline.get_app_comments",
        lambda client, ios_app_id, android_app_id, start_date, end_date, limit=20, csrf_token=None: [],
    )

    with httpx.Client(base_url="https://example.com") as client:
        result = run_snapshot_pipeline(
            client,
            "Xero",
            start_date=date(2026, 3, 9),
            end_date=date(2026, 3, 22),
            match_query="https://apps.apple.com/us/app/xero-accounting/id422067011",
        )

    assert isinstance(result, PipelineSuccess)
    assert result.payload["selected"]["humanized_name"] == "Xero Accounting"


def test_snapshot_pipeline_includes_growth_and_market_share(monkeypatch):
    def fake_autocomplete_search(client: httpx.Client, term: str, limit: int = 20):
        del client, limit
        assert term == "Duolingo"
        return [
            {
                "name": "Duolingo",
                "publisher_name": "Duolingo",
                "ios_apps": [{"app_id": 570060128}],
                "android_apps": [{"app_id": "com.duolingo"}],
                "categories": [{"id": 6014}],
                "active": True,
            }
        ]

    def fake_apps_facets_v2_month_slice(
        client,
        app_ids,
        start_date,
        end_date,
        comparison_start,
        comparison_end,
        csrf_token=None,
    ):
        del client, start_date, end_date, comparison_start, comparison_end, csrf_token
        if app_ids == [570060128]:
            return [
                {
                    "appId": None,
                    "revenueAbsolute": 123400,
                    "revenueAbsolutePrevious": 100000,
                    "revenueGrowthPercent": 23.4,
                    "downloadsAbsolute": 220.0,
                    "downloadsAbsolutePrevious": 200.0,
                    "downloadsGrowthPercent": 10.0,
                    "activeUsersMAUAbsolute": 550.0,
                    "activeUsersMAUAbsolutePrevious": 500.0,
                    "activeUsersWAUAbsolute": 330.0,
                    "activeUsersWAUAbsolutePrevious": 300.0,
                    "activeUsersWAUGrowthPercent": 10.0,
                    "releaseDate": "2020/01/01",
                    "unifiedAppId": 99,
                }
            ]
        assert app_ids == [111, 222]
        return [{"appId": None, "revenueAbsolute": 1000000}]

    monkeypatch.setattr("st_cli.pipeline.autocomplete_search", fake_autocomplete_search)
    monkeypatch.setattr("st_cli.pipeline.get_csrf_token_for_top_apps_page", lambda client: None)
    monkeypatch.setattr("st_cli.pipeline.apps_facets_v2_month_slice", fake_apps_facets_v2_month_slice)
    monkeypatch.setattr(
        "st_cli.pipeline.get_app_comments",
        lambda client, ios_app_id, android_app_id, start_date, end_date, limit=20, csrf_token=None: [],
    )
    monkeypatch.setattr(
        "st_cli.pipeline.top_sub_app_ids",
        lambda client, measure, start_date, end_date, comparison_attribute, category, regions, limit, csrf_token=None: [
            111,
            222,
        ],
    )

    with httpx.Client(base_url="https://example.com") as client:
        result = run_snapshot_pipeline(
            client,
            "Duolingo",
            start_date=date(2026, 3, 9),
            end_date=date(2026, 3, 22),
        )

    assert isinstance(result, PipelineSuccess)
    snapshot = result.payload["snapshot"]
    assert snapshot["revenue_previous_window_usd"] == 1000.0
    assert snapshot["revenue_growth_vs_previous_window_percent"] == 23.4
    assert snapshot["downloads_growth_vs_previous_window_percent"] == 10.0
    assert snapshot["mau_previous_window_absolute"] == 500.0
    assert snapshot["wau_growth_vs_previous_window_percent"] == 10.0
    market_share = result.payload["market_share_in_window"]
    assert market_share["share_percent"] == 12.34
    assert market_share["market_revenue_total_proxy_usd"] == 10000.0
    assert market_share["category"] == 6014
