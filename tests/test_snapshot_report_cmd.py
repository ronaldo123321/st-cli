"""Tests for the ``st snapshot-report`` command."""

import json
from pathlib import Path

from click.testing import CliRunner

from st_cli.cli import cli


def _snapshot_env(shape: str = "raw") -> dict:
    data: dict = {
        "source": {
            "shape": shape,
            "start_date": "2026-03-09",
            "end_date": "2026-03-22",
            "facet_regions": ["US"],
        }
    }
    if shape in {"raw", "both"}:
        data["raw"] = {
            "items": [
                {
                    "query": "QuickBooks",
                    "selected": {"name": "QuickBooks"},
                    "first_release_date_us": "2013/02/14",
                    "snapshot_window": {
                        "start_date": "2026-03-09",
                        "end_date": "2026-03-22",
                        "comparison_start_date": "2026-02-23",
                        "comparison_end_date": "2026-03-08",
                    },
                    "snapshot": {
                        "revenue_usd": 1058329.34,
                        "revenue_growth_vs_previous_window_percent": -15.206651,
                        "downloads_absolute": 37690.0,
                        "downloads_growth_vs_previous_window_percent": -5.612181,
                        "mau_absolute": None,
                        "wau_absolute": 519435.0,
                        "wau_growth_vs_previous_window_percent": 15.400509,
                    },
                    "market_share_in_window": {
                        "share_percent": 2.342059,
                        "market_revenue_total_proxy_usd": 45187996.2,
                        "top_apps_limit": 100,
                    },
                    "comments": [
                        {
                            "content": "Been using QuickBooks for 10+ years.",
                            "rating": 1,
                            "created_at": "2026-03-15",
                        }
                    ],
                    "versions": [
                        {
                            "time": "2026-03-10T00:00:00Z",
                            "version": "24.3.1",
                            "featured_user_feedback": "Performance fixes.",
                        }
                    ],
                    "version_timeline": {
                        "country": "US",
                        "max_age_days": 30,
                        "reference_end_date": "2026-03-22",
                        "platform": "ios",
                    },
                    "warnings": [],
                }
            ],
            "errors": [
                {
                    "query": "Wave",
                    "name": "Wave",
                    "code": "needs_disambiguation",
                    "message": "Multiple autocomplete matches; refine query.",
                    "details": {},
                }
            ],
        }
    if shape in {"landscape", "both"}:
        data["landscape"] = {
            "source": data["source"],
            "competitors": [
                {
                    "name": "QuickBooks",
                    "store_url": "https://apps.apple.com/us/app/quickbooks-accounting/id584606479",
                    "st": {
                        "first_release_date_us": "2013/02/14",
                        "snapshot_window": {
                            "start_date": "2026-03-09",
                            "end_date": "2026-03-22",
                        },
                        "revenue_in_window_usd": 1058329.34,
                        "revenue_growth_vs_previous_window_percent": -15.206651,
                        "downloads_in_window": {
                            "downloads_absolute": 37690.0,
                            "growth_vs_previous_window_percent": -5.612181,
                        },
                        "mau_in_window": {"mau_absolute": None, "growth_vs_previous_window_percent": None},
                        "wau_in_window": {
                            "wau_absolute": 519435.0,
                            "growth_vs_previous_window_percent": 15.400509,
                        },
                        "market_share_in_window": {
                            "share_percent": 2.342059,
                            "market_revenue_total_proxy_usd": 45187996.2,
                            "top_apps_limit": 100,
                        },
                        "reviews_in_window": [
                            {
                                "content": "Been using QuickBooks for 10+ years.",
                                "rating": 1,
                                "created_at": "2026-03-15",
                            }
                        ],
                        "versions": [
                            {
                                "time": "2026-03-10T00:00:00Z",
                                "version": "24.3.1",
                                "featured_user_feedback": "Performance fixes.",
                            }
                        ],
                        "version_timeline": {
                            "country": "US",
                            "max_age_days": 30,
                            "reference_end_date": "2026-03-22",
                            "platform": "ios",
                        },
                        "warnings": [],
                    },
                    "error": None,
                },
                {
                    "name": "Wave",
                    "store_url": "https://apps.apple.com/us/app/wave-accounting/id449637421",
                    "st": None,
                    "error": {
                        "code": "needs_disambiguation",
                        "message": "Multiple autocomplete matches; refine query.",
                        "details": {},
                    },
                },
            ],
        }
    return {"ok": True, "schema_version": "1", "data": data}


def test_snapshot_report_renders_markdown_from_snapshot_json(tmp_path: Path):
    runner = CliRunner()
    in_path = tmp_path / "snapshot.json"
    out_path = tmp_path / "snapshot.md"
    in_path.write_text(json.dumps(_snapshot_env("both")), encoding="utf-8")

    result = runner.invoke(
        cli,
        [
            "snapshot-report",
            "--in",
            str(in_path),
            "--out",
            str(out_path),
            "--json",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["ok"] is True
    assert payload["data"]["report"]["path"] == str(out_path)

    report_md = out_path.read_text(encoding="utf-8")
    assert "# Snapshot Summary" in report_md
    assert "2026-03-09" in report_md
    assert "QuickBooks" in report_md
    assert "$1.06M" in report_md
    assert "2.34%" in report_md
    assert "Been using QuickBooks for 10+ years." in report_md
    assert "## Recent version updates" in report_md
    assert "24.3.1" in report_md
    assert "Performance fixes." in report_md
    assert "Wave" in report_md
    assert "needs_disambiguation" in report_md


def test_snapshot_report_rejects_non_snapshot_envelope(tmp_path: Path):
    runner = CliRunner()
    in_path = tmp_path / "bad.json"
    out_path = tmp_path / "snapshot.md"
    in_path.write_text(json.dumps({"ok": True, "schema_version": "1", "data": {}}), encoding="utf-8")

    result = runner.invoke(
        cli,
        [
            "snapshot-report",
            "--in",
            str(in_path),
            "--out",
            str(out_path),
            "--json",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["ok"] is False
    assert payload["error"]["code"] == "bad_request"
    assert "snapshot" in payload["error"]["message"].lower()
