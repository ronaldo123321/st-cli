"""Tests for the ``st landscape`` command."""

import json
from pathlib import Path

from click.testing import CliRunner

from st_cli.cli import cli
from st_cli.commands import landscape_cmd
from st_cli.pipeline import PipelineSuccess


class _Cred:
    def __init__(self) -> None:
        self.cookies = {"session": "ok"}


class _ClientContext:
    def __enter__(self):
        return object()

    def __exit__(self, exc_type, exc, tb) -> None:
        return None


def test_landscape_passes_region_to_pipeline(monkeypatch, tmp_path: Path):
    runner = CliRunner()
    competitors_file = tmp_path / "competitors.txt"
    competitors_file.write_text(
        "Duolingo\thttps://apps.apple.com/us/app/duolingo/id570060128\n",
        encoding="utf-8",
    )
    seen_kwargs = []

    monkeypatch.setattr(landscape_cmd, "get_credential", lambda: _Cred())
    monkeypatch.setattr(landscape_cmd, "create_st_client", lambda cookies: _ClientContext())
    monkeypatch.setattr(landscape_cmd, "get_csrf_token_for_top_apps_page", lambda client: "csrf-test")
    monkeypatch.setattr(
        landscape_cmd,
        "fetch_version_timeline_for_selected",
        lambda client, selected, reference_end_date, csrf_token, warnings: ([], {"country": "US"}),
    )

    def fake_run_fetch_pipeline(client, raw_query, **kwargs):
        del client
        seen_kwargs.append(kwargs)
        return PipelineSuccess(
            payload={
                "selected": {"name": "Duolingo", "ios_apps": [{"app_id": 570060128}]},
                "first_release_date_us": "2013-05-29T00:00:00Z",
                "revenue": {
                    "monthly_estimates": [
                        {"month": landscape_cmd._previous_month_key(), "revenue_absolute_usd": 1200.0}
                    ]
                },
                "downloads": {
                    "monthly_estimates": [
                        {"month": landscape_cmd._previous_month_key(), "downloads_absolute": 3400.0}
                    ]
                },
                "mau": {
                    "monthly_estimates": [
                        {"month": landscape_cmd._previous_month_key(), "mau_absolute": 5600.0}
                    ]
                },
                "market_share_as_of_last_month": {"share_percent": 12.0, "category": 6014},
                "comments": [],
                "warnings": [],
            }
        )

    monkeypatch.setattr(landscape_cmd, "run_fetch_pipeline", fake_run_fetch_pipeline)

    result = runner.invoke(
        cli,
        [
            "landscape",
            "--competitors-file",
            str(competitors_file),
            "--region",
            "US",
            "--json",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["ok"] is True
    assert payload["data"]["source"]["facet_regions"] == ["US"]
    assert seen_kwargs[0]["facet_regions"] == ["US"]
