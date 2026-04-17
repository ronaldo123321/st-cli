"""st snapshot - arbitrary date-window app snapshots."""

import logging
from datetime import date
from pathlib import Path
from typing import Any

import click

from st_cli.auth import get_credential
from st_cli.constants import CREDENTIAL_FILE, DEFAULT_FACET_REGIONS
from st_cli.output import error_payload, print_payload, success_payload
from st_cli.pipeline import (
    PipelineDisambiguation,
    PipelineFailure,
    PipelineSuccess,
    run_snapshot_pipeline,
)
from st_cli.st_client import create_st_client

logger = logging.getLogger(__name__)


def _normalize_text(v: Any) -> str:
    if v is None:
        return ""
    return str(v).strip()


def _parse_date(value: str) -> date:
    return date.fromisoformat(value.strip())


def _looks_like_url(text: str) -> bool:
    low = text.lower()
    return low.startswith("http://") or low.startswith("https://")


def _parse_competitor_line(line: str) -> tuple[str, str] | None:
    s = line.strip()
    if not s or s.startswith("#"):
        return None
    if "\t" in s:
        name, url = s.split("\t", 1)
    elif "," in s:
        name, url = s.split(",", 1)
    else:
        return None
    name = name.strip()
    url = url.strip()
    if not name or not url:
        return None
    return name, url


def _build_raw_item(query: str, payload: dict[str, Any]) -> dict[str, Any]:
    input_obj = payload.get("input") if isinstance(payload.get("input"), dict) else {}
    resolved_query = input_obj.get("raw") if isinstance(input_obj.get("raw"), str) else query
    return {
        "query": resolved_query,
        "selected": payload.get("selected"),
        "unified_app_id": payload.get("unified_app_id"),
        "first_release_date_us": payload.get("first_release_date_us"),
        "snapshot_window": payload.get("snapshot_window"),
        "snapshot": payload.get("snapshot"),
        "market_share_in_window": payload.get("market_share_in_window"),
        "comments": payload.get("comments", []),
        "versions": payload.get("versions", []),
        "version_timeline": payload.get("version_timeline"),
        "warnings": payload.get("warnings", []),
    }


def _build_landscape_item(name: str, store_url: str | None, payload: dict[str, Any]) -> dict[str, Any]:
    snapshot = payload.get("snapshot", {}) if isinstance(payload.get("snapshot"), dict) else {}
    return {
        "name": name,
        "store_url": store_url,
        "st": {
            "selected": payload.get("selected"),
            "first_release_date_us": payload.get("first_release_date_us"),
            "snapshot_window": payload.get("snapshot_window"),
            "revenue_in_window_usd": snapshot.get("revenue_usd"),
            "revenue_previous_window_usd": snapshot.get("revenue_previous_window_usd"),
            "revenue_growth_vs_previous_window_percent": snapshot.get(
                "revenue_growth_vs_previous_window_percent"
            ),
            "downloads_in_window": {
                "downloads_absolute": snapshot.get("downloads_absolute"),
                "previous_window_downloads_absolute": snapshot.get("downloads_previous_window_absolute"),
                "growth_vs_previous_window_percent": snapshot.get(
                    "downloads_growth_vs_previous_window_percent"
                ),
            },
            "mau_in_window": {
                "mau_absolute": snapshot.get("mau_absolute"),
                "previous_window_mau_absolute": snapshot.get("mau_previous_window_absolute"),
                "growth_vs_previous_window_percent": snapshot.get("mau_growth_vs_previous_window_percent"),
            },
            "wau_in_window": {
                "wau_absolute": snapshot.get("wau_absolute"),
                "previous_window_wau_absolute": snapshot.get("wau_previous_window_absolute"),
                "growth_vs_previous_window_percent": snapshot.get("wau_growth_vs_previous_window_percent"),
            },
            "market_share_in_window": payload.get("market_share_in_window"),
            "reviews_in_window": payload.get("comments", []),
            "versions": payload.get("versions", []),
            "version_timeline": payload.get("version_timeline"),
            "warnings": payload.get("warnings", []),
        },
        "error": None,
    }


def _shape_output(
    *,
    shape: str,
    source: dict[str, Any],
    raw_items: list[dict[str, Any]],
    raw_errors: list[dict[str, Any]],
    landscape_items: list[dict[str, Any]],
) -> dict[str, Any]:
    data: dict[str, Any] = {"source": source}
    if shape in {"raw", "both"}:
        data["raw"] = {"items": raw_items, "errors": raw_errors}
    if shape in {"landscape", "both"}:
        data["landscape"] = {"source": source, "competitors": landscape_items}
    return data


def _run_snapshot_with_fallback(
    *,
    client: Any,
    lookup_query: str,
    display_name: str | None,
    start_date: date,
    end_date: date,
    pick_strategy: str,
    allow_name_fallback: bool,
) -> PipelineSuccess | PipelineDisambiguation | PipelineFailure:
    result = run_snapshot_pipeline(
        client,
        lookup_query,
        start_date=start_date,
        end_date=end_date,
        pick_strategy=pick_strategy,
    )
    if not allow_name_fallback:
        return result
    if not isinstance(result, PipelineFailure):
        return result
    fallback_name = _normalize_text(display_name)
    if not fallback_name or fallback_name == lookup_query:
        return result
    fallback_result = run_snapshot_pipeline(
        client,
        fallback_name,
        start_date=start_date,
        end_date=end_date,
        match_query=lookup_query,
        pick_strategy=pick_strategy,
    )
    return fallback_result


@click.command("snapshot")
@click.argument("query", required=False)
@click.option(
    "--competitors-file",
    "competitors_file",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    required=False,
    default=None,
    help="Competitors file. Each line: name<TAB>store_url (or name, url).",
)
@click.option("--start-date", "start_date_text", required=True, help="Snapshot window start date (YYYY-MM-DD).")
@click.option("--end-date", "end_date_text", required=True, help="Snapshot window end date (YYYY-MM-DD).")
@click.option(
    "--shape",
    "shape",
    type=click.Choice(["raw", "landscape", "both"], case_sensitive=False),
    default="raw",
    show_default=True,
    help="Output shape for raw items, landscape-style competitors, or both.",
)
@click.option(
    "--pick-strategy",
    "pick_strategy",
    type=click.Choice(["heuristic", "first", "fail"], case_sensitive=False),
    default="heuristic",
    show_default=True,
    help="How to resolve multiple autocomplete matches.",
)
@click.option("--json", "as_json", is_flag=True)
@click.option("--yaml", "as_yaml", is_flag=True)
def snapshot(
    query: str | None,
    competitors_file: Path | None,
    start_date_text: str,
    end_date_text: str,
    shape: str,
    pick_strategy: str,
    as_json: bool,
    as_yaml: bool,
) -> None:
    """Fetch one arbitrary-date snapshot for a single app or competitor list."""
    if bool(query) == bool(competitors_file):
        print_payload(
            error_payload(
                "bad_request",
                "Provide exactly one input source: QUERY or --competitors-file.",
                None,
            ),
            as_json=as_json,
            as_yaml=as_yaml,
        )
        raise SystemExit(1)

    try:
        start_date = _parse_date(start_date_text)
        end_date = _parse_date(end_date_text)
    except ValueError:
        print_payload(
            error_payload(
                "bad_request",
                "start_date and end_date must use YYYY-MM-DD.",
                {
                    "start_date": start_date_text,
                    "end_date": end_date_text,
                },
            ),
            as_json=as_json,
            as_yaml=as_yaml,
        )
        raise SystemExit(1)

    if end_date < start_date:
        print_payload(
            error_payload(
                "bad_request",
                "end_date must be on or after start_date.",
                {
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat(),
                },
            ),
            as_json=as_json,
            as_yaml=as_yaml,
        )
        raise SystemExit(1)

    cred = get_credential()
    if not cred or not cred.cookies:
        print_payload(
            error_payload(
                "not_authenticated",
                "No Sensor Tower session. Run: st login",
                {"credential_file": str(CREDENTIAL_FILE)},
            ),
            as_json=as_json,
            as_yaml=as_yaml,
        )
        raise SystemExit(1)

    queries: list[tuple[str, str | None]] = []
    source: dict[str, Any] = {
        "shape": shape,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "facet_regions": DEFAULT_FACET_REGIONS,
    }
    if query:
        queries.append((query, query if _looks_like_url(query) else None))
        source["query"] = query
    else:
        parsed: list[tuple[str, str]] = []
        for line in competitors_file.read_text(encoding="utf-8", errors="ignore").splitlines():
            row = _parse_competitor_line(line)
            if row is not None:
                parsed.append(row)
        if not parsed:
            print_payload(
                error_payload(
                    "bad_request",
                    "Could not parse any competitors from --competitors-file. Expected: name<TAB>store_url per line.",
                    {"competitors_file": str(competitors_file)},
                ),
                as_json=as_json,
                as_yaml=as_yaml,
            )
            raise SystemExit(1)
        queries.extend((url, name) for name, url in parsed)
        source["competitors_file"] = str(competitors_file)

    raw_items: list[dict[str, Any]] = []
    raw_errors: list[dict[str, Any]] = []
    landscape_items: list[dict[str, Any]] = []

    try:
        with create_st_client(cred.cookies) as client:
            for lookup_query, display_name in queries:
                result = _run_snapshot_with_fallback(
                    client=client,
                    lookup_query=lookup_query,
                    display_name=display_name,
                    start_date=start_date,
                    end_date=end_date,
                    pick_strategy=pick_strategy,
                    allow_name_fallback=competitors_file is not None,
                )
                if isinstance(result, PipelineFailure):
                    if query:
                        print_payload(
                            error_payload(result.code, result.message, result.details),
                            as_json=as_json,
                            as_yaml=as_yaml,
                        )
                        raise SystemExit(1)
                    raw_errors.append(
                        {
                            "query": lookup_query,
                            "name": display_name,
                            "code": result.code,
                            "message": result.message,
                            "details": result.details,
                        }
                    )
                    if shape in {"landscape", "both"}:
                        landscape_items.append(
                            {
                                "name": display_name or lookup_query,
                                "store_url": lookup_query if _looks_like_url(lookup_query) else None,
                                "st": None,
                                "error": {
                                    "code": result.code,
                                    "message": result.message,
                                    "details": result.details,
                                },
                            }
                        )
                    continue
                if isinstance(result, PipelineDisambiguation):
                    details = {
                        "candidates": result.candidates,
                        "warnings": result.warnings,
                        "search_term_used": result.search_term,
                        "input": {"raw": result.raw_query},
                    }
                    if query:
                        print_payload(
                            error_payload(
                                "needs_disambiguation",
                                "Multiple autocomplete matches; refine query or add --pick support.",
                                details,
                            ),
                            as_json=as_json,
                            as_yaml=as_yaml,
                        )
                        raise SystemExit(1)
                    raw_errors.append(
                        {
                            "query": lookup_query,
                            "name": display_name,
                            "code": "needs_disambiguation",
                            "message": "Multiple autocomplete matches; refine query.",
                            "details": details,
                        }
                    )
                    if shape in {"landscape", "both"}:
                        landscape_items.append(
                            {
                                "name": display_name or lookup_query,
                                "store_url": lookup_query if _looks_like_url(lookup_query) else None,
                                "st": None,
                                "error": {
                                    "code": "needs_disambiguation",
                                    "message": "Multiple autocomplete matches; refine query.",
                                    "details": details,
                                },
                            }
                        )
                    continue

                assert isinstance(result, PipelineSuccess)
                payload = result.payload
                raw_items.append(_build_raw_item(lookup_query, payload))
                if shape in {"landscape", "both"}:
                    selected = payload.get("selected")
                    resolved_name = display_name
                    if not resolved_name and isinstance(selected, dict):
                        resolved_name = _normalize_text(selected.get("name")) or lookup_query
                    landscape_items.append(
                        _build_landscape_item(
                            resolved_name or lookup_query,
                            lookup_query if _looks_like_url(lookup_query) else None,
                            payload,
                        )
                    )
    except RuntimeError as exc:
        logger.exception("snapshot failed")
        print_payload(error_payload("upstream_error", str(exc)), as_json=as_json, as_yaml=as_yaml)
        raise SystemExit(1) from None

    print_payload(
        success_payload(
            _shape_output(
                shape=shape,
                source=source,
                raw_items=raw_items,
                raw_errors=raw_errors,
                landscape_items=landscape_items,
            )
        ),
        as_json=as_json,
        as_yaml=as_yaml,
    )
