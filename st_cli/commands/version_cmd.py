"""st version — Sensor Tower app update / version timeline (get_app_update_history)."""

import logging
import re
from typing import Any

import click
import httpx

from st_cli.auth import get_credential
from st_cli.constants import CREDENTIAL_FILE
from st_cli.output import error_payload, print_payload, success_payload
from st_cli.pipeline import (
    PipelineDisambiguation,
    PipelineFailure,
    _choose_candidate_heuristic,
    autocomplete_search,
    prepare_match_query,
    prepare_search_term,
)
from st_cli.st_api import (
    extract_store_hints,
    filter_timeline_entries_within_days,
    get_android_app_update_history,
    get_csrf_token_for_top_apps_page,
    get_ios_app_update_history,
    slim_app_update_timeline_entries,
)
from st_cli.st_client import create_st_client

logger = logging.getLogger(__name__)

_ANDROID_PACKAGE_RE = re.compile(
    r"^[a-z][a-z0-9_]*(\.[a-z0-9_]+)+$",
    re.IGNORECASE,
)


def _normalize_platform(value: str) -> str:
    v = (value or "auto").strip().lower()
    if v not in ("auto", "ios", "android"):
        raise click.BadParameter("platform must be auto, ios, or android")
    return v


def _looks_like_android_package(text: str) -> bool:
    s = text.strip()
    return bool(s and _ANDROID_PACKAGE_RE.match(s))


def _fast_path_platform_and_app_id(query: str, platform: str) -> tuple[str, str] | None:
    """Return ``(ios|android, app_id)`` when autocomplete is unnecessary."""
    plat = _normalize_platform(platform)
    q = query.strip()
    hints = extract_store_hints(q)

    if hints.get("ios_numeric_id") and plat != "android":
        return "ios", str(hints["ios_numeric_id"])
    if hints.get("android_package") and plat != "ios":
        return "android", str(hints["android_package"])

    if q.isdigit() and plat != "android":
        return "ios", q
    if plat in {"android", "auto"} and _looks_like_android_package(q) and plat != "ios":
        return "android", q
    return None


def _ios_app_id_from_candidate(chosen: dict[str, Any]) -> str | None:
    ios_apps = chosen.get("ios_apps")
    if not isinstance(ios_apps, list) or not ios_apps:
        return None
    first = ios_apps[0]
    if not isinstance(first, dict):
        return None
    aid = first.get("app_id")
    if aid is None:
        return None
    s = str(aid).strip()
    return s if s else None


def _android_app_id_from_candidate(chosen: dict[str, Any]) -> str | None:
    android_apps = chosen.get("android_apps")
    if not isinstance(android_apps, list) or not android_apps:
        return None
    first = android_apps[0]
    if not isinstance(first, dict):
        return None
    aid = first.get("app_id")
    if aid is None:
        return None
    s = str(aid).strip()
    return s if s else None


def _resolve_via_autocomplete(
    client: httpx.Client,
    raw_query: str,
    *,
    pick_1based: int | None,
    pick_strategy: str,
    platform: str,
) -> PipelineFailure | PipelineDisambiguation | tuple[dict[str, Any], str, str, list[str]]:
    """Return ``(chosen, resolved_platform, app_id, warnings)`` or failure/disambiguation."""
    plat = _normalize_platform(platform)
    search_term, warnings = prepare_search_term(raw_query)
    candidates = autocomplete_search(client, search_term, limit=20)
    if not candidates:
        return PipelineFailure(
            "not_found",
            "No apps returned from autocomplete",
            {"term": search_term},
        )

    score_query = prepare_match_query(raw_query)
    idx: int | None = None
    if pick_1based is not None:
        idx = pick_1based - 1
    else:
        strategy = (pick_strategy or "heuristic").strip().lower()
        if len(candidates) <= 1:
            idx = 0
        elif strategy == "first":
            idx = 0
            warnings = [*warnings, "pick_strategy=first"]
        elif strategy == "heuristic":
            idx = _choose_candidate_heuristic(raw_query=score_query, candidates=candidates, warnings=warnings)
        elif strategy == "fail":
            idx = None
            warnings = [*warnings, "pick_strategy=fail"]
        else:
            idx = _choose_candidate_heuristic(raw_query=score_query, candidates=candidates, warnings=warnings)

    if idx is None:
        return PipelineDisambiguation(
            candidates=candidates,
            warnings=[*warnings, "needs_disambiguation:true"],
            search_term=search_term,
            raw_query=raw_query,
        )

    if idx < 0 or idx >= len(candidates):
        return PipelineFailure(
            "bad_request",
            f"--pick out of range (1-{len(candidates)})",
            None,
        )

    chosen = candidates[idx]
    ios_id = _ios_app_id_from_candidate(chosen)
    android_id = _android_app_id_from_candidate(chosen)

    resolved_plat: str | None = None
    app_id: str | None = None
    if plat == "ios":
        if ios_id:
            resolved_plat, app_id = "ios", ios_id
        else:
            return PipelineFailure(
                "upstream_error",
                "Selected app has no iOS App Store id; try another match or use an App Store URL.",
                chosen,
            )
    elif plat == "android":
        if android_id:
            resolved_plat, app_id = "android", android_id
        else:
            return PipelineFailure(
                "upstream_error",
                "Selected app has no Android app id; try another match or use a Play Store URL.",
                chosen,
            )
    else:
        if ios_id:
            resolved_plat, app_id = "ios", ios_id
        elif android_id:
            resolved_plat, app_id = "android", android_id
        else:
            return PipelineFailure(
                "upstream_error",
                "Could not derive iOS or Android app id from autocomplete result.",
                chosen,
            )

    return chosen, resolved_plat, app_id, warnings


@click.command("version")
@click.argument("query")
@click.option(
    "--platform",
    "platform",
    type=click.Choice(["auto", "ios", "android"], case_sensitive=False),
    default="auto",
    show_default=True,
    help="Which store timeline to request (auto prefers iOS when both exist).",
)
@click.option(
    "--country",
    "country",
    default="US",
    show_default=True,
    help="Two-letter country / storefront code.",
)
@click.option(
    "--max-age-days",
    "max_age_days",
    type=int,
    default=30,
    show_default=True,
    help="Only include rows whose time is within this many days (UTC); default ~one month.",
)
@click.option(
    "--pick",
    "pick",
    type=int,
    default=None,
    help="When search returns multiple apps, pick 1-based index from candidates",
)
@click.option(
    "--pick-strategy",
    "pick_strategy",
    type=click.Choice(["heuristic", "first", "fail"], case_sensitive=False),
    default="heuristic",
    show_default=True,
    help="How to resolve multiple autocomplete matches when --pick is not set.",
)
@click.option("--json", "as_json", is_flag=True)
@click.option("--yaml", "as_yaml", is_flag=True)
def version(
    query: str,
    platform: str,
    country: str,
    max_age_days: int,
    pick: int | None,
    pick_strategy: str,
    as_json: bool,
    as_yaml: bool,
) -> None:
    """Fetch Sensor Tower **update timeline** (version / metadata changes) for one app.

    QUERY may be an App Store or Play Store URL, a numeric iOS App Store id, an Android
    package name (``com.example.app``), or a free-text name (autocomplete).
    """
    if max_age_days < 0:
        print_payload(
            error_payload("bad_request", "--max-age-days must be >= 0", None),
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

    search_term_used, warn0 = prepare_search_term(query)
    warnings: list[str] = list(warn0)
    chosen: dict[str, Any] | None = None
    resolved_platform: str
    app_id: str

    try:
        with create_st_client(cred.cookies) as client:
            csrf_token = get_csrf_token_for_top_apps_page(client)
            fast = _fast_path_platform_and_app_id(query, platform)
            if fast is not None:
                resolved_platform, app_id = fast
                history = (
                    get_ios_app_update_history(
                        client, app_id=app_id, country=country, csrf_token=csrf_token
                    )
                    if resolved_platform == "ios"
                    else get_android_app_update_history(
                        client, app_id=app_id, country=country, csrf_token=csrf_token
                    )
                )
            else:
                resolved = _resolve_via_autocomplete(
                    client,
                    query,
                    pick_1based=pick,
                    pick_strategy=pick_strategy,
                    platform=platform,
                )
                if isinstance(resolved, PipelineFailure):
                    print_payload(
                        error_payload(resolved.code, resolved.message, resolved.details),
                        as_json=as_json,
                        as_yaml=as_yaml,
                    )
                    raise SystemExit(1)
                if isinstance(resolved, PipelineDisambiguation):
                    print_payload(
                        success_payload(
                            {
                                "needs_disambiguation": True,
                                "candidates": resolved.candidates,
                                "warnings": resolved.warnings,
                                "search_term_used": resolved.search_term,
                                "input": {"raw": resolved.raw_query},
                                "comments": [],
                            }
                        ),
                        as_json=as_json,
                        as_yaml=as_yaml,
                    )
                    raise SystemExit(0)

                chosen, resolved_platform, app_id, ac_warnings = resolved
                warnings = [*warnings, *ac_warnings]
                history = (
                    get_ios_app_update_history(
                        client, app_id=app_id, country=country, csrf_token=csrf_token
                    )
                    if resolved_platform == "ios"
                    else get_android_app_update_history(
                        client, app_id=app_id, country=country, csrf_token=csrf_token
                    )
                )
    except RuntimeError as exc:
        logger.exception("version command failed")
        print_payload(error_payload("upstream_error", str(exc)), as_json=as_json, as_yaml=as_yaml)
        raise SystemExit(1) from None

    slim = slim_app_update_timeline_entries(history)
    versions = filter_timeline_entries_within_days(slim, days=max_age_days)

    print_payload(
        success_payload(
            {
                "input": {"raw": query, "search_term_used": search_term_used},
                "platform": resolved_platform,
                "app_id": app_id,
                "country": str(country).strip().upper(),
                "max_age_days": max_age_days,
                "selected": chosen,
                "versions": versions,
                "warnings": warnings,
                "comments": [],
            }
        ),
        as_json=as_json,
        as_yaml=as_yaml,
    )
