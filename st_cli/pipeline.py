"""Shared fetch pipeline: autocomplete → internal_entities → monthly facets."""

from __future__ import annotations

import time
from datetime import date, timedelta
from dataclasses import dataclass
from typing import Any

import httpx

from st_cli.constants import DEFAULT_FACET_REGIONS
from st_cli.st_api import (
    autocomplete_search,
    extract_store_hints,
    extract_revenue_absolute_from_facets_v2_rows,
    extract_total_revenue_absolute_from_facets_v2_rows,
    get_csrf_token_for_top_apps_page,
    apps_facets_v2_month_slice,
    get_app_comments,
    month_ranges_last_n_months,
    top_unified_app_ids,
    internal_entities,
)

MONTH_WINDOW_MONTHS = 12
MARKET_SHARE_TOP_APPS_LIMIT = 100
COMMENTS_LOOKBACK_DAYS = 120
COMMENTS_LIMIT = 20


def _shift_month(d: date, months: int) -> date:
    """Shift `d` by N calendar months, clamping the day to month end."""
    m0 = (d.month - 1) + months
    y = d.year + m0 // 12
    m = (m0 % 12) + 1
    if m == 12:
        next_month_year = y + 1
        next_month = 1
    else:
        next_month_year = y
        next_month = m + 1
    last_day = (date(next_month_year, next_month, 1) - timedelta(days=1)).day
    day = min(d.day, last_day)
    return date(y, m, day)


def _derive_facet_ids_from_candidate(candidate: dict[str, Any]) -> list[int | str]:
    facet_ids: list[int | str] = []

    ios_apps = candidate.get("ios_apps")
    if isinstance(ios_apps, list) and ios_apps:
        ios0 = ios_apps[0] if isinstance(ios_apps[0], dict) else {}
        app_id = ios0.get("app_id")
        if isinstance(app_id, int):
            facet_ids.append(app_id)
        elif isinstance(app_id, str) and app_id.strip().isdigit():
            facet_ids.append(int(app_id.strip()))

    if not facet_ids:
        android_apps = candidate.get("android_apps")
        if isinstance(android_apps, list) and android_apps:
            android0 = android_apps[0] if isinstance(android_apps[0], dict) else {}
            app_id = android0.get("app_id")
            if app_id is not None:
                facet_ids.append(app_id)

    if not facet_ids:
        fallback = candidate.get("app_id")
        if isinstance(fallback, int):
            facet_ids.append(fallback)
        elif isinstance(fallback, str) and fallback.strip().isdigit():
            facet_ids.append(int(fallback.strip()))

    return facet_ids


def prepare_search_term(raw_query: str) -> tuple[str, list[str]]:
    """Return search term for autocomplete and warning tags (store URL hints)."""
    warnings: list[str] = []
    search_term = raw_query.strip()
    hints = extract_store_hints(search_term)
    if hints.get("ios_slug"):
        search_term = hints["ios_slug"]
        warnings.append("using_ios_slug_from_url")
    elif hints.get("ios_numeric_id"):
        search_term = hints["ios_numeric_id"]
        warnings.append("using_ios_store_id_from_url")
    elif hints.get("android_package"):
        search_term = hints["android_package"]
        warnings.append("using_android_package_from_url")
    return search_term, warnings


@dataclass(frozen=True)
class PipelineFailure:
    """Unrecoverable step for this query."""

    code: str
    message: str
    details: Any = None


@dataclass(frozen=True)
class PipelineDisambiguation:
    """Multiple autocomplete hits; user must pass --pick N (fetch only)."""

    candidates: list[dict[str, Any]]
    warnings: list[str]
    search_term: str
    raw_query: str


@dataclass(frozen=True)
class PipelineSuccess:
    """Resolved app + revenue series."""

    payload: dict[str, Any]


def collect_monthly_revenue(
    client: httpx.Client,
    app_ids: list[int | str],
    warnings: list[str],
    *,
    csrf_token: str | None,
) -> list[dict[str, Any]]:
    """Fill monthly ``revenue_absolute_usd`` for ``MONTH_WINDOW_MONTHS`` calendar months."""
    monthly_estimates: list[dict[str, Any]] = []
    # Align with SensorTower "as-of" delay (data available up to ~2 days ago).
    month_windows = month_ranges_last_n_months(MONTH_WINDOW_MONTHS, end=date.today() - timedelta(days=2))
    for i, (m_start, m_end) in enumerate(month_windows):
        try:
            # v2 requires comparison range (previous month of current month).
            prev_end = m_start - timedelta(days=1)
            prev_start = prev_end.replace(day=1)
            rows = apps_facets_v2_month_slice(
                client,
                app_ids,
                m_start,
                m_end,
                prev_start,
                prev_end,
                csrf_token=csrf_token,
            )
            rev = extract_revenue_absolute_from_facets_v2_rows(rows)
            monthly_estimates.append(
                {
                    "month": m_start.strftime("%Y-%m"),
                    "revenue_absolute_usd": rev,
                }
            )
        except RuntimeError as exc:
            warnings.append(f"month_failed:{m_start}:{exc}")
            monthly_estimates.append(
                {"month": m_start.strftime("%Y-%m"), "revenue_absolute_usd": None}
            )
        if i % 6 == 0:
            time.sleep(0.2)
    return monthly_estimates


def run_fetch_pipeline(
    client: httpx.Client,
    raw_query: str,
    *,
    pick_1based: int | None = None,
    auto_pick_first: bool = False,
    category: int = 0,
) -> PipelineSuccess | PipelineDisambiguation | PipelineFailure:
    """Resolve QUERY to ST app and pull monthly revenue (see ``MONTH_WINDOW_MONTHS``).

    Args:
        client: httpx client with Sensor Tower session cookies.
        raw_query: Store URL or free-text app name.
        pick_1based: When multiple autocomplete results, use this 1-based index.
        auto_pick_first: If True, always take the first candidate (batch mode).

    Returns:
        Success with ``payload``, disambiguation request, or failure.
    """
    search_term, warnings = prepare_search_term(raw_query)
    candidates = autocomplete_search(client, search_term, limit=20)
    if not candidates:
        return PipelineFailure(
            "not_found",
            "No apps returned from autocomplete",
            {"term": search_term},
        )

    if len(candidates) > 1 and not auto_pick_first and pick_1based is None:
        return PipelineDisambiguation(
            candidates=candidates,
            warnings=warnings + ["pass --pick N to choose"],
            search_term=search_term,
            raw_query=raw_query,
        )

    if auto_pick_first:
        idx = 0
    elif pick_1based is not None:
        idx = pick_1based - 1
    else:
        idx = 0

    if idx < 0 or idx >= len(candidates):
        return PipelineFailure(
            "bad_request",
            f"--pick out of range (1-{len(candidates)})",
            None,
        )

    chosen = candidates[idx]

    facet_ids = _derive_facet_ids_from_candidate(chosen)
    if not facet_ids:
        return PipelineFailure(
            "upstream_error",
            "Could not derive an `app_id` for `/api/apps/facets` from autocomplete result.",
            chosen,
        )
    csrf_token = get_csrf_token_for_top_apps_page(client)
    monthly_estimates = collect_monthly_revenue(
        client,
        facet_ids,
        warnings,
        csrf_token=csrf_token,
    )

    # Market share (as-of current month):
    # 1) numerator: selected app's revenueAbsolute (v2 facets) for current month as-of date
    # 2) denominator: sum of revenueAbsolute over sub_app_ids expanded from top unified apps
    #    using the same current-month as-of window.
    market_share_as_of_current_month: dict[str, Any] | None = None
    try:
        today = date.today()
        month_start = today.replace(day=1)
        # SensorTower UI often uses "latest available date" rather than the full calendar month.
        # Keep it aligned with the rest of this CLI logic by using a small delay.
        month_end = today - timedelta(days=2)
        if month_end < month_start:
            month_end = month_start

        month_key = month_start.strftime("%Y-%m")
        comparison_start = _shift_month(month_start, -1)
        comparison_end = _shift_month(month_end, -1)

        # Numerator: facets revenueAbsolute for the selected app(s).
        num_rows = apps_facets_v2_month_slice(
            client,
            facet_ids,
            month_start,
            month_end,
            comparison_start,
            comparison_end,
            csrf_token=csrf_token,
        )
        chosen_rev = extract_revenue_absolute_from_facets_v2_rows(num_rows)

        if chosen_rev is None:
            raise RuntimeError("chosen revenueAbsolute not found for current month as-of")

        top_ids = top_unified_app_ids(
            client,
            measure="revenue",
            start_date=month_start,
            end_date=month_end,
            comparison_attribute="absolute",
            category=category,
            regions=DEFAULT_FACET_REGIONS,
            limit=MARKET_SHARE_TOP_APPS_LIMIT,
            offset=0,
            csrf_token=csrf_token,
        )

        # Follow innovation-crawler flow:
        # top_apps(unified ids) -> internal_entities(sub apps) -> facets(sub apps)
        apps_for_top = internal_entities(client, top_ids, csrf_token=csrf_token)
        sub_app_ids: list[int | str] = []
        seen_sub: set[str] = set()
        for a in apps_for_top:
            for ios_app in a.get("ios_apps", []) or []:
                v = ios_app.get("app_id")
                if v is None:
                    continue
                key = f"ios:{v}"
                if key in seen_sub:
                    continue
                seen_sub.add(key)
                sub_app_ids.append(v)
            for android_app in a.get("android_apps", []) or []:
                v = android_app.get("app_id")
                if v is None:
                    continue
                key = f"and:{v}"
                if key in seen_sub:
                    continue
                seen_sub.add(key)
                sub_app_ids.append(v)

        rows = apps_facets_v2_month_slice(
            client,
            sub_app_ids,
            month_start,
            month_end,
            comparison_start,
            comparison_end,
            csrf_token=csrf_token,
        )
        total_rev = extract_total_revenue_absolute_from_facets_v2_rows(rows)
        if total_rev and total_rev > 0:
            share = float(chosen_rev) / float(total_rev)
            market_share_as_of_current_month = {
                "month": month_key,
                "as_of": month_end.isoformat(),
                "proxy": {
                    "denominator_app_ids_count": len(sub_app_ids),
                    "denominator_app_ids_preview": sub_app_ids[:20],
                    "note": "denominator = facets revenueAbsolute sum over sub_app_ids from top unified apps (regions=DEFAULT_FACET_REGIONS)",
                },
                "revenue_absolute_usd": chosen_rev,
                "market_revenue_absolute_usd": total_rev,
                "share": share,
                "share_percent": share * 100.0,
            }
    except RuntimeError as exc:
        warnings.append(f"market_share_failed:{exc}")

    # Comments: fetch last 120 days, first N items.
    comments: list[dict[str, Any]] = []
    try:
        ios_apps = chosen.get("ios_apps")
        ios_app_id: int | str | None = None
        if isinstance(ios_apps, list) and ios_apps and isinstance(ios_apps[0], dict):
            got = ios_apps[0].get("app_id")
            if got is not None:
                ios_app_id = got

        android_apps = chosen.get("android_apps")
        android_app_id: str | None = None
        if isinstance(android_apps, list) and android_apps and isinstance(android_apps[0], dict):
            got = android_apps[0].get("app_id")
            if isinstance(got, str):
                android_app_id = got

        start_date = date.today() - timedelta(days=COMMENTS_LOOKBACK_DAYS)
        end_date = date.today()
        comments = get_app_comments(
            client,
            ios_app_id=ios_app_id,
            android_app_id=android_app_id,
            start_date=start_date,
            end_date=end_date,
            limit=COMMENTS_LIMIT,
            csrf_token=csrf_token,
        )
    except RuntimeError as exc:
        warnings.append(f"comments_failed:{exc}")

    payload: dict[str, Any] = {
        "input": {"raw": raw_query, "search_term_used": search_term},
        "selected": chosen,
        "unified_app_id": None,
        "apps": [],
        "revenue": {
            "currency": "USD",
            "monthly_estimates": monthly_estimates,
            "window_months": MONTH_WINDOW_MONTHS,
        },
        # Backward-compat: historically this key was named "...last_month" but now it represents
        # "as-of current month" to match SensorTower UI.
        "market_share_last_month": market_share_as_of_current_month,
        "market_share_as_of_current_month": market_share_as_of_current_month,
        "comments": comments,
        "warnings": warnings,
    }
    return PipelineSuccess(payload=payload)
