"""Shared fetch pipeline: autocomplete → internal_entities → monthly facets."""

from __future__ import annotations

import re
import time
from datetime import date, timedelta
from dataclasses import dataclass
from typing import Any

import httpx

from st_cli.st_api import (
    autocomplete_search,
    extract_store_hints,
    extract_first_release_date_us_from_facets_v2_rows,
    extract_revenue_absolute_from_facets_v2_rows,
    get_csrf_token_for_top_apps_page,
    apps_facets_v2_month_slice,
    get_app_comments,
    month_ranges_last_n_months,
)

MONTH_WINDOW_MONTHS = 12
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


def _tokenize_name(text: str) -> set[str]:
    low = str(text or "").lower()
    toks = {t for t in re.split(r"[^a-z0-9]+", low) if len(t) >= 3}
    return toks


def _candidate_title(candidate: dict[str, Any]) -> str:
    return str(candidate.get("humanized_name") or candidate.get("name") or "").strip()


def _candidate_publisher(candidate: dict[str, Any]) -> str:
    return str(candidate.get("publisher_name") or "").strip()


def _candidate_has_both_platforms(candidate: dict[str, Any]) -> bool:
    ios_apps = candidate.get("ios_apps")
    android_apps = candidate.get("android_apps")
    return bool(isinstance(ios_apps, list) and ios_apps) and bool(
        isinstance(android_apps, list) and android_apps
    )


def _score_candidate(raw_query: str, candidate: dict[str, Any]) -> float:
    q = (raw_query or "").strip().lower()
    title = _candidate_title(candidate).lower()
    pub = _candidate_publisher(candidate).lower()

    score = 0.0
    if not q:
        return score

    if q == title:
        score += 8.0
    elif q and title and (q in title or title in q):
        score += 5.0

    q_toks = _tokenize_name(q)
    t_toks = _tokenize_name(title)
    if q_toks and t_toks:
        overlap = len(q_toks & t_toks)
        score += 3.0 * (overlap / max(1, len(q_toks)))

    p_toks = _tokenize_name(pub)
    if q_toks and p_toks and (q_toks & p_toks):
        score += 0.8

    if _candidate_has_both_platforms(candidate):
        score += 0.5

    if candidate.get("active") is False:
        score -= 1.0

    return score


def _choose_candidate_heuristic(
    *,
    raw_query: str,
    candidates: list[dict[str, Any]],
    warnings: list[str],
) -> int | None:
    scored: list[tuple[float, int]] = []
    for idx, c in enumerate(candidates):
        if not isinstance(c, dict):
            continue
        scored.append((_score_candidate(raw_query, c), idx))
    if not scored:
        return None
    scored.sort(key=lambda x: (-x[0], x[1]))
    top_score, top_idx = scored[0]
    second_score = scored[1][0] if len(scored) > 1 else None

    # Guardrails: ensure match is confident.
    if top_score < 2.2:
        warnings.append("pick_strategy=heuristic:low_confidence")
        return None
    if second_score is not None and (top_score - second_score) < 0.8:
        warnings.append("pick_strategy=heuristic:ambiguous_top2")
        return None
    warnings.append(f"pick_strategy=heuristic:score={top_score:.2f}")
    return top_idx


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
    pick_strategy: str = "heuristic",
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

    idx: int | None = None
    if pick_1based is not None:
        idx = pick_1based - 1
    elif auto_pick_first:
        idx = 0
        warnings.append("pick_strategy=first:auto_pick_first")
    else:
        strategy = (pick_strategy or "heuristic").strip().lower()
        if len(candidates) <= 1:
            idx = 0
        elif strategy == "first":
            idx = 0
            warnings.append("pick_strategy=first")
        elif strategy == "heuristic":
            idx = _choose_candidate_heuristic(raw_query=raw_query, candidates=candidates, warnings=warnings)
        elif strategy == "fail":
            idx = None
            warnings.append("pick_strategy=fail")
        else:
            idx = _choose_candidate_heuristic(raw_query=raw_query, candidates=candidates, warnings=warnings)

    if idx is None:
        return PipelineDisambiguation(
            candidates=candidates,
            warnings=warnings + ["needs_disambiguation:true"],
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

    first_release_date_us: str | None = None
    try:
        today = date.today()
        month_start = today.replace(day=1)
        month_end = today - timedelta(days=2)
        if month_end < month_start:
            month_end = month_start
        comparison_start = _shift_month(month_start, -1)
        comparison_end = _shift_month(month_end, -1)

        num_rows = apps_facets_v2_month_slice(
            client,
            facet_ids,
            month_start,
            month_end,
            comparison_start,
            comparison_end,
            csrf_token=csrf_token,
        )
        first_release_date_us = extract_first_release_date_us_from_facets_v2_rows(num_rows)
    except RuntimeError as exc:
        warnings.append(f"first_release_failed:{exc}")

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
        "first_release_date_us": first_release_date_us,
        "revenue": {
            "currency": "USD",
            "monthly_estimates": monthly_estimates,
            "window_months": MONTH_WINDOW_MONTHS,
        },
        "comments": comments,
        "warnings": warnings,
    }
    return PipelineSuccess(payload=payload)
