"""Shared fetch pipeline: autocomplete → internal_entities → monthly facets."""

from __future__ import annotations

import re
import time
from datetime import date, timedelta
from dataclasses import dataclass
from typing import Any

import httpx

from st_cli.constants import DEFAULT_FACET_REGIONS
from st_cli.st_api import (
    autocomplete_search,
    extract_store_hints,
    extract_first_release_date_us_from_facets_v2_rows,
    extract_downloads_absolute_from_facets_v2_rows,
    extract_mau_absolute_from_facets_v2_rows,
    extract_revenue_absolute_from_facets_v2_rows,
    extract_total_revenue_absolute_from_facets_v2_rows,
    extract_total_revenue_absolute_any_from_facets_v2_rows,
    extract_unified_app_id_from_facets_v2_rows,
    extract_wau_absolute_from_facets_v2_rows,
    get_csrf_token_for_top_apps_page,
    apps_facets_v2_month_slice,
    get_app_comments,
    internal_entities,
    resolve_internal_entities_app_id,
    month_ranges_last_n_months,
    top_sub_app_ids,
)

MONTH_WINDOW_MONTHS = 12
COMMENTS_LOOKBACK_DAYS = 120
COMMENTS_LIMIT = 20
MARKET_SHARE_TOP_APPS_LIMIT_DEFAULT = 100

_APP_CATEGORY_IDS_CACHE: dict[int, list[int]] = {}

_MARKET_SHARE_TOTAL_CACHE: dict[tuple[Any, ...], float | None] = {}


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


def _unique_ints(values: list[int]) -> list[int]:
    seen: set[int] = set()
    out: list[int] = []
    for v in values:
        if v in seen:
            continue
        seen.add(v)
        out.append(v)
    return out


def _parse_int_id(v: Any) -> int | None:
    if v is None or isinstance(v, bool):
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, float) and v.is_integer():
        return int(v)
    if isinstance(v, str):
        s = v.strip()
        if s.isdigit():
            return int(s)
    return None


def _extract_category_ids_from_obj(obj: Any, *, _max_depth: int = 8) -> list[int]:
    """Best-effort extraction of ST category id(s) from autocomplete/internal_entities payloads.

    The exact key naming may vary across ST responses; we focus on keys that include "category" and "id",
    plus common containers like "categories"/"category_ids".
    """

    found: list[int] = []

    def walk(v: Any, depth: int) -> None:
        if depth > _max_depth:
            return
        if isinstance(v, list):
            for item in v:
                if isinstance(item, (dict, list)):
                    walk(item, depth + 1)
            return
        if not isinstance(v, dict):
            return

        for k, vv in v.items():
            lk = str(k).lower()

            if "category" in lk or lk in {"categories", "category_ids"}:
                # Direct scalar id: category_id / categoryId / categoryID / .../_id
                if ("id" in lk and not isinstance(vv, (dict, list))) or (lk.endswith("_id")):
                    got = _parse_int_id(vv)
                    if got is not None:
                        found.append(got)
                elif lk in {"categories", "category_ids"}:
                    # Common list container: categories: [{"id": ...}, ...] or [123, ...]
                    if isinstance(vv, list):
                        for item in vv:
                            if isinstance(item, dict):
                                got = (
                                    _parse_int_id(item.get("id"))
                                    or _parse_int_id(item.get("category_id"))
                                    or _parse_int_id(item.get("categoryId"))
                                    or _parse_int_id(item.get("categoryID"))
                                )
                                if got is not None:
                                    found.append(got)
                            else:
                                got = _parse_int_id(item)
                                if got is not None:
                                    found.append(got)
                    else:
                        got = _parse_int_id(vv)
                        if got is not None:
                            found.append(got)
                elif lk == "category":
                    if isinstance(vv, dict):
                        got = (
                            _parse_int_id(vv.get("id"))
                            or _parse_int_id(vv.get("category_id"))
                            or _parse_int_id(vv.get("categoryId"))
                            or _parse_int_id(vv.get("categoryID"))
                        )
                        if got is not None:
                            found.append(got)
                    else:
                        got = _parse_int_id(vv)
                        if got is not None:
                            found.append(got)

            if isinstance(vv, (dict, list)):
                walk(vv, depth + 1)

    walk(obj, 0)
    return _unique_ints(found)


def prepare_search_term(raw_query: str) -> tuple[str, list[str]]:
    """Return search term for autocomplete and warning tags (store URL hints)."""
    warnings: list[str] = []
    search_term = raw_query.strip()
    hints = extract_store_hints(search_term)
    # Prefer numeric id for iOS because slugs can be variant/redirect-specific and
    # might not autocomplete reliably; numeric id is stable.
    if hints.get("ios_numeric_id"):
        search_term = hints["ios_numeric_id"]
        warnings.append("using_ios_store_id_from_url")
    elif hints.get("ios_slug"):
        search_term = hints["ios_slug"]
        warnings.append("using_ios_slug_from_url")
    elif hints.get("android_package"):
        search_term = hints["android_package"]
        warnings.append("using_android_package_from_url")
    return search_term, warnings


def prepare_search_term_candidates(raw_query: str) -> list[tuple[str, list[str]]]:
    """Return prioritized autocomplete search terms for store URLs and plain queries."""
    base = raw_query.strip()
    if not base:
        return [("", [])]

    hints = extract_store_hints(base)
    candidates: list[tuple[str, list[str]]] = []

    def add(term: str | None, warning: str | None = None) -> None:
        s = str(term or "").strip()
        if not s:
            return
        warnings = [warning] if warning else []
        item = (s, warnings)
        if item not in candidates:
            candidates.append(item)

    if hints.get("ios_numeric_id"):
        add(hints["ios_numeric_id"], "using_ios_store_id_from_url")
    if hints.get("ios_slug"):
        add(hints["ios_slug"], "using_ios_slug_from_url")
    if hints.get("android_package"):
        add(hints["android_package"], "using_android_package_from_url")
    add(base)
    return candidates


def prepare_match_query(raw_query: str) -> str:
    """Return a descriptive string for candidate scoring."""
    base = raw_query.strip()
    if not base:
        return base
    hints = extract_store_hints(base)
    if hints.get("ios_slug"):
        return str(hints["ios_slug"]).strip()
    if hints.get("android_package"):
        return str(hints["android_package"]).strip()
    return base


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


def collect_monthly_metrics(
    client: httpx.Client,
    app_ids: list[int | str],
    warnings: list[str],
    *,
    csrf_token: str | None,
    month_windows: list[tuple[date, date]],
) -> list[dict[str, Any]]:
    """Fill monthly revenue/downloads/MAU for given calendar months (newest first)."""
    monthly_revenue: list[dict[str, Any]] = []
    monthly_downloads: list[dict[str, Any]] = []
    monthly_mau: list[dict[str, Any]] = []

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
            downloads = extract_downloads_absolute_from_facets_v2_rows(rows)
            mau = extract_mau_absolute_from_facets_v2_rows(rows)

            monthly_revenue.append(
                {
                    "month": m_start.strftime("%Y-%m"),
                    "revenue_absolute_usd": rev,
                }
            )
            monthly_downloads.append(
                {
                    "month": m_start.strftime("%Y-%m"),
                    "downloads_absolute": downloads,
                }
            )
            monthly_mau.append(
                {
                    "month": m_start.strftime("%Y-%m"),
                    "mau_absolute": mau,
                }
            )
        except RuntimeError as exc:
            warnings.append(f"month_failed:{m_start}:{exc}")
            month_key = m_start.strftime("%Y-%m")
            monthly_revenue.append({"month": month_key, "revenue_absolute_usd": None})
            monthly_downloads.append({"month": month_key, "downloads_absolute": None})
            monthly_mau.append({"month": month_key, "mau_absolute": None})

        if i % 6 == 0:
            time.sleep(0.2)

    return [
        {"type": "revenue", "monthly_estimates": monthly_revenue},
        {"type": "downloads", "monthly_estimates": monthly_downloads},
        {"type": "mau", "monthly_estimates": monthly_mau},
    ]


def _month_key_from_window(m_start: date) -> str:
    return m_start.strftime("%Y-%m")


def _get_previous_month_comparison_range(m_start: date) -> tuple[date, date]:
    prev_end = m_start - timedelta(days=1)
    prev_start = prev_end.replace(day=1)
    return prev_start, prev_end


def _comparison_range_for_window(start_date: date, end_date: date) -> tuple[date, date]:
    window_days = (end_date - start_date).days
    comparison_end = start_date - timedelta(days=1)
    comparison_start = comparison_end - timedelta(days=window_days)
    return comparison_start, comparison_end


def _extract_unified_numeric_value(
    facet_rows: list[dict[str, Any]],
    key: str,
    *,
    divide_by: float = 1.0,
) -> float | None:
    for row in facet_rows:
        if row.get("appId") is not None:
            continue
        val = row.get(key)
        if val is None or val == "":
            return None
        if isinstance(val, (int, float)):
            return float(val) / divide_by
        s = str(val).strip()
        if not s:
            return None
        try:
            return float(s) / divide_by
        except ValueError:
            return None
    return None


def _growth_vs_previous_percent(current: float | None, previous: float | None) -> float | None:
    if current is None or previous is None or previous == 0:
        return None
    return round((float(current) - float(previous)) / float(previous) * 100.0, 6)


def _empty_market_share_payload(start_date: date, end_date: date) -> dict[str, Any]:
    return {
        "share_percent": None,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "market_revenue_total_proxy_usd": None,
        "top_apps_limit": MARKET_SHARE_TOP_APPS_LIMIT_DEFAULT,
        "category": None,
        "category_candidates": [],
    }


def _resolve_market_share_category_ids(
    client: httpx.Client,
    chosen: dict[str, Any],
    *,
    csrf_token: str | None,
    category_override: int | None,
) -> list[int]:
    if category_override is not None:
        return [category_override]

    category_ids = _extract_category_ids_from_obj(chosen)
    if category_ids:
        return category_ids

    resolved_app_id = resolve_internal_entities_app_id(chosen)
    if not isinstance(resolved_app_id, int):
        return []
    if resolved_app_id in _APP_CATEGORY_IDS_CACHE:
        return _APP_CATEGORY_IDS_CACHE[resolved_app_id]

    category_ids = []
    apps = internal_entities(client, [resolved_app_id], csrf_token=csrf_token)
    for app in apps:
        if not isinstance(app, dict):
            continue
        category_ids.extend(_extract_category_ids_from_obj(app))
    category_ids = _unique_ints(category_ids)
    _APP_CATEGORY_IDS_CACHE[resolved_app_id] = category_ids
    return category_ids


def _compute_market_share_for_window(
    client: httpx.Client,
    *,
    chosen: dict[str, Any],
    warnings: list[str],
    numerator_revenue_usd: float | None,
    start_date: date,
    end_date: date,
    comparison_start: date,
    comparison_end: date,
    csrf_token: str | None,
    market_share_category_override: int | None = None,
    market_share_top_apps_limit: int = MARKET_SHARE_TOP_APPS_LIMIT_DEFAULT,
) -> dict[str, Any]:
    category_ids = _resolve_market_share_category_ids(
        client,
        chosen,
        csrf_token=csrf_token,
        category_override=market_share_category_override,
    )
    if not category_ids:
        category_ids = [0]
        warnings.append("market_share_category_infer_failed")

    market_share_category_id = category_ids[0]
    cache_key = (
        start_date.isoformat(),
        end_date.isoformat(),
        market_share_category_id,
        market_share_top_apps_limit,
    )
    if cache_key in _MARKET_SHARE_TOTAL_CACHE:
        denom_total = _MARKET_SHARE_TOTAL_CACHE[cache_key]
    else:
        top_sub_ids = top_sub_app_ids(
            client,
            measure="revenue",
            start_date=end_date,
            end_date=end_date,
            comparison_attribute="absolute",
            category=market_share_category_id,
            regions=DEFAULT_FACET_REGIONS,
            limit=market_share_top_apps_limit,
            csrf_token=csrf_token,
        )
        denom_rows = apps_facets_v2_month_slice(
            client,
            top_sub_ids,
            start_date,
            end_date,
            comparison_start,
            comparison_end,
            csrf_token=csrf_token,
        )
        denom_total = extract_total_revenue_absolute_from_facets_v2_rows(denom_rows)
        if denom_total is None:
            denom_total = extract_total_revenue_absolute_any_from_facets_v2_rows(denom_rows)
        _MARKET_SHARE_TOTAL_CACHE[cache_key] = denom_total

    share_percent: float | None = None
    if numerator_revenue_usd is not None and denom_total is not None and denom_total > 0:
        share_percent = round(float(numerator_revenue_usd) / float(denom_total) * 100.0, 6)

    return {
        "share_percent": share_percent,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "market_revenue_total_proxy_usd": denom_total,
        "top_apps_limit": market_share_top_apps_limit,
        "category": market_share_category_id,
        "category_candidates": category_ids,
    }


def run_snapshot_pipeline(
    client: httpx.Client,
    raw_query: str,
    *,
    start_date: date,
    end_date: date,
    match_query: str | None = None,
    pick_1based: int | None = None,
    auto_pick_first: bool = False,
    pick_strategy: str = "heuristic",
    market_share_category_override: int | None = None,
    market_share_top_apps_limit: int = MARKET_SHARE_TOP_APPS_LIMIT_DEFAULT,
) -> PipelineSuccess | PipelineDisambiguation | PipelineFailure:
    """Resolve QUERY to ST app and pull one arbitrary-date snapshot."""
    search_candidates = prepare_search_term_candidates(raw_query)
    search_term = raw_query.strip()
    warnings: list[str] = []
    candidates: list[dict[str, Any]] = []
    for term, term_warnings in search_candidates:
        search_term = term
        warnings = list(term_warnings)
        candidates = autocomplete_search(client, search_term, limit=20)
        if candidates:
            break
    if not candidates:
        return PipelineFailure(
            "not_found",
            "No apps returned from autocomplete",
            {"term": search_term},
        )

    score_query = prepare_match_query(match_query or raw_query)
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
            idx = _choose_candidate_heuristic(raw_query=score_query, candidates=candidates, warnings=warnings)
        elif strategy == "fail":
            idx = None
            warnings.append("pick_strategy=fail")
        else:
            idx = _choose_candidate_heuristic(raw_query=score_query, candidates=candidates, warnings=warnings)
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
    comparison_start, comparison_end = _comparison_range_for_window(start_date, end_date)

    try:
        rows = apps_facets_v2_month_slice(
            client,
            facet_ids,
            start_date,
            end_date,
            comparison_start,
            comparison_end,
            csrf_token=csrf_token,
        )
    except RuntimeError as exc:
        return PipelineFailure("upstream_error", str(exc), {"query": raw_query})

    unified_app_id = extract_unified_app_id_from_facets_v2_rows(rows)
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

    snapshot_revenue_usd = extract_revenue_absolute_from_facets_v2_rows(rows)
    snapshot_revenue_previous_usd = _extract_unified_numeric_value(
        rows,
        "revenueAbsolutePrevious",
        divide_by=100.0,
    )
    snapshot_downloads_absolute = extract_downloads_absolute_from_facets_v2_rows(rows)
    snapshot_downloads_previous_absolute = _extract_unified_numeric_value(rows, "downloadsAbsolutePrevious")
    snapshot_mau_absolute = extract_mau_absolute_from_facets_v2_rows(rows)
    snapshot_mau_previous_absolute = _extract_unified_numeric_value(rows, "activeUsersMAUAbsolutePrevious")
    snapshot_wau_absolute = extract_wau_absolute_from_facets_v2_rows(rows)
    snapshot_wau_previous_absolute = _extract_unified_numeric_value(rows, "activeUsersWAUAbsolutePrevious")

    payload: dict[str, Any] = {
        "input": {"raw": raw_query, "search_term_used": search_term},
        "selected": chosen,
        "unified_app_id": unified_app_id,
        "first_release_date_us": extract_first_release_date_us_from_facets_v2_rows(rows),
        "snapshot_window": {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "comparison_start_date": comparison_start.isoformat(),
            "comparison_end_date": comparison_end.isoformat(),
        },
        "snapshot": {
            "revenue_usd": snapshot_revenue_usd,
            "revenue_previous_window_usd": snapshot_revenue_previous_usd,
            "revenue_growth_vs_previous_window_percent": _growth_vs_previous_percent(
                snapshot_revenue_usd,
                snapshot_revenue_previous_usd,
            ),
            "downloads_absolute": snapshot_downloads_absolute,
            "downloads_previous_window_absolute": snapshot_downloads_previous_absolute,
            "downloads_growth_vs_previous_window_percent": _growth_vs_previous_percent(
                snapshot_downloads_absolute,
                snapshot_downloads_previous_absolute,
            ),
            "mau_absolute": snapshot_mau_absolute,
            "mau_previous_window_absolute": snapshot_mau_previous_absolute,
            "mau_growth_vs_previous_window_percent": _growth_vs_previous_percent(
                snapshot_mau_absolute,
                snapshot_mau_previous_absolute,
            ),
            "wau_absolute": snapshot_wau_absolute,
            "wau_previous_window_absolute": snapshot_wau_previous_absolute,
            "wau_growth_vs_previous_window_percent": _growth_vs_previous_percent(
                snapshot_wau_absolute,
                snapshot_wau_previous_absolute,
            ),
        },
        "comments": comments,
        "warnings": warnings,
    }
    payload["market_share_in_window"] = _empty_market_share_payload(start_date, end_date)
    payload["market_share_in_window"]["top_apps_limit"] = market_share_top_apps_limit
    try:
        payload["market_share_in_window"] = _compute_market_share_for_window(
            client,
            chosen=chosen,
            warnings=warnings,
            numerator_revenue_usd=snapshot_revenue_usd,
            start_date=start_date,
            end_date=end_date,
            comparison_start=comparison_start,
            comparison_end=comparison_end,
            csrf_token=csrf_token,
            market_share_category_override=market_share_category_override,
            market_share_top_apps_limit=market_share_top_apps_limit,
        )
    except (RuntimeError, httpx.HTTPError) as exc:
        warnings.append(f"market_share_failed:{exc}")
    return PipelineSuccess(payload=payload)


def run_fetch_pipeline(
    client: httpx.Client,
    raw_query: str,
    *,
    pick_1based: int | None = None,
    auto_pick_first: bool = False,
    pick_strategy: str = "heuristic",
    include_market_share: bool = False,
    market_share_category_override: int | None = None,
    market_share_top_apps_limit: int = MARKET_SHARE_TOP_APPS_LIMIT_DEFAULT,
    market_share_month_key: str | None = None,
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

    score_query = prepare_match_query(raw_query)
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
            idx = _choose_candidate_heuristic(raw_query=score_query, candidates=candidates, warnings=warnings)
        elif strategy == "fail":
            idx = None
            warnings.append("pick_strategy=fail")
        else:
            idx = _choose_candidate_heuristic(raw_query=score_query, candidates=candidates, warnings=warnings)

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

    # Align with SensorTower "as-of" delay (data available up to ~2 days ago).
    month_windows = month_ranges_last_n_months(
        MONTH_WINDOW_MONTHS,
        end=date.today() - timedelta(days=2),
    )

    metrics = collect_monthly_metrics(
        client,
        facet_ids,
        warnings,
        csrf_token=csrf_token,
        month_windows=month_windows,
    )

    monthly_revenue = next(it["monthly_estimates"] for it in metrics if it["type"] == "revenue")
    monthly_downloads = next(it["monthly_estimates"] for it in metrics if it["type"] == "downloads")
    monthly_mau = next(it["monthly_estimates"] for it in metrics if it["type"] == "mau")

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
            "monthly_estimates": monthly_revenue,
            "window_months": MONTH_WINDOW_MONTHS,
        },
        "downloads": {"monthly_estimates": monthly_downloads, "window_months": MONTH_WINDOW_MONTHS},
        "mau": {"monthly_estimates": monthly_mau, "window_months": MONTH_WINDOW_MONTHS},
        "comments": comments,
        "warnings": warnings,
    }

    if include_market_share:
        # Use the same month_key as the caller's reporting month (e.g. st landscape uses previous calendar month).
        target_window = month_windows[0]
        if market_share_month_key:
            for win in month_windows:
                if _month_key_from_window(win[0]) == market_share_month_key:
                    target_window = win
                    break

        as_of_month_start, as_of_month_end = target_window
        as_of_month_key = _month_key_from_window(as_of_month_start)

        category_ids: list[int] = []
        if market_share_category_override is not None:
            category_ids = [market_share_category_override]
        else:
            category_ids = _extract_category_ids_from_obj(chosen)
            if not category_ids:
                resolved_app_id = resolve_internal_entities_app_id(chosen)
                if isinstance(resolved_app_id, int):
                    if resolved_app_id in _APP_CATEGORY_IDS_CACHE:
                        category_ids = _APP_CATEGORY_IDS_CACHE[resolved_app_id]
                    else:
                        apps = internal_entities(client, [resolved_app_id], csrf_token=csrf_token)
                        for app in apps:
                            if not isinstance(app, dict):
                                continue
                            category_ids.extend(_extract_category_ids_from_obj(app))
                        category_ids = _unique_ints(category_ids)
                        _APP_CATEGORY_IDS_CACHE[resolved_app_id] = category_ids

        if not category_ids:
            category_ids = [0]
            warnings.append("market_share_category_infer_failed")

        market_share_category_id = category_ids[0]

        numerator: float | None = None
        if monthly_revenue:
            for row in monthly_revenue:
                if isinstance(row, dict) and row.get("month") == as_of_month_key:
                    v = row.get("revenue_absolute_usd")
                    numerator = v if isinstance(v, (int, float)) else None
                    break

        cache_key = (as_of_month_key, market_share_category_id)
        if cache_key in _MARKET_SHARE_TOTAL_CACHE:
            denom_total = _MARKET_SHARE_TOTAL_CACHE[cache_key]
        else:
            prev_start, prev_end = _get_previous_month_comparison_range(as_of_month_start)
            # Denominator uses ST "top apps" as a proxy for the full market.
            # We feed v2 facets with `sub_app_ids` (more compatible than ObjectId-like `unified_app_id`).
            top_sub_ids = top_sub_app_ids(
                client,
                measure="revenue",
                start_date=as_of_month_end,
                end_date=as_of_month_end,
                comparison_attribute="absolute",
                category=market_share_category_id,
                regions=DEFAULT_FACET_REGIONS,
                limit=market_share_top_apps_limit,
                csrf_token=csrf_token,
            )
            denom_rows = apps_facets_v2_month_slice(
                client,
                top_sub_ids,
                as_of_month_start,
                as_of_month_end,
                prev_start,
                prev_end,
                csrf_token=csrf_token,
            )
            denom_total = extract_total_revenue_absolute_from_facets_v2_rows(denom_rows)
            if denom_total is None:
                # Some ST responses may omit unified (appId=None) aggregation rows.
                # Fallback to summing revenueAbsolute across all returned rows.
                denom_total = extract_total_revenue_absolute_any_from_facets_v2_rows(denom_rows)
            _MARKET_SHARE_TOTAL_CACHE[cache_key] = denom_total

        share_percent: float | None = None
        if numerator is not None and denom_total is not None and denom_total > 0:
            share_percent = float(numerator) / float(denom_total) * 100.0

        payload["market_share_as_of_last_month"] = {
            "share_percent": share_percent,
            "month": as_of_month_key,
            "market_revenue_total_proxy_usd": denom_total,
            "top_apps_limit": market_share_top_apps_limit,
            "category": market_share_category_id,
            "category_candidates": category_ids,
        }

    return PipelineSuccess(payload=payload)
