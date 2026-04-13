"""Sensor Tower JSON API calls via httpx (cookie session)."""

import json
import re
import urllib.parse
from datetime import date, timedelta
from typing import Any

import httpx

from st_cli.constants import DEFAULT_DATA_MODEL, DEFAULT_FACET_REGIONS, POST_JSON_HEADERS


def _coerce_actual_type_value(v: int | str) -> int | str:
    """Match innovation-crawler `convert_actual_type`: try int, else keep string."""
    if isinstance(v, int):
        return v
    s = str(v).strip()
    if s.isdigit():
        return int(s)
    return v


def _response_diag(resp: httpx.Response) -> dict[str, str]:
    """Surface WAF/CDN hints when status is not 200."""
    h = resp.headers
    out: dict[str, str] = {}
    for name in ("server", "cf-ray", "cf-cache-status", "content-type"):
        v = h.get(name)
        if v:
            out[name] = v
    return out


def _parse_json_response(resp: httpx.Response) -> Any:
    text = resp.text
    if resp.status_code >= 400:
        raise RuntimeError(f"HTTP {resp.status_code}: {text[:500]}")
    if not text.strip():
        return {}
    return json.loads(text)


def probe_session(client: httpx.Client) -> dict[str, Any]:
    """GET autocomplete to verify cookies (same as crawler)."""
    params = {
        "entity_type": "app",
        "expand_entities": "true",
        "flags": "false",
        "limit": "1",
        "mark_usage_disabled_apps": "false",
        "os": "unified",
        "term": "a",
    }
    q = urllib.parse.urlencode(params)
    r = client.get(f"/api/autocomplete_search?{q}")
    if r.status_code == 200:
        try:
            _parse_json_response(r)
        except json.JSONDecodeError:
            return {
                "reachable": True,
                "api_ok": False,
                "hint": "Unexpected non-JSON from Sensor Tower.",
            }
        return {"reachable": True, "api_ok": True}
    preview = (r.text or "")[:400].replace("\n", " ")
    err: dict[str, Any] = {
        "reachable": True,
        "api_ok": False,
        "http_status": r.status_code,
        "body_preview": preview,
        "response_headers": _response_diag(r),
        "hint": (
            "403: stale/wrong cookies, or WAF (see response_headers.cf-ray). "
            "Quit Chrome and retry ``st login``; or set ST_CHROME_COOKIES_DB to your Profile's Cookies file."
        ),
    }
    if err["response_headers"].get("cf-ray"):
        err["hint"] += " If cf-ray is present, Cloudflare may be blocking non-browser TLS; try another network or contact ST."
    return err


def autocomplete_search(client: httpx.Client, term: str, limit: int = 20) -> list[dict[str, Any]]:
    """GET /api/autocomplete_search."""
    params = {
        "entity_type": "app",
        "expand_entities": "true",
        "flags": "false",
        "limit": str(limit),
        "mark_usage_disabled_apps": "false",
        "os": "unified",
        "term": term,
    }
    q = urllib.parse.urlencode(params)
    r = client.get(f"/api/autocomplete_search?{q}")
    data = _parse_json_response(r)
    entities = (
        data.get("data", {}).get("entities", [])
        if isinstance(data, dict)
        else []
    )
    out: list[dict[str, Any]] = []
    for e in entities:
        if not isinstance(e, dict):
            continue
        # Keep full expanded entity so we can find numeric ``unified_app_id`` (``app_id`` is often ObjectId).
        out.append(dict(e))
    return out


def resolve_internal_entities_app_id(chosen: dict[str, Any], _depth: int = 0) -> int | None:
    """Pick numeric unified id for ``POST /api/unified/internal_entities``.

    Autocomplete ``app_id`` is often a 24-char ObjectId string; that value returns HTTP 422.
    With ``expand_entities=true``, the numeric id usually appears as ``unified_app_id`` or under
    ``entity`` / platform app blobs.
    """
    if _depth > 6:
        return None

    def as_int(v: Any) -> int | None:
        if v is None or isinstance(v, bool):
            return None
        if isinstance(v, int):
            return v
        if isinstance(v, float) and v.is_integer():
            return int(v)
        if isinstance(v, str) and v.strip().isdigit():
            return int(v.strip())
        return None

    for key in ("unified_app_id", "canonical_unified_app_id"):
        got = as_int(chosen.get(key))
        if got is not None:
            return got

    aid = as_int(chosen.get("app_id"))
    if aid is not None:
        return aid

    for key in ("entity", "unified_app", "app"):
        sub = chosen.get(key)
        if isinstance(sub, dict):
            got = resolve_internal_entities_app_id(sub, _depth + 1)
            if got is not None:
                return got

    for plat in ("ios_apps", "android_apps"):
        arr = chosen.get(plat)
        if not isinstance(arr, list) or not arr:
            continue
        first = arr[0]
        if isinstance(first, dict):
            for k in ("unified_app_id", "canonical_unified_app_id"):
                got = as_int(first.get(k))
                if got is not None:
                    return got
            got = as_int(first.get("app_id"))
            if got is not None:
                return got
            nested = resolve_internal_entities_app_id(first, _depth + 1)
            if nested is not None:
                return nested

    return None


def _coerce_internal_entity_app_ids(values: list[Any]) -> list[int | str]:
    """ST expects numeric unified ids as JSON numbers, not strings (422 otherwise)."""
    out: list[int | str] = []
    for v in values:
        if v is None:
            continue
        if type(v) is int:
            out.append(v)
            continue
        s = str(v).strip()
        if s.isdigit():
            out.append(int(s))
        else:
            out.append(s)
    return out


def internal_entities(
    client: httpx.Client,
    app_ids: list[str | int],
    *,
    csrf_token: str | None = None,
) -> list[dict[str, Any]]:
    """POST /api/unified/internal_entities."""
    payload_ids = _coerce_internal_entity_app_ids(list(app_ids))
    headers: dict[str, str] = dict(POST_JSON_HEADERS)
    if csrf_token:
        headers["x-csrf-token"] = csrf_token
    r = client.post(
        "/api/unified/internal_entities",
        json={"app_ids": payload_ids, "load_launch_date": True},
        headers=headers,
    )
    data = _parse_json_response(r)
    return data.get("apps", []) if isinstance(data, dict) else []


def apps_facets_month_slice(
    client: httpx.Client,
    app_ids: list[int | str],
    month_start: date,
    month_end: date,
) -> list[dict[str, Any]]:
    """POST `/api/apps/facets` for one date window (inclusive).

    Note: `facets` schema must match the backend expectations (mirrors innovation-crawler).
    """
    body = {
        "app_ids": app_ids,
        "facets": [
            {
                "type": "custom",
                "name": "Release Date (US)",
            },
            {
                "type": "absolute",
                "measure": "downloads",
            },
            {
                "type": "growth",
                "measure": "downloads",
            },
            {
                "type": "absolute",
                "measure": "revenue",
            },
            {
                "type": "growth",
                "measure": "revenue",
            },
            {
                "type": "delta",
                "measure": "downloads",
            },
            {
                "type": "delta",
                "measure": "revenue",
            },
            {
                "type": "absolute",
                "measure": "dau",
            },
            {
                "type": "growth",
                "measure": "dau",
            },
            {
                "type": "delta",
                "measure": "dau",
            },
            {
                "type": "absolute",
                "measure": "wau",
            },
            {
                "type": "growth",
                "measure": "wau",
            },
            {
                "type": "delta",
                "measure": "wau",
            },
        ],
        "breakdowns": [
            "app_id",
            "unified_app_id",
        ],
        "filters": {
            "start_date": month_start.strftime("%Y-%m-%d"),
            "end_date": month_end.strftime("%Y-%m-%d"),
            "devices": [
                "iphone",
                "ipad",
                "android",
            ],
            "regions": DEFAULT_FACET_REGIONS,
            "time_range": "day",
        },
        "data_model": DEFAULT_DATA_MODEL,
    }
    r = client.post("/api/apps/facets", json=body, headers=dict(POST_JSON_HEADERS))
    data = _parse_json_response(r)
    return data.get("data", []) if isinstance(data, dict) else []


def get_csrf_token_for_top_apps_page(client: httpx.Client) -> str | None:
    """Best-effort scrape csrf token from the market-analysis top-apps HTML."""
    try:
        r = client.get("/market-analysis/top-apps?os=unified&edit=1")
    except httpx.HTTPError:
        return None
    if r.status_code != 200:
        return None
    # Example:
    # <meta name="csrf-token" content="..."/>
    m = re.search(r'<meta\s+name="csrf-token"\s+content="([^"]+)"\s*/?>', r.text, re.IGNORECASE)
    if not m:
        return None
    return m.group(1)


def apps_facets_v2_month_slice(
    client: httpx.Client,
    app_ids: list[int | str],
    month_start: date,
    month_end: date,
    comparison_start: date,
    comparison_end: date,
    *,
    csrf_token: str | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    """Call Sensor Tower v2 facets API (mirrors innovation-crawler `get_app_facets_v2`)."""
    coerced_ids = [_coerce_actual_type_value(v) for v in app_ids]

    body = {
        "facets": [
            {"facet": "est_active_users", "measure": "absolute", "time_period": "month", "aggregation": "avg_for_date_granularity", "alias": "activeUsersMAUAbsolute"},
            {"facet": "est_active_users", "measure": "delta", "time_period": "month", "aggregation": "avg_for_date_granularity", "alias": "activeUsersMAUDelta"},
            {"facet": "est_active_users", "measure": "comparison", "time_period": "month", "aggregation": "avg_for_date_granularity", "alias": "activeUsersMAUAbsolutePrevious"},
            {"facet": "est_active_users", "measure": "absolute", "time_period": "day", "aggregation": "avg_for_date_granularity", "alias": "activeUsersDAUAbsolute"},
            {"facet": "est_active_users", "measure": "growth", "time_period": "day", "aggregation": "avg_for_date_granularity", "alias": "activeUsersDAUGrowthPercent"},
            {"facet": "est_active_users", "measure": "comparison", "time_period": "day", "aggregation": "avg_for_date_granularity", "alias": "activeUsersDAUAbsolutePrevious"},
            {"facet": "est_active_users", "measure": "delta", "time_period": "day", "aggregation": "avg_for_date_granularity", "alias": "activeUsersDAUDelta"},
            {"facet": "est_active_users", "measure": "absolute", "time_period": "week", "aggregation": "avg_for_date_granularity", "alias": "activeUsersWAUAbsolute"},
            {"facet": "est_active_users", "measure": "growth", "time_period": "week", "aggregation": "avg_for_date_granularity", "alias": "activeUsersWAUGrowthPercent"},
            {"facet": "est_active_users", "measure": "comparison", "time_period": "week", "aggregation": "avg_for_date_granularity", "alias": "activeUsersWAUAbsolutePrevious"},
            {"facet": "est_active_users", "measure": "delta", "time_period": "week", "aggregation": "avg_for_date_granularity", "alias": "activeUsersWAUDelta"},
            {"facet": "downloads", "measure": "absolute", "alias": "downloadsAbsolute"},
            {"facet": "downloads", "measure": "delta", "alias": "downloadsDelta"},
            {"facet": "downloads", "measure": "growth", "alias": "downloadsGrowthPercent"},
            {"facet": "downloads", "measure": "comparison", "alias": "downloadsAbsolutePrevious"},
            {"facet": "revenue", "measure": "absolute", "alias": "revenueAbsolute"},
            {"facet": "revenue", "measure": "growth", "alias": "revenueGrowthPercent"},
            {"facet": "revenue", "measure": "delta", "alias": "revenueDelta"},
            {"facet": "revenue", "measure": "comparison", "alias": "revenueAbsolutePrevious"},
            {"facet": "global_tag", "field_name": "Earliest Release Date", "alias": "earliestReleaseDate"},
            {"facet": "global_tag", "field_name": "RPD (All Time, US)", "alias": "rpd"},
            {"facet": "global_tag", "field_name": "Release Date (US)", "alias": "releaseDate"},
            {"facet": "global_tag", "field_name": "Website URL", "alias": "websiteUrl"},
            {"facet": "unified_app_id", "alias": "unifiedAppId"},
            {"facet": "app_id", "alias": "appId"},
        ],
        "filters": {
            "start_date": month_start.strftime("%Y-%m-%d"),
            "end_date": month_end.strftime("%Y-%m-%d"),
            "comparison_start_date": comparison_start.strftime("%Y-%m-%d"),
            "comparison_end_date": comparison_end.strftime("%Y-%m-%d"),
            "devices": ["iphone", "ipad", "android"],
            "regions": DEFAULT_FACET_REGIONS,
        },
        "breakdowns": [["unifiedAppId", "appId"], ["unifiedAppId"]],
        "data_model": DEFAULT_DATA_MODEL,
    }
    if coerced_ids:
        body["filters"]["app_ids"] = coerced_ids
    if isinstance(limit, int) and limit > 0:
        body["limit"] = limit
    headers: dict[str, str] = dict(POST_JSON_HEADERS)
    if csrf_token:
        headers["x-csrf-token"] = csrf_token
    r = client.post(
        "/api/v2/apps/facets?query_identifier=TopAppsData",
        json=body,
        headers=headers,
    )
    data = _parse_json_response(r)
    return data.get("data", []) if isinstance(data, dict) else []


def top_unified_app_ids(
    client: httpx.Client,
    *,
    measure: str,
    start_date: date,
    end_date: date,
    comparison_attribute: str,
    category: int = 0,
    regions: list[str],
    limit: int,
    offset: int = 0,
    csrf_token: str | None = None,
) -> list[int | str]:
    """POST `/api/unified/top_apps` and return numeric `unified_app_id`s."""
    params = {
        "os": "unified",
        "filters": {
            "measure": measure,
            "comparison_attribute": comparison_attribute,
            "category": category,
            "devices": ["iphone", "ipad", "android"],
            "regions": regions,
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
            "time_range": "day",
        },
        "pagination": {"limit": limit, "offset": offset},
        "data_model": DEFAULT_DATA_MODEL,
    }
    headers: dict[str, str] = dict(POST_JSON_HEADERS)
    if csrf_token:
        headers["x-csrf-token"] = csrf_token
    r = client.post("/api/unified/top_apps", json=params, headers=headers)
    data = _parse_json_response(r)
    items = (
        data.get("data", {}).get("apps_ids", [])
        if isinstance(data, dict)
        else []
    )
    out: list[int | str] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        uid = it.get("unified_app_id")
        if uid is None:
            continue
        if isinstance(uid, int):
            out.append(uid)
        elif isinstance(uid, str) and uid.strip():
            out.append(uid.strip())
    return out


def top_sub_app_ids(
    client: httpx.Client,
    *,
    measure: str,
    start_date: date,
    end_date: date,
    comparison_attribute: str,
    category: int = 0,
    regions: list[str],
    limit: int,
    offset: int = 0,
    csrf_token: str | None = None,
) -> list[int | str]:
    """POST `/api/unified/top_apps` and return `sub_app_ids` for each top row.

    `unified_app_id` is often an ObjectId-like string; v2 facets works better with
    numeric sub app ids (and possibly package-name strings) from `sub_app_ids`.
    """
    params = {
        "os": "unified",
        "filters": {
            "measure": measure,
            "comparison_attribute": comparison_attribute,
            "category": category,
            "devices": ["iphone", "ipad", "android"],
            "regions": regions,
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
            "time_range": "day",
        },
        "pagination": {"limit": limit, "offset": offset},
        "data_model": DEFAULT_DATA_MODEL,
    }
    headers: dict[str, str] = dict(POST_JSON_HEADERS)
    if csrf_token:
        headers["x-csrf-token"] = csrf_token
    r = client.post("/api/unified/top_apps", json=params, headers=headers)
    data = _parse_json_response(r)
    items = (
        data.get("data", {}).get("apps_ids", [])
        if isinstance(data, dict)
        else []
    )
    out: list[int | str] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        sub_ids = it.get("sub_app_ids") or []
        if not isinstance(sub_ids, list):
            continue
        for sid in sub_ids:
            if sid is None:
                continue
            if isinstance(sid, int):
                out.append(sid)
                continue
            if isinstance(sid, str) and sid.strip():
                out.append(sid.strip())
    return out


def extract_revenue_absolute_from_facets_v2_rows(
    facet_rows: list[dict[str, Any]],
) -> float | None:
    """Extract `revenueAbsolute` (USD) from the unified row (`appId is None`).

    Sensor Tower facets v2 commonly returns revenue in cents; we normalize to USD.
    """
    for row in facet_rows:
        if row.get("appId") is not None:
            continue
        val = row.get("revenueAbsolute")
        if val is None or val == "":
            return None
        if isinstance(val, (int, float)):
            return float(val) / 100.0
        s = str(val).strip()
        if not s:
            return None
        try:
            return float(s) / 100.0
        except ValueError:
            return None
    return None


def extract_downloads_absolute_from_facets_v2_rows(
    facet_rows: list[dict[str, Any]],
) -> float | None:
    """Extract `downloadsAbsolute` from the unified row (`appId is None`)."""
    for row in facet_rows:
        if row.get("appId") is not None:
            continue
        val = row.get("downloadsAbsolute")
        if val is None or val == "":
            return None
        if isinstance(val, (int, float)):
            return float(val)
        s = str(val).strip()
        if not s:
            return None
        try:
            return float(s)
        except ValueError:
            return None
    return None


def extract_mau_absolute_from_facets_v2_rows(
    facet_rows: list[dict[str, Any]],
) -> float | None:
    """Extract `activeUsersMAUAbsolute` from the unified row (`appId is None`)."""
    for row in facet_rows:
        if row.get("appId") is not None:
            continue
        val = row.get("activeUsersMAUAbsolute")
        if val is None or val == "":
            return None
        if isinstance(val, (int, float)):
            return float(val)
        s = str(val).strip()
        if not s:
            return None
        try:
            return float(s)
        except ValueError:
            return None
    return None


def extract_wau_absolute_from_facets_v2_rows(
    facet_rows: list[dict[str, Any]],
) -> float | None:
    """Extract `activeUsersWAUAbsolute` from the unified row (`appId is None`)."""
    for row in facet_rows:
        if row.get("appId") is not None:
            continue
        val = row.get("activeUsersWAUAbsolute")
        if val is None or val == "":
            return None
        if isinstance(val, (int, float)):
            return float(val)
        s = str(val).strip()
        if not s:
            return None
        try:
            return float(s)
        except ValueError:
            return None
    return None


def extract_unified_app_id_from_facets_v2_rows(
    facet_rows: list[dict[str, Any]],
) -> int | None:
    """Extract unifiedAppId from the unified row (`appId is None`)."""
    for row in facet_rows:
        if row.get("appId") is not None:
            continue
        uid = row.get("unifiedAppId")
        if uid is None:
            continue
        if isinstance(uid, int):
            return uid
        if isinstance(uid, str) and uid.strip().isdigit():
            return int(uid.strip())
    return None


def extract_total_revenue_absolute_from_facets_v2_rows(
    facet_rows: list[dict[str, Any]],
) -> float | None:
    """Sum `revenueAbsolute` (USD) across unified rows (`appId is None`).

    Sensor Tower facets v2 commonly returns revenue in cents; we normalize to USD.
    """
    total = 0.0
    seen_any = False
    for row in facet_rows:
        if row.get("appId") is not None:
            continue
        val = row.get("revenueAbsolute")
        if val is None or val == "":
            continue
        seen_any = True
        if isinstance(val, (int, float)):
            total += float(val) / 100.0
            continue
        s = str(val).strip()
        if not s:
            continue
        try:
            total += float(s) / 100.0
        except ValueError:
            continue
    if not seen_any:
        return None
    return total


def extract_total_revenue_absolute_any_from_facets_v2_rows(
    facet_rows: list[dict[str, Any]],
) -> float | None:
    """Sum `revenueAbsolute` (USD) across all rows.

    Fallback when unified (appId=None) aggregation rows are missing.
    """
    total = 0.0
    seen_any = False
    for row in facet_rows:
        val = row.get("revenueAbsolute")
        if val is None or val == "":
            continue
        seen_any = True
        if isinstance(val, (int, float)):
            total += float(val) / 100.0
            continue
        s = str(val).strip()
        if not s:
            continue
        try:
            total += float(s) / 100.0
        except ValueError:
            continue
    if not seen_any:
        return None
    return total


def extract_first_release_date_us_from_facets_v2_rows(
    facet_rows: list[dict[str, Any]],
) -> str | None:
    """Extract first release date (US) from facets v2 unified row.

    We prefer `earliestReleaseDate` when present, then fall back to `releaseDate`.
    Both are returned as ISO-ish strings by Sensor Tower (often with timezone suffix).
    """
    for row in facet_rows:
        if row.get("appId") is not None:
            continue
        for key in ("earliestReleaseDate", "releaseDate"):
            v = row.get(key)
            if v is None:
                continue
            s = str(v).strip()
            if s:
                return s
    return None


def format_date(d: date) -> str:
    """Sensor Tower API expects `YYYY-MM-DD`."""
    return d.strftime("%Y-%m-%d")


def get_app_comments(
    client: httpx.Client,
    *,
    ios_app_id: int | str | None,
    android_app_id: str | None,
    start_date: date,
    end_date: date,
    limit: int = 20,
    csrf_token: str | None = None,
) -> list[dict[str, Any]]:
    """Fetch recent Sensor Tower app review texts (first page)."""
    device_type: str | None = "ios" if ios_app_id is not None else "android"
    if device_type == "ios" and ios_app_id is None:
        device_type = None
    if device_type == "android" and android_app_id is None:
        device_type = None
    if device_type is None:
        return []

    app_id_str = str(ios_app_id if device_type == "ios" else android_app_id)

    ios_params: dict[str, Any] = {
        "app_id": app_id_str,
        "start_date": format_date(start_date),
        "end_date": format_date(end_date),
        "rating_filters": [1, 2, 3, 4, 5],
        "search_terms": [],
        "content_keywords": [],
        "tags": [],
        "versions": [],
        "sentiments": ["happy", "mixed", "neutral", "unhappy"],
        "sort_by": "date",
        "sort_order": "desc",
        "limit": limit,
        "page": 1,
        "exclude_rating_breakdown": "true",
        "countries": ["US"],
    }
    android_params: dict[str, Any] = {
        "app_id": app_id_str,
        "start_date": format_date(start_date),
        "end_date": format_date(end_date),
        "rating_filters": [1, 2, 3, 4, 5],
        "search_terms": [],
        "content_keywords": [],
        "tags": [],
        "versions": [],
        "sentiments": ["happy", "mixed", "neutral", "unhappy"],
        "sort_by": "date",
        "sort_order": "desc",
        "limit": limit,
        "page": 1,
        "exclude_rating_breakdown": "true",
        "languages": [
            "AR",
            "AZ",
            "BG",
            "CS",
            "DA",
            "DE",
            "EL",
            "EN",
            "ES",
            "ET",
            "FI",
            "FR",
            "HE",
            "HI",
            "HR",
            "HU",
            "ID",
            "IT",
            "JA",
            "KK",
            "KO",
            "LO",
            "LT",
            "LV",
            "MS",
            "MY",
            "NL",
            "NO",
            "PL",
            "PT",
            "RO",
            "RU",
            "SK",
            "SL",
            "SR",
            "SV",
            "SW",
            "TH",
            "TR",
            "UK",
            "VI",
            "ZH",
        ],
    }

    params = ios_params if device_type == "ios" else android_params
    headers: dict[str, str] = dict(POST_JSON_HEADERS)
    if csrf_token:
        headers["x-csrf-token"] = csrf_token
    r = client.post(
        f"/api/{device_type}/review/get_reviews",
        json=params,
        headers=headers,
    )
    data = _parse_json_response(r)
    feedbacks = data.get("feedback", []) if isinstance(data, dict) else []
    if not isinstance(feedbacks, list):
        return []

    out: list[dict[str, Any]] = []
    for feedback in feedbacks:
        if not isinstance(feedback, dict):
            continue
        out.append(
            {
                "id": feedback.get("id", ""),
                "app_id": feedback.get("app_id", ""),
                "title": feedback.get("title", ""),
                "username": feedback.get("username", ""),
                "country": feedback.get("country", ""),
                "sentiment": feedback.get("sentiment", ""),
                "rating": feedback.get("rating", ""),
                "tags": feedback.get("tags", ""),
                "content": feedback.get("content", ""),
                "created_at": feedback.get("date") or feedback.get("created_at") or None,
            }
        )
    return out


def extract_revenue_absolute_from_facets_rows(
    facet_rows: list[dict[str, Any]],
) -> float | None:
    """Extract `revenue` absolute for the unified (app_id=None) row."""
    for row in facet_rows:
        if row.get("app_id") is not None:
            continue
        for single in row.get("facets", []):
            if single.get("measure") == "revenue" and single.get("type") == "absolute":
                val = single.get("value")
                if val is None or val == "":
                    return None
                try:
                    return float(val)
                except (TypeError, ValueError):
                    return None
    # Fallback: best-effort first revenue absolute facet.
    for row in facet_rows:
        for single in row.get("facets", []):
            if single.get("measure") == "revenue" and single.get("type") == "absolute":
                val = single.get("value")
                if val is None or val == "":
                    return None
                try:
                    return float(val)
                except (TypeError, ValueError):
                    return None
    return None


def extract_unified_app_id_from_facets_rows(
    facet_rows: list[dict[str, Any]],
) -> int | None:
    """Extract numeric `unified_app_id` from the unified row (app_id=None)."""
    for row in facet_rows:
        if row.get("app_id") is not None:
            continue
        uid = row.get("unified_app_id")
        if isinstance(uid, int):
            return uid
        if isinstance(uid, str) and uid.strip().isdigit():
            return int(uid.strip())
    return None


def extract_release_date_us_from_facets_rows(
    facet_rows: list[dict[str, Any]],
) -> str | None:
    """Extract `Release Date (US)` from facets rows for the unified row."""
    for row in facet_rows:
        if row.get("app_id") is not None:
            continue
        for single in row.get("facets", []):
            if (
                single.get("type") == "custom"
                and single.get("name") == "Release Date (US)"
            ):
                val = single.get("value")
                if val is None or val == "":
                    return None
                return str(val)
    return None


def month_ranges_last_n_months(n: int, *, end: date | None = None) -> list[tuple[date, date]]:
    """Return list of (start, end) per calendar month, newest first."""
    if end is None:
        end = date.today()
    cursor = date(end.year, end.month, 1)
    ranges: list[tuple[date, date]] = []
    for _ in range(n):
        start = cursor
        if cursor.month == 12:
            next_month = date(cursor.year + 1, 1, 1)
        else:
            next_month = date(cursor.year, cursor.month + 1, 1)
        last = next_month - timedelta(days=1)
        ranges.append((start, last))
        if cursor.month == 1:
            cursor = date(cursor.year - 1, 12, 1)
        else:
            cursor = date(cursor.year, cursor.month - 1, 1)
    return ranges


def extract_store_hints(text: str) -> dict[str, str | None]:
    """Parse App Store / Play Store URLs for ids (search hints only)."""
    t = text.strip()
    out: dict[str, str | None] = {
        "ios_numeric_id": None,
        "android_package": None,
        "ios_slug": None,
    }
    if "apps.apple.com" in t or "itunes.apple.com" in t:
        # Prefer /app/<slug>/id<n>: slug search returns unified autocomplete; raw id often does not.
        m_slug = re.search(r"/app/([^/]+)/id(\d+)", t, re.IGNORECASE)
        if m_slug:
            out["ios_slug"] = m_slug.group(1)
            out["ios_numeric_id"] = m_slug.group(2)
        else:
            m = re.search(r"/id(\d+)", t)
            if m:
                out["ios_numeric_id"] = m.group(1)
    if "play.google.com" in t:
        q = urllib.parse.urlparse(t)
        qs = urllib.parse.parse_qs(q.query)
        if "id" in qs:
            out["android_package"] = qs["id"][0]
        else:
            m = re.search(r"id=([\w.]+)", t)
            if m:
                out["android_package"] = m.group(1)
    return out


def pick_unified_id_from_apps(apps_payload: list[dict[str, Any]]) -> str | None:
    """Return unified app id string from ``internal_entities`` app object."""
    for app in apps_payload:
        uid = app.get("app_id") or app.get("id") or app.get("unified_app_id")
        if uid is not None:
            return str(uid)
    return None


def extract_revenue_absolute_for_unified(
    facet_rows: list[dict[str, Any]], unified_target: str
) -> float | None:
    """Parse one ``/api/apps/facets`` response row for ``revenue`` absolute."""
    for row in facet_rows:
        if str(row.get("unified_app_id", "")) != unified_target:
            continue
        if row.get("app_id") is not None:
            continue
        for single in row.get("facets", []):
            if single.get("measure") == "revenue" and single.get("type") == "absolute":
                val = single.get("value")
                if val is None or val == "":
                    return None
                try:
                    return float(val)
                except (TypeError, ValueError):
                    return None
    return None
