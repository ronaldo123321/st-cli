"""Render snapshot markdown from structured JSON data."""

from __future__ import annotations

import html
import re
from typing import Any


def _normalize_text(v: Any) -> str:
    if v is None:
        return ""
    return str(v).strip()


def _money_compact_usd(v: float | None) -> str:
    if v is None:
        return "N/A"
    n = float(v)
    if n >= 1_000_000_000:
        return f"${n / 1_000_000_000:.2f}B"
    if n >= 1_000_000:
        return f"${n / 1_000_000:.2f}M"
    if n >= 1_000:
        return f"${n / 1_000:.2f}K"
    return f"${n:.0f}"


def _compact_count(v: float | int | None) -> str:
    if v is None:
        return "N/A"
    try:
        n = int(round(float(v)))
    except (TypeError, ValueError):
        return "N/A"
    return f"{n:,}"


def _format_percent(v: float | None) -> str:
    if v is None:
        return "N/A"
    try:
        return f"{float(v):.2f}%"
    except (TypeError, ValueError):
        return "N/A"


def _clip_sentence(text: str, max_len: int = 180) -> str:
    s = " ".join(text.split())
    if len(s) <= max_len:
        return s
    return s[: max_len - 1].rstrip() + "…"


def _clean_snippet(text: str) -> str:
    s = html.unescape(_normalize_text(text))
    s = re.sub(r"<[^>]+>", " ", s)
    return _clip_sentence(s, 220)


def _entry_from_raw_item(item: dict[str, Any]) -> dict[str, Any]:
    snapshot = item.get("snapshot") if isinstance(item.get("snapshot"), dict) else {}
    market_share = (
        item.get("market_share_in_window")
        if isinstance(item.get("market_share_in_window"), dict)
        else {}
    )
    comments = item.get("comments") if isinstance(item.get("comments"), list) else []
    versions = item.get("versions") if isinstance(item.get("versions"), list) else []
    version_timeline = (
        item.get("version_timeline") if isinstance(item.get("version_timeline"), dict) else {}
    )
    selected = item.get("selected") if isinstance(item.get("selected"), dict) else {}
    name = (
        _normalize_text(selected.get("humanized_name"))
        or _normalize_text(selected.get("name"))
        or _normalize_text(item.get("query"))
    )
    return {
        "name": name,
        "first_release_date_us": _normalize_text(item.get("first_release_date_us")),
        "revenue_usd": snapshot.get("revenue_usd"),
        "revenue_growth_percent": snapshot.get("revenue_growth_vs_previous_window_percent"),
        "downloads_absolute": snapshot.get("downloads_absolute"),
        "downloads_growth_percent": snapshot.get("downloads_growth_vs_previous_window_percent"),
        "mau_absolute": snapshot.get("mau_absolute"),
        "wau_absolute": snapshot.get("wau_absolute"),
        "wau_growth_percent": snapshot.get("wau_growth_vs_previous_window_percent"),
        "market_share_percent": market_share.get("share_percent"),
        "review_count": len(comments),
        "comments": comments,
        "versions": versions,
        "version_timeline": version_timeline,
        "warnings": item.get("warnings") if isinstance(item.get("warnings"), list) else [],
        "error": None,
    }


def _entry_from_landscape_competitor(item: dict[str, Any]) -> dict[str, Any]:
    st = item.get("st") if isinstance(item.get("st"), dict) else {}
    downloads = st.get("downloads_in_window") if isinstance(st.get("downloads_in_window"), dict) else {}
    mau = st.get("mau_in_window") if isinstance(st.get("mau_in_window"), dict) else {}
    wau = st.get("wau_in_window") if isinstance(st.get("wau_in_window"), dict) else {}
    market_share = (
        st.get("market_share_in_window")
        if isinstance(st.get("market_share_in_window"), dict)
        else {}
    )
    comments = st.get("reviews_in_window") if isinstance(st.get("reviews_in_window"), list) else []
    versions = st.get("versions") if isinstance(st.get("versions"), list) else []
    version_timeline = st.get("version_timeline") if isinstance(st.get("version_timeline"), dict) else {}
    error = item.get("error") if isinstance(item.get("error"), dict) else None
    return {
        "name": _normalize_text(item.get("name")),
        "first_release_date_us": _normalize_text(st.get("first_release_date_us")),
        "revenue_usd": st.get("revenue_in_window_usd"),
        "revenue_growth_percent": st.get("revenue_growth_vs_previous_window_percent"),
        "downloads_absolute": downloads.get("downloads_absolute"),
        "downloads_growth_percent": downloads.get("growth_vs_previous_window_percent"),
        "mau_absolute": mau.get("mau_absolute"),
        "wau_absolute": wau.get("wau_absolute"),
        "wau_growth_percent": wau.get("growth_vs_previous_window_percent"),
        "market_share_percent": market_share.get("share_percent"),
        "review_count": len(comments),
        "comments": comments,
        "versions": versions,
        "version_timeline": version_timeline,
        "warnings": st.get("warnings") if isinstance(st.get("warnings"), list) else [],
        "error": error,
    }


def render_snapshot_report_md(
    *,
    source: dict[str, Any],
    raw_items: list[dict[str, Any]],
    raw_errors: list[dict[str, Any]],
    landscape_items: list[dict[str, Any]],
) -> str:
    """Render markdown report for ``st snapshot-report``."""
    start_date = _normalize_text(source.get("start_date"))
    end_date = _normalize_text(source.get("end_date"))
    shape = _normalize_text(source.get("shape")) or "unknown"
    regions = source.get("facet_regions", [])
    regions_str = ", ".join(str(r) for r in regions) if isinstance(regions, list) else _normalize_text(regions)

    entries = (
        [_entry_from_landscape_competitor(it) for it in landscape_items]
        if landscape_items
        else [_entry_from_raw_item(it) for it in raw_items]
    )
    entries.sort(
        key=lambda it: (
            it.get("error") is not None,
            -(float(it["revenue_usd"]) if isinstance(it.get("revenue_usd"), (int, float)) else 0.0),
        )
    )

    if landscape_items:
        failed = [it for it in entries if it.get("error") is not None]
    else:
        failed = [
            {
                "name": _normalize_text(it.get("name")) or _normalize_text(it.get("query")),
                "error": {
                    "code": _normalize_text(it.get("code")),
                    "message": _normalize_text(it.get("message")),
                },
            }
            for it in raw_errors
        ]

    lines: list[str] = []
    lines.append(f"# Snapshot Summary — {start_date} to {end_date}")
    lines.append("")
    lines.append("## Overview")
    lines.append("")
    lines.append(f"- **Window**: {start_date} to {end_date}")
    lines.append(f"- **Regions**: {regions_str or 'N/A'}")
    lines.append(f"- **Input shape**: {shape}")
    lines.append(f"- **Successful items**: {sum(1 for it in entries if it.get('error') is None)}")
    lines.append(f"- **Failed items**: {len(failed)}")
    lines.append("")
    lines.append("## Results")
    lines.append("")
    lines.append(
        "| # | Product | Revenue | Revenue Growth | Downloads | WAU | Market Share | First Release | Reviews |"
    )
    lines.append("|---:|---|---|---|---|---|---|---|---:|")

    idx = 0
    for item in entries:
        if item.get("error") is not None:
            continue
        idx += 1
        lines.append(
            "| "
            + " | ".join(
                [
                    str(idx),
                    _normalize_text(item.get("name")) or "Unknown",
                    _money_compact_usd(item.get("revenue_usd")),
                    _format_percent(item.get("revenue_growth_percent")),
                    _compact_count(item.get("downloads_absolute")),
                    _compact_count(item.get("wau_absolute")),
                    _format_percent(item.get("market_share_percent")),
                    _normalize_text(item.get("first_release_date_us")) or "N/A",
                    str(item.get("review_count") or 0),
                ]
            )
            + " |"
        )

    if idx == 0:
        lines.append("| 1 | No successful items | N/A | N/A | N/A | N/A | N/A | N/A | 0 |")

    lines.append("")
    if failed:
        lines.append("## Failed Lookups")
        lines.append("")
        for item in failed:
            err = item.get("error") if isinstance(item.get("error"), dict) else {}
            code = _normalize_text(err.get("code")) or "unknown_error"
            message = _normalize_text(err.get("message")) or "Unknown error"
            lines.append(f"- **{_normalize_text(item.get('name')) or 'Unknown'}**: `{code}` - {message}")
        lines.append("")

    review_entries = [it for it in entries if it.get("error") is None and it.get("comments")]
    if review_entries:
        lines.append("## Review Samples")
        lines.append("")
        for item in review_entries:
            comments = item.get("comments") if isinstance(item.get("comments"), list) else []
            first = comments[0] if comments else {}
            content = _clean_snippet(first.get("content")) if isinstance(first, dict) else ""
            if not content:
                continue
            lines.append(f"### {_normalize_text(item.get('name'))}")
            lines.append("")
            lines.append(f"- {_normalize_text(content)}")
            lines.append("")

    version_entries = [
        it
        for it in entries
        if it.get("error") is None
        and isinstance(it.get("versions"), list)
        and len(it["versions"]) > 0
    ]
    if version_entries:
        lines.append("## Recent version updates")
        lines.append("")
        lines.append(
            "Rows are from the store update timeline, filtered to the age window in "
            "`version_timeline` (default:30 days ending at snapshot `end_date`, storefront "
            "`US`)."
        )
        lines.append("")
        for item in version_entries:
            versions = item.get("versions") if isinstance(item.get("versions"), list) else []
            meta = item.get("version_timeline") if isinstance(item.get("version_timeline"), dict) else {}
            plat = _normalize_text(meta.get("platform")) or "unknown"
            country = _normalize_text(meta.get("country")) or "US"
            lines.append(f"### {_normalize_text(item.get('name'))}")
            lines.append("")
            lines.append(f"- **Platform**: {plat} · **Storefront**: {country}")
            lines.append("")
            for row in versions[:8]:
                if not isinstance(row, dict):
                    continue
                when = _normalize_text(row.get("time")) or "N/A"
                ver = _normalize_text(row.get("version")) or "—"
                note = row.get("featured_user_feedback")
                note_s = _clean_snippet(note) if isinstance(note, str) and note.strip() else ""
                if note_s:
                    lines.append(f"- `{when}` **{ver}** — {note_s}")
                else:
                    lines.append(f"- `{when}` **{ver}**")
            lines.append("")

    return "\n".join(lines).rstrip() + "\n"
