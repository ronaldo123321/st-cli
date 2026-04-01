"""Render competitive landscape markdown from structured JSON data."""

from __future__ import annotations

import html
import re
from datetime import date, datetime
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


def _clip_sentence(text: str, max_len: int = 200) -> str:
    s = " ".join(text.split())
    if len(s) <= max_len:
        return s
    return s[: max_len - 1].rstrip() + "…"


def _clean_snippet(text: str) -> str:
    s = html.unescape(_normalize_text(text))
    s = re.sub(r"<[^>]+>", " ", s)
    return _clip_sentence(s, 220)


def _parse_iso_date(v: Any) -> date | None:
    s = _normalize_text(v)
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s).date()
    except ValueError:
        return None


def _format_date(v: date | None) -> str:
    return v.isoformat() if v else "N/A"


def _sparkline(values: list[float]) -> str:
    if not values:
        return ""
    vmin = min(values)
    vmax = max(values)
    levels = " .:-=+*#%@"
    if vmax == vmin:
        return levels[len(levels) // 2] * len(values)
    span = vmax - vmin
    last_idx = len(levels) - 1
    chars: list[str] = []
    for v in values:
        idx = int((v - vmin) / span * last_idx)
        idx = max(0, min(last_idx, idx))
        chars.append(levels[idx])
    return "".join(chars)


def _ai_powered_label(ai_label: str) -> str:
    if ai_label == "AI-enabled":
        return "Yes"
    if ai_label == "No AI features":
        return "No"
    return "Unclear"


def render_landscape_report_md(*, source: dict[str, Any], competitors: list[dict[str, Any]]) -> str:
    """Render markdown report for `st landscape --out`.

    Args:
        source: `data.source` object from `st landscape` JSON.
        competitors: `data.competitors` array from `st landscape` JSON.
    """
    month = _normalize_text(source.get("month"))
    as_of = _normalize_text(source.get("as_of"))
    regions = source.get("facet_regions", [])
    regions_str = (
        "global"
        if isinstance(regions, list) and len(regions) > 20
        else ", ".join(str(r) for r in (regions or []))
    )

    ranked: list[tuple[float | None, dict[str, Any]]] = []
    for it in competitors:
        st = it.get("st") if isinstance(it, dict) else None
        rev = None
        if isinstance(st, dict):
            v = st.get("revenue_as_of_current_month_usd")
            if isinstance(v, (int, float)):
                rev = float(v)
        ranked.append((rev, it))
    ranked.sort(key=lambda x: (x[0] is None, -(x[0] or 0.0)))

    market_proxy: float | None = None
    for _, it in ranked:
        st = it.get("st") if isinstance(it, dict) else None
        if not isinstance(st, dict):
            continue
        ms = st.get("market_share_as_of_current_month")
        if not isinstance(ms, dict):
            continue
        v = ms.get("market_revenue_absolute_usd")
        if isinstance(v, (int, float)) and float(v) > 0:
            market_proxy = float(v)
            break

    def _key_point(points: list[str]) -> str:
        if not points:
            return "N/A"
        p = points[0]
        for prefix in ("Reddit:", "App store (sample):"):
            if p.startswith(prefix):
                p = p[len(prefix) :].strip()
        return _clip_sentence(p, 120)

    lines: list[str] = []
    lines.append(f"# Competitive Landscape — {month} Mobile Revenue (Global, as-of {as_of})")
    lines.append("")
    lines.append("## Overview")
    lines.append("")
    lines.append("### Market size")
    lines.append(f"- **Total market revenue (proxy)**: {_money_compact_usd(market_proxy)} (Top N proxy)")
    lines.append(f"- **As-of window**: {month} (as-of {as_of})")
    lines.append(f"- **Regions**: {regions_str}")
    lines.append("")
    lines.append("### Competitive Landscape")
    lines.append("")
    lines.append(
        "| # | Product | B2B/B2C | AI-Powered | "
        f"{month} Revenue | 6M Growth | First Release | Share (as-of month) | Key Strength | Key Weakness |"
    )
    lines.append("|---:|---|---|---|---|---|---|---|---|")

    computed: list[dict[str, Any]] = []
    for idx, (rev, it) in enumerate(ranked, start=1):
        name = _normalize_text(it.get("name"))
        st = it.get("st") if isinstance(it.get("st"), dict) else {}
        err = it.get("error")
        selected = st.get("selected") if isinstance(st.get("selected"), dict) else None
        comments = st.get("comments") if isinstance(st.get("comments"), list) else []

        share_percent = None
        ms = st.get("market_share_as_of_current_month")
        if isinstance(ms, dict):
            sp = ms.get("share_percent")
            if isinstance(sp, (int, float)):
                share_percent = float(sp)

        product_cell = name
        if err:
            product_cell = f"{name} (fetch failed)"

        lines.append(
            "| "
            + " | ".join(
                [
                    str(idx),
                    product_cell,
                    _normalize_text(it.get("segment")),
                    _ai_powered_label(_normalize_text(it.get("ai_label"))),
                    _money_compact_usd(rev),
                    _normalize_text(it.get("growth_6m_label")),
                    _format_date(_parse_iso_date(_normalize_text(st.get("first_release_date_us")))),
                    f"{share_percent:.2f}%" if share_percent is not None else "N/A",
                    _key_point(it.get("strengths") if isinstance(it.get("strengths"), list) else []),
                    _key_point(it.get("weaknesses") if isinstance(it.get("weaknesses"), list) else []),
                ]
            )
            + " |"
        )

        computed.append(
            {
                "idx": idx,
                "name": name,
                "rev": rev,
                "share_percent": share_percent,
                "selected": selected,
                "comments": comments,
                "monthly_estimates": st.get("monthly_estimates", []) if isinstance(st, dict) else [],
                "strengths": it.get("strengths") if isinstance(it.get("strengths"), list) else [],
                "weaknesses": it.get("weaknesses") if isinstance(it.get("weaknesses"), list) else [],
                "ai_label": _normalize_text(it.get("ai_label")),
                "segment": _normalize_text(it.get("segment")),
                "core_review": _normalize_text(it.get("core_review")),
                "caveat": _normalize_text(it.get("caveat")),
                "growth_6m_label": _normalize_text(it.get("growth_6m_label")),
                "error": err,
                "st": st,
            }
        )

    lines.append("")

    for it in computed:
        name = it["name"]
        idx = it["idx"]
        rev = it["rev"]
        share_percent = it["share_percent"]
        ai_label = it["ai_label"]
        segment = it["segment"]
        selected = it["selected"] or {}
        caveat = it["caveat"]
        st = it["st"] or {}

        lines.append(f"## {idx}. {name}")
        if it["error"]:
            lines.append("")
            lines.append(f"Fetch failed: `{_normalize_text(it['error'])[:200]}`")
            lines.append("")
            continue

        lines.append("")
        lines.append(f"- **{month} Revenue (as-of)**: {_money_compact_usd(rev)}")
        lines.append(f"- **6M Growth**: {_normalize_text(it.get('growth_6m_label'))}")
        lines.append(f"- **Market share**: {f'{share_percent:.2f}%' if share_percent is not None else 'N/A'}")
        lines.append(f"- **First release**: {_format_date(_parse_iso_date(_normalize_text(st.get('first_release_date_us'))))}")
        lines.append(f"- **Focus**: {segment}")
        lines.append(f"- **AI**: {ai_label}")
        pub = _normalize_text(selected.get("publisher_name"))
        if pub:
            lines.append(f"- **Publisher**: {pub}")
        lines.append("")
        lines.append("**What it does**")
        lines.append("")
        what = _clean_snippet(_normalize_text(it.get("core_review"))) or "N/A"
        lines.append(f"{what}")
        if caveat:
            lines.append("")
            lines.append(f"**Caveat**: {caveat}")

        lines.append("")
        lines.append("**Notes (evidence)**")
        lines.append("")
        title = _normalize_text(selected.get("humanized_name") or selected.get("name"))
        if title:
            lines.append(f"- SensorTower resolved app: {title}")
        monthly = it.get("monthly_estimates", [])
        if isinstance(monthly, list) and monthly:
            pts: list[float] = []
            for row in monthly[:6]:
                if not isinstance(row, dict):
                    continue
                val = row.get("revenue_absolute_usd")
                if isinstance(val, (int, float)):
                    pts.append(float(val))
                    continue
                if val is None:
                    continue
                try:
                    pts.append(float(str(val)))
                except ValueError:
                    continue
            if pts:
                lines.append(f"- Trend (6m sparkline): {_sparkline(pts)}")
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"

