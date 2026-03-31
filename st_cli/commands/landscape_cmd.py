"""st landscape — build competitive landscape inputs from rdt-cli-research report."""

import html
import logging
import re
from datetime import datetime
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import click

from st_cli.auth import get_credential
from st_cli.constants import CREDENTIAL_FILE, DEFAULT_FACET_REGIONS
from st_cli.output import error_payload, print_payload, success_payload
from st_cli.pipeline import PipelineFailure, PipelineSuccess, run_fetch_pipeline
from st_cli.st_client import create_st_client

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CompetitorRow:
    name: str
    mentions: int | None
    positive: int | None
    negative: int | None
    core_review: str


def _normalize_text(v: Any) -> str:
    if v is None:
        return ""
    return str(v).strip()


def _parse_int(s: str) -> int | None:
    v = s.strip()
    if not v:
        return None
    if v.isdigit():
        return int(v)
    return None


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


def _shift_month_key(month_key: str, delta_months: int) -> str | None:
    """Shift a YYYY-MM string by delta months."""
    s = _normalize_text(month_key)
    if len(s) != 7 or s[4] != "-":
        return None
    try:
        y = int(s[:4])
        m = int(s[5:])
    except ValueError:
        return None
    if m < 1 or m > 12:
        return None
    idx = (y * 12 + (m - 1)) + delta_months
    if idx < 0:
        return None
    ny = idx // 12
    nm = (idx % 12) + 1
    return f"{ny:04d}-{nm:02d}"


def _extract_month_value(monthly: list[dict[str, Any]], month_key: str) -> float | None:
    for it in monthly:
        if not isinstance(it, dict):
            continue
        if _normalize_text(it.get("month")) != month_key:
            continue
        val = it.get("revenue_absolute_usd")
        if isinstance(val, (int, float)):
            return float(val)
        if val is None:
            return None
        try:
            return float(str(val))
        except ValueError:
            return None
    return None


def _format_growth_ratio(cur: float | None, prev: float | None) -> str:
    if cur is None or prev is None or prev <= 0:
        return "N/A"
    ratio = cur / prev
    return f"{ratio:.2f}x"


def _parse_iso_date(v: Any) -> date | None:
    s = _normalize_text(v)
    if not s:
        return None
    # Common ST shapes:
    # - "2012-11-13T08:00:00Z"
    # - "2013-05-29T00:00:00Z"
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


def _review_summary(comments: list[dict[str, Any]]) -> str:
    if not comments:
        return "N/A"
    ratings: list[float] = []
    for c in comments:
        if not isinstance(c, dict):
            continue
        r = c.get("rating")
        if isinstance(r, (int, float)):
            ratings.append(float(r))
        else:
            s = _normalize_text(r)
            if s.isdigit():
                ratings.append(float(int(s)))
    avg = sum(ratings) / len(ratings) if ratings else None
    # pick a representative snippet: unhappy > otherwise first
    pick = None
    for c in comments:
        if isinstance(c, dict) and str(c.get("sentiment", "")).lower() == "unhappy":
            pick = c
            break
    if pick is None:
        pick = comments[0] if isinstance(comments[0], dict) else None
    snippet = ""
    if isinstance(pick, dict):
        snippet = _clean_snippet(_normalize_text(pick.get("content")))
    avg_part = f"{avg:.1f}/5" if avg is not None else "N/A"
    if snippet:
        return f"avg(sample)={avg_part}; \"{_clip_sentence(snippet, 90)}\""
    return f"avg(sample)={avg_part}"

def _clip_sentence(text: str, max_len: int = 200) -> str:
    s = " ".join(text.split())
    if len(s) <= max_len:
        return s
    return s[: max_len - 1].rstrip() + "…"


def _clean_snippet(text: str) -> str:
    s = html.unescape(_normalize_text(text))
    s = re.sub(r"<[^>]+>", " ", s)
    return _clip_sentence(s, 220)


_NEGATIVE_WORDS = (
    "worst",
    "terrible",
    "awful",
    "hate",
    "scam",
    "refund",
    "rip off",
    "ripoff",
    "charged",
    "bug",
    "crash",
    "broken",
    "doesn't work",
    "does not work",
    "can't log",
    "cannot log",
    "难用",
    "垃圾",
    "骗",
)
_POSITIVE_WORDS = (
    "great",
    "love",
    "best",
    "easy",
    "helpful",
    "recommend",
    "awesome",
    "good",
    "nice",
    "好用",
    "推荐",
    "喜欢",
)


def _text_looks_negative(text: str) -> bool:
    low = text.lower()
    return any(w in low for w in _NEGATIVE_WORDS)


def _text_looks_positive(text: str) -> bool:
    low = text.lower()
    return any(w in low for w in _POSITIVE_WORDS)


def _rdt_looks_finance_smallbiz(core_review: str) -> bool:
    low = core_review.lower()
    keys = (
        "invoice",
        "expense",
        "payroll",
        "accounting",
        "bookkeeping",
        "receipt",
        "tax",
        "mileage",
        "client",
        "business",
        "quickbook",
        "xero",
        "wave",
        "freshbook",
        "expensify",
    )
    return any(k in low for k in keys)


def _st_comments_look_media_game(bodies_joined: str) -> bool:
    low = bodies_joined.lower()
    keys = (
        "episode",
        "drama",
        "vip",
        "coins",
        "unlock",
        "story",
        "watching",
        "weekly stream",
        "game",
    )
    hits = sum(1 for k in keys if k in low)
    return hits >= 2


def _vertical_mismatch_rdt_vs_st(core_review: str, comments: list[dict[str, Any]]) -> bool:
    """True when Reddit quote is finance-ish but ST samples read like entertainment/game."""
    if not _rdt_looks_finance_smallbiz(core_review):
        return False
    bodies = " ".join(
        _normalize_text(c.get("content")) for c in comments if isinstance(c, dict)
    )
    if len(bodies) < 40:
        return False
    return _st_comments_look_media_game(bodies)


def _extract_strength_weakness_bullets(
    *,
    core_review: str,
    comments: list[dict[str, Any]],
    rdt_positive: int | None,
    rdt_negative: int | None,
) -> tuple[list[str], list[str]]:
    """Derive Key Strength/Weakness from ST user review sentiment only.

    `core_review` is only used to detect likely app mismatch and then we fallback to a generic warning.
    """
    if _vertical_mismatch_rdt_vs_st(core_review, comments):
        msg = "App-match uncertainty; verify SensorTower resolved app before using Key Strength/Key Weakness."
        return [msg], [msg]

    pos_comments: list[str] = []
    neg_comments: list[str] = []
    other_comments: list[str] = []

    for c in comments:
        if not isinstance(c, dict):
            continue
        body = _clean_snippet(_normalize_text(c.get("content")))
        if not body:
            continue
        sent = str(c.get("sentiment", "")).lower()
        if sent == "happy":
            pos_comments.append(body)
        elif sent == "unhappy":
            neg_comments.append(body)
        else:
            other_comments.append(body)

    strengths: list[str] = []
    weaknesses: list[str] = []

    for s in pos_comments[:2]:
        strengths.append(s)

    for s in neg_comments[:2]:
        weaknesses.append(s)

    if len(strengths) < 2:
        for s in other_comments:
            if _text_looks_positive(s) and not _text_looks_negative(s):
                strengths.append(s)
            if len(strengths) >= 2:
                break

    if len(weaknesses) < 2:
        for s in other_comments:
            if _text_looks_negative(s):
                weaknesses.append(s)
            if len(weaknesses) >= 2:
                break

    if not strengths:
        strengths = ["N/A"]
    if not weaknesses:
        weaknesses = ["N/A"]

    return strengths[:3], weaknesses[:3]


def _match_warning(query_name: str, selected: dict[str, Any] | None) -> str | None:
    """Warn when ST resolved app title likely differs from the rdt competitor string."""
    if not selected:
        return None
    title = _normalize_text(selected.get("humanized_name") or selected.get("name"))
    if not title:
        return None
    q = query_name.lower().strip()
    t = title.lower()
    if not q:
        return None
    if q in t or t in q:
        return None
    q_tokens = {x for x in re.split(r"[^a-z0-9]+", q) if len(x) > 2}
    t_tokens = {x for x in re.split(r"[^a-z0-9]+", t) if len(x) > 2}
    if q_tokens and q_tokens & t_tokens:
        return None
    return (
        "SensorTower autocomplete may not match this competitor name; verify the resolved app "
        f"({title}) before trusting revenue/share."
    )


def _classify_ai_label(*, name: str, core_review: str, comments: list[dict[str, Any]]) -> str:
    text = " ".join(
        [
            name,
            core_review,
            " ".join(_normalize_text(c.get("content")) for c in comments[:5]),
            " ".join(_normalize_text(c.get("title")) for c in comments[:5]),
        ]
    ).lower()
    ai_keywords = [
        "ai",
        "llm",
        "gpt",
        "assistant",
        "autopilot",
        "copilot",
        "智能",
        "自动生成",
        "自动分类",
        "transcribe",
        "transcription",
        "summarize",
        "summary",
        "chat",
    ]
    no_ai_keywords = ["no ai", "without ai", "not ai", "不含ai", "没有ai", "非ai"]
    if any(k in text for k in no_ai_keywords):
        return "No AI features"
    if any(k in text for k in ai_keywords):
        return "AI-enabled"
    return "AI-unclear"


def _classify_segment(*, name: str, core_review: str, comments: list[dict[str, Any]], selected: dict[str, Any] | None) -> str:
    base = " ".join(
        [
            name,
            core_review,
            " ".join(_normalize_text(c.get("content")) for c in comments[:5]),
            _normalize_text((selected or {}).get("publisher_name")),
        ]
    ).lower()
    b2b_keywords = [
        "invoice",
        "invoicing",
        "payroll",
        "bookkeeping",
        "accounting",
        "expense report",
        "reimburse",
        "receipt",
        "tax",
        "firm",
        "client",
        "team",
        "enterprise",
        "workflow",
        "approval",
        "compliance",
        "practice management",
        "engagement letter",
        "proposal",
        "ar ",
        "accounts receivable",
        "billing",
        "b2b",
        "报销",
        "发票",
        "会计",
        "账",
        "企业",
        "团队",
        "客户",
        "审批",
        "合规",
        "事务所",
    ]
    b2c_keywords = [
        "personal",
        "family",
        "learn",
        "hobby",
        "subscription",
        "streak",
        "kids",
        "b2c",
        "个人",
        "家庭",
        "学习",
        "订阅",
    ]
    b2b_score = sum(1 for k in b2b_keywords if k in base)
    b2c_score = sum(1 for k in b2c_keywords if k in base)
    if b2b_score > 0 and b2c_score > 0:
        return "Hybrid"
    if b2b_score > 0:
        return "B2B"
    if b2c_score > 0:
        return "B2C"
    return "Hybrid"


def _extract_competitor_table_rows(md: str) -> list[CompetitorRow]:
    lines = md.splitlines()

    # Find the section header containing "竞品生态".
    header_idx = -1
    for i, line in enumerate(lines):
        if "竞品生态" in line:
            header_idx = i
            break
    if header_idx < 0:
        return []

    # Find the first markdown table header row that contains "竞品" and "核心评价".
    table_start = -1
    for i in range(header_idx, len(lines)):
        line = lines[i].strip()
        if line.startswith("|") and "竞品" in line and "核心评价" in line:
            table_start = i
            break
    if table_start < 0:
        return []

    rows: list[CompetitorRow] = []
    for i in range(table_start + 1, len(lines)):
        line = lines[i].strip()
        if not line.startswith("|"):
            break
        # Skip separator row like |---|---|
        if set(line.replace("|", "").strip()) <= {"-", ":", " "}:
            continue
        parts = [p.strip() for p in line.strip("|").split("|")]
        if len(parts) < 5:
            continue
        name = parts[0]
        if not name:
            continue
        rows.append(
            CompetitorRow(
                name=name,
                mentions=_parse_int(parts[1]),
                positive=_parse_int(parts[2]),
                negative=_parse_int(parts[3]),
                core_review=parts[4],
            )
        )
    return rows


def _current_month_as_of() -> tuple[date, date]:
    today = date.today()
    month_start = today.replace(day=1)
    month_end = today - timedelta(days=2)
    if month_end < month_start:
        month_end = month_start
    return month_start, month_end


def _render_report_md(*, source: dict[str, Any], competitors: list[dict[str, Any]]) -> str:
    month = _normalize_text(source.get("month"))
    as_of = _normalize_text(source.get("as_of"))
    regions = source.get("facet_regions", [])
    regions_str = "global" if isinstance(regions, list) and len(regions) > 20 else ", ".join(str(r) for r in (regions or []))

    ranked = []
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
        # Strip prefixes like "Reddit:" / "App store (sample):"
        for prefix in ("Reddit:", "App store (sample):"):
            if p.startswith(prefix):
                p = p[len(prefix) :].strip()
        return _clip_sentence(p, 120)

    def _ai_powered_label(ai_label: str) -> str:
        if ai_label == "AI-enabled":
            return "Yes"
        if ai_label == "No AI features":
            return "No"
        return "Unclear"

    def _positive_sentiments(comments: list[dict[str, Any]]) -> list[str]:
        out: list[str] = []
        for c in comments:
            if not isinstance(c, dict):
                continue
            sent = str(c.get("sentiment", "")).lower()
            if sent != "happy":
                continue
            body = _clean_snippet(_normalize_text(c.get("content")))
            if body:
                out.append(body)
            if len(out) >= 3:
                break
        return out

    def _negative_sentiments(comments: list[dict[str, Any]]) -> list[str]:
        out: list[str] = []
        for c in comments:
            if not isinstance(c, dict):
                continue
            sent = str(c.get("sentiment", "")).lower()
            if sent != "unhappy":
                continue
            body = _clean_snippet(_normalize_text(c.get("content")))
            if body:
                out.append(body)
            if len(out) >= 3:
                break
        if out:
            return out
        # fallback
        for c in comments:
            if not isinstance(c, dict):
                continue
            body = _clean_snippet(_normalize_text(c.get("content")))
            if body and _text_looks_negative(body):
                out.append(body)
            if len(out) >= 2:
                break
        return out

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
        f"{month} Revenue | Past 6 Months Growth | Created | Share (as-of month) | Key Strength | Key Weakness |"
    )
    lines.append("|---:|---|---|---|---|---|---|---|---|")

    computed: list[dict[str, Any]] = []
    for idx, (rev, it) in enumerate(ranked, start=1):
        name = _normalize_text(it.get("name"))
        rdt = it.get("rdt") if isinstance(it.get("rdt"), dict) else {}
        st = it.get("st") if isinstance(it.get("st"), dict) else {}
        err = it.get("error")
        selected = st.get("selected") if isinstance(st.get("selected"), dict) else None
        comments = st.get("comments") if isinstance(st.get("comments"), list) else []
        core_review = _normalize_text(rdt.get("core_review"))
        ai_label = _classify_ai_label(name=name, core_review=core_review, comments=comments)
        segment = _classify_segment(name=name, core_review=core_review, comments=comments, selected=selected)
        strengths, weaknesses = _extract_strength_weakness_bullets(
            core_review=core_review,
            comments=comments,
            rdt_positive=rdt.get("positive") if isinstance(rdt.get("positive"), int) else None,
            rdt_negative=rdt.get("negative") if isinstance(rdt.get("negative"), int) else None,
        )
        caveat = _match_warning(name, selected)

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
                    segment,
                    _ai_powered_label(ai_label),
                    _money_compact_usd(rev),
                    _format_growth_ratio(
                        _extract_month_value(
                            st.get("monthly_estimates", []) if isinstance(st, dict) else [],
                            month,
                        ),
                        _extract_month_value(
                            st.get("monthly_estimates", []) if isinstance(st, dict) else [],
                            _shift_month_key(month, -6) or "",
                        ),
                    ),
                    _format_date(_parse_iso_date((selected or {}).get("release_date"))),
                    f"{share_percent:.2f}%" if share_percent is not None else "N/A",
                    _key_point(strengths),
                    _key_point(weaknesses),
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
                "ai_label": ai_label,
                "segment": segment,
                "core_review": core_review,
                "selected": selected,
                "comments": comments,
                "monthly_estimates": st.get("monthly_estimates", []) if isinstance(st, dict) else [],
                "strengths": strengths,
                "weaknesses": weaknesses,
                "positive_sentiments": _positive_sentiments(comments),
                "negative_sentiments": _negative_sentiments(comments),
                "caveat": caveat,
                "error": err,
                "rdt_mentions": rdt.get("mentions"),
                "rdt_positive": rdt.get("positive"),
                "rdt_negative": rdt.get("negative"),
            }
        )

    lines.append("")

    # Detailed sections
    for it in computed:
        name = it["name"]
        idx = it["idx"]
        rev = it["rev"]
        share_percent = it["share_percent"]
        ai_label = it["ai_label"]
        segment = it["segment"]
        selected = it["selected"] or {}
        core_review = it["core_review"]
        caveat = it["caveat"]

        lines.append(f"## {idx}. {name}")
        if it["error"]:
            lines.append("")
            lines.append(f"Fetch failed: `{_normalize_text(it['error'])[:200]}`")
            lines.append("")
            continue

        lines.append("")
        lines.append(f"- **{month} Revenue (as-of)**: {_money_compact_usd(rev)}")
        m6 = _shift_month_key(month, -6)
        if m6:
            cur_v = _extract_month_value(it.get("monthly_estimates", []), month)
            prev_v = _extract_month_value(it.get("monthly_estimates", []), m6)
            lines.append(f"- **Past 6 Months Growth**: {_format_growth_ratio(cur_v, prev_v)} (={month}/{m6})")
        lines.append(f"- **Market share**: {f'{share_percent:.2f}%' if share_percent is not None else 'N/A'}")
        created = _parse_iso_date(_normalize_text(selected.get("release_date")))
        lines.append(f"- **Created**: {_format_date(created)}")
        lines.append(f"- **Focus**: {segment}")
        lines.append(f"- **AI**: {ai_label}")
        pub = _normalize_text(selected.get("publisher_name"))
        if pub:
            lines.append(f"- **Publisher**: {pub}")
        lines.append("")
        lines.append("**What it does**")
        lines.append("")
        # Best-effort: use rdt core quote as short functional description.
        what = _clean_snippet(core_review) or "N/A"
        lines.append(f"{what}")

        if caveat:
            lines.append("")
            lines.append(f"**Caveat**: {caveat}")

        pos = it["positive_sentiments"]
        neg = it["negative_sentiments"]
        lines.append("")
        lines.append("**Positive user sentiment**")
        lines.append("")
        if pos:
            for b in pos:
                lines.append(f"- {_clip_sentence(b, 200)}")
        else:
            lines.append("- (no strong positive signal in sampled reviews)")

        lines.append("")
        lines.append("**Negative user sentiment**")
        lines.append("")
        if neg:
            for b in neg:
                lines.append(f"- {_clip_sentence(b, 200)}")
        else:
            lines.append("- (no strong negative signal in sampled reviews)")

        lines.append("")
        lines.append("**Notes (evidence)**")
        lines.append("")
        lines.append(
            f"- rdt mentions: {_normalize_text(it['rdt_mentions'])}, +{_normalize_text(it['rdt_positive'])}/-{_normalize_text(it['rdt_negative'])}"
        )
        title = _normalize_text(selected.get("humanized_name") or selected.get("name"))
        if title:
            lines.append(f"- SensorTower resolved app: {title}")
        # Keep a tiny trend hint in notes: sparkline over last 6 available points.
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


@click.command("landscape")
@click.option("--rdt-report", "rdt_report", type=click.Path(exists=True, dir_okay=False, path_type=Path), required=True)
@click.option("--limit", "limit", type=int, default=12, show_default=True, help="Max competitors to include")
@click.option("--category", "category", type=int, default=0, show_default=True, help="SensorTower category id for market share denominator (0=all apps)")
@click.option("--out", "out_path", type=click.Path(dir_okay=False, path_type=Path), default=None, help="Write final report markdown to this path")
@click.option("--json", "as_json", is_flag=True)
@click.option("--yaml", "as_yaml", is_flag=True)
def landscape(
    rdt_report: Path,
    limit: int,
    category: int,
    out_path: Path | None,
    as_json: bool,
    as_yaml: bool,
) -> None:
    """Prepare competitive landscape input data from a rdt-cli-research markdown report."""
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

    md = rdt_report.read_text(encoding="utf-8", errors="ignore")
    competitors = _extract_competitor_table_rows(md)
    if not competitors:
        print_payload(
            error_payload(
                "bad_request",
                "Could not find competitor table in rdt report (section '竞品生态').",
                {"rdt_report": str(rdt_report)},
            ),
            as_json=as_json,
            as_yaml=as_yaml,
        )
        raise SystemExit(1)

    month_start, month_end = _current_month_as_of()
    month_key = month_start.strftime("%Y-%m")

    out_rows: list[dict[str, Any]] = []
    with create_st_client(cred.cookies) as client:
        for row in competitors[: max(1, limit)]:
            try:
                res = run_fetch_pipeline(client, row.name, auto_pick_first=True, category=category)
            except RuntimeError as exc:
                logger.exception("fetch failed for %s", row.name)
                out_rows.append(
                    {
                        "name": row.name,
                        "rdt": {
                            "mentions": row.mentions,
                            "positive": row.positive,
                            "negative": row.negative,
                            "core_review": row.core_review,
                        },
                        "st": None,
                        "error": str(exc),
                    }
                )
                continue

            if isinstance(res, PipelineFailure):
                out_rows.append(
                    {
                        "name": row.name,
                        "rdt": {
                            "mentions": row.mentions,
                            "positive": row.positive,
                            "negative": row.negative,
                            "core_review": row.core_review,
                        },
                        "st": None,
                        "error": {"code": res.code, "message": res.message},
                    }
                )
                continue

            assert isinstance(res, PipelineSuccess)
            payload = res.payload
            monthly = (payload.get("revenue", {}) or {}).get("monthly_estimates", [])
            revenue_as_of_month: float | None = None
            if isinstance(monthly, list):
                for it in monthly:
                    if not isinstance(it, dict):
                        continue
                    if it.get("month") == month_key:
                        val = it.get("revenue_absolute_usd")
                        if isinstance(val, (int, float)):
                            revenue_as_of_month = float(val)
                        elif val is None:
                            revenue_as_of_month = None
                        else:
                            try:
                                revenue_as_of_month = float(str(val))
                            except ValueError:
                                revenue_as_of_month = None
                        break

            out_rows.append(
                {
                    "name": row.name,
                    "rdt": {
                        "mentions": row.mentions,
                        "positive": row.positive,
                        "negative": row.negative,
                        "core_review": row.core_review,
                    },
                    "st": {
                        "selected": payload.get("selected"),
                        "revenue_as_of_current_month_usd": revenue_as_of_month,
                        "market_share_as_of_current_month": payload.get("market_share_as_of_current_month"),
                        "comments": payload.get("comments", [])[:5],
                        "monthly_estimates": monthly if isinstance(monthly, list) else [],
                        "warnings": payload.get("warnings", []),
                    },
                    "error": None,
                }
            )

    data = {
        "source": {
            "rdt_report": str(rdt_report),
            "month": month_key,
            "as_of": month_end.isoformat(),
            "facet_regions": DEFAULT_FACET_REGIONS,
            "category": category,
        },
        "competitors": out_rows,
    }
    if out_path is not None:
        report_md = _render_report_md(source=data["source"], competitors=out_rows)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(report_md, encoding="utf-8")
        data["report"] = {"path": str(out_path)}
    print_payload(success_payload(data), as_json=as_json, as_yaml=as_yaml)

