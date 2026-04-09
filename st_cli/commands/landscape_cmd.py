"""st landscape — fetch competitive landscape data (and optionally render report)."""

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
from st_cli.pipeline import PipelineDisambiguation, PipelineFailure, PipelineSuccess, run_fetch_pipeline
from st_cli.reports.landscape import render_landscape_report_md
from st_cli.st_client import create_st_client

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CompetitorRow:
    name: str
    store_url: str | None
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


def _sum_revenue_trailing_12_months(monthly: list[dict[str, Any]], end_month_key: str) -> float | None:
    """Sum monthly revenue for trailing 12 months ending at `end_month_key`.

    Returns None if any month in the 12-month window is missing/unparseable.
    """
    total = 0.0
    for offset in range(12):
        mk = _shift_month_key(end_month_key, -offset)
        if not mk:
            return None
        v = _extract_month_value(monthly, mk)
        if v is None:
            return None
        total += v
    return total


def _growth_vs_prev_percent(cur: float | None, prev: float | None) -> float | None:
    if cur is None or prev is None or prev <= 0:
        return None
    return (cur - prev) / prev * 100.0


def _format_growth_ratio(cur: float | None, prev: float | None) -> str:
    if cur is None or prev is None or prev <= 0:
        return "N/A"
    pct = (cur - prev) / prev * 100.0
    sign = "+" if pct >= 0 else ""
    return f"{sign}{pct:.1f}%"


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


def _previous_month_key() -> str:
    """Use previous calendar month as the reporting month key (YYYY-MM)."""
    today = date.today()
    this_month_start = today.replace(day=1)
    prev_month_end = this_month_start - timedelta(days=1)
    return prev_month_end.strftime("%Y-%m")


_URL_RE = re.compile(r"https?://\\S+", re.IGNORECASE)


def _parse_competitor_line(line: str) -> tuple[str, str] | None:
    """Parse a competitors file line into (name, store_url).

    Supported formats:
    - name<TAB>url
    - name, url
    - name | url
    """
    s = _normalize_text(line)
    if not s or s.startswith("#"):
        return None
    if "\t" in s:
        left, right = s.split("\t", 1)
        name = _normalize_text(left)
        url = _normalize_text(right)
        return (name, url) if name and url else None
    if " | " in s:
        left, right = s.split(" | ", 1)
        name = _normalize_text(left)
        url = _normalize_text(right)
        return (name, url) if name and url else None
    if "," in s:
        left, right = s.split(",", 1)
        name = _normalize_text(left)
        url = _normalize_text(right)
        return (name, url) if name and url else None
    m = _URL_RE.search(s)
    if not m:
        return None
    url = _normalize_text(m.group(0))
    name = _normalize_text(s.replace(m.group(0), ""))
    return (name, url) if name and url else None


@click.command("landscape")
@click.option(
    "--rdt-report",
    "rdt_report",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    required=False,
    default=None,
    help="Path to rdt-cli-research markdown report (optional).",
)
@click.option(
    "--competitors-file",
    "competitors_file",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    required=False,
    default=None,
    help="Competitors file. Each line: name<TAB>store_url (or name, url).",
)
@click.option("--limit", "limit", type=int, default=12, show_default=True, help="Max competitors to include")
@click.option(
    "--pick-strategy",
    "pick_strategy",
    type=click.Choice(["heuristic", "first", "fail"], case_sensitive=False),
    default="heuristic",
    show_default=True,
    help="How to resolve multiple autocomplete matches.",
)
@click.option(
    "--market-share-category-override",
    "--category",
    "market_share_category_override",
    type=int,
    required=False,
    default=None,
    show_default=False,
    help="Optional: override inferred SensorTower category id for market share denominator.",
)
@click.option("--out", "out_path", type=click.Path(dir_okay=False, path_type=Path), default=None, help="Write final report markdown to this path")
@click.option("--json", "as_json", is_flag=True)
@click.option("--yaml", "as_yaml", is_flag=True)
def landscape(
    rdt_report: Path | None,
    limit: int,
    competitors_file: Path | None,
    pick_strategy: str,
    market_share_category_override: int | None,
    out_path: Path | None,
    as_json: bool,
    as_yaml: bool,
) -> None:
    """Prepare competitive landscape input data from either:

    - a rdt-cli-research markdown report (competitor table), or
    - a list of competitor names.
    """
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

    competitors: list[CompetitorRow] = []
    source_input: dict[str, Any] = {}

    if rdt_report is not None:
        md = rdt_report.read_text(encoding="utf-8", errors="ignore")
        competitors = _extract_competitor_table_rows(md)
        source_input["rdt_report"] = str(rdt_report)
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

    if not competitors:
        if competitors_file is None:
            print_payload(
                error_payload(
                    "bad_request",
                    "Provide either --rdt-report or --competitors-file (name + store URL).",
                    None,
                ),
                as_json=as_json,
                as_yaml=as_yaml,
            )
            raise SystemExit(1)
        parsed: list[tuple[str, str]] = []
        for line in competitors_file.read_text(encoding="utf-8", errors="ignore").splitlines():
            row = _parse_competitor_line(line)
            if row is None:
                continue
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
        source_input["competitors_file"] = str(competitors_file)
        competitors = [
            CompetitorRow(name=name, store_url=url, mentions=None, positive=None, negative=None, core_review="")
            for name, url in parsed
        ]

    # Reporting month uses previous calendar month.
    month_key = _previous_month_key()
    _, month_end = _current_month_as_of()

    out_rows: list[dict[str, Any]] = []
    with create_st_client(cred.cookies) as client:
        for row in competitors[: max(1, limit)]:
            if row.store_url is None:
                out_rows.append(
                    {
                        "name": row.name,
                        "store_url": None,
                        "rdt": {
                            "mentions": row.mentions,
                            "positive": row.positive,
                            "negative": row.negative,
                            "core_review": row.core_review,
                        },
                        "st": None,
                        "error": {
                            "code": "missing_store_url",
                            "message": "Competitor missing store URL. Provide name + store URL in --competitors-file.",
                        },
                    }
                )
                continue
            try:
                res = run_fetch_pipeline(
                    client,
                    row.store_url,
                    auto_pick_first=False,
                    pick_strategy=pick_strategy,
                    include_market_share=True,
                    market_share_category_override=market_share_category_override,
                    market_share_month_key=month_key,
                )
            except RuntimeError as exc:
                logger.exception("fetch failed for %s", row.name)
                out_rows.append(
                    {
                        "name": row.name,
                        "store_url": row.store_url,
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

            if isinstance(res, PipelineDisambiguation):
                out_rows.append(
                    {
                        "name": row.name,
                        "store_url": row.store_url,
                        "rdt": {
                            "mentions": row.mentions,
                            "positive": row.positive,
                            "negative": row.negative,
                            "core_review": row.core_review,
                        },
                        "st": None,
                        "error": {
                            "code": "needs_disambiguation",
                            "message": "Multiple autocomplete matches; refine name or disambiguate via st fetch --pick.",
                            "details": {
                                "candidates": res.candidates,
                                "warnings": res.warnings,
                                "search_term_used": res.search_term,
                                "input": {"raw": res.raw_query},
                            },
                        },
                    }
                )
                continue

            if isinstance(res, PipelineFailure):
                out_rows.append(
                    {
                        "name": row.name,
                        "store_url": row.store_url,
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
            monthly_downloads = (payload.get("downloads", {}) or {}).get("monthly_estimates", [])
            monthly_mau = (payload.get("mau", {}) or {}).get("monthly_estimates", [])
            revenue_last_month: float | None = None
            downloads_last_month: float | None = None
            mau_last_month: float | None = None
            if isinstance(monthly, list):
                for it in monthly:
                    if not isinstance(it, dict):
                        continue
                    if it.get("month") == month_key:
                        val = it.get("revenue_absolute_usd")
                        if isinstance(val, (int, float)):
                            revenue_last_month = float(val)
                        elif val is None:
                            revenue_last_month = None
                        else:
                            try:
                                revenue_last_month = float(str(val))
                            except ValueError:
                                revenue_last_month = None
                        break

            if isinstance(monthly_downloads, list):
                for it in monthly_downloads:
                    if not isinstance(it, dict):
                        continue
                    if it.get("month") == month_key:
                        val = it.get("downloads_absolute")
                        if isinstance(val, (int, float)):
                            downloads_last_month = float(val)
                        elif val is None:
                            downloads_last_month = None
                        else:
                            try:
                                downloads_last_month = float(str(val))
                            except ValueError:
                                downloads_last_month = None
                        break

            if isinstance(monthly_mau, list):
                for it in monthly_mau:
                    if not isinstance(it, dict):
                        continue
                    if it.get("month") == month_key:
                        val = it.get("mau_absolute")
                        if isinstance(val, (int, float)):
                            mau_last_month = float(val)
                        elif val is None:
                            mau_last_month = None
                        else:
                            try:
                                mau_last_month = float(str(val))
                            except ValueError:
                                mau_last_month = None
                        break

            market_share_obj = payload.get("market_share_as_of_last_month")
            share_percent: float | None = None
            category_id: int | None = None
            if isinstance(market_share_obj, dict):
                v = market_share_obj.get("share_percent")
                if isinstance(v, (int, float)):
                    share_percent = float(v)
                elif v is None:
                    share_percent = None
                else:
                    try:
                        share_percent = float(str(v))
                    except ValueError:
                        share_percent = None

                v_cat = market_share_obj.get("category")
                if isinstance(v_cat, int):
                    category_id = v_cat
                elif isinstance(v_cat, float) and v_cat.is_integer():
                    category_id = int(v_cat)
                elif isinstance(v_cat, str) and v_cat.strip().isdigit():
                    category_id = int(v_cat.strip())

            selected = payload.get("selected") if isinstance(payload.get("selected"), dict) else None
            comments = payload.get("comments", [])[:5]
            if not isinstance(comments, list):
                comments = []
            core_review = _normalize_text(row.core_review)
            ai_label = _classify_ai_label(name=row.name, core_review=core_review, comments=comments)
            segment = _classify_segment(
                name=row.name, core_review=core_review, comments=comments, selected=selected
            )
            strengths, weaknesses = _extract_strength_weakness_bullets(
                core_review=core_review,
                comments=comments,
                rdt_positive=row.positive,
                rdt_negative=row.negative,
            )
            caveat = _match_warning(row.name, selected)

            out_rows.append(
                {
                    "name": row.name,
                    "store_url": row.store_url,
                    "rdt": {
                        "mentions": row.mentions,
                        "positive": row.positive,
                        "negative": row.negative,
                        "core_review": row.core_review,
                    },
                    "segment": segment,
                    "ai_label": ai_label,
                    "strengths": strengths,
                    "weaknesses": weaknesses,
                    "caveat": caveat,
                    "st": {
                        "selected": selected,
                        "first_release_date_us": payload.get("first_release_date_us"),
                        "revenue_last_month_usd": revenue_last_month,
                        "revenue_as_of_last_month_usd": revenue_last_month,
                        "revenue_trailing_12_months_usd": (
                            _sum_revenue_trailing_12_months(monthly, month_key)
                            if isinstance(monthly, list)
                            else None
                        ),
                        "market_share_as_of_last_month": {"share_percent": share_percent, "category": category_id},
                        "downloads_as_of_last_month": {"downloads_absolute": downloads_last_month},
                        "mau_as_of_last_month": {"mau_absolute": mau_last_month},
                        "revenue_6_months_ago_usd": (
                            _extract_month_value(monthly, _shift_month_key(month_key, -6) or "")
                            if isinstance(monthly, list)
                            else None
                        ),
                        "growth_vs_6m_percent": (
                            _growth_vs_prev_percent(
                                revenue_last_month,
                                _extract_month_value(monthly, _shift_month_key(month_key, -6) or ""),
                            )
                            if isinstance(monthly, list)
                            else None
                        ),
                        "comments": comments,
                        "monthly_estimates": monthly if isinstance(monthly, list) else [],
                        "warnings": payload.get("warnings", []),
                    },
                    "error": None,
                }
            )

    data = {
        "source": {
            **source_input,
            "month": month_key,
            "as_of": month_end.isoformat(),
            "facet_regions": DEFAULT_FACET_REGIONS,
            "market_share_category_override": market_share_category_override,
        },
        "competitors": out_rows,
    }
    if out_path is not None:
        report_md = render_landscape_report_md(source=data["source"], competitors=out_rows)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(report_md, encoding="utf-8")
        data["report"] = {"path": str(out_path)}
    print_payload(success_payload(data), as_json=as_json, as_yaml=as_yaml)

