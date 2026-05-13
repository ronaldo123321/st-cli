"""st aso-keywords — Store Marketing ASO keyword management."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import click

from st_cli.auth import get_credential
from st_cli.constants import CREDENTIAL_FILE
from st_cli.output import error_payload, print_payload, success_payload
from st_cli.st_api import (
    download_aso_keywords_csv,
    get_app_intel_user_app_metadata,
    get_app_intel_user_apps,
    get_csrf_token_for_aso_page,
    get_csrf_token_for_top_apps_page,
    lookup_aso_keywords,
)
from st_cli.st_client import create_st_client

logger = logging.getLogger(__name__)


def _require_credential(as_json: bool, as_yaml: bool):
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
    return cred


def _csrf_token(client) -> str | None:
    return get_csrf_token_for_aso_page(client) or get_csrf_token_for_top_apps_page(client)


def _read_terms(keywords: tuple[str, ...], keywords_file: str | None) -> list[str]:
    out: list[str] = []
    for value in keywords:
        for token in value.split(","):
            term = token.strip()
            if term:
                out.append(term)
    if keywords_file:
        for line in Path(keywords_file).read_text(encoding="utf-8").splitlines():
            term = line.strip()
            if term and not term.startswith("#"):
                out.append(term)
    seen: set[str] = set()
    deduped: list[str] = []
    for term in out:
        key = term.casefold()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(term)
    return deduped


def _infer_comparison_period(start_date: str, end_date: str) -> tuple[str, str]:
    try:
        start = datetime.strptime(start_date, "%Y-%m-%d").date()
        end = datetime.strptime(end_date, "%Y-%m-%d").date()
    except ValueError as exc:
        raise click.ClickException("Dates must use YYYY-MM-DD.") from exc
    days = (end - start).days + 1
    if days <= 0:
        raise click.ClickException("--end must be on or after --start.")
    previous_end = start - timedelta(days=1)
    previous_start = previous_end - timedelta(days=days - 1)
    return previous_start.isoformat(), previous_end.isoformat()


def _summarize_user_apps(data: Any) -> Any:
    if isinstance(data, dict):
        for key in ("apps", "user_apps", "data"):
            value = data.get(key)
            if isinstance(value, list):
                return value
    return data


def _candidate_summary(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": row.get("id") or row.get("_id"),
        "appId": row.get("appId") or row.get("app_id"),
        "appName": row.get("appName") or row.get("name"),
        "os": row.get("os"),
        "country": row.get("country"),
        "numTerms": row.get("numTerms"),
    }


def _matches_app_ref(row: dict[str, Any], *, app_id: str | None, app_name: str | None) -> bool:
    if app_id:
        wanted = app_id.strip()
        values = [
            row.get("id"),
            row.get("_id"),
            row.get("appId"),
            row.get("app_id"),
            row.get("unifiedAppId"),
            row.get("unified_app_id"),
        ]
        if any(str(value) == wanted for value in values if value is not None):
            return True
    if app_name:
        wanted_name = app_name.strip().casefold()
        names = [row.get("appName"), row.get("name"), row.get("subtitle")]
        possible = row.get("possibleNames")
        if isinstance(possible, list):
            names.extend(possible)
        if any(str(value).casefold() == wanted_name for value in names if value):
            return True
        if any(wanted_name in str(value).casefold() for value in names if value):
            return True
    return False


def _resolve_user_app_id_from_rows(
    rows: Any,
    *,
    user_app_id: str | None,
    app_id: str | None,
    app_name: str | None,
    os_name: str,
    country: str,
) -> tuple[str, dict[str, Any] | None]:
    if user_app_id:
        return user_app_id, None
    if not app_id and not app_name:
        raise click.ClickException("Provide --app-id, --app-name, or --user-app-id.")
    if not isinstance(rows, list):
        raise click.ClickException("Could not read ASO user apps from Sensor Tower response.")
    country_upper = country.upper()
    matches: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        row_os = str(row.get("os") or "").lower()
        row_country = str(row.get("country") or "").upper()
        valid_countries = row.get("validCountries")
        country_matches = row_country == country_upper or (
            isinstance(valid_countries, list) and country_upper in {str(c).upper() for c in valid_countries}
        )
        if row_os and row_os != os_name.lower():
            continue
        if not country_matches:
            continue
        if _matches_app_ref(row, app_id=app_id, app_name=app_name):
            matches.append(row)

    if not matches:
        candidates = [_candidate_summary(row) for row in rows if isinstance(row, dict)][:10]
        raise click.ClickException(
            "Could not resolve ASO user app. Run `st aso-keywords user-apps --json` "
            f"or pass --user-app-id directly. First candidates: {candidates}"
        )
    if len(matches) > 1:
        candidates = [_candidate_summary(row) for row in matches[:10]]
        raise click.ClickException(
            "Multiple ASO user apps matched. Pass --user-app-id directly or refine --app-name. "
            f"Matches: {candidates}"
        )
    chosen = matches[0]
    resolved = chosen.get("id") or chosen.get("_id")
    if not resolved:
        raise click.ClickException(f"Matched ASO user app has no id: {_candidate_summary(chosen)}")
    return str(resolved), chosen


def _resolve_user_app_id(
    client,
    *,
    csrf_token: str | None,
    user_app_id: str | None,
    app_id: str | None,
    app_name: str | None,
    os_name: str,
    country: str,
) -> tuple[str, dict[str, Any] | None]:
    if user_app_id:
        return user_app_id, None
    data = get_app_intel_user_apps(client, csrf_token=csrf_token)
    rows = _summarize_user_apps(data)
    return _resolve_user_app_id_from_rows(
        rows,
        user_app_id=user_app_id,
        app_id=app_id,
        app_name=app_name,
        os_name=os_name,
        country=country,
    )


@click.group("aso-keywords")
def aso_keywords() -> None:
    """Manage Store Marketing -> ASO Keywords."""


@aso_keywords.command("user-apps")
@click.option("--country", default="US", show_default=True, help="Country used for metadata lookup.")
@click.option("--metadata", is_flag=True, help="Also fetch metadata for each returned user app.")
@click.option("--json", "as_json", is_flag=True)
@click.option("--yaml", "as_yaml", is_flag=True)
def user_apps(country: str, metadata: bool, as_json: bool, as_yaml: bool) -> None:
    """List ASO user apps / keyword views available to this account."""
    cred = _require_credential(as_json, as_yaml)
    try:
        with create_st_client(cred.cookies) as client:
            csrf_token = _csrf_token(client)
            data = get_app_intel_user_apps(client, csrf_token=csrf_token)
            if metadata:
                rows = _summarize_user_apps(data)
                if isinstance(rows, list):
                    enriched = []
                    for row in rows:
                        if not isinstance(row, dict):
                            enriched.append(row)
                            continue
                        user_app_id = str(row.get("id") or row.get("_id") or "")
                        meta = None
                        if user_app_id:
                            meta = get_app_intel_user_app_metadata(
                                client,
                                user_app_id=user_app_id,
                                country=country,
                                csrf_token=csrf_token,
                            )
                        enriched.append({**row, "metadata": meta})
                    data = enriched
    except RuntimeError as exc:
        logger.exception("aso user-apps failed")
        print_payload(error_payload("upstream_error", str(exc)), as_json=as_json, as_yaml=as_yaml)
        raise SystemExit(1) from None
    print_payload(success_payload(data), as_json=as_json, as_yaml=as_yaml)


@aso_keywords.command("metadata")
@click.option("--user-app-id", help="ASO keyword view / user app id.")
@click.option("--app-id", help="App Store id, package name, or Sensor Tower unified app id.")
@click.option("--app-name", help="App name to resolve from ASO user apps.")
@click.option("--os", "os_name", type=click.Choice(["ios", "android"]), default="ios", show_default=True)
@click.option("--country", default="US", show_default=True)
@click.option("--json", "as_json", is_flag=True)
@click.option("--yaml", "as_yaml", is_flag=True)
def metadata(
    user_app_id: str | None,
    app_id: str | None,
    app_name: str | None,
    os_name: str,
    country: str,
    as_json: bool,
    as_yaml: bool,
) -> None:
    """Fetch metadata for one ASO user app / keyword view."""
    cred = _require_credential(as_json, as_yaml)
    try:
        with create_st_client(cred.cookies) as client:
            csrf_token = _csrf_token(client)
            resolved_user_app_id, matched = _resolve_user_app_id(
                client,
                csrf_token=csrf_token,
                user_app_id=user_app_id,
                app_id=app_id,
                app_name=app_name,
                os_name=os_name,
                country=country,
            )
            data = get_app_intel_user_app_metadata(
                client,
                user_app_id=resolved_user_app_id,
                country=country,
                csrf_token=csrf_token,
            )
    except RuntimeError as exc:
        logger.exception("aso metadata failed")
        print_payload(error_payload("upstream_error", str(exc)), as_json=as_json, as_yaml=as_yaml)
        raise SystemExit(1) from None
    print_payload(
        success_payload(
            {
                "user_app_id": resolved_user_app_id,
                "matched_app": _candidate_summary(matched) if matched else None,
                "metadata": data,
            }
        ),
        as_json=as_json,
        as_yaml=as_yaml,
    )


@aso_keywords.command("add")
@click.option("--user-app-id", help="ASO keyword view / user app id.")
@click.option("--app-id", help="App Store id, package name, or Sensor Tower unified app id.")
@click.option("--app-name", help="App name to resolve from ASO user apps.")
@click.option("--os", "os_name", type=click.Choice(["ios", "android"]), default="ios", show_default=True)
@click.option("--country", default="US", show_default=True)
@click.option("--keyword", "keywords", multiple=True, help="Keyword to add. Repeat or comma-separate.")
@click.option("--keywords-file", help="Text file with one keyword per line.")
@click.option("--json", "as_json", is_flag=True)
@click.option("--yaml", "as_yaml", is_flag=True)
def add_keywords(
    user_app_id: str | None,
    app_id: str | None,
    app_name: str | None,
    os_name: str,
    country: str,
    keywords: tuple[str, ...],
    keywords_file: str | None,
    as_json: bool,
    as_yaml: bool,
) -> None:
    """Add or look up ASO keywords for a keyword view."""
    terms = _read_terms(keywords, keywords_file)
    if not terms:
        print_payload(
            error_payload("bad_request", "Provide --keyword or --keywords-file."),
            as_json=as_json,
            as_yaml=as_yaml,
        )
        raise SystemExit(1)
    cred = _require_credential(as_json, as_yaml)
    try:
        with create_st_client(cred.cookies) as client:
            csrf_token = _csrf_token(client)
            resolved_user_app_id, matched = _resolve_user_app_id(
                client,
                csrf_token=csrf_token,
                user_app_id=user_app_id,
                app_id=app_id,
                app_name=app_name,
                os_name=os_name,
                country=country,
            )
            data = lookup_aso_keywords(
                client,
                os=os_name,
                user_app_id=resolved_user_app_id,
                country=country,
                terms=terms,
                csrf_token=csrf_token,
            )
    except RuntimeError as exc:
        logger.exception("aso keyword add failed")
        print_payload(error_payload("upstream_error", str(exc)), as_json=as_json, as_yaml=as_yaml)
        raise SystemExit(1) from None
    print_payload(
        success_payload(
            {
                "user_app_id": resolved_user_app_id,
                "matched_app": _candidate_summary(matched) if matched else None,
                "os": os_name,
                "country": country.upper(),
                "terms": terms,
                "result": data,
            }
        ),
        as_json=as_json,
        as_yaml=as_yaml,
    )


@aso_keywords.command("export")
@click.option("--keyword-view-id", help="ASO keyword view id, same as user app id in the page URL.")
@click.option("--user-app-id", help="Alias for --keyword-view-id.")
@click.option("--app-id", help="App Store id, package name, or Sensor Tower unified app id.")
@click.option("--app-name", help="App name to resolve from ASO user apps.")
@click.option("--os", "os_name", type=click.Choice(["ios", "android"]), default="ios", show_default=True)
@click.option("--country", default="US", show_default=True)
@click.option("--device", default="iphone", show_default=True, help="Device filter, e.g. iphone/ipad/android.")
@click.option("--start", "start_date", required=True, help="Start date YYYY-MM-DD.")
@click.option("--end", "end_date", required=True, help="End date YYYY-MM-DD.")
@click.option("--comparison-start", "comparison_start_date", help="Comparison start date YYYY-MM-DD.")
@click.option("--comparison-end", "comparison_end_date", help="Comparison end date YYYY-MM-DD.")
@click.option("--search-term", "search_terms", multiple=True, help="Optional keyword table search term.")
@click.option("--limit", default=10000, show_default=True, type=int)
@click.option("--out", "out_file", type=click.Path(dir_okay=False, path_type=Path), help="CSV output path.")
@click.option("--stdout", "to_stdout", is_flag=True, help="Write CSV to stdout instead of --out.")
@click.option("--json", "as_json", is_flag=True)
@click.option("--yaml", "as_yaml", is_flag=True)
def export_keywords(
    keyword_view_id: str | None,
    user_app_id: str | None,
    app_id: str | None,
    app_name: str | None,
    os_name: str,
    country: str,
    device: str,
    start_date: str,
    end_date: str,
    comparison_start_date: str,
    comparison_end_date: str,
    search_terms: tuple[str, ...],
    limit: int,
    out_file: Path | None,
    to_stdout: bool,
    as_json: bool,
    as_yaml: bool,
) -> None:
    """Download ASO Keywords CSV."""
    if not out_file and not to_stdout:
        print_payload(
            error_payload("bad_request", "Provide --out or --stdout."),
            as_json=as_json,
            as_yaml=as_yaml,
        )
        raise SystemExit(1)
    if bool(comparison_start_date) != bool(comparison_end_date):
        print_payload(
            error_payload("bad_request", "Provide both --comparison-start and --comparison-end, or neither."),
            as_json=as_json,
            as_yaml=as_yaml,
        )
        raise SystemExit(1)
    if comparison_start_date is None or comparison_end_date is None:
        comparison_start_date, comparison_end_date = _infer_comparison_period(start_date, end_date)
    if keyword_view_id and user_app_id and keyword_view_id != user_app_id:
        print_payload(
            error_payload("bad_request", "--keyword-view-id and --user-app-id disagree."),
            as_json=as_json,
            as_yaml=as_yaml,
        )
        raise SystemExit(1)
    cred = _require_credential(as_json, as_yaml)
    try:
        with create_st_client(cred.cookies) as client:
            csrf_token = _csrf_token(client)
            resolved_user_app_id, matched = _resolve_user_app_id(
                client,
                csrf_token=csrf_token,
                user_app_id=keyword_view_id or user_app_id,
                app_id=app_id,
                app_name=app_name,
                os_name=os_name,
                country=country,
            )
            content = download_aso_keywords_csv(
                client,
                keyword_view_id=resolved_user_app_id,
                os=os_name,
                country=country,
                device=device,
                start_date=start_date,
                end_date=end_date,
                comparison_start_date=comparison_start_date,
                comparison_end_date=comparison_end_date,
                limit=limit,
                search_terms=list(search_terms),
                csrf_token=csrf_token,
            )
    except RuntimeError as exc:
        logger.exception("aso keyword export failed")
        print_payload(error_payload("upstream_error", str(exc)), as_json=as_json, as_yaml=as_yaml)
        raise SystemExit(1) from None
    if to_stdout:
        click.get_binary_stream("stdout").write(content)
        return
    assert out_file is not None
    out_file.parent.mkdir(parents=True, exist_ok=True)
    out_file.write_bytes(content)
    print_payload(
        success_payload(
            {
                "keyword_view_id": resolved_user_app_id,
                "user_app_id": resolved_user_app_id,
                "matched_app": _candidate_summary(matched) if matched else None,
                "os": os_name,
                "country": country.upper(),
                "device": device,
                "start_date": start_date,
                "end_date": end_date,
                "out_file": str(out_file),
                "bytes": len(content),
            }
        ),
        as_json=as_json,
        as_yaml=as_yaml,
    )
