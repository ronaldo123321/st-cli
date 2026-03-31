"""st fetch — resolve app by URL or name, pull ~36 months revenue via /api/apps/facets."""

import logging

import click

from st_cli.auth import get_credential
from st_cli.constants import CREDENTIAL_FILE
from st_cli.output import error_payload, print_payload, success_payload
from st_cli.pipeline import (
    PipelineDisambiguation,
    PipelineFailure,
    PipelineSuccess,
    run_fetch_pipeline,
)
from st_cli.st_client import create_st_client

logger = logging.getLogger(__name__)


@click.command("fetch")
@click.argument("query")
@click.option(
    "--pick",
    "pick",
    type=int,
    default=None,
    help="When search returns multiple apps, pick 1-based index from candidates",
)
@click.option("--json", "as_json", is_flag=True)
@click.option("--yaml", "as_yaml", is_flag=True)
def fetch(
    query: str,
    pick: int | None,
    as_json: bool,
    as_yaml: bool,
) -> None:
    """Fetch app metrics: QUERY is store URL or free-text app name."""
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

    try:
        with create_st_client(cred.cookies) as client:
            result = run_fetch_pipeline(
                client,
                query,
                pick_1based=pick,
                auto_pick_first=False,
            )
    except RuntimeError as exc:
        logger.exception("fetch failed")
        print_payload(error_payload("upstream_error", str(exc)), as_json=as_json, as_yaml=as_yaml)
        raise SystemExit(1) from None

    if isinstance(result, PipelineFailure):
        print_payload(
            error_payload(result.code, result.message, result.details),
            as_json=as_json,
            as_yaml=as_yaml,
        )
        raise SystemExit(1)

    if isinstance(result, PipelineDisambiguation):
        print_payload(
            success_payload(
                {
                    "needs_disambiguation": True,
                    "candidates": result.candidates,
                    "warnings": result.warnings,
                    "search_term_used": result.search_term,
                    "input": {"raw": result.raw_query},
                }
            ),
            as_json=as_json,
            as_yaml=as_yaml,
        )
        raise SystemExit(0)

    assert isinstance(result, PipelineSuccess)
    print_payload(success_payload(result.payload), as_json=as_json, as_yaml=as_yaml)
