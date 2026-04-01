"""st batch — run shared fetch pipeline for each non-empty line in a file."""

import logging

import click

from st_cli.auth import get_credential
from st_cli.constants import CREDENTIAL_FILE
from st_cli.output import error_payload, print_payload, success_payload
from st_cli.pipeline import PipelineDisambiguation, PipelineFailure, PipelineSuccess, run_fetch_pipeline
from st_cli.st_client import create_st_client

logger = logging.getLogger(__name__)


@click.command("batch")
@click.option("-f", "--file", "file_path", type=click.Path(exists=True), required=True, help="One query per line")
@click.option(
    "--pick-strategy",
    "pick_strategy",
    type=click.Choice(["heuristic", "first", "fail"], case_sensitive=False),
    default="heuristic",
    show_default=True,
    help="How to resolve multiple autocomplete matches.",
)
@click.option("--json", "as_json", is_flag=True)
@click.option("--yaml", "as_yaml", is_flag=True)
def batch(file_path: str, pick_strategy: str, as_json: bool, as_yaml: bool) -> None:
    """Run the same pipeline as ``st fetch`` for each line."""
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

    lines: list[str] = []
    with open(file_path, encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            lines.append(s)

    items: list[dict[str, object]] = []
    errors: list[dict[str, object]] = []

    try:
        with create_st_client(cred.cookies) as client:
            for line in lines:
                result = run_fetch_pipeline(
                    client,
                    line,
                    pick_1based=None,
                    auto_pick_first=False,
                    pick_strategy=pick_strategy,
                )
                if isinstance(result, PipelineFailure):
                    errors.append(
                        {
                            "query": line,
                            "code": result.code,
                            "message": result.message,
                            "details": result.details,
                        }
                    )
                    continue
                if isinstance(result, PipelineDisambiguation):
                    errors.append(
                        {
                            "query": line,
                            "code": "needs_disambiguation",
                            "message": "Multiple autocomplete matches; use `st fetch --pick` to decide.",
                            "details": {
                                "candidates": result.candidates,
                                "warnings": result.warnings,
                                "search_term_used": result.search_term,
                                "input": {"raw": result.raw_query},
                            },
                        }
                    )
                    continue
                assert isinstance(result, PipelineSuccess)
                p = result.payload
                items.append(
                    {
                        "query": line,
                        "selected": p["selected"],
                        "unified_app_id": p["unified_app_id"],
                        "revenue": p["revenue"],
                        "warnings": p["warnings"],
                    }
                )
    except RuntimeError as exc:
        logger.exception("batch failed")
        print_payload(error_payload("upstream_error", str(exc)), as_json=as_json, as_yaml=as_yaml)
        raise SystemExit(1) from None

    print_payload(success_payload({"items": items, "errors": errors}), as_json=as_json, as_yaml=as_yaml)
