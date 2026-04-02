"""st landscape-report — render markdown from `st landscape` JSON.

The rendered report includes per-competitor:
- revenue (as-of month)
- market share (as-of last month, 2-decimal percent)
- downloads (as-of last month)
- MAU (as-of last month)
"""

import json
from pathlib import Path
from typing import Any

import click

from st_cli.output import error_payload, print_payload, success_payload
from st_cli.reports.landscape import render_landscape_report_md


def _read_json_input(path: Path | None) -> dict[str, Any]:
    if path is None:
        raw = click.get_text_stream("stdin").read()
    else:
        raw = path.read_text(encoding="utf-8", errors="ignore")
    obj = json.loads(raw)
    if not isinstance(obj, dict):
        raise ValueError("input_json_not_object")
    return obj


@click.command("landscape-report")
@click.option(
    "--in",
    "in_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    default=None,
    help="Path to `st landscape --json` output. If omitted, read from stdin.",
)
@click.option(
    "--out",
    "out_path",
    type=click.Path(dir_okay=False, path_type=Path),
    required=True,
    help="Write markdown report to this path.",
)
@click.option("--json", "as_json", is_flag=True)
@click.option("--yaml", "as_yaml", is_flag=True)
def landscape_report(in_path: Path | None, out_path: Path, as_json: bool, as_yaml: bool) -> None:
    """Render a competitive landscape markdown report from `st landscape` JSON."""
    try:
        env = _read_json_input(in_path)
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        print_payload(
            error_payload(
                "bad_request",
                "Could not read valid JSON input for landscape-report.",
                {"input": str(in_path) if in_path else "<stdin>", "error": str(exc)},
            ),
            as_json=as_json,
            as_yaml=as_yaml,
        )
        raise SystemExit(1) from None

    if env.get("ok") is not True:
        print_payload(
            error_payload(
                "bad_request",
                "Input JSON is not an ok=true envelope. Pass output from `st landscape --json`.",
                {"input": str(in_path) if in_path else "<stdin>"},
            ),
            as_json=as_json,
            as_yaml=as_yaml,
        )
        raise SystemExit(1)

    data = env.get("data")
    if not isinstance(data, dict):
        print_payload(
            error_payload("bad_request", "Missing `data` object in input JSON.", None),
            as_json=as_json,
            as_yaml=as_yaml,
        )
        raise SystemExit(1)

    source = data.get("source")
    competitors = data.get("competitors")
    if not isinstance(source, dict) or not isinstance(competitors, list):
        print_payload(
            error_payload(
                "bad_request",
                "Input JSON missing `data.source` or `data.competitors`.",
                None,
            ),
            as_json=as_json,
            as_yaml=as_yaml,
        )
        raise SystemExit(1)

    report_md = render_landscape_report_md(source=source, competitors=competitors)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(report_md, encoding="utf-8")

    print_payload(
        success_payload({"report": {"path": str(out_path)}}),
        as_json=as_json,
        as_yaml=as_yaml,
    )

