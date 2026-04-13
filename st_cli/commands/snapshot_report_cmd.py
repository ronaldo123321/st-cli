"""st snapshot-report — render markdown from `st snapshot` JSON."""

import json
from pathlib import Path
from typing import Any

import click

from st_cli.output import error_payload, print_payload, success_payload
from st_cli.reports.snapshot import render_snapshot_report_md


def _read_json_input(path: Path | None) -> dict[str, Any]:
    if path is None:
        raw = click.get_text_stream("stdin").read()
    else:
        raw = path.read_text(encoding="utf-8", errors="ignore")
    obj = json.loads(raw)
    if not isinstance(obj, dict):
        raise ValueError("input_json_not_object")
    return obj


@click.command("snapshot-report")
@click.option(
    "--in",
    "in_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    default=None,
    help="Path to `st snapshot --json` output. If omitted, read from stdin.",
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
def snapshot_report(in_path: Path | None, out_path: Path, as_json: bool, as_yaml: bool) -> None:
    """Render a snapshot markdown summary from `st snapshot` JSON."""
    try:
        env = _read_json_input(in_path)
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        print_payload(
            error_payload(
                "bad_request",
                "Could not read valid JSON input for snapshot-report.",
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
                "Input JSON is not an ok=true envelope. Pass output from `st snapshot --json`.",
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
    raw_block = data.get("raw")
    landscape_block = data.get("landscape")
    raw_items = raw_block.get("items") if isinstance(raw_block, dict) else None
    raw_errors = raw_block.get("errors") if isinstance(raw_block, dict) else None
    landscape_items = landscape_block.get("competitors") if isinstance(landscape_block, dict) else None

    has_raw = isinstance(raw_items, list) and isinstance(raw_errors, list)
    has_landscape = isinstance(landscape_items, list)
    if not isinstance(source, dict) or (not has_raw and not has_landscape):
        print_payload(
            error_payload(
                "bad_request",
                "Input JSON missing snapshot report fields. Pass output from `st snapshot --json`.",
                None,
            ),
            as_json=as_json,
            as_yaml=as_yaml,
        )
        raise SystemExit(1)

    report_md = render_snapshot_report_md(
        source=source,
        raw_items=raw_items if isinstance(raw_items, list) else [],
        raw_errors=raw_errors if isinstance(raw_errors, list) else [],
        landscape_items=landscape_items if isinstance(landscape_items, list) else [],
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(report_md, encoding="utf-8")

    print_payload(
        success_payload({"report": {"path": str(out_path)}}),
        as_json=as_json,
        as_yaml=as_yaml,
    )
