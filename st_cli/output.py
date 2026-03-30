"""Structured stdout (JSON envelope) for agents — aligned with rdt-cli style."""

import json
import os
import sys
from typing import Any

_SCHEMA_VERSION = "1"
_OUTPUT_ENV = "OUTPUT"


def success_payload(data: Any) -> dict[str, Any]:
    return {"ok": True, "schema_version": _SCHEMA_VERSION, "data": data}


def error_payload(code: str, message: str, details: Any | None = None) -> dict[str, Any]:
    err: dict[str, Any] = {"code": code, "message": message}
    if details is not None:
        err["details"] = details
    return {"ok": False, "schema_version": _SCHEMA_VERSION, "error": err}


def resolve_machine_json(as_json: bool, as_yaml: bool) -> bool:
    if as_json and as_yaml:
        raise SystemExit("Use only one of --json or --yaml.")
    if as_json:
        return True
    if as_yaml:
        return False
    mode = os.getenv(_OUTPUT_ENV, "auto").strip().lower()
    if mode == "json":
        return True
    if mode == "yaml":
        return False
    return not sys.stdout.isatty()


def print_payload(payload: dict[str, Any], *, as_json: bool, as_yaml: bool) -> None:
    use_json = resolve_machine_json(as_json, as_yaml)
    if use_json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return
    try:
        import yaml

        print(
            yaml.dump(
                payload,
                allow_unicode=True,
                default_flow_style=False,
                sort_keys=False,
            )
        )
    except ImportError:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
