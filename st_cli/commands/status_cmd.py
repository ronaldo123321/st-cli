"""st status — verify saved cookies + Sensor Tower API."""

import logging

import click
import httpx

from st_cli.auth import get_credential
from st_cli.constants import CREDENTIAL_FILE
from st_cli.output import error_payload, print_payload, success_payload
from st_cli.st_api import probe_session
from st_cli.st_client import create_st_client

logger = logging.getLogger(__name__)


@click.command("status")
@click.option("--json", "as_json", is_flag=True, help="Print JSON envelope")
@click.option("--yaml", "as_yaml", is_flag=True, help="Print YAML envelope")
def status(as_json: bool, as_yaml: bool) -> None:
    """Check that saved cookies can call ST autocomplete API."""
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
            info = probe_session(client)
    except httpx.HTTPError as exc:
        logger.exception("status failed")
        print_payload(
            error_payload("network_error", str(exc)),
            as_json=as_json,
            as_yaml=as_yaml,
        )
        raise SystemExit(1) from None

    info["credential_file"] = str(CREDENTIAL_FILE)
    info["credential_source"] = cred.source
    if info.get("api_ok"):
        print_payload(success_payload(info), as_json=as_json, as_yaml=as_yaml)
        return
    print_payload(
        error_payload("not_authenticated", "Sensor Tower session not valid", info),
        as_json=as_json,
        as_yaml=as_yaml,
    )
    raise SystemExit(1)
