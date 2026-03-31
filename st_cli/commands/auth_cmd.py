"""st login / st logout — cookie session (rdt-cli style)."""

import logging

import click

from st_cli.auth import Credential, clear_credential, extract_browser_credential, save_credential
from st_cli.constants import CREDENTIAL_FILE
from st_cli.output import error_payload, print_payload, success_payload
from st_cli.st_client import create_st_client
from st_cli.st_api import probe_session

logger = logging.getLogger(__name__)


@click.command("login")
@click.option("--json", "as_json", is_flag=True)
@click.option("--yaml", "as_yaml", is_flag=True)
@click.option(
    "--cookies-file",
    "cookies_file",
    type=click.Path(exists=True, dir_okay=False, readable=True, path_type=str),
    default=None,
)
def login(as_json: bool, as_yaml: bool, cookies_file: str | None) -> None:
    """Save Sensor Tower cookies from this machine's browser (Chrome/Firefox/Edge/Brave).

    Log in to https://app.sensortower.com in your browser first, then run this command.
    """
    if cookies_file:
        import json

        raw = open(cookies_file, "r", encoding="utf-8").read()
        parsed = json.loads(raw)
        cookies = parsed.get("cookies") if isinstance(parsed, dict) else None
        if not isinstance(cookies, dict):
            # also accept plain `{cookieName: cookieValue}` JSON
            cookies = parsed if isinstance(parsed, dict) else None

        if not isinstance(cookies, dict) or not cookies:
            print_payload(
                error_payload(
                    "login_failed",
                    "Invalid cookies-file format; expected `{ \"cookies\": {...} }` or a plain cookie map.",
                    {"cookies_file": cookies_file},
                ),
                as_json=as_json,
                as_yaml=as_yaml,
            )
            raise SystemExit(1)

        cred = Credential(cookies=cookies, source=f"manual:cookies-file:{cookies_file}")
        save_credential(cred)
    else:
        cred = extract_browser_credential()
        if not cred or not cred.cookies:
            print_payload(
                error_payload(
                    "login_failed",
                    "Could not read cookies for .sensortower.com from any browser.",
                    {"hint": "Open app.sensortower.com in Chrome (or Firefox/Edge), log in, then retry."},
                ),
                as_json=as_json,
                as_yaml=as_yaml,
            )
            raise SystemExit(1)

    with create_st_client(cred.cookies) as client:
        info = probe_session(client)
    if not info.get("api_ok"):
        print_payload(
            error_payload(
                "not_authenticated",
                "Cookies were saved but Sensor Tower API did not accept them.",
                {**info, "credential_file": str(CREDENTIAL_FILE)},
            ),
            as_json=as_json,
            as_yaml=as_yaml,
        )
        raise SystemExit(1)

    print_payload(
        success_payload(
            {
                "logged_in": True,
                "credential_file": str(CREDENTIAL_FILE),
                "source": cred.source,
                "probe": info,
            }
        ),
        as_json=as_json,
        as_yaml=as_yaml,
    )


@click.command("logout")
@click.option("--json", "as_json", is_flag=True)
@click.option("--yaml", "as_yaml", is_flag=True)
def logout(as_json: bool, as_yaml: bool) -> None:
    """Remove saved cookies (~/.config/st-cli/credential.json)."""
    clear_credential()
    print_payload(success_payload({"logged_out": True}), as_json=as_json, as_yaml=as_yaml)
