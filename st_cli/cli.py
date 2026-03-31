"""st-cli entry point."""

import logging

import click

from st_cli import __version__
from st_cli.commands import auth_cmd, batch_cmd, fetch_cmd, landscape_cmd, status_cmd


@click.group()
@click.version_option(version=__version__, prog_name="st")
@click.option("-v", "--verbose", is_flag=True, help="Verbose logging on stderr")
@click.pass_context
def cli(ctx: click.Context, verbose: bool) -> None:
    """Sensor Tower CLI — cookies from browser (st login) or ~/.config/st-cli."""
    ctx.ensure_object(dict)
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.WARNING,
        format="%(levelname)s %(message)s",
    )


cli.add_command(auth_cmd.login)
cli.add_command(auth_cmd.logout)
cli.add_command(status_cmd.status)
cli.add_command(fetch_cmd.fetch)
cli.add_command(batch_cmd.batch)
cli.add_command(landscape_cmd.landscape)
