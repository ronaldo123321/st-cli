"""Tests for the top-level ``st --version`` option."""

from importlib.metadata import version

from click.testing import CliRunner

from st_cli.cli import cli


def test_cli_version_matches_package_metadata():
    expected = version("sensortower-st-cli")

    result = CliRunner().invoke(cli, ["--version"])

    assert result.exit_code == 0
    assert result.output.strip() == f"st, version {expected}"
