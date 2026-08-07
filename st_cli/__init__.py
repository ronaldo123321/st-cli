"""st-cli: Sensor Tower helper using local Chrome profile + Playwright."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("sensortower-st-cli")
except PackageNotFoundError:
    # Keep source-tree imports usable before the package is installed.
    __version__ = "0+unknown"
