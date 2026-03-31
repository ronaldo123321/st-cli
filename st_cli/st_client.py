"""HTTP client for app.sensortower.com with cookie jar."""

from __future__ import annotations

from collections.abc import Mapping

import httpx

from st_cli.constants import DEFAULT_HEADERS, ST_ORIGIN


def create_st_client(cookies: Mapping[str, str]) -> httpx.Client:
    """Return a sync httpx client scoped to Sensor Tower origin."""
    return httpx.Client(
        base_url=ST_ORIGIN,
        cookies=dict(cookies),
        headers=dict(DEFAULT_HEADERS),
        timeout=httpx.Timeout(60.0),
        follow_redirects=True,
    )
