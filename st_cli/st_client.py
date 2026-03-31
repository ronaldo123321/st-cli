"""HTTP client for app.sensortower.com with cookie jar."""

from __future__ import annotations

from collections.abc import Mapping
import random
import time

import httpx

from st_cli.constants import DEFAULT_HEADERS, ST_ORIGIN


_REQUEST_DELAY_MEAN_S = 1.0
_REQUEST_DELAY_SIGMA_S = 0.3
_REQUEST_DELAY_MIN_S = 0.0
_REQUEST_DELAY_MAX_S = 2.0

_LONG_PAUSE_PROB = 0.05
_LONG_PAUSE_MIN_S = 2.0
_LONG_PAUSE_MAX_S = 5.0


def _sleep_before_request() -> None:
    base = random.gauss(_REQUEST_DELAY_MEAN_S, _REQUEST_DELAY_SIGMA_S)
    if base < _REQUEST_DELAY_MIN_S:
        base = _REQUEST_DELAY_MIN_S
    if base > _REQUEST_DELAY_MAX_S:
        base = _REQUEST_DELAY_MAX_S

    extra = 0.0
    if random.random() < _LONG_PAUSE_PROB:
        extra = random.uniform(_LONG_PAUSE_MIN_S, _LONG_PAUSE_MAX_S)

    delay = base + extra
    if delay > 0:
        time.sleep(delay)


def create_st_client(cookies: Mapping[str, str]) -> httpx.Client:
    """Return a sync httpx client scoped to Sensor Tower origin."""
    return httpx.Client(
        base_url=ST_ORIGIN,
        cookies=dict(cookies),
        headers=dict(DEFAULT_HEADERS),
        timeout=httpx.Timeout(60.0),
        follow_redirects=True,
        event_hooks={"request": [lambda _req: _sleep_before_request()]},
    )
