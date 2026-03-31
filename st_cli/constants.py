"""Sensor Tower web app base URLs, config paths, and HTTP defaults."""

import os
from pathlib import Path

ST_ORIGIN = "https://app.sensortower.com"
ST_API = f"{ST_ORIGIN}/api"

CONFIG_DIR = Path.home() / ".config" / "st-cli"
CREDENTIAL_FILE = CONFIG_DIR / "credential.json"

# Optional: path to Chrome/Chromium `Cookies` sqlite (e.g. Profile 1)
ST_CHROME_COOKIES_DB = os.environ.get("ST_CHROME_COOKIES_DB")
ST_CHROME_KEY_FILE = os.environ.get("ST_CHROME_KEY_FILE")

# Align with innovation-crawler usage; bump when ST changes models.
DEFAULT_DATA_MODEL = "DM_2025_Q2"

GLOBAL_FACET_REGIONS = [
    "US",
    "AU",
    "CA",
    "CN",
    "FR",
    "DE",
    "GB",
    "IT",
    "JP",
    "RU",
    "KR",
    "DZ",
    "AO",
    "AR",
    "AT",
    "AZ",
    "BH",
    "BD",
    "BY",
    "BE",
    "BJ",
    "BO",
    "BR",
    "BG",
    "BF",
    "KH",
    "CM",
    "CL",
    "CO",
    "CG",
    "CR",
    "CI",
    "HR",
    "CY",
    "CZ",
    "DK",
    "DO",
    "EC",
    "EG",
    "SV",
    "EE",
    "FI",
    "GE",
    "GH",
    "GR",
    "GT",
    "HK",
    "HU",
    "IN",
    "ID",
    "IQ",
    "IE",
    "IL",
    "JO",
    "KZ",
    "KE",
    "KW",
    "LA",
    "LV",
    "LB",
    "LY",
    "LT",
    "LU",
    "MO",
    "MY",
    "ML",
    "MT",
    "MX",
    "MA",
    "MZ",
    "MM",
    "NL",
    "NZ",
    "NI",
    "NG",
    "NO",
    "OM",
    "PK",
    "PA",
    "PY",
    "PE",
    "PH",
    "PL",
    "PT",
    "QA",
    "RO",
    "SA",
    "SN",
    "RS",
    "SG",
    "SK",
    "SI",
    "ZA",
    "ES",
    "LK",
    "SE",
    "CH",
    "TW",
    "TZ",
    "TH",
    "TN",
    "TR",
    "UG",
    "UA",
    "AE",
    "UY",
    "UZ",
    "VE",
    "VN",
    "YE",
    "ZM",
    "ZW",
]


def _resolve_facet_regions() -> list[str]:
    raw = os.environ.get("ST_FACET_REGIONS") or os.environ.get("ST_REGIONS") or "US"
    v = raw.strip()
    if not v:
        return ["US"]
    if v.lower() in {"global", "world", "worldwide", "ww"}:
        return list(GLOBAL_FACET_REGIONS)
    parts = [p.strip().upper() for p in v.split(",")]
    out = [p for p in parts if p]
    return out or ["US"]


DEFAULT_FACET_REGIONS = _resolve_facet_regions()

# browser_cookie3 domain filter (same pattern as rdt-cli's ".reddit.com")
COOKIE_DOMAIN = ".sensortower.com"

COOKIE_DOMAIN_APP_HOST = "app.sensortower.com"

# At least one must be present after extraction (session actually logged in)
# Cookie requirements:
# - `sensor_tower_session` is commonly present after login and enough for autocomplete.
# - Some `/api/unified/*` endpoints additionally require ASP.NET session cookies.
REQUIRED_COOKIES = frozenset({"sensor_tower_session"})
ASPNET_SESSION_COOKIES = frozenset(
    {
        ".ASPXAUTH",
        "ASP.NET_SessionId",
        ".AspNet.ApplicationCookie",
        "sessionToken",
    }
)

# ST web app XHR to ``/api/*`` sends Origin + Referer + X-Requested-With (unlike reddit.com read API).
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/133.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": ST_ORIGIN,
    "Referer": f"{ST_ORIGIN}/",
    "X-Requested-With": "XMLHttpRequest",
    "sec-ch-ua": '"Chromium";v="133", "Not(A:Brand";v="99", "Google Chrome";v="133"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
}

# POST JSON body — merged with DEFAULT_HEADERS on client.post(..., json=...)
POST_JSON_HEADERS = {
    "Content-Type": "application/json",
}
