"""Sensor Tower session — same strategy as rdt-cli (subprocess cookie extract + httpx)."""

from __future__ import annotations

import json
import logging
from pathlib import Path
import shutil
import subprocess
import time
from typing import Any

import browser_cookie3
from browser_cookie3 import BrowserCookieError

from st_cli.constants import (
    ASPNET_SESSION_COOKIES,
    CONFIG_DIR,
    COOKIE_DOMAIN,
    COOKIE_DOMAIN_APP_HOST,
    CREDENTIAL_FILE,
    ST_CHROME_KEY_FILE,
    ST_CHROME_COOKIES_DB,
)

logger = logging.getLogger(__name__)

CREDENTIAL_TTL_DAYS = 7
_CREDENTIAL_TTL_SECONDS = CREDENTIAL_TTL_DAYS * 86400


class Credential:
    """Holds Sensor Tower session cookies."""

    def __init__(
        self,
        cookies: dict[str, str],
        *,
        source: str = "unknown",
        saved_at: float | None = None,
        last_verified_at: float | None = None,
    ):
        self.cookies = cookies
        self.source = source
        self.saved_at = saved_at
        self.last_verified_at = last_verified_at

    @property
    def is_valid(self) -> bool:
        return bool(self.cookies)

    def to_dict(self) -> dict[str, Any]:
        saved_at = self.saved_at or time.time()
        return {
            "cookies": self.cookies,
            "source": self.source,
            "saved_at": saved_at,
            "last_verified_at": self.last_verified_at,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Credential:
        return cls(
            cookies=data.get("cookies", {}),
            source=data.get("source", "saved"),
            saved_at=data.get("saved_at"),
            last_verified_at=data.get("last_verified_at"),
        )


def save_credential(credential: Credential) -> None:
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    if credential.saved_at is None:
        credential.saved_at = time.time()
    CREDENTIAL_FILE.write_text(json.dumps(credential.to_dict(), indent=2, ensure_ascii=False))
    CREDENTIAL_FILE.chmod(0o600)


def load_credential() -> Credential | None:
    if not CREDENTIAL_FILE.exists():
        return None
    try:
        data = json.loads(CREDENTIAL_FILE.read_text())
        cred = Credential.from_dict(data)
        if not cred.is_valid:
            return None
        saved_at = data.get("saved_at", 0)
        if saved_at and (time.time() - saved_at) > _CREDENTIAL_TTL_SECONDS:
            logger.info("Credential older than %d days, attempting browser refresh", CREDENTIAL_TTL_DAYS)
            fresh = extract_browser_credential()
            if fresh:
                logger.info("Auto-refreshed credential from browser")
                return fresh
            logger.warning("Cookie refresh failed; using existing cookies")
        return cred
    except (json.JSONDecodeError, KeyError):
        return None


def clear_credential() -> None:
    if CREDENTIAL_FILE.exists():
        CREDENTIAL_FILE.unlink()


def _cookies_have_session(cookies: dict[str, str]) -> bool:
    # Sensor Tower 的前端会话里，`sensor_tower_session` 通常足够覆盖 autocomplete。
    # 目前我们先以这个为准，便于继续验证 `/api/apps/facets`。
    return bool(cookies.get("sensor_tower_session"))


def extract_browser_credential() -> Credential | None:
    """Extract ST cookies from browsers — subprocess first (avoids SQLite lock), then in-process."""
    if shutil.which("uv"):
        cred = _extract_subprocess()
        if cred:
            return cred
    return _extract_direct()


def _guess_chrome_local_state() -> str | None:
    """Best-effort local state path for browser-cookie3 decryption."""
    candidates = [
        Path.home() / "Library" / "Application Support" / "Google" / "Chrome" / "Local State",
        Path.home() / "Library" / "Application Support" / "Chromium" / "Local State",
        Path.home()
        / "Library"
        / "Application Support"
        / "BraveSoftware"
        / "Brave-Browser"
        / "Local State",
        Path.home()
        / "Library"
        / "Application Support"
        / "Microsoft Edge"
        / "Local State",
    ]
    for p in candidates:
        try:
            if p.exists():
                return str(p)
        except OSError:
            continue
    return None


def _guess_chrome_cookie_db_files() -> list[str]:
    """Best-effort Chrome Cookies sqlite candidates across profiles."""
    if ST_CHROME_COOKIES_DB:
        return [ST_CHROME_COOKIES_DB]

    base_dir = Path.home() / "Library" / "Application Support" / "Google" / "Chrome"
    candidates: list[Path] = []
    default_db = base_dir / "Default" / "Cookies"
    if default_db.exists():
        candidates.append(default_db)

    # Try a few common profile indices.
    for i in range(1, 6):
        p = base_dir / f"Profile {i}" / "Cookies"
        if p.exists():
            candidates.append(p)

    return [str(p) for p in candidates]


def _extract_subprocess() -> Credential | None:
    """Same pattern as rdt-cli: ``uv run --with browser-cookie3 python3 -c ...``."""
    aspnet_keys = sorted(list(ASPNET_SESSION_COOKIES))
    chrome_cookie_files = _guess_chrome_cookie_db_files()
    chrome_key_file = ST_CHROME_KEY_FILE or _guess_chrome_local_state() or ""
    script = f'''
import browser_cookie3, json
ASPNET_KEYS = {aspnet_keys!r}
SENSOR_KEY = "sensor_tower_session"
cookies = {{}}
domains = ["{COOKIE_DOMAIN}", "{COOKIE_DOMAIN_APP_HOST}"]
def add_jar(jar):
    for c in jar:
        cookies[c.name] = c.value

chrome_cookie_files = {chrome_cookie_files!r}

best = {{}}
try:
    for cookie_file in chrome_cookie_files:
        temp = {{}}
        for domain_name in domains:
            if {chrome_key_file!r}:
                jar = browser_cookie3.chrome(
                    cookie_file=cookie_file,
                    key_file={chrome_key_file!r},
                    domain_name=domain_name,
                )
            else:
                jar = browser_cookie3.chrome(
                    cookie_file=cookie_file,
                    domain_name=domain_name,
                )
            for c in jar:
                temp[c.name] = c.value
        if temp.get(SENSOR_KEY):
            # Prefer cookie set that has at least one non-empty ASP.NET session cookie.
            if any(temp.get(k) for k in ASPNET_KEYS):
                cookies = temp
                break
            best = temp
    if cookies:
        pass
    elif best and best.get(SENSOR_KEY):
        cookies = best
except Exception:
    pass

if not cookies.get(SENSOR_KEY):
    for browser_fn in [browser_cookie3.firefox, browser_cookie3.edge, browser_cookie3.brave]:
        try:
            cookies = {{}}
            for domain_name in domains:
                jar = browser_fn(domain_name=domain_name)
                add_jar(jar)
            if cookies.get(SENSOR_KEY):
                break
        except Exception:
            continue
if cookies:
    print(json.dumps(cookies))
'''
    try:
        result = subprocess.run(
            ["uv", "run", "--with", "browser-cookie3", "python3", "-c", script],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode == 0 and result.stdout.strip():
            cookies = json.loads(result.stdout.strip())
            if _cookies_have_session(cookies):
                cred = Credential(cookies=cookies, source="browser:subprocess")
                save_credential(cred)
                return cred
    except OSError as e:
        logger.debug("Subprocess extraction failed: %s", e)
    return None


def _extract_direct() -> Credential | None:
    """Fallback: in-process extraction (may fail if browser holds DB lock)."""
    domains = [COOKIE_DOMAIN, COOKIE_DOMAIN_APP_HOST]
    chrome_key_file = ST_CHROME_KEY_FILE or _guess_chrome_local_state()
    chrome_cookie_files = _guess_chrome_cookie_db_files()
    best: dict[str, str] = {}
    if chrome_cookie_files:
        for cookie_file in chrome_cookie_files:
            try:
                cookies: dict[str, str] = {}
                for domain_name in domains:
                    if chrome_key_file:
                        jar = browser_cookie3.chrome(
                            cookie_file=cookie_file,
                            key_file=chrome_key_file,
                            domain_name=domain_name,
                        )
                    else:
                        jar = browser_cookie3.chrome(
                            cookie_file=cookie_file,
                            domain_name=domain_name,
                        )
                    for c in jar:
                        cookies[c.name] = c.value
                if cookies.get("sensor_tower_session"):
                    if any(cookies.get(k) for k in ASPNET_SESSION_COOKIES):
                        cred = Credential(cookies=cookies, source="browser:chrome:cookie_file")
                        save_credential(cred)
                        return cred
                    best = cookies
            except (OSError, BrowserCookieError):
                continue
    if best and _cookies_have_session(best):
        cred = Credential(cookies=best, source="browser:chrome:cookie_file")
        save_credential(cred)
        return cred

    for fn in [browser_cookie3.chrome, browser_cookie3.firefox, browser_cookie3.edge, browser_cookie3.brave]:
        try:
            cookies = {}
            for domain_name in domains:
                if fn is browser_cookie3.chrome and chrome_key_file:
                    jar = fn(domain_name=domain_name, key_file=chrome_key_file)
                else:
                    jar = fn(domain_name=domain_name)
                for c in jar:
                    cookies[c.name] = c.value
        except (OSError, BrowserCookieError):
            continue
        if _cookies_have_session(cookies):
            cred = Credential(cookies=cookies, source=f"browser:{fn.__name__}")
            save_credential(cred)
            return cred
    return None


def get_credential() -> Credential | None:
    cred = load_credential()
    if cred:
        return cred
    cred = extract_browser_credential()
    if cred:
        return cred
    return None
