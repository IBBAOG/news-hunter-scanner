"""Authenticated scraping layer for Brasil Energia (brasilenergia.com.br).

Brasil Energia is an ASP.NET Core site behind a subscriber paywall. Anonymous
requests still return HTTP 200, but article bodies are truncated and the page
carries a login link plus "conteudo exclusivo / assinante" markers. With a
paying account we log in, obtain the `be-auth` session cookie, and fetch the
"ultimas noticias" listing + full article pages.

Login flow (reverse-engineered against the live site, 2026-06-10):

  1. GET  /login?ReturnUrl=<path>
       -> sets `.AspNetCore.Antiforgery.*` + `be_uuid` cookies
       -> the login <form> (POST, same URL) carries a hidden
          `__RequestVerificationToken` field.
  2. POST /login?ReturnUrl=<path>  (application/x-www-form-urlencoded)
       fields: Tipo=login, LoginForm.Email, LoginForm.Password,
               LoginForm.AcceptTerms=true, g-recaptcha-response="" (the server
               accepts an empty reCAPTCHA token for this account), and the
               __RequestVerificationToken read from step 1.
       success -> HTTP 302 to ReturnUrl + Set-Cookie `be-auth` (the session).
       failure -> HTTP 200 re-rendering the form, no `be-auth` cookie.

Expiry signal (the classic silent-expiry trap — a 200 that is really a logged-out
page): an authenticated request returns 401/403, OR a 200 whose body still shows
the login link (`/login?ReturnUrl`) or the paywall markers ("conteudo exclusivo",
"assinante"). `get()` detects this, re-logs in once, and retries the request.

Credentials come from the environment (BRASIL_ENERGIA_USER / BRASIL_ENERGIA_PASS).
If absent, the layer disables itself and callers fall back gracefully — the rest
of the scan keeps running. Never hardcode the credentials.

A best-effort on-disk cookie cache (.be_session.json, gitignored) lets cloud runs
reuse a session across the ~5 min cron invocations instead of logging in every
time. It is purely an optimization; a missing/stale cache just triggers login().
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# HTTP client (2026-11-10). Cloudflare rejects the TLS fingerprint of Python
# `requests` on brasilenergia.com.br: from the residential self-hosted runner a
# plain requests login got `GET /login: HTTP 403` while curl_cffi impersonating
# Chrome got a 200 from the same IP (SectorData
# scripts/clipping/refresh_brasil_energia_cookie.py, live since 2026-11-10). So
# the session is a curl_cffi Session(impersonate="chrome") whenever curl_cffi
# is importable, and falls back to requests.Session only where it is not.
# NOTE: from GitHub-hosted IPs NEITHER client passes (managed challenge on every
# page); that is why Brasil Energia is scanned from the self-hosted runner
# (SCAN_ONLY_SOURCES=brasil_energia) and skipped by scan.yml.
try:  # pragma: no cover - exercised by the import itself
    from curl_cffi import requests as cffi_requests
except Exception:  # noqa: BLE001
    cffi_requests = None  # type: ignore[assignment]

#: curl_cffi profile: the newest Chrome the installed curl_cffi ships.
IMPERSONATE = "chrome"

log = logging.getLogger(__name__)

BASE_URL = "https://brasilenergia.com.br"
LOGIN_PATH = "/login"
AUTH_COOKIE = "be-auth"

# On-disk cookie cache (optional optimization). Gitignored — see .gitignore.
_SESSION_CACHE = Path(__file__).resolve().parent.parent / ".be_session.json"
#: Overrides the cache location. The self-hosted job sets it OUTSIDE the
#: checkout: actions/checkout cleans the workspace every run, which would turn
#: every 30-min scan into a fresh login.
SESSION_CACHE_ENV = "BRASIL_ENERGIA_SESSION_CACHE"


def session_cache_path() -> Path:
    """Where the best-effort session cache lives (env override, else repo root)."""
    override = os.environ.get(SESSION_CACHE_ENV, "").strip()
    return Path(override).expanduser() if override else _SESSION_CACHE


def _atomic_private_write(path: Path, text: str) -> None:
    """Write `text` to `path` readable by the owner only, atomically.

    The file holds a live subscriber session. It is created 0600 from the start
    (os.open with a mode, never write-then-chmod, so there is no window where it
    is world-readable) under a temporary name and moved into place with
    os.replace, so a reader never sees a half-written file. The parent directory
    is made 0700 when it is the dedicated cache directory (env override) or
    when this call creates it; the repo-root default is never chmod'ed.
    """
    parent = path.parent
    created = not parent.exists()
    if created:
        parent.mkdir(parents=True, exist_ok=True)
    if created or os.environ.get(SESSION_CACHE_ENV, "").strip():
        try:
            os.chmod(parent, 0o700)
        except OSError:
            pass
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:  # a leftover tmp would keep its old mode: O_CREAT's mode only applies on creation
        os.unlink(tmp)
    except FileNotFoundError:
        pass
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(text)
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


# A cached session older than this is treated as stale and re-logged in.
_SESSION_MAX_AGE = 6 * 3600  # 6 hours

# Brotli is intentionally NOT advertised: requests does not decode `br` unless
# the brotli package is installed, and an undecoded br body parses as an empty
# page silently (bit the Porto de Itaqui scraper before). Stick to gzip/deflate.
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate",
}

# curl_cffi path: NO User-Agent / Accept / Accept-Encoding by hand. curl_cffi
# emits the impersonated browser's own (a hand-written UA over a Chrome TLS
# fingerprint is itself a bot signal) and decodes `br` natively.
_CFFI_HEADERS = {
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
}


def http_client() -> str:
    """'curl_cffi' or 'requests': the client new sessions are built with."""
    return "curl_cffi" if cffi_requests is not None else "requests"


def new_http_session():
    """A Session for brasilenergia.com.br: curl_cffi Chrome impersonation when
    available, else requests with browser-like headers (see the note above)."""
    if cffi_requests is not None:
        s = cffi_requests.Session(impersonate=IMPERSONATE)
        s.headers.update(_CFFI_HEADERS)
        return s
    s = requests.Session()
    s.headers.update(_HEADERS)
    return s


def cookie_jar(session) -> list:
    """The session's cookies as http.cookiejar.Cookie objects, for either client.

    Iterating a requests jar yields Cookie objects; iterating curl_cffi's
    Cookies mapping yields NAMES, its Cookie objects live in `.jar`.
    """
    cookies = session.cookies
    jar = getattr(cookies, "jar", None)
    return list(jar if jar is not None else cookies)

# Substrings whose presence in a 200 body means the page is logged-out /
# paywalled (i.e. the session is missing or expired). Lowercased compare.
_LOGGED_OUT_MARKERS = (
    "/login?returnurl",
    "conteúdo exclusivo",   # "conteúdo exclusivo"
    "exclusivo para assinantes",
)

_TIMEOUT = 20

_lock = threading.Lock()


class BrasilEnergiaAuth:
    """Holds the authenticated HTTP session and renews it on expiry.

    The session is a curl_cffi Session (Chrome impersonation) when curl_cffi is
    installed, else a requests.Session; both expose the same get/post/cookies
    surface used here.
    """

    def __init__(self, user: str, password: str) -> None:
        self._user = user
        self._password = password
        self._session = None
        log.info("Brasil Energia: HTTP client %s", http_client())

    # -- login -------------------------------------------------------------
    def _new_session(self):
        return new_http_session()

    def login(self) -> bool:
        """Perform a fresh login. Returns True on success.

        On success the live session (with the `be-auth` cookie) is stored in
        memory and written to the on-disk cache.
        """
        s = self._new_session()
        login_url = f"{BASE_URL}{LOGIN_PATH}?ReturnUrl=%2F"
        try:
            r = s.get(login_url, timeout=_TIMEOUT)
            r.raise_for_status()
        except Exception as e:  # noqa: BLE001
            log.warning("Brasil Energia: GET login page failed (%s): %s", http_client(), e)
            return False

        token = self._read_token(r.text)
        if not token:
            log.warning("Brasil Energia: could not find __RequestVerificationToken on login page")
            return False

        payload = {
            "Tipo": "login",
            "g-recaptcha-response": "",
            "LoginForm.Email": self._user,
            "LoginForm.Password": self._password,
            "LoginForm.AcceptTerms": "true",
            "__RequestVerificationToken": token,
        }
        try:
            resp = s.post(
                login_url,
                data=payload,
                timeout=_TIMEOUT,
                allow_redirects=False,
                headers={
                    "Referer": login_url,
                    "Origin": BASE_URL,
                    "Content-Type": "application/x-www-form-urlencoded",
                },
            )
        except Exception as e:  # noqa: BLE001
            log.warning("Brasil Energia: login POST failed: %s", e)
            return False

        # Success = a 302 to the ReturnUrl AND the be-auth cookie is set.
        if AUTH_COOKIE not in s.cookies.get_dict():
            log.warning(
                "Brasil Energia: login did not yield a %s cookie (status %s) — "
                "check credentials", AUTH_COOKIE, resp.status_code,
            )
            return False

        self._session = s
        self._write_cache(s)
        log.info("Brasil Energia: logged in (be-auth cookie acquired)")
        return True

    @staticmethod
    def _read_token(html: str) -> str | None:
        try:
            soup = BeautifulSoup(html, "lxml")
        except Exception:  # noqa: BLE001
            return None
        tag = soup.find("input", attrs={"name": "__RequestVerificationToken"})
        if tag and tag.get("value"):
            return tag["value"].strip()
        return None

    # -- on-disk cache (best-effort) --------------------------------------
    def _write_cache(self, s) -> None:
        try:
            data = {"ts": time.time(), "cookies": s.cookies.get_dict()}
            _atomic_private_write(session_cache_path(), json.dumps(data))
        except Exception as e:  # noqa: BLE001
            log.debug("Brasil Energia: could not write session cache: %s", e)

    def _load_cache(self):
        try:
            path = session_cache_path()
            if not path.exists():
                return None
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception as e:  # noqa: BLE001
            log.debug("Brasil Energia: could not read session cache: %s", e)
            return None
        if time.time() - float(data.get("ts", 0)) > _SESSION_MAX_AGE:
            return None
        cookies = data.get("cookies") or {}
        if AUTH_COOKIE not in cookies:
            return None
        s = self._new_session()
        for k, v in cookies.items():
            s.cookies.set(k, v, domain="brasilenergia.com.br")
        return s

    def invalidate(self) -> None:
        """Drop the in-memory session and the on-disk cache (for testing/expiry)."""
        self._session = None
        try:
            session_cache_path().unlink(missing_ok=True)
        except Exception:  # noqa: BLE001
            pass

    # -- authenticated GET -------------------------------------------------
    def _ensure_session(self) -> bool:
        if self._session is not None:
            return True
        cached = self._load_cache()
        if cached is not None:
            self._session = cached
            log.debug("Brasil Energia: reusing cached session")
            return True
        return self.login()

    @staticmethod
    def _looks_logged_out(resp) -> bool:
        if resp.status_code in (401, 403):
            return True
        if resp.status_code != 200:
            # Other errors (5xx, etc.) are not an auth problem; let the caller
            # treat the response as-is.
            return False
        body = resp.text.lower()
        return any(m in body for m in _LOGGED_OUT_MARKERS)

    def get(self, url: str, *, timeout: int = _TIMEOUT):
        """Authenticated GET with transparent re-login on expiry.

        Returns the Response on success, or None if it could not obtain an
        authenticated page (so callers skip Brasil Energia gracefully without
        crashing the scan).
        """
        if not self._ensure_session():
            return None
        assert self._session is not None

        for attempt in (1, 2):
            try:
                resp = self._session.get(url, timeout=timeout, allow_redirects=True)
            except Exception as e:  # noqa: BLE001
                log.warning("Brasil Energia: GET %s failed: %s", url, e)
                return None

            if not self._looks_logged_out(resp):
                return resp

            if attempt == 1:
                log.info("Brasil Energia: session expired on %s — re-logging in", url)
                self._session = None
                if not self.login():
                    return None
            else:
                log.warning(
                    "Brasil Energia: still logged out after re-login on %s — giving up",
                    url,
                )
                return None
        return None


# -- module-level singleton ------------------------------------------------
_instance: "BrasilEnergiaAuth | None" = None
_init_tried = False


def get_auth() -> "BrasilEnergiaAuth | None":
    """Return the shared auth client, or None if credentials are not configured.

    Reads BRASIL_ENERGIA_USER / BRASIL_ENERGIA_PASS from the environment. When
    they are absent (e.g. a run without the GitHub Actions secrets), returns
    None so callers skip Brasil Energia with a logged warning — never crashing.
    """
    global _instance, _init_tried
    if _instance is not None:
        return _instance
    with _lock:
        if _instance is None and not _init_tried:
            _init_tried = True
            user = os.environ.get("BRASIL_ENERGIA_USER", "").strip()
            password = os.environ.get("BRASIL_ENERGIA_PASS", "").strip()
            if not user or not password:
                log.info(
                    "Brasil Energia auth disabled (BRASIL_ENERGIA_USER/"
                    "BRASIL_ENERGIA_PASS absent) — source will be skipped"
                )
                return None
            _instance = BrasilEnergiaAuth(user, password)
    return _instance


def is_brasilenergia_domain(domain: str) -> bool:
    d = domain.lower()
    return d in ("brasilenergia.com.br", "www.brasilenergia.com.br")
