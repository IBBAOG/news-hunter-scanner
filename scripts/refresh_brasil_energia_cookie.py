"""Refresh the Brasil Energia (brasilenergia.com.br) cookie used by the
SectorData clipping generator.

Brasil Energia's `be-auth` session cookie has a ~14-day TTL. When it expires the
SectorData /api/clipping path quietly falls back to public/teaser bodies (a
silent quality drop the user only notices after a digest goes out short).

This script runs on a cron (Mon + Thu 06:00 UTC, see
`.github/workflows/refresh_brasil_energia_cookie.yml`) and replaces the manual
"someone notices BE is broken and re-runs the login by hand" loop.

Flow:

  1. Log in via `news_hunter.brasilenergia_auth.get_auth().login()` — the same
     Python + requests path the news scanner already uses successfully against
     Cloudflare. The TS/undici / curl-impersonate path on Vercel could not beat
     Cloudflare; this Python path can.
  2. Dump the live cookie jar to the Netscape cookie body shape expected by
     SectorData's `public.clipping_cookies.cookies_netscape` text column —
     7 tab-separated fields per line, LF endings, NO header. ALL cookies are
     included (be-auth, .AspNetCore.Antiforgery.*, be_uuid, etc.) — the
     clipping path needs the antiforgery cookies too.
  3. UPDATE the SectorData Supabase row `domain = 'brasilenergia.com.br'`.
     FAIL HARD (exit 1) on zero rows affected — a missing row means the
     schema drifted or the row was deleted, and the cron should surface
     that as a red workflow.

Secrets required (already configured in IBBAOG/news-hunter-scanner repo
secrets — verified before workflow ships):

  BRASIL_ENERGIA_USER, BRASIL_ENERGIA_PASS  — BE subscriber login
  SUPABASE_URL, SUPABASE_SERVICE_KEY        — SectorData service-role for UPDATE

Cookie values are secrets and are NEVER printed; logs only carry cookie
names, counts, body length and rows-affected.
"""
from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timezone

# Cookies with no expiry (session cookies) get this far-future placeholder so
# downstream Netscape cookie-jar parsers don't drop them as expired.
_FAR_FUTURE_EPOCH = 2000000000  # 2033-05-18

_TARGET_DOMAIN = "brasilenergia.com.br"


def _format_netscape_line(cookie) -> str:
    """Format a single http.cookiejar.Cookie as one Netscape cookie body line.

    Netscape format = 7 tab-separated fields, LF terminated:

        <domain>\\t<include_subdomains>\\t<path>\\t<secure>\\t<expires>\\t<name>\\t<value>

    include_subdomains is "TRUE" if the cookie domain begins with a dot
    (per the historical Netscape spec), else "FALSE".
    """
    domain = cookie.domain or _TARGET_DOMAIN
    include_subdomains = "TRUE" if domain.startswith(".") else "FALSE"
    path = cookie.path or "/"
    secure = "TRUE" if cookie.secure else "FALSE"
    expires = int(cookie.expires) if cookie.expires else _FAR_FUTURE_EPOCH
    name = cookie.name or ""
    value = cookie.value or ""
    return f"{domain}\t{include_subdomains}\t{path}\t{secure}\t{expires}\t{name}\t{value}"


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    log = logging.getLogger("refresh_be_cookie")

    # --- 1. Login -------------------------------------------------------
    try:
        from news_hunter.brasilenergia_auth import get_auth
    except Exception as e:  # noqa: BLE001
        log.error("could not import news_hunter.brasilenergia_auth: %s", e)
        return 1

    auth = get_auth()
    if auth is None:
        log.error(
            "Brasil Energia auth is disabled (BRASIL_ENERGIA_USER / "
            "BRASIL_ENERGIA_PASS missing from the environment) — cannot refresh"
        )
        return 1

    ok = auth.login()
    if not ok:
        log.error(
            "Brasil Energia login failed — credentials, reCAPTCHA gate, or "
            "Cloudflare block. Check the brasilenergia_auth.py logs above."
        )
        return 1

    # The login populates auth._session. We read its cookie jar directly:
    # internal access is fine inside this same repo and avoids inventing a
    # public accessor for one caller.
    session = getattr(auth, "_session", None)
    if session is None:
        log.error("login() returned True but _session is None — internal bug")
        return 1

    cookies = list(session.cookies)
    if not cookies:
        log.error("login() returned True but cookie jar is empty — bug")
        return 1

    # --- 2. Format Netscape body ----------------------------------------
    cookie_names = [c.name for c in cookies]
    log.info("dumped %d cookies: %s", len(cookies), ", ".join(cookie_names))

    body_lines = [_format_netscape_line(c) for c in cookies]
    body = "\n".join(body_lines) + "\n"
    log.info("netscape body length: %d chars", len(body))

    if not any(c.name == "be-auth" for c in cookies):
        log.error(
            "be-auth cookie missing from the jar even though login() returned "
            "True — refusing to overwrite the live row with a half-baked body"
        )
        return 1

    # --- 3. UPDATE Supabase ---------------------------------------------
    supabase_url = os.environ.get("SUPABASE_URL", "").strip()
    supabase_key = os.environ.get("SUPABASE_SERVICE_KEY", "").strip()
    if not supabase_url or not supabase_key:
        log.error("SUPABASE_URL / SUPABASE_SERVICE_KEY missing — cannot UPDATE")
        return 1

    try:
        from supabase import create_client
    except Exception as e:  # noqa: BLE001
        log.error("could not import supabase: %s", e)
        return 1

    client = create_client(supabase_url, supabase_key)

    # supabase-py sends `updated_at` as a JSON string via PostgREST, so the
    # literal "now()" would land as text rather than trigger NOW(). Send a
    # real ISO8601 UTC timestamp instead.
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+00:00")

    try:
        resp = (
            client.table("clipping_cookies")
            .update({"cookies_netscape": body, "updated_at": now_iso})
            .eq("domain", _TARGET_DOMAIN)
            .execute()
        )
    except Exception as e:  # noqa: BLE001
        log.error("Supabase UPDATE failed: %s", e)
        return 1

    rows = resp.data or []
    rows_affected = len(rows)
    log.info("rows_affected: %d", rows_affected)

    if rows_affected == 0:
        log.error(
            "0 rows affected — the clipping_cookies row for domain=%s is "
            "missing. Schema drift or the row was deleted; refusing to "
            "silently no-op.",
            _TARGET_DOMAIN,
        )
        return 1

    log.info("OK — brasilenergia.com.br cookie refreshed in clipping_cookies")
    return 0


if __name__ == "__main__":
    sys.exit(main())
