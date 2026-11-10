"""Brasil Energia auth: curl_cffi Chrome impersonation with a requests fallback.

2026-11-10: from the residential self-hosted runner a plain `requests` login
got `GET /login: HTTP 403` (Cloudflare rejects the TLS fingerprint) while
curl_cffi impersonating Chrome got a 200. These tests pin the client choice,
the fallback where curl_cffi is missing, the cookie-jar accessor the cookie
refresher depends on, the session-cache override and the login flow over a
scripted session (no network).

Run from repo root: python -m pytest tests/test_brasilenergia_auth.py -v
"""
from __future__ import annotations

import http.cookiejar
import http.server
import json
import os
import stat
import sys
import threading
import time

import pytest
import requests

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from news_hunter import brasilenergia_auth as be  # noqa: E402

LOGIN_HTML = (
    '<form method="post"><input type="hidden" name="__RequestVerificationToken" '
    'value="tok123"></form>'
)


@pytest.fixture(autouse=True)
def _cache_in_tmp(tmp_path, monkeypatch):
    monkeypatch.setenv(be.SESSION_CACHE_ENV, str(tmp_path / "be_session.json"))


# ---------------------------------------------------------------------------
# 1. client choice
# ---------------------------------------------------------------------------
def test_curl_cffi_is_the_client_when_installed():
    pytest.importorskip("curl_cffi")
    from curl_cffi import requests as cffi_requests

    s = be.new_http_session()
    assert be.http_client() == "curl_cffi"
    assert isinstance(s, cffi_requests.Session)
    # No hand-written UA over a Chrome fingerprint (bot signal).
    headers = {k.lower(): v for k, v in dict(s.headers).items()}
    assert "user-agent" not in headers
    assert headers.get("accept-language", "").startswith("pt-BR")


def test_falls_back_to_requests_without_curl_cffi(monkeypatch):
    monkeypatch.setattr(be, "cffi_requests", None)
    s = be.new_http_session()
    assert be.http_client() == "requests"
    assert isinstance(s, requests.Session)
    assert s.headers["User-Agent"].startswith("Mozilla/5.0")
    # br is never advertised on the requests path (no brotli decoder).
    assert "br" not in s.headers["Accept-Encoding"]


class _SetCookieHandler(http.server.BaseHTTPRequestHandler):
    """Answers like the BE login POST does on success: a Set-Cookie be-auth."""

    def do_GET(self):  # noqa: N802
        self.send_response(200)
        self.send_header("Set-Cookie", "be-auth=abc123; Path=/; HttpOnly")
        self.send_header("Set-Cookie", "be_uuid=u-1; Path=/")
        self.send_header("Content-Type", "text/html")
        self.send_header("Content-Length", "2")
        self.end_headers()
        self.wfile.write(b"ok")

    def log_message(self, *_a):  # keep pytest output quiet
        pass


@pytest.fixture
def set_cookie_server():
    srv = http.server.HTTPServer(("127.0.0.1", 0), _SetCookieHandler)
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    try:
        yield f"http://127.0.0.1:{srv.server_address[1]}/"
    finally:
        srv.shutdown()
        srv.server_close()


@pytest.mark.parametrize("client", ["curl_cffi", "requests"])
def test_cookie_jar_yields_cookie_objects_for_both_clients(monkeypatch, client, set_cookie_server):
    """Cookies set by a real HTTP response (not s.cookies.set), read back through
    cookie_jar() as http.cookiejar.Cookie objects: the shape the cookie
    refresher's Netscape dump iterates."""
    if client == "requests":
        monkeypatch.setattr(be, "cffi_requests", None)
    else:
        pytest.importorskip("curl_cffi")
    for var in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "ALL_PROXY", "all_proxy"):
        monkeypatch.delenv(var, raising=False)
    s = be.new_http_session()
    r = s.get(set_cookie_server, timeout=5)
    assert r.status_code == 200
    jar = be.cookie_jar(s)
    assert all(isinstance(c, http.cookiejar.Cookie) for c in jar)
    by_name = {c.name: c for c in jar}
    assert set(by_name) == {"be-auth", "be_uuid"}
    assert by_name["be-auth"].value == "abc123"
    assert by_name["be-auth"].path == "/"
    assert "127.0.0.1" in by_name["be-auth"].domain
    assert s.cookies.get_dict()["be-auth"] == "abc123"


@pytest.mark.skipif(os.name == "nt", reason="POSIX permission bits")
def test_cache_file_is_0600_and_dedicated_dir_0700(tmp_path, monkeypatch):
    target = tmp_path / "cache" / "be_session.json"
    monkeypatch.setenv(be.SESSION_CACHE_ENV, str(target))
    be._atomic_private_write(target, "{}")
    assert stat.S_IMODE(target.stat().st_mode) == 0o600
    assert stat.S_IMODE(target.parent.stat().st_mode) == 0o700


def test_atomic_write_replaces_and_leaves_no_tmp(tmp_path, monkeypatch):
    target = tmp_path / "be_session.json"
    monkeypatch.setenv(be.SESSION_CACHE_ENV, str(target))
    be._atomic_private_write(target, '{"v": 1}')
    be._atomic_private_write(target, '{"v": 2}')
    assert json.loads(target.read_text(encoding="utf-8")) == {"v": 2}
    assert [p.name for p in tmp_path.iterdir()] == ["be_session.json"]


# ---------------------------------------------------------------------------
# 2. session cache location
# ---------------------------------------------------------------------------
def test_session_cache_path_honours_env(tmp_path, monkeypatch):
    target = tmp_path / "sub" / "c.json"
    monkeypatch.setenv(be.SESSION_CACHE_ENV, str(target))
    assert be.session_cache_path() == target
    monkeypatch.delenv(be.SESSION_CACHE_ENV)
    assert be.session_cache_path() == be._SESSION_CACHE


def test_cache_roundtrip_through_the_override(tmp_path, monkeypatch):
    target = tmp_path / "sub" / "c.json"
    monkeypatch.setenv(be.SESSION_CACHE_ENV, str(target))
    auth = be.BrasilEnergiaAuth("u", "p")
    s = be.new_http_session()
    s.cookies.set("be-auth", "abc", domain="brasilenergia.com.br")
    auth._write_cache(s)
    assert json.loads(target.read_text(encoding="utf-8"))["cookies"]["be-auth"] == "abc"
    loaded = auth._load_cache()
    assert loaded is not None and loaded.cookies.get_dict()["be-auth"] == "abc"


def test_stale_cache_is_ignored(tmp_path, monkeypatch):
    target = tmp_path / "c.json"
    monkeypatch.setenv(be.SESSION_CACHE_ENV, str(target))
    target.write_text(json.dumps({"ts": time.time() - be._SESSION_MAX_AGE - 1,
                                  "cookies": {"be-auth": "old"}}), encoding="utf-8")
    assert be.BrasilEnergiaAuth("u", "p")._load_cache() is None


# ---------------------------------------------------------------------------
# 3. login flow over a scripted session
# ---------------------------------------------------------------------------
class _Resp:
    def __init__(self, status=200, text=""):
        self.status_code = status
        self.text = text

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class _Cookies(dict):
    def get_dict(self):
        return dict(self)

    def set(self, k, v, domain=""):  # noqa: ARG002
        self[k] = v


class _FakeSession:
    def __init__(self, get_status=200, set_auth=True):
        self.cookies = _Cookies()
        self.get_status = get_status
        self.set_auth = set_auth
        self.posted = None

    def get(self, url, **_k):
        return _Resp(self.get_status, LOGIN_HTML if self.get_status == 200 else "Just a moment...")

    def post(self, url, data=None, **_k):
        self.posted = data
        if self.set_auth:
            self.cookies["be-auth"] = "fresh"
            return _Resp(302)
        return _Resp(200, LOGIN_HTML)


def test_login_success_posts_token_and_caches(monkeypatch):
    fake = _FakeSession()
    auth = be.BrasilEnergiaAuth("user@x", "secret")
    monkeypatch.setattr(auth, "_new_session", lambda: fake)
    assert auth.login() is True
    assert fake.posted["__RequestVerificationToken"] == "tok123"
    assert fake.posted["LoginForm.Email"] == "user@x"
    cached = json.loads(be.session_cache_path().read_text(encoding="utf-8"))
    assert cached["cookies"]["be-auth"] == "fresh"


def test_login_page_403_fails_cleanly(monkeypatch, caplog):
    fake = _FakeSession(get_status=403)
    auth = be.BrasilEnergiaAuth("u", "p")
    monkeypatch.setattr(auth, "_new_session", lambda: fake)
    assert auth.login() is False
    assert fake.posted is None
    assert any("GET login page failed" in r.getMessage() for r in caplog.records)


def test_login_without_be_auth_cookie_fails(monkeypatch):
    auth = be.BrasilEnergiaAuth("u", "p")
    monkeypatch.setattr(auth, "_new_session", lambda: _FakeSession(set_auth=False))
    assert auth.login() is False
    assert not be.session_cache_path().exists()


def test_logged_out_detection():
    assert be.BrasilEnergiaAuth._looks_logged_out(_Resp(403))
    assert be.BrasilEnergiaAuth._looks_logged_out(_Resp(200, '<a href="/login?ReturnUrl=/x">'))
    assert not be.BrasilEnergiaAuth._looks_logged_out(_Resp(200, "<p>corpo completo</p>"))
    assert not be.BrasilEnergiaAuth._looks_logged_out(_Resp(503))
