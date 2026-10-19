"""Contract between the scanner and googlenewsdecoder's result dict.

2026-09-20 incident: googlenewsdecoder 0.2.x renamed the result flag from
`status` to `success`; the resolver only read `status`, so every Google News
URL stayed undecoded (resolve ok=0) for four weeks with no error anywhere.
"""
from news_hunter import enrich

GNEWS = "https://news.google.com/rss/articles/CBMiAAA?oc=5"
REAL = "https://www.reuters.com/business/energy/some-article-2026-10-19/"


def _with_decoder(monkeypatch, result):
    monkeypatch.setattr(enrich, "gnewsdecoder", lambda url, interval=0: result)


def test_legacy_status_key_decodes(monkeypatch):
    _with_decoder(monkeypatch, {"status": True, "decoded_url": REAL})
    assert enrich._resolve_google_news_url(GNEWS) == (REAL, "www.reuters.com")


def test_new_success_key_decodes(monkeypatch):
    _with_decoder(monkeypatch, {"success": True, "decoded_url": REAL})
    assert enrich._resolve_google_news_url(GNEWS) == (REAL, "www.reuters.com")


def test_failure_keeps_wrapper(monkeypatch):
    _with_decoder(monkeypatch, {"success": False, "message": "x"})
    assert enrich._resolve_google_news_url(GNEWS) == (GNEWS, "news.google.com")


def test_installed_decoder_is_importable():
    # A transitive break (selectolax 1.0 dropped the backend 0.1.7 imports)
    # turns the import into None and silences the whole resolve stage.
    assert enrich.gnewsdecoder is not None
