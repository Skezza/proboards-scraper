import pytest

from oatcake_scraper.config import ScraperConfig
from oatcake_scraper.fetcher import FetchError, Fetcher


class _FakeResponse:
    def __init__(self, status_code: int, text: str = "", headers=None, url: str = "https://example.com"):
        self.status_code = status_code
        self.text = text
        self.headers = headers or {}
        self.url = url

    def raise_for_status(self):
        if self.status_code >= 400:
            raise Exception(f"status={self.status_code}")


def test_fetch_success_event(monkeypatch):
    fetcher = Fetcher(ScraperConfig(delay=0, max_retries=1))
    events = []
    fetcher.set_event_hook(events.append)

    monkeypatch.setattr(fetcher._session, "get", lambda url, timeout=20: _FakeResponse(200, text="ok"))
    body = fetcher.fetch("https://example.com")
    assert body == "ok"
    assert events
    assert events[-1].outcome == "success"
    assert events[-1].transport == "requests"


def test_fetch_406_uses_wget_fallback(monkeypatch):
    fetcher = Fetcher(ScraperConfig(delay=0, max_retries=1))
    events = []
    fetcher.set_event_hook(events.append)

    monkeypatch.setattr(fetcher._session, "get", lambda url, timeout=20: _FakeResponse(406))
    monkeypatch.setattr(fetcher, "_fetch_with_wget", lambda url, timeout=45: "<html>ok</html>")
    body = fetcher.fetch("https://example.com")
    assert body == "<html>ok</html>"
    assert events[-1].outcome == "success"
    assert events[-1].transport == "wget"


def test_fetch_limited_status_emits_limited_event(monkeypatch):
    fetcher = Fetcher(ScraperConfig(delay=0, max_retries=1, retry_after_cap_seconds=5))
    events = []
    fetcher.set_event_hook(events.append)

    monkeypatch.setattr(
        fetcher._session,
        "get",
        lambda url, timeout=20: _FakeResponse(429, headers={"Retry-After": "30"}),
    )
    monkeypatch.setattr(fetcher, "_fetch_with_wget", lambda url, timeout=45: "unused")
    with pytest.raises(FetchError):
        fetcher.fetch("https://example.com")
    assert any(event.outcome == "limited" for event in events)


def test_fetch_solves_proboards_pow_challenge(monkeypatch):
    fetcher = Fetcher(ScraperConfig(delay=0, max_retries=1))
    events = []
    fetcher.set_event_hook(events.append)
    url = "https://oatcakefanzine.proboards.com"
    challenge = """
    <script>
    window.POW_CHALLENGE_DATA={
        challenge_nonce:'41255ad919557f21ebe6b545601f2248',
        challenge_hmac:'beeb3ae219e4856ffabb9e45',
        difficulty:'1',
        difficulty_char:'b',
        issued_at:'1780640782',
        cookie_duration:'3600',
        referrer:'(null)'
    };
    </script>
    """
    responses = [
        _FakeResponse(202, text=challenge, headers={"Retry-After": "0"}, url=url),
        _FakeResponse(200, text="<html>forum home</html>", url=url),
    ]

    monkeypatch.setattr(fetcher._session, "get", lambda request_url, timeout=20: responses.pop(0))

    assert fetcher.fetch(url) == "<html>forum home</html>"
    assert events[-1].outcome == "success"
    assert events[-1].status_code == 200
    cookie = fetcher._session.cookies.get("pow_bypass", domain="oatcakefanzine.proboards.com", path="/")
    assert cookie is not None
    assert cookie.startswith("41255ad919557f21ebe6b545601f2248|1780640782|")
