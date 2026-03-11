import logging
import random
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Callable, Optional

import requests

from .config import ScraperConfig

logger = logging.getLogger(__name__)


class FetchError(Exception):
    pass


@dataclass
class FetchEvent:
    url: str
    outcome: str
    transport: str
    status_code: Optional[int]
    latency_ms: Optional[int]
    retry_after_seconds: Optional[int]
    error: Optional[str] = None


class Fetcher:
    def __init__(self, config: ScraperConfig) -> None:
        self._session = requests.Session()
        self._session.headers.update(
            {
                "User-Agent": config.user_agent,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-GB,en;q=0.9",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
            }
        )
        self._user_agent = config.user_agent
        self._delay = config.delay
        self._max_retries = config.max_retries
        self._backoff = config.backoff_factor
        self._jitter_ratio = max(0.0, min(1.0, float(config.jitter_ratio)))
        self._max_backoff = max(1, int(config.max_backoff_seconds))
        self._max_consecutive_failures = max(1, int(config.max_consecutive_failures))
        self._retry_after_cap = max(1, int(config.retry_after_cap_seconds))
        self._last_request = 0.0
        self._force_wget_until = 0.0
        self._consecutive_failures = 0
        self._cooldown_until = 0.0
        self._event_hook: Optional[Callable[[FetchEvent], None]] = None

    def set_event_hook(self, hook: Optional[Callable[[FetchEvent], None]]) -> None:
        self._event_hook = hook

    def _emit_event(self, event: FetchEvent) -> None:
        if self._event_hook is None:
            return
        try:
            self._event_hook(event)
        except Exception:  # pragma: no cover - hook failures should not kill fetches
            logger.debug("Fetch event hook failed", exc_info=True)

    def _jittered(self, value: float) -> float:
        if value <= 0 or self._jitter_ratio <= 0:
            return value
        delta = value * self._jitter_ratio
        return max(0.0, value + random.uniform(-delta, delta))

    def _wait(self) -> None:
        now = time.monotonic()
        if now < self._cooldown_until:
            wait_cooldown = self._cooldown_until - now
            logger.warning("Fetcher cooldown active; sleeping %.2f seconds", wait_cooldown)
            time.sleep(wait_cooldown)
            now = time.monotonic()
        if self._delay <= 0:
            return
        elapsed = now - self._last_request
        target = self._jittered(self._delay)
        if elapsed < target:
            remaining = target - elapsed
            logger.debug("Rate limiting: sleeping %.2f seconds", remaining)
            time.sleep(remaining)

    def _parse_retry_after(self, value: Optional[str]) -> Optional[int]:
        if not value:
            return None
        value = value.strip()
        if not value:
            return None
        if value.isdigit():
            return min(int(value), self._retry_after_cap)
        try:
            dt = parsedate_to_datetime(value)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            now = datetime.now(timezone.utc)
            seconds = max(0, int((dt - now).total_seconds()))
            return min(seconds, self._retry_after_cap)
        except (TypeError, ValueError, OverflowError):
            return None

    def _register_failure(self, suggested_backoff: Optional[float]) -> None:
        self._consecutive_failures += 1
        if suggested_backoff is not None:
            cooldown = min(self._max_backoff, suggested_backoff)
        else:
            cooldown = min(self._max_backoff, self._delay * (self._backoff ** max(1, self._consecutive_failures)))
        if self._consecutive_failures >= self._max_consecutive_failures:
            self._cooldown_until = max(self._cooldown_until, time.monotonic() + cooldown)

    def _register_success(self) -> None:
        self._consecutive_failures = 0
        self._cooldown_until = 0.0

    def fetch(self, url: str) -> str:
        last_error: Optional[Exception] = None
        for attempt in range(1, self._max_retries + 1):
            self._wait()
            now = time.monotonic()
            force_wget = now < self._force_wget_until
            if force_wget:
                try:
                    started = time.monotonic()
                    body = self._fetch_with_wget(url, timeout=45)
                    latency_ms = int((time.monotonic() - started) * 1000)
                    self._emit_event(
                        FetchEvent(
                            url=url,
                            outcome="success",
                            transport="wget",
                            status_code=200,
                            latency_ms=latency_ms,
                            retry_after_seconds=None,
                        )
                    )
                    self._register_success()
                    self._last_request = time.monotonic()
                    return body
                except FetchError as exc:
                    last_error = exc
                    self._register_failure(None)
                    self._emit_event(
                        FetchEvent(
                            url=url,
                            outcome="transient_fail",
                            transport="wget",
                            status_code=None,
                            latency_ms=None,
                            retry_after_seconds=None,
                            error=str(exc),
                        )
                    )
                    logger.warning(
                        "Fetch attempt %s for %s failed (%s)",
                        attempt,
                        url,
                        exc,
                    )
                    if attempt == self._max_retries:
                        break
                    backoff = self._delay * (self._backoff ** attempt)
                    logger.debug("Backing off %.2f seconds before retry", backoff)
                    time.sleep(backoff)
                continue
            try:
                started = time.monotonic()
                response = self._session.get(url, timeout=20)
                latency_ms = int((time.monotonic() - started) * 1000)
                status = response.status_code
                retry_after = self._parse_retry_after(response.headers.get("Retry-After"))
                if status in (406, 403):
                    logger.warning(
                        "Received %s from requests for %s, retrying with degraded wget path",
                        status,
                        url,
                    )
                    self._force_wget_until = time.monotonic() + 1800
                    body = self._fetch_with_wget(url, timeout=45)
                    self._emit_event(
                        FetchEvent(
                            url=url,
                            outcome="success",
                            transport="wget",
                            status_code=200,
                            latency_ms=latency_ms,
                            retry_after_seconds=retry_after,
                        )
                    )
                    self._register_success()
                    self._last_request = time.monotonic()
                    return body
                if status in (429, 503):
                    self._register_failure(float(retry_after) if retry_after else None)
                    self._emit_event(
                        FetchEvent(
                            url=url,
                            outcome="limited",
                            transport="requests",
                            status_code=status,
                            latency_ms=latency_ms,
                            retry_after_seconds=retry_after,
                            error=f"status={status}",
                        )
                    )
                    if retry_after:
                        self._cooldown_until = max(self._cooldown_until, time.monotonic() + retry_after)
                    raise requests.HTTPError(f"limited status {status}")
                if status >= 500:
                    self._register_failure(float(retry_after) if retry_after else None)
                    self._emit_event(
                        FetchEvent(
                            url=url,
                            outcome="transient_fail",
                            transport="requests",
                            status_code=status,
                            latency_ms=latency_ms,
                            retry_after_seconds=retry_after,
                            error=f"status={status}",
                        )
                    )
                    raise requests.HTTPError("transient status %s" % status)
                if status >= 400:
                    self._emit_event(
                        FetchEvent(
                            url=url,
                            outcome="permanent_fail",
                            transport="requests",
                            status_code=status,
                            latency_ms=latency_ms,
                            retry_after_seconds=retry_after,
                            error=f"status={status}",
                        )
                    )
                response.raise_for_status()
                self._emit_event(
                    FetchEvent(
                        url=url,
                        outcome="success",
                        transport="requests",
                        status_code=status,
                        latency_ms=latency_ms,
                        retry_after_seconds=retry_after,
                    )
                )
                self._register_success()
                self._last_request = time.monotonic()
                return response.text
            except requests.RequestException as exc:  # pragma: no cover - network behavior
                last_error = exc
                self._register_failure(None)
                self._emit_event(
                    FetchEvent(
                        url=url,
                        outcome="transient_fail",
                        transport="requests",
                        status_code=None,
                        latency_ms=None,
                        retry_after_seconds=None,
                        error=str(exc),
                    )
                )
                logger.warning(
                    "Fetch attempt %s for %s failed (%s)",
                    attempt,
                    url,
                    exc,
                )
                if attempt == self._max_retries:
                    break
                backoff = min(self._max_backoff, self._delay * (self._backoff ** attempt))
                backoff = self._jittered(backoff)
                logger.debug("Backing off %.2f seconds before retry", backoff)
                time.sleep(backoff)
        raise FetchError(f"Could not fetch {url}") from last_error

    def _fetch_with_wget(self, url: str, timeout: int) -> str:
        command = [
            "wget",
            "-qO-",
            f"--user-agent={self._user_agent}",
            "--header=Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "--header=Accept-Language: en-GB,en;q=0.9",
            "--header=Accept-Encoding: gzip, deflate, br",
            url,
        ]
        proc = subprocess.run(command, capture_output=True, text=True, timeout=timeout)
        if proc.returncode != 0:
            stderr = (proc.stderr or "").strip()
            raise FetchError(f"wget fallback failed for {url}: {stderr}")
        return proc.stdout
