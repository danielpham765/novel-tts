from __future__ import annotations

import os
import json
import subprocess
import sys
import time
from dataclasses import dataclass
from urllib.parse import urlparse

import requests
from requests import RequestException

from novel_tts.common.logging import get_logger
from novel_tts.config.models import BrowserDebugConfig, CrawlConfig

from .challenge import ChallengePolicy
from .types import FetchResult

LOGGER = get_logger(__name__)


def _is_playwright_sync_loop_error(exc: Exception) -> bool:
    return "Playwright Sync API inside the asyncio loop" in str(exc)


def _default_headers(cookie_header: str = "") -> dict[str, str]:
    headers = {
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
        "accept-language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
    }
    if cookie_header:
        headers["cookie"] = cookie_header
    return headers


def _run_playwright_worker(payload: dict[str, object], *, timeout_seconds: int) -> dict[str, object]:
    result = subprocess.run(
        [sys.executable, "-m", "novel_tts.crawl.playwright_worker"],
        input=json.dumps(payload, ensure_ascii=False),
        text=True,
        capture_output=True,
        timeout=max(10, int(timeout_seconds) + 15),
    )
    stdout = (result.stdout or "").strip()
    stderr = (result.stderr or "").strip()
    if not stdout:
        raise RuntimeError(stderr or f"playwright worker failed with exit code {result.returncode}")
    try:
        parsed = json.loads(stdout)
    except Exception as exc:
        raise RuntimeError(f"invalid worker output: {stdout[:400]}") from exc
    if result.returncode != 0 or not bool(parsed.get("ok", False)):
        raise RuntimeError(str(parsed.get("error", "")) or stderr or "playwright worker failed")
    return parsed


class FetchStrategy:
    name = "base"

    def fetch(self, url: str, timeout_seconds: int) -> FetchResult:
        raise NotImplementedError


class HttpFetchStrategy(FetchStrategy):
    name = "http"

    def __init__(self, policy: ChallengePolicy) -> None:
        self.policy = policy

    def fetch(self, url: str, timeout_seconds: int) -> FetchResult:
        cookie_header = os.environ.get("NOVEL_TTS_COOKIE_HEADER", "").strip()
        response = requests.get(url, headers=_default_headers(cookie_header), timeout=timeout_seconds)
        html = response.text
        title = ""
        if "<title>" in html.lower():
            lower = html.lower()
            start = lower.find("<title>")
            end = lower.find("</title>", start)
            if start != -1 and end != -1:
                title = html[start + 7 : end].strip()
        block_reason = self.policy.classify(html, title)
        challenge_detected = bool(block_reason) or response.status_code >= 400
        if response.status_code >= 400:
            LOGGER.warning("http fetch returned status %s for %s", response.status_code, url)
        return FetchResult(
            url=url,
            final_url=response.url,
            html=html,
            status_code=response.status_code,
            title=title,
            strategy_name=self.name,
            challenge_detected=challenge_detected,
            block_reason=block_reason,
        )


class BootstrapHttpFetchStrategy(FetchStrategy):
    name = "http-session"

    def __init__(self, browser_config: BrowserDebugConfig, policy: ChallengePolicy) -> None:
        self.browser_config = browser_config
        self.policy = policy
        self.session = requests.Session()
        self.user_agent = _default_headers()["user-agent"]
        self.bootstrapped_at = 0.0

    def _bootstrap_from_browser(self, url: str) -> None:
        if self.browser_config.mode != "debug-attach" or not self.browser_config.remote_debugging_url:
            raise RuntimeError("browser bootstrap requires debug-attach mode with remote_debugging_url")

        parsed = urlparse(url)
        origin = f"{parsed.scheme}://{parsed.netloc}"
        version_url = self.browser_config.remote_debugging_url.rstrip("/") + "/json/version"
        try:
            version_payload = requests.get(version_url, timeout=10).json()
            self.user_agent = version_payload.get("User-Agent", self.user_agent)
        except Exception:
            LOGGER.warning("Unable to read browser user-agent from %s", version_url)

        worker = _run_playwright_worker(
            {
                "action": "cookies",
                "url": origin,
                "browser_config": {
                    "remote_debugging_url": self.browser_config.remote_debugging_url,
                },
            },
            timeout_seconds=30,
        )
        cookies = list(worker.get("cookies", []) or [])

        self.session.cookies.clear()
        for cookie in cookies:
            self.session.cookies.set(
                cookie["name"],
                cookie["value"],
                domain=cookie.get("domain"),
                path=cookie.get("path"),
            )
        self.bootstrapped_at = time.time()
        LOGGER.info(
            "Bootstrapped HTTP session from browser | origin=%s cookies=%s ua=%s",
            origin,
            len(cookies),
            self.user_agent,
        )

    def _request(self, url: str, timeout_seconds: int) -> requests.Response:
        headers = _default_headers()
        headers["user-agent"] = self.user_agent
        return self.session.get(url, headers=headers, timeout=timeout_seconds)

    def fetch(self, url: str, timeout_seconds: int) -> FetchResult:
        if not self.session.cookies:
            self._bootstrap_from_browser(url)

        try:
            response = self._request(url, timeout_seconds)
        except RequestException:
            LOGGER.warning("http-session request failed for %s", url, exc_info=True)
            raise
        html = response.text
        title = ""
        if "<title>" in html.lower():
            lower = html.lower()
            start = lower.find("<title>")
            end = lower.find("</title>", start)
            if start != -1 and end != -1:
                title = html[start + 7 : end].strip()
        block_reason = self.policy.classify(html, title)
        challenge_detected = bool(block_reason) or response.status_code >= 400

        if challenge_detected:
            LOGGER.warning(
                "http-session hit %s status=%s at %s",
                block_reason or "http_error",
                response.status_code,
                url,
            )
            return FetchResult(
                url=url,
                final_url=response.url,
                html=html,
                status_code=response.status_code,
                title=title,
                strategy_name=self.name,
                challenge_detected=challenge_detected,
                block_reason=block_reason,
            )

        if response.status_code >= 400:
            LOGGER.warning("http-session returned status %s for %s", response.status_code, url)
        return FetchResult(
            url=url,
            final_url=response.url,
            html=html,
            status_code=response.status_code,
            title=title,
            strategy_name=self.name,
            challenge_detected=challenge_detected,
            block_reason=block_reason,
        )


class BrowserFetchStrategy(FetchStrategy):
    name = "browser"
    _stabilize_wait_ms = 5000
    _expand_text_candidates = (
        "展開全部",
        "展开全部",
        "點擊展開全部",
        "点击展开全部",
    )

    def __init__(self, browser_config: BrowserDebugConfig, policy: ChallengePolicy) -> None:
        self.browser_config = browser_config
        self.policy = policy
        self._playwright = None
        self._browser = None
        self._context = None
        self._page = None

    def _invalidate_attached_page(self) -> None:
        if self._page is not None:
            try:
                if not self._page.is_closed():
                    self._page.close()
            except Exception:
                LOGGER.debug("Failed to close attached worker page", exc_info=True)
        self._page = None

    def _get_attached_page(self, *, fresh: bool = False):
        if fresh:
            self._invalidate_attached_page()
        if self._page is not None and not self._page.is_closed():
            return self._page
        if self._playwright is None:
            from playwright.sync_api import sync_playwright

            self._playwright = sync_playwright().start()
        if self._browser is None:
            self._browser = self._playwright.chromium.connect_over_cdp(self.browser_config.remote_debugging_url)
        if self._context is None:
            self._context = self._browser.contexts[0] if self._browser.contexts else self._browser.new_context()
        # Keep a dedicated worker page for fallback instead of hijacking the user's active tab.
        self._page = self._context.new_page()
        return self._page

    def _expand_directory(self, page) -> None:
        for text in self._expand_text_candidates:
            try:
                locator = page.get_by_text(text, exact=False)
                if locator.count() <= 0:
                    continue
                locator.first.click(timeout=2000)
                page.wait_for_timeout(self._stabilize_wait_ms)
                LOGGER.info("browser fetch expanded directory using text=%s", text)
                return
            except Exception:
                LOGGER.debug("browser fetch expand attempt failed for text=%s", text, exc_info=True)

    def fetch(self, url: str, timeout_seconds: int) -> FetchResult:
        debug_artifacts = []
        debug_dir = self.policy.debug_image_dir()
        LOGGER.info("browser fetch start for %s (mode=%s)", url, self.browser_config.mode)
        screenshot_path = debug_dir / "crawl-page.png"
        worker = _run_playwright_worker(
            {
                "action": "fetch",
                "url": url,
                "timeout_ms": timeout_seconds * 1000,
                "stabilize_wait_ms": self._stabilize_wait_ms,
                "screenshot_path": str(screenshot_path),
                "expand_text_candidates": list(self._expand_text_candidates),
                "allow_fallback": True,
                "browser_config": {
                    "mode": self.browser_config.mode,
                    "remote_debugging_url": self.browser_config.remote_debugging_url,
                    "executable_path": self.browser_config.executable_path,
                    "user_data_dir": self.browser_config.user_data_dir,
                    "headless": self.browser_config.headless,
                },
            },
            timeout_seconds=timeout_seconds,
        )
        if screenshot_path.exists():
            debug_artifacts.append(screenshot_path)
            LOGGER.info("browser fetch saved screenshot to %s", screenshot_path)
        html = str(worker.get("html", "") or "")
        title = str(worker.get("title", "") or "")
        final_url = str(worker.get("final_url", "") or url)
        mode_used = str(worker.get("mode_used", "") or "").strip()
        if mode_used.startswith("standalone:"):
            LOGGER.warning(
                "browser debug-attach unavailable for %s via %s; falling back to standalone browser (%s)",
                url,
                self.browser_config.remote_debugging_url,
                mode_used.split(":", 1)[1],
            )
        block_reason = self.policy.classify(html, title)
        challenge_detected = bool(block_reason)
        return FetchResult(
            url=url,
            final_url=final_url,
            html=html,
            title=title,
            strategy_name=self.name,
            challenge_detected=challenge_detected,
            block_reason=block_reason,
            debug_artifacts=debug_artifacts,
        )


@dataclass
class StrategyChain:
    policy: ChallengePolicy
    strategies: list[FetchStrategy]

    def fetch(self, url: str, timeout_seconds: int) -> FetchResult:
        last_result: FetchResult | None = None
        for strategy in self.strategies:
            try:
                result = strategy.fetch(url, timeout_seconds)
            except Exception as exc:
                if strategy.name == "browser" and last_result is not None:
                    LOGGER.warning(
                        "browser fetch failed at %s; keeping previous %s result (%s)",
                        url,
                        last_result.strategy_name,
                        exc,
                    )
                    return last_result
                if _is_playwright_sync_loop_error(exc):
                    LOGGER.warning("%s fetch unavailable at %s (%s)", strategy.name, url, exc)
                elif isinstance(exc, RequestException):
                    LOGGER.warning("%s fetch request failed at %s (%s)", strategy.name, url, exc)
                else:
                    LOGGER.exception("%s fetch failed at %s", strategy.name, url)
                if strategy.name in {"http", "http-session"} and self.policy.should_try_browser_fallback():
                    continue
                raise
            last_result = result
            if not result.challenge_detected:
                return result
            reason = result.block_reason or "challenge"
            LOGGER.warning("%s fetch hit %s at %s", strategy.name, reason, url)
            if strategy.name in {"http", "http-session"} and not self.policy.should_try_browser_fallback():
                return result
            if strategy.name in {"http", "http-session"} and self.policy.browser_config.mode == "debug-launch":
                self.policy.launch_debug_browser(url)
                time.sleep(2)
        if last_result is None:
            raise RuntimeError(f"No fetch strategy available for {url}")
        return last_result


def build_strategy_chain(crawl_config: CrawlConfig, browser_config: BrowserDebugConfig) -> StrategyChain:
    policy = ChallengePolicy(browser_config)
    strategies: list[FetchStrategy] = []
    if crawl_config.preferred_fetch_mode == "browser-bootstrap-http":
        strategies.append(BootstrapHttpFetchStrategy(browser_config, policy))
    elif crawl_config.preferred_fetch_mode != "browser-only":
        strategies.append(HttpFetchStrategy(policy))
    if policy.should_try_browser_fallback():
        strategies.append(BrowserFetchStrategy(browser_config, policy))
    return StrategyChain(policy=policy, strategies=strategies)
