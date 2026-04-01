from __future__ import annotations

import os
import json
import socket
import subprocess
import sys
import time
from dataclasses import dataclass
from urllib.parse import urlparse

import requests
from requests import ConnectionError as RequestsConnectionError
from requests import RequestException, Timeout as RequestsTimeout

from novel_tts.common.logging import get_logger
from novel_tts.config.models import BrowserDebugConfig, CrawlConfig, ProxyGatewayConfig, RedisConfig
from novel_tts.net import proxy_gateway as proxy_gateway_mod

from .challenge import ChallengePolicy
from .types import FetchResult

LOGGER = get_logger(__name__)
_TIMEOUT_STATUS_CODES = {408, 504, 522, 524}
_CRAWL_PROXY_TIMEOUT_CAP_SECONDS = 30.0
_CRAWL_PROXY_ATTEMPTS_WITH_BROWSER_FALLBACK = 2
_BROWSER_PROXY_CONNECT_TIMEOUT_SECONDS = 1.5


class ProxyTimeoutError(RequestsTimeout):
    def __init__(self, message: str, *, proxy_name: str = "", proxy_server: str = "") -> None:
        super().__init__(message)
        self.proxy_name = proxy_name
        self.proxy_server = proxy_server


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


def _serialize_cookie_jar(cookie_jar) -> str:
    parts: list[str] = []
    try:
        for cookie in cookie_jar:
            name = str(getattr(cookie, "name", "") or "").strip()
            value = str(getattr(cookie, "value", "") or "").strip()
            if name:
                parts.append(f"{name}={value}")
    except Exception:
        return ""
    return "; ".join(parts)


def _is_timeout_like_exception(exc: Exception) -> bool:
    if isinstance(exc, (RequestsTimeout, RequestsConnectionError)):
        return True
    lowered = str(exc).lower()
    return "timed out" in lowered or "timeout" in lowered


def _is_timeout_like_response(response: requests.Response) -> bool:
    status_code = int(getattr(response, "status_code", 0) or 0)
    if status_code in _TIMEOUT_STATUS_CODES:
        return True
    if status_code < 400:
        return False
    lowered = ((getattr(response, "text", "") or "")[:300] or "").lower()
    return "timed out" in lowered or "timeout" in lowered


def _resolve_proxy_names(cfg: ProxyGatewayConfig, redis_cfg: RedisConfig | None) -> list[str]:
    if not bool(getattr(cfg, "enabled", False)):
        return []
    if bool(getattr(cfg, "auto_discovery", True)):
        healthy, _reason = proxy_gateway_mod.load_healthy_proxy_names_from_redis(
            cfg=cfg,
            redis_cfg=redis_cfg,
        )
        return [str(item) for item in (healthy or []) if str(item).strip()]
    return [str(item) for item in (getattr(cfg, "proxies", None) or []) if str(item).strip()]


def _cfg_for_single_proxy(cfg: ProxyGatewayConfig, proxy_name: str) -> ProxyGatewayConfig:
    return ProxyGatewayConfig(
        enabled=True,
        base_url=cfg.base_url,
        mode=cfg.mode,
        auto_discovery=False,
        keys_per_proxy=cfg.keys_per_proxy,
        proxies=[proxy_name],
        direct_run_strategy="proxy_1",
    )


def _resolve_browser_proxy(
    cfg: ProxyGatewayConfig,
    redis_cfg: RedisConfig | None,
    *,
    preferred_proxy_name: str = "",
) -> tuple[str, str]:
    if not bool(getattr(cfg, "enabled", False)):
        return "", ""
    proxy_names = _resolve_proxy_names(cfg, redis_cfg)
    inventory = proxy_gateway_mod.load_proxy_inventory(cfg=cfg)
    if not inventory:
        return "", ""
    host_by_name = {
        str(item.get("name") or "").strip(): str(item.get("host") or "").strip()
        for item in inventory
        if str(item.get("name") or "").strip() and str(item.get("host") or "").strip()
    }
    ordered_names: list[str] = []
    preferred = str(preferred_proxy_name or "").strip()
    if preferred:
        ordered_names.append(preferred)
    for name in proxy_names:
        if name not in ordered_names:
            ordered_names.append(name)
    for item in inventory:
        name = str(item.get("name") or "").strip()
        if bool(item.get("is_healthy")) and name and name not in ordered_names:
            ordered_names.append(name)
    for name in ordered_names:
        host = host_by_name.get(name, "").strip()
        if host:
            return name, proxy_gateway_mod.normalize_browser_proxy_server(host)
    return "", ""


def _resolve_browser_proxy_candidates(
    cfg: ProxyGatewayConfig,
    redis_cfg: RedisConfig | None,
    *,
    preferred_proxy_name: str = "",
    preferred_proxy_server: str = "",
) -> list[tuple[str, str]]:
    candidates: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()

    preferred_name = str(preferred_proxy_name or "").strip()
    preferred_server = str(preferred_proxy_server or "").strip()
    if preferred_server:
        item = (preferred_name, preferred_server)
        candidates.append(item)
        seen.add(item)

    name, server = _resolve_browser_proxy(cfg, redis_cfg, preferred_proxy_name=preferred_proxy_name)
    if server:
        item = (name, server)
        if item not in seen:
            candidates.append(item)
            seen.add(item)

    if not bool(getattr(cfg, "enabled", False)):
        return candidates

    inventory = proxy_gateway_mod.load_proxy_inventory(cfg=cfg)
    if not inventory:
        return candidates

    proxy_names = _resolve_proxy_names(cfg, redis_cfg)
    ordered_names: list[str] = []
    if preferred_name:
        ordered_names.append(preferred_name)
    for item in proxy_names:
        if item not in ordered_names:
            ordered_names.append(item)
    for item in inventory:
        item_name = str(item.get("name") or "").strip()
        if bool(item.get("is_healthy")) and item_name and item_name not in ordered_names:
            ordered_names.append(item_name)

    host_by_name = {
        str(item.get("name") or "").strip(): str(item.get("host") or "").strip()
        for item in inventory
        if str(item.get("name") or "").strip() and str(item.get("host") or "").strip()
    }
    for item_name in ordered_names:
        host = host_by_name.get(item_name, "").strip()
        if not host:
            continue
        item = (item_name, proxy_gateway_mod.normalize_browser_proxy_server(host))
        if item not in seen:
            candidates.append(item)
            seen.add(item)
    return candidates


def _is_browser_proxy_reachable(proxy_server: str, *, timeout_seconds: float = _BROWSER_PROXY_CONNECT_TIMEOUT_SECONDS) -> bool:
    value = str(proxy_server or "").strip()
    if not value:
        return True
    try:
        parsed = urlparse(value)
    except Exception:
        return False
    host = str(parsed.hostname or "").strip()
    port = int(parsed.port or (443 if parsed.scheme == "https" else 80))
    if not host:
        return False
    try:
        with socket.create_connection((host, port), timeout=max(0.1, float(timeout_seconds))):
            return True
    except Exception:
        return False


def _request_with_proxy_rotation(
    method: str,
    url: str,
    *,
    headers: dict[str, str] | None,
    cfg: ProxyGatewayConfig,
    redis_cfg: RedisConfig | None,
    timeout_seconds: float,
    max_proxy_attempts: int | None = None,
    per_proxy_timeout_seconds: float | None = None,
) -> tuple[requests.Response, str, str]:
    proxy_names = _resolve_proxy_names(cfg, redis_cfg)
    if max_proxy_attempts is not None:
        try:
            max_attempts_int = max(1, int(max_proxy_attempts))
        except Exception:
            max_attempts_int = 1
        proxy_names = proxy_names[:max_attempts_int]
    if not proxy_names:
        response = proxy_gateway_mod.request(
            method,
            url,
            headers=headers,
            cfg=cfg,
            redis_cfg=redis_cfg,
            timeout_seconds=timeout_seconds,
        )
        return response, "", ""

    last_timeout_error: Exception | None = None
    last_proxy_name = ""
    last_proxy_server = ""
    effective_timeout = timeout_seconds
    if per_proxy_timeout_seconds is not None:
        effective_timeout = min(float(timeout_seconds), max(1.0, float(per_proxy_timeout_seconds)))
    for attempt_idx, proxy_name in enumerate(proxy_names, start=1):
        proxy_server = ""
        try:
            _resolved_name, proxy_server = _resolve_browser_proxy(
                cfg,
                redis_cfg,
                preferred_proxy_name=proxy_name,
            )
        except Exception:
            proxy_server = ""
        try:
            response = proxy_gateway_mod.request(
                method,
                url,
                headers=headers,
                cfg=_cfg_for_single_proxy(cfg, proxy_name),
                redis_cfg=redis_cfg,
                timeout_seconds=effective_timeout,
            )
        except Exception as exc:
            if not _is_timeout_like_exception(exc):
                raise
            last_timeout_error = exc
            last_proxy_name = proxy_name
            last_proxy_server = proxy_server
            LOGGER.warning(
                "crawl proxy timeout | url=%s proxy=%s attempt=%s/%s err=%s",
                url,
                proxy_name,
                attempt_idx,
                len(proxy_names),
                exc,
            )
            continue

        if _is_timeout_like_response(response):
            last_timeout_error = ProxyTimeoutError(
                f"Timeout-like proxy response status={response.status_code} for {url} via {proxy_name}"
                ,
                proxy_name=proxy_name,
                proxy_server=proxy_server,
            )
            last_proxy_name = proxy_name
            last_proxy_server = proxy_server
            LOGGER.warning(
                "crawl proxy timeout-like response | url=%s proxy=%s attempt=%s/%s status=%s",
                url,
                proxy_name,
                attempt_idx,
                len(proxy_names),
                response.status_code,
            )
            continue

        LOGGER.info(
            "crawl proxy success | url=%s proxy=%s attempt=%s/%s status=%s",
            url,
            proxy_name,
            attempt_idx,
            len(proxy_names),
            response.status_code,
        )
        return response, proxy_name, proxy_server

    raise ProxyTimeoutError(
        f"All healthy proxies timed out for {url}: {last_timeout_error}"
        if last_timeout_error is not None
        else f"All healthy proxies timed out for {url}",
        proxy_name=last_proxy_name,
        proxy_server=last_proxy_server,
    )


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

    def __init__(
        self,
        policy: ChallengePolicy,
        *,
        proxy_gateway: ProxyGatewayConfig | None = None,
        redis_cfg: RedisConfig | None = None,
    ) -> None:
        self.policy = policy
        self.proxy_gateway = proxy_gateway or ProxyGatewayConfig()
        self.redis_cfg = redis_cfg

    def fetch(self, url: str, timeout_seconds: int) -> FetchResult:
        cookie_header = os.environ.get("NOVEL_TTS_COOKIE_HEADER", "").strip()
        browser_fallback_enabled = self.policy.should_try_browser_fallback()
        response, proxy_name, proxy_server = _request_with_proxy_rotation(
            "GET",
            url,
            headers=_default_headers(cookie_header),
            cfg=self.proxy_gateway,
            redis_cfg=self.redis_cfg,
            timeout_seconds=timeout_seconds,
            max_proxy_attempts=(
                _CRAWL_PROXY_ATTEMPTS_WITH_BROWSER_FALLBACK if browser_fallback_enabled else None
            ),
            per_proxy_timeout_seconds=_CRAWL_PROXY_TIMEOUT_CAP_SECONDS,
        )
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
            proxy_name=proxy_name,
            proxy_server=proxy_server,
        )


class BootstrapHttpFetchStrategy(FetchStrategy):
    name = "http-session"

    def __init__(
        self,
        browser_config: BrowserDebugConfig,
        policy: ChallengePolicy,
        *,
        proxy_gateway: ProxyGatewayConfig | None = None,
        redis_cfg: RedisConfig | None = None,
    ) -> None:
        self.browser_config = browser_config
        self.policy = policy
        self.session = requests.Session()
        self.user_agent = _default_headers()["user-agent"]
        self.bootstrapped_at = 0.0
        self.proxy_gateway = proxy_gateway or ProxyGatewayConfig()
        self.redis_cfg = redis_cfg
        self._last_proxy_name = ""
        self._last_proxy_server = ""

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
        cookie_header = _serialize_cookie_jar(self.session.cookies)
        if cookie_header:
            headers["cookie"] = cookie_header
        browser_fallback_enabled = self.policy.should_try_browser_fallback()
        response, proxy_name, proxy_server = _request_with_proxy_rotation(
            "GET",
            url,
            headers=headers,
            cfg=self.proxy_gateway,
            redis_cfg=self.redis_cfg,
            timeout_seconds=timeout_seconds,
            max_proxy_attempts=(
                _CRAWL_PROXY_ATTEMPTS_WITH_BROWSER_FALLBACK if browser_fallback_enabled else None
            ),
            per_proxy_timeout_seconds=_CRAWL_PROXY_TIMEOUT_CAP_SECONDS,
        )
        self._last_proxy_name = proxy_name
        self._last_proxy_server = proxy_server
        return response

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
                proxy_name=self._last_proxy_name,
                proxy_server=self._last_proxy_server,
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
            proxy_name=self._last_proxy_name,
            proxy_server=self._last_proxy_server,
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

    def __init__(
        self,
        browser_config: BrowserDebugConfig,
        policy: ChallengePolicy,
        *,
        proxy_gateway: ProxyGatewayConfig | None = None,
        redis_cfg: RedisConfig | None = None,
    ) -> None:
        self.browser_config = browser_config
        self.policy = policy
        self.proxy_gateway = proxy_gateway or ProxyGatewayConfig()
        self.redis_cfg = redis_cfg
        self._playwright = None
        self._browser = None
        self._context = None
        self._page = None
        self._preferred_proxy_name = ""
        self._preferred_proxy_server = ""

    def set_preferred_proxy(self, *, proxy_name: str = "", proxy_server: str = "") -> None:
        self._preferred_proxy_name = str(proxy_name or "").strip()
        self._preferred_proxy_server = str(proxy_server or "").strip()

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
        proxy_candidates = _resolve_browser_proxy_candidates(
            self.proxy_gateway,
            self.redis_cfg,
            preferred_proxy_name=self._preferred_proxy_name,
            preferred_proxy_server=self._preferred_proxy_server,
        )
        reachable_candidates: list[tuple[str, str]] = []
        for proxy_name, proxy_server in proxy_candidates:
            if _is_browser_proxy_reachable(proxy_server):
                reachable_candidates.append((proxy_name, proxy_server))
            else:
                LOGGER.warning(
                    "browser proxy unreachable from local machine | proxy=%s proxy_server=%s",
                    proxy_name or "-",
                    proxy_server or "-",
                )
        if reachable_candidates:
            proxy_candidates = reachable_candidates
        else:
            proxy_candidates = [("", "")]
            if self.proxy_gateway.enabled:
                LOGGER.warning("No browser-reachable proxies available; falling back to direct browser fetch")

        screenshot_path = debug_dir / "crawl-page.png"
        last_exc: Exception | None = None
        for attempt_idx, (proxy_name, proxy_server) in enumerate(proxy_candidates, start=1):
            LOGGER.info(
                "browser fetch start for %s (mode=%s proxy=%s proxy_server=%s attempt=%s/%s)",
                url,
                self.browser_config.mode,
                proxy_name or "-",
                proxy_server or "-",
                attempt_idx,
                len(proxy_candidates),
            )
            try:
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
                            "proxy_server": proxy_server,
                        },
                    },
                    timeout_seconds=timeout_seconds,
                )
            except Exception as exc:
                last_exc = exc
                LOGGER.warning(
                    "browser fetch proxy failed | url=%s proxy=%s proxy_server=%s attempt=%s/%s err=%s",
                    url,
                    proxy_name or "-",
                    proxy_server or "-",
                    attempt_idx,
                    len(proxy_candidates),
                    exc,
                )
                continue

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
                proxy_name=proxy_name,
                proxy_server=proxy_server,
                debug_artifacts=debug_artifacts,
            )

        if last_exc is not None:
            raise last_exc
        raise RuntimeError(f"browser fetch failed for {url}")


@dataclass
class StrategyChain:
    policy: ChallengePolicy
    strategies: list[FetchStrategy]

    def fetch(self, url: str, timeout_seconds: int) -> FetchResult:
        last_result: FetchResult | None = None
        browser_proxy_name = ""
        browser_proxy_server = ""
        for strategy in self.strategies:
            if isinstance(strategy, BrowserFetchStrategy):
                strategy.set_preferred_proxy(
                    proxy_name=browser_proxy_name,
                    proxy_server=browser_proxy_server,
                )
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
                    browser_proxy_name = str(getattr(exc, "proxy_name", "") or "").strip()
                    browser_proxy_server = str(getattr(exc, "proxy_server", "") or "").strip()
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
                browser_proxy_name = result.proxy_name
                browser_proxy_server = result.proxy_server
                self.policy.launch_debug_browser(url, proxy_server=browser_proxy_server)
                time.sleep(2)
        if last_result is None:
            raise RuntimeError(f"No fetch strategy available for {url}")
        return last_result


def build_strategy_chain(
    crawl_config: CrawlConfig,
    browser_config: BrowserDebugConfig,
    *,
    proxy_gateway: ProxyGatewayConfig | None = None,
    redis_cfg: RedisConfig | None = None,
) -> StrategyChain:
    policy = ChallengePolicy(browser_config)
    strategies: list[FetchStrategy] = []
    if crawl_config.preferred_fetch_mode == "browser-bootstrap-http":
        strategies.append(
            BootstrapHttpFetchStrategy(
                browser_config,
                policy,
                proxy_gateway=proxy_gateway,
                redis_cfg=redis_cfg,
            )
        )
    elif crawl_config.preferred_fetch_mode != "browser-only":
        strategies.append(HttpFetchStrategy(policy, proxy_gateway=proxy_gateway, redis_cfg=redis_cfg))
    if policy.should_try_browser_fallback():
        strategies.append(
            BrowserFetchStrategy(
                browser_config,
                policy,
                proxy_gateway=proxy_gateway,
                redis_cfg=redis_cfg,
            )
        )
    return StrategyChain(policy=policy, strategies=strategies)
