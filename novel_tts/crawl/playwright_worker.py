from __future__ import annotations

import json
import sys
from pathlib import Path
from urllib.parse import urlparse

_ATTACHED_WORKER_NAME = "novel-tts-watch-worker"


def _emit(payload: dict[str, object], *, code: int = 0) -> int:
    sys.stdout.write(json.dumps(payload, ensure_ascii=False))
    sys.stdout.flush()
    return code


def _default_browser_executable() -> str:
    candidates = [
        "/Applications/Chromium.app/Contents/MacOS/Chromium",
        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    ]
    for item in candidates:
        path = Path(item)
        if path.exists() and path.is_file():
            return str(path)
    return ""


def _browser_executable_candidates(explicit_path: str) -> list[str]:
    items: list[str] = []
    explicit = str(explicit_path or "").strip()
    if explicit:
        items.append(explicit)
    for candidate in [
        "/Applications/Chromium.app/Contents/MacOS/Chromium",
        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    ]:
        if candidate not in items and Path(candidate).exists():
            items.append(candidate)
    return items


def _expand_directory(page, candidates: list[str], stabilize_wait_ms: int) -> None:
    for text in candidates:
        try:
            locator = page.get_by_text(text, exact=False)
            if locator.count() <= 0:
                continue
            locator.first.click(timeout=2000)
            page.wait_for_timeout(stabilize_wait_ms)
            return
        except Exception:
            continue


def _page_window_name(page) -> str:
    try:
        return str(page.evaluate("() => window.name || ''") or "")
    except Exception:
        return ""


def _find_or_create_attached_page(context):
    for page in context.pages:
        try:
            if page.is_closed():
                continue
        except Exception:
            continue
        if _page_window_name(page) == _ATTACHED_WORKER_NAME:
            return page, False

    page = context.new_page()
    page.goto("about:blank", wait_until="load", timeout=10000)
    page.evaluate(f"() => {{ window.name = {_ATTACHED_WORKER_NAME!r}; }}")
    return page, True


def _connect_or_launch(playwright, browser_config: dict[str, object], *, allow_fallback: bool):
    mode = str(browser_config.get("mode", "") or "").strip()
    remote_debugging_url = str(browser_config.get("remote_debugging_url", "") or "").strip()
    executable_path = str(browser_config.get("executable_path", "") or "").strip()
    user_data_dir = str(browser_config.get("user_data_dir", "") or "").strip()
    headless = bool(browser_config.get("headless", False))
    browser_proxy_server = str(browser_config.get("proxy_server", "") or "").strip()

    if mode == "debug-attach" and remote_debugging_url and not browser_proxy_server:
        try:
            browser = playwright.chromium.connect_over_cdp(remote_debugging_url)
            context = browser.contexts[0] if browser.contexts else browser.new_context()
            page, created = _find_or_create_attached_page(context)
            mode_used = "debug-attach:new-tab" if created else "debug-attach:reuse-tab"
            return browser, context, page, mode_used
        except Exception as exc:
            if not allow_fallback:
                raise
            fallback_reason = str(exc)
    else:
        fallback_reason = "proxy_server_requested" if browser_proxy_server else ""

    if user_data_dir:
        last_exc: Exception | None = None
        for candidate in _browser_executable_candidates(executable_path) or [""]:
            try:
                launch_args: dict[str, object] = {
                    "user_data_dir": user_data_dir,
                    "headless": headless,
                    "executable_path": candidate or None,
                }
                if browser_proxy_server:
                    launch_args["proxy"] = {"server": browser_proxy_server}
                context = playwright.chromium.launch_persistent_context(**launch_args)
                page = context.pages[0] if context.pages else context.new_page()
                return None, context, page, ("standalone:" + fallback_reason if fallback_reason else "standalone")
            except Exception as exc:
                last_exc = exc
        assert last_exc is not None
        raise last_exc

    last_exc: Exception | None = None
    for candidate in _browser_executable_candidates(executable_path) or [_default_browser_executable()]:
        launch_args: dict[str, object] = {"headless": headless}
        if candidate:
            launch_args["executable_path"] = candidate
        if browser_proxy_server:
            launch_args["proxy"] = {"server": browser_proxy_server}
        try:
            browser = playwright.chromium.launch(**launch_args)
            context = browser.new_context()
            page = context.new_page()
            return browser, context, page, ("standalone:" + fallback_reason if fallback_reason else "standalone")
        except Exception as exc:
            last_exc = exc
    assert last_exc is not None
    raise last_exc


def _handle_fetch(payload: dict[str, object]) -> int:
    from playwright.sync_api import sync_playwright

    url = str(payload["url"])
    timeout_ms = int(payload.get("timeout_ms", 120000))
    stabilize_wait_ms = int(payload.get("stabilize_wait_ms", 5000))
    screenshot_path = str(payload.get("screenshot_path", "") or "").strip()
    expand_text_candidates = [str(item) for item in (payload.get("expand_text_candidates", []) or [])]
    browser_config = dict(payload.get("browser_config", {}) or {})
    allow_fallback = bool(payload.get("allow_fallback", True))

    playwright = sync_playwright().start()
    browser = None
    context = None
    try:
        browser, context, page, mode_used = _connect_or_launch(
            playwright,
            browser_config,
            allow_fallback=allow_fallback,
        )
        page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
        page.wait_for_timeout(stabilize_wait_ms)
        _expand_directory(page, expand_text_candidates, stabilize_wait_ms)
        if screenshot_path:
            path = Path(screenshot_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            page.screenshot(path=str(path), full_page=True)
        return _emit(
            {
                "ok": True,
                "mode_used": mode_used,
                "html": page.content(),
                "title": page.title(),
                "final_url": page.url,
            }
        )
    finally:
        try:
            if context is not None and not str(mode_used if 'mode_used' in locals() else "").startswith("debug-attach:"):
                context.close()
        except Exception:
            pass
        try:
            if browser is not None and not str(mode_used if 'mode_used' in locals() else "").startswith("debug-attach:"):
                browser.close()
        except Exception:
            pass
        try:
            playwright.stop()
        except Exception:
            pass


def _handle_cookies(payload: dict[str, object]) -> int:
    from playwright.sync_api import sync_playwright

    url = str(payload["url"])
    browser_config = dict(payload.get("browser_config", {}) or {})
    remote_debugging_url = str(browser_config.get("remote_debugging_url", "") or "").strip()
    if not remote_debugging_url:
        raise RuntimeError("browser bootstrap requires remote_debugging_url")
    parsed = urlparse(url)
    origin = f"{parsed.scheme}://{parsed.netloc}"

    playwright = sync_playwright().start()
    try:
        browser = playwright.chromium.connect_over_cdp(remote_debugging_url)
        context = browser.contexts[0] if browser.contexts else browser.new_context()
        cookies = context.cookies([origin])
        return _emit({"ok": True, "cookies": cookies})
    finally:
        try:
            playwright.stop()
        except Exception:
            pass


def main() -> int:
    raw = sys.stdin.read()
    if not raw.strip():
        return _emit({"ok": False, "error": "missing payload"}, code=2)
    try:
        payload = json.loads(raw)
    except Exception as exc:
        return _emit({"ok": False, "error": f"invalid json: {exc}"}, code=2)

    action = str(payload.get("action", "") or "").strip()
    try:
        if action == "fetch":
            return _handle_fetch(payload)
        if action == "cookies":
            return _handle_cookies(payload)
        return _emit({"ok": False, "error": f"unsupported action: {action}"}, code=2)
    except Exception as exc:
        return _emit({"ok": False, "error": str(exc)}, code=1)


if __name__ == "__main__":
    raise SystemExit(main())
