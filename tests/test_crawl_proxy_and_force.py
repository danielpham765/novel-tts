from __future__ import annotations

from pathlib import Path

import pytest
import requests

from novel_tts.config.models import (
    BrowserDebugConfig,
    CaptionConfig,
    CrawlConfig,
    MediaConfig,
    ModelsConfig,
    NovelConfig,
    ProxyGatewayConfig,
    QueueConfig,
    QueueModelConfig,
    SourceConfig,
    StorageConfig,
    TtsConfig,
    TranslationConfig,
    VideoConfig,
    VisualConfig,
)
from novel_tts.crawl import service as crawl_service
from novel_tts.crawl.challenge import ChallengePolicy
from novel_tts.crawl.service import SourceDiscoveryResult, crawl_range
from novel_tts.crawl.strategies import (
    BrowserFetchStrategy,
    CrawlProxySessionState,
    _request_with_proxy_rotation,
    _resolve_browser_proxy,
)


def _make_config(tmp_path: Path) -> NovelConfig:
    root = tmp_path
    input_dir = root / "input"
    output_dir = root / "output"
    storage = StorageConfig(
        root=root,
        input_dir=input_dir,
        output_dir=output_dir,
        image_dir=root / "image",
        logs_dir=root / ".logs",
        tmp_dir=root / "tmp",
    )
    crawl = CrawlConfig(site_id="test", chapter_batch_size=10, chapter_regex=r"^第(\d+)章([^\n]*)")
    browser_debug = BrowserDebugConfig(mode="auto")
    source = SourceConfig(source_id="test", resolver_id="test", crawl=crawl)
    models = ModelsConfig(
        provider="gemini_http",
        enabled_models=["dummy"],
        model_configs={"dummy": QueueModelConfig(chunk_max_len=4000, chunk_sleep_seconds=0.0)},
    )
    translation = TranslationConfig(
        chapter_regex=r"^第(\d+)章([^\n]*)",
        base_rules="",
        auto_update_glossary=True,
        glossary_file="",
    )
    return NovelConfig(
        novel_id="novel",
        title="Novel",
        slug="novel",
        source_language="zh",
        target_language="vi",
        source_id="test",
        source=source,
        storage=storage,
        crawl=crawl,
        models=models,
        translation=translation,
        captions=CaptionConfig(),
        queue=QueueConfig(),
        tts=TtsConfig(provider="local", voice="test"),
        media=MediaConfig(
            visual=VisualConfig(background_video=""),
            video=VideoConfig(),
        ),
        proxy_gateway=ProxyGatewayConfig(),
    )


def _make_entry(chapter_number: int) -> crawl_service.ChapterEntry:
    return crawl_service.ChapterEntry(
        chapter_number=chapter_number,
        title=f"第{chapter_number}章 标题",
        url=f"https://example.com/{chapter_number}",
    )


def _make_response(url: str, status_code: int = 200, body: str = "ok") -> requests.Response:
    response = requests.Response()
    response.status_code = status_code
    response._content = body.encode("utf-8")
    response.url = url
    response.encoding = "utf-8"
    return response


class _DummyRegistry:
    def get(self, _resolver_id: str):
        return object()


def test_crawl_range_skips_existing_chapters_without_force(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config = _make_config(tmp_path)
    config.storage.origin_dir.mkdir(parents=True, exist_ok=True)
    existing_body = "现有内容" * 50
    (config.storage.origin_dir / "chuong_1-10.txt").write_text(
        f"第1章 标题\n\n{existing_body}\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(crawl_service, "build_default_registry", lambda: _DummyRegistry())
    monkeypatch.setattr(
        crawl_service,
        "discover_source_entries",
        lambda *args, **kwargs: SourceDiscoveryResult(
            source_config=config.source,
            entries={1: _make_entry(1), 2: _make_entry(2)},
            latest_chapter=2,
        ),
    )

    fetched: list[int] = []

    def _fake_fetch(entry, _config, _resolver, _strategy_chain):
        fetched.append(entry.chapter_number)
        body = "新内容" * 50
        return (
            f"第{entry.chapter_number}章 标题\n\n{body}",
            entry.chapter_number,
            {
                "title": f"第{entry.chapter_number}章 标题",
                "chars": len(body),
                "parts": 1,
                "duration_seconds": 0.1,
                "strategy": "http",
                "final_url": entry.url,
            },
        )

    monkeypatch.setattr(crawl_service, "_fetch_chapter", _fake_fetch)
    monkeypatch.setattr(crawl_service.time, "sleep", lambda _seconds: None)

    outputs = crawl_range(config, 1, 2)

    assert fetched == [2]
    assert len(outputs) == 1
    merged = outputs[0].read_text(encoding="utf-8")
    assert "第1章 标题" in merged
    assert "第2章 标题" in merged


def test_crawl_range_force_recrawls_existing_chapters(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config = _make_config(tmp_path)
    config.storage.origin_dir.mkdir(parents=True, exist_ok=True)
    (config.storage.origin_dir / "chuong_1-10.txt").write_text(
        f"第1章 标题\n\n{'现有内容' * 50}\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(crawl_service, "build_default_registry", lambda: _DummyRegistry())
    monkeypatch.setattr(
        crawl_service,
        "discover_source_entries",
        lambda *args, **kwargs: SourceDiscoveryResult(
            source_config=config.source,
            entries={1: _make_entry(1), 2: _make_entry(2)},
            latest_chapter=2,
        ),
    )

    fetched: list[int] = []

    def _fake_fetch(entry, _config, _resolver, _strategy_chain):
        fetched.append(entry.chapter_number)
        body = "强制刷新内容" * 20
        return (
            f"第{entry.chapter_number}章 标题\n\n{body}",
            entry.chapter_number,
            {
                "title": f"第{entry.chapter_number}章 标题",
                "chars": len(body),
                "parts": 1,
                "duration_seconds": 0.1,
                "strategy": "http",
                "final_url": entry.url,
            },
        )

    monkeypatch.setattr(crawl_service, "_fetch_chapter", _fake_fetch)
    monkeypatch.setattr(crawl_service.time, "sleep", lambda _seconds: None)

    outputs = crawl_range(config, 1, 2, force=True)

    assert fetched == [1, 2]
    assert len(outputs) == 1
    merged = outputs[0].read_text(encoding="utf-8")
    assert merged.count("第1章 标题") == 1
    assert "强制刷新内容" in merged


def test_request_with_proxy_rotation_tries_next_proxy_on_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = ProxyGatewayConfig(enabled=True, auto_discovery=True, base_url="http://localhost:8888", mode="socket")
    monkeypatch.setattr(
        "novel_tts.crawl.strategies.proxy_gateway_mod.load_healthy_proxy_names_from_redis",
        lambda **kwargs: (["proxy-a", "proxy-b"], ""),
    )
    monkeypatch.setattr(
        "novel_tts.crawl.strategies.proxy_gateway_mod.load_proxy_inventory",
        lambda **kwargs: [
            {"name": "proxy-a", "host": "1.1.1.1:8080", "is_healthy": True},
            {"name": "proxy-b", "host": "2.2.2.2:8080", "is_healthy": True},
        ],
    )

    attempted: list[str] = []

    def _fake_request(method, url, *, headers=None, cfg=None, redis_cfg=None, timeout_seconds=None, body=None, key_index=None):
        del method, headers, redis_cfg, timeout_seconds, body, key_index
        attempted.append(cfg.proxies[0] if cfg and cfg.proxies else "direct")
        if attempted == ["proxy-a"]:
            raise requests.Timeout(f"timed out for {url}")
        return _make_response(url)

    monkeypatch.setattr("novel_tts.crawl.strategies.proxy_gateway_mod.request", _fake_request)

    response, proxy_name, proxy_server = _request_with_proxy_rotation(
        "GET",
        "https://example.com/chapter",
        headers={"user-agent": "ua"},
        cfg=cfg,
        redis_cfg=None,
        timeout_seconds=10,
    )

    assert attempted == ["proxy-a", "proxy-b"]
    assert response.status_code == 200
    assert proxy_name == "proxy-b"
    assert proxy_server == "http://2.2.2.2:8080"


def test_request_with_proxy_rotation_forces_direct_mode_for_crawl(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = ProxyGatewayConfig(enabled=True, auto_discovery=True, base_url="http://localhost:8888", mode="socket")
    monkeypatch.setattr(
        "novel_tts.crawl.strategies.proxy_gateway_mod.load_healthy_proxy_names_from_redis",
        lambda **kwargs: (["proxy-a"], ""),
    )
    monkeypatch.setattr(
        "novel_tts.crawl.strategies.proxy_gateway_mod.load_proxy_inventory",
        lambda **kwargs: [
            {"name": "proxy-a", "host": "1.1.1.1:8080", "is_healthy": True},
        ],
    )

    seen_modes: list[str] = []

    def _fake_request(method, url, *, headers=None, cfg=None, redis_cfg=None, timeout_seconds=None, body=None, key_index=None):
        del method, url, headers, redis_cfg, timeout_seconds, body, key_index
        seen_modes.append(str(getattr(cfg, "mode", "")))
        return _make_response("https://example.com/chapter")

    monkeypatch.setattr("novel_tts.crawl.strategies.proxy_gateway_mod.request", _fake_request)

    response, proxy_name, proxy_server = _request_with_proxy_rotation(
        "GET",
        "https://example.com/chapter",
        headers={"user-agent": "ua"},
        cfg=cfg,
        redis_cfg=None,
        timeout_seconds=10,
    )

    assert response.status_code == 200
    assert proxy_name == "proxy-a"
    assert proxy_server == "http://1.1.1.1:8080"
    assert seen_modes == ["direct"]


def test_request_with_proxy_rotation_falls_back_direct_when_no_healthy_proxy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = ProxyGatewayConfig(enabled=True, auto_discovery=True, base_url="http://localhost:8888", mode="socket")
    monkeypatch.setattr(
        "novel_tts.crawl.strategies.proxy_gateway_mod.load_healthy_proxy_names_from_redis",
        lambda **kwargs: (None, "proxy_list_missing"),
    )

    attempted: list[str] = []

    def _fake_request(method, url, *, headers=None, cfg=None, redis_cfg=None, timeout_seconds=None, body=None, key_index=None):
        del method, headers, redis_cfg, timeout_seconds, body, key_index
        attempted.append("direct" if not cfg.proxies else cfg.proxies[0])
        return _make_response(url)

    monkeypatch.setattr("novel_tts.crawl.strategies.proxy_gateway_mod.request", _fake_request)

    response, proxy_name, proxy_server = _request_with_proxy_rotation(
        "GET",
        "https://example.com/chapter",
        headers={"user-agent": "ua"},
        cfg=cfg,
        redis_cfg=None,
        timeout_seconds=10,
    )

    assert attempted == ["direct"]
    assert response.status_code == 200
    assert proxy_name == ""
    assert proxy_server == ""


def test_request_with_proxy_rotation_raises_after_all_proxies_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = ProxyGatewayConfig(enabled=True, auto_discovery=True, base_url="http://localhost:8888", mode="socket")
    monkeypatch.setattr(
        "novel_tts.crawl.strategies.proxy_gateway_mod.load_healthy_proxy_names_from_redis",
        lambda **kwargs: (["proxy-a", "proxy-b"], ""),
    )
    monkeypatch.setattr(
        "novel_tts.crawl.strategies.proxy_gateway_mod.load_proxy_inventory",
        lambda **kwargs: [
            {"name": "proxy-a", "host": "1.1.1.1:8080", "is_healthy": True},
            {"name": "proxy-b", "host": "2.2.2.2:8080", "is_healthy": True},
        ],
    )

    attempted: list[str] = []

    def _fake_request(method, url, *, headers=None, cfg=None, redis_cfg=None, timeout_seconds=None, body=None, key_index=None):
        del method, url, headers, redis_cfg, timeout_seconds, body, key_index
        attempted.append(cfg.proxies[0] if cfg and cfg.proxies else "direct")
        raise requests.Timeout("timed out")

    monkeypatch.setattr("novel_tts.crawl.strategies.proxy_gateway_mod.request", _fake_request)

    with pytest.raises(requests.Timeout, match="All healthy proxies timed out"):
        _request_with_proxy_rotation(
            "GET",
            "https://example.com/chapter",
            headers={"user-agent": "ua"},
            cfg=cfg,
            redis_cfg=None,
            timeout_seconds=10,
        )

    assert attempted == ["proxy-a", "proxy-b"]


def test_request_with_proxy_rotation_respects_attempt_limit_and_timeout_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = ProxyGatewayConfig(enabled=True, auto_discovery=True, base_url="http://localhost:8888", mode="socket")
    monkeypatch.setattr(
        "novel_tts.crawl.strategies.proxy_gateway_mod.load_healthy_proxy_names_from_redis",
        lambda **kwargs: (["proxy-a", "proxy-b", "proxy-c"], ""),
    )
    monkeypatch.setattr(
        "novel_tts.crawl.strategies.proxy_gateway_mod.load_proxy_inventory",
        lambda **kwargs: [
            {"name": "proxy-a", "host": "1.1.1.1:8080", "is_healthy": True},
            {"name": "proxy-b", "host": "2.2.2.2:8080", "is_healthy": True},
            {"name": "proxy-c", "host": "3.3.3.3:8080", "is_healthy": True},
        ],
    )

    calls: list[tuple[str, float | None]] = []

    def _fake_request(method, url, *, headers=None, cfg=None, redis_cfg=None, timeout_seconds=None, body=None, key_index=None):
        del method, url, headers, redis_cfg, body, key_index
        calls.append((cfg.proxies[0] if cfg and cfg.proxies else "direct", timeout_seconds))
        raise requests.Timeout("timed out")

    monkeypatch.setattr("novel_tts.crawl.strategies.proxy_gateway_mod.request", _fake_request)

    with pytest.raises(requests.Timeout):
        _request_with_proxy_rotation(
            "GET",
            "https://example.com/chapter",
            headers={"user-agent": "ua"},
            cfg=cfg,
            redis_cfg=None,
            timeout_seconds=120,
            max_proxy_attempts=2,
            per_proxy_timeout_seconds=30,
        )

    assert calls == [("proxy-a", 30.0), ("proxy-b", 30.0)]


def test_request_with_proxy_rotation_blacklists_proxy_after_two_timeouts(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = ProxyGatewayConfig(enabled=True, auto_discovery=True, base_url="http://localhost:8888", mode="socket")
    session_state = CrawlProxySessionState()
    now = {"value": 1000.0}
    monkeypatch.setattr("novel_tts.crawl.strategies.time.time", lambda: now["value"])
    monkeypatch.setattr(
        "novel_tts.crawl.strategies.proxy_gateway_mod.load_healthy_proxy_names_from_redis",
        lambda **kwargs: (["proxy-a", "proxy-b"], ""),
    )
    monkeypatch.setattr(
        "novel_tts.crawl.strategies.proxy_gateway_mod.load_proxy_inventory",
        lambda **kwargs: [
            {"name": "proxy-a", "host": "1.1.1.1:8080", "is_healthy": True},
            {"name": "proxy-b", "host": "2.2.2.2:8080", "is_healthy": True},
        ],
    )
    monkeypatch.setattr("novel_tts.crawl.strategies._ensure_proxy_recheck_worker", lambda *args, **kwargs: None)

    calls: list[str] = []

    def _fake_request(method, url, *, headers=None, cfg=None, redis_cfg=None, timeout_seconds=None, body=None, key_index=None):
        del method, url, headers, redis_cfg, timeout_seconds, body, key_index
        proxy_name = cfg.proxies[0] if cfg and cfg.proxies else "direct"
        calls.append(proxy_name)
        if proxy_name == "proxy-a":
            raise requests.Timeout("timed out")
        return _make_response("https://example.com")

    monkeypatch.setattr("novel_tts.crawl.strategies.proxy_gateway_mod.request", _fake_request)

    _request_with_proxy_rotation(
        "GET",
        "https://example.com/1",
        headers={"user-agent": "ua"},
        cfg=cfg,
        redis_cfg=None,
        timeout_seconds=10,
        proxy_session_state=session_state,
    )
    _request_with_proxy_rotation(
        "GET",
        "https://example.com/2",
        headers={"user-agent": "ua"},
        cfg=cfg,
        redis_cfg=None,
        timeout_seconds=10,
        proxy_session_state=session_state,
    )

    calls.clear()
    _request_with_proxy_rotation(
        "GET",
        "https://example.com/3",
        headers={"user-agent": "ua"},
        cfg=cfg,
        redis_cfg=None,
        timeout_seconds=10,
        proxy_session_state=session_state,
    )

    assert calls == ["proxy-b"]


def test_request_with_proxy_rotation_keeps_blacklisted_proxy_out_of_main_path_after_expiry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = ProxyGatewayConfig(enabled=True, auto_discovery=True, base_url="http://localhost:8888", mode="socket")
    session_state = CrawlProxySessionState(
        timeout_counts={},
        blacklisted_until={"proxy-a": 1300.0},
        recheck_running={"proxy-a"},
    )
    now = {"value": 1301.0}
    monkeypatch.setattr("novel_tts.crawl.strategies.time.time", lambda: now["value"])
    monkeypatch.setattr(
        "novel_tts.crawl.strategies.proxy_gateway_mod.load_healthy_proxy_names_from_redis",
        lambda **kwargs: (["proxy-a", "proxy-b"], ""),
    )
    monkeypatch.setattr(
        "novel_tts.crawl.strategies.proxy_gateway_mod.load_proxy_inventory",
        lambda **kwargs: [
            {"name": "proxy-a", "host": "1.1.1.1:8080", "is_healthy": True},
            {"name": "proxy-b", "host": "2.2.2.2:8080", "is_healthy": True},
        ],
    )

    calls: list[str] = []

    def _fake_request(method, url, *, headers=None, cfg=None, redis_cfg=None, timeout_seconds=None, body=None, key_index=None):
        del method, url, headers, redis_cfg, timeout_seconds, body, key_index
        proxy_name = cfg.proxies[0] if cfg and cfg.proxies else "direct"
        calls.append(proxy_name)
        return _make_response("https://example.com")

    monkeypatch.setattr("novel_tts.crawl.strategies.proxy_gateway_mod.request", _fake_request)

    _request_with_proxy_rotation(
        "GET",
        "https://example.com/after-expiry",
        headers={"user-agent": "ua"},
        cfg=cfg,
        redis_cfg=None,
        timeout_seconds=10,
        proxy_session_state=session_state,
    )
    assert calls == ["proxy-b"]


def test_background_recheck_removes_blacklist_after_success(monkeypatch: pytest.MonkeyPatch) -> None:
    from novel_tts.crawl import strategies as strategies_mod

    cfg = ProxyGatewayConfig(enabled=True, auto_discovery=True, base_url="http://localhost:8888", mode="socket")
    session_state = CrawlProxySessionState(
        blacklisted_until={"proxy-a": 1000.0},
        probe_urls={"proxy-a": "https://example.com/probe"},
    )
    monkeypatch.setattr("novel_tts.crawl.strategies.time.time", lambda: 1000.0)
    monkeypatch.setattr("novel_tts.crawl.strategies.time.sleep", lambda _seconds: None)
    monkeypatch.setattr("novel_tts.crawl.strategies._probe_proxy_recovery", lambda **kwargs: True)

    class _ImmediateThread:
        def __init__(self, *, target, name, daemon):
            del name, daemon
            self._target = target

        def start(self):
            self._target()

    monkeypatch.setattr("novel_tts.crawl.strategies.threading.Thread", _ImmediateThread)

    strategies_mod._ensure_proxy_recheck_worker(
        session_state,
        proxy_name="proxy-a",
        probe_url="https://example.com/probe",
        cfg=cfg,
        redis_cfg=None,
    )

    assert "proxy-a" not in session_state.blacklisted_until


def test_resolve_browser_proxy_prefers_requested_proxy_name(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = ProxyGatewayConfig(enabled=True, auto_discovery=True, base_url="http://localhost:8888", mode="socket")
    monkeypatch.setattr(
        "novel_tts.crawl.strategies.proxy_gateway_mod.load_healthy_proxy_names_from_redis",
        lambda **kwargs: (["proxy-a", "proxy-b"], ""),
    )
    monkeypatch.setattr(
        "novel_tts.crawl.strategies.proxy_gateway_mod.load_proxy_inventory",
        lambda **kwargs: [
            {"name": "proxy-a", "host": "1.1.1.1:8080", "is_healthy": True},
            {"name": "proxy-b", "host": "2.2.2.2:8080", "is_healthy": True},
        ],
    )

    proxy_name, proxy_server = _resolve_browser_proxy(cfg, None, preferred_proxy_name="proxy-b")

    assert proxy_name == "proxy-b"
    assert proxy_server == "http://2.2.2.2:8080"


def test_launch_debug_browser_adds_proxy_server(monkeypatch: pytest.MonkeyPatch) -> None:
    browser_cfg = BrowserDebugConfig(executable_path="/Applications/Google Chrome.app/Contents/MacOS/Google Chrome")
    policy = ChallengePolicy(browser_cfg)
    calls: list[list[str]] = []

    class _Popen:
        def __call__(self, args):
            calls.append(list(args))
            return object()

    monkeypatch.setattr("novel_tts.crawl.challenge.subprocess.Popen", _Popen())

    policy.launch_debug_browser("https://example.com", proxy_server="http://1.1.1.1:8080")

    assert calls
    assert "--proxy-server=http://1.1.1.1:8080" in calls[0]


def test_browser_fetch_rotates_across_proxy_candidates(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    browser_cfg = BrowserDebugConfig(mode="debug-attach", remote_debugging_url="http://127.0.0.1:9222")
    policy = ChallengePolicy(browser_cfg)
    strategy = BrowserFetchStrategy(browser_cfg, policy, proxy_gateway=ProxyGatewayConfig(enabled=True), redis_cfg=None)
    debug_dir = tmp_path / "debug"
    browser_cfg.debug_image_dir = str(debug_dir)

    monkeypatch.setattr(
        "novel_tts.crawl.strategies._resolve_browser_proxy_candidates",
        lambda *args, **kwargs: [
            ("proxy-a", "http://1.1.1.1:8080"),
            ("proxy-b", "http://2.2.2.2:8080"),
        ],
    )
    monkeypatch.setattr("novel_tts.crawl.strategies._is_browser_proxy_reachable", lambda *args, **kwargs: True)

    calls: list[str] = []

    def _fake_worker(payload, *, timeout_seconds):
        del timeout_seconds
        proxy_server = payload["browser_config"].get("proxy_server", "")
        calls.append(proxy_server)
        if proxy_server == "http://1.1.1.1:8080":
            raise RuntimeError("Page.goto: net::ERR_TUNNEL_CONNECTION_FAILED")
        return {
            "ok": True,
            "mode_used": "standalone:proxy_server_requested",
            "html": "<html><title>ok</title><body>done</body></html>",
            "title": "ok",
            "final_url": "https://example.com",
        }

    monkeypatch.setattr("novel_tts.crawl.strategies._run_playwright_worker", _fake_worker)

    result = strategy.fetch("https://example.com", 30)

    assert calls == ["http://1.1.1.1:8080", "http://2.2.2.2:8080"]
    assert result.proxy_name == "proxy-b"
    assert result.proxy_server == "http://2.2.2.2:8080"


def test_browser_fetch_falls_back_to_direct_when_all_proxies_unreachable(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    browser_cfg = BrowserDebugConfig(mode="debug-attach", remote_debugging_url="http://127.0.0.1:9222")
    browser_cfg.debug_image_dir = str(tmp_path / "debug")
    policy = ChallengePolicy(browser_cfg)
    strategy = BrowserFetchStrategy(browser_cfg, policy, proxy_gateway=ProxyGatewayConfig(enabled=True), redis_cfg=None)

    monkeypatch.setattr(
        "novel_tts.crawl.strategies._resolve_browser_proxy_candidates",
        lambda *args, **kwargs: [
            ("proxy-a", "http://1.1.1.1:8080"),
            ("proxy-b", "http://2.2.2.2:8080"),
        ],
    )
    monkeypatch.setattr("novel_tts.crawl.strategies._is_browser_proxy_reachable", lambda *args, **kwargs: False)

    calls: list[str] = []

    def _fake_worker(payload, *, timeout_seconds):
        del timeout_seconds
        calls.append(payload["browser_config"].get("proxy_server", ""))
        return {
            "ok": True,
            "mode_used": "standalone:proxy_server_requested",
            "html": "<html><title>ok</title><body>done</body></html>",
            "title": "ok",
            "final_url": "https://example.com",
        }

    monkeypatch.setattr("novel_tts.crawl.strategies._run_playwright_worker", _fake_worker)

    result = strategy.fetch("https://example.com", 30)

    assert calls == [""]
    assert result.proxy_name == ""
    assert result.proxy_server == ""
