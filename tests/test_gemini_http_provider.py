from __future__ import annotations

import os
from types import SimpleNamespace
from pathlib import Path

import pytest

from novel_tts.common.errors import RateLimitExceededError
from novel_tts.translate import providers


def test_gemini_generate_raises_on_429_and_does_not_retry(monkeypatch) -> None:
    os.environ["GEMINI_API_KEY"] = "test"

    acquire_calls: list[int] = []

    def fake_acquire(model: str, estimated_tokens: int) -> None:
        del model, estimated_tokens
        acquire_calls.append(1)

    request_calls: list[int] = []

    def fake_request(*args, **kwargs):
        del args, kwargs
        request_calls.append(1)
        return SimpleNamespace(
            status_code=429,
            headers={"Retry-After": "7"},
            content=b"1",
            json=lambda: {"error": {"message": "Too Many Requests", "status": "RESOURCE_EXHAUSTED"}},
            text="",
            raise_for_status=lambda: None,
        )

    monkeypatch.setattr(providers, "_acquire_gemini_rate_slot", fake_acquire)
    monkeypatch.setattr(providers.proxy_gateway_mod, "request", fake_request)
    monkeypatch.setattr(providers.time, "sleep", lambda *_args, **_kwargs: None)

    p = providers.GeminiHttpProvider()
    with pytest.raises(RateLimitExceededError) as exc:
        p.generate("gemma-3-27b-it", "hi", "sys")

    assert "429" in str(exc.value)
    assert "retry_after=7" in str(exc.value)
    assert len(request_calls) == 1
    assert len(acquire_calls) == 1


def test_gemini_generate_acquires_per_attempt_for_generic_retries(monkeypatch) -> None:
    os.environ["GEMINI_API_KEY"] = "test"

    acquire_calls: list[int] = []

    def fake_acquire(model: str, estimated_tokens: int) -> None:
        del model, estimated_tokens
        acquire_calls.append(1)

    calls = {"n": 0}

    def fake_request(*args, **kwargs):
        del args, kwargs
        calls["n"] += 1
        if calls["n"] < 3:
            raise RuntimeError("network error")
        return SimpleNamespace(
            status_code=200,
            json=lambda: {"candidates": [{"content": {"parts": [{"text": "ok"}]}}]},
            raise_for_status=lambda: None,
        )

    monkeypatch.setattr(providers, "_acquire_gemini_rate_slot", fake_acquire)
    monkeypatch.setattr(providers.proxy_gateway_mod, "request", fake_request)
    monkeypatch.setattr(providers.time, "sleep", lambda *_args, **_kwargs: None)

    p = providers.GeminiHttpProvider()
    assert p.generate("gemma-3-27b-it", "hi", "sys") == "ok"
    assert calls["n"] == 3
    assert len(acquire_calls) == 3


def test_gemini_generate_in_queue_worker_mode_releases_on_timeout(monkeypatch) -> None:
    os.environ["GEMINI_API_KEY"] = "test"
    os.environ["NOVEL_TTS_QUOTA_MODE"] = "raise"
    os.environ["NOVEL_TTS_QUOTA_MAX_WAIT_SECONDS"] = "0"

    acquire_calls: list[int] = []

    def fake_acquire(model: str, estimated_tokens: int) -> None:
        del model, estimated_tokens
        acquire_calls.append(1)

    request_calls: list[int] = []

    def fake_request(*args, **kwargs):
        del args, kwargs
        request_calls.append(1)
        raise providers.requests.exceptions.ReadTimeout("read timeout")

    monkeypatch.setattr(providers, "_acquire_gemini_rate_slot", fake_acquire)
    monkeypatch.setattr(providers.proxy_gateway_mod, "request", fake_request)
    monkeypatch.setattr(providers.time, "sleep", lambda *_args, **_kwargs: None)

    p = providers.GeminiHttpProvider()
    with pytest.raises(RateLimitExceededError) as exc:
        p.generate("gemma-3-27b-it", "hi", "sys")

    assert "timeout" in str(exc.value).lower()
    assert len(request_calls) == 1
    assert len(acquire_calls) == 1


@pytest.mark.parametrize("status_code", [403, 503])
def test_gemini_generate_in_queue_worker_mode_releases_on_transient_proxy_http(monkeypatch, status_code: int) -> None:
    os.environ["GEMINI_API_KEY"] = "test"
    os.environ["NOVEL_TTS_QUOTA_MODE"] = "raise"
    os.environ["NOVEL_TTS_QUOTA_MAX_WAIT_SECONDS"] = "0"

    acquire_calls: list[int] = []

    def fake_acquire(model: str, estimated_tokens: int) -> None:
        del model, estimated_tokens
        acquire_calls.append(1)

    request_calls: list[int] = []

    def fake_request(*args, **kwargs):
        del args, kwargs
        request_calls.append(1)
        return SimpleNamespace(
            status_code=status_code,
            url="http://localhost:8888/proxy",
            headers={},
            content=b"",
            text="proxy error",
            json=lambda: {},
            raise_for_status=lambda: (_ for _ in ()).throw(
                providers.requests.exceptions.HTTPError(f"{status_code} proxy")
            ),
        )

    monkeypatch.setattr(providers, "_acquire_gemini_rate_slot", fake_acquire)
    monkeypatch.setattr(providers.proxy_gateway_mod, "request", fake_request)
    monkeypatch.setattr(providers.time, "sleep", lambda *_args, **_kwargs: None)

    p = providers.GeminiHttpProvider(
        proxy_gateway=providers.ProxyGatewayConfig(enabled=True, base_url="http://localhost:8888")
    )
    with pytest.raises(RateLimitExceededError) as exc:
        p.generate("gemma-3-27b-it", "hi", "sys")

    assert "proxy transient http" in str(exc.value).lower()
    assert f"{status_code}" in str(exc.value)
    assert len(request_calls) == 1
    assert len(acquire_calls) == 1


def test_gemini_generate_in_queue_worker_mode_uses_long_cooldown_for_suspended_proxy_key(monkeypatch) -> None:
    os.environ["GEMINI_API_KEY"] = "test"
    os.environ["NOVEL_TTS_QUOTA_MODE"] = "raise"
    os.environ["NOVEL_TTS_QUOTA_MAX_WAIT_SECONDS"] = "0"

    def fake_request(*args, **kwargs):
        del args, kwargs
        return SimpleNamespace(
            status_code=403,
            url="http://localhost:8888/proxy",
            headers={},
            content=b"1",
            text="Permission denied: Consumer 'api_key:test' has been suspended.",
            json=lambda: {"error": {"message": "Permission denied: Consumer 'api_key:test' has been suspended."}},
            raise_for_status=lambda: (_ for _ in ()).throw(
                providers.requests.exceptions.HTTPError("403 proxy")
            ),
        )

    monkeypatch.setattr(providers, "_acquire_gemini_rate_slot", lambda *args, **kwargs: None)
    monkeypatch.setattr(providers.proxy_gateway_mod, "request", fake_request)
    monkeypatch.setattr(providers.time, "sleep", lambda *_args, **_kwargs: None)

    p = providers.GeminiHttpProvider(
        proxy_gateway=providers.ProxyGatewayConfig(enabled=True, base_url="http://localhost:8888")
    )
    with pytest.raises(RateLimitExceededError) as exc:
        p.generate("gemini-3.1-flash-lite-preview", "hi", "sys")

    assert "proxy transient http 403" in str(exc.value).lower()
    assert "suggested_wait=3600.00s" in str(exc.value)


@pytest.mark.parametrize("status_code", [500, 502, 503, 504])
def test_gemini_generate_in_queue_worker_mode_releases_on_transient_upstream_http(monkeypatch, status_code: int) -> None:
    os.environ["GEMINI_API_KEY"] = "test"
    os.environ["NOVEL_TTS_QUOTA_MODE"] = "raise"
    os.environ["NOVEL_TTS_QUOTA_MAX_WAIT_SECONDS"] = "0"

    acquire_calls: list[int] = []

    def fake_acquire(model: str, estimated_tokens: int) -> None:
        del model, estimated_tokens
        acquire_calls.append(1)

    request_calls: list[int] = []

    def fake_request(*args, **kwargs):
        del args, kwargs
        request_calls.append(1)
        return SimpleNamespace(
            status_code=status_code,
            url="https://generativelanguage.googleapis.com/v1beta/models/gemini-3.1-flash-lite-preview:generateContent?key=test",
            headers={},
            content=b"",
            text="temporary upstream error",
            json=lambda: {},
            raise_for_status=lambda: (_ for _ in ()).throw(
                providers.requests.exceptions.HTTPError(f"{status_code} upstream")
            ),
        )

    monkeypatch.setattr(providers, "_acquire_gemini_rate_slot", fake_acquire)
    monkeypatch.setattr(providers.proxy_gateway_mod, "request", fake_request)
    monkeypatch.setattr(providers.time, "sleep", lambda *_args, **_kwargs: None)

    p = providers.GeminiHttpProvider()
    with pytest.raises(RateLimitExceededError) as exc:
        p.generate("gemini-3.1-flash-lite-preview", "hi", "sys")

    assert "upstream transient http" in str(exc.value).lower()
    assert f"{status_code}" in str(exc.value)
    assert len(request_calls) == 1
    assert len(acquire_calls) == 1


def test_gemini_generate_uses_first_key_from_file_when_env_missing(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.delenv("GEMINI_RATE_LIMIT_KEY_PREFIX", raising=False)
    monkeypatch.delenv("NOVEL_TTS_CENTRAL_QUOTA", raising=False)
    monkeypatch.delenv("NOVEL_TTS_QUOTA_MODE", raising=False)
    monkeypatch.delenv("NOVEL_TTS_QUOTA_MAX_WAIT_SECONDS", raising=False)

    (tmp_path / ".secrets").mkdir(parents=True)
    (tmp_path / ".secrets" / "gemini-keys.txt").write_text("\n# comment\nfirst-key\nsecond-key\n", encoding="utf-8")

    seen_urls: list[str] = []

    def fake_request(method, url, **kwargs):
        del method, kwargs
        seen_urls.append(url)
        return SimpleNamespace(
            status_code=200,
            json=lambda: {"candidates": [{"content": {"parts": [{"text": "ok"}]}}]},
            raise_for_status=lambda: None,
        )

    monkeypatch.setattr(providers.proxy_gateway_mod, "request", fake_request)
    monkeypatch.setattr(providers, "_acquire_gemini_rate_slot", lambda *args, **kwargs: None)

    p = providers.GeminiHttpProvider(config=SimpleNamespace(storage=SimpleNamespace(root=tmp_path), queue=SimpleNamespace(redis=None), proxy_gateway=providers.ProxyGatewayConfig()))
    assert p.generate("gemma-3-27b-it", "hi", "sys") == "ok"
    assert seen_urls and "key=first-key" in seen_urls[0]


def test_gemini_generate_does_not_fallback_to_file_in_queue_worker_mode(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.setenv("NOVEL_TTS_QUOTA_MODE", "raise")
    monkeypatch.setenv("NOVEL_TTS_QUOTA_MAX_WAIT_SECONDS", "0")
    monkeypatch.setenv("NOVEL_TTS_CENTRAL_QUOTA", "1")
    monkeypatch.setenv("GEMINI_RATE_LIMIT_KEY_PREFIX", "novel_tts:novel:k1")

    (tmp_path / ".secrets").mkdir(parents=True)
    (tmp_path / ".secrets" / "gemini-keys.txt").write_text("first-key\n", encoding="utf-8")

    p = providers.GeminiHttpProvider(config=SimpleNamespace(storage=SimpleNamespace(root=tmp_path), queue=SimpleNamespace(redis=None), proxy_gateway=providers.ProxyGatewayConfig()))
    with pytest.raises(RuntimeError, match="Missing GEMINI_API_KEY"):
        p.generate("gemma-3-27b-it", "hi", "sys")
