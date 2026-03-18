from __future__ import annotations

import os
from types import SimpleNamespace

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
