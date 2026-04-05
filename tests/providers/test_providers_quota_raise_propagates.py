from __future__ import annotations

import json
import os
from types import SimpleNamespace

import pytest

from novel_tts.common.errors import RateLimitExceededError
from novel_tts.translate import providers as providers_mod


class _FakePipe:
    def __init__(self) -> None:
        self._unwatched = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def watch(self, *args, **kwargs):
        return None

    def zrangebyscore(self, *args, **kwargs):
        # active_members for the 60s window: 1 member (hits rpm=1).
        if kwargs.get("withscores"):
            return [("m", 0.0)]
        return ["m"]

    def hgetall(self, *args, **kwargs):
        return {"m": "0"}

    def zcount(self, *args, **kwargs):
        return 0

    def zrange(self, *args, **kwargs):
        return [("m", 0.0)]

    def unwatch(self):
        self._unwatched = True
        return None


class _FakeRedis:
    def pipeline(self):
        return _FakePipe()


def test_acquire_rate_slot_raise_is_not_swallowed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(providers_mod, "_get_rate_limit_client", lambda: _FakeRedis())

    monkeypatch.setenv(
        "GEMINI_MODEL_CONFIGS_JSON",
        json.dumps({"gemma-3-27b-it": {"rpm_limit": 1, "tpm_limit": 0, "rpd_limit": 0}}),
    )
    monkeypatch.setenv("GEMINI_RATE_LIMIT_KEY_PREFIX", "testprefix")
    monkeypatch.setenv("NOVEL_TTS_QUOTA_MODE", "raise")
    monkeypatch.setenv("NOVEL_TTS_QUOTA_MAX_WAIT_SECONDS", "0")

    with pytest.raises(RateLimitExceededError):
        providers_mod._acquire_gemini_rate_slot("gemma-3-27b-it", estimated_tokens=1)
