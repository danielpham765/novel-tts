"""Live smoke-test: calls the real Gemini API and checks for a valid response.

Run with:
    uv run pytest tests/test_llm_live.py -s -v

Requires GEMINI_API_KEY env-var or .secrets/gemini-keys.txt to be present.
Skip this test in CI by not setting the API key.
"""
from __future__ import annotations

import os
import time

import pytest

from novel_tts.translate import providers


MODEL = os.environ.get("NOVEL_TTS_TEST_LLM_MODEL", "gemma-4-26b-it")
PROMPT = "Chỉ dịch sang tiếng Việt, không giải thích: 第1章 黑缎缠目. 炎炎八月。滴滴滴——！"


def _has_api_key() -> bool:
    env_key = os.environ.get("GEMINI_API_KEY", "").strip()
    if env_key:
        return True
    keys_file = providers._repo_root() / ".secrets" / "gemini-keys.txt"
    if keys_file.exists():
        for line in keys_file.read_text(encoding="utf-8").splitlines():
            if line.strip() and not line.strip().startswith("#"):
                return True
    return False


@pytest.mark.skipif(not _has_api_key(), reason="No GEMINI_API_KEY configured")
def test_llm_live_ping() -> None:
    p = providers.GeminiHttpProvider()
    t0 = time.monotonic()
    response = p.generate(MODEL, PROMPT)
    elapsed = time.monotonic() - t0

    print(f"\nModel  : {MODEL}")
    print(f"Elapsed: {elapsed:.2f}s")
    print(f"Response: {response!r}")

    assert isinstance(response, str), "Expected a string response"
    assert len(response.strip()) > 0, "Got empty response"
