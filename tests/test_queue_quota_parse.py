from __future__ import annotations

from novel_tts.queue.translation_queue import _parse_quota_blocked_model, _parse_quota_suggested_wait_seconds


def test_parse_quota_suggested_wait_seconds_from_cli_message() -> None:
    text = "Gemini quota exceeded (model=gemma-3-27b-it reasons=TPM suggested_wait=16.09s)"
    assert _parse_quota_suggested_wait_seconds(text) == 16.09


def test_parse_quota_blocked_model_prefers_blocked_model_field() -> None:
    text = "Worker quota wait | novel=x key_index=1 model=gemini-3.1 blocked_model=gemma-3-27b-it wait_seconds=12.3"
    assert _parse_quota_blocked_model(text) == "gemma-3-27b-it"


def test_parse_quota_blocked_model_from_cli_model_field() -> None:
    text = "Rate limited (exit=76): Gemini quota exceeded (model=gemini-3.1-flash-lite-preview reasons=RPM suggested_wait=17.22s)"
    assert _parse_quota_blocked_model(text) == "gemini-3.1-flash-lite-preview"

