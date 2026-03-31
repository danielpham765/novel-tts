from __future__ import annotations

from novel_tts.queue.translation_queue import (
    _normalize_quota_wait_seconds,
    _parse_quota_blocked_model,
    _parse_quota_suggested_wait_seconds,
)


def test_parse_quota_suggested_wait_seconds_from_cli_message() -> None:
    text = "Gemini quota exceeded (model=gemma-3-27b-it reasons=TPM suggested_wait=16.09s)"
    assert _parse_quota_suggested_wait_seconds(text) == 16.09


def test_parse_quota_blocked_model_prefers_blocked_model_field() -> None:
    text = "Worker quota wait | novel=x key_index=1 model=gemini-3.1 blocked_model=gemma-3-27b-it wait_seconds=12.3"
    assert _parse_quota_blocked_model(text) == "gemma-3-27b-it"


def test_parse_quota_blocked_model_from_cli_model_field() -> None:
    text = "Rate limited (exit=76): Gemini quota exceeded (model=gemini-3.1-flash-lite-preview reasons=RPM suggested_wait=17.22s)"
    assert _parse_quota_blocked_model(text) == "gemini-3.1-flash-lite-preview"


def test_normalize_quota_wait_caps_non_rpd_waits_to_60_seconds(monkeypatch) -> None:
    monkeypatch.setattr("novel_tts.queue.translation_queue._model_rpd_wait_seconds", lambda *args, **kwargs: 0.0)

    wait_seconds, is_rpd_wait = _normalize_quota_wait_seconds(
        None,
        None,
        1,
        "gemma-3-27b-it",
        proposed_wait_seconds=3200.0,
        text="Central quota redirect (model=gemma-3-27b-it suggested_wait=3200.00s requeue=1)",
    )

    assert wait_seconds == 60.0
    assert is_rpd_wait is False


def test_normalize_quota_wait_preserves_long_rpd_waits(monkeypatch) -> None:
    monkeypatch.setattr("novel_tts.queue.translation_queue._model_rpd_wait_seconds", lambda *args, **kwargs: 3700.0)

    wait_seconds, is_rpd_wait = _normalize_quota_wait_seconds(
        None,
        None,
        1,
        "gemma-3-27b-it",
        proposed_wait_seconds=120.0,
        text="Gemini quota exceeded (model=gemma-3-27b-it reasons=RPD suggested_wait=120.00s)",
    )

    assert wait_seconds == 3700.0
    assert is_rpd_wait is True
