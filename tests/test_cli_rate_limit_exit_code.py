from __future__ import annotations

from novel_tts.cli.main import _rate_limit_exit_code


def test_rate_limit_exit_code_75_for_429() -> None:
    assert _rate_limit_exit_code("Gemini 429 persisted after 2/4 attempts (model=x)") == 75
    assert _rate_limit_exit_code("Too many requests; sleeping for 3.0s") == 75


def test_rate_limit_exit_code_76_for_quota() -> None:
    assert _rate_limit_exit_code("Gemini quota exceeded (model=x reasons=RPM suggested_wait=12.00s)") == 76
    assert _rate_limit_exit_code("") == 76

