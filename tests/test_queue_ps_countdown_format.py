from __future__ import annotations

from novel_tts.queue.translation_queue import _format_countdown


def test_format_countdown_under_3m_includes_seconds() -> None:
    assert _format_countdown(None) == ""
    assert _format_countdown(0) == ""
    assert _format_countdown(-1) == ""
    assert _format_countdown(47) == "47s"
    assert _format_countdown(60) == "1m:0s"
    assert _format_countdown(179) == "2m:59s"
    # Exactly 3 minutes keeps seconds per the spec (> 3m hides seconds).
    assert _format_countdown(180) == "3m:0s"


def test_format_countdown_over_3m_hides_seconds() -> None:
    assert _format_countdown(181) == "3m"
    assert _format_countdown(3485) == "58m"
    assert _format_countdown(3600) == "1h:0m"
    assert _format_countdown(3723) == "1h:2m"

