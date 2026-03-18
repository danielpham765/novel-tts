from __future__ import annotations

from novel_tts.queue.translation_queue import _cooldown_jitter_seconds


def test_cooldown_jitter_is_deterministic_and_bounded() -> None:
    assert _cooldown_jitter_seconds(1, max_jitter_seconds=5.0) == _cooldown_jitter_seconds(1, max_jitter_seconds=5.0)
    assert 0.0 <= _cooldown_jitter_seconds(1, max_jitter_seconds=5.0) <= 5.0
    assert 0.0 <= _cooldown_jitter_seconds(10, max_jitter_seconds=5.0) <= 5.0


def test_cooldown_jitter_differs_across_keys() -> None:
    a = _cooldown_jitter_seconds(1, max_jitter_seconds=5.0)
    b = _cooldown_jitter_seconds(2, max_jitter_seconds=5.0)
    assert a != b

