from __future__ import annotations

import time

from novel_tts.queue.translation_queue import _interruptible_sleep


def test_interruptible_sleep_wakes_early_when_gate_clears(monkeypatch) -> None:
    slept: list[float] = []

    def fake_sleep(seconds: float) -> None:
        slept.append(float(seconds))

    monkeypatch.setattr(time, "sleep", fake_sleep)

    remaining = [10.0, 10.0, 0.0]

    def check_remaining() -> float:
        return remaining.pop(0) if remaining else 0.0

    _interruptible_sleep(
        max_seconds=60.0,
        check_remaining_seconds=check_remaining,
        step_seconds=1.0,
        min_sleep_seconds=0.01,
    )

    # Slept twice, then exited immediately when the gate cleared.
    assert len(slept) == 2

