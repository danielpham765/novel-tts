from __future__ import annotations

import time

from novel_tts.queue.translation_queue import (
    _extend_rate_limit_cooldown_capped,
    _get_rate_limit_cooldown_remaining_seconds,
)


class FakeRedis:
    def __init__(self):
        self.store: dict[str, str] = {}

    def get(self, key: str):
        return self.store.get(key)

    def set(self, key: str, value: str, ex: int | None = None):
        del ex
        self.store[key] = str(value)


def test_extend_rate_limit_cooldown_capped_limits_remaining() -> None:
    client = FakeRedis()
    key = "cooldown"
    _extend_rate_limit_cooldown_capped(client, key, seconds=1000.0, max_seconds=65.0)
    remaining = _get_rate_limit_cooldown_remaining_seconds(client, key)
    # Small tolerance for time elapsed during the test.
    assert 60.0 <= remaining <= 65.0

