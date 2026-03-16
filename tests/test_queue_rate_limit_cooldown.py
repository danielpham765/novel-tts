from __future__ import annotations

import time

from novel_tts.queue.translation_queue import _extend_rate_limit_cooldown, _get_rate_limit_cooldown_remaining_seconds


class FakeRedis:
    def __init__(self):
        self.store: dict[str, str] = {}

    def get(self, key: str):
        return self.store.get(key)

    def set(self, key: str, value: str, ex: int | None = None):
        del ex
        self.store[key] = str(value)


def test_extend_rate_limit_cooldown_is_monotonic() -> None:
    client = FakeRedis()
    key = "cooldown"
    until1 = _extend_rate_limit_cooldown(client, key, seconds=5.0)
    until2 = _extend_rate_limit_cooldown(client, key, seconds=1.0)
    assert until2 == until1
    until3 = _extend_rate_limit_cooldown(client, key, seconds=10.0)
    assert until3 >= until1


def test_get_rate_limit_cooldown_remaining_seconds() -> None:
    client = FakeRedis()
    key = "cooldown"
    client.set(key, str(time.time() + 2.0), ex=10)
    remaining = _get_rate_limit_cooldown_remaining_seconds(client, key)
    assert remaining > 0.5
