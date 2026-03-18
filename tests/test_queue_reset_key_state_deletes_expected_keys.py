from __future__ import annotations

from types import SimpleNamespace

from novel_tts.queue.translation_queue import _reset_queue_key_state


class FakeRedis:
    def __init__(self):
        self.deleted: list[str] = []

    def delete(self, key: str):
        self.deleted.append(str(key))
        return 1


def _dummy_config(prefix: str = "novel_tts", novel_id: str = "novel"):
    return SimpleNamespace(
        novel_id=novel_id,
        queue=SimpleNamespace(redis=SimpleNamespace(prefix=prefix)),
    )


def test_reset_queue_key_state_deletes_cooldown_quota_and_throttle() -> None:
    client = FakeRedis()
    config = _dummy_config(prefix="pfx", novel_id="n1")

    deleted = _reset_queue_key_state(client, config, key_indices=[5], models=["gemma-3-27b-it"])
    assert deleted == 15
    assert any("last_pick_ms:k5" in key for key in client.deleted)
    assert any("rate_limit_cooldown:k5:gemma-3-27b-it" in key for key in client.deleted)
    assert any("out_of_quota_cooldown:k5:gemma-3-27b-it" in key for key in client.deleted)
    assert any(":k5:gemma-3-27b-it:quota:reqs" in key for key in client.deleted)
    assert any(":k5:gemma-3-27b-it:quota:tokens" in key for key in client.deleted)
    assert any(":k5:gemma-3-27b-it:quota:daily_reqs" in key for key in client.deleted)
    assert any(":k5:gemma-3-27b-it:quota:alloc:queue" in key for key in client.deleted)
    assert any(":k5:gemma-3-27b-it:quota:tpm:freezed" in key for key in client.deleted)
    assert any(":k5:gemma-3-27b-it:quota:tpm:freezed_tokens" in key for key in client.deleted)
    assert any(":k5:gemma-3-27b-it:quota:tpm:locked" in key for key in client.deleted)
    assert any(":k5:gemma-3-27b-it:quota:tpm:locked_tokens" in key for key in client.deleted)
    assert any(":k5:gemma-3-27b-it:quota:rpm:freezed" in key for key in client.deleted)
    assert any(":k5:gemma-3-27b-it:quota:rpm:locked" in key for key in client.deleted)
    assert any(":k5:gemma-3-27b-it:quota:rpd:freezed" in key for key in client.deleted)
    assert any(":k5:gemma-3-27b-it:quota:rpd:locked" in key for key in client.deleted)
