from __future__ import annotations

from types import SimpleNamespace

from novel_tts.quota.supervisor import _parse_alloc_queue_key
from novel_tts.quota import supervisor


def test_parse_alloc_queue_key_supports_shared_shape() -> None:
    parsed = _parse_alloc_queue_key("novel_tts:key_deadbeef:gemma-3-27b-it:quota:alloc:queue")

    assert parsed == (
        "__shared__",
        "novel_tts:key_deadbeef",
        "gemma-3-27b-it",
    )


def test_parse_alloc_queue_key_rejects_legacy_shape() -> None:
    assert _parse_alloc_queue_key("novel_tts:tro-lai-dai-hoc:key_deadbeef:gemma-3-27b-it:quota:alloc:queue") is None


def test_model_limits_for_shared_queue_uses_app_queue_config(monkeypatch) -> None:
    supervisor._model_limits_for.cache_clear()
    monkeypatch.setattr(
        supervisor,
        "load_queue_config",
        lambda: SimpleNamespace(
            model_configs={
                "gemma-3-27b-it": SimpleNamespace(rpm_limit=30, tpm_limit=15000, rpd_limit=14400),
            }
        ),
    )

    assert supervisor._model_limits_for("__shared__", "gemma-3-27b-it") == (30, 15000, 14400)
