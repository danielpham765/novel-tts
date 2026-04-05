from __future__ import annotations

import time
from types import SimpleNamespace

from novel_tts.config.models import ProxyGatewayConfig, RedisConfig
from novel_tts.queue import translation_queue as tq
from novel_tts.queue.translation_queue import (
    _cooldown_jitter_seconds,
    _extend_rate_limit_cooldown,
    _extend_rate_limit_cooldown_capped,
    _get_rate_limit_cooldown_remaining_seconds,
    _interruptible_sleep,
    _normalize_quota_wait_seconds,
    _parse_quota_blocked_model,
    _parse_quota_suggested_wait_seconds,
    _rate_limit_requeue_delay_seconds,
)


class _FakeRedis:
    def __init__(self) -> None:
        self.store: dict[str, str] = {}

    def get(self, key: str):
        return self.store.get(key)

    def set(self, key: str, value: str, ex: int | None = None):
        del ex
        self.store[key] = str(value)


def _config_for_proxy(cfg: ProxyGatewayConfig):
    return SimpleNamespace(
        proxy_gateway=cfg,
        queue=SimpleNamespace(redis=RedisConfig()),
    )


class _Resp:
    def __init__(self, status_code: int) -> None:
        self.status_code = status_code


def test_cooldown_jitter_is_deterministic_and_bounded() -> None:
    assert _cooldown_jitter_seconds(1, max_jitter_seconds=5.0) == _cooldown_jitter_seconds(1, max_jitter_seconds=5.0)
    assert 0.0 <= _cooldown_jitter_seconds(1, max_jitter_seconds=5.0) <= 5.0
    assert 0.0 <= _cooldown_jitter_seconds(10, max_jitter_seconds=5.0) <= 5.0


def test_cooldown_jitter_differs_across_keys() -> None:
    assert _cooldown_jitter_seconds(1, max_jitter_seconds=5.0) != _cooldown_jitter_seconds(2, max_jitter_seconds=5.0)


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

    assert len(slept) == 2


def test_rate_limit_requeue_delay_backoff() -> None:
    assert _rate_limit_requeue_delay_seconds(1) == 3.0
    assert _rate_limit_requeue_delay_seconds(2) == 6.0
    assert _rate_limit_requeue_delay_seconds(3) == 12.0
    assert _rate_limit_requeue_delay_seconds(4) == 24.0
    assert _rate_limit_requeue_delay_seconds(5) == 48.0
    assert _rate_limit_requeue_delay_seconds(6) == 60.0
    assert _rate_limit_requeue_delay_seconds(20) == 60.0


def test_extend_rate_limit_cooldown_is_monotonic() -> None:
    client = _FakeRedis()
    key = "cooldown"
    until1 = _extend_rate_limit_cooldown(client, key, seconds=5.0)
    until2 = _extend_rate_limit_cooldown(client, key, seconds=1.0)
    until3 = _extend_rate_limit_cooldown(client, key, seconds=10.0)
    assert until2 == until1
    assert until3 >= until1


def test_get_rate_limit_cooldown_remaining_seconds() -> None:
    client = _FakeRedis()
    key = "cooldown"
    client.set(key, str(time.time() + 2.0), ex=10)
    remaining = _get_rate_limit_cooldown_remaining_seconds(client, key)
    assert remaining > 0.5


def test_extend_rate_limit_cooldown_capped_limits_remaining() -> None:
    client = _FakeRedis()
    key = "cooldown"
    _extend_rate_limit_cooldown_capped(client, key, seconds=1000.0, max_seconds=65.0)
    remaining = _get_rate_limit_cooldown_remaining_seconds(client, key)
    assert 60.0 <= remaining <= 65.0


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


def test_worker_key_limit_when_proxy_gateway_disabled() -> None:
    config = _config_for_proxy(ProxyGatewayConfig(enabled=False))
    limit, reason = tq._effective_worker_key_limit(config, total_keys=12)
    assert limit == 5
    assert reason == "proxy_gateway_disabled"


def test_worker_key_limit_when_proxy_gateway_enabled_but_no_proxies(monkeypatch) -> None:
    config = _config_for_proxy(ProxyGatewayConfig(enabled=True, auto_discovery=True))
    monkeypatch.setattr(
        tq.proxy_gateway_mod,
        "load_healthy_proxy_names_from_redis",
        lambda **kwargs: ([], "proxy_list_missing"),
    )
    limit, reason = tq._effective_worker_key_limit(config, total_keys=12)
    assert limit == 5
    assert reason == "proxy_gateway_no_proxies:proxy_list_missing"


def test_worker_key_limit_when_proxy_gateway_has_proxies() -> None:
    config = _config_for_proxy(ProxyGatewayConfig(enabled=True, auto_discovery=False, proxies=["p1"]))
    limit, reason = tq._effective_worker_key_limit(config, total_keys=12)
    assert limit == 12
    assert reason == ""


def test_probe_gemini_429_true(monkeypatch) -> None:
    cfg = ProxyGatewayConfig(enabled=True, auto_discovery=False, base_url="http://gw", mode="direct", proxies=["cloud-node-1"])
    config = _config_for_proxy(cfg)

    def _fake_post(url: str, *args, **kwargs):
        assert url == "http://gw/proxy"
        payload = kwargs.get("json") or {}
        assert payload.get("method") == "POST"
        assert payload.get("mode") == "direct"
        assert payload.get("proxy") == "cloud-node-1"
        return _Resp(429)

    monkeypatch.setattr(tq.proxy_gateway_mod.requests, "post", _fake_post)
    assert tq._probe_gemini_429(config=config, api_key="k", model="m", key_index=2) is True


def test_probe_gemini_429_false(monkeypatch) -> None:
    cfg = ProxyGatewayConfig(enabled=True, auto_discovery=False, base_url="http://gw", mode="direct", proxies=["cloud-node-1"])
    config = _config_for_proxy(cfg)
    monkeypatch.setattr(tq.proxy_gateway_mod.requests, "post", lambda *args, **kwargs: _Resp(200))
    assert tq._probe_gemini_429(config=config, api_key="k", model="m", key_index=2) is False


def test_probe_gemini_429_unknown_on_proxy_503(monkeypatch) -> None:
    cfg = ProxyGatewayConfig(enabled=True, auto_discovery=False, base_url="http://gw", mode="direct", proxies=["cloud-node-1"])
    config = _config_for_proxy(cfg)
    monkeypatch.setattr(tq.proxy_gateway_mod.requests, "post", lambda *args, **kwargs: _Resp(503))
    assert tq._probe_gemini_429(config=config, api_key="k", model="m", key_index=2) is None


def test_probe_gemini_429_unknown_on_proxy_403(monkeypatch) -> None:
    cfg = ProxyGatewayConfig(enabled=True, auto_discovery=False, base_url="http://gw", mode="direct", proxies=["cloud-node-1"])
    config = _config_for_proxy(cfg)
    monkeypatch.setattr(tq.proxy_gateway_mod.requests, "post", lambda *args, **kwargs: _Resp(403))
    assert tq._probe_gemini_429(config=config, api_key="k", model="m", key_index=2) is None


def test_probe_gemini_429_unknown_on_exception(monkeypatch) -> None:
    cfg = ProxyGatewayConfig(enabled=True, auto_discovery=False, base_url="http://gw", mode="direct", proxies=["cloud-node-1"])
    config = _config_for_proxy(cfg)

    def _boom(*args, **kwargs):
        raise RuntimeError("net")

    monkeypatch.setattr(tq.proxy_gateway_mod.requests, "post", _boom)
    assert tq._probe_gemini_429(config=config, api_key="k", model="m", key_index=2) is None
