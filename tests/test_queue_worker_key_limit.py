from __future__ import annotations

from types import SimpleNamespace

from novel_tts.config.models import ProxyGatewayConfig, RedisConfig
from novel_tts.queue import translation_queue as tq


def _config_for_proxy(cfg: ProxyGatewayConfig):
    return SimpleNamespace(
        proxy_gateway=cfg,
        queue=SimpleNamespace(redis=RedisConfig()),
    )


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
