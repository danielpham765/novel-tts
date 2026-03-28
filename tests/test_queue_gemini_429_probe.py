from __future__ import annotations

from types import SimpleNamespace

from novel_tts.config.models import ProxyGatewayConfig, RedisConfig
from novel_tts.queue import translation_queue as tq


class _Resp:
    def __init__(self, status_code: int) -> None:
        self.status_code = status_code


def test_probe_gemini_429_true(monkeypatch) -> None:
    cfg = ProxyGatewayConfig(enabled=True, auto_discovery=False, base_url="http://gw", mode="direct", proxies=["cloud-node-1"])
    config = SimpleNamespace(proxy_gateway=cfg, queue=SimpleNamespace(redis=RedisConfig()))

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
    config = SimpleNamespace(proxy_gateway=cfg, queue=SimpleNamespace(redis=RedisConfig()))
    monkeypatch.setattr(tq.proxy_gateway_mod.requests, "post", lambda *args, **kwargs: _Resp(200))
    assert tq._probe_gemini_429(config=config, api_key="k", model="m", key_index=2) is False


def test_probe_gemini_429_unknown_on_proxy_503(monkeypatch) -> None:
    cfg = ProxyGatewayConfig(enabled=True, auto_discovery=False, base_url="http://gw", mode="direct", proxies=["cloud-node-1"])
    config = SimpleNamespace(proxy_gateway=cfg, queue=SimpleNamespace(redis=RedisConfig()))
    monkeypatch.setattr(tq.proxy_gateway_mod.requests, "post", lambda *args, **kwargs: _Resp(503))
    assert tq._probe_gemini_429(config=config, api_key="k", model="m", key_index=2) is None


def test_probe_gemini_429_unknown_on_proxy_403(monkeypatch) -> None:
    cfg = ProxyGatewayConfig(enabled=True, auto_discovery=False, base_url="http://gw", mode="direct", proxies=["cloud-node-1"])
    config = SimpleNamespace(proxy_gateway=cfg, queue=SimpleNamespace(redis=RedisConfig()))
    monkeypatch.setattr(tq.proxy_gateway_mod.requests, "post", lambda *args, **kwargs: _Resp(403))
    assert tq._probe_gemini_429(config=config, api_key="k", model="m", key_index=2) is None


def test_probe_gemini_429_unknown_on_exception(monkeypatch) -> None:
    cfg = ProxyGatewayConfig(enabled=True, auto_discovery=False, base_url="http://gw", mode="direct", proxies=["cloud-node-1"])
    config = SimpleNamespace(proxy_gateway=cfg, queue=SimpleNamespace(redis=RedisConfig()))

    def _boom(*args, **kwargs):
        raise RuntimeError("net")

    monkeypatch.setattr(tq.proxy_gateway_mod.requests, "post", _boom)
    assert tq._probe_gemini_429(config=config, api_key="k", model="m", key_index=2) is None
