from __future__ import annotations

import json
import logging
from typing import Any

import requests

from novel_tts.config.models import ProxyGatewayConfig, RedisConfig

LOGGER = logging.getLogger(__name__)

_SNAPSHOT_CACHE: dict[str, object] = {
    "expires_at": 0.0,
    "healthy": [],
    "updated_at": 0.0,
    "error": "",
}
_LAST_FALLBACK_WARN_AT = 0.0


def _status_key(prefix: str) -> str:
    return f"{prefix}:proxy_gateway:status:v1"


def _proxies_key(prefix: str) -> str:
    return f"{prefix}:proxy_gateway:proxies:v1"


def _get_redis_client(redis_cfg: RedisConfig | None):
    if redis_cfg is None:
        return None
    try:
        import redis

        return redis.Redis(
            host=str(redis_cfg.host or "127.0.0.1"),
            port=int(redis_cfg.port or 6379),
            db=int(redis_cfg.database or 0),
            decode_responses=True,
        )
    except Exception:
        return None


def _load_proxy_snapshot_from_redis(
    *,
    cfg: ProxyGatewayConfig,
    redis_cfg: RedisConfig | None,
    now: float,
    cache_ttl_seconds: float = 10.0,
) -> tuple[list[str] | None, str]:
    """
    Returns (healthy_proxy_names, reason_if_unavailable).

    Source of truth is Redis updated by quota-supervisor.
    """

    if not bool(getattr(cfg, "auto_discovery", True)):
        return None, "auto_discovery_disabled"

    try:
        expires_at = float(_SNAPSHOT_CACHE.get("expires_at") or 0.0)
    except Exception:
        expires_at = 0.0
    if now < expires_at:
        healthy = _SNAPSHOT_CACHE.get("healthy") or []
        if isinstance(healthy, list):
            return [str(x) for x in healthy if str(x).strip()], ""

    client = _get_redis_client(redis_cfg)
    if client is None:
        return None, "redis_unavailable"

    prefix = str(getattr(redis_cfg, "prefix", "") or "").strip() or "novel_tts"
    raw = None
    try:
        raw = client.get(_proxies_key(prefix))
    except Exception:
        raw = None

    if not raw:
        # Try reading status for better diagnostics.
        reason = "proxy_list_missing"
        try:
            status_raw = client.get(_status_key(prefix))
        except Exception:
            status_raw = None
        if status_raw:
            try:
                payload = json.loads(status_raw)
                err = str(payload.get("error") or "").strip()
                if err:
                    reason = f"{reason}: {err}"
            except Exception:
                pass
        return None, reason

    try:
        payload = json.loads(raw)
    except Exception:
        return None, "proxy_list_invalid_json"
    items = payload.get("proxies") if isinstance(payload, dict) else None
    if not isinstance(items, list):
        return None, "proxy_list_invalid_shape"
    healthy_names: list[str] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "").strip()
        if not name:
            continue
        is_healthy = bool(item.get("is_healthy"))
        if is_healthy:
            healthy_names.append(name)

    _SNAPSHOT_CACHE["healthy"] = healthy_names
    _SNAPSHOT_CACHE["updated_at"] = float(payload.get("updated_at") or 0.0) if isinstance(payload, dict) else 0.0
    _SNAPSHOT_CACHE["expires_at"] = now + max(0.5, float(cache_ttl_seconds))
    _SNAPSHOT_CACHE["error"] = ""
    return healthy_names, ""


def load_healthy_proxy_names_from_redis(
    *,
    cfg: ProxyGatewayConfig,
    redis_cfg: RedisConfig | None,
    now: float | None = None,
    cache_ttl_seconds: float = 10.0,
) -> tuple[list[str] | None, str]:
    import time

    ts = time.time() if now is None else float(now)
    return _load_proxy_snapshot_from_redis(cfg=cfg, redis_cfg=redis_cfg, now=ts, cache_ttl_seconds=cache_ttl_seconds)


def select_proxy_for_key_index(*, key_index: int, proxies: list[str], keys_per_proxy: int) -> str | None:
    if not proxies:
        return None
    try:
        key_index_int = int(key_index)
    except Exception:
        return None
    if key_index_int <= 0:
        return None
    # k1 is reserved for direct (never use proxy).
    if key_index_int == 1:
        return None
    try:
        kpp = int(keys_per_proxy)
    except Exception:
        kpp = 0
    kpp = max(1, kpp)
    # Proxy distribution:
    # - Round 1: each proxy serves `keys_per_proxy` keys (excluding k1).
    # - Round 2+: each proxy serves 1 key (round robin) so the distribution stays even when key counts grow.
    #
    # Example with keys_per_proxy=3:
    #   k1: direct
    #   k2-k4 -> proxy[0]
    #   k5-k7 -> proxy[1]
    #   ...
    #   then k(3P+2) -> proxy[0], next -> proxy[1], ...
    pos = key_index_int - 2  # k2 -> 0 (first proxied key)
    round1_span = kpp * len(proxies)
    if pos < round1_span:
        proxy_idx = pos // kpp
    else:
        proxy_idx = (pos - round1_span) % len(proxies)
    return proxies[proxy_idx]


def _select_proxy_for_request(cfg: ProxyGatewayConfig, *, key_index: int | None) -> str | None:
    proxies = list(getattr(cfg, "proxies", None) or [])
    if key_index is not None:
        return select_proxy_for_key_index(key_index=key_index, proxies=proxies, keys_per_proxy=int(cfg.keys_per_proxy or 3))
    strategy = (getattr(cfg, "direct_run_strategy", "") or "").strip().lower()
    if strategy == "proxy_1":
        return proxies[0] if proxies else None
    if strategy == "gateway_rr":
        return None
    return proxies[0] if proxies else None


def _normalize_proxy_body(body: Any) -> str:
    if body is None:
        return ""
    if isinstance(body, bytes):
        try:
            return body.decode("utf-8")
        except Exception:
            return body.decode("utf-8", errors="replace")
    if isinstance(body, str):
        return body
    try:
        return json.dumps(body, ensure_ascii=False)
    except Exception:
        return str(body)


def request(
    method: str,
    url: str,
    *,
    headers: dict[str, str] | None = None,
    body: Any = None,
    cfg: ProxyGatewayConfig,
    key_index: int | None = None,
    redis_cfg: RedisConfig | None = None,
    timeout_seconds: float | None = None,
) -> requests.Response:
    method = (method or "").strip().upper()
    if not method:
        raise ValueError("Missing HTTP method")
    url = (url or "").strip()
    if not url:
        raise ValueError("Missing URL")

    hdrs = dict(headers or {})

    enabled = bool(getattr(cfg, "enabled", False))
    # Never proxy requests from k1: always call upstream directly.
    if key_index == 1:
        enabled = False
    if enabled and bool(getattr(cfg, "auto_discovery", True)):
        import time

        now = time.time()
        healthy, reason = load_healthy_proxy_names_from_redis(cfg=cfg, redis_cfg=redis_cfg, now=now)
        if not healthy:
            # Treat as disabled when quota-supervisor isn't running or proxy list is unavailable.
            global _LAST_FALLBACK_WARN_AT
            if now - float(_LAST_FALLBACK_WARN_AT or 0.0) >= 60.0:
                _LAST_FALLBACK_WARN_AT = now
                LOGGER.warning(
                    "ProxyGateway enabled but proxy list unavailable; falling back to direct | reason=%s",
                    reason or "unknown",
                )
            enabled = False
        else:
            # Override configured proxy list with the latest healthy proxies.
            cfg = ProxyGatewayConfig(
                enabled=True,
                base_url=cfg.base_url,
                mode=cfg.mode,
                auto_discovery=cfg.auto_discovery,
                keys_per_proxy=cfg.keys_per_proxy,
                proxies=list(healthy),
                direct_run_strategy=cfg.direct_run_strategy,
            )

    if not enabled:
        data = body if isinstance(body, (str, bytes)) else None
        json_body = None if isinstance(body, (str, bytes)) else body
        return requests.request(
            method,
            url,
            headers=hdrs or None,
            data=data,
            json=json_body,
            timeout=timeout_seconds,
        )

    base_url = (getattr(cfg, "base_url", "") or "").strip().rstrip("/")
    if not base_url:
        raise RuntimeError("ProxyGateway is enabled but proxy_gateway.base_url is empty")

    mode = (getattr(cfg, "mode", "") or "direct").strip().lower()
    if mode not in {"direct", "socket"}:
        raise ValueError(f"Invalid proxy gateway mode: {mode}")

    proxy = _select_proxy_for_request(cfg, key_index=key_index)
    payload: dict[str, Any] = {
        "method": method,
        "url": url,
        "headers": hdrs or None,
        "body": _normalize_proxy_body(body),
        "mode": mode,
    }
    if proxy:
        payload["proxy"] = proxy

    effective_timeout = timeout_seconds
    if effective_timeout is None:
        # Socket mode waits for a response; keep a safety cushion above the gateway's wait timeout.
        effective_timeout = 135.0 if mode == "socket" else 90.0

    try:
        return requests.post(
            f"{base_url}/proxy",
            json=payload,
            timeout=max(1.0, float(effective_timeout)),
        )
    except Exception as exc:
        LOGGER.warning("ProxyGateway request failed | method=%s url=%s proxy=%s mode=%s err=%s", method, url, proxy, mode, exc)
        raise
