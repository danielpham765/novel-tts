from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from uuid import uuid4

import redis
import requests
import yaml

from novel_tts.common import logrotate
from novel_tts.config.loader import load_novel_config
from novel_tts.config.models import ProxyGatewayConfig
from novel_tts.quota import keys as quota_keys
from novel_tts.quota.lua_scripts import TRY_GRANT_LUA
from novel_tts.quota.eta import QuotaRequest, TpmEvent, estimate_grant_times

LOGGER = logging.getLogger("quota.supervisor")
_LAST_PROXY_FINGERPRINT = ""


@dataclass(frozen=True)
class RedisCfg:
    host: str
    port: int
    database: int
    prefix: str


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _load_redis_cfg() -> RedisCfg:
    path = _repo_root() / "configs" / "app.yaml"
    payload = {}
    if path.exists():
        payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    queue = payload.get("queue") if isinstance(payload, dict) else {}
    redis_raw = queue.get("redis") if isinstance(queue, dict) else {}
    if not isinstance(redis_raw, dict):
        redis_raw = {}
    host = str(redis_raw.get("host") or "").strip() or "127.0.0.1"
    port = int(redis_raw.get("port") or 6379)
    database = int(redis_raw.get("database") or 0)
    prefix = str(redis_raw.get("prefix") or "").strip() or "novel_tts"
    return RedisCfg(host=host, port=port, database=database, prefix=prefix)


def _load_proxy_gateway_cfg() -> ProxyGatewayConfig:
    path = _repo_root() / "configs" / "app.yaml"
    payload: dict = {}
    if path.exists():
        payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    proxy_raw = payload.get("proxy_gateway") if isinstance(payload, dict) else {}
    if proxy_raw is None:
        proxy_raw = {}
    if not isinstance(proxy_raw, dict):
        proxy_raw = {}

    def _clean_text(value) -> str:
        if value is None:
            return ""
        text = str(value).strip()
        return text

    def _clean_bool(value) -> bool:
        if isinstance(value, bool):
            return value
        if value is None:
            return False
        text = str(value).strip().lower()
        if not text:
            return False
        return text in {"1", "true", "yes", "on", "y"}

    enabled = _clean_bool(proxy_raw.get("enabled"))
    base_url = _clean_text(proxy_raw.get("base_url")) or "http://localhost:8888"
    mode = (_clean_text(proxy_raw.get("mode")) or "direct").strip().lower()
    auto_discovery = _clean_bool(proxy_raw.get("auto_discovery", True))
    try:
        keys_per_proxy = int(proxy_raw.get("keys_per_proxy", 3) or 3)
    except Exception:
        keys_per_proxy = 3
    proxies_raw = proxy_raw.get("proxies") or []
    proxies: list[str] = []
    if isinstance(proxies_raw, list):
        for item in proxies_raw:
            t = _clean_text(item)
            if t:
                proxies.append(t)
    direct_strategy = (_clean_text(proxy_raw.get("direct_run_strategy")) or "proxy_1").strip().lower()
    return ProxyGatewayConfig(
        enabled=enabled,
        base_url=base_url,
        mode=mode,
        auto_discovery=auto_discovery,
        keys_per_proxy=max(1, int(keys_per_proxy)),
        proxies=proxies,
        direct_run_strategy=direct_strategy,
    )


def _proxy_gateway_proxies_key(prefix: str) -> str:
    return f"{prefix}:proxy_gateway:proxies:v1"


def _proxy_gateway_status_key(prefix: str) -> str:
    return f"{prefix}:proxy_gateway:status:v1"


def _refresh_proxy_gateway_proxies(client: redis.Redis, *, prefix: str) -> None:
    cfg = _load_proxy_gateway_cfg()
    if not (cfg.enabled and cfg.auto_discovery):
        try:
            client.delete(_proxy_gateway_proxies_key(prefix))
            client.delete(_proxy_gateway_status_key(prefix))
        except Exception:
            pass
        return

    base_url = str(cfg.base_url or "").strip().rstrip("/")
    if not base_url:
        return

    url = f"{base_url}/proxies"
    now = time.time()
    try:
        resp = requests.get(url, timeout=5.0)
        resp.raise_for_status()
        payload = resp.json()
        if not isinstance(payload, list):
            raise ValueError(f"Unexpected /proxies response shape: {type(payload)}")
        proxies: list[dict] = []
        healthy_names: list[str] = []
        for item in payload:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name") or "").strip()
            host = str(item.get("host") or "").strip()
            is_healthy = bool(item.get("is_healthy"))
            if not name:
                continue
            proxies.append({"name": name, "host": host, "is_healthy": is_healthy})
            if is_healthy:
                healthy_names.append(name)

        global _LAST_PROXY_FINGERPRINT
        fingerprint = "|".join(
            f"{p.get('name','')}:{'1' if p.get('is_healthy') else '0'}" for p in sorted(proxies, key=lambda x: x.get("name", ""))
        )
        if fingerprint != (_LAST_PROXY_FINGERPRINT or ""):
            _LAST_PROXY_FINGERPRINT = fingerprint
            preview = ", ".join(healthy_names[:8])
            extra = "" if len(healthy_names) <= 8 else f" (+{len(healthy_names) - 8})"
            LOGGER.info(
                "proxy-gateway proxies refreshed | url=%s total=%s healthy=%s healthy_names=%s%s",
                url,
                len(proxies),
                len(healthy_names),
                preview,
                extra,
            )

        out = {"updated_at": now, "proxies": proxies}
        with client.pipeline() as pipe:
            pipe.set(_proxy_gateway_proxies_key(prefix), json.dumps(out, ensure_ascii=False), ex=150)
            pipe.set(
                _proxy_gateway_status_key(prefix),
                json.dumps({"updated_at": now, "ok": True, "error": ""}, ensure_ascii=False),
                ex=150,
            )
            pipe.execute()
    except Exception as exc:
        err = str(exc)
        LOGGER.warning("proxy-gateway proxies refresh failed; clients will fall back to direct | err=%s", err)
        try:
            with client.pipeline() as pipe:
                pipe.delete(_proxy_gateway_proxies_key(prefix))
                pipe.set(
                    _proxy_gateway_status_key(prefix),
                    json.dumps({"updated_at": now, "ok": False, "error": err}, ensure_ascii=False),
                    ex=150,
                )
                pipe.execute()
        except Exception:
            pass


def _client(cfg: RedisCfg) -> redis.Redis:
    return redis.Redis(host=cfg.host, port=cfg.port, db=cfg.database, decode_responses=True)


def _parse_alloc_queue_key(key: str) -> tuple[str, int, str] | None:
    # Expected: {prefix}:{novel_id}:k{idx}:{model}:quota:alloc:queue
    parts = [p for p in str(key).split(":") if p]
    if len(parts) < 7:
        return None
    if parts[-3:] != ["quota", "alloc", "queue"]:
        return None
    try:
        key_token = parts[-5]
        if not key_token.startswith("k"):
            return None
        key_index = int(key_token[1:])
    except Exception:
        return None
    model = parts[-4]
    novel_id = parts[-6]
    return novel_id, key_index, model


@lru_cache(maxsize=256)
def _model_limits_for(novel_id: str, model: str) -> tuple[int, int, int]:
    config = load_novel_config(novel_id)
    cfg = config.queue.model_configs.get(model)
    if cfg is None:
        return 0, 0, 0
    return int(cfg.rpm_limit or 0), int(cfg.tpm_limit or 0), int(cfg.rpd_limit or 0)


def _script_keys_for(prefix: str, novel_id: str, key_index: int, model: str) -> list[str]:
    key_prefix = f"{prefix}:{novel_id}:k{key_index}"
    from novel_tts.quota.client import _quota_script_keys

    return _quota_script_keys(key_prefix=key_prefix, model=model)


def _eta_key_for_queue(queue_key: str) -> str:
    # {key_prefix}:{model}:quota:alloc:queue -> {key_prefix}:{model}:quota:alloc:eta
    return str(queue_key).removesuffix(":queue") + ":eta"


def _refresh_queue_etas(
    client: redis.Redis,
    *,
    queue_key: str,
    prefix: str,
    novel_id: str,
    key_index: int,
    model: str,
    rpm_limit: int,
    tpm_limit: int,
    rpd_limit: int,
    max_items: int = 200,
) -> None:
    """
    Persist estimated grant times for requests currently waiting in the alloc queue.

    This provides stable countdowns for operator UIs (`queue ps-all`) without requiring every worker to
    re-implement backlog simulation.
    """

    now_t = client.time()
    now = float(now_t[0]) + float(now_t[1]) / 1_000_000.0

    raw_items = client.lrange(queue_key, 0, max_items - 1)
    if not raw_items:
        client.delete(_eta_key_for_queue(queue_key))
        return

    reqs: list[QuotaRequest] = []
    for raw in raw_items:
        try:
            payload = json.loads(raw or "")
        except Exception:
            continue
        request_id = str(payload.get("request_id") or "").strip()
        if not request_id:
            continue
        try:
            tokens = int(payload.get("tokens") or 0)
        except Exception:
            tokens = 0
        try:
            rpm_req = int(payload.get("rpm_req") or 1)
        except Exception:
            rpm_req = 1
        try:
            rpd_req = int(payload.get("rpd_req") or 1)
        except Exception:
            rpd_req = 1
        if tokens <= 0:
            tokens = 1
        expires_at = float(payload.get("expires_at") or 0.0)
        if expires_at and expires_at < time.time():
            continue
        reqs.append(QuotaRequest(request_id=request_id, tokens=tokens, rpm_req=rpm_req, rpd_req=rpd_req))

    if not reqs:
        client.delete(_eta_key_for_queue(queue_key))
        return

    script_keys = _script_keys_for(prefix, novel_id, key_index, model)
    (
        tpm_freezed_zset,
        tpm_freezed_hash,
        tpm_locked_zset,
        tpm_locked_hash,
        rpm_freezed_zset,
        rpm_locked_zset,
        rpd_freezed_zset,
        rpd_locked_zset,
    ) = script_keys

    rpm_events: list[float] = []
    if rpm_limit > 0:
        window_start = now - 60.0
        for _member, score in client.zrangebyscore(rpm_freezed_zset, window_start, "+inf", withscores=True):
            try:
                rpm_events.append(float(score))
            except Exception:
                continue
        for _member, score in client.zrangebyscore(rpm_locked_zset, window_start, "+inf", withscores=True):
            try:
                rpm_events.append(float(score))
            except Exception:
                continue

    rpd_events: list[float] = []
    if rpd_limit > 0:
        day_start = now - 86400.0
        for _member, score in client.zrangebyscore(rpd_freezed_zset, day_start, "+inf", withscores=True):
            try:
                rpd_events.append(float(score))
            except Exception:
                continue
        for _member, score in client.zrangebyscore(rpd_locked_zset, day_start, "+inf", withscores=True):
            try:
                rpd_events.append(float(score))
            except Exception:
                continue

    tpm_events: list[TpmEvent] = []
    if tpm_limit > 0:
        window_start = now - 60.0
        freezed = list(client.zrangebyscore(tpm_freezed_zset, window_start, "+inf", withscores=True))
        locked = list(client.zrangebyscore(tpm_locked_zset, window_start, "+inf", withscores=True))
        if freezed:
            members = [m for m, _s in freezed]
            vals = client.hmget(tpm_freezed_hash, members)
            for (m, s), v in zip(freezed, vals):
                try:
                    tok = int(v or 0)
                except Exception:
                    tok = 0
                if tok <= 0:
                    continue
                try:
                    ts = float(s)
                except Exception:
                    ts = now
                tpm_events.append(TpmEvent(ts=ts, tokens=tok))
        if locked:
            members = [m for m, _s in locked]
            vals = client.hmget(tpm_locked_hash, members)
            for (m, s), v in zip(locked, vals):
                try:
                    tok = int(v or 0)
                except Exception:
                    tok = 0
                if tok <= 0:
                    continue
                try:
                    ts = float(s)
                except Exception:
                    ts = now
                tpm_events.append(TpmEvent(ts=ts, tokens=tok))

    grant_times = estimate_grant_times(
        now=now,
        rpm_limit=rpm_limit,
        tpm_limit=tpm_limit,
        rpd_limit=rpd_limit,
        rpm_events=rpm_events,
        tpm_events=tpm_events,
        rpd_events=rpd_events,
        requests=reqs,
    )

    eta_key = _eta_key_for_queue(queue_key)
    with client.pipeline() as pipe:
        pipe.delete(eta_key)
        if grant_times:
            pipe.hset(eta_key, mapping={rid: f"{ts:.6f}" for rid, ts in grant_times.items()})
            pipe.expire(eta_key, 5)
        pipe.execute()


def run_quota_supervisor(*, poll_interval_seconds: float = 0.05) -> int:
    cfg = _load_redis_cfg()
    client = _client(cfg)
    proxy_cfg = _load_proxy_gateway_cfg()

    pattern = f"{cfg.prefix}:*:k*:*:quota:alloc:queue"
    rotate_requests_key = f"{cfg.prefix}:logrotate:requests"
    LOGGER.info(
        "quota-supervisor started | redis=%s:%s db=%s prefix=%s pattern=%s",
        cfg.host,
        cfg.port,
        cfg.database,
        cfg.prefix,
        pattern,
    )
    LOGGER.info(
        "proxy-gateway discovery | enabled=%s auto_discovery=%s base_url=%s mode=%s keys_per_proxy=%s",
        bool(proxy_cfg.enabled),
        bool(proxy_cfg.auto_discovery),
        (proxy_cfg.base_url or "-"),
        (proxy_cfg.mode or "-"),
        int(proxy_cfg.keys_per_proxy or 0),
    )
    repo_root = _repo_root()
    logs_root = repo_root / ".logs"
    logs_root.mkdir(parents=True, exist_ok=True)
    next_scan = 0.0
    queues: list[str] = []
    next_check_by_queue: dict[str, float] = {}
    next_eta_refresh_by_queue: dict[str, float] = {}
    last_deny_log_by_request: dict[tuple[str, str], float] = {}
    last_heartbeat = 0.0
    grants_since = 0
    grants_total = 0
    next_rotate_large = 0.0
    next_rotate_daily = 0.0
    next_housekeeping = 0.0
    next_rotate_requests_poll = 0.0
    next_proxy_refresh = 0.0

    while True:
        now = time.time()
        if now >= next_proxy_refresh:
            try:
                _refresh_proxy_gateway_proxies(client, prefix=cfg.prefix)
            except Exception as exc:
                LOGGER.warning("proxy-gateway refresh loop failed: %s", exc)
            next_proxy_refresh = now + 60.0
        # Log rotation maintenance is owned by quota-supervisor.
        # Keep it time-gated so it doesn't interfere with quota grants.
        if now >= next_rotate_large:
            try:
                rotated = logrotate.rotate_large_logs_to_today(
                    logs_root=logs_root,
                    size_threshold_bytes=10 * 1024 * 1024,
                )
                if rotated:
                    LOGGER.info("logrotate large | rotated=%s", rotated)
            except Exception as exc:
                LOGGER.warning("logrotate large failed: %s", exc)
            next_rotate_large = now + 5.0

        if now >= next_rotate_daily:
            try:
                rotated = logrotate.rotate_old_logs_to_date_folders(logs_root=logs_root)
                if rotated:
                    LOGGER.info("logrotate daily | rotated=%s", rotated)
            except Exception as exc:
                LOGGER.warning("logrotate daily failed: %s", exc)
            next_rotate_daily = now + 60.0

        if now >= next_housekeeping:
            try:
                logrotate.housekeeping_archived(logs_root=logs_root)
            except Exception as exc:
                LOGGER.warning("logrotate housekeeping failed: %s", exc)
            next_housekeeping = now + 60.0

        if now >= next_rotate_requests_poll:
            handled = 0
            for _ in range(10):
                try:
                    raw = client.lpop(rotate_requests_key)
                except Exception:
                    raw = None
                if not raw:
                    break
                try:
                    payload = json.loads(raw or "{}")
                except Exception:
                    payload = {}
                cmd = str(payload.get("cmd") or "").strip()
                novel_id = str(payload.get("novel_id") or "").strip()
                reply_key = str(payload.get("reply_key") or "").strip()
                request_id = str(payload.get("request_id") or "").strip() or uuid4().hex
                ok = False
                rotated = 0
                if cmd == "rotate_novel_logs" and novel_id:
                    try:
                        rotated = logrotate.rotate_novel_logs_to_today(logs_root=logs_root, novel_id=novel_id)
                        ok = True
                        LOGGER.info(
                            "logrotate request ok | request_id=%s novel=%s rotated=%s",
                            request_id,
                            novel_id,
                            rotated,
                        )
                    except Exception as exc:
                        LOGGER.warning(
                            "logrotate request failed | request_id=%s novel=%s err=%s",
                            request_id,
                            novel_id,
                            exc,
                        )
                        ok = False
                else:
                    LOGGER.warning("logrotate request ignored | request_id=%s cmd=%s novel=%s", request_id, cmd, novel_id)
                    ok = False

                if reply_key:
                    try:
                        client.setex(
                            reply_key,
                            30,
                            json.dumps(
                                {
                                    "ok": bool(ok),
                                    "cmd": cmd,
                                    "novel_id": novel_id,
                                    "rotated": int(rotated),
                                    "request_id": request_id,
                                    "ts": time.time(),
                                },
                                ensure_ascii=False,
                            ),
                        )
                    except Exception:
                        pass
                handled += 1
            next_rotate_requests_poll = now + (0.25 if handled else 1.0)

        if now >= next_scan:
            queues = sorted(set(str(k) for k in client.scan_iter(match=pattern, count=2000)))
            next_scan = now + 2.0

        if not queues:
            time.sleep(0.2)
            continue

        progressed = False
        for queue_key in queues:
            not_before = next_check_by_queue.get(queue_key, 0.0)
            if now < not_before:
                continue
            head = client.lindex(queue_key, 0)
            if not head:
                next_check_by_queue[queue_key] = now + 0.25
                continue
            meta = _parse_alloc_queue_key(queue_key)
            if meta is None:
                # Unknown format; avoid tight loop.
                next_check_by_queue[queue_key] = now + 1.0
                continue
            novel_id, key_index, model = meta
            try:
                request = json.loads(head)
            except Exception:
                client.lpop(queue_key)
                progressed = True
                continue

            expires_at = float(request.get("expires_at") or 0.0)
            if expires_at and time.time() > expires_at:
                client.lpop(queue_key)
                progressed = True
                continue

            request_id = str(request.get("request_id") or "").strip()
            reply_key = str(request.get("reply_key") or "").strip()
            tokens = int(request.get("tokens") or 0)
            try:
                rpm_req = int(request.get("rpm_req") or 1)
            except Exception:
                rpm_req = 1
            try:
                rpd_req = int(request.get("rpd_req") or 1)
            except Exception:
                rpd_req = 1
            pid = request.get("pid")
            if not request_id or not reply_key or tokens <= 0:
                client.lpop(queue_key)
                progressed = True
                continue
            if rpm_req != 1 or rpd_req != 1:
                LOGGER.warning(
                    "quota request rejected (unsupported rpm_req/rpd_req) | novel=%s key_index=%s model=%s pid=%s request_id=%s rpm_req=%s rpd_req=%s",
                    novel_id,
                    key_index,
                    model,
                    pid,
                    request_id,
                    rpm_req,
                    rpd_req,
                )
                client.lpop(queue_key)
                progressed = True
                continue

            # Soft upstream backoff: when workers still see HTTP 429 even though quota was granted, they may set
            # a per key-model penalty window. Avoid granting during that window to reduce 429 storms.
            try:
                key_prefix = f"{cfg.prefix}:{novel_id}:k{int(key_index)}"
                penalty_key = quota_keys.penalty_until_key(key_prefix=key_prefix, model=model)
                penalty_raw = client.get(penalty_key)
                penalty_until = float(penalty_raw) if penalty_raw else 0.0
            except Exception:
                penalty_until = 0.0
            if penalty_until and penalty_until > now:
                wait = max(0.05, float(penalty_until - now))
                next_check_by_queue[queue_key] = penalty_until
                deny_key = (queue_key, request_id)
                last_logged = last_deny_log_by_request.get(deny_key, 0.0)
                if wait >= 1.0 and (now - last_logged) >= 5.0:
                    last_deny_log_by_request[deny_key] = now
                    LOGGER.info(
                        "quota deny | novel=%s key=%s model=%s pid=%s | req=%s tokens=%s | wait=%.2fs reasons=%s detail=%s",
                        novel_id,
                        key_index,
                        model,
                        pid,
                        "1",
                        f"{int(tokens):,}",
                        wait,
                        "PENALTY",
                        "PENALTY upstream_429_backoff",
                    )
                continue

            rpm_limit, tpm_limit, rpd_limit = _model_limits_for(novel_id, model)

            eta_due = next_eta_refresh_by_queue.get(queue_key, 0.0)
            if now >= eta_due:
                try:
                    _refresh_queue_etas(
                        client,
                        queue_key=queue_key,
                        prefix=cfg.prefix,
                        novel_id=novel_id,
                        key_index=key_index,
                        model=model,
                        rpm_limit=rpm_limit,
                        tpm_limit=tpm_limit,
                        rpd_limit=rpd_limit,
                    )
                except Exception as exc:
                    LOGGER.debug("quota supervisor: eta refresh failed | queue=%s err=%s", queue_key, exc)
                next_eta_refresh_by_queue[queue_key] = now + 1.0

            script_keys = _script_keys_for(cfg.prefix, novel_id, key_index, model)
            try:
                result = client.eval(
                    TRY_GRANT_LUA,
                    len(script_keys),
                    *script_keys,
                    str(int(rpm_limit)),
                    str(int(tpm_limit)),
                    str(int(rpd_limit)),
                    str(int(tokens)),
                    request_id,
                )
            except Exception as exc:
                LOGGER.warning("quota supervisor: eval failed | queue=%s err=%s", queue_key, exc)
                next_check_by_queue[queue_key] = now + 1.0
                continue
            if isinstance(result, (list, tuple)):
                granted = result[0] if len(result) > 0 else 0
                grant_id = result[1] if len(result) > 1 else ""
                retry_after = result[2] if len(result) > 2 else "0.25"
                reason_text = result[3] if len(result) > 3 else ""
                rpm_used_raw = result[4] if len(result) > 4 else ""
                rpm_limit_raw = result[5] if len(result) > 5 else ""
                tpm_used_raw = result[6] if len(result) > 6 else ""
                tpm_limit_raw = result[7] if len(result) > 7 else ""
                rpd_used_raw = result[8] if len(result) > 8 else ""
                rpd_limit_raw = result[9] if len(result) > 9 else ""
                requested_tokens_raw = result[10] if len(result) > 10 else ""
                rpm_would_raw = result[11] if len(result) > 11 else ""
                tpm_would_raw = result[12] if len(result) > 12 else ""
                rpd_would_raw = result[13] if len(result) > 13 else ""
            else:
                granted = 0
                grant_id = ""
                retry_after = "0.25"
                reason_text = ""
                rpm_used_raw = ""
                rpm_limit_raw = ""
                tpm_used_raw = ""
                tpm_limit_raw = ""
                rpd_used_raw = ""
                rpd_limit_raw = ""
                requested_tokens_raw = ""
                rpm_would_raw = ""
                tpm_would_raw = ""
                rpd_would_raw = ""

            def _as_int(raw, default: int = 0) -> int:
                try:
                    return int(float(raw))
                except Exception:
                    return int(default)

            rpm_used_val = _as_int(rpm_used_raw)
            rpm_limit_val = _as_int(rpm_limit_raw)
            tpm_used_val = _as_int(tpm_used_raw)
            tpm_limit_val = _as_int(tpm_limit_raw)
            rpd_used_val = _as_int(rpd_used_raw)
            rpd_limit_val = _as_int(rpd_limit_raw)
            req_tokens_val = _as_int(requested_tokens_raw, default=int(tokens))
            rpm_would_val = _as_int(rpm_would_raw, default=rpm_used_val + 1)
            tpm_would_val = _as_int(tpm_would_raw, default=tpm_used_val + req_tokens_val)
            rpd_would_val = _as_int(rpd_would_raw, default=rpd_used_val + 1)

            try:
                granted_int = int(granted)
            except Exception:
                granted_int = 0

            if granted_int == 1 and str(grant_id).strip():
                client.lpop(queue_key)
                reply_payload = json.dumps(
                    {"grant_id": str(grant_id), "granted_at": time.time()},
                    ensure_ascii=False,
                )
                client.rpush(reply_key, reply_payload)
                progressed = True
                grants_since += 1
                grants_total += 1
                next_check_by_queue[queue_key] = now
                next_eta_refresh_by_queue[queue_key] = 0.0
                LOGGER.info(
                    "quota grant | novel=%s key=%s model=%s pid=%s | req=%s tokens=%s | rpm=%s/%s tpm=%s/%s rpd=%s/%s",
                    novel_id,
                    key_index,
                    model,
                    pid,
                    "1",
                    f"{int(tokens):,}",
                    rpm_would_val,
                    rpm_limit_val,
                    f"{tpm_would_val:,}",
                    f"{tpm_limit_val:,}",
                    f"{rpd_would_val:,}",
                    f"{rpd_limit_val:,}",
                )
            else:
                try:
                    wait = float(retry_after or 0.25)
                except Exception:
                    wait = 0.25
                wait = max(0.05, min(wait, 5.0))
                next_check_by_queue[queue_key] = now + wait
                # Deny logging can be noisy; log at most once per ~5s per (queue, request_id), and only when wait is meaningful.
                deny_key = (queue_key, request_id)
                last_logged = last_deny_log_by_request.get(deny_key, 0.0)
                if wait >= 1.0 and (now - last_logged) >= 5.0:
                    last_deny_log_by_request[deny_key] = now
                    detail_parts: list[str] = []
                    for token in (str(reason_text or "") or "").split(","):
                        t = token.strip().upper()
                        if not t:
                            continue
                        if t == "TPM":
                            detail_parts.append(
                                f"TPM cur={tpm_used_val:,}/{tpm_limit_val:,} + {req_tokens_val:,} = {tpm_would_val:,}/{tpm_limit_val:,}"
                            )
                        elif t == "RPM":
                            detail_parts.append(
                                f"RPM cur={rpm_used_val}/{rpm_limit_val} + {rpm_req} = {rpm_would_val}/{rpm_limit_val}"
                            )
                        elif t == "RPD":
                            detail_parts.append(
                                f"RPD cur={rpd_used_val:,}/{rpd_limit_val:,} + {rpd_req:,} = {rpd_would_val:,}/{rpd_limit_val:,}"
                            )
                    reasons_detail = "; ".join(detail_parts) if detail_parts else "-"
                    LOGGER.info(
                        "quota deny | novel=%s key=%s model=%s pid=%s | req=%s tokens=%s | wait=%.2fs reasons=%s detail=%s",
                        novel_id,
                        key_index,
                        model,
                        pid,
                        "1",
                        f"{int(tokens):,}",
                        wait,
                        (str(reason_text or "") or "-"),
                        reasons_detail,
                    )

        if not progressed:
            time.sleep(max(0.01, float(poll_interval_seconds)))

        if last_heartbeat <= 0.0:
            last_heartbeat = now
        if now - last_heartbeat >= 10.0:
            nonempty = 0
            total_waiting = 0
            try:
                with client.pipeline() as pipe:
                    for q in queues:
                        pipe.llen(q)
                    lens = pipe.execute()
                for value in lens or []:
                    try:
                        n = int(value or 0)
                    except Exception:
                        n = 0
                    if n > 0:
                        nonempty += 1
                        total_waiting += n
            except Exception:
                nonempty = 0
                total_waiting = 0
            LOGGER.info(
                "quota-supervisor heartbeat | queues=%s nonempty=%s waiting=%s grants_10s=%s grants_total=%s",
                len(queues),
                nonempty,
                total_waiting,
                grants_since,
                grants_total,
            )
            grants_since = 0
            last_heartbeat = now
