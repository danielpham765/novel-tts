from __future__ import annotations

import json
import math
import os
import re
import requests
import shlex
import subprocess
import sys
import time
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path

from novel_tts.common import logrotate
from novel_tts.common.logging import get_logger, get_novel_log_path
from novel_tts.config.models import NovelConfig, QueueConfig, RedisConfig
from novel_tts.key_identity import build_global_key_prefix, build_key_prefix
from novel_tts.net import proxy_gateway as proxy_gateway_mod
from novel_tts.quota import keys as quota_keys
from novel_tts.translate.novel import (
    chapter_part_path,
    chapter_source_changed,
    is_glossary_pending,
    load_chapter_map,
    load_source_chapters,
)

LOGGER = get_logger(__name__)
CAPTIONS_JOB_ID = "captions"
REPAIR_GLOSSARY_JOB_PREFIX = "repair-glossary::"

_RATE_LIMIT_TOKENS = (
    "429",
    "rate limit",
    "too many requests",
    "resource_exhausted",
)

_QUOTA_WAIT_TOKENS = (
    "quota is exhausted",
    "paused because model quota is exhausted",
    "blocked_model=",
)

_OUT_OF_QUOTA_TOKENS = (
    "worker entering out-of-quota cooldown",
    "out_of_quota",
)

_INLINE_QUOTA_WAIT_MAX_SECONDS = 5.0
_RATE_LIMIT_PROBE_COOLDOWN_SECONDS = 60.0
_IP_BAN_MAX_SECONDS = 180.0  # Max probe backoff interval (seconds)
_IP_BAN_DETECT_WINDOW_SECONDS = 20.0
_IP_BAN_DETECT_MIN_EVENTS = 6
_IP_BAN_DETECT_MIN_KEYS = 3
_IP_BAN_INITIAL_BACKOFF_SECONDS = 8.0
_IP_BAN_MAX_BACKOFF_SECONDS = _IP_BAN_MAX_SECONDS
_IP_BAN_PROBE_LOCK_SECONDS = 12
_IP_RECOVER_SECONDS = 60.0
_IP_RECOVER_RPS = 1
_DIRECT_FALLBACK_MAX_KEYS = 5


def _rate_limit_requeue_delay_seconds(consecutive_releases: int) -> float:
    """
    Backoff used when releasing a job due to HTTP 429 rate limits.

    The goal is to avoid immediately re-picking the same job across workers/keys, which can create a 429 storm.
    Keep this delay short (seconds) because longer "out of quota" situations are handled separately by the
    worker cooldown logic.
    """

    try:
        n = int(consecutive_releases)
    except Exception:
        n = 1
    n = max(1, n)
    # Exponential backoff capped at 60s: 3, 6, 12, 24, 48, 60...
    base = min(60.0, 3.0 * (2 ** (min(n, 6) - 1)))
    return max(1.0, float(base))


def _worker_key_prefix(config: NovelConfig | QueueConfig, *, raw_key: str) -> str:
    """Build shared key prefix for rate-limit/quota keys (no novel_id)."""
    if isinstance(config, QueueConfig):
        prefix = str(config.redis.prefix or "").strip() or "novel_tts"
    else:
        prefix = str(config.queue.redis.prefix or "").strip() or "novel_tts"
    return build_global_key_prefix(prefix=prefix, raw_key=raw_key)


def _worker_key_prefix_for_index(config: NovelConfig | QueueConfig, *, key_index: int) -> str:
    keys = _load_keys(config)
    idx = int(key_index)
    if idx <= 0 or idx > len(keys):
        raise ValueError(f"Invalid key index: {key_index}")
    return _worker_key_prefix(config, raw_key=keys[idx - 1])


def _rate_limit_cooldown_key(config: NovelConfig | QueueConfig, *, key_index: int, model: str) -> str:
    safe_model = (model or "").strip() or "unknown"
    key_prefix = _worker_key_prefix_for_index(config, key_index=int(key_index))
    return f"{key_prefix}:{safe_model}:rate_limit_cooldown"


def _out_of_quota_cooldown_key(config: NovelConfig | QueueConfig, *, key_index: int, model: str) -> str:
    safe_model = (model or "").strip() or "unknown"
    key_prefix = _worker_key_prefix_for_index(config, key_index=int(key_index))
    return f"{key_prefix}:{safe_model}:out_of_quota_cooldown"


def _ip_ban_429_key(config: NovelConfig | QueueConfig, *, model: str) -> str:
    safe_model = (model or "").strip() or "unknown"
    return _global_key(config, f"ip_ban_429:{safe_model}")


def _ip_ban_state_key(config: NovelConfig | QueueConfig, *, model: str) -> str:
    safe_model = (model or "").strip() or "unknown"
    return _global_key(config, f"ip_ban_state:{safe_model}")


def _ip_ban_probe_lock_key(config: NovelConfig | QueueConfig, *, model: str) -> str:
    safe_model = (model or "").strip() or "unknown"
    return _global_key(config, f"ip_ban_probe_lock:{safe_model}")


def _ip_recover_state_key(config: NovelConfig | QueueConfig, *, model: str) -> str:
    safe_model = (model or "").strip() or "unknown"
    return _global_key(config, f"ip_recover_state:{safe_model}")


def _ip_recover_slot_key(config: NovelConfig | QueueConfig, *, model: str, slot: int) -> str:
    safe_model = (model or "").strip() or "unknown"
    return _global_key(config, f"ip_recover_slot:{safe_model}:{int(slot)}")


def _startup_ramp_applied_key(config: NovelConfig | QueueConfig, *, model: str) -> str:
    safe_model = (model or "").strip() or "unknown"
    return _global_key(config, f"startup_ramp_applied:{safe_model}")


def _get_ip_ban_state(client, config: NovelConfig | QueueConfig, *, model: str) -> dict:
    try:
        raw = client.get(_ip_ban_state_key(config, model=model))
    except Exception:
        raw = None
    if not raw:
        return {}
    try:
        payload = json.loads(raw)
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    if "next_probe_at" not in payload:
        # Backward compatibility: older payload used "until" as the next eligible time.
        try:
            payload["next_probe_at"] = float(payload.get("until") or 0.0)
        except Exception:
            payload["next_probe_at"] = 0.0
        _set_ip_ban_state(client, config, model=model, payload=payload)
    return payload


def _set_ip_ban_state(client, config: NovelConfig | QueueConfig, *, model: str, payload: dict) -> None:
    try:
        next_probe_at = float(payload.get("next_probe_at") or 0.0)
        # Keep state around across longer bans; expire a bit after next scheduled probe.
        ttl = int(max(10 * 60.0, (next_probe_at - time.time()) + 5 * 60.0)) if next_probe_at else int(10 * 60.0)
        client.set(_ip_ban_state_key(config, model=model), json.dumps(payload, ensure_ascii=False), ex=ttl)
    except Exception:
        return


def _clear_ip_ban_state(client, config: NovelConfig | QueueConfig, *, model: str) -> None:
    try:
        client.delete(_ip_ban_state_key(config, model=model))
        client.delete(_ip_ban_429_key(config, model=model))
        client.delete(_ip_ban_probe_lock_key(config, model=model))
    except Exception:
        return


def _ip_ban_is_active(client, config: NovelConfig | QueueConfig, *, model: str) -> bool:
    state = _get_ip_ban_state(client, config, model=model)
    if not state:
        return False
    try:
        start = float(state.get("start") or 0.0)
    except Exception:
        start = 0.0
    return start > 0.0


def _ip_ban_next_probe_in_seconds(client, config: NovelConfig | QueueConfig, *, model: str) -> float:
    state = _get_ip_ban_state(client, config, model=model)
    if not state:
        return 0.0
    try:
        next_probe_at = float(state.get("next_probe_at") or 0.0)
    except Exception:
        next_probe_at = 0.0
    return max(0.0, next_probe_at - time.time())


def _get_ip_recover_state(client, config: NovelConfig | QueueConfig, *, model: str) -> dict:
    try:
        raw = client.get(_ip_recover_state_key(config, model=model))
    except Exception:
        raw = None
    if not raw:
        return {}
    try:
        payload = json.loads(raw)
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _set_ip_recover_state(client, config: NovelConfig | QueueConfig, *, model: str, payload: dict) -> None:
    try:
        until = float(payload.get("until") or 0.0)
    except Exception:
        until = 0.0
    ttl = int(max(5.0, (until - time.time()) + 30.0)) if until else int(_IP_RECOVER_SECONDS + 60.0)
    try:
        client.set(_ip_recover_state_key(config, model=model), json.dumps(payload, ensure_ascii=False), ex=ttl)
    except Exception:
        return


def _clear_ip_recover_state(client, config: NovelConfig | QueueConfig, *, model: str) -> None:
    try:
        client.delete(_ip_recover_state_key(config, model=model))
    except Exception:
        return


def _ip_recover_is_active(client, config: NovelConfig | QueueConfig, *, model: str) -> bool:
    state = _get_ip_recover_state(client, config, model=model)
    if not state:
        return False
    try:
        until = float(state.get("until") or 0.0)
    except Exception:
        until = 0.0
    return until > time.time()


def _ip_recover_try_admit(client, config: NovelConfig | QueueConfig, *, model: str) -> bool:
    state = _get_ip_recover_state(client, config, model=model)
    if not state:
        return True
    now = time.time()
    try:
        until = float(state.get("until") or 0.0)
    except Exception:
        until = 0.0
    if until <= now:
        _clear_ip_recover_state(client, config, model=model)
        return True
    try:
        rps = int(state.get("rps") or _IP_RECOVER_RPS)
    except Exception:
        rps = _IP_RECOVER_RPS
    rps = max(1, rps)
    slot = int(now)
    slot_key = _ip_recover_slot_key(config, model=model, slot=slot)
    try:
        n = int(client.incr(slot_key))
        if n == 1:
            client.expire(slot_key, 2)
    except Exception:
        return False
    return n <= rps


def _maybe_apply_startup_ramp(client, config: NovelConfig | QueueConfig, *, model: str) -> None:
    """
    Apply a one-time "startup ramp" after queue launch/restart so that spawning many workers does not
    immediately burst into upstream 429 storms.
    """

    try:
        seconds = float(getattr(config.queue, "startup_ramp_seconds", 0.0) or 0.0)
    except Exception:
        seconds = 0.0
    if seconds <= 0.0:
        return
    try:
        rps = int(getattr(config.queue, "startup_ramp_rps", 1) or 1)
    except Exception:
        rps = 1
    rps = max(1, rps)

    marker = _startup_ramp_applied_key(config, model=model)
    try:
        if client.get(marker):
            return
    except Exception:
        return

    if _ip_ban_is_active(client, config, model=model):
        return
    if _ip_recover_is_active(client, config, model=model):
        try:
            client.set(marker, "1", ex=int(max(60.0, seconds + 10 * 60.0)))
        except Exception:
            pass
        return

    now = time.time()
    _set_ip_recover_state(client, config, model=model, payload={"start": now, "until": now + seconds, "rps": rps})
    try:
        client.set(marker, "1", ex=int(max(60.0, seconds + 10 * 60.0)))
    except Exception:
        pass
    LOGGER.warning(
        "Queue startup ramp started | novel=%s model=%s rps=%s seconds=%.0f",
        config.novel_id,
        model,
        rps,
        seconds,
    )


def _sync_cooldown_until(client, cooldown_key: str, *, until: float) -> None:
    now = time.time()
    if until <= now:
        return
    try:
        current_raw = client.get(cooldown_key)
        current_until = float(current_raw) if current_raw is not None else 0.0
    except Exception:
        current_until = 0.0
    if current_until >= until:
        return
    try:
        ttl = int(max(2.0, (until - now) + 15.0))
        client.set(cooldown_key, str(until), ex=ttl)
    except Exception:
        return


def _maybe_trigger_ip_ban_on_429(client, config: NovelConfig | QueueConfig, *, key_index: int, model: str) -> bool:
    """
    Heuristic: if we see many 429s across multiple keys in a short window, treat it as a potential IP-level ban
    (or global throttling) and pause all workers for this novel+model.
    """

    now = time.time()
    zkey = _ip_ban_429_key(config, model=model)
    member = f"{now:.6f}:k{int(key_index)}:{uuid.uuid4().hex}"
    try:
        client.zadd(zkey, {member: now})
        client.zremrangebyscore(zkey, 0, now - _IP_BAN_MAX_SECONDS)
        client.expire(zkey, int(_IP_BAN_MAX_SECONDS + 120.0))
    except Exception:
        return False

    try:
        recent = client.zrangebyscore(zkey, now - _IP_BAN_DETECT_WINDOW_SECONDS, "+inf") or []
    except Exception:
        recent = []
    if len(recent) < _IP_BAN_DETECT_MIN_EVENTS:
        return False
    keys = set()
    for item in recent:
        m = re.search(r":k(\d+):", str(item))
        if m:
            keys.add(int(m.group(1)))
    if len(keys) < _IP_BAN_DETECT_MIN_KEYS:
        return False

    # If we were in recovery ramp-up and still see a 429 burst, immediately re-enter ban.
    if _ip_recover_is_active(client, config, model=model):
        _clear_ip_recover_state(client, config, model=model)
        state = {}

    state = _get_ip_ban_state(client, config, model=model)
    try:
        start = float(state.get("start") or 0.0)
    except Exception:
        start = 0.0
    # If an old ban state is stale, restart the ban window.
    if start <= 0.0 or (now - start) > (10 * 60.0):
        start = now
        backoff = _IP_BAN_INITIAL_BACKOFF_SECONDS
    else:
        try:
            backoff = float(state.get("backoff") or _IP_BAN_INITIAL_BACKOFF_SECONDS)
        except Exception:
            backoff = _IP_BAN_INITIAL_BACKOFF_SECONDS
        backoff = max(_IP_BAN_INITIAL_BACKOFF_SECONDS, min(_IP_BAN_MAX_BACKOFF_SECONDS, backoff))

    next_probe_at = now + backoff
    payload = {
        "start": start,
        "backoff": backoff,
        "updated_at": now,
        "reason": "ip_ban_suspected",
        "recent_events": int(len(recent)),
        "recent_keys": int(len(keys)),
        "next_probe_at": next_probe_at,
    }
    _set_ip_ban_state(client, config, model=model, payload=payload)
    return True


def _ip_ban_probe_if_due(client, config: NovelConfig | QueueConfig, *, key_index: int, model: str, api_key: str) -> None:
    """
    Probe upstream at increasing intervals while IP-ban is suspected.
    Only one worker probes at a time (Redis lock).
    """

    state = _get_ip_ban_state(client, config, model=model)
    if not state:
        return
    now = time.time()
    try:
        start = float(state.get("start") or 0.0)
    except Exception:
        start = 0.0
    try:
        next_probe = float(state.get("next_probe_at") or 0.0)
    except Exception:
        next_probe = 0.0
    if start <= 0.0 or now < next_probe:
        return

    lock_key = _ip_ban_probe_lock_key(config, model=model)
    try:
        got = client.set(lock_key, str(now), nx=True, ex=int(_IP_BAN_PROBE_LOCK_SECONDS))
    except Exception:
        got = False
    if not got:
        return

    probe = _probe_gemini_429(config=config, api_key=api_key, model=model, key_index=key_index)
    if probe is False:
        LOGGER.warning(
            "Suspected IP ban probe: no 429; clearing | novel=%s key_index=%s model=%s",
            config.novel_id,
            key_index,
            model,
        )
        _clear_ip_ban_state(client, config, model=model)
        _set_ip_recover_state(
            client,
            config,
            model=model,
            payload={"start": now, "until": now + _IP_RECOVER_SECONDS, "rps": _IP_RECOVER_RPS},
        )
        LOGGER.warning(
            "IP ban recovery ramp started | novel=%s model=%s rps=%s seconds=%.0f",
            config.novel_id,
            model,
            _IP_RECOVER_RPS,
            _IP_RECOVER_SECONDS,
        )
        return

    # Still 429 (or unknown): increase backoff and schedule the next probe.
    try:
        backoff = float(state.get("backoff") or _IP_BAN_INITIAL_BACKOFF_SECONDS)
    except Exception:
        backoff = _IP_BAN_INITIAL_BACKOFF_SECONDS
    backoff = max(_IP_BAN_INITIAL_BACKOFF_SECONDS, min(_IP_BAN_MAX_BACKOFF_SECONDS, backoff * 2.0))
    next_probe_at = now + backoff
    state["backoff"] = backoff
    state["updated_at"] = now
    state["next_probe_at"] = next_probe_at
    state["probe_result"] = "429" if probe is True else "unknown"
    _set_ip_ban_state(client, config, model=model, payload=state)
    LOGGER.warning(
        "Suspected IP ban probe: still 429; backing off | novel=%s key_index=%s model=%s backoff=%.0fs",
        config.novel_id,
        key_index,
        model,
        backoff,
    )


def _get_rate_limit_cooldown_remaining_seconds(client, cooldown_key: str) -> float:
    try:
        raw = client.get(cooldown_key)
        until = float(raw) if raw is not None else 0.0
    except Exception:
        return 0.0
    return max(0.0, float(until) - time.time())


def _extend_rate_limit_cooldown_capped(client, cooldown_key: str, *, seconds: float, max_seconds: float) -> float:
    try:
        cap = float(max_seconds)
    except Exception:
        cap = 0.0
    if cap > 0:
        try:
            seconds = min(float(seconds), cap)
        except Exception:
            seconds = cap
    return _extend_rate_limit_cooldown(client, cooldown_key, seconds=float(seconds))


def _extend_rate_limit_cooldown(client, cooldown_key: str, *, seconds: float) -> float:
    try:
        wait_seconds = float(seconds)
    except Exception:
        wait_seconds = 0.0
    wait_seconds = max(0.0, wait_seconds)
    now = time.time()
    until = now + wait_seconds
    ttl = int(max(2.0, wait_seconds + 10.0))
    try:
        current_raw = client.get(cooldown_key)
        if current_raw is not None:
            try:
                current_until = float(current_raw)
            except Exception:
                current_until = 0.0
            if current_until >= until:
                return current_until
        client.set(cooldown_key, str(until), ex=ttl)
    except Exception:
        return until
    return until


def _cooldown_jitter_seconds(key_index: int, *, max_jitter_seconds: float) -> float:
    """
    Deterministic jitter to avoid synchronized cooldown expiry bursts across keys.

    We keep this stable per key_index so operators can reason about it, while still ensuring
    different keys don't all wake up at the exact same second.
    """
    try:
        max_jitter = float(max_jitter_seconds)
    except Exception:
        max_jitter = 0.0
    if max_jitter <= 0:
        return 0.0
    # Simple hash-like mix to spread 1..N across [0,1).
    frac = ((int(key_index) * 9973) % 1000) / 1000.0
    return max(0.0, min(max_jitter, frac * max_jitter))


def _interruptible_sleep(
    *,
    max_seconds: float,
    check_remaining_seconds,
    step_seconds: float = 1.0,
    min_sleep_seconds: float = 0.25,
    should_stop=None,
) -> bool:
    """
    Sleep up to max_seconds, but wake early when the wait condition clears.

    Used so operator actions (e.g. `queue reset-key` clearing Redis keys) can unblock workers promptly,
    instead of waiting for a long `time.sleep()` to finish.

    Returns True when interrupted by `should_stop`, else False.
    """
    deadline = time.monotonic() + max(0.0, float(max_seconds or 0.0))
    step = max(0.05, float(step_seconds or 0.0))
    min_sleep = max(0.01, float(min_sleep_seconds or 0.0))
    while True:
        if should_stop is not None:
            try:
                if bool(should_stop()):
                    return True
            except Exception:
                pass
        remaining_gate = 0.0
        try:
            remaining_gate = float(check_remaining_seconds() or 0.0)
        except Exception:
            remaining_gate = 0.0
        if remaining_gate <= 0.05:
            return False

        remaining_budget = deadline - time.monotonic()
        if remaining_budget <= 0:
            return False

        sleep_seconds = min(remaining_budget, remaining_gate, step)
        time.sleep(max(min_sleep, sleep_seconds))


def _probe_gemini_429(
    *,
    config: NovelConfig | QueueConfig,
    api_key: str,
    model: str,
    key_index: int,
    proxy_cfg=None,
    timeout_seconds: float = 10.0,
) -> bool | None:
    """
    Lightweight "ping" request to detect whether the Gemini API is currently returning HTTP 429.

    Returns:
      - True: confirmed 429
      - False: request completed successfully and was not 429
      - None: probe failed or returned another transient/proxy error (403/5xx/etc.)
    """

    api_key = (api_key or "").strip()
    model = (model or "").strip()
    if not api_key or not model:
        return None
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}"
    body = {
        "contents": [{"parts": [{"text": "hello"}]}],
        "generationConfig": {"temperature": 0.0, "topP": 0.9, "maxOutputTokens": 1},
    }
    _pcfg = proxy_cfg if proxy_cfg is not None else getattr(config, "proxy_gateway", None)
    _rcfg = _queue_config(config).redis
    try:
        response = proxy_gateway_mod.request(
            "POST",
            url,
            headers={"Content-Type": "application/json"},
            body=body,
            cfg=_pcfg,
            key_index=int(key_index),
            redis_cfg=_rcfg,
            timeout_seconds=max(1.0, float(timeout_seconds)),
        )
    except Exception as exc:
        LOGGER.debug("Gemini 429 probe failed | model=%s err=%s", model, exc)
        return None
    status_code = int(getattr(response, "status_code", 0) or 0)
    if status_code == 429:
        return True
    if status_code == 200:
        return False
    if status_code == 403 or status_code >= 500:
        return None
    return False


def _parse_quota_suggested_wait_seconds(text: str) -> float | None:
    if not text:
        return None
    # CLI logs: "Gemini quota exceeded (... suggested_wait=4.58s)"
    match = re.search(r"suggested_wait=([0-9]+(?:\.[0-9]+)?)s", text, flags=re.I)
    if match:
        try:
            return float(match.group(1))
        except ValueError:
            return None
    # Provider logs: "Gemini quota wait 5.5s | ..."
    match = re.search(r"\bquota wait\s+([0-9]+(?:\.[0-9]+)?)s\b", text, flags=re.I)
    if match:
        try:
            return float(match.group(1))
        except ValueError:
            return None
    return None


def _parse_quota_blocked_model(text: str) -> str | None:
    if not text:
        return None
    # Worker logs: "... blocked_model=<model> ..."
    match = re.search(r"\bblocked_model\s*=\s*([A-Za-z0-9_.:-]+)", text, flags=re.I)
    if match:
        value = (match.group(1) or "").strip()
        return value or None
    # CLI quota error: "... quota exceeded (model=<model> ...)"
    match = re.search(r"\bmodel\s*=\s*([A-Za-z0-9_.:-]+)", text, flags=re.I)
    if match:
        value = (match.group(1) or "").strip()
        return value or None
    return None


def _parse_quota_reason_tokens(text: str) -> set[str]:
    if not text:
        return set()
    match = re.search(r"\breasons\s*=\s*([A-Za-z0-9_,.-]+)", text, flags=re.I)
    if not match:
        return set()
    raw = (match.group(1) or "").strip()
    if not raw:
        return set()
    out: set[str] = set()
    for token in raw.split(","):
        value = (token or "").strip().upper()
        if value:
            out.add(value)
    return out


def _parse_quota_estimated_tokens(text: str) -> int | None:
    """
    Parse the "+<estimated>/<limit>" token component from provider quota logs.

    Example:
      "tokens=8801+7791/15000" -> 7791
    """

    if not text:
        return None
    match = re.search(r"tokens=\s*\d+\s*\+\s*(\d+)\s*/\s*\d+", text, flags=re.I)
    if not match:
        return None
    try:
        value = int(match.group(1))
    except ValueError:
        return None
    return value if value > 0 else None


def _parse_quota_should_requeue(text: str) -> bool:
    if not text:
        return False
    return bool(re.search(r"\brequeue\s*=\s*1\b", text, flags=re.I))


def _tail_lines(path: str, max_bytes: int = 16384, max_lines: int = 80) -> list[str]:
    """Return the last N lines of a text file without reading it all."""
    if not path:
        return []
    try:
        with open(path, "rb") as fh:
            try:
                fh.seek(0, os.SEEK_END)
                end = fh.tell()
                start = max(0, end - max_bytes)
                fh.seek(start, os.SEEK_SET)
                data = fh.read()
            except OSError:
                data = fh.read()
    except OSError:
        return []

    try:
        text = data.decode("utf-8", errors="replace")
    except Exception:
        return []
    lines = text.splitlines()
    return lines[-max_lines:] if len(lines) > max_lines else lines


def _parse_log_timestamp(line: str) -> datetime | None:
    # Format: "YYYY-MM-DD HH:MM:SS,mmm | LEVEL | ..."
    if not line:
        return None
    prefix = line.split("|", 1)[0].strip()
    try:
        return datetime.strptime(prefix, "%Y-%m-%d %H:%M:%S,%f")
    except Exception:
        return None


def _waiting_expired(line: str, *, now: datetime, grace_seconds: float = 2.0) -> tuple[bool, float | None]:
    """Return (expired, wait_seconds) for known waiting/sleep lines, or (False, None) if unknown."""
    if not line:
        return False, None
    ts = _parse_log_timestamp(line)
    if ts is None:
        return False, None

    lowered = line.lower()
    sleep_match = re.search(r"sleeping for ([0-9]+(?:\.[0-9]+)?)s", lowered)
    if sleep_match:
        wait_s = float(sleep_match.group(1))
        return (now >= (ts + timedelta(seconds=wait_s + grace_seconds))), wait_s

    quota_match = re.search(r"wait_seconds=([0-9]+(?:\.[0-9]+)?)", lowered)
    if quota_match:
        wait_s = float(quota_match.group(1))
        return (now >= (ts + timedelta(seconds=wait_s + grace_seconds))), wait_s

    return False, None


def _waiting_countdown_seconds(line: str, *, now: datetime, grace_seconds: float = 2.0) -> float | None:
    """Return remaining seconds for a waiting line, or None if not a waiting line."""
    if not line:
        return None
    ts = _parse_log_timestamp(line)
    if ts is None:
        return None
    expired, wait_s = _waiting_expired(line, now=now, grace_seconds=grace_seconds)
    if wait_s is None:
        return None
    if expired:
        return 0.0
    end = ts + timedelta(seconds=float(wait_s) + float(grace_seconds))
    return max(0.0, (end - now).total_seconds())


def _format_countdown(seconds: float | None) -> str:
    """Human-friendly countdown format for queue ps tables.

    Rules:
    - Base format: hh:mm:ss with units (e.g. 1h:23m:39s ; 43m:24s ; 47s)
    - If countdown > 3 minutes, only show hh:mm (e.g. 1h:32m ; 34m)
    """
    if seconds is None:
        return ""
    try:
        value = float(seconds)
    except Exception:
        return ""
    if value <= 0:
        return ""

    total_s = int(math.ceil(value))
    if total_s <= 0:
        return ""

    if total_s > 180:
        total_m = total_s // 60
        hours = total_m // 60
        minutes = total_m % 60
        if hours > 0:
            return f"{hours}h:{minutes}m"
        return f"{minutes}m"

    hours = total_s // 3600
    rem = total_s % 3600
    minutes = rem // 60
    secs = rem % 60

    parts: list[str] = []
    if hours > 0:
        parts.append(f"{hours}h")
    if minutes > 0 or hours > 0:
        parts.append(f"{minutes}m")
    parts.append(f"{secs}s")
    return ":".join(parts)


def _format_target(file_arg: str, chapter_arg: str) -> str:
    file_arg = (file_arg or "").strip()
    chapter_arg = (chapter_arg or "").strip()
    if not file_arg or not chapter_arg:
        return ""
    base = os.path.basename(file_arg)
    if chapter_arg.isdigit():
        chapter_arg = f"{int(chapter_arg):04d}"
    return f"{base}:{chapter_arg}"


def _extract_target_from_argv(argv: list[str]) -> str:
    file_arg = ""
    chapter_arg = ""
    for idx, token in enumerate(argv):
        if token == "--file" and idx + 1 < len(argv):
            file_arg = argv[idx + 1]
        elif token == "--chapter" and idx + 1 < len(argv):
            chapter_arg = argv[idx + 1]
    return _format_target(file_arg, chapter_arg)


def _split_csv_flags(values: list[str]) -> list[str]:
    """
    Parse repeatable argparse flags that also allow comma-separated values.

    Example:
      ["k1,k2", "k3"] -> ["k1", "k2", "k3"]
    """
    parsed: list[str] = []
    for raw in values or []:
        for part in str(raw).split(","):
            part = part.strip()
            if part:
                parsed.append(part)
    return parsed


def _resolve_key_indices(selectors: list[str], keys: list[str]) -> list[int]:
    """
    Resolve key selectors into key indices (1-based).

    Selectors can be:
      - "k5" (key index)
      - raw key string (exact match in keys file)
    """
    resolved: list[int] = []
    for selector in selectors:
        value = str(selector or "").strip()
        if not value:
            continue
        lowered = value.lower()
        if lowered.startswith("k") and lowered[1:].isdigit():
            idx = int(lowered[1:])
            if idx <= 0 or idx > len(keys):
                raise ValueError(f"Invalid key index {value!r} (expected 1..{len(keys)})")
            resolved.append(idx)
            continue
        # Raw key: must exactly match a key in the file.
        try:
            resolved.append(keys.index(value) + 1)
        except ValueError as exc:
            raise ValueError(f"Unknown raw key {value!r} (no exact match in keys file)") from exc

    # Deduplicate while preserving order.
    seen: set[int] = set()
    unique: list[int] = []
    for idx in resolved:
        if idx in seen:
            continue
        seen.add(idx)
        unique.append(idx)
    return unique


def _reset_queue_key_state(client, config: NovelConfig, *, key_indices: list[int], models: list[str]) -> int:
    deleted = 0
    for key_index in key_indices:
        # Per-key pick throttle.
        deleted += int(client.delete(_pick_last_ms_key(config, key_index)) or 0)
        for model in models:
            deleted += int(client.delete(_rate_limit_cooldown_key(config, key_index=key_index, model=model)) or 0)
            deleted += int(client.delete(_out_of_quota_cooldown_key(config, key_index=key_index, model=model)) or 0)
            deleted += int(client.delete(_minute_quota_key(config, key_index, model)) or 0)
            deleted += int(client.delete(_minute_token_key(config, key_index, model)) or 0)
            deleted += int(client.delete(_daily_quota_key(config, key_index, model)) or 0)
            # Central quota v2 keys (freezed/locked, alloc queue).
            key_prefix = _worker_key_prefix_for_index(config, key_index=int(key_index))
            deleted += int(client.delete(f"{key_prefix}:{model}:quota:alloc:queue") or 0)
            deleted += int(client.delete(f"{key_prefix}:{model}:quota:tpm:freezed") or 0)
            deleted += int(client.delete(f"{key_prefix}:{model}:quota:tpm:freezed_tokens") or 0)
            deleted += int(client.delete(f"{key_prefix}:{model}:quota:tpm:locked") or 0)
            deleted += int(client.delete(f"{key_prefix}:{model}:quota:tpm:locked_tokens") or 0)
            deleted += int(client.delete(f"{key_prefix}:{model}:quota:rpm:freezed") or 0)
            deleted += int(client.delete(f"{key_prefix}:{model}:quota:rpm:locked") or 0)
            deleted += int(client.delete(f"{key_prefix}:{model}:quota:rpd:freezed") or 0)
            deleted += int(client.delete(f"{key_prefix}:{model}:quota:rpd:locked") or 0)
            # Legacy index-based keys from before stable raw-key identity.
            legacy_prefix = f"{config.queue.redis.prefix}:{config.novel_id}:k{int(key_index)}"
            deleted += int(client.delete(f"{legacy_prefix}:{model}:quota:alloc:queue") or 0)
            deleted += int(client.delete(f"{legacy_prefix}:{model}:quota:tpm:freezed") or 0)
            deleted += int(client.delete(f"{legacy_prefix}:{model}:quota:tpm:freezed_tokens") or 0)
            deleted += int(client.delete(f"{legacy_prefix}:{model}:quota:tpm:locked") or 0)
            deleted += int(client.delete(f"{legacy_prefix}:{model}:quota:tpm:locked_tokens") or 0)
            deleted += int(client.delete(f"{legacy_prefix}:{model}:quota:rpm:freezed") or 0)
            deleted += int(client.delete(f"{legacy_prefix}:{model}:quota:rpm:locked") or 0)
            deleted += int(client.delete(f"{legacy_prefix}:{model}:quota:rpd:freezed") or 0)
            deleted += int(client.delete(f"{legacy_prefix}:{model}:quota:rpd:locked") or 0)
    return deleted


def reset_queue_key_state(
    config: NovelConfig | QueueConfig,
    *,
    key_selectors: list[str],
    all_keys: bool = False,
    model_selectors: list[str] | None = None,
) -> int:
    """
    Reset per-key queue state in Redis (cooldown + quota + pick throttle).

    - key_selectors: list of "kN" or raw keys (exact match).
    - all_keys: if True, reset all keys found in the keys file.
    - model_selectors: optional list of enabled model names (supports comma-separated in CLI parsing).
      If omitted/empty, defaults to enabled_models.
    """
    qcfg = _queue_config(config)
    keys_raw = _load_keys(config)
    if all_keys:
        if key_selectors:
            raise ValueError("Use either --all or --key, not both")
        if not keys_raw:
            raise ValueError("No keys found in .secrets/gemini-keys.txt")
        key_indices = list(range(1, len(keys_raw) + 1))
    else:
        selectors = _split_csv_flags(key_selectors or [])
        if not selectors:
            raise ValueError("Missing --key (expected kN or raw key), or use --all")
        key_indices = _resolve_key_indices(selectors, keys_raw)
        if not key_indices:
            raise ValueError("No valid keys resolved from --key")

    enabled_models = list(qcfg.enabled_models or [])
    models = _split_csv_flags(list(model_selectors or [])) if model_selectors else []
    if not models:
        models = enabled_models
    if not models:
        raise ValueError("No models configured (queue.enabled_models is empty)")

    unknown_models = [m for m in models if m not in enabled_models]
    if unknown_models:
        raise ValueError(
            "Unknown --model value(s): "
            + ", ".join(sorted(set(unknown_models)))
            + " (expected one of enabled_models: "
            + ", ".join(enabled_models)
            + ")"
        )

    client = _client(config)
    deleted = _reset_queue_key_state(client, config, key_indices=key_indices, models=models)
    keys_text = ",".join(f"k{idx}" for idx in key_indices)
    models_text = ",".join(models)
    print(f"Reset key state | keys={keys_text} models={models_text} deleted={deleted}")
    return 0


def _unique_target_count(rows: list[dict[str, str]]) -> int:
    """Count unique chapter targets currently being processed.

    Prefer translate-chapter subprocess targets to avoid double-counting when a worker
    surfaces the same target as its child.
    """

    def _targets_for_role(role: str) -> set[str]:
        targets: set[str] = set()
        for row in rows:
            if (row.get("role") or "") != role:
                continue
            value = (row.get("target") or "").strip()
            if value:
                targets.add(value)
        return targets

    translate_targets = _targets_for_role("translate-chapter")
    if translate_targets:
        return len(translate_targets)

    # Fallback: count any surfaced target (e.g. when subprocess roles aren't present).
    any_targets: set[str] = set()
    for row in rows:
        value = (row.get("target") or "").strip()
        if value:
            any_targets.add(value)
    return len(any_targets)


def _classify_process_state(role: str, *, is_busy: bool, log_file: str) -> tuple[str, float | None]:
    """Heuristic state classifier for ps output."""
    lines = _tail_lines(log_file)
    tail = "\n".join(lines[-40:]).lower()

    # Hard errors (exceptions, crashes) should be visible briefly, but not sticky.
    # If an error occurred recently (within a short window), surface "error" to catch operator attention.
    now = datetime.now()
    error_hold_seconds = 5.0
    recent_error_ts: datetime | None = None
    for idx in range(len(lines) - 1, max(-1, len(lines) - 201), -1):
        raw = lines[idx]
        line = (raw or "").strip()
        lowered = line.lower()
        if not lowered:
            continue
        if "traceback" not in lowered and "command failed" not in lowered:
            continue

        # Try to find a timestamp for this error event.
        # Some traceback lines don't include timestamps; in that case, walk backwards to find
        # the nearest preceding timestamped log line, then fall back to the file mtime.
        for j in range(idx, max(-1, idx - 21), -1):
            ts = _parse_log_timestamp((lines[j] or "").strip())
            if ts is not None:
                recent_error_ts = ts
                break
        if recent_error_ts is None and log_file:
            try:
                recent_error_ts = datetime.fromtimestamp(os.path.getmtime(log_file))
            except Exception:
                recent_error_ts = None
        break

    if recent_error_ts is not None and (now - recent_error_ts).total_seconds() <= error_hold_seconds:
        return "error", None

    if role == "worker":
        # Prefer process-tree truth when available.
        if is_busy:
            return "busy", None

        # When idle (no translate-chapter child), classify based on the most recent relevant log event.
        # We scan from the end so older 429s don't permanently label the worker.
        for raw in reversed(lines[-200:]):
            line = (raw or "").lower()
            if not line:
                continue
            if any(token in line for token in _OUT_OF_QUOTA_TOKENS):
                remaining = _waiting_countdown_seconds(raw or "", now=now)
                if remaining is not None and remaining > 0:
                    return "out-of-quota", remaining
            if any(token in line for token in _QUOTA_WAIT_TOKENS):
                remaining = _waiting_countdown_seconds(raw or "", now=now)
                if remaining is not None and remaining > 0:
                    return "waiting-quota", remaining
            # Match the specific "rate limit hit + sleeping" style so we don't trigger on unrelated "429" text.
            if ("rate limit" in line or "429" in line or "too many requests" in line) and "sleep" in line:
                remaining = _waiting_countdown_seconds(raw or "", now=now)
                if remaining is not None and remaining > 0:
                    return "waiting-429", remaining
            if "worker done:" in line or "rebuilt file:" in line or "translated chapter part:" in line:
                return "idle", None
        return "idle", None

    if role == "translate-chapter":
        # This process exists only while doing work, but it may be sleeping on rate limits/quota.
        phase: str | None = None
        for raw in reversed(lines[-200:]):
            lowered = (raw or "").lower()
            if not lowered:
                continue
            if any(token in lowered for token in _QUOTA_WAIT_TOKENS):
                remaining = _waiting_countdown_seconds(raw or "", now=now)
                if remaining is not None and remaining > 0:
                    return "waiting-quota", remaining
            if ("rate limit" in lowered or "429" in lowered or "too many requests" in lowered) and "sleep" in lowered:
                remaining = _waiting_countdown_seconds(raw or "", now=now)
                if remaining is not None and remaining > 0:
                    return "waiting-429", remaining
            if "read timed out" in lowered or ("connectionpool" in lowered and "timed out" in lowered):
                phase = "upstream-timeout"
                break
            if "connectionerror" in lowered or "connection error" in lowered:
                phase = "upstream-conn"
                break
            # Infer phases from high-signal translation logs so long-running units don't degrade to generic "busy".
            if (
                "queue_phase glossary" in lowered
                or "glossary extract" in lowered
                or "updated glossary" in lowered
                or "keeping existing glossary entry" in lowered
            ):
                phase = "glossary"
                break
            if (
                "queue_phase repair" in lowered
                or "final_cleanup" in lowered
                or "placeholder tokens detected" in lowered
                or "han residue detected" in lowered
                or "patch_remaining_han" in lowered
                or "repair_against_source" in lowered
                or "aggressive_repair" in lowered
            ):
                phase = "repair"
                break
            if (
                "queue_phase translate" in lowered
                or ("translating " in lowered and " chunk " in lowered)
                or ("translated " in lowered and " chunk " in lowered)
            ):
                phase = "translate"
                break
            # If we hit a completion marker in this log stream, treat the child as busy only if it's actually running.
            if "worker done:" in lowered or "rebuilt file:" in lowered or "translated chapter part:" in lowered:
                break
        return phase or "busy", None
    return "running", None


def _combine_worker_child_states(child_states: list[str]) -> str | None:
    """Pick a representative state for a worker based on its translate-chapter children."""
    if not child_states:
        return None
    # Priority: if any child is waiting/error, the worker is effectively waiting/error too.
    priority = {
        "error": 4,
        "out-of-quota": 3,
        "waiting-quota": 3,
        "waiting-429": 2,
        "glossary": 2,
        "repair": 2,
        "translate": 1,
        "busy": 1,
    }
    best = None
    best_score = 0
    for state in child_states:
        score = priority.get(state or "", 0)
        if score > best_score:
            best_score = score
            best = state
    return best


def _combine_worker_child_states_with_countdown(children: list[dict[str, str]]) -> tuple[str | None, float | None]:
    if not children:
        return None, None
    # Reuse the same priority as the string-only combiner.
    priority = {
        "error": 4,
        "out-of-quota": 3,
        "waiting-quota": 3,
        "waiting-429": 2,
        "glossary": 2,
        "repair": 2,
        "translate": 1,
        "busy": 1,
    }
    best_state: str | None = None
    best_score = 0
    for child in children:
        state = (child.get("state") or "").strip()
        score = priority.get(state, 0)
        if score > best_score:
            best_score = score
            best_state = state
    if not best_state:
        return None, None
    if best_state.startswith("waiting-"):
        remaining: float | None = None
        for child in children:
            if (child.get("state") or "").strip() != best_state:
                continue
            try:
                value = float(child.get("countdown") or 0.0)
            except Exception:
                continue
            remaining = value if remaining is None else max(remaining, value)
        return best_state, remaining
    return best_state, None


def _redis_cfg(config: NovelConfig | QueueConfig | RedisConfig) -> RedisConfig:
    if isinstance(config, RedisConfig):
        return config
    if isinstance(config, QueueConfig):
        return config.redis
    return config.queue.redis


def _prefix(config: NovelConfig | QueueConfig | RedisConfig) -> str:
    return str(_redis_cfg(config).prefix or "").strip() or "novel_tts"


def _client(config: NovelConfig | QueueConfig | RedisConfig):
    import redis

    rcfg = _redis_cfg(config)
    return redis.Redis(
        host=rcfg.host,
        port=rcfg.port,
        db=rcfg.database,
        decode_responses=True,
    )


def _global_key(config: NovelConfig | QueueConfig | RedisConfig, suffix: str) -> str:
    """Shared (cross-novel) Redis key: {prefix}:{suffix}."""
    return f"{_prefix(config)}:{suffix}"


def _novel_key(config: NovelConfig | QueueConfig | RedisConfig, novel_id: str, suffix: str) -> str:
    """Per-novel Redis key: {prefix}:novel:{novel_id}:{suffix}."""
    return f"{_prefix(config)}:novel:{novel_id}:{suffix}"


def _key(config: NovelConfig, suffix: str) -> str:
    """Per-novel key using config.novel_id."""
    return _novel_key(config, config.novel_id, suffix)


def _scan_novel_keys(client, prefix: str, suffix: str) -> list[str]:
    """SCAN for all per-novel keys matching {prefix}:novel:*:{suffix}."""
    pattern = f"{prefix}:novel:*:{suffix}"
    result: list[str] = []
    cursor = "0"
    while True:
        cursor, keys = client.scan(cursor=cursor, match=pattern, count=200)
        for k in keys:
            result.append(k if isinstance(k, str) else k.decode("utf-8"))
        if cursor == 0 or cursor == "0" or cursor == b"0":
            break
    return result


def _pending_priority_key(config: NovelConfig | QueueConfig) -> str:
    return _global_key(config, "pending_priority")


def _pending_delayed_key(config: NovelConfig | QueueConfig) -> str:
    return _global_key(config, "pending_delayed")


def _global_pending_key(config: NovelConfig | QueueConfig) -> str:
    return _global_key(config, "pending")


def _global_queued_key(config: NovelConfig | QueueConfig) -> str:
    return _global_key(config, "queued")


def _global_inflight_key(config: NovelConfig | QueueConfig) -> str:
    return _global_key(config, "inflight")


def _global_force_key(config: NovelConfig | QueueConfig) -> str:
    return _global_key(config, "force")


def _global_stopping_key(config: NovelConfig | QueueConfig) -> str:
    return _global_key(config, "stopping")


def _pending_key(config: NovelConfig | QueueConfig) -> str:
    return _global_pending_key(config)


def _queued_key(config: NovelConfig | QueueConfig) -> str:
    return _global_queued_key(config)


def _inflight_key(config: NovelConfig | QueueConfig) -> str:
    return _global_inflight_key(config)


def _force_key(config: NovelConfig | QueueConfig) -> str:
    return _global_force_key(config)


def _pending_total_len(config: NovelConfig | QueueConfig, client) -> int:
    return int(client.llen(_pending_priority_key(config)) or 0) + int(client.llen(_pending_key(config)) or 0)


# ---------------------------------------------------------------------------
# Graceful-shutdown ("stopping") signal
# ---------------------------------------------------------------------------


def _stopping_key(config: NovelConfig | QueueConfig) -> str:
    return _global_key(config, "stopping")


def _is_stopping(config: NovelConfig | QueueConfig, client) -> bool:
    """Return True if a graceful-stop signal has been set."""
    return bool(client.exists(_stopping_key(config)))


def _set_stopping(config: NovelConfig | QueueConfig, client) -> None:
    """Fire the graceful-stop signal.  Workers will finish their current job
    then exit; the supervisor will stop spawning new workers."""
    client.set(
        _stopping_key(config),
        json.dumps({"requested_at": time.time(), "pid": os.getpid()}),
    )
    LOGGER.info("Graceful-stop signal set")


def _clear_stopping(config: NovelConfig | QueueConfig, client) -> None:
    """Remove the graceful-stop signal (e.g. after all workers have drained)."""
    client.delete(_stopping_key(config))


_PICK_THROTTLE_POP_LUA = r"""
-- KEYS[1] = pending_priority list
-- KEYS[2] = pending list
-- KEYS[3] = last_pick_ms key
-- ARGV[1] = min_interval_ms

local min_ms = tonumber(ARGV[1]) or 0
if min_ms <= 0 then
  local job = redis.call('LPOP', KEYS[1])
  if not job then
    job = redis.call('LPOP', KEYS[2])
  end
  return { job or "", "0" }
end

local now = redis.call('TIME')
local now_ms = (tonumber(now[1]) * 1000) + math.floor(tonumber(now[2]) / 1000)

local last = redis.call('GET', KEYS[3])
if last then
  last = tonumber(last) or 0
  local elapsed = now_ms - last
  if elapsed < min_ms then
    return { "", tostring(min_ms - elapsed) }
  end
end

local job = redis.call('LPOP', KEYS[1])
if not job then
  job = redis.call('LPOP', KEYS[2])
end
if not job then
  return { "", "0" }
end

redis.call('SET', KEYS[3], tostring(now_ms))
return { job, "0" }
"""


def _pick_last_ms_key(config: NovelConfig | QueueConfig, key_index: int) -> str:
    # Per-key throttle: all workers for the same key_index will serialize picks.
    key_prefix = _worker_key_prefix_for_index(config, key_index=int(key_index))
    return f"{key_prefix}:last_pick_ms"


def _queue_config(config: NovelConfig | QueueConfig) -> QueueConfig:
    """Extract QueueConfig regardless of whether NovelConfig or QueueConfig was passed."""
    return config if isinstance(config, QueueConfig) else config.queue


def _throttled_pick_job_id(
    config: NovelConfig | QueueConfig,
    client,
    *,
    key_index: int,
    model: str,
    timeout_seconds: float = 5.0,
) -> str | None:
    """
    Pick a job id from the shared pending_priority/pending lists with a Redis throttle.

    Throttle is scoped to key_index so multiple worker processes for the same API key
    won't all pick at once (and then burst LLM requests).
    """

    qcfg = _queue_config(config)
    min_interval = 0.0
    try:
        min_interval = float(getattr(qcfg, "min_pick_interval_seconds", 0.0) or 0.0)
    except Exception:
        min_interval = 0.0
    if min_interval <= 0:
        deadline = time.monotonic() + max(0.0, float(timeout_seconds or 0.0))
        while True:
            if _ip_ban_is_active(client, config, model=model):
                return None
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None
            item = client.blpop(
                [_pending_priority_key(config), _pending_key(config)],
                timeout=min(1, max(1, int(remaining))),
            )
            job_id = item[1] if item else None
            if not job_id:
                continue
            # If an IP-ban is triggered while we were blocked waiting, push the job back and pause.
            if _ip_ban_is_active(client, config, model=model):
                client.lpush(_pending_priority_key(config), job_id)
                return None
            return job_id

    min_ms = max(1, int(min_interval * 1000.0))
    deadline = time.monotonic() + max(0.0, float(timeout_seconds or 0.0))
    last_key = _pick_last_ms_key(config, key_index)
    pending_priority = _pending_priority_key(config)
    pending = _pending_key(config)

    while True:
        if _ip_ban_is_active(client, config, model=model):
            return None
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return None

        try:
            job_id, wait_ms = client.eval(
                _PICK_THROTTLE_POP_LUA,
                3,
                pending_priority,
                pending,
                last_key,
                str(min_ms),
            )
        except Exception:
            # Fall back to legacy behavior if scripting is unavailable/misconfigured.
            item = client.blpop([pending_priority, pending], timeout=min(5, max(1, int(remaining))))
            return item[1] if item else None

        job_id = (job_id or "").strip()
        if job_id:
            if _ip_ban_is_active(client, config, model=model):
                client.lpush(_pending_priority_key(config), job_id)
                return None
            return job_id

        try:
            wait_seconds = max(0.0, float(wait_ms or 0) / 1000.0)
        except Exception:
            wait_seconds = 0.0

        base_sleep = max(0.05, min(0.25, remaining))
        sleep_seconds = min(remaining, max(base_sleep, wait_seconds) + random.uniform(0.0, 0.05))
        time.sleep(max(0.01, sleep_seconds))


def _requeue_job_priority(config: NovelConfig | QueueConfig, client, job_id: str) -> bool:
    """
    Requeue a job at the front of the queue to bias toward finishing partially-started work.

    Returns True if the job was newly queued and pushed.
    """

    if client.sadd(_queued_key(config), job_id):
        client.lpush(_pending_priority_key(config), job_id)
        return True
    return False


def _delay_job(config: NovelConfig | QueueConfig, client, job_id: str, delay_seconds: float) -> None:
    """
    Put a released job into a delayed queue so it won't be re-picked until the delay expires.

    This prevents tight requeue/pick loops when the provider recommends a long wait (e.g. TPM gate ~50s).
    """

    try:
        delay = float(delay_seconds)
    except Exception:
        delay = 0.0
    if delay <= 0:
        _requeue_job_priority(config, client, job_id)
        return
    now = time.time()
    ready_at = now + max(0.25, delay)
    # Keep the job "queued" so the supervisor doesn't enqueue duplicates.
    client.sadd(_queued_key(config), job_id)
    client.zadd(_pending_delayed_key(config), {job_id: ready_at})


def _drain_delayed_jobs(config: NovelConfig | QueueConfig, client, *, max_items: int = 500) -> int:
    """Move due delayed jobs back into the priority queue."""
    now = time.time()
    delayed_key = _pending_delayed_key(config)
    try:
        ready = client.zrangebyscore(delayed_key, "-inf", now, start=0, num=max_items)
    except Exception:
        return 0
    if not ready:
        return 0
    # Maintain zset order (oldest ready first) while pushing into the priority list head.
    # If ready=[a,b,c], pushing reversed via LPUSH yields [a,b,c,...].
    with client.pipeline() as pipe:
        pipe.zrem(delayed_key, *ready)
        for job_id in reversed(ready):
            pipe.lpush(_pending_priority_key(config), job_id)
        pipe.execute()
    return len(ready)


def _any_idle_worker(config: NovelConfig | QueueConfig) -> bool:
    """
    Best-effort check for whether *any* queue worker appears idle (not busy, not waiting).

    Used as a heuristic when deciding whether to hold a quota-gated job vs releasing it back to the queue.
    If we cannot determine this safely (e.g., ps permission denied), return True to avoid holding work.
    """

    try:
        proc = subprocess.run(
            ["ps", "ax", "-o", "pid=,ppid=,command="],
            check=False,
            capture_output=True,
            text=True,
        )
    except PermissionError:
        return True
    except Exception:
        return True
    if proc.returncode != 0:
        return True

    lines = (proc.stdout or "").splitlines()
    workers: dict[str, dict[str, str]] = {}
    children_by_ppid: dict[str, list[dict[str, str]]] = {}
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            pid_str, ppid_str, cmd = line.split(None, 2)
        except ValueError:
            continue
        pid = pid_str.strip()
        ppid = ppid_str.strip()
        if "novel_tts" not in cmd:
            continue

        try:
            argv = shlex.split(cmd)
        except Exception:
            argv = cmd.split()

        role = ""
        log_file = ""
        if "queue" in argv:
            q_idx = argv.index("queue")
            subcmd = argv[q_idx + 1] if q_idx + 1 < len(argv) else ""
            if subcmd == "worker":
                role = "worker"

        if not role and "translate" in argv:
            t_idx = argv.index("translate")
            if t_idx + 1 < len(argv) and argv[t_idx + 1] == "chapter":
                role = "translate-chapter"

        if not role:
            continue

        for idx, token in enumerate(argv):
            if token == "--log-file" and idx + 1 < len(argv):
                log_file = argv[idx + 1]

        row = {"pid": pid, "ppid": ppid, "role": role, "log_file": log_file}
        children_by_ppid.setdefault(ppid, []).append(row)
        if role == "worker":
            workers[pid] = row

    for pid, row in workers.items():
        is_busy = any(child.get("role") == "translate-chapter" for child in children_by_ppid.get(pid, []))
        state, _countdown = _classify_process_state("worker", is_busy=is_busy, log_file=row.get("log_file", "") or "")
        if state == "idle":
            return True
    return False


def _worker_is_recently_picking(config: NovelConfig | QueueConfig, client, *, key_index: int) -> bool:
    try:
        raw = client.get(_pick_last_ms_key(config, key_index))
        last_ms = float(raw or 0.0)
    except Exception:
        return False
    if last_ms <= 0:
        return False
    try:
        min_interval = float(getattr(_queue_config(config), "min_pick_interval_seconds", 0.0) or 0.0)
    except Exception:
        min_interval = 0.0
    # Keep the UI hint short-lived: enough to cover the throttle/handoff window, but not a steady state.
    recent_window_ms = max(1500.0, min(5000.0, max(min_interval * 1500.0, 1500.0)))
    now_ms = time.time() * 1000.0
    return (now_ms - last_ms) <= recent_window_ms

def _project_root() -> Path:
    """Resolve project root (where .secrets/ lives) without a NovelConfig."""
    return Path(__file__).resolve().parents[2]


def _key_file(config: NovelConfig | QueueConfig | None = None) -> Path:
    if isinstance(config, NovelConfig):
        return config.storage.root / ".secrets" / "gemini-keys.txt"
    return _project_root() / ".secrets" / "gemini-keys.txt"


def _load_keys(config: NovelConfig | QueueConfig | None = None) -> list[str]:
    key_file = _key_file(config)
    if not key_file.exists():
        raise FileNotFoundError(f"Missing key file: {key_file}")
    keys = [line.strip() for line in key_file.read_text(encoding="utf-8").splitlines() if line.strip()]
    if not keys:
        raise RuntimeError(f"No Gemini keys found in {key_file}")
    return keys


def _effective_worker_key_limit(config: NovelConfig | QueueConfig, *, total_keys: int, proxy_cfg=None) -> tuple[int, str]:
    """
    Compute effective max key-index that supervisor may spawn workers for.

    Rules:
    - Proxy gateway disabled -> direct mode -> cap to first 5 keys.
    - Proxy gateway enabled but healthy proxy list is unavailable/empty -> fallback direct -> cap to first 5 keys.
    - Proxy gateway enabled and has proxies -> no cap (use all keys).
    """

    total = max(0, int(total_keys))
    if total <= 0:
        return 0, ""

    if proxy_cfg is None:
        proxy_cfg = getattr(config, "proxy_gateway", None)
    if not bool(proxy_cfg and getattr(proxy_cfg, "enabled", False)):
        return min(total, _DIRECT_FALLBACK_MAX_KEYS), "proxy_gateway_disabled"

    redis_cfg = _redis_cfg(config)
    proxies: list[str] = []
    reason = ""
    if bool(getattr(proxy_cfg, "auto_discovery", True)):
        try:
            healthy, reason = proxy_gateway_mod.load_healthy_proxy_names_from_redis(
                cfg=proxy_cfg,
                redis_cfg=redis_cfg,
                now=time.time(),
            )
            proxies = [str(x).strip() for x in (healthy or []) if str(x).strip()]
        except Exception:
            proxies = []
            reason = "proxy_lookup_failed"
    else:
        proxies = [str(x).strip() for x in (getattr(proxy_cfg, "proxies", None) or []) if str(x).strip()]

    if not proxies:
        detail = (reason or "proxy_list_empty").strip()
        return min(total, _DIRECT_FALLBACK_MAX_KEYS), f"proxy_gateway_no_proxies:{detail}"
    return total, ""


def _job_id(novel_id: str, file_name: str, chapter_num: str) -> str:
    """Job ID format: {novel_id}::{file_name}::{chapter:04d}."""
    return f"{novel_id}::{file_name}::{int(chapter_num):04d}"


def _extract_novel_id(job_id: str) -> str:
    """Extract novel_id from a job_id (first segment before '::')."""
    return (job_id or "").split("::", 1)[0]


def _is_captions_job(job_id: str) -> bool:
    stripped = (job_id or "").strip()
    # Format: {novel_id}::captions
    return stripped.endswith(f"::{CAPTIONS_JOB_ID}")


def _captions_job_id(novel_id: str) -> str:
    return f"{novel_id}::{CAPTIONS_JOB_ID}"


def _is_repair_glossary_job(job_id: str) -> bool:
    # Format: {novel_id}::repair-glossary::{chunk:04d}
    parts = (job_id or "").split("::", 2)
    return len(parts) >= 2 and parts[1].startswith("repair-glossary")


def _repair_glossary_job_id(novel_id: str, chunk_index: int) -> str:
    return f"{novel_id}::{REPAIR_GLOSSARY_JOB_PREFIX}{chunk_index:04d}"


def _parse_repair_glossary_chunk_index(job_id: str) -> int:
    """Parse chunk index from repair glossary job id: {novel_id}::repair-glossary::{chunk:04d}."""
    parts = (job_id or "").split("::", 2)
    if len(parts) >= 3:
        return int(parts[2])
    # Fallback: {novel_id}::repair-glossary::XXXX -> last segment
    tail = parts[-1] if parts else ""
    return int(tail.replace("repair-glossary", "").strip(":"))


def _repair_glossary_chunk_needs_work(config: NovelConfig, chunk_index: int) -> bool:
    from novel_tts.translate.glossary_repair import get_repair_chunk_output_path
    return not get_repair_chunk_output_path(config, chunk_index).exists()


def _parse_job_id(job_id: str) -> tuple[str, str, str]:
    """Parse job_id into (novel_id, file_name, chapter_num).

    Format: {novel_id}::{file_name}::{chapter_num:04d}
    """
    parts = (job_id or "").split("::", 2)
    if len(parts) != 3:
        raise ValueError(f"Invalid job_id format: {job_id!r}")
    novel_id, file_name, chapter_num = parts
    return novel_id, file_name, str(int(chapter_num))


def _chapter_needs_translation(
    config: NovelConfig,
    source_path: Path,
    chapter_num: str,
    chapter_text: str | None = None,
) -> bool:
    part_path = chapter_part_path(config, source_path, chapter_num)
    if not part_path.exists():
        return True
    if chapter_text is None:
        try:
            chapter_text = load_chapter_map(config, source_path).get(str(int(chapter_num)), "")
        except Exception:
            chapter_text = ""
    if not (chapter_text or "").strip():
        return False
    # Hash-based staleness: only re-translate if the chapter source text changed.
    return chapter_source_changed(
        config,
        source_path,
        chapter_num,
        source_text=chapter_text,
        baseline_if_missing=True,
    )


def _chapter_needs_work(
    config: NovelConfig,
    source_path: Path,
    chapter_num: str,
    chapter_text: str | None = None,
) -> bool:
    if is_glossary_pending(config, source_path, chapter_num):
        return True
    return _chapter_needs_translation(config, source_path, chapter_num, chapter_text=chapter_text)


def _captions_needs_translation(config: NovelConfig) -> bool:
    input_path = config.storage.captions_dir / config.captions.input_file
    output_path = config.storage.captions_dir / config.captions.output_file
    if not input_path.exists():
        return False
    if not output_path.exists():
        return True
    return input_path.stat().st_mtime > output_path.stat().st_mtime


def _chapter_jobs_for_file(config: NovelConfig, source_path: Path) -> list[str]:
    jobs: list[str] = []
    for chapter_num, chapter_text in load_source_chapters(config, source_path):
        if _chapter_needs_work(config, source_path, chapter_num, chapter_text=chapter_text):
            jobs.append(_job_id(config.novel_id, source_path.name, chapter_num))
    return jobs


def _retry_count(config: NovelConfig | QueueConfig, client, job_id: str) -> int:
    novel_id = _extract_novel_id(job_id)
    retries_key = _novel_key(config, novel_id, "retries") if novel_id else _key(config, "retries")
    value = client.hget(retries_key, job_id)
    if value is None:
        return 0
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _has_exhausted_retries(config: NovelConfig | QueueConfig, client, job_id: str) -> bool:
    return _retry_count(config, client, job_id) >= _queue_config(config).max_retries


def _exhausted_retry_count(config: NovelConfig, client) -> int:
    count = 0
    for value in client.hgetall(_novel_key(config, config.novel_id, "retries")).values():
        try:
            retries = int(value)
        except (TypeError, ValueError):
            continue
        if retries >= config.queue.max_retries:
            count += 1
    return count


def _enqueue_needed_jobs(config: NovelConfig, client) -> None:
    for path in sorted(config.storage.origin_dir.glob("*.txt")):
        for job_id in _chapter_jobs_for_file(config, path):
            if client.hexists(_inflight_key(config), job_id):
                continue
            if _has_exhausted_retries(config, client, job_id):
                continue
            if client.sadd(_queued_key(config), job_id):
                client.rpush(_pending_key(config), job_id)
    if _captions_needs_translation(config):
        job_id = _captions_job_id(config.novel_id)
        if not client.hexists(_inflight_key(config), job_id) and not _has_exhausted_retries(config, client, job_id):
            if client.sadd(_queued_key(config), job_id):
                client.rpush(_pending_key(config), job_id)


def add_jobs_to_queue(config: NovelConfig, from_chapter: int, to_chapter: int, *, force: bool = False) -> int:
    """Enqueue a specific chapter range for translation.

    - Range is chapter-number based, independent of origin batch boundaries.
    - When force=True, jobs are marked so workers will translate even if parts are up-to-date.
    """
    if from_chapter > to_chapter:
        from_chapter, to_chapter = to_chapter, from_chapter

    client = _client(config)
    added = 0
    skipped_done = 0
    skipped_exhausted = 0

    for source_path in sorted(config.storage.origin_dir.glob("*.txt")):
        for chapter_num, chapter_text in load_source_chapters(config, source_path):
            try:
                chap = int(str(chapter_num))
            except Exception:
                continue
            if chap < from_chapter or chap > to_chapter:
                continue
            job_id = _job_id(config.novel_id, source_path.name, str(chap))
            if client.hexists(_inflight_key(config), job_id):
                continue
            if not force:
                if _has_exhausted_retries(config, client, job_id):
                    skipped_exhausted += 1
                    continue
                if not _chapter_needs_work(config, source_path, str(chap), chapter_text=chapter_text):
                    skipped_done += 1
                    continue

            if force:
                # Mark as force so workers won't skip due to up-to-date parts.
                client.hset(_force_key(config), job_id, str(int(time.time())))
                # Clear retries so a force enqueue gets a full retry budget again.
                client.hdel(_novel_key(config, config.novel_id, "retries"), job_id)

            if client.sadd(_queued_key(config), job_id):
                client.rpush(_pending_key(config), job_id)
                added += 1

    LOGGER.info(
        "Queue add | novel=%s range=%s-%s force=%s added=%s skipped_done=%s skipped_exhausted=%s",
        config.novel_id,
        from_chapter,
        to_chapter,
        force,
        added,
        skipped_done,
        skipped_exhausted,
    )
    print(
        f"Queued {added} job(s) for novel {config.novel_id} chapters {from_chapter}-{to_chapter}"
        f"{' (force)' if force else ''}. Skipped already-done={skipped_done}, exhausted-retries={skipped_exhausted}."
    )
    return 0


def wait_for_range_completion(
    config: NovelConfig,
    from_chapter: int,
    to_chapter: int,
    *,
    poll_interval_seconds: float = 2.0,
) -> int:
    """
    Block until every chapter in the requested range no longer needs queue work.

    This is intended for orchestration flows like `pipeline run`, where we want
    to use queue-first translation but still wait until the requested chapter
    range is complete before starting downstream media stages.
    """
    if from_chapter > to_chapter:
        from_chapter, to_chapter = to_chapter, from_chapter

    client = _client(config)
    logged_waiting = False

    while True:
        pending = _pending_total_len(config, client)
        queued = int(client.scard(_queued_key(config)) or 0)
        inflight = int(client.hlen(_inflight_key(config)) or 0)
        remaining = 0

        for source_path in sorted(config.storage.origin_dir.glob("*.txt")):
            for chapter_num, chapter_text in load_source_chapters(config, source_path):
                try:
                    chap = int(str(chapter_num))
                except Exception:
                    continue
                if chap < from_chapter or chap > to_chapter:
                    continue
                if _chapter_needs_work(config, source_path, str(chap), chapter_text=chapter_text):
                    remaining += 1

        if remaining == 0 and pending == 0 and queued == 0 and inflight == 0:
            LOGGER.info(
                "Queue range completed | novel=%s range=%s-%s",
                config.novel_id,
                from_chapter,
                to_chapter,
            )
            return 0

        if not logged_waiting:
            LOGGER.info(
                "Waiting for queue range completion | novel=%s range=%s-%s remaining=%s pending=%s queued=%s inflight=%s",
                config.novel_id,
                from_chapter,
                to_chapter,
                remaining,
                pending,
                queued,
                inflight,
            )
            logged_waiting = True

        time.sleep(max(0.5, float(poll_interval_seconds)))


def add_chapters_to_queue(config: NovelConfig, chapters: list[int], *, force: bool = False) -> int:
    """Enqueue an explicit list of chapters for translation.

    Useful for selective re-translation after crawl repair (replacement/placeholder rewrite).
    """
    wanted = sorted({int(ch) for ch in chapters if int(ch) > 0})
    if not wanted:
        print("No chapters provided.")
        return 0
    wanted_set = set(wanted)

    client = _client(config)
    added = 0
    skipped_done = 0
    skipped_exhausted = 0
    found = 0
    found_set: set[int] = set()

    for source_path in sorted(config.storage.origin_dir.glob("*.txt")):
        for chapter_num, chapter_text in load_source_chapters(config, source_path):
            try:
                chap = int(str(chapter_num))
            except Exception:
                continue
            if chap not in wanted_set:
                continue
            found += 1
            found_set.add(chap)
            job_id = _job_id(config.novel_id, source_path.name, str(chap))
            if client.hexists(_inflight_key(config), job_id):
                continue
            if not force:
                if _has_exhausted_retries(config, client, job_id):
                    skipped_exhausted += 1
                    continue
                if not _chapter_needs_work(config, source_path, str(chap), chapter_text=chapter_text):
                    skipped_done += 1
                    continue

            if force:
                client.hset(_force_key(config), job_id, str(int(time.time())))
                client.hdel(_novel_key(config, config.novel_id, "retries"), job_id)

            if client.sadd(_queued_key(config), job_id):
                client.rpush(_pending_key(config), job_id)
                added += 1

    missing = len(wanted_set) - found
    LOGGER.info(
        "Queue add chapters | novel=%s chapters=%s force=%s added=%s found=%s missing=%s skipped_done=%s skipped_exhausted=%s",
        config.novel_id,
        len(wanted_set),
        force,
        added,
        found,
        missing,
        skipped_done,
        skipped_exhausted,
    )
    print(
        f"Queued {added} job(s) for novel {config.novel_id} chapters={len(wanted_set)}"
        f"{' (force)' if force else ''}. Found={found}, missing={missing}, skipped_done={skipped_done}, exhausted_retries={skipped_exhausted}."
    )
    if missing:
        missing_chaps = [ch for ch in wanted if ch not in found_set]
        if missing_chaps:
            preview = ", ".join(str(ch) for ch in missing_chaps[:50])
            suffix = " ..." if len(missing_chaps) > 50 else ""
            print(f"Missing chapters not found in origin: {preview}{suffix}")
    return 0


def add_all_jobs_to_queue(config: NovelConfig, *, force: bool = False) -> int:
    """Enqueue all chapter jobs that still need work across all origin batches."""
    client = _client(config)
    added = 0
    skipped_done = 0
    skipped_exhausted = 0
    total_candidates = 0

    for source_path in sorted(config.storage.origin_dir.glob("*.txt")):
        for chapter_num, chapter_text in load_source_chapters(config, source_path):
            try:
                chap_str = str(int(str(chapter_num)))
            except Exception:
                continue
            total_candidates += 1
            job_id = _job_id(config.novel_id, source_path.name, chap_str)
            if client.hexists(_inflight_key(config), job_id):
                continue
            if not force:
                if _has_exhausted_retries(config, client, job_id):
                    skipped_exhausted += 1
                    continue
                if not _chapter_needs_work(config, source_path, chap_str, chapter_text=chapter_text):
                    skipped_done += 1
                    continue

            if force:
                client.hset(_force_key(config), job_id, str(int(time.time())))
                client.hdel(_novel_key(config, config.novel_id, "retries"), job_id)

            if client.sadd(_queued_key(config), job_id):
                client.rpush(_pending_key(config), job_id)
                added += 1

    LOGGER.info(
        "Queue add --all | novel=%s force=%s candidates=%s added=%s skipped_done=%s skipped_exhausted=%s",
        config.novel_id,
        force,
        total_candidates,
        added,
        skipped_done,
        skipped_exhausted,
    )
    print(
        f"Queued {added} job(s) for novel {config.novel_id} (all chapters){' (force)' if force else ''}. "
        f"Skipped already-done={skipped_done}, exhausted-retries={skipped_exhausted}."
    )
    return 0


def requeue_untranslated_exhausted_jobs(config: NovelConfig) -> int:
    """
    Requeue only chapter jobs that still need work but were skipped by `queue add --all`
    because their retry budget was exhausted.

    This intentionally does not requeue chapters that are already up to date, but it *does*
    clean stale retry entries for those chapters so queue status reflects actual remaining work.
    """
    client = _client(config)
    job_ids: list[str] = []
    stale_retry_job_ids: list[str] = []

    for source_path in sorted(config.storage.origin_dir.glob("*.txt")):
        for chapter_num, chapter_text in load_source_chapters(config, source_path):
            try:
                chap_str = str(int(str(chapter_num)))
            except Exception:
                continue
            job_id = _job_id(config.novel_id, source_path.name, chap_str)
            if not _has_exhausted_retries(config, client, job_id):
                continue
            if not _chapter_needs_work(config, source_path, chap_str, chapter_text=chapter_text):
                stale_retry_job_ids.append(job_id)
                continue
            job_ids.append(job_id)

    if stale_retry_job_ids:
        client.hdel(_novel_key(config, config.novel_id, "retries"), *stale_retry_job_ids)
        LOGGER.info(
            "Cleaned stale exhausted retries | novel=%s removed=%s",
            config.novel_id,
            len(stale_retry_job_ids),
        )

    if not job_ids:
        if stale_retry_job_ids:
            print(
                "Queued 0 job(s). "
                f"Cleaned {len(stale_retry_job_ids)} stale retry record(s) for already-translated chapters."
            )
        else:
            print("Queued 0 job(s).")
        return 0

    rc = add_job_ids_to_queue(
        config,
        job_ids,
        force=True,
        label="requeue untranslated exhausted",
    )
    if stale_retry_job_ids:
        print(f"Cleaned {len(stale_retry_job_ids)} stale retry record(s) for already-translated chapters.")
    return rc


def add_job_ids_to_queue(
    config: NovelConfig,
    job_ids: list[str],
    *,
    force: bool = True,
    label: str = "queue add",
) -> int:
    """Enqueue an explicit list of job IDs (file::chapter)."""
    if not job_ids:
        print("Queued 0 job(s).")
        return 0

    client = _client(config)
    added = 0
    skipped_inflight = 0
    skipped_exhausted = 0
    missing_origin = 0

    for job_id in job_ids:
        if _is_captions_job(job_id):
            if client.hexists(_inflight_key(config), job_id):
                skipped_inflight += 1
                continue
            if not force:
                if _has_exhausted_retries(config, client, job_id):
                    skipped_exhausted += 1
                    continue
                if not _captions_needs_translation(config):
                    continue
            if client.sadd(_queued_key(config), job_id):
                client.rpush(_pending_key(config), job_id)
                added += 1
            continue
        try:
            _nid, file_name, chapter_num = _parse_job_id(job_id)
        except Exception:
            LOGGER.warning("Skipping invalid job_id: %r", job_id)
            continue
        source_path = config.storage.origin_dir / file_name
        if not source_path.exists():
            missing_origin += 1
            continue

        if client.hexists(_inflight_key(config), job_id):
            skipped_inflight += 1
            continue

        if not force:
            if _has_exhausted_retries(config, client, job_id):
                skipped_exhausted += 1
                continue
            if not _chapter_needs_work(config, source_path, chapter_num):
                continue

        if force:
            client.hset(_force_key(config), job_id, str(int(time.time())))
            client.hdel(_novel_key(config, config.novel_id, "retries"), job_id)

        if client.sadd(_queued_key(config), job_id):
            client.rpush(_pending_key(config), job_id)
            added += 1

    LOGGER.info(
        "%s | novel=%s job_ids=%s force=%s added=%s skipped_inflight=%s skipped_exhausted=%s missing_origin=%s",
        label,
        config.novel_id,
        len(job_ids),
        force,
        added,
        skipped_inflight,
        skipped_exhausted,
        missing_origin,
    )
    print(
        f"Queued {added} job(s) for novel {config.novel_id} ({label}). "
        f"Skipped inflight={skipped_inflight}, exhausted={skipped_exhausted}, missing_origin={missing_origin}."
    )
    return 0


def remove_jobs_from_queue(
    config: NovelConfig,
    *,
    chapters: list[int] | None = None,
    from_chapter: int | None = None,
    to_chapter: int | None = None,
    all_pending: bool = False,
) -> int:
    """Remove pending (non-inflight) jobs from the queue.

    - ``chapters``: remove specific chapter numbers.
    - ``from_chapter``/``to_chapter``: remove a chapter range.
    - ``all_pending``: remove every pending job (flushes all three pending lists + queued set,
      but leaves inflight and done untouched).

    Inflight jobs are skipped — stopping a running worker must be done separately via ``queue stop``.
    """
    client = _client(config)

    if all_pending:
        # Collect queued job_ids for this novel only, excluding inflight work.
        all_queued: list[str] = [
            j.decode() if isinstance(j, bytes) else j
            for j in (client.smembers(_queued_key(config)) or [])
        ]
        inflight_ids: set[str] = {j.decode() if isinstance(j, bytes) else j for j in (client.hkeys(_inflight_key(config)) or [])}
        job_ids = [
            j for j in all_queued
            if _extract_novel_id(j) == config.novel_id and j not in inflight_ids
        ]
    else:
        # Build the target set of job IDs from origin files.
        if chapters is not None:
            wanted: set[int] = {int(ch) for ch in chapters if int(ch) > 0}
        elif from_chapter is not None and to_chapter is not None:
            lo, hi = (from_chapter, to_chapter) if from_chapter <= to_chapter else (to_chapter, from_chapter)
            wanted = set(range(lo, hi + 1))
        else:
            raise ValueError("remove_jobs_from_queue: provide chapters, a range, or all_pending=True")

        job_ids = []
        for source_path in sorted(config.storage.origin_dir.glob("*.txt")):
            for chapter_num, _text in load_source_chapters(config, source_path):
                try:
                    chap = int(str(chapter_num))
                except Exception:
                    continue
                if chap not in wanted:
                    continue
                job_ids.append(_job_id(config.novel_id, source_path.name, str(chap)))

    if not job_ids:
        print(f"No matching pending jobs found for novel {config.novel_id}.")
        return 0

    inflight_ids = {j.decode() if isinstance(j, bytes) else j for j in (client.hkeys(_inflight_key(config)) or [])}
    skipped_inflight = 0
    removed = 0

    pipe = client.pipeline()
    for job_id in job_ids:
        if job_id in inflight_ids:
            skipped_inflight += 1
            continue
        pipe.srem(_queued_key(config), job_id)
        pipe.lrem(_pending_key(config), 0, job_id)
        pipe.lrem(_pending_priority_key(config), 0, job_id)
        pipe.zrem(_pending_delayed_key(config), job_id)
        removed += 1
    pipe.execute()

    LOGGER.info(
        "Queue remove | novel=%s removed=%s skipped_inflight=%s",
        config.novel_id,
        removed,
        skipped_inflight,
    )
    print(
        f"Removed {removed} job(s) from queue for novel {config.novel_id}."
        + (f" Skipped inflight={skipped_inflight} (use 'queue stop' to stop running workers)." if skipped_inflight else "")
    )
    return 0


def drain_novel_from_queue(config: NovelConfig) -> int:
    """Remove all pending (non-inflight) jobs for one novel from the shared queue."""
    return remove_jobs_from_queue(config, all_pending=True)


def _worker_pid_from_worker_id(worker_id: str) -> int | None:
    text = str(worker_id or "").strip()
    if not text:
        return None
    tail = text.rsplit(":", 1)[-1].strip()
    try:
        pid = int(tail)
    except Exception:
        return None
    return pid if pid > 0 else None


def _pid_is_alive(pid: int) -> bool:
    try:
        os.kill(int(pid), 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except Exception:
        return False
    return True


def _requeue_stale_inflight(config: NovelConfig | QueueConfig, client) -> None:
    qcfg = _queue_config(config)
    now = time.time()
    for job_id, payload in client.hgetall(_inflight_key(config)).items():
        meta = json.loads(payload)
        worker_pid = _worker_pid_from_worker_id(meta.get("worker") or "")
        if worker_pid is not None and (not _pid_is_alive(worker_pid)):
            client.hdel(_inflight_key(config), job_id)
            if _has_exhausted_retries(config, client, job_id):
                continue
            _requeue_job_priority(config, client, job_id)
            continue
        started_at = float(meta.get("started_at", 0))
        if now - started_at < qcfg.inflight_ttl_seconds:
            continue
        client.hdel(_inflight_key(config), job_id)
        if _has_exhausted_retries(config, client, job_id):
            continue
        _requeue_job_priority(config, client, job_id)


def _count_origin_files(config: NovelConfig) -> int:
    return sum(1 for _ in config.storage.origin_dir.glob("*.txt"))


def _count_translated_files(config: NovelConfig) -> int:
    return sum(1 for _ in config.storage.translated_dir.glob("*.txt"))


def _count_parts(config: NovelConfig) -> int:
    return sum(1 for _ in config.storage.parts_dir.rglob("*.txt"))


def _count_checkpoints(config: NovelConfig) -> int:
    return sum(1 for _ in config.storage.progress_dir.glob("*.json"))


def _total_chapters(config: NovelConfig) -> int:
    total = 0
    chapter_regex = re.compile(config.translation.chapter_regex, flags=re.M)
    for path in config.storage.origin_dir.glob("*.txt"):
        total += len(chapter_regex.findall(path.read_text(encoding="utf-8")))
    return total


def _status_paths(config: NovelConfig | QueueConfig) -> tuple[Path, Path]:
    if isinstance(config, QueueConfig):
        log_dir = _shared_queue_log_dir()
        return log_dir / "status.log", log_dir / "status.state.json"
    return (
        get_novel_log_path(config.storage.logs_dir, config.novel_id, "queue/status.log"),
        get_novel_log_path(config.storage.logs_dir, config.novel_id, "queue/status.state.json"),
    )


def _decode_done_payload(value: str) -> dict[str, str]:
    try:
        payload = json.loads(value)
        if isinstance(payload, dict):
            return payload
    except Exception:
        pass
    return {"finished_at": value}


def _parse_finished_at(payload: dict[str, object]) -> float | None:
    value = payload.get("finished_at")
    if value is None:
        return None
    try:
        return float(value)  # stored as time.time() or a numeric-ish string
    except (TypeError, ValueError):
        return None


def _write_status_line(
    config: NovelConfig | QueueConfig,
    client,
    last_snapshot: dict[str, int] | None,
    *,
    append_log: bool = True,
) -> dict[str, int]:
    inflight_payloads = client.hgetall(_inflight_key(config))
    # Aggregate done/model_done/model_failed across all novels via SCAN.
    done_payloads: dict = {}
    model_done: dict = {}
    model_failed: dict = {}
    prefix = _prefix(config)
    novel_key_pattern = f"{prefix}:novel:*"
    cursor = "0"
    novel_done_keys: list[str] = []
    novel_model_done_keys: list[str] = []
    novel_model_failed_keys: list[str] = []
    while True:
        cursor, keys = client.scan(cursor=cursor, match=novel_key_pattern, count=200)
        for k in keys:
            k_str = k if isinstance(k, str) else k.decode("utf-8")
            if k_str.endswith(":done"):
                novel_done_keys.append(k_str)
            elif k_str.endswith(":model_done"):
                novel_model_done_keys.append(k_str)
            elif k_str.endswith(":model_failed"):
                novel_model_failed_keys.append(k_str)
        if cursor == 0 or cursor == "0" or cursor == b"0":
            break
    for dk in novel_done_keys:
        done_payloads.update(client.hgetall(dk))
    for mdk in novel_model_done_keys:
        for m, c in client.hgetall(mdk).items():
            model_done[m] = str(int(model_done.get(m, "0")) + int(c))
    for mfk in novel_model_failed_keys:
        for m, c in client.hgetall(mfk).items():
            model_failed[m] = str(int(model_failed.get(m, "0")) + int(c))
    inflight_by_model: dict[str, int] = {}
    for payload in inflight_payloads.values():
        try:
            model = json.loads(payload).get("model", "unknown")
        except Exception:
            model = "unknown"
        inflight_by_model[model] = inflight_by_model.get(model, 0) + 1
    status_log, state_log = _status_paths(config)
    pending_priority = int(client.llen(_pending_priority_key(config)) or 0)
    pending_normal = int(client.llen(_pending_key(config)) or 0)
    pending_delayed = int(client.zcard(_pending_delayed_key(config)) or 0)
    # Per-novel file counts are only available when config is NovelConfig.
    if isinstance(config, QueueConfig):
        origin_files = translated_files = parts = checkpoints = chapter_total = 0
    else:
        origin_files = _count_origin_files(config)
        translated_files = _count_translated_files(config)
        parts = _count_parts(config)
        checkpoints = _count_checkpoints(config)
        chapter_total = _total_chapters(config)
    snapshot = {
        "ts": int(time.time()),
        "origin_files": origin_files,
        "translated_files": translated_files,
        "parts": parts,
        "checkpoints": checkpoints,
        "chapter_total": chapter_total,
        "pending": pending_priority + pending_normal,
        "pending_priority": pending_priority,
        "pending_normal": pending_normal,
        "pending_delayed": pending_delayed,
        "queued": client.scard(_queued_key(config)),
        "inflight": len(inflight_payloads),
        "retries": sum(int(client.hlen(k) or 0) for k in _scan_novel_keys(client, prefix, "retries")),
        "exhausted": 0,  # TODO: aggregate across novels
        "done": len(done_payloads),
        "inflight_by_model": inflight_by_model,
        "done_by_model": {model: int(count) for model, count in model_done.items()},
        "failed_by_model": {model: int(count) for model, count in model_failed.items()},
    }
    files_per_min = 0.0
    parts_per_min = 0.0
    eta_files = "unknown"
    eta_parts = "unknown"
    eta_queue = "unknown"
    if last_snapshot:
        delta_s = snapshot["ts"] - last_snapshot["ts"]
        if delta_s > 0:
            # For both normal and force re-translate, the file/part *counts* may not change.
            # Instead, infer throughput from "done" jobs in the time window.
            # Each job corresponds to one chapter part; file throughput is measured as distinct batch files touched.
            window_start = float(last_snapshot["ts"])
            completed_jobs = 0
            touched_files: set[str] = set()
            for raw in done_payloads.values():
                payload = _decode_done_payload(raw)
                finished_at = _parse_finished_at(payload) or 0.0
                if finished_at < window_start:
                    continue
                completed_jobs += 1
                file_name = str(payload.get("file_name") or "").strip()
                if file_name:
                    touched_files.add(file_name)

            parts_per_min = completed_jobs * 60.0 / delta_s
            files_per_min = len(touched_files) * 60.0 / delta_s
            if files_per_min > 0 and snapshot["origin_files"] > snapshot["translated_files"]:
                minutes = (snapshot["origin_files"] - snapshot["translated_files"]) / files_per_min
                eta_files = datetime.fromtimestamp(time.time() + minutes * 60).strftime("%Y-%m-%d %H:%M:%S")
            if parts_per_min > 0 and snapshot["chapter_total"] > snapshot["parts"]:
                minutes = (snapshot["chapter_total"] - snapshot["parts"]) / parts_per_min
                eta_parts = datetime.fromtimestamp(time.time() + minutes * 60).strftime("%Y-%m-%d %H:%M:%S")
            queue_remaining = snapshot["pending"] + snapshot["queued"] + snapshot["inflight"]
            if parts_per_min > 0 and queue_remaining > 0:
                minutes = queue_remaining / parts_per_min
                eta_queue = datetime.fromtimestamp(time.time() + minutes * 60).strftime("%Y-%m-%d %H:%M:%S")
    done_pct = (snapshot["translated_files"] / snapshot["origin_files"] * 100) if snapshot["origin_files"] else 0.0
    part_pct = (snapshot["parts"] / snapshot["chapter_total"] * 100) if snapshot["chapter_total"] else 0.0
    line = (
        f"translated={snapshot['translated_files']}/{snapshot['origin_files']} "
        f"| done={done_pct:.2f}% | parts={snapshot['parts']}/{snapshot['chapter_total']} "
        f"| part_done={part_pct:.2f}% | files/min={files_per_min:.2f} | parts/min={parts_per_min:.2f} "
        f"| ETA_files={eta_files} | ETA_parts={eta_parts} | ETA_queue={eta_queue} | checkpoints={snapshot['checkpoints']} "
        f"| retries={snapshot['retries']} | exhausted={snapshot['exhausted']} | pending={snapshot['pending']} | queued={snapshot['queued']} "
        f"| inflight={snapshot['inflight']} | workers={snapshot['inflight']} "
        f"| inflight_by_model={snapshot['inflight_by_model']} "
        f"| done_by_model={snapshot['done_by_model']} | failed_by_model={snapshot['failed_by_model']}"
    )
    state_log.parent.mkdir(parents=True, exist_ok=True)
    if append_log:
        with status_log.open("a", encoding="utf-8") as fh:
            fh.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | {line}\n")
    state_log.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
    return snapshot


def _daily_quota_key(config: NovelConfig | QueueConfig, key_index: int, model: str) -> str:
    key_prefix = _worker_key_prefix_for_index(config, key_index=int(key_index))
    return f"{key_prefix}:{model}:quota:daily_reqs"


def _minute_quota_key(config: NovelConfig | QueueConfig, key_index: int, model: str) -> str:
    key_prefix = _worker_key_prefix_for_index(config, key_index=int(key_index))
    return f"{key_prefix}:{model}:quota:reqs"


def _minute_token_key(config: NovelConfig | QueueConfig, key_index: int, model: str) -> str:
    key_prefix = _worker_key_prefix_for_index(config, key_index=int(key_index))
    return f"{key_prefix}:{model}:quota:tokens"


def _estimate_tokens_from_chars(char_count: int) -> int:
    """
    Rough TPM estimate for "should worker pause" checks.

    This is intentionally lightweight (no tokenization call); it should broadly align with the provider-side
    estimator in `novel_tts.translate.providers` but only has access to chunk character count here.
    """

    chars = max(0, int(char_count))
    if chars <= 0:
        return 1

    chars_per_token_raw = os.environ.get("NOVEL_TTS_GEMINI_TPM_CHARS_PER_TOKEN", "").strip()
    try:
        chars_per_token = float(chars_per_token_raw) if chars_per_token_raw else 4.0
    except ValueError:
        chars_per_token = 4.0
    chars_per_token = max(0.8, chars_per_token)

    input_tokens = max(1, int(math.ceil(chars / chars_per_token)))

    output_ratio_raw = os.environ.get("NOVEL_TTS_GEMINI_TPM_OUTPUT_RESERVE_RATIO", "").strip()
    min_out_raw = os.environ.get("NOVEL_TTS_GEMINI_TPM_OUTPUT_RESERVE_MIN", "").strip()
    try:
        output_ratio = float(output_ratio_raw) if output_ratio_raw else 0.0
    except ValueError:
        output_ratio = 0.0
    try:
        min_out = int(min_out_raw) if min_out_raw else 0
    except ValueError:
        min_out = 0

    output_reserve = 0
    if output_ratio > 0:
        output_reserve = max(min_out, int(math.ceil(input_tokens * output_ratio)))

    multiplier_raw = os.environ.get("NOVEL_TTS_GEMINI_TPM_SAFETY_MULTIPLIER", "").strip()
    try:
        multiplier = float(multiplier_raw) if multiplier_raw else 1.10
    except ValueError:
        multiplier = 1.10
    multiplier = max(1.0, multiplier)

    return max(1, int(math.ceil((input_tokens + output_reserve) * multiplier)))


def _estimated_request_tokens_for_model(config: NovelConfig | QueueConfig, model: str) -> int:
    model_cfg = _queue_config(config).model_configs.get(model)
    chunk_max_len = model_cfg.chunk_max_len if model_cfg and model_cfg.chunk_max_len > 0 else 0
    if chunk_max_len <= 0:
        # Model configs are canonical; keep a conservative fallback for telemetry/estimates only.
        chunk_max_len = 800
    return _estimate_tokens_from_chars(chunk_max_len)


def _model_rpd_exhausted(config: NovelConfig | QueueConfig, client, key_index: int, model: str) -> bool:
    return _model_rpd_wait_seconds(config, client, key_index, model) > 0.0


def _model_rpd_wait_seconds(config: NovelConfig | QueueConfig, client, key_index: int, model: str) -> float:
    model_cfg = _queue_config(config).model_configs.get(model)
    if model_cfg is None or model_cfg.rpd_limit <= 0:
        return 0.0
    now = time.time()
    day_window_start = now - 86400.0
    rpd_limit = int(model_cfg.rpd_limit)
    waits: list[float] = []

    try:
        daily_key = _daily_quota_key(config, key_index, model)
        client.zremrangebyscore(daily_key, 0, day_window_start)
        daily_count = int(client.zcount(daily_key, day_window_start, "+inf") or 0)
        if daily_count >= rpd_limit:
            need_drop = daily_count - (rpd_limit - 1)
            scored = client.zrangebyscore(
                daily_key,
                day_window_start,
                "+inf",
                start=max(0, need_drop - 1),
                num=1,
                withscores=True,
            )
            if scored:
                waits.append(max(1.0, (float(scored[0][1]) + 86400.0) - now + 0.05))
            else:
                waits.append(60.0)
    except Exception:
        pass

    try:
        key_prefix = _worker_key_prefix_for_index(config, key_index=int(key_index))
        freezed_key = quota_keys.rpd_freezed_key(key_prefix=key_prefix, model=model)
        locked_key = quota_keys.rpd_locked_key(key_prefix=key_prefix, model=model)
        client.zremrangebyscore(freezed_key, 0, day_window_start)
        client.zremrangebyscore(locked_key, 0, day_window_start)
        freezed = client.zrangebyscore(freezed_key, day_window_start, "+inf", withscores=True) or []
        locked = client.zrangebyscore(locked_key, day_window_start, "+inf", withscores=True) or []
        combined = sorted(
            [float(score) for _member, score in list(freezed) + list(locked)],
        )
        if len(combined) >= rpd_limit:
            need_drop = len(combined) - (rpd_limit - 1)
            idx = max(0, min(len(combined) - 1, need_drop - 1))
            waits.append(max(1.0, (combined[idx] + 86400.0) - now + 0.05))
    except Exception:
        pass

    return max(waits) if waits else 0.0


def _normalize_quota_wait_seconds(
    config: NovelConfig | QueueConfig,
    client,
    key_index: int,
    model: str,
    *,
    proposed_wait_seconds: float,
    text: str = "",
) -> tuple[float, bool]:
    wait_seconds = max(0.0, float(proposed_wait_seconds or 0.0))
    if wait_seconds <= 0:
        return 0.0, False
    rpd_wait = _model_rpd_wait_seconds(config, client, key_index, model)
    if rpd_wait > 0:
        return max(wait_seconds, rpd_wait), True
    reasons = _parse_quota_reason_tokens(text)
    if "RPD" in reasons and wait_seconds >= 3600.0:
        return wait_seconds, True
    return min(wait_seconds, 60.0), False


def _model_short_quota_wait_seconds(config: NovelConfig | QueueConfig, client, key_index: int, model: str) -> float:
    model_cfg = _queue_config(config).model_configs.get(model)
    if model_cfg is None:
        return 0.0
    rpm_limit = max(0, int(model_cfg.rpm_limit))
    tpm_limit = max(0, int(model_cfg.tpm_limit))
    if rpm_limit <= 0 and tpm_limit <= 0:
        return 0.0

    now = time.time()
    window_start = now - 60.0
    req_key = _minute_quota_key(config, key_index, model)
    token_key = _minute_token_key(config, key_index, model)
    stale_members = client.zrangebyscore(req_key, 0, window_start)
    if stale_members:
        client.zrem(req_key, *stale_members)
        client.hdel(token_key, *stale_members)
    active_members = client.zrangebyscore(req_key, window_start, "+inf", withscores=True)
    token_map = client.hgetall(token_key)
    current_requests = len(active_members)
    current_tokens = 0
    for member, _score in active_members:
        try:
            current_tokens += int(token_map.get(member, "0"))
        except (TypeError, ValueError):
            continue

    estimated_tokens = _estimated_request_tokens_for_model(config, model)
    wait_rpm = 0.0
    if rpm_limit > 0 and current_requests >= rpm_limit and active_members:
        need_drop = current_requests - (rpm_limit - 1)
        idx = min(len(active_members) - 1, max(0, need_drop - 1))
        cutoff_score = float(active_members[idx][1])
        wait_rpm = max(0.25, (cutoff_score + 60.0) - now + 0.05)
    wait_tpm = 0.0
    if tpm_limit > 0 and (current_tokens + estimated_tokens) > tpm_limit and active_members:
        if estimated_tokens > tpm_limit:
            return 60.0
        need_reduce = (current_tokens + estimated_tokens) - tpm_limit
        reduced = 0
        cutoff_score: float | None = None
        for member, score in active_members:
            try:
                reduced += int(token_map.get(member, "0"))
            except (TypeError, ValueError):
                continue
            if reduced >= need_reduce:
                cutoff_score = float(score)
                break
        if cutoff_score is None:
            cutoff_score = float(active_members[0][1])
        wait_tpm = max(0.25, (cutoff_score + 60.0) - now + 0.05)
    return max(wait_rpm, wait_tpm, 0.0)


def _quota_wait_seconds_for_request(config: NovelConfig | QueueConfig, client, key_index: int, model: str, *, estimated_tokens: int) -> float:
    """
    Compute remaining wait time for a specific request size.

    This is used by workers when they decide to hold a job and poll until the quota gate opens,
    so they don't keep releasing/requeuing or sleeping the whole suggested_wait.
    """

    qcfg = _queue_config(config)
    model_cfg = qcfg.model_configs.get(model)
    if model_cfg is None:
        return 0.0
    rpm_limit = max(0, int(model_cfg.rpm_limit))
    tpm_limit = max(0, int(model_cfg.tpm_limit))
    rpd_limit = max(0, int(model_cfg.rpd_limit))

    if rpm_limit <= 0 and tpm_limit <= 0 and rpd_limit <= 0:
        return 0.0

    now = time.time()
    window_start = now - 60.0
    req_key = _minute_quota_key(config, key_index, model)
    token_key = _minute_token_key(config, key_index, model)
    stale_members = client.zrangebyscore(req_key, 0, window_start)
    if stale_members:
        client.zrem(req_key, *stale_members)
        client.hdel(token_key, *stale_members)
    active_members = client.zrangebyscore(req_key, window_start, "+inf", withscores=True)
    token_map = client.hgetall(token_key)

    current_requests = len(active_members)
    current_tokens = 0
    for member, _score in active_members:
        try:
            current_tokens += int(token_map.get(member, "0"))
        except (TypeError, ValueError):
            continue

    wait_rpm = 0.0
    if rpm_limit > 0 and current_requests >= rpm_limit and active_members:
        need_drop = current_requests - (rpm_limit - 1)
        idx = min(len(active_members) - 1, max(0, need_drop - 1))
        cutoff_score = float(active_members[idx][1])
        wait_rpm = max(0.05, (cutoff_score + 60.0) - now + 0.05)

    wait_tpm = 0.0
    if tpm_limit > 0 and (current_tokens + estimated_tokens) > tpm_limit and active_members:
        if estimated_tokens > tpm_limit:
            wait_tpm = 60.0
        else:
            need_reduce = (current_tokens + estimated_tokens) - tpm_limit
            reduced = 0
            cutoff_score: float | None = None
            for member, score in active_members:
                try:
                    reduced += int(token_map.get(member, "0"))
                except (TypeError, ValueError):
                    continue
                if reduced >= need_reduce:
                    cutoff_score = float(score)
                    break
            if cutoff_score is None:
                cutoff_score = float(active_members[0][1])
            wait_tpm = max(0.05, (cutoff_score + 60.0) - now + 0.05)

    wait_rpd = 0.0
    if rpd_limit > 0:
        day_window_start = now - 86400.0
        daily_key = _daily_quota_key(config, key_index, model)
        client.zremrangebyscore(daily_key, 0, day_window_start)
        daily_count = int(client.zcount(daily_key, day_window_start, "+inf") or 0)
        if daily_count >= rpd_limit:
            need_drop = daily_count - (rpd_limit - 1)
            scored = client.zrangebyscore(
                daily_key,
                day_window_start,
                "+inf",
                start=max(0, need_drop - 1),
                num=1,
                withscores=True,
            )
            if scored:
                wait_rpd = max(1.0, (float(scored[0][1]) + 86400.0) - now + 0.05)
            else:
                wait_rpd = 60.0

    return max(wait_rpm, wait_tpm, wait_rpd, 0.0)


def _worker_should_pause_for_quota(config: NovelConfig | QueueConfig, client, key_index: int, model: str) -> tuple[bool, str, float]:
    rpd_wait = _model_rpd_wait_seconds(config, client, key_index, model)
    if rpd_wait > 0:
        return True, model, rpd_wait
    short_wait = _model_short_quota_wait_seconds(config, client, key_index, model)
    if short_wait > 0:
        return True, model, short_wait
    model_cfg = _queue_config(config).model_configs.get(model)
    repair_model = model_cfg.repair_model if model_cfg else ""
    if repair_model:
        repair_rpd_wait = _model_rpd_wait_seconds(config, client, key_index, repair_model)
        if repair_rpd_wait > 0:
            return True, repair_model, repair_rpd_wait
    if repair_model:
        repair_wait = _model_short_quota_wait_seconds(config, client, key_index, repair_model)
        if repair_wait > 0:
            return True, repair_model, repair_wait
    return False, "", 0.0


_novel_config_cache: dict[str, tuple[float, NovelConfig]] = {}
_NOVEL_CONFIG_CACHE_TTL = 60.0


def _cached_novel_config(novel_id: str) -> NovelConfig:
    """Load NovelConfig with LRU-style caching (60s TTL)."""
    from novel_tts.config.loader import load_novel_config

    now = time.time()
    entry = _novel_config_cache.get(novel_id)
    if entry and (now - entry[0]) < _NOVEL_CONFIG_CACHE_TTL:
        return entry[1]
    config = load_novel_config(novel_id)
    _novel_config_cache[novel_id] = (now, config)
    return config


def run_worker(queue_config: QueueConfig, key_index: int, model: str, *, proxy_cfg=None) -> int:
    keys = _load_keys(queue_config)
    if key_index < 1 or key_index > len(keys):
        raise ValueError(f"Invalid key index: {key_index}")
    api_key = keys[key_index - 1]
    client = _client(queue_config)
    if bool(proxy_cfg and getattr(proxy_cfg, "enabled", False)):
        proxy_name = None
        reason = ""
        if key_index == 1:
            LOGGER.info(
                "Worker proxy disabled for k1; using direct | key_index=%s model=%s",
                key_index,
                model,
            )
        else:
            proxies = []
            if bool(getattr(proxy_cfg, "auto_discovery", True)):
                try:
                    healthy, reason = proxy_gateway_mod.load_healthy_proxy_names_from_redis(
                        cfg=proxy_cfg,
                        redis_cfg=queue_config.redis,
                        now=time.time(),
                        cache_ttl_seconds=0.0,
                    )
                    proxies = list(healthy or [])
                except Exception:
                    proxies = []
            else:
                proxies = list(getattr(proxy_cfg, "proxies", None) or [])
            if proxies:
                proxy_name = proxy_gateway_mod.select_proxy_for_key_index(
                    key_index=key_index,
                    proxies=proxies,
                    keys_per_proxy=int(getattr(proxy_cfg, "keys_per_proxy", 3) or 3),
                )
            if proxy_name:
                LOGGER.info(
                    "Worker proxy selected | key_index=%s model=%s proxy=%s",
                    key_index,
                    model,
                    proxy_name,
                )
            else:
                LOGGER.info(
                    "Worker proxy unavailable; using direct | key_index=%s model=%s reason=%s",
                    key_index,
                    model,
                    reason or "-",
                )
    config = queue_config  # alias for compatibility with downstream code
    worker_id = f"k{key_index}:{model}:{os.getpid()}"
    consecutive_rate_limit_releases = 0
    cooldown_key = _rate_limit_cooldown_key(config, key_index=key_index, model=model)
    out_of_quota_key = _out_of_quota_cooldown_key(config, key_index=key_index, model=model)
    max_429_cooldown_seconds = 65.0

    def _stop_requested() -> bool:
        return _is_stopping(config, client)

    while True:
        # Fast-exit path: if we are in a cooldown/sleep and the stop signal
        # arrives, there is no in-flight job — exit immediately.
        if _is_stopping(config, client):
            LOGGER.info(
                "Worker exiting (graceful stop, idle) key_index=%s model=%s worker=%s",
                key_index,
                model,
                worker_id,
            )
            return 0
        # Suspected IP-level throttling: pause all workers for this model (per novel), with spaced probing.
        if _ip_ban_is_active(client, config, model=model):
            _ip_ban_probe_if_due(client, config, key_index=key_index, model=model, api_key=api_key)
            if _ip_ban_is_active(client, config, model=model):
                next_in = _ip_ban_next_probe_in_seconds(client, config, model=model)
                state = _get_ip_ban_state(client, config, model=model)
                try:
                    next_probe_at = float(state.get("next_probe_at") or 0.0)
                except Exception:
                    next_probe_at = 0.0
                until = next_probe_at if next_probe_at > time.time() else (time.time() + max(1.0, next_in))
                _sync_cooldown_until(client, cooldown_key, until=until)
                sleep_seconds = max(0.5, min(max(1.0, next_in), 10.0))
                LOGGER.warning(
                    "Worker cooling down due to suspected IP ban (429) key_index=%s model=%s sleeping for %.2fs",
                    key_index,
                    model,
                    sleep_seconds,
                )
                interrupted = _interruptible_sleep(
                    max_seconds=sleep_seconds,
                    check_remaining_seconds=lambda: _ip_ban_next_probe_in_seconds(client, config, model=model),
                    step_seconds=2.0,
                    min_sleep_seconds=0.5,
                    should_stop=_stop_requested,
                )
                if interrupted:
                    LOGGER.info(
                        "Worker exiting during IP-ban cooldown (graceful stop) key_index=%s model=%s worker=%s",
                        key_index,
                        model,
                        worker_id,
                    )
                    return 0
                continue

        out_remaining = _get_rate_limit_cooldown_remaining_seconds(client, out_of_quota_key)
        if out_remaining > 0.05:
            sleep_seconds = max(1.0, out_remaining)
            LOGGER.warning(
                "Worker cooling down (out-of-quota) key_index=%s model=%s sleeping for %.2fs",
                key_index,
                model,
                sleep_seconds,
            )
            interrupted = _interruptible_sleep(
                max_seconds=sleep_seconds,
                check_remaining_seconds=lambda: _get_rate_limit_cooldown_remaining_seconds(client, out_of_quota_key),
                step_seconds=2.0,
                min_sleep_seconds=0.5,
                should_stop=_stop_requested,
            )
            if interrupted:
                LOGGER.info(
                    "Worker exiting during out-of-quota cooldown (graceful stop) key_index=%s model=%s worker=%s",
                    key_index,
                    model,
                    worker_id,
                )
                return 0
            continue
        remaining = _get_rate_limit_cooldown_remaining_seconds(client, cooldown_key)
        if remaining > 0.05:
            sleep_seconds = max(0.25, remaining)
            LOGGER.warning(
                "Worker cooling down due to rate limit (429) key_index=%s model=%s sleeping for %.2fs",
                key_index,
                model,
                sleep_seconds,
            )
            interrupted = _interruptible_sleep(
                max_seconds=sleep_seconds,
                check_remaining_seconds=lambda: _get_rate_limit_cooldown_remaining_seconds(client, cooldown_key),
                step_seconds=1.0,
                min_sleep_seconds=0.25,
                should_stop=_stop_requested,
            )
            if interrupted:
                LOGGER.info(
                    "Worker exiting during rate-limit cooldown (graceful stop) key_index=%s model=%s worker=%s",
                    key_index,
                    model,
                    worker_id,
                )
                return 0
            continue
        should_pause, blocked_model, wait_seconds = _worker_should_pause_for_quota(config, client, key_index, model)
        if should_pause:
            LOGGER.warning(
                "Worker paused because model quota is exhausted key_index=%s model=%s blocked_model=%s wait_seconds=%.2f",
                key_index,
                model,
                blocked_model,
                wait_seconds,
            )
            planned = max(1.0, float(wait_seconds or 0.0))

            def _quota_remaining() -> float:
                pause, _blocked, seconds = _worker_should_pause_for_quota(config, client, key_index, model)
                return max(0.0, float(seconds or 0.0)) if pause else 0.0

            interrupted = _interruptible_sleep(
                max_seconds=planned,
                check_remaining_seconds=_quota_remaining,
                step_seconds=1.0,
                min_sleep_seconds=0.5,
                should_stop=_stop_requested,
            )
            if interrupted:
                LOGGER.info(
                    "Worker exiting during quota pause (graceful stop) key_index=%s model=%s worker=%s",
                    key_index,
                    model,
                    worker_id,
                )
                return 0
            continue
        # Graceful shutdown: stop picking new jobs and exit cleanly.
        if _is_stopping(config, client):
            LOGGER.info(
                "Worker exiting (graceful stop) key_index=%s model=%s worker=%s",
                key_index,
                model,
                worker_id,
            )
            return 0
        job_id = _throttled_pick_job_id(config, client, key_index=key_index, model=model, timeout_seconds=5.0)
        if not job_id:
            continue
        # If an IP-ban is suspected, do not start new translate subprocesses.
        # Push the job back and let all workers cool down together.
        if _ip_ban_is_active(client, config, model=model):
            client.lpush(_pending_priority_key(config), job_id)
            time.sleep(0.5)
            continue
        # After an IP-ban clears, ramp up slowly to avoid bursting into another ban.
        if _ip_recover_is_active(client, config, model=model) and not _ip_recover_try_admit(client, config, model=model):
            client.lpush(_pending_priority_key(config), job_id)
            _sync_cooldown_until(client, cooldown_key, until=time.time() + 1.0)
            time.sleep(0.25)
            continue
        client.srem(_queued_key(config), job_id)
        # Extract novel_id from job and load the novel-specific config.
        novel_id = _extract_novel_id(job_id)
        try:
            novel_config = _cached_novel_config(novel_id)
        except Exception:
            LOGGER.error("Failed to load config for novel %s (job %s), requeueing", novel_id, job_id)
            _requeue_job_priority(config, client, job_id)
            continue
        is_captions = _is_captions_job(job_id)
        is_repair_glossary = _is_repair_glossary_job(job_id)
        is_force = bool(client.hexists(_force_key(config), job_id)) if not (is_captions or is_repair_glossary) else False
        file_name = ""
        chapter_num = ""
        chunk_index = -1
        source_path: Path | None = None
        if is_captions:
            if not _captions_needs_translation(novel_config):
                continue
        elif is_repair_glossary:
            chunk_index = _parse_repair_glossary_chunk_index(job_id)
            if not _repair_glossary_chunk_needs_work(novel_config, chunk_index):
                continue
        else:
            _nid, file_name, chapter_num = _parse_job_id(job_id)
            source_path = novel_config.storage.origin_dir / file_name
            if not source_path.exists() or ((not is_force) and (not _chapter_needs_work(novel_config, source_path, chapter_num))):
                continue
        if is_repair_glossary:
            job_type = "repair-glossary"
        elif is_captions:
            job_type = "captions"
        else:
            job_type = "chapter"
        client.hset(
            _inflight_key(config),
            job_id,
            json.dumps(
                {
                    "worker": worker_id,
                    "novel_id": novel_id,
                    "started_at": time.time(),
                    "model": model,
                    "job_type": job_type,
                    "file_name": file_name,
                    "chapter_num": chapter_num,
                    "chunk_index": chunk_index if is_repair_glossary else -1,
                }
            ),
        )
        env = os.environ.copy()
        env["GEMINI_API_KEY"] = api_key
        env["GEMINI_MODEL"] = model
        env["NOVEL_TTS_NOVEL_ID"] = novel_id
        env["NOVEL_TTS_KEY_INDEX"] = str(int(key_index))
        env["NOVEL_TTS_KEY_COUNT"] = str(int(len(keys)))
        env["NOVEL_TTS_QUOTA_MODE"] = "raise"
        env["NOVEL_TTS_QUOTA_MAX_WAIT_SECONDS"] = "0"
        env["NOVEL_TTS_CENTRAL_QUOTA"] = "1"
        env["NOVEL_TTS_CENTRAL_QUOTA_NONBLOCKING"] = "1"
        env["NOVEL_TTS_ALL_KEY_PREFIXES_JSON"] = json.dumps(
            [_worker_key_prefix(queue_config, raw_key=raw) for raw in keys],
            ensure_ascii=False,
        )
        if not is_captions and not is_repair_glossary:
            env["NOVEL_TTS_GLOSSARY_STRICT"] = "1"
        env["GEMINI_RATE_LIMIT_KEY_PREFIX"] = _worker_key_prefix(queue_config, raw_key=api_key)
        env["GEMINI_REDIS_HOST"] = queue_config.redis.host
        env["GEMINI_REDIS_PORT"] = str(queue_config.redis.port)
        env["GEMINI_REDIS_DB"] = str(queue_config.redis.database)
        model_cfg = queue_config.model_configs.get(model)
        env["GEMINI_MODEL_CONFIGS_JSON"] = json.dumps(
            {
                model_name: {
                    "rpm_limit": cfg.rpm_limit,
                    "tpm_limit": cfg.tpm_limit,
                    "rpd_limit": cfg.rpd_limit,
                }
                for model_name, cfg in queue_config.model_configs.items()
            }
        )
        def _clean_model(value) -> str:
            if value is None:
                return ""
            text = str(value).strip()
            if not text:
                return ""
            if text.lower() in {"none", "null"}:
                return ""
            return text

        repair_model = _clean_model(model_cfg.repair_model) if model_cfg else ""
        glossary_model = _clean_model(getattr(model_cfg, "glossary_model", "")) if model_cfg else ""
        # Default to the worker's key-model when overrides are not configured.
        env["REPAIR_MODEL"] = repair_model or model
        env["GLOSSARY_MODEL"] = glossary_model or model
        if model_cfg and model_cfg.chunk_max_len > 0:
            env["CHUNK_MAX_LEN"] = str(model_cfg.chunk_max_len)
        if model_cfg and model_cfg.chunk_sleep_seconds > 0:
            env["CHUNK_SLEEP_SECONDS"] = str(model_cfg.chunk_sleep_seconds)
        # In queue mode, avoid spending a long time sleeping on 429 in a single worker.
        # Let the job be picked up by another worker/key instead.
        env.setdefault("NOVEL_TTS_RATE_LIMIT_MAX_ATTEMPTS", "4")
        import logging
        log_file_args = []
        for handler in logging.getLogger().handlers:
            if isinstance(handler, logging.FileHandler):
                log_file_args = ["--log-file", handler.baseFilename]
                break

        cmd_args = [sys.executable, "-m", "novel_tts"] + log_file_args
        if is_repair_glossary:
            cmd_args += ["glossary", "repair-chunk", novel_id, "--chunk-index", str(chunk_index)]
        else:
            cmd_args += ["translate"]
            if is_captions:
                cmd_args += ["captions", novel_id]
            else:
                cmd_args += ["chapter", novel_id]
                if is_force:
                    cmd_args.append("--force")
                cmd_args += ["--file", file_name, "--chapter", chapter_num]

        # If we hit the internal quota gate (exit=76), prefer waiting in-place for short waits rather than
        # releasing/requeueing the job, to reduce churn when workers are only slightly over TPM/RPM.
        inline_waited_seconds = 0.0
        inline_budget_raw = os.environ.get("NOVEL_TTS_INLINE_QUOTA_WAIT_BUDGET_SECONDS", "").strip()
        try:
            inline_budget_seconds = float(inline_budget_raw) if inline_budget_raw else 20.0
        except ValueError:
            inline_budget_seconds = 20.0
        hold_waited_seconds = 0.0
        hold_budget_raw = os.environ.get("NOVEL_TTS_HOLD_QUOTA_WAIT_BUDGET_SECONDS", "").strip()
        try:
            hold_budget_seconds = float(hold_budget_raw) if hold_budget_raw else 180.0
        except ValueError:
            hold_budget_seconds = 180.0
        while True:
            proc = subprocess.run(
                cmd_args,
                cwd=str(novel_config.storage.root),
                env=env,
                capture_output=True,
                text=True,
            )
            if proc.returncode != 76:
                break
            combined = "\n".join([proc.stdout or "", proc.stderr or ""]).strip()
            if _parse_quota_should_requeue(combined):
                break
            wait_seconds = _parse_quota_suggested_wait_seconds(combined)
            blocked_model = model
            if wait_seconds is None:
                should_pause, blocked_model, wait_seconds = _worker_should_pause_for_quota(config, client, key_index, model)
                if not should_pause:
                    wait_seconds = 0.0
                    blocked_model = model
            else:
                blocked_model = _parse_quota_blocked_model(combined) or model
            wait_seconds, _is_rpd_wait = _normalize_quota_wait_seconds(
                config,
                client,
                key_index,
                blocked_model or model,
                proposed_wait_seconds=float(wait_seconds or 0.0),
                text=combined,
            )
            if 0 < float(wait_seconds) < _INLINE_QUOTA_WAIT_MAX_SECONDS and (
                inline_budget_seconds <= 0 or (inline_waited_seconds + float(wait_seconds) <= inline_budget_seconds)
            ):
                LOGGER.warning(
                    "Worker inline quota wait | novel=%s key_index=%s model=%s blocked_model=%s wait_seconds=%.2f budget=%.2f waited=%.2f",
                    novel_id,
                    key_index,
                    model,
                    blocked_model,
                    float(wait_seconds),
                    float(inline_budget_seconds),
                    float(inline_waited_seconds),
                )
                sleep_seconds = max(0.25, float(wait_seconds))
                if _stop_requested():
                    LOGGER.info(
                        "Worker exiting before retry after inline quota wait (graceful stop) | novel=%s key_index=%s model=%s worker=%s",
                        novel_id,
                        key_index,
                        model,
                        worker_id,
                    )
                    client.hdel(_inflight_key(config), job_id)
                    _requeue_job_priority(config, client, job_id)
                    return 0
                time.sleep(sleep_seconds)
                inline_waited_seconds += sleep_seconds
                continue

            # If the recommended wait is longer, decide whether to hold the job (keep it inflight) vs releasing it.
            # Holding can reduce churn when there are no idle workers available to pick other work anyway.
            if float(wait_seconds) >= _INLINE_QUOTA_WAIT_MAX_SECONDS:
                has_idle = _any_idle_worker(config)
                if (not has_idle) and (hold_budget_seconds <= 0 or (hold_waited_seconds + float(wait_seconds) <= hold_budget_seconds)):
                    LOGGER.warning(
                        "Worker holding job due to quota gate (no idle workers) | job=%s novel=%s key_index=%s model=%s wait_seconds=%.2f hold_budget=%.2f hold_waited=%.2f",
                        job_id,
                        novel_id,
                        key_index,
                        model,
                        float(wait_seconds),
                        float(hold_budget_seconds),
                        float(hold_waited_seconds),
                    )
                    estimated_tokens = _parse_quota_estimated_tokens(combined) or _estimated_request_tokens_for_model(config, model)
                    started_hold = time.time()
                    while True:
                        if _stop_requested():
                            LOGGER.info(
                                "Worker exiting while holding quota-gated job (graceful stop) | job=%s novel=%s key_index=%s model=%s worker=%s",
                                job_id,
                                novel_id,
                                key_index,
                                model,
                                worker_id,
                            )
                            client.hdel(_inflight_key(config), job_id)
                            _requeue_job_priority(config, client, job_id)
                            return 0
                        waited = time.time() - started_hold
                        if hold_budget_seconds > 0 and (hold_waited_seconds + waited) > hold_budget_seconds:
                            break
                        remaining = 0.0
                        try:
                            remaining = _quota_wait_seconds_for_request(
                                config,
                                client,
                                key_index,
                                model,
                                estimated_tokens=estimated_tokens,
                            )
                        except Exception:
                            remaining = max(0.0, float(wait_seconds) - waited)
                        if remaining <= 0.05:
                            break
                        # Poll frequently so we resume as soon as quota opens, without busy looping.
                        base_sleep = min(0.5, max(0.05, remaining))
                        jitter = random.uniform(0.0, 0.2)
                        time.sleep(min(0.75, base_sleep + jitter))
                    hold_waited_seconds += max(0.0, time.time() - started_hold)
                    # Retry the job in-process (new subprocess invocation) now that quota should be available.
                    # If quota is still blocked, the next loop iteration will re-evaluate and either inline-wait,
                    # hold again, or fall back to release/delay.
                    continue
            break

        # Special transient exit code from CLI when providers keep returning 429.
        if proc.returncode == 75:
            client.hdel(_inflight_key(config), job_id)
            consecutive_rate_limit_releases += 1
            LOGGER.warning(
                "Worker releasing job due to rate limit | job=%s key_index=%s model=%s",
                job_id,
                key_index,
                model,
            )
            ip_triggered = _maybe_trigger_ip_ban_on_429(client, config, key_index=key_index, model=model)
            # Requeue without counting as a failure retry.
            delay_seconds = _rate_limit_requeue_delay_seconds(consecutive_rate_limit_releases)
            delay_seconds += random.uniform(0.0, min(1.0, delay_seconds * 0.25))
            _extend_rate_limit_cooldown_capped(
                client,
                cooldown_key,
                seconds=float(delay_seconds),
                max_seconds=max_429_cooldown_seconds,
            )
            _delay_job(config, client, job_id, float(delay_seconds))
            time.sleep(min(2.0, max(0.25, delay_seconds)))
            if ip_triggered:
                # When IP-ban is suspected, avoid additional probes from individual workers.
                consecutive_rate_limit_releases = 0
                continue
            if consecutive_rate_limit_releases >= 2:
                provider = (novel_config.models.provider or "").strip().lower()
                probe_429: bool | None = None
                if provider == "gemini_http":
                    # Probe with a tiny request to confirm we are *still* getting HTTP 429.
                    # If the probe fails (None), be conservative and keep the long cooldown behavior.
                    probe_429 = _probe_gemini_429(config=config, api_key=api_key, model=model, key_index=key_index, proxy_cfg=proxy_cfg)

                if probe_429 is False:
                    cooldown_seconds = float(_RATE_LIMIT_PROBE_COOLDOWN_SECONDS)
                    cooldown_seconds += _cooldown_jitter_seconds(key_index, max_jitter_seconds=5.0)
                    LOGGER.warning(
                        "Worker rate limit probe: no 429; sleeping for %.2fs | novel=%s key_index=%s model=%s",
                        cooldown_seconds,
                        novel_id,
                        key_index,
                        model,
                    )
                    # Even if a tiny probe succeeds, the real workload may still be quota-exhausted.
                    # Set a shared (key+model + global) cooldown so other workers don't immediately retry and re-429.
                    _extend_rate_limit_cooldown_capped(
                        client,
                        cooldown_key,
                        seconds=float(cooldown_seconds),
                        max_seconds=max_429_cooldown_seconds,
                    )
                    interrupted = _interruptible_sleep(
                        max_seconds=float(cooldown_seconds),
                        check_remaining_seconds=lambda: _get_rate_limit_cooldown_remaining_seconds(client, cooldown_key),
                        step_seconds=1.0,
                        min_sleep_seconds=0.25,
                        should_stop=_stop_requested,
                    )
                    if interrupted:
                        LOGGER.info(
                            "Worker exiting during probe cooldown (graceful stop) | novel=%s key_index=%s model=%s worker=%s",
                            novel_id,
                            key_index,
                            model,
                            worker_id,
                        )
                        return 0
                else:
                    rpd_wait_seconds = _model_rpd_wait_seconds(config, client, key_index, model)
                    is_rpd_wait = rpd_wait_seconds > 0.0
                    cooldown_seconds = min(rpd_wait_seconds, 3600.0) if is_rpd_wait else 60.0
                    probe_text = "probe=429" if probe_429 is True else "probe=unknown"
                    LOGGER.warning(
                        "Worker entering out-of-quota cooldown | out_of_quota=1 novel=%s key_index=%s model=%s wait_seconds=%.2f %s",
                        novel_id,
                        key_index,
                        model,
                        cooldown_seconds,
                        probe_text,
                    )
                    _extend_rate_limit_cooldown(client, out_of_quota_key, seconds=float(cooldown_seconds))
                    interrupted = _interruptible_sleep(
                        max_seconds=float(cooldown_seconds),
                        check_remaining_seconds=lambda: _get_rate_limit_cooldown_remaining_seconds(client, out_of_quota_key),
                        step_seconds=2.0,
                        min_sleep_seconds=0.5,
                        should_stop=_stop_requested,
                    )
                    if interrupted:
                        LOGGER.info(
                            "Worker exiting during out-of-quota cooldown after 429 (graceful stop) | novel=%s key_index=%s model=%s worker=%s",
                            novel_id,
                            key_index,
                            model,
                            worker_id,
                        )
                        return 0
                consecutive_rate_limit_releases = 0
            continue
        # Quota gating (RPM/TPM/RPD) without necessarily any HTTP 429.
        # Do not enter long out-of-quota cooldown; instead requeue and wait briefly.
        if proc.returncode == 76:
            client.hdel(_inflight_key(config), job_id)
            consecutive_rate_limit_releases = 0
            combined = "\n".join([proc.stdout or "", proc.stderr or ""]).strip()
            force_requeue = _parse_quota_should_requeue(combined)
            lowered = combined.lower()
            release_reason = "quota gate"
            if "timeout" in lowered or "timed out" in lowered or "connectionerror" in lowered:
                release_reason = "upstream timeout"
            LOGGER.warning(
                "Worker releasing job due to %s | job=%s key_index=%s model=%s",
                release_reason,
                job_id,
                key_index,
                model,
            )
            if force_requeue:
                # Central quota redirect: requeue so another key can pick it up.
                _requeue_job_priority(config, client, job_id)
                suggested_wait_fr = _parse_quota_suggested_wait_seconds(combined)
                blocked_model = _parse_quota_blocked_model(combined) or model
                normalized_wait = 0.0
                is_rpd_wait = False
                if suggested_wait_fr is not None and suggested_wait_fr > 0:
                    normalized_wait, is_rpd_wait = _normalize_quota_wait_seconds(
                        config,
                        client,
                        key_index,
                        blocked_model,
                        proposed_wait_seconds=float(suggested_wait_fr),
                        text=combined,
                    )
                if normalized_wait >= 3600.0 and is_rpd_wait:
                    # RPD-level exhaustion: this worker's key is depleted for hours.
                    # Enter out-of-quota cooldown so we don't hot-loop picking up jobs
                    # that will always be redirected back.
                    cooldown_seconds = min(float(normalized_wait), 3600.0)
                    LOGGER.warning(
                        "Worker entering out-of-quota cooldown (quota redirect with RPD-level wait) "
                        "| out_of_quota=1 novel=%s key_index=%s model=%s wait_seconds=%.2f",
                        novel_id,
                        key_index,
                        model,
                        cooldown_seconds,
                    )
                    _extend_rate_limit_cooldown(client, out_of_quota_key, seconds=float(cooldown_seconds))
                    interrupted = _interruptible_sleep(
                        max_seconds=float(cooldown_seconds),
                        check_remaining_seconds=lambda: _get_rate_limit_cooldown_remaining_seconds(client, out_of_quota_key),
                        step_seconds=2.0,
                        min_sleep_seconds=0.5,
                        should_stop=_stop_requested,
                    )
                    if interrupted:
                        LOGGER.info(
                            "Worker exiting during out-of-quota cooldown after quota redirect (graceful stop) | novel=%s key_index=%s model=%s worker=%s",
                            novel_id,
                            key_index,
                            model,
                            worker_id,
                        )
                        return 0
                elif normalized_wait > 0:
                    wait_seconds = max(1.0, float(normalized_wait))
                    LOGGER.warning(
                        "Worker quota wait | novel=%s key_index=%s model=%s blocked_model=%s wait_seconds=%.2f",
                        novel_id,
                        key_index,
                        model,
                        blocked_model,
                        wait_seconds,
                    )
                    interrupted = _interruptible_sleep(
                        max_seconds=wait_seconds,
                        check_remaining_seconds=lambda: wait_seconds,
                        step_seconds=1.0,
                        min_sleep_seconds=0.25,
                        should_stop=_stop_requested,
                    )
                    if interrupted:
                        LOGGER.info(
                            "Worker exiting during quota wait after quota redirect (graceful stop) | novel=%s key_index=%s model=%s worker=%s",
                            novel_id,
                            key_index,
                            model,
                            worker_id,
                        )
                        return 0
                else:
                    interrupted = _interruptible_sleep(
                        max_seconds=0.5,
                        check_remaining_seconds=lambda: 0.5,
                        step_seconds=0.25,
                        min_sleep_seconds=0.1,
                        should_stop=_stop_requested,
                    )
                    if interrupted:
                        LOGGER.info(
                            "Worker exiting during short retry wait after quota redirect (graceful stop) | novel=%s key_index=%s model=%s worker=%s",
                            novel_id,
                            key_index,
                            model,
                            worker_id,
                        )
                        return 0
                continue
            suggested_wait = _parse_quota_suggested_wait_seconds(combined)
            parsed_blocked_model = _parse_quota_blocked_model(combined) or ""
            should_pause, blocked_model, wait_seconds = _worker_should_pause_for_quota(config, client, key_index, model)
            if suggested_wait is not None and suggested_wait > 0:
                wait_seconds = max(float(wait_seconds), float(suggested_wait))
                blocked_model = parsed_blocked_model or blocked_model or model
            if not should_pause and (suggested_wait is None):
                wait_seconds = 1.0
                blocked_model = parsed_blocked_model or model
            wait_seconds, is_rpd_wait = _normalize_quota_wait_seconds(
                config,
                client,
                key_index,
                blocked_model or model,
                proposed_wait_seconds=float(wait_seconds or 0.0),
                text=combined,
            )
            LOGGER.warning(
                "Worker quota wait | novel=%s key_index=%s model=%s blocked_model=%s wait_seconds=%.2f",
                novel_id,
                key_index,
                model,
                blocked_model,
                wait_seconds,
            )
            # If there are idle workers, avoid delaying the job: requeue immediately so other keys/workers
            # can attempt it while this worker pauses.
            has_idle = _any_idle_worker(config)
            if has_idle:
                _requeue_job_priority(config, client, job_id)
            else:
                # Delay the job until the quota window should have cleared.
                _delay_job(config, client, job_id, float(wait_seconds))
            # Sleep so this worker doesn't keep pulling jobs that will likely be quota-gated too.
            # For RPD-level waits (>= 1h), enter out-of-quota cooldown; otherwise cap at 60s.
            if is_rpd_wait and float(wait_seconds) >= 3600.0:
                cooldown_seconds = min(float(wait_seconds), 3600.0)
                LOGGER.warning(
                    "Worker entering out-of-quota cooldown (quota gate with RPD-level wait) "
                    "| out_of_quota=1 novel=%s key_index=%s model=%s wait_seconds=%.2f",
                    novel_id,
                    key_index,
                    model,
                    cooldown_seconds,
                )
                _extend_rate_limit_cooldown(client, out_of_quota_key, seconds=float(cooldown_seconds))
                interrupted = _interruptible_sleep(
                    max_seconds=float(cooldown_seconds),
                    check_remaining_seconds=lambda: _get_rate_limit_cooldown_remaining_seconds(client, out_of_quota_key),
                    step_seconds=2.0,
                    min_sleep_seconds=0.5,
                    should_stop=_stop_requested,
                )
                if interrupted:
                    LOGGER.info(
                        "Worker exiting during out-of-quota cooldown after quota gate (graceful stop) | novel=%s key_index=%s model=%s worker=%s",
                        novel_id,
                        key_index,
                        model,
                        worker_id,
                    )
                    return 0
            else:
                sleep_seconds = max(1.0, min(float(wait_seconds), 60.0))
                interrupted = _interruptible_sleep(
                    max_seconds=sleep_seconds,
                    check_remaining_seconds=lambda: sleep_seconds,
                    step_seconds=1.0,
                    min_sleep_seconds=0.25,
                    should_stop=_stop_requested,
                )
                if interrupted:
                    LOGGER.info(
                        "Worker exiting during short quota wait (graceful stop) | novel=%s key_index=%s model=%s worker=%s",
                        novel_id,
                        key_index,
                        model,
                        worker_id,
                    )
                    return 0
            continue
        if proc.returncode == 0:
            client.hdel(_inflight_key(config), job_id)
            consecutive_rate_limit_releases = 0
            client.hdel(_novel_key(config, novel_id, "retries"), job_id)
            if is_force:
                client.hdel(_force_key(config), job_id)
            client.hset(
                _novel_key(config, novel_id, "done"),
                job_id,
                json.dumps(
                    {
                        "finished_at": time.time(),
                        "model": model,
                        "worker": worker_id,
                        "file_name": file_name,
                        "chapter_num": chapter_num,
                        "force": bool(is_force),
                    }
                ),
            )
            client.hincrby(_novel_key(config, novel_id, "model_done"), model, 1)
            LOGGER.info("Worker done: %s", job_id)
            continue
        if proc.returncode == 77:
            client.hdel(_inflight_key(config), job_id)
            stdout = (proc.stdout or "").strip()
            stderr = (proc.stderr or "").strip()
            LOGGER.error(
                "Worker input failure | job=%s key_index=%s model=%s returncode=%s stdout=%r stderr=%r",
                job_id,
                key_index,
                model,
                proc.returncode,
                stdout[-4000:],
                stderr[-4000:],
            )
            consecutive_rate_limit_releases = 0
            client.hincrby(_novel_key(config, novel_id, "model_failed"), model, 1)
            retries = client.hincrby(_novel_key(config, novel_id, "retries"), job_id, 1)
            if is_repair_glossary:
                needs_work = _repair_glossary_chunk_needs_work(novel_config, chunk_index)
            elif is_captions:
                needs_work = _captions_needs_translation(novel_config)
            else:
                assert source_path is not None
                needs_work = _chapter_needs_work(novel_config, source_path, chapter_num)
            if retries < _queue_config(config).max_retries and (is_force or needs_work):
                if client.sadd(_queued_key(config), job_id):
                    client.rpush(_pending_key(config), job_id)
            else:
                LOGGER.error("Worker gave up on %s after %s input retries", job_id, retries)
            continue
        client.hdel(_inflight_key(config), job_id)
        stdout = (proc.stdout or "").strip()
        stderr = (proc.stderr or "").strip()
        LOGGER.error(
            "Worker transient/unclassified failure | job=%s key_index=%s model=%s returncode=%s stdout=%r stderr=%r",
            job_id,
            key_index,
            model,
            proc.returncode,
            stdout[-4000:],
            stderr[-4000:],
        )
        consecutive_rate_limit_releases = 0
        client.hincrby(_novel_key(config, novel_id, "model_failed"), model, 1)
        if is_repair_glossary:
            needs_work = _repair_glossary_chunk_needs_work(novel_config, chunk_index)
        elif is_captions:
            needs_work = _captions_needs_translation(novel_config)
        else:
            assert source_path is not None
            needs_work = _chapter_needs_work(novel_config, source_path, chapter_num)
        if is_force or needs_work:
            _delay_job(config, client, job_id, 15.0)
            time.sleep(1.0)
            continue


def run_supervisor(config: NovelConfig | QueueConfig) -> int:
    qcfg = _queue_config(config)
    client = _client(config)
    while True:
        stopping = _is_stopping(config, client)
        if stopping:
            # Do NOT spawn new workers.  Wait for existing ones to drain.
            inflight = client.hlen(_inflight_key(config))
            alive_workers = _count_alive_worker_processes(config)
            LOGGER.info(
                "Supervisor stopping | inflight=%s alive_workers=%s",
                inflight,
                alive_workers,
            )
            if alive_workers == 0 and inflight == 0:
                _clear_stopping(config, client)
                LOGGER.info("Supervisor exiting (all workers drained)")
                return 0
        else:
            launched = _ensure_worker_processes(config)
            drained = _drain_delayed_jobs(config, client)
            _requeue_stale_inflight(config, client)
            LOGGER.info(
                "queue pending=%s queued=%s inflight=%s launched_workers=%s drained_delayed=%s",
                _pending_total_len(config, client),
                client.scard(_queued_key(config)),
                client.hlen(_inflight_key(config)),
                launched,
                drained,
            )
        time.sleep(qcfg.supervisor_interval_seconds)


def run_status_monitor(config: NovelConfig | QueueConfig) -> int:
    qcfg = _queue_config(config)
    client = _client(config)
    last_snapshot: dict[str, int] | None = None
    was_idle = False
    while True:
        # Consider the queue "idle" when there's nothing pending/queued/inflight.
        # We still update the state json (for ps/monitoring), but stop appending status.log to avoid noise.
        inflight = client.hlen(_inflight_key(config))
        pending = _pending_total_len(config, client)
        queued = client.scard(_queued_key(config))
        is_idle = (pending == 0) and (queued == 0) and (inflight == 0)

        # Write one line on the transition into idle, then stay quiet until work resumes.
        append_log = (not is_idle) or (not was_idle)
        last_snapshot = _write_status_line(config, client, last_snapshot, append_log=append_log)
        was_idle = is_idle
        time.sleep(qcfg.status_interval_seconds)


def _spawn_process(cmd: list[str], log_path: Path, cwd: Path) -> int:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("w", encoding="utf-8") as _:
        pass
    devnull = open(os.devnull, "w", encoding="utf-8")
    process = subprocess.Popen(
        cmd,
        cwd=str(cwd),
        stdout=devnull,
        stderr=devnull,
        start_new_session=True,
        text=True,
    )
    devnull.close()
    return process.pid


def _shared_queue_log_dir() -> Path:
    """Shared log directory for queue processes (supervisor, monitor, workers)."""
    from novel_tts.config.loader import _root_dir

    return _root_dir() / ".logs" / "_shared" / "queue"


def _worker_log_path(config: NovelConfig | QueueConfig, key_index: int, model: str, worker_idx: int) -> Path:
    safe_model = model.replace("-", "_")
    return _shared_queue_log_dir() / "workers" / f"k{key_index}-{safe_model}-w{worker_idx}.log"


def _worker_command(config: NovelConfig | QueueConfig, key_index: int, model: str, worker_idx: int) -> tuple[list[str], Path]:
    worker_log = _worker_log_path(config, key_index, model, worker_idx)
    cmd = [
        sys.executable,
        "-m",
        "novel_tts",
        "--log-file",
        str(worker_log),
        "queue",
        "worker",
        "--key-index",
        str(key_index),
        "--model",
        model,
    ]
    return cmd, worker_log


def _matching_worker_pids(config: NovelConfig | QueueConfig, key_index: int, model: str) -> list[int]:
    pattern = (
        f"novel_tts --log-file .* queue worker "
        f"--key-index {key_index} --model {model}"
    )
    proc = subprocess.run(
        ["pgrep", "-f", pattern],
        check=False,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        return []
    pids: list[int] = []
    for line in (proc.stdout or "").splitlines():
        value = line.strip()
        if not value:
            continue
        try:
            pids.append(int(value))
        except ValueError:
            continue
    return pids


def _count_alive_worker_processes(config: NovelConfig | QueueConfig) -> int:
    """Return the number of queue worker processes currently alive."""
    pattern = "novel_tts .* queue worker"
    proc = subprocess.run(
        ["pgrep", "-fc", pattern],
        check=False,
        capture_output=True,
        text=True,
    )
    try:
        return int((proc.stdout or "").strip())
    except ValueError:
        return 0


def _reap_unwanted_worker_processes(config: NovelConfig | QueueConfig, *, max_key_index: int, worker_models: list[str]) -> int:
    """
    Supervisor reconciliation:
    - If the keys file shrinks, stop workers whose --key-index is now out of range.
    - If enabled models change or worker_count decreases, stop extra workers.

    This prevents "orphan" workers from continuing to run after config/keys updates.
    """
    try:
        proc = subprocess.run(
            ["ps", "ax", "-o", "pid=,command="],
            check=False,
            capture_output=True,
            text=True,
        )
    except Exception:
        return 0
    if proc.returncode != 0:
        return 0

    enabled_models = set(worker_models or [])
    by_group: dict[tuple[int, str], list[int]] = {}
    killed = 0

    for line in (proc.stdout or "").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            pid_str, cmd = line.split(None, 1)
        except ValueError:
            continue
        if "novel_tts" not in cmd or "queue worker" not in cmd:
            continue
        try:
            pid = int(pid_str.strip())
        except ValueError:
            continue

        try:
            argv = shlex.split(cmd)
        except Exception:
            argv = cmd.split()

        key_index: int | None = None
        model = ""
        for idx, token in enumerate(argv):
            if token == "--key-index" and idx + 1 < len(argv):
                try:
                    key_index = int(str(argv[idx + 1]).strip())
                except Exception:
                    key_index = None
            elif token == "--model" and idx + 1 < len(argv):
                model = str(argv[idx + 1]).strip()

        if key_index is None or not model:
            continue

        if key_index > max_key_index:
            subprocess.run(["kill", str(pid)], check=False)
            killed += 1
            continue

        if enabled_models and model not in enabled_models:
            subprocess.run(["kill", str(pid)], check=False)
            killed += 1
            continue

        by_group.setdefault((key_index, model), []).append(pid)

    qcfg = _queue_config(config)
    for (key_index, model), pids in by_group.items():
        model_cfg = qcfg.model_configs.get(model)
        desired = max(0, int(model_cfg.worker_count if model_cfg else 1))
        if len(pids) <= desired:
            continue
        for pid in sorted(pids)[desired:]:
            subprocess.run(["kill", str(pid)], check=False)
            killed += 1

    return killed


def _ensure_worker_processes(config: NovelConfig | QueueConfig) -> int:
    keys = _load_keys(config)
    max_key_index, cap_reason = _effective_worker_key_limit(config, total_keys=len(keys))
    qcfg = _queue_config(config)
    worker_models = qcfg.enabled_models or ["gemma-3-27b-it", "gemma-3-12b-it"]
    client = _client(config)
    for model in worker_models:
        _maybe_apply_startup_ramp(client, config, model=model)
    _reap_unwanted_worker_processes(config, max_key_index=max_key_index, worker_models=worker_models)
    if cap_reason:
        LOGGER.info(
            "Worker key limit active | reason=%s using_keys=%s total_keys=%s",
            cap_reason,
            max_key_index,
            len(keys),
        )
    spawn_interval = 0.0
    try:
        spawn_interval = float(getattr(qcfg, "spawn_key_interval_seconds", 0.0) or 0.0)
    except Exception:
        spawn_interval = 0.0
    launched = 0
    from novel_tts.config.loader import _root_dir
    cwd = _root_dir()
    for key_index in range(1, max_key_index + 1):
        launched_before = launched
        for model in worker_models:
            model_cfg = qcfg.model_configs.get(model)
            worker_count = max(0, int(model_cfg.worker_count if model_cfg else 1))
            running = len(_matching_worker_pids(config, key_index, model))
            for worker_idx in range(running + 1, worker_count + 1):
                cmd, worker_log = _worker_command(config, key_index, model, worker_idx)
                pid = _spawn_process(cmd, worker_log, cwd)
                launched += 1
                LOGGER.info(
                    "Launched worker pid=%s key_index=%s model=%s worker_idx=%s log=%s",
                    pid,
                    key_index,
                    model,
                    worker_idx,
                    worker_log,
                )
        key_launched = launched - launched_before
        if key_launched > 0 and spawn_interval > 0 and key_index < max_key_index:
            time.sleep(spawn_interval)
    return launched


def launch_queue_stack(config: NovelConfig | QueueConfig, restart: bool = False, *, add_queue: bool = False, add_queue_novel_ids: list[str] | None = None) -> int:
    qcfg = _queue_config(config)
    keys = _load_keys(config)
    worker_keys, worker_cap_reason = _effective_worker_key_limit(config, total_keys=len(keys))
    client = _client(config)
    from novel_tts.config.loader import _root_dir
    cwd = _root_dir()
    prefix = str(qcfg.redis.prefix or "").strip() or "novel_tts"

    if restart:
        # Graceful drain: signal workers to finish current job, then wait.
        _set_stopping(config, client)
        print("Restarting: waiting for workers to finish current jobs …", flush=True)
        drained = _wait_for_workers_drain(config, client, timeout_seconds=300.0)
        if not drained:
            print(
                "Timeout waiting for workers — force-killing remaining processes.",
                file=sys.stderr,
                flush=True,
            )
        # Kill supervisor, monitor (and any stragglers) after drain.
        patterns = [
            "novel_tts .* queue supervisor",
            "novel_tts .* queue monitor",
            "novel_tts .* queue worker",
            "novel_tts .* translate chapter",
        ]
        for pattern in patterns:
            subprocess.run(["pkill", "-f", pattern], check=False)
        _clear_stopping(config, client)
        # Delete global keys.
        client.delete(
            _pending_priority_key(config),
            _pending_delayed_key(config),
            _pending_key(config),
            _queued_key(config),
            _inflight_key(config),
            _force_key(config),
        )
        # Delete all per-novel keys.
        for suffix in ("done", "retries", "model_done", "model_failed", "pending_count"):
            for k in _scan_novel_keys(client, prefix, suffix):
                client.delete(k)
        # Allow startup ramp to re-apply after restart.
        for model in (qcfg.enabled_models or []):
            _clear_ip_recover_state(client, config, model=model)
            try:
                client.delete(_startup_ramp_applied_key(config, model=model))
            except Exception:
                pass
        status_log, state_log = _status_paths(config)
        for path in (status_log, state_log):
            try:
                path.unlink(missing_ok=True)
            except Exception:
                LOGGER.warning("Failed to remove status artifact on restart: %s", path)
        time.sleep(1)
    else:
        rc, ps_stdout = _run_ps_ax(cwd=cwd)
        if rc == 0:
            rows, _ppid_by_pid, _worker_meta_by_pid = _collect_queue_rows_from_ps(ps_stdout)
            has_supervisor = any(row.get("role") == "supervisor" for row in rows)
            has_monitor = any(row.get("role") == "monitor" for row in rows)
            if has_supervisor and has_monitor:
                LOGGER.info("Queue stack already running")
                if add_queue and add_queue_novel_ids:
                    for nid in add_queue_novel_ids:
                        nc = _cached_novel_config(nid)
                        add_all_jobs_to_queue(nc)
                return 0

    # Clear any stale stopping signal or inflight entries left behind by a previous force-stop.
    _clear_stopping(config, client)
    _requeue_stale_inflight(config, client)

    # Ask the global quota-supervisor to rotate queue logs. If that control-plane request fails,
    # fall back to a local best-effort rotation so queue launch itself is not blocked by log upkeep.
    requests_key = f"{prefix}:logrotate:requests"
    request_id = uuid.uuid4().hex
    reply_key = f"{prefix}:logrotate:reply:{request_id}"
    payload = json.dumps(
        {
            "cmd": "rotate_queue_logs",
            "request_id": request_id,
            "reply_key": reply_key,
            "created_at": time.time(),
            "pid": os.getpid(),
        },
        ensure_ascii=False,
    )
    rotate_warning: str | None = None
    try:
        client.rpush(requests_key, payload)
    except Exception as exc:
        rotate_warning = f"queue launch: unable to send logrotate request to quota-supervisor: {exc}"
    else:
        deadline = time.time() + 5.0
        ack_raw = ""
        while time.time() < deadline:
            try:
                ack_raw = str(client.get(reply_key) or "").strip()
            except Exception:
                ack_raw = ""
            if ack_raw:
                break
            time.sleep(0.05)
        if not ack_raw:
            rotate_warning = "queue launch: logrotate ack timeout (continuing with local fallback)"
        else:
            try:
                ack = json.loads(ack_raw)
            except Exception:
                ack = {}
            ok = bool(ack.get("ok")) if isinstance(ack, dict) else False
            if not ok:
                rotate_warning = f"queue launch: logrotate failed (ack={ack_raw!r}); continuing with local fallback"

    if rotate_warning:
        print(rotate_warning, file=sys.stderr)
        try:
            queue_log_root = _shared_queue_log_dir()
            rotated = 0
            for root, _dirs, files in os.walk(queue_log_root):
                root_path = Path(root)
                for name in files:
                    if not name.endswith(".log"):
                        continue
                    moved = logrotate.rotate_log_file_to_today(
                        logs_root=queue_log_root.parents[1],
                        src=root_path / name,
                    )
                    if moved is not None:
                        rotated += 1
            LOGGER.warning("Queue launch used local logrotate fallback | rotated=%s warning=%s", rotated, rotate_warning)
        except Exception as exc:
            LOGGER.warning("Queue launch local logrotate fallback failed: %s", exc)
            print(f"queue launch: local logrotate fallback failed: {exc}", file=sys.stderr)

    log_dir = _shared_queue_log_dir()
    supervisor_log = log_dir / "supervisor.log"
    supervisor_pid = _spawn_process(
        [
            sys.executable,
            "-m",
            "novel_tts",
            "--log-file",
            str(supervisor_log),
            "queue",
            "supervisor",
        ],
        supervisor_log,
        cwd,
    )
    LOGGER.info("Launched supervisor pid=%s log=%s", supervisor_pid, supervisor_log)
    monitor_log = log_dir / "monitor.log"
    monitor_pid = _spawn_process(
        [
            sys.executable,
            "-m",
            "novel_tts",
            "--log-file",
            str(monitor_log),
            "queue",
            "monitor",
        ],
        monitor_log,
        cwd,
    )
    LOGGER.info("Launched status monitor pid=%s log=%s", monitor_pid, monitor_log)

    # Workers are spawned by the supervisor on its first loop iteration.
    LOGGER.info(
        "Queue stack launched | keys=%s worker_keys=%s worker_key_cap_reason=%s supervisor=%s monitor=%s (supervisor will spawn workers)",
        len(keys),
        worker_keys,
        worker_cap_reason or "-",
        supervisor_pid,
        monitor_pid,
    )

    if add_queue and add_queue_novel_ids:
        for nid in add_queue_novel_ids:
            nc = _cached_novel_config(nid)
            add_all_jobs_to_queue(nc)
    return 0


def _run_ps_ax(*, cwd: Path | None = None) -> tuple[int, str]:
    try:
        proc = subprocess.run(
            ["ps", "ax", "-o", "pid=,ppid=,command="],
            cwd=str(cwd) if cwd is not None else None,
            check=False,
            capture_output=True,
            text=True,
        )
    except PermissionError as exc:
        LOGGER.error("Unable to run ps to list processes: %s", exc)
        return 1, ""
    if proc.returncode != 0:
        LOGGER.error("Unable to run ps ax to list processes")
        return 1, ""
    return 0, proc.stdout or ""


def _queue_role_and_novel_id(argv: list[str]) -> tuple[str, str]:
    role = ""
    novel_id = ""

    # Queue commands: supervisor/monitor/worker no longer have novel_id in argv.
    if "queue" in argv:
        q_idx = argv.index("queue")
        if q_idx + 1 < len(argv):
            subcmd = argv[q_idx + 1]
            if subcmd == "supervisor":
                role = "supervisor"
            elif subcmd == "monitor":
                role = "monitor"
            elif subcmd == "worker":
                role = "worker"
            elif subcmd == "launch":
                role = "launcher"

    # Translate chapter subprocesses still have: "translate chapter <novel_id>"
    if not role and "translate" in argv:
        t_idx = argv.index("translate")
        if t_idx + 2 < len(argv) and argv[t_idx + 1] == "chapter":
            novel_id = argv[t_idx + 2]
            role = "translate-chapter"

    return role, novel_id


def _extract_proc_meta(argv: list[str]) -> tuple[str, str, str]:
    log_file = ""
    key_index = ""
    model = ""
    for idx, token in enumerate(argv):
        if token == "--log-file" and idx + 1 < len(argv):
            log_file = argv[idx + 1]
        elif token == "--key-index" and idx + 1 < len(argv):
            key_index = argv[idx + 1]
        elif token == "--model" and idx + 1 < len(argv):
            model = argv[idx + 1]
    return log_file, key_index, model


def _collect_queue_rows_from_ps(ps_stdout: str) -> tuple[list[dict[str, str]], dict[str, str], dict[str, dict[str, str]]]:
    rows: list[dict[str, str]] = []
    ppid_by_pid: dict[str, str] = {}
    worker_meta_by_pid: dict[str, dict[str, str]] = {}

    for raw_line in (ps_stdout or "").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        try:
            pid_str, ppid_str, cmd = line.split(None, 2)
        except ValueError:
            continue
        pid = pid_str.strip()
        ppid = ppid_str.strip()
        ppid_by_pid[pid] = ppid
        if "novel_tts" not in cmd:
            continue

        try:
            argv = shlex.split(cmd)
        except Exception:
            argv = cmd.split()

        role, novel_id = _queue_role_and_novel_id(argv)
        if not role:
            continue

        log_file, key_index, model = _extract_proc_meta(argv)
        target = _extract_target_from_argv(argv) if role == "translate-chapter" else ""

        row = {
            "pid": pid,
            "ppid": ppid,
            "novel_id": novel_id,
            "role": role,
            "key_index": key_index,
            "model": model,
            "target": target,
            "log_file": log_file,
            "state": "",
            "countdown": "",
        }
        rows.append(row)
        if role == "worker":
            worker_meta_by_pid[pid] = {"key_index": key_index, "model": model}

    return rows, ppid_by_pid, worker_meta_by_pid


def _inherit_worker_meta(*, pid: str, ppid_by_pid: dict[str, str], worker_meta_by_pid: dict[str, dict[str, str]]) -> dict[str, str] | None:
    cursor = pid
    for _ in range(6):
        if not cursor:
            break
        meta = worker_meta_by_pid.get(cursor)
        if meta and meta.get("key_index") and meta.get("model"):
            return meta
        cursor = ppid_by_pid.get(cursor, "")
    return None


def _infer_worker_meta_from_log_path(path: str) -> dict[str, str] | None:
    base = os.path.basename(path or "")
    match = re.search(r"^k(?P<key>\d+)-(?P<model>.+?)(?:-w\d+)?\.log$", base)
    if not match:
        return None
    return {"key_index": match.group("key"), "model": match.group("model").replace("_", "-")}


def _enrich_translate_chapter_meta(
    rows: list[dict[str, str]],
    *,
    ppid_by_pid: dict[str, str],
    worker_meta_by_pid: dict[str, dict[str, str]],
) -> None:
    for row in rows:
        if row.get("role") != "translate-chapter":
            continue
        if row.get("key_index") and row.get("model"):
            continue
        inherited = _inherit_worker_meta(pid=row.get("ppid", ""), ppid_by_pid=ppid_by_pid, worker_meta_by_pid=worker_meta_by_pid)
        if inherited:
            row["key_index"] = row.get("key_index") or inherited.get("key_index", "")
            row["model"] = row.get("model") or inherited.get("model", "")
        if row.get("key_index") and row.get("model"):
            continue
        inferred = _infer_worker_meta_from_log_path(row.get("log_file", ""))
        if inferred:
            row["key_index"] = row.get("key_index") or inferred.get("key_index", "")
            row["model"] = row.get("model") or inferred.get("model", "")


def _children_by_ppid(rows: list[dict[str, str]]) -> dict[str, list[dict[str, str]]]:
    children: dict[str, list[dict[str, str]]] = {}
    for row in rows:
        children.setdefault(row.get("ppid", ""), []).append(row)
    return children


def _classify_queue_rows(rows: list[dict[str, str]], *, surface_worker_target: bool) -> dict[str, list[dict[str, str]]]:
    children_by_ppid = _children_by_ppid(rows)

    for row in rows:
        role = row.get("role", "")
        pid = row.get("pid", "")
        is_busy = False
        if role == "worker":
            children = [c for c in children_by_ppid.get(pid, []) if c.get("role") == "translate-chapter"]
            if children:
                is_busy = True
                if surface_worker_target:
                    children.sort(key=lambda c: int(c.get("pid") or 10**12))
                    row["target"] = (children[0].get("target") or "").strip()
        state, countdown = _classify_process_state(role, is_busy=is_busy, log_file=row.get("log_file", ""))
        row["state"] = state
        row["countdown"] = str(int(math.ceil(countdown))) if countdown is not None and countdown > 0 else ""

    # If a worker has translate-chapter children, align the worker's state with the child's effective state.
    for row in rows:
        if row.get("role") != "worker":
            continue
        pid = row.get("pid", "")
        children = [c for c in children_by_ppid.get(pid, []) if c.get("role") == "translate-chapter"]
        combined_state, combined_countdown = _combine_worker_child_states_with_countdown(children)
        if combined_state:
            row["state"] = combined_state
            row["countdown"] = (
                str(int(math.ceil(combined_countdown)))
                if combined_countdown is not None and combined_countdown > 0
                else ""
            )

    return children_by_ppid


def _format_log_path_for_display(path: str, *, root: Path | None = None) -> str:
    raw = path or ""
    if not raw:
        return ""
    if root is not None:
        try:
            root_s = str(root)
            if raw.startswith(root_s + os.sep):
                return os.path.relpath(raw, root_s)
        except Exception:
            pass
    marker = f"{os.sep}.logs{os.sep}"
    idx = raw.find(marker)
    if idx >= 0:
        return raw[idx + 1 :]
    try:
        cwd = os.getcwd()
        if raw.startswith(cwd + os.sep):
            return os.path.relpath(raw, cwd)
    except Exception:
        pass
    return os.path.basename(raw)


def _truncate_middle(value: str, max_len: int) -> str:
    value = value or ""
    if max_len <= 0 or len(value) <= max_len:
        return value
    head = max(1, (max_len - 3) // 2)
    tail = max(1, max_len - 3 - head)
    return value[:head] + "..." + value[-tail:]


def _render_queue_table(rows: list[dict[str, str]], *, target_count: int, root: Path | None = None) -> None:
    headers = ["PID", "ROLE", "KEY", "STATE", "COUNTDOWN", "MODEL", "TARGET", "LOG"]
    target_header = f"TARGET ({target_count})"

    def _countdown_display(raw: str) -> str:
        raw = (raw or "").strip()
        if not raw:
            return ""
        try:
            return _format_countdown(float(raw))
        except Exception:
            return ""

    display_rows: list[dict[str, str]] = []
    for r in rows:
        display_rows.append(
            {
                "PID": r.get("pid", ""),
                "ROLE": r.get("role", ""),
                "KEY": r.get("key_index", "") or "",
                "STATE": r.get("state", ""),
                "COUNTDOWN": _countdown_display(r.get("countdown", "")),
                "MODEL": r.get("model", "") or "",
                "TARGET": r.get("target", "") or "",
                "LOG": _truncate_middle(_format_log_path_for_display(r.get("log_file", ""), root=root), 110),
            }
        )

    widths: dict[str, int] = {h: len(h) for h in headers}
    widths["TARGET"] = max(widths["TARGET"], len(target_header))
    for r in display_rows:
        for h in headers:
            widths[h] = max(widths[h], len(r.get(h, "")))

    def _hr() -> str:
        return "+-" + "-+-".join("-" * widths[h] for h in headers) + "-+"

    def _row(values: dict[str, str]) -> str:
        cells: list[str] = []
        for h in headers:
            val = values.get(h, "")
            if h in {"PID", "KEY"}:
                cells.append(val.rjust(widths[h]))
            else:
                cells.append(val.ljust(widths[h]))
        return "| " + " | ".join(cells) + " |"

    print(_hr())
    print(_row({h: (target_header if h == "TARGET" else h) for h in headers}))
    print(_hr())
    for r in display_rows:
        print(_row(r))
    print(_hr())


def _render_empty_queue_table(*, root: Path | None = None) -> None:
    del root
    _render_queue_table([], target_count=0)


def _sort_queue_rows(rows: list[dict[str, str]]) -> None:
    def _role_rank(role: str) -> int:
        order = {
            "supervisor": 0,
            "monitor": 1,
            "worker": 2,
            "translate-chapter": 3,
            "launcher": 4,
        }
        return order.get(role or "", 9)

    def _safe_int(value: str, default: int = 10**12) -> int:
        try:
            return int(str(value).strip())
        except Exception:
            return default

    rows.sort(
        key=lambda r: (
            _role_rank(r.get("role", "")),
            _safe_int(r.get("key_index", ""), default=10**12),
            r.get("model", "") or "",
            _safe_int(r.get("pid", ""), default=10**12),
        )
    )


def _queue_counts_from_redis(config: NovelConfig, client) -> tuple[int, int, int, int, int, int]:
    pending = int(_pending_total_len(config, client) or 0)
    queued = int(client.scard(_queued_key(config)) or 0)
    inflight = int(client.hlen(_inflight_key(config)) or 0)
    retries = int(client.hlen(_novel_key(config, config.novel_id, "retries")) or 0)
    exhausted = int(_exhausted_retry_count(config, client) or 0)
    done = int(client.hlen(_novel_key(config, config.novel_id, "done")) or 0)
    return pending, queued, inflight, retries, exhausted, done


def _queue_counts_from_state_log(config: NovelConfig) -> tuple[int, int, int, int, int, int]:
    _status_log, state_log = _status_paths(config)
    if not state_log.exists():
        return 0, 0, 0, 0, 0, 0
    try:
        snapshot = json.loads(state_log.read_text(encoding="utf-8"))
    except Exception:
        return 0, 0, 0, 0, 0, 0
    pending = int(snapshot.get("pending", 0) or 0)
    queued = int(snapshot.get("queued", 0) or 0)
    inflight = int(snapshot.get("inflight", 0) or 0)
    retries = int(snapshot.get("retries", 0) or 0)
    exhausted = int(snapshot.get("exhausted", 0) or 0)
    done = int(snapshot.get("done", 0) or 0)
    return pending, queued, inflight, retries, exhausted, done


def _apply_live_redis_overrides(
    config: NovelConfig | QueueConfig,
    client,
    rows: list[dict[str, str]],
    *,
    children_by_ppid: dict[str, list[dict[str, str]]],
    pending: int,
    queued: int,
) -> None:
    # If Redis is available, use cooldown keys as the source of truth for remaining time so
    # `queue reset-key` immediately reflects in ps output (log-derived countdowns can be stale).
    for row in rows:
        if row.get("role") != "worker":
            continue
        raw_key_index = (row.get("key_index") or "").strip()
        raw_model = (row.get("model") or "").strip()
        if (not raw_key_index) or (not raw_model):
            continue
        try:
            key_index = int(raw_key_index)
        except Exception:
            continue
        out_key = _out_of_quota_cooldown_key(config, key_index=key_index, model=raw_model)
        rl_key = _rate_limit_cooldown_key(config, key_index=key_index, model=raw_model)
        out_remaining = _get_rate_limit_cooldown_remaining_seconds(client, out_key)
        rl_remaining = _get_rate_limit_cooldown_remaining_seconds(client, rl_key)
        if out_remaining > 0.05:
            row["state"] = "out-of-quota"
            row["countdown"] = str(int(math.ceil(out_remaining)))
        elif float(rl_remaining or 0.0) > 0.05:
            row["state"] = "waiting-429"
            row["countdown"] = str(int(math.ceil(float(rl_remaining or 0.0))))
        elif row.get("state") in {"waiting-429", "out-of-quota"}:
            row["state"] = "idle"
            row["countdown"] = ""

    # Central quota v2: if a translate-chapter process is waiting for a grant, surface countdown from
    # quota-supervisor ETA cache and propagate to its worker via the existing child-state combiner.
    now_s = time.time()
    for row in rows:
        if row.get("role") != "translate-chapter":
            continue
        raw_pid = (row.get("pid") or "").strip()
        raw_key_index = (row.get("key_index") or "").strip()
        raw_model = (row.get("model") or "").strip()
        if (not raw_pid) or (not raw_key_index) or (not raw_model):
            continue
        try:
            pid_int = int(raw_pid)
        except Exception:
            continue
        key_prefix = _worker_key_prefix_for_index(config, key_index=int(raw_key_index))
        inflight_key = f"{key_prefix}:{raw_model}:quota:alloc:inflight:{pid_int}"
        try:
            inflight_raw = client.get(inflight_key)
        except Exception:
            inflight_raw = None
        if not inflight_raw:
            continue
        try:
            inflight_meta = json.loads(inflight_raw)
        except Exception:
            inflight_meta = {}
        request_id = str((inflight_meta or {}).get("request_id") or "").strip()
        if not request_id:
            continue
        eta_key = f"{key_prefix}:{raw_model}:quota:alloc:eta"
        try:
            eta_raw = client.hget(eta_key, request_id)
        except Exception:
            eta_raw = None
        remaining = None
        if eta_raw:
            try:
                grant_at = float(eta_raw)
                remaining = max(0.0, grant_at - now_s)
            except Exception:
                remaining = None
        row["state"] = "waiting-quota"
        row["countdown"] = str(int(math.ceil(remaining))) if remaining is not None and remaining > 0.05 else ""

    # Re-combine worker state after central-quota updates so waiting-quota propagates to the worker rows.
    for row in rows:
        if row.get("role") != "worker":
            continue
        pid = row.get("pid", "")
        children = [c for c in children_by_ppid.get(pid, []) if c.get("role") == "translate-chapter"]
        combined_state, combined_countdown = _combine_worker_child_states_with_countdown(children)
        if combined_state:
            row["state"] = combined_state
            row["countdown"] = (
                str(int(math.ceil(combined_countdown)))
                if combined_countdown is not None and combined_countdown > 0
                else ""
            )

    has_queue_work = int(pending or 0) > 0 or int(queued or 0) > 0
    if not has_queue_work:
        return

    for row in rows:
        if row.get("role") != "worker":
            continue
        if (row.get("state") or "").strip() != "idle":
            continue
        raw_key_index = (row.get("key_index") or "").strip()
        raw_model = (row.get("model") or "").strip()
        if (not raw_key_index) or (not raw_model):
            continue
        try:
            key_index = int(raw_key_index)
        except Exception:
            continue
        try:
            should_pause, _blocked_model, wait_seconds = _worker_should_pause_for_quota(
                config,
                client,
                key_index,
                raw_model,
            )
        except Exception:
            continue
        if should_pause and float(wait_seconds or 0.0) > 0.05:
            row["state"] = "waiting-quota"
            row["countdown"] = str(int(math.ceil(float(wait_seconds))))
            continue

        if _worker_is_recently_picking(config, client, key_index=key_index):
            # Surface the brief handoff/pick window explicitly, but only when we saw a recent pick attempt.
            row["state"] = "picking"
            row["countdown"] = ""


def list_queue_processes(config: NovelConfig, include_all: bool = False) -> int:
    """List queue-related processes for a novel in a pm2-like summary, plus progress."""
    rc, stdout = _run_ps_ax(cwd=config.storage.root)
    if rc != 0:
        return 1

    all_rows, ppid_by_pid, worker_meta_by_pid = _collect_queue_rows_from_ps(stdout)
    # Include shared-queue roles (no novel_id) and translate-chapter rows for this novel.
    rows = [
        r for r in all_rows
        if r.get("role") in {"supervisor", "monitor", "worker"}
        or (r.get("novel_id") or "").strip() == config.novel_id
    ]
    if not rows:
        print(f"No queue processes found for novel {config.novel_id}")
        _render_empty_queue_table(root=config.storage.root)
        return 0

    _enrich_translate_chapter_meta(rows, ppid_by_pid=ppid_by_pid, worker_meta_by_pid=worker_meta_by_pid)
    children_by_ppid = _classify_queue_rows(rows, surface_worker_target=True)

    pending = queued = inflight = retries = exhausted = done = 0
    client = None
    try:
        client = _client(config)
        pending, queued, inflight, retries, exhausted, done = _queue_counts_from_redis(config, client)
        _apply_live_redis_overrides(
            config,
            client,
            rows,
            children_by_ppid=children_by_ppid,
            pending=pending,
            queued=queued,
        )
    except Exception:
        pending, queued, inflight, retries, exhausted, done = _queue_counts_from_state_log(config)

    print(
        f"\nNovel {config.novel_id}:"
        f" pending={pending} queued={queued} inflight={inflight} retries={retries} exhausted={exhausted} done={done}"
    )

    _sort_queue_rows(rows)
    render_rows = rows if include_all else [r for r in rows if r.get("role") != "translate-chapter"]
    _render_queue_table(render_rows, target_count=_unique_target_count(rows), root=config.storage.root)
    return 0


def _novel_counts_from_redis(
    queue_config: QueueConfig, client
) -> dict[str, tuple[int, int, int]]:
    """Return {novel_id: (done, retries, exhausted)} by scanning per-novel keys."""
    prefix = _prefix(queue_config)
    novel_ids: set[str] = set()
    for suffix in ("done", "retries"):
        for key in _scan_novel_keys(client, prefix, suffix):
            # Key format: {prefix}:novel:{novel_id}:{suffix}
            parts = key.split(":")
            # Find "novel" marker in parts
            try:
                n_idx = parts.index("novel")
                novel_ids.add(parts[n_idx + 1])
            except (ValueError, IndexError):
                pass

    result: dict[str, tuple[int, int, int]] = {}
    for novel_id in sorted(novel_ids):
        done = int(client.hlen(_novel_key(queue_config, novel_id, "done")) or 0)
        retries_all = client.hgetall(_novel_key(queue_config, novel_id, "retries"))
        retries = len(retries_all)
        exhausted = sum(
            1
            for v in retries_all.values()
            if int(v or 0) >= queue_config.max_retries
        )
        result[novel_id] = (done, retries, exhausted)
    return result


def list_all_queue_processes(include_all: bool = False) -> int:
    """Consolidated view: global queue stats, per-novel job counts, shared worker table."""
    rc, stdout = _run_ps_ax()
    if rc != 0:
        return 1

    all_rows, ppid_by_pid, worker_meta_by_pid = _collect_queue_rows_from_ps(stdout)
    _enrich_translate_chapter_meta(all_rows, ppid_by_pid=ppid_by_pid, worker_meta_by_pid=worker_meta_by_pid)
    children_by_ppid = _classify_queue_rows(all_rows, surface_worker_target=True)

    # --- Global queue stats from Redis ---
    pending = queued = inflight = 0
    per_novel: dict[str, tuple[int, int, int]] = {}  # novel_id -> (done, retries, exhausted)
    client = None
    queue_config = None
    redis_ok = False
    try:
        from novel_tts.config.loader import load_queue_config as _load_queue_config

        queue_config = _load_queue_config()
        client = _client(queue_config)
        pending = int(_pending_total_len(queue_config, client) or 0)
        queued = int(client.scard(_queued_key(queue_config)) or 0)
        inflight = int(client.hlen(_inflight_key(queue_config)) or 0)
        per_novel = _novel_counts_from_redis(queue_config, client)
        _apply_live_redis_overrides(
            queue_config,
            client,
            all_rows,
            children_by_ppid=children_by_ppid,
            pending=pending,
            queued=queued,
        )
        redis_ok = True
    except Exception:
        redis_ok = False

    # Fallback: read the shared state log.
    if not redis_ok:
        try:
            state_log = _shared_queue_log_dir() / "status.state.json"
            if state_log.exists():
                snapshot = json.loads(state_log.read_text(encoding="utf-8"))
                pending = int(snapshot.get("pending", 0) or 0)
                queued = int(snapshot.get("queued", 0) or 0)
                inflight = int(snapshot.get("inflight", 0) or 0)
        except Exception:
            pass

    print(f"\nQueue: pending={pending} queued={queued} inflight={inflight}")

    # --- Per-novel breakdown ---
    # Collect novel_ids seen in inflight processes too (translate-chapter rows).
    seen_novel_ids: set[str] = set(per_novel.keys())
    for row in all_rows:
        n = (row.get("novel_id") or "").strip()
        if n:
            seen_novel_ids.add(n)

    for novel_id in sorted(seen_novel_ids):
        done, retries, exhausted = per_novel.get(novel_id, (0, 0, 0))
        print(f"  {novel_id}: done={done} retries={retries} exhausted={exhausted}")

    if not all_rows:
        print("No queue processes found")
        _render_empty_queue_table()
        return 0

    # --- Single unified worker table ---
    _sort_queue_rows(all_rows)
    render_rows = all_rows if include_all else [r for r in all_rows if r.get("role") != "translate-chapter"]
    _render_queue_table(render_rows, target_count=_unique_target_count(all_rows))
    return 0


def _force_stop_queue_processes(config: NovelConfig | QueueConfig, roles: set[str] | None = None) -> None:
    """Immediately kill queue processes with SIGTERM."""
    patterns: list[str] = []
    if roles is None or "supervisor" in roles:
        patterns.append("novel_tts .* queue supervisor")
    if roles is None or "monitor" in roles:
        patterns.append("novel_tts .* queue monitor")
    if roles is None or "worker" in roles:
        patterns.append("novel_tts .* queue worker")
    if roles is None or "translate-chapter" in roles:
        patterns.append("novel_tts .* translate chapter")
    for pattern in patterns:
        subprocess.run(["pkill", "-f", pattern], check=False)
    LOGGER.info(
        "Force-stopped queue processes | roles=%s",
        ", ".join(sorted(roles or [])) or "all",
    )


def _wait_for_workers_drain(config: NovelConfig | QueueConfig, client, *, timeout_seconds: float = 300.0) -> bool:
    """Block until all workers have exited.  Returns True if drained, False on timeout."""
    deadline = time.time() + timeout_seconds
    interval = 2.0
    while time.time() < deadline:
        alive = _count_alive_worker_processes(config)
        inflight = client.hlen(_inflight_key(config))
        if alive == 0 and inflight == 0:
            return True
        LOGGER.info(
            "Waiting for workers to drain | alive_workers=%s inflight=%s",
            alive,
            inflight,
        )
        print(
            f"  stopping: {alive} worker(s) alive, {inflight} job(s) in-flight …",
            flush=True,
        )
        time.sleep(interval)
    return False


def stop_queue_processes(
    config: NovelConfig | QueueConfig,
    pids: list[int] | None = None,
    roles: list[str] | None = None,
    *,
    force: bool = False,
) -> int:
    """Stop queue-related processes.

    Default (graceful): sets a stopping signal so workers finish their current
    sub-phase (translate chapter, repair, glossary) then exit.  The supervisor
    stops spawning new workers and exits once all workers have drained.

    --force: immediately SIGTERM all matching processes.
    --pid:   immediately SIGTERM the specified PIDs.
    """
    # Direct PID kill — always immediate.
    if pids:
        for pid in pids:
            subprocess.run(["kill", str(pid)], check=False)
            LOGGER.info("Sent SIGTERM to pid=%s", pid)
        return 0

    selected = {r.strip() for r in (roles or []) if r.strip()} or None

    # --force: immediate kill.
    if force:
        _force_stop_queue_processes(config, roles=selected)
        client = _client(config)
        _clear_stopping(config, client)
        _requeue_stale_inflight(config, client)
        return 0

    # Graceful path ---------------------------------------------------------
    client = _client(config)
    _set_stopping(config, client)
    print(
        "Graceful stop initiated. Workers will finish current jobs then exit …",
        flush=True,
    )

    drained = _wait_for_workers_drain(config, client, timeout_seconds=300.0)
    if not drained:
        print(
            "Timeout waiting for workers to drain. Sending SIGTERM to remaining processes.",
            file=sys.stderr,
            flush=True,
        )
        _force_stop_queue_processes(config, roles=selected)
        _clear_stopping(config, client)
        return 1

    # Workers are gone — kill supervisor and monitor.
    for role_pattern in ("novel_tts .* queue supervisor", "novel_tts .* queue monitor"):
        subprocess.run(["pkill", "-f", role_pattern], check=False)

    _clear_stopping(config, client)
    print("All queue processes stopped gracefully.", flush=True)
    LOGGER.info("Graceful stop complete")
    return 0
