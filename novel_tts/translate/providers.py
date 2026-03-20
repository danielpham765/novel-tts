from __future__ import annotations

import json
import math
import os
import re
from pathlib import Path
import requests
import sys
import time
import uuid
import logging
import redis

from novel_tts.common.errors import RateLimitExceededError
from novel_tts.config.models import NovelConfig, ProxyGatewayConfig, RedisConfig
from novel_tts.net import proxy_gateway as proxy_gateway_mod
from novel_tts.quota.client import CentralQuotaClient

LOGGER = logging.getLogger(__name__)
QUOTA_LOGGER = logging.getLogger("quota.client")

_HAN_REGEX = re.compile(r"[\u4e00-\u9fff]")


class TranslationProvider:
    def generate(self, model: str, prompt: str, system_prompt: str = "") -> str:
        raise NotImplementedError


class PromptBlockedError(RuntimeError):
    def __init__(self, reason: str, payload: dict) -> None:
        self.reason = reason
        self.payload = payload
        super().__init__(f"Prompt blocked by provider: {reason}")


_RATE_LIMIT_CLIENT = None
_RATE_LIMIT_CONFIGS = None
_RATE_LIMIT_CONFIGS_RAW = None


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _keys_file_path(config: NovelConfig | None = None) -> Path:
    if config is not None:
        return config.storage.root / ".secrets" / "gemini-keys.txt"
    return _repo_root() / ".secrets" / "gemini-keys.txt"


def _is_queue_worker_env() -> bool:
    key_prefix = os.environ.get("GEMINI_RATE_LIMIT_KEY_PREFIX", "").strip()
    if key_prefix:
        return True
    quota_mode = os.environ.get("NOVEL_TTS_QUOTA_MODE", "wait").strip().lower()
    central = os.environ.get("NOVEL_TTS_CENTRAL_QUOTA", "").strip().lower() in {"1", "true", "yes", "on"}
    max_wait_raw = os.environ.get("NOVEL_TTS_QUOTA_MAX_WAIT_SECONDS", "").strip()
    try:
        max_wait_seconds = float(max_wait_raw) if max_wait_raw else 0.0
    except ValueError:
        max_wait_seconds = 0.0
    return bool(central and quota_mode == "raise" and max_wait_seconds <= 0.0)


def is_queue_worker_env() -> bool:
    return _is_queue_worker_env()


def _resolve_gemini_api_key(*, config: NovelConfig | None = None) -> str:
    env_key = os.environ.get("GEMINI_API_KEY", "").strip()
    if env_key:
        return env_key
    if _is_queue_worker_env():
        return ""
    path = _keys_file_path(config)
    if not path.exists():
        return ""
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            key = line.strip()
            if key and not key.startswith("#"):
                return key
    except Exception:
        return ""
    return ""


def _get_rate_limit_client():
    global _RATE_LIMIT_CLIENT
    if _RATE_LIMIT_CLIENT is not None:
        return _RATE_LIMIT_CLIENT
    host = os.environ.get("GEMINI_REDIS_HOST", "").strip()
    port = os.environ.get("GEMINI_REDIS_PORT", "").strip()
    database = os.environ.get("GEMINI_REDIS_DB", "").strip()
    if not host or not port:
        return None
    try:
        import redis

        _RATE_LIMIT_CLIENT = redis.Redis(
            host=host,
            port=int(port),
            db=int(database or "0"),
            decode_responses=True,
        )
    except Exception:
        _RATE_LIMIT_CLIENT = None
    return _RATE_LIMIT_CLIENT


def _get_rate_limit_configs() -> dict[str, dict]:
    global _RATE_LIMIT_CONFIGS
    global _RATE_LIMIT_CONFIGS_RAW
    if _RATE_LIMIT_CONFIGS is not None:
        raw = os.environ.get("GEMINI_MODEL_CONFIGS_JSON", "").strip()
        if raw == (_RATE_LIMIT_CONFIGS_RAW or ""):
            return _RATE_LIMIT_CONFIGS
    raw = os.environ.get("GEMINI_MODEL_CONFIGS_JSON", "").strip()
    if not raw:
        _RATE_LIMIT_CONFIGS = {}
        _RATE_LIMIT_CONFIGS_RAW = ""
        return _RATE_LIMIT_CONFIGS
    try:
        payload = json.loads(raw)
        _RATE_LIMIT_CONFIGS = payload if isinstance(payload, dict) else {}
        _RATE_LIMIT_CONFIGS_RAW = raw
    except Exception:
        _RATE_LIMIT_CONFIGS = {}
        _RATE_LIMIT_CONFIGS_RAW = raw
    return _RATE_LIMIT_CONFIGS


def _redis_now_seconds(client) -> float:
    try:
        sec, usec = client.time()
        return float(sec) + float(usec) / 1_000_000.0
    except Exception:
        return time.time()


def _estimate_gemini_tokens(prompt: str, system_prompt: str = "") -> int:
    """
    Estimate tokens for quota gating (TPM).

    Notes:
    - Provider dashboards often report *input* tokens per minute, while some limits count input+output.
    - Default behavior here is to gate mostly on input tokens, with an optional output reserve ratio.
      Tune via env vars if needed.
    """

    text = f"{system_prompt}\n\n{prompt}".strip()
    chars = len(text)
    if chars <= 0:
        return 1

    chars_per_token_raw = os.environ.get("NOVEL_TTS_GEMINI_TPM_CHARS_PER_TOKEN", "").strip()
    if chars_per_token_raw:
        try:
            chars_per_token = max(0.8, float(chars_per_token_raw))
        except ValueError:
            chars_per_token = 0.0
    else:
        han = len(_HAN_REGEX.findall(text))
        han_ratio = han / max(1, chars)
        # Heuristic: CJK-heavy prompts tokenize denser than Latin-heavy prompts.
        if han_ratio >= 0.12:
            chars_per_token = 2.0
        elif han_ratio >= 0.03:
            chars_per_token = 2.6
        else:
            chars_per_token = 4.0

    input_tokens = max(1, int(math.ceil(chars / max(0.8, chars_per_token))))

    output_ratio_raw = os.environ.get("NOVEL_TTS_GEMINI_TPM_OUTPUT_RESERVE_RATIO", "").strip()
    min_out_raw = os.environ.get("NOVEL_TTS_GEMINI_TPM_OUTPUT_RESERVE_MIN", "").strip()
    try:
        if output_ratio_raw:
            output_ratio = float(output_ratio_raw)
        else:
            # Central quota mode is more sensitive to token underestimation because it gates concurrency.
            # Default to reserving output tokens to approximate "input+output TPM" style upstream limits.
            central = os.environ.get("NOVEL_TTS_CENTRAL_QUOTA", "").strip().lower() in {"1", "true", "yes", "on"}
            output_ratio = 1.0 if central else 0.0
    except ValueError:
        output_ratio = 0.0
    try:
        if min_out_raw:
            min_out = int(min_out_raw)
        else:
            central = os.environ.get("NOVEL_TTS_CENTRAL_QUOTA", "").strip().lower() in {"1", "true", "yes", "on"}
            min_out = 256 if central else 0
    except ValueError:
        min_out = 0

    output_reserve = 0
    if output_ratio > 0:
        output_reserve = max(min_out, int(math.ceil(input_tokens * output_ratio)))

    multiplier_raw = os.environ.get("NOVEL_TTS_GEMINI_TPM_SAFETY_MULTIPLIER", "").strip()
    try:
        multiplier = float(multiplier_raw) if multiplier_raw else 1.05
    except ValueError:
        multiplier = 1.05
    multiplier = max(1.0, multiplier)

    return max(1, int(math.ceil((input_tokens + output_reserve) * multiplier)))

def _env_int(name: str) -> int | None:
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return None
    try:
        value = int(raw)
    except Exception:
        return None
    return value if value > 0 else None


def _wait_seconds_until_rpm_allows(active_members_scored: list[tuple[str, float]], *, now: float, rpm: int) -> float:
    if rpm <= 0:
        return 0.0
    current_requests = len(active_members_scored)
    if current_requests < rpm:
        return 0.0
    # Need enough members to expire so that current_requests becomes (rpm - 1) or lower.
    need_drop = current_requests - (rpm - 1)
    if need_drop <= 0:
        return 0.0
    idx = min(len(active_members_scored) - 1, need_drop - 1)
    _member, score = active_members_scored[idx]
    expiry = float(score) + 60.0
    return max(0.05, expiry - now + 0.05)


def _wait_seconds_until_tpm_allows(
    active_members_scored: list[tuple[str, float]],
    token_map: dict[str, str],
    *,
    now: float,
    tpm: int,
    estimated_tokens: int,
) -> float:
    if tpm <= 0:
        return 0.0
    if estimated_tokens > tpm:
        # This request can never fit into the TPM window.
        raise RateLimitExceededError(
            f"Gemini quota exceeded (reasons=TPM estimated_tokens={estimated_tokens} tpm_limit={tpm})"
        )
    current_tokens = 0
    for member, _score in active_members_scored:
        try:
            current_tokens += int(token_map.get(member, "0"))
        except (TypeError, ValueError):
            continue
    if current_tokens + estimated_tokens <= tpm:
        return 0.0
    need_reduce = (current_tokens + estimated_tokens) - tpm
    reduced = 0
    cutoff_score: float | None = None
    for member, score in active_members_scored:
        try:
            reduced += int(token_map.get(member, "0"))
        except (TypeError, ValueError):
            continue
        if reduced >= need_reduce:
            cutoff_score = float(score)
            break
    if cutoff_score is None:
        # Be conservative: if token map is missing, wait for the oldest to expire.
        cutoff_score = float(active_members_scored[0][1]) if active_members_scored else now
    expiry = cutoff_score + 60.0
    return max(0.05, expiry - now + 0.05)


def _acquire_gemini_rate_slot(model: str, estimated_tokens: int) -> None:
    model_cfg = _get_rate_limit_configs().get(model, {})
    rpm_raw = str(model_cfg.get("rpm_limit", os.environ.get("GEMINI_RATE_LIMIT_RPM", ""))).strip()
    tpm_raw = str(model_cfg.get("tpm_limit", os.environ.get("GEMINI_RATE_LIMIT_TPM", ""))).strip()
    rpd_raw = str(model_cfg.get("rpd_limit", os.environ.get("GEMINI_RATE_LIMIT_RPD", ""))).strip()
    key_prefix = os.environ.get("GEMINI_RATE_LIMIT_KEY_PREFIX", "").strip()
    key = f"{key_prefix}:{model}" if key_prefix else os.environ.get("GEMINI_RATE_LIMIT_KEY", "").strip()
    if (not rpm_raw and not tpm_raw and not rpd_raw) or not key:
        return
    try:
        rpm = int(rpm_raw) if rpm_raw else 0
    except ValueError:
        rpm = 0
    try:
        tpm = int(tpm_raw) if tpm_raw else 0
    except ValueError:
        tpm = 0
    try:
        rpd = int(rpd_raw) if rpd_raw else 0
    except ValueError:
        rpd = 0
    if rpm <= 0 and tpm <= 0 and rpd <= 0:
        return
    client = _get_rate_limit_client()
    if client is None:
        return
    redis_key = f"{key}:quota:reqs"
    token_key = f"{key}:quota:tokens"
    daily_key = f"{key}:quota:daily_reqs"
    member = f"{time.time():.6f}:{os.getpid()}:{uuid.uuid4().hex}"
    quota_mode = os.environ.get("NOVEL_TTS_QUOTA_MODE", "wait").strip().lower()
    max_wait_raw = os.environ.get("NOVEL_TTS_QUOTA_MAX_WAIT_SECONDS", "").strip()
    try:
        max_wait_seconds = float(max_wait_raw) if max_wait_raw else 0.0
    except ValueError:
        max_wait_seconds = 0.0
    started = time.time()
    while True:
        now = _redis_now_seconds(client)
        window_start = now - 60.0
        day_window_start = now - 86400.0
        try:
            with client.pipeline() as pipe:
                pipe.watch(redis_key, token_key, daily_key)
                stale_members = pipe.zrangebyscore(redis_key, 0, window_start)
                active_members_scored = pipe.zrangebyscore(redis_key, window_start, "+inf", withscores=True)
                token_map = pipe.hgetall(token_key)
                stale_daily_members = pipe.zrangebyscore(daily_key, 0, day_window_start)
                daily_count = pipe.zcount(daily_key, day_window_start, "+inf")
                current_requests = len(active_members_scored)
                current_tokens = 0
                for active_member, _score in active_members_scored:
                    try:
                        current_tokens += int(token_map.get(active_member, "0"))
                    except (TypeError, ValueError):
                        continue
                allow_rpm = rpm <= 0 or current_requests < rpm
                allow_tpm = tpm <= 0 or (current_tokens + estimated_tokens) <= tpm
                allow_rpd = rpd <= 0 or daily_count < rpd
                if allow_rpm and allow_tpm and allow_rpd:
                    pipe.multi()
                    if stale_members:
                        pipe.zrem(redis_key, *stale_members)
                        pipe.hdel(token_key, *stale_members)
                    if stale_daily_members:
                        pipe.zrem(daily_key, *stale_daily_members)
                    pipe.zadd(redis_key, {member: now})
                    pipe.hset(token_key, member, estimated_tokens)
                    pipe.zadd(daily_key, {member: now})
                    pipe.expire(redis_key, 120)
                    pipe.expire(token_key, 120)
                    pipe.expire(daily_key, 172800)
                    pipe.execute()
                    return
                oldest_daily = None
                if rpd > 0 and daily_count >= rpd:
                    need_drop_daily = int(daily_count) - (int(rpd) - 1)
                    if need_drop_daily <= 0:
                        need_drop_daily = 1
                    oldest_daily = pipe.zrangebyscore(
                        daily_key,
                        day_window_start,
                        "+inf",
                        start=max(0, need_drop_daily - 1),
                        num=1,
                        withscores=True,
                    )
                pipe.unwatch()
            wait_rpm = 0.0
            if rpm > 0 and current_requests >= rpm and active_members_scored:
                wait_rpm = _wait_seconds_until_rpm_allows(active_members_scored, now=now, rpm=rpm)
            wait_tpm = 0.0
            if tpm > 0 and (current_tokens + estimated_tokens) > tpm and active_members_scored:
                wait_tpm = _wait_seconds_until_tpm_allows(
                    active_members_scored,
                    token_map,
                    now=now,
                    tpm=tpm,
                    estimated_tokens=estimated_tokens,
                )
            wait_rpd = 0.0
            if rpd > 0 and daily_count >= rpd and oldest_daily:
                try:
                    wait_rpd = max(1.0, (float(oldest_daily[0][1]) + 86400.0) - now + 0.05)
                except Exception:
                    wait_rpd = 60.0
            wait_seconds = max(wait_rpm, wait_tpm, wait_rpd, 0.25)
            reasons: list[str] = []
            if wait_rpm > 0:
                reasons.append("RPM")
            if wait_tpm > 0:
                reasons.append("TPM")
            if wait_rpd > 0:
                reasons.append("RPD")
            reason_text = ",".join(reasons) if reasons else "UNKNOWN"
            if wait_seconds >= 3.0:
                LOGGER.info(
                    "Gemini quota wait %.1fs | model=%s reasons=%s reqs=%s/%s tokens=%s+%s/%s daily=%s/%s",
                    wait_seconds,
                    model,
                    reason_text,
                    current_requests,
                    rpm if rpm > 0 else "-",
                    current_tokens,
                    estimated_tokens,
                    tpm if tpm > 0 else "-",
                    daily_count,
                    rpd if rpd > 0 else "-",
                )
            if quota_mode == "raise":
                raise RateLimitExceededError(
                    f"Gemini quota exceeded (model={model} reasons={reason_text} suggested_wait={wait_seconds:.2f}s)"
                )
            if quota_mode == "wait_then_raise":
                # Allow short waits to smooth bursts, but release quickly when the wait is long.
                if max_wait_seconds <= 0:
                    raise RateLimitExceededError(
                        f"Gemini quota exceeded (model={model} reasons={reason_text} suggested_wait={wait_seconds:.2f}s)"
                    )
                waited = max(0.0, time.time() - started)
                if waited + wait_seconds > max_wait_seconds:
                    raise RateLimitExceededError(
                        f"Gemini quota exceeded (model={model} reasons={reason_text} suggested_wait={wait_seconds:.2f}s)"
                    )
            time.sleep(wait_seconds)
        except redis.exceptions.WatchError:
            time.sleep(0.01)
            continue
        except RateLimitExceededError:
            raise
        except Exception as e:
            LOGGER.warning("Redis error in rate limit tracking: %s", e)
            return


def _record_gemini_api_attempt(model: str) -> None:
    """
    Record a single HTTP attempt to the Gemini API in a 60s rolling window.

    This is used by `novel-tts ai-key ps` to report "api call count" (including retries).

    Key shape (when running in queue worker mode):
      {GEMINI_RATE_LIMIT_KEY_PREFIX}:{model}:api:reqs

    If Redis isn't configured via GEMINI_REDIS_* or if the key prefix is missing, this is a no-op.
    """

    key_prefix = os.environ.get("GEMINI_RATE_LIMIT_KEY_PREFIX", "").strip()
    key = f"{key_prefix}:{model}" if key_prefix else os.environ.get("GEMINI_RATE_LIMIT_KEY", "").strip()
    if not key:
        return

    client = _get_rate_limit_client()
    if client is None:
        return

    now = _redis_now_seconds(client)
    window_start = now - 60.0
    api_key = f"{key}:api:reqs"
    member = f"{now:.6f}:{os.getpid()}:{uuid.uuid4().hex}"
    try:
        with client.pipeline() as pipe:
            pipe.zremrangebyscore(api_key, 0, window_start)
            pipe.zadd(api_key, {member: now})
            pipe.expire(api_key, 120)
            pipe.execute()
    except Exception as exc:
        # Never fail the translation call due to stats tracking.
        LOGGER.debug("Failed to record Gemini api attempt: %s", exc)


def _record_gemini_429_attempt(model: str) -> None:
    """
    Record a single HTTP 429 response from the Gemini API in a 60s rolling window.

    Key shape:
      {GEMINI_RATE_LIMIT_KEY_PREFIX}:{model}:api:429
      OR {GEMINI_RATE_LIMIT_KEY}:api:429
    """

    key_prefix = os.environ.get("GEMINI_RATE_LIMIT_KEY_PREFIX", "").strip()
    key = f"{key_prefix}:{model}" if key_prefix else os.environ.get("GEMINI_RATE_LIMIT_KEY", "").strip()
    if not key:
        return

    client = _get_rate_limit_client()
    if client is None:
        return

    now = _redis_now_seconds(client)
    window_start = now - 60.0
    stat_key = f"{key}:api:429"
    member = f"{now:.6f}:{os.getpid()}:{uuid.uuid4().hex}"
    try:
        with client.pipeline() as pipe:
            pipe.zremrangebyscore(stat_key, 0, window_start)
            pipe.zadd(stat_key, {member: now})
            pipe.expire(stat_key, 120)
            pipe.execute()
    except Exception as exc:
        LOGGER.debug("Failed to record Gemini 429 attempt: %s", exc)


def _record_gemini_api_call(model: str) -> None:
    """
    Record a single logical Gemini API call (one generate() invocation that proceeds to send HTTP)
    in a 60s rolling window.

    Key shape:
      {GEMINI_RATE_LIMIT_KEY_PREFIX}:{model}:api:calls
      OR {GEMINI_RATE_LIMIT_KEY}:api:calls
    """

    key_prefix = os.environ.get("GEMINI_RATE_LIMIT_KEY_PREFIX", "").strip()
    key = f"{key_prefix}:{model}" if key_prefix else os.environ.get("GEMINI_RATE_LIMIT_KEY", "").strip()
    if not key:
        return

    client = _get_rate_limit_client()
    if client is None:
        return

    now = _redis_now_seconds(client)
    window_start = now - 60.0
    stat_key = f"{key}:api:calls"
    member = f"{now:.6f}:{os.getpid()}:{uuid.uuid4().hex}"
    try:
        with client.pipeline() as pipe:
            pipe.zremrangebyscore(stat_key, 0, window_start)
            pipe.zadd(stat_key, {member: now})
            pipe.expire(stat_key, 120)
            pipe.execute()
    except Exception as exc:
        LOGGER.debug("Failed to record Gemini api call: %s", exc)


def _record_gemini_llm_call(model: str) -> None:
    """
    Record a single LLM attempt (one outbound HTTP attempt) in a 60s rolling window.

    Key shape:
      {GEMINI_RATE_LIMIT_KEY_PREFIX}:{model}:llm:reqs
      OR {GEMINI_RATE_LIMIT_KEY}:llm:reqs
    """

    key_prefix = os.environ.get("GEMINI_RATE_LIMIT_KEY_PREFIX", "").strip()
    key = f"{key_prefix}:{model}" if key_prefix else os.environ.get("GEMINI_RATE_LIMIT_KEY", "").strip()
    if not key:
        return

    client = _get_rate_limit_client()
    if client is None:
        return

    now = _redis_now_seconds(client)
    window_start = now - 60.0
    llm_key = f"{key}:llm:reqs"
    member = f"{now:.6f}:{os.getpid()}:{uuid.uuid4().hex}"
    try:
        with client.pipeline() as pipe:
            pipe.zremrangebyscore(llm_key, 0, window_start)
            pipe.zadd(llm_key, {member: now})
            pipe.expire(llm_key, 120)
            pipe.execute()
    except Exception as exc:
        LOGGER.debug("Failed to record Gemini llm call: %s", exc)


class GeminiHttpProvider(TranslationProvider):
    def __init__(
        self,
        *,
        config: NovelConfig | None = None,
        proxy_gateway: ProxyGatewayConfig | None = None,
        redis_cfg: RedisConfig | None = None,
    ) -> None:
        self.config = config
        self.proxy_gateway = proxy_gateway or ProxyGatewayConfig()
        self.redis_cfg = redis_cfg

    def generate(self, model: str, prompt: str, system_prompt: str = "") -> str:
        api_key = _resolve_gemini_api_key(config=self.config)
        if not api_key:
            key_path = _keys_file_path(self.config)
            if _is_queue_worker_env():
                raise RuntimeError("Missing GEMINI_API_KEY")
            raise RuntimeError(f"Missing GEMINI_API_KEY and no keys found in {key_path}")
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
        url_with_key = f"{url}?key={api_key}"
        body = {
            "contents": [{"parts": [{"text": f"{system_prompt}\n\n{prompt}".strip()}]}],
            "generationConfig": {"temperature": 0.2, "topP": 0.9},
        }
        estimated_tokens = _estimate_gemini_tokens(prompt, system_prompt)
        _record_gemini_api_call(model)
        key_index = _env_int("NOVEL_TTS_KEY_INDEX")

        quota_mode = os.environ.get("NOVEL_TTS_QUOTA_MODE", "wait").strip().lower()
        max_wait_raw = os.environ.get("NOVEL_TTS_QUOTA_MAX_WAIT_SECONDS", "").strip()
        try:
            max_wait_seconds = float(max_wait_raw) if max_wait_raw else 0.0
        except ValueError:
            max_wait_seconds = 0.0
        is_queue_worker_mode = (quota_mode == "raise") and (max_wait_seconds <= 0.0)
        timeout_wait_raw = os.environ.get("NOVEL_TTS_UPSTREAM_TIMEOUT_SUGGESTED_WAIT_SECONDS", "").strip()
        try:
            timeout_suggested_wait_seconds = float(timeout_wait_raw) if timeout_wait_raw else 15.0
        except ValueError:
            timeout_suggested_wait_seconds = 15.0

        central_quota = CentralQuotaClient()
        key_prefix = os.environ.get("GEMINI_RATE_LIMIT_KEY_PREFIX", "").strip()
        use_central_quota = bool(is_queue_worker_mode and central_quota.enabled() and key_prefix)
        model_cfg = _get_rate_limit_configs().get(model, {}) if isinstance(_get_rate_limit_configs(), dict) else {}
        try:
            rpm_limit_cfg = int(model_cfg.get("rpm_limit") or 0)
        except Exception:
            rpm_limit_cfg = 0
        try:
            tpm_limit_cfg = int(model_cfg.get("tpm_limit") or 0)
        except Exception:
            tpm_limit_cfg = 0
        try:
            rpd_limit_cfg = int(model_cfg.get("rpd_limit") or 0)
        except Exception:
            rpd_limit_cfg = 0

        if is_queue_worker_mode and central_quota.enabled() and not key_prefix:
            raise RuntimeError("Central quota is enabled but GEMINI_RATE_LIMIT_KEY_PREFIX is missing")

        # Central quota path: exactly one HTTP attempt per generate() in queue worker mode.
        if use_central_quota:
            grant = central_quota.acquire(
                key_prefix=key_prefix,
                model=model,
                tokens=estimated_tokens,
                rpm_limit=rpm_limit_cfg,
                tpm_limit=tpm_limit_cfg,
                rpd_limit=rpd_limit_cfg,
            )
            try:
                _record_gemini_llm_call(model)
                _record_gemini_api_attempt(model)
                response = proxy_gateway_mod.request(
                    "POST",
                    url_with_key,
                    headers={"Content-Type": "application/json"},
                    body=body,
                    cfg=self.proxy_gateway,
                    key_index=key_index,
                    redis_cfg=self.redis_cfg,
                    timeout_seconds=90,
                )
                if response.status_code == 429:
                    _record_gemini_429_attempt(model)
                    retry_after = (response.headers.get("Retry-After") or "").strip()
                    message = ""
                    status = ""
                    try:
                        payload = response.json() if response.content else {}
                    except Exception:
                        payload = {}
                    if isinstance(payload, dict):
                        err = payload.get("error") or {}
                        if isinstance(err, dict):
                            message = str(err.get("message") or "").strip()
                            status = str(err.get("status") or "").strip()
                    if not message:
                        try:
                            message = (response.text or "").strip()
                        except Exception:
                            message = ""
                    if len(message) > 240:
                        message = message[:237] + "..."
                    try:
                        snap = central_quota.snapshot_usage(key_prefix=key_prefix, model=model)
                    except Exception:
                        snap = {"rpm_used_1m": 0, "tpm_used_1m": 0, "rpd_used_1d": 0}
                    novel_id = (os.environ.get("NOVEL_TTS_NOVEL_ID") or "").strip()
                    key_str = (os.environ.get("NOVEL_TTS_KEY_INDEX") or "").strip()
                    if not novel_id or not key_str:
                        # Fallback: parse from key_prefix tail "...:<novel_id>:k{idx}"
                        parts = [p for p in str(key_prefix).split(":") if p]
                        if len(parts) >= 2 and parts[-1].startswith("k"):
                            key_str = key_str or parts[-1][1:]
                            novel_id = novel_id or parts[-2]

                    QUOTA_LOGGER.warning(
                        "Gemini 429 | novel=%s key=%s model=%s pid=%s req=%s tokens=%s rpm=%s/%s tpm=%s/%s rpd=%s/%s retry_after=%s status=%s message=%r",
                        novel_id or "-",
                        key_str or "-",
                        model,
                        os.getpid(),
                        "1",
                        f"{int(estimated_tokens):,}",
                        int(snap.get("rpm_used_1m") or 0),
                        rpm_limit_cfg if rpm_limit_cfg > 0 else "-",
                        f"{int(snap.get('tpm_used_1m') or 0):,}",
                        f"{int(tpm_limit_cfg):,}" if tpm_limit_cfg > 0 else "-",
                        f"{int(snap.get('rpd_used_1d') or 0):,}",
                        f"{int(rpd_limit_cfg):,}" if rpd_limit_cfg > 0 else "-",
                        retry_after or "-",
                        status or "-",
                        message or "",
                    )

                    penalty_seconds = 10.0
                    if retry_after:
                        try:
                            penalty_seconds = float(retry_after)
                        except Exception:
                            penalty_seconds = 10.0
                    central_quota.penalize(key_prefix=key_prefix, model=model, seconds=penalty_seconds)
                    meta = []
                    if status:
                        meta.append(f"status={status}")
                    if retry_after:
                        meta.append(f"retry_after={retry_after}")
                    meta_text = (" " + " ".join(meta)) if meta else ""
                    detail_text = f" message={message!r}" if message else ""
                    raise RateLimitExceededError(f"Gemini HTTP 429 (model={model}){meta_text}{detail_text}")
                response.raise_for_status()
                payload = response.json()
                prompt_feedback = payload.get("promptFeedback") or {}
                block_reason = prompt_feedback.get("blockReason")
                if block_reason:
                    raise PromptBlockedError(block_reason, payload)
                used_tokens = 0
                try:
                    usage = payload.get("usageMetadata") or {}
                    if isinstance(usage, dict):
                        used_tokens = int(usage.get("totalTokenCount") or 0)
                        if used_tokens <= 0:
                            used_tokens = int(usage.get("promptTokenCount") or 0) + int(usage.get("candidatesTokenCount") or 0)
                except Exception:
                    used_tokens = 0
                candidates = payload.get("candidates") or []
                if not candidates:
                    raise RuntimeError(f"Empty Gemini response: {payload}")
                parts = candidates[0].get("content", {}).get("parts", [])
                text = "".join(part.get("text", "") for part in parts if isinstance(part, dict)).strip()
                if not text:
                    raise RuntimeError(f"Empty Gemini parts: {payload}")
                central_quota.commit(key_prefix=key_prefix, model=model, grant=grant, success=True, used_tokens=used_tokens)
                return text
            except Exception as exc:
                try:
                    central_quota.commit(key_prefix=key_prefix, model=model, grant=grant, success=False)
                except Exception:
                    pass
                # Preserve existing queue-mode behavior: map timeouts/429 upstream to RateLimitExceededError.
                if isinstance(exc, PromptBlockedError):
                    raise
                if isinstance(exc, RateLimitExceededError):
                    raise
                if isinstance(exc, (requests.exceptions.Timeout, requests.exceptions.ConnectionError)):
                    raise RateLimitExceededError(
                        f"Gemini upstream timeout (model={model} suggested_wait={timeout_suggested_wait_seconds:.2f}s): {exc}"
                    )
                raise

        generic_attempt = 0
        while True:
            try:
                # Acquire a slot for each HTTP attempt (including any retries within this method).
                _acquire_gemini_rate_slot(model, estimated_tokens)
                _record_gemini_llm_call(model)
                _record_gemini_api_attempt(model)
                response = proxy_gateway_mod.request(
                    "POST",
                    url_with_key,
                    headers={"Content-Type": "application/json"},
                    body=body,
                    cfg=self.proxy_gateway,
                    key_index=key_index,
                    redis_cfg=self.redis_cfg,
                    timeout_seconds=90,
                )
                if response.status_code == 429:
                    _record_gemini_429_attempt(model)
                    # Do not retry 429 here; let the queue worker release/requeue the job to shift keys/workers.
                    retry_after = (response.headers.get("Retry-After") or "").strip()
                    message = ""
                    status = ""
                    try:
                        payload = response.json() if response.content else {}
                    except Exception:
                        payload = {}
                    if isinstance(payload, dict):
                        err = payload.get("error") or {}
                        if isinstance(err, dict):
                            message = str(err.get("message") or "").strip()
                            status = str(err.get("status") or "").strip()
                    if not message:
                        try:
                            message = (response.text or "").strip()
                        except Exception:
                            message = ""
                    # Keep logs compact: avoid large bodies.
                    if len(message) > 240:
                        message = message[:237] + "..."
                    meta = []
                    if status:
                        meta.append(f"status={status}")
                    if retry_after:
                        meta.append(f"retry_after={retry_after}")
                    meta_text = (" " + " ".join(meta)) if meta else ""
                    detail_text = f" message={message!r}" if message else ""
                    raise RateLimitExceededError(f"Gemini HTTP 429 (model={model}){meta_text}{detail_text}")
                response.raise_for_status()
                payload = response.json()
                prompt_feedback = payload.get("promptFeedback") or {}
                block_reason = prompt_feedback.get("blockReason")
                if block_reason:
                    raise PromptBlockedError(block_reason, payload)
                candidates = payload.get("candidates") or []
                if not candidates:
                    raise RuntimeError(f"Empty Gemini response: {payload}")
                parts = candidates[0].get("content", {}).get("parts", [])
                text = "".join(part.get("text", "") for part in parts if isinstance(part, dict)).strip()
                if not text:
                    raise RuntimeError(f"Empty Gemini parts: {payload}")
                return text
            except PromptBlockedError:
                raise
            except RateLimitExceededError:
                raise
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
                # In queue worker mode, avoid spending a long time retrying within a single worker process.
                # Release the job back to the supervisor so another key/worker (or a later time slice) can pick it up.
                if is_queue_worker_mode:
                    raise RateLimitExceededError(
                        f"Gemini upstream timeout (model={model} suggested_wait={timeout_suggested_wait_seconds:.2f}s): {exc}"
                    )
                generic_attempt += 1
                LOGGER.warning("Gemini API generation error (attempt %d/12): %s", generic_attempt, exc)
                if generic_attempt >= 12:
                    raise
                time.sleep(5 + (generic_attempt - 1) * 5)
            except Exception as e:
                generic_attempt += 1
                LOGGER.warning("Gemini API generation error (attempt %d/12): %s", generic_attempt, e)
                if generic_attempt >= 12:
                    raise
                time.sleep(5 + (generic_attempt - 1) * 5)


class OpenAIChatProvider(TranslationProvider):
    def __init__(
        self,
        *,
        proxy_gateway: ProxyGatewayConfig | None = None,
        redis_cfg: RedisConfig | None = None,
    ) -> None:
        self.proxy_gateway = proxy_gateway or ProxyGatewayConfig()
        self.redis_cfg = redis_cfg

    def generate(self, model: str, prompt: str, system_prompt: str = "") -> str:
        api_key = (os.environ.get("OPENAI_API_KEY") or "").strip()
        if not api_key:
            raise RuntimeError("Missing OPENAI_API_KEY")
        url = "https://api.openai.com/v1/chat/completions"
        body = {
            "model": model,
            "messages": [
                {"role": "system", "content": system_prompt or "You are a helpful translator."},
                {"role": "user", "content": prompt},
            ],
        }
        key_index = _env_int("NOVEL_TTS_KEY_INDEX")
        response = proxy_gateway_mod.request(
            "POST",
            url,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {api_key}",
            },
            body=body,
            cfg=self.proxy_gateway,
            key_index=key_index,
            redis_cfg=self.redis_cfg,
            timeout_seconds=90,
        )
        response.raise_for_status()
        payload = response.json() if response.content else {}
        choices = payload.get("choices") if isinstance(payload, dict) else None
        if not choices:
            raise RuntimeError(f"Empty OpenAI response: {payload}")
        msg = choices[0].get("message") if isinstance(choices[0], dict) else None
        text = (msg or {}).get("content") if isinstance(msg, dict) else ""
        return (text or "").strip()


def get_translation_provider(provider_name: str, *, config: NovelConfig | None = None) -> TranslationProvider:
    proxy_cfg = config.proxy_gateway if config is not None else ProxyGatewayConfig()
    redis_cfg = config.queue.redis if (config is not None and getattr(config, "queue", None) is not None) else None
    if provider_name == "gemini_http":
        return GeminiHttpProvider(config=config, proxy_gateway=proxy_cfg, redis_cfg=redis_cfg)
    if provider_name == "openai_chat":
        return OpenAIChatProvider(proxy_gateway=proxy_cfg, redis_cfg=redis_cfg)
    if provider_name not in {"gemini_http", "openai_chat"}:
        raise ValueError(f"Unsupported translation provider: {provider_name}")
    raise RuntimeError(f"Unhandled translation provider: {provider_name}")
