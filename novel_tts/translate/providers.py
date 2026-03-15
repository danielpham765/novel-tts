from __future__ import annotations

import json
import math
import os
import requests
import sys
import time
import uuid
import logging
import redis

from novel_tts.common.errors import RateLimitExceededError

LOGGER = logging.getLogger(__name__)


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
    if _RATE_LIMIT_CONFIGS is not None:
        return _RATE_LIMIT_CONFIGS
    raw = os.environ.get("GEMINI_MODEL_CONFIGS_JSON", "").strip()
    if not raw:
        _RATE_LIMIT_CONFIGS = {}
        return _RATE_LIMIT_CONFIGS
    try:
        payload = json.loads(raw)
        _RATE_LIMIT_CONFIGS = payload if isinstance(payload, dict) else {}
    except Exception:
        _RATE_LIMIT_CONFIGS = {}
    return _RATE_LIMIT_CONFIGS


def _estimate_gemini_tokens(prompt: str, system_prompt: str = "") -> int:
    chars = len((system_prompt or "").strip()) + len((prompt or "").strip())
    # Conservative estimate for zh/vi prompts plus model output reserve.
    input_tokens = max(1, math.ceil(chars / 2.2))
    output_reserve = max(256, math.ceil(input_tokens * 0.8))
    return input_tokens + output_reserve


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
        now = time.time()
        window_start = now - 60.0
        day_window_start = now - 86400.0
        try:
            with client.pipeline() as pipe:
                pipe.watch(redis_key, token_key, daily_key)
                stale_members = pipe.zrangebyscore(redis_key, 0, window_start)
                active_members = pipe.zrangebyscore(redis_key, window_start, "+inf")
                oldest_active = pipe.zrangebyscore(redis_key, window_start, "+inf", start=0, num=1, withscores=True)
                token_map = pipe.hgetall(token_key)
                stale_daily_members = pipe.zrangebyscore(daily_key, 0, day_window_start)
                daily_count = pipe.zcount(daily_key, day_window_start, "+inf")
                oldest_daily = pipe.zrangebyscore(daily_key, day_window_start, "+inf", start=0, num=1, withscores=True)
                current_requests = len(active_members)
                current_tokens = 0
                for active_member in active_members:
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
                oldest = pipe.zrange(redis_key, 0, 0, withscores=True)
                pipe.unwatch()
            wait_rpm = 0.0
            if rpm > 0 and current_requests >= rpm and oldest:
                wait_rpm = max(0.05, 60.0 - (now - float(oldest[0][1])) + 0.05)
            wait_tpm = 0.0
            if tpm > 0 and (current_tokens + estimated_tokens) > tpm and oldest_active:
                wait_tpm = max(0.05, 60.0 - (now - float(oldest_active[0][1])) + 0.05)
            wait_rpd = 0.0
            if rpd > 0 and daily_count >= rpd and oldest_daily:
                wait_rpd = max(1.0, 86400.0 - (now - float(oldest_daily[0][1])) + 0.05)
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
        except Exception as e:
            LOGGER.warning("Redis error in rate limit tracking: %s", e)
            return


class GeminiHttpProvider(TranslationProvider):
    def generate(self, model: str, prompt: str, system_prompt: str = "") -> str:
        api_key = os.environ.get("GEMINI_API_KEY", "").strip()
        if not api_key:
            raise RuntimeError("Missing GEMINI_API_KEY")
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
        body = {
            "contents": [{"parts": [{"text": f"{system_prompt}\n\n{prompt}".strip()}]}],
            "generationConfig": {"temperature": 0.2, "topP": 0.9},
        }
        _acquire_gemini_rate_slot(model, _estimate_gemini_tokens(prompt, system_prompt))
        max_429_attempts_raw = os.environ.get("NOVEL_TTS_RATE_LIMIT_MAX_ATTEMPTS", "").strip()
        try:
            max_429_attempts = int(max_429_attempts_raw) if max_429_attempts_raw else 20
        except ValueError:
            max_429_attempts = 20
        max_429_attempts = max(1, max_429_attempts)

        rate_attempt = 0
        generic_attempt = 0
        while True:
            try:
                response = requests.post(
                    url,
                    params={"key": api_key},
                    headers={"Content-Type": "application/json"},
                    json=body,
                    timeout=90,
                )
                if response.status_code == 429:
                    rate_attempt += 1
                    # Parse Google's recommended retry delay as base
                    import random, re
                    base_delay = 8.0
                    try:
                        msg = response.json().get("error", {}).get("message", "")
                        m = re.search(r"retry in (\d+\.?\d*)s", msg, re.IGNORECASE)
                        if m:
                            base_delay = max(float(m.group(1)), 3.0)
                        else:
                            for detail in response.json().get("error", {}).get("details", []):
                                if detail.get("@type", "").endswith("RetryInfo"):
                                    ds = detail.get("retryDelay", "")
                                    if ds.endswith("s"):
                                        base_delay = max(float(ds[:-1]), 3.0)
                    except Exception:
                        pass
                    # Exponential backoff: base * 2^attempt, capped at 120s
                    wait_seconds = min(base_delay * (2 ** (rate_attempt - 1)), 120.0)
                    # Wide jitter (±50%) to desynchronize workers
                    jitter = wait_seconds * 0.5
                    wait_seconds = max(wait_seconds + random.uniform(-jitter, jitter), 3.0)
                    if rate_attempt >= max_429_attempts:
                        raise RateLimitExceededError(
                            f"Gemini 429 persisted after {rate_attempt}/{max_429_attempts} attempts (model={model})"
                        )
                    LOGGER.warning(
                        "Gemini API Rate Limit (429) hit. Attempt %d/%d. Sleeping for %.1fs",
                        rate_attempt,
                        max_429_attempts,
                        wait_seconds,
                    )
                    time.sleep(wait_seconds)
                    continue
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
            except Exception as e:
                generic_attempt += 1
                LOGGER.warning("Gemini API generation error (attempt %d/12): %s", generic_attempt, e)
                if generic_attempt >= 12:
                    raise
                time.sleep(5 + (generic_attempt - 1) * 5)


class OpenAIChatProvider(TranslationProvider):
    def __init__(self) -> None:
        from openai import OpenAI

        self.client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

    def generate(self, model: str, prompt: str, system_prompt: str = "") -> str:
        response = self.client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system_prompt or "You are a helpful translator."},
                {"role": "user", "content": prompt},
            ],
        )
        return (response.choices[0].message.content or "").strip()


def get_translation_provider(provider_name: str) -> TranslationProvider:
    if provider_name == "gemini_http":
        return GeminiHttpProvider()
    if provider_name == "openai_chat":
        return OpenAIChatProvider()
    if provider_name not in {"gemini_http", "openai_chat"}:
        raise ValueError(f"Unsupported translation provider: {provider_name}")
    raise RuntimeError(f"Unhandled translation provider: {provider_name}")
