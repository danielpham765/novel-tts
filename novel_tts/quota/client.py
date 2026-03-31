from __future__ import annotations

import json
import os
import time
import uuid
from dataclasses import dataclass

import redis

from novel_tts.common.errors import RateLimitExceededError

from . import keys as quota_keys
from .eta import QuotaRequest, TpmEvent, estimate_grant_times


@dataclass(frozen=True)
class QuotaGrant:
    grant_id: str
    tokens: int
    granted_at: float


def _redis_from_env() -> redis.Redis | None:
    host = os.environ.get("GEMINI_REDIS_HOST", "").strip()
    port_raw = os.environ.get("GEMINI_REDIS_PORT", "").strip()
    db_raw = os.environ.get("GEMINI_REDIS_DB", "").strip()
    if not host:
        return None
    try:
        port = int(port_raw) if port_raw else 6379
    except ValueError:
        port = 6379
    try:
        db = int(db_raw) if db_raw else 0
    except ValueError:
        db = 0
    return redis.Redis(host=host, port=port, db=db, decode_responses=True)


class CentralQuotaClient:
    def __init__(self) -> None:
        self.client = _redis_from_env()

    def enabled(self) -> bool:
        flag = os.environ.get("NOVEL_TTS_CENTRAL_QUOTA", "").strip().lower()
        return flag in {"1", "true", "yes", "on"}

    def _nonblocking(self) -> bool:
        flag = os.environ.get("NOVEL_TTS_CENTRAL_QUOTA_NONBLOCKING", "").strip().lower()
        return flag in {"1", "true", "yes", "on"}

    def _key_prefix_candidates_for_same_novel(self, *, key_prefix: str) -> list[str]:
        """
        Enumerate other key_prefixes for the same novel, based on:
          - env NOVEL_TTS_ALL_KEY_PREFIXES_JSON set by queue worker
          - legacy key_prefix format: <prefix>:<novel_id>:k<idx> + NOVEL_TTS_KEY_COUNT
        """

        raw_prefixes = os.environ.get("NOVEL_TTS_ALL_KEY_PREFIXES_JSON", "").strip()
        if raw_prefixes:
            try:
                payload = json.loads(raw_prefixes)
            except Exception:
                payload = []
            if isinstance(payload, list):
                out: list[str] = []
                seen: set[str] = set()
                for item in payload:
                    value = str(item or "").strip()
                    if not value or value in seen:
                        continue
                    seen.add(value)
                    out.append(value)
                if key_prefix and key_prefix in seen:
                    out.sort(key=lambda s: 0 if s == key_prefix else 1)
                if out:
                    return out

        parts = [p for p in str(key_prefix).split(":") if p]
        if len(parts) < 3 or not parts[-1].startswith("k"):
            return [key_prefix]
        base = ":".join(parts[:-1])
        current = parts[-1]
        try:
            cur_idx = int(current[1:])
        except Exception:
            cur_idx = 0
        count_raw = os.environ.get("NOVEL_TTS_KEY_COUNT", "").strip()
        try:
            count = int(count_raw) if count_raw else 0
        except Exception:
            count = 0
        if count <= 0:
            return [key_prefix]
        out: list[str] = []
        for i in range(1, count + 1):
            out.append(f"{base}:k{i}")
        # Keep stable order with current prefix first.
        if cur_idx > 0:
            out.sort(key=lambda s: 0 if s.endswith(f":k{cur_idx}") else 1)
        return out

    def _load_quota_events(
        self,
        *,
        key_prefix: str,
        model: str,
        now: float,
    ) -> tuple[list[float], list[TpmEvent], list[float]]:
        """
        Load current (freezed+locked) events for ETA simulation.
        """

        if self.client is None:
            return [], [], []

        window_1m = now - 60.0
        window_1d = now - 86400.0

        rpm_freezed = quota_keys.rpm_freezed_key(key_prefix=key_prefix, model=model)
        rpm_locked = quota_keys.rpm_locked_key(key_prefix=key_prefix, model=model)
        rpd_freezed = quota_keys.rpd_freezed_key(key_prefix=key_prefix, model=model)
        rpd_locked = quota_keys.rpd_locked_key(key_prefix=key_prefix, model=model)
        tpm_freezed = quota_keys.tpm_freezed_key(key_prefix=key_prefix, model=model)
        tpm_locked = quota_keys.tpm_locked_key(key_prefix=key_prefix, model=model)
        tpm_freezed_tokens = quota_keys.tpm_freezed_tokens_key(key_prefix=key_prefix, model=model)
        tpm_locked_tokens = quota_keys.tpm_locked_tokens_key(key_prefix=key_prefix, model=model)

        try:
            with self.client.pipeline() as pipe:
                pipe.zrangebyscore(rpm_freezed, window_1m, "+inf", withscores=True)
                pipe.zrangebyscore(rpm_locked, window_1m, "+inf", withscores=True)
                pipe.zrangebyscore(rpd_freezed, window_1d, "+inf", withscores=True)
                pipe.zrangebyscore(rpd_locked, window_1d, "+inf", withscores=True)
                pipe.zrangebyscore(tpm_freezed, window_1m, "+inf", withscores=True)
                pipe.zrangebyscore(tpm_locked, window_1m, "+inf", withscores=True)
                rpm_f, rpm_l, rpd_f, rpd_l, tpm_f, tpm_l = pipe.execute()
        except Exception:
            return [], [], []

        rpm_events: list[float] = []
        for _m, score in list(rpm_f or []) + list(rpm_l or []):
            try:
                rpm_events.append(float(score))
            except Exception:
                continue

        rpd_events: list[float] = []
        for _m, score in list(rpd_f or []) + list(rpd_l or []):
            try:
                rpd_events.append(float(score))
            except Exception:
                continue

        tpm_events: list[TpmEvent] = []
        try:
            freezed_ids = [str(m) for m, _s in (tpm_f or []) if m]
            locked_ids = [str(m) for m, _s in (tpm_l or []) if m]
            if freezed_ids:
                freezed_vals = self.client.hmget(tpm_freezed_tokens, freezed_ids) or []
                for (_mid, score), raw in zip(tpm_f, freezed_vals):
                    try:
                        tok = int(raw or 0)
                        ts = float(score)
                    except Exception:
                        continue
                    if tok > 0:
                        tpm_events.append(TpmEvent(ts=ts, tokens=tok))
            if locked_ids:
                locked_vals = self.client.hmget(tpm_locked_tokens, locked_ids) or []
                for (_mid, score), raw in zip(tpm_l, locked_vals):
                    try:
                        tok = int(raw or 0)
                        ts = float(score)
                    except Exception:
                        continue
                    if tok > 0:
                        tpm_events.append(TpmEvent(ts=ts, tokens=tok))
        except Exception:
            tpm_events = []

        return rpm_events, tpm_events, rpd_events

    def _estimate_wait_seconds_if_enqueued(
        self,
        *,
        key_prefix: str,
        model: str,
        tokens: int,
        rpm_limit: int,
        tpm_limit: int,
        rpd_limit: int,
        max_queue_items: int = 200,
    ) -> float:
        if self.client is None:
            return 0.0
        try:
            sec, usec = self.client.time()
            now = float(sec) + float(usec) / 1_000_000.0
        except Exception:
            now = time.time()

        rpm_events, tpm_events, rpd_events = self._load_quota_events(key_prefix=key_prefix, model=model, now=now)
        queue_key = quota_keys.alloc_queue_key(key_prefix=key_prefix, model=model)
        raw_items = []
        try:
            raw_items = self.client.lrange(queue_key, 0, max_queue_items - 1) or []
        except Exception:
            raw_items = []

        reqs: list[QuotaRequest] = []
        for raw in raw_items:
            try:
                payload = json.loads(raw or "")
            except Exception:
                continue
            rid = str(payload.get("request_id") or "").strip()
            if not rid:
                continue
            try:
                tok = int(payload.get("tokens") or 0)
            except Exception:
                tok = 0
            reqs.append(QuotaRequest(request_id=rid, tokens=max(1, tok)))

        our_id = uuid.uuid4().hex
        reqs.append(QuotaRequest(request_id=our_id, tokens=max(1, int(tokens or 0))))
        out = estimate_grant_times(
            now=now,
            rpm_limit=int(rpm_limit or 0),
            tpm_limit=int(tpm_limit or 0),
            rpd_limit=int(rpd_limit or 0),
            rpm_events=rpm_events,
            tpm_events=tpm_events,
            rpd_events=rpd_events,
            requests=reqs,
        )
        grant_at = float(out.get(our_id) or now)
        return max(0.0, grant_at - now)

    def _other_key_has_immediate_quota(
        self,
        *,
        key_prefix: str,
        model: str,
        tokens: int,
        rpm_limit: int,
        tpm_limit: int,
        rpd_limit: int,
    ) -> bool:
        """
        Returns True if some other key (same novel) can grant immediately, including queued quota:
          - alloc queue is empty
          - current used windows allow (freezed+locked)
        """

        if self.client is None:
            return False
        parts = [p for p in str(key_prefix).split(":") if p]
        if len(parts) < 3 or not parts[-1].startswith("k"):
            return False
        current_prefix = str(key_prefix)
        candidates = self._key_prefix_candidates_for_same_novel(key_prefix=key_prefix)
        for other_prefix in candidates:
            if other_prefix == current_prefix:
                continue
            queue_key = quota_keys.alloc_queue_key(key_prefix=other_prefix, model=model)
            try:
                if int(self.client.llen(queue_key) or 0) > 0:
                    continue
            except Exception:
                continue
            snap = self.snapshot_usage(key_prefix=other_prefix, model=model)
            rpm_used = int(snap.get("rpm_used_1m") or 0)
            tpm_used = int(snap.get("tpm_used_1m") or 0)
            rpd_used = int(snap.get("rpd_used_1d") or 0)
            ok_rpm = (int(rpm_limit or 0) <= 0) or (rpm_used < int(rpm_limit))
            ok_tpm = (int(tpm_limit or 0) <= 0) or ((tpm_used + int(tokens)) <= int(tpm_limit))
            ok_rpd = (int(rpd_limit or 0) <= 0) or (rpd_used < int(rpd_limit))
            if ok_rpm and ok_tpm and ok_rpd:
                return True
        return False

    def snapshot_usage(self, *, key_prefix: str, model: str) -> dict[str, int | float]:
        """
        Best-effort usage snapshot for debugging (e.g., when upstream still returns HTTP 429).

        Returns a dict with:
          - now
          - rpm_used_1m
          - rpd_used_1d
          - tpm_used_1m
        """

        if self.client is None:
            return {"now": time.time(), "rpm_used_1m": 0, "rpd_used_1d": 0, "tpm_used_1m": 0}

        try:
            sec, usec = self.client.time()
            now = float(sec) + float(usec) / 1_000_000.0
        except Exception:
            now = time.time()

        rpm_freezed = quota_keys.rpm_freezed_key(key_prefix=key_prefix, model=model)
        rpm_locked = quota_keys.rpm_locked_key(key_prefix=key_prefix, model=model)
        rpd_freezed = quota_keys.rpd_freezed_key(key_prefix=key_prefix, model=model)
        rpd_locked = quota_keys.rpd_locked_key(key_prefix=key_prefix, model=model)
        tpm_freezed = quota_keys.tpm_freezed_key(key_prefix=key_prefix, model=model)
        tpm_freezed_tokens = quota_keys.tpm_freezed_tokens_key(key_prefix=key_prefix, model=model)
        tpm_locked = quota_keys.tpm_locked_key(key_prefix=key_prefix, model=model)
        tpm_locked_tokens = quota_keys.tpm_locked_tokens_key(key_prefix=key_prefix, model=model)

        window_1m = now - 60.0
        window_1d = now - 86400.0

        try:
            with self.client.pipeline() as pipe:
                pipe.zcount(rpm_freezed, window_1m, "+inf")
                pipe.zcount(rpm_locked, window_1m, "+inf")
                pipe.zcount(rpd_freezed, window_1d, "+inf")
                pipe.zcount(rpd_locked, window_1d, "+inf")
                pipe.zrangebyscore(tpm_freezed, window_1m, "+inf")
                pipe.zrangebyscore(tpm_locked, window_1m, "+inf")
                rpm_freezed_n, rpm_locked_n, rpd_freezed_n, rpd_locked_n, tpm_freezed_ids, tpm_locked_ids = pipe.execute()
        except Exception:
            return {"now": now, "rpm_used_1m": 0, "rpd_used_1d": 0, "tpm_used_1m": 0}

        rpm_used = int(rpm_freezed_n or 0) + int(rpm_locked_n or 0)
        rpd_used = int(rpd_freezed_n or 0) + int(rpd_locked_n or 0)

        tpm_used = 0
        try:
            freezed_ids = [str(x) for x in (tpm_freezed_ids or []) if x]
            locked_ids = [str(x) for x in (tpm_locked_ids or []) if x]
            raw_freezed = []
            raw_locked = []
            if freezed_ids and locked_ids:
                with self.client.pipeline() as pipe:
                    pipe.hmget(tpm_freezed_tokens, freezed_ids)
                    pipe.hmget(tpm_locked_tokens, locked_ids)
                    raw = pipe.execute()
                raw_freezed = raw[0] if raw and len(raw) > 0 else []
                raw_locked = raw[1] if raw and len(raw) > 1 else []
            elif freezed_ids:
                raw_freezed = self.client.hmget(tpm_freezed_tokens, freezed_ids) or []
            elif locked_ids:
                raw_locked = self.client.hmget(tpm_locked_tokens, locked_ids) or []
            for item in list(raw_freezed or []) + list(raw_locked or []):
                try:
                    tpm_used += int(item or 0)
                except Exception:
                    continue
        except Exception:
            tpm_used = 0

        return {
            "now": now,
            "rpm_used_1m": int(rpm_used),
            "rpd_used_1d": int(rpd_used),
            "tpm_used_1m": int(tpm_used),
        }

    def penalize(self, *, key_prefix: str, model: str, seconds: float) -> None:
        """
        Soft backoff used when upstream still returns 429 even though internal quota granted.
        This is a best-effort hint for the quota-supervisor to temporarily stop granting.
        """

        if self.client is None:
            return
        try:
            secs = float(seconds)
        except Exception:
            return
        if secs <= 0:
            return
        secs = max(1.0, min(secs, 300.0))
        try:
            until = time.time() + secs
            key = quota_keys.penalty_until_key(key_prefix=key_prefix, model=model)
            self.client.set(key, str(until), ex=int(secs + 30.0))
        except Exception:
            return

    def acquire(
        self,
        *,
        key_prefix: str,
        model: str,
        tokens: int,
        rpm_limit: int = 0,
        tpm_limit: int = 0,
        rpd_limit: int = 0,
    ) -> QuotaGrant:
        if not self.enabled():
            raise RuntimeError("Central quota is disabled")
        if self.client is None:
            raise RuntimeError("Central quota requires GEMINI_REDIS_* env vars")

        safe_tokens = max(1, int(tokens or 0))
        safe_model = str(model or "").strip()
        if safe_model.lower() in {"none", "null"}:
            safe_model = ""
        if not safe_model:
            safe_model = str(os.environ.get("GEMINI_MODEL", "") or "").strip()
        if not safe_model:
            raise RuntimeError("Central quota acquire requires a model (set GEMINI_MODEL)")

        # Non-blocking redirect: if this key-model is likely to wait > 5s, but some other key (same novel) can grant
        # immediately (including queued quota), then exit with code=76 so the worker can requeue for another key.
        if self._nonblocking() and (rpm_limit or tpm_limit or rpd_limit):
            try:
                wait_self = self._estimate_wait_seconds_if_enqueued(
                    key_prefix=key_prefix,
                    model=safe_model,
                    tokens=safe_tokens,
                    rpm_limit=int(rpm_limit or 0),
                    tpm_limit=int(tpm_limit or 0),
                    rpd_limit=int(rpd_limit or 0),
                )
            except Exception:
                wait_self = 0.0
            if wait_self > 5.0:
                try:
                    other_ok = self._other_key_has_immediate_quota(
                        key_prefix=key_prefix,
                        model=safe_model,
                        tokens=safe_tokens,
                        rpm_limit=int(rpm_limit or 0),
                        tpm_limit=int(tpm_limit or 0),
                        rpd_limit=int(rpd_limit or 0),
                    )
                except Exception:
                    other_ok = False
                if other_ok:
                    raise RateLimitExceededError(
                        f"Central quota redirect (model={safe_model} suggested_wait={wait_self:.2f}s requeue=1)"
                    )

        request_id = uuid.uuid4().hex
        queue_key = quota_keys.alloc_queue_key(key_prefix=key_prefix, model=safe_model)
        reply_key = quota_keys.alloc_reply_key(key_prefix=key_prefix, model=safe_model, request_id=request_id)
        inflight_key = f"{key_prefix}:{safe_model}:quota:alloc:inflight:{os.getpid()}"

        ttl_raw = os.environ.get("NOVEL_TTS_CENTRAL_QUOTA_REQUEST_TTL_SECONDS", "").strip()
        try:
            ttl_seconds = float(ttl_raw) if ttl_raw else 30.0
        except ValueError:
            ttl_seconds = 30.0
        ttl_seconds = max(5.0, min(ttl_seconds, 300.0))

        wait_raw = os.environ.get("NOVEL_TTS_CENTRAL_QUOTA_WAIT_SECONDS", "").strip()
        # Redis BLPOP interprets timeout=0 as "block forever", which can deadlock queue workers if the
        # supervisor is down or misconfigured. Default to the request TTL so acquire() always returns.
        try:
            wait_seconds = float(wait_raw) if wait_raw else float(ttl_seconds)
        except ValueError:
            wait_seconds = float(ttl_seconds)

        created_at = time.time()
        payload = {
            "request_id": request_id,
            "reply_key": reply_key,
            "tokens": safe_tokens,
            "rpm_req": 1,
            "rpd_req": 1,
            "expires_at": created_at + ttl_seconds,
            "created_at": created_at,
            "pid": os.getpid(),
        }
        self.client.set(
            inflight_key,
            json.dumps(payload, ensure_ascii=False),
            ex=int(max(60.0, ttl_seconds + 30.0)),
        )
        self.client.rpush(queue_key, json.dumps(payload, ensure_ascii=False))
        self.client.expire(reply_key, int(max(30.0, ttl_seconds + 30.0)))

        if wait_seconds <= 0:
            item = self.client.lpop(reply_key)
            if item is not None:
                item = (reply_key, item)
        else:
            timeout = max(1, int(min(wait_seconds, ttl_seconds)))
            item = self.client.blpop([reply_key], timeout=timeout)
        if not item:
            # Keep inflight key briefly for ps-all introspection.
            raise RateLimitExceededError(
                f"Central quota grant timed out (model={safe_model} tokens={safe_tokens} ttl={ttl_seconds:.0f}s)"
            )
        _key, raw = item
        try:
            reply = json.loads(raw or "")
        except Exception as exc:
            raise RuntimeError(f"Invalid quota reply payload: {raw!r}") from exc
        grant_id = str(reply.get("grant_id") or "").strip()
        granted_at = float(reply.get("granted_at") or 0.0)
        if not grant_id:
            raise RuntimeError(f"Quota reply missing grant_id: {reply!r}")
        try:
            self.client.delete(inflight_key)
        except Exception:
            pass
        return QuotaGrant(grant_id=grant_id, tokens=safe_tokens, granted_at=granted_at)

    def commit(self, *, key_prefix: str, model: str, grant: QuotaGrant, success: bool, used_tokens: int | None = None) -> None:
        if not self.enabled():
            return
        if self.client is None:
            return

        from .lua_scripts import COMMIT_LUA

        safe_model = str(model or "").strip()
        if safe_model.lower() in {"none", "null"}:
            safe_model = ""
        if not safe_model:
            safe_model = str(os.environ.get("GEMINI_MODEL", "") or "").strip()
        if not safe_model:
            return

        outcome = "success" if success else "fail"
        used = 0
        if used_tokens is not None:
            try:
                used = int(used_tokens or 0)
            except Exception:
                used = 0
        used = max(0, used)
        base_keys = _quota_script_keys(key_prefix=key_prefix, model=safe_model)
        # Using EVAL directly keeps dependencies small and avoids script cache issues during iteration.
        self.client.eval(
            COMMIT_LUA,
            len(base_keys),
            *base_keys,
            grant.grant_id,
            outcome,
            str(int(grant.tokens)),
            str(int(used)),
        )


def _quota_script_keys(*, key_prefix: str, model: str) -> list[str]:
    return [
        quota_keys.tpm_freezed_key(key_prefix=key_prefix, model=model),
        quota_keys.tpm_freezed_tokens_key(key_prefix=key_prefix, model=model),
        quota_keys.tpm_locked_key(key_prefix=key_prefix, model=model),
        quota_keys.tpm_locked_tokens_key(key_prefix=key_prefix, model=model),
        quota_keys.rpm_freezed_key(key_prefix=key_prefix, model=model),
        quota_keys.rpm_locked_key(key_prefix=key_prefix, model=model),
        quota_keys.rpd_freezed_key(key_prefix=key_prefix, model=model),
        quota_keys.rpd_locked_key(key_prefix=key_prefix, model=model),
    ]
