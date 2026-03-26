from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass
from pathlib import Path

import redis
import yaml

from novel_tts.config.models import ProxyGatewayConfig
from novel_tts.net.proxy_gateway import select_proxy_for_key_index

@dataclass(frozen=True)
class RedisCfg:
    host: str
    port: int
    database: int
    prefix: str


_KEY_INDEX_RE = re.compile(r":k(?P<idx>\d+):")
_KEY_MODEL_429_RE = re.compile(r":k(?P<idx>\d+):(?P<model>[^:]+):api:429$")
_KEY_MODEL_API_CALLS_RE = re.compile(r":k(?P<idx>\d+):(?P<model>[^:]+):api:calls$")
_KEY_MODEL_API_REQS_RE = re.compile(r":k(?P<idx>\d+):(?P<model>[^:]+):api:reqs$")
_KEY_MODEL_LLM_RE = re.compile(r":k(?P<idx>\d+):(?P<model>[^:]+):llm:reqs$")
_KEY_MODEL_QUOTA_RE = re.compile(r":k(?P<idx>\d+):(?P<model>[^:]+):quota:reqs$")
_KEY_MODEL_TPM_FREEZED_RE = re.compile(r":k(?P<idx>\d+):(?P<model>[^:]+):quota:tpm:freezed$")
_KEY_MODEL_TPM_LOCKED_RE = re.compile(r":k(?P<idx>\d+):(?P<model>[^:]+):quota:tpm:locked$")
_KEY_MODEL_RPM_FREEZED_RE = re.compile(r":k(?P<idx>\d+):(?P<model>[^:]+):quota:rpm:freezed$")
_KEY_MODEL_RPM_LOCKED_RE = re.compile(r":k(?P<idx>\d+):(?P<model>[^:]+):quota:rpm:locked$")
_KEY_MODEL_RPD_FREEZED_RE = re.compile(r":k(?P<idx>\d+):(?P<model>[^:]+):quota:rpd:freezed$")
_KEY_MODEL_RPD_LOCKED_RE = re.compile(r":k(?P<idx>\d+):(?P<model>[^:]+):quota:rpd:locked$")


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _keys_file_path() -> Path:
    return _repo_root() / ".secrets" / "gemini-keys.txt"


def _load_keys() -> list[str]:
    path = _keys_file_path()
    if not path.exists():
        raise FileNotFoundError(f"Missing key file: {path}")
    keys = [line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    if not keys:
        raise RuntimeError(f"No Gemini keys found in {path}")
    return keys


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


def _load_enabled_models() -> list[str]:
    path = _repo_root() / "configs" / "app.yaml"
    payload = {}
    if path.exists():
        payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    models = payload.get("models") if isinstance(payload, dict) else {}
    enabled = models.get("enabled_models") if isinstance(models, dict) else None
    if isinstance(enabled, list):
        out: list[str] = []
        for item in enabled:
            value = str(item or "").strip()
            if value and value not in out:
                out.append(value)
        return out
    return []


def _load_model_limits() -> tuple[dict[str, int], dict[str, int], dict[str, int]]:
    path = _repo_root() / "configs" / "app.yaml"
    payload = {}
    if path.exists():
        payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    models = payload.get("models") if isinstance(payload, dict) else {}
    if not isinstance(models, dict):
        models = {}

    model_cfgs = models.get("model_configs")
    tpm: dict[str, int] = {}
    rpm: dict[str, int] = {}
    rpd: dict[str, int] = {}
    if isinstance(model_cfgs, dict):
        for model, cfg in model_cfgs.items():
            model_name = str(model or "").strip()
            if not model_name or not isinstance(cfg, dict):
                continue
            try:
                tpm[model_name] = int(cfg.get("tpm_limit") or 0)
            except (TypeError, ValueError):
                tpm[model_name] = 0
            try:
                rpm[model_name] = int(cfg.get("rpm_limit") or 0)
            except (TypeError, ValueError):
                rpm[model_name] = 0
            try:
                rpd[model_name] = int(cfg.get("rpd_limit") or 0)
            except (TypeError, ValueError):
                rpd[model_name] = 0

    # Backward-compatible: legacy config key `model_tpm_limits`.
    legacy = models.get("model_tpm_limits")
    if isinstance(legacy, dict):
        for model, limit in legacy.items():
            model_name = str(model or "").strip()
            if not model_name:
                continue
            try:
                tpm.setdefault(model_name, int(limit or 0))
            except (TypeError, ValueError):
                tpm.setdefault(model_name, 0)

    legacy_rpm = models.get("model_rpm_limits")
    if isinstance(legacy_rpm, dict):
        for model, limit in legacy_rpm.items():
            model_name = str(model or "").strip()
            if not model_name:
                continue
            try:
                rpm.setdefault(model_name, int(limit or 0))
            except (TypeError, ValueError):
                rpm.setdefault(model_name, 0)

    legacy_rpd = models.get("model_rpd_limits")
    if isinstance(legacy_rpd, dict):
        for model, limit in legacy_rpd.items():
            model_name = str(model or "").strip()
            if not model_name:
                continue
            try:
                rpd.setdefault(model_name, int(limit or 0))
            except (TypeError, ValueError):
                rpd.setdefault(model_name, 0)

    return tpm, rpm, rpd


def _load_proxy_gateway_cfg() -> ProxyGatewayConfig:
    path = _repo_root() / "configs" / "app.yaml"
    payload = {}
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

    def _clean_bool(value, *, default: bool = False) -> bool:
        if isinstance(value, bool):
            return value
        if value is None:
            return bool(default)
        text = str(value).strip().lower()
        if not text:
            return bool(default)
        return text in {"1", "true", "yes", "on", "y"}

    enabled = _clean_bool(proxy_raw.get("enabled"), default=False)
    base_url = _clean_text(proxy_raw.get("base_url")) or "http://localhost:8888"
    mode = (_clean_text(proxy_raw.get("mode")) or "direct").strip().lower()
    auto_discovery = _clean_bool(proxy_raw.get("auto_discovery"), default=True)
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


def _load_healthy_proxy_names(client, *, prefix: str) -> list[str]:
    raw = None
    try:
        raw = client.get(_proxy_gateway_proxies_key(prefix))
    except Exception:
        raw = None
    if not raw:
        return []
    try:
        payload = json.loads(raw)
    except Exception:
        return []
    proxies = payload.get("proxies") if isinstance(payload, dict) else None
    if not isinstance(proxies, list):
        return []
    healthy: list[str] = []
    for item in proxies:
        if not isinstance(item, dict):
            continue
        if not bool(item.get("is_healthy")):
            continue
        name = str(item.get("name") or "").strip()
        if name:
            healthy.append(name)
    return healthy


def _parse_filter_values(values: list[str]) -> list[str]:
    tokens: list[str] = []
    for value in values or []:
        for part in str(value).split(","):
            part = part.strip()
            if part:
                tokens.append(part)
    # Dedupe while preserving order.
    seen: set[str] = set()
    out: list[str] = []
    for token in tokens:
        if token in seen:
            continue
        seen.add(token)
        out.append(token)
    return out


def _extract_key_index(redis_key: str) -> int | None:
    match = _KEY_INDEX_RE.search(redis_key or "")
    if not match:
        return None
    try:
        value = int(match.group("idx"))
    except Exception:
        return None
    return value if value > 0 else None


def _extract_key_index_and_model_for_429(redis_key: str) -> tuple[int | None, str]:
    match = _KEY_MODEL_429_RE.search(redis_key or "")
    if not match:
        return None, ""
    try:
        idx = int(match.group("idx"))
    except Exception:
        return None, ""
    model = (match.group("model") or "").strip()
    if idx <= 0 or not model:
        return None, ""
    return idx, model


def _extract_key_index_and_model(redis_key: str, *, pattern: re.Pattern[str]) -> tuple[int | None, str]:
    match = pattern.search(redis_key or "")
    if not match:
        return None, ""
    try:
        idx = int(match.group("idx"))
    except Exception:
        return None, ""
    model = (match.group("model") or "").strip()
    if idx <= 0 or not model:
        return None, ""
    return idx, model


def _select_indices(
    keys: list[str],
    *,
    filter_tokens: list[str],
    filter_raw_tokens: list[str],
) -> tuple[set[int] | None, int]:
    """
    Returns (selected_indices or None for all, unknown_raw_count).
    """

    selected: set[int] = set()
    unknown_raw = 0

    # --filter-raw: exact match on raw key -> index
    if filter_raw_tokens:
        key_to_index = {raw: idx for idx, raw in enumerate(keys, start=1)}
        for raw in filter_raw_tokens:
            idx = key_to_index.get(raw)
            if idx is None:
                unknown_raw += 1
                continue
            selected.add(idx)

    # --filter: index (kN/N) or last4
    if filter_tokens:
        last4_to_indices: dict[str, list[int]] = {}
        for idx, raw in enumerate(keys, start=1):
            last4 = (raw[-4:] if raw else "").strip()
            if last4:
                last4_to_indices.setdefault(last4, []).append(idx)

        for token in filter_tokens:
            token = token.strip()
            if not token:
                continue
            # Prefer interpreting 4-char tokens as last4 matches (even if numeric),
            # since API keys commonly end with digits.
            if len(token) == 4:
                for idx in last4_to_indices.get(token, []):
                    selected.add(idx)
                # If it matched as last4, don't also treat it as an index.
                if token in last4_to_indices:
                    continue
            if re.fullmatch(r"k?\d+", token, flags=re.IGNORECASE):
                token_num = token[1:] if token.lower().startswith("k") else token
                try:
                    idx = int(token_num)
                except Exception:
                    idx = 0
                if 0 < idx <= len(keys):
                    selected.add(idx)
                continue

    if not selected and not filter_tokens and not filter_raw_tokens:
        return None, 0
    return selected, unknown_raw


def _client(cfg: RedisCfg):
    return redis.Redis(host=cfg.host, port=cfg.port, db=cfg.database, decode_responses=True)


def _redis_now_seconds(client) -> float:
    try:
        sec, usec = client.time()
        return float(sec) + float(usec) / 1_000_000.0
    except Exception:
        return time.time()


def _zcount_1m(client, key: str, now: float) -> int:
    window_start = now - 60.0
    try:
        return int(client.zcount(key, window_start, "+inf"))
    except Exception:
        return 0


def _zcount_window(client, key: str, now: float, *, window_seconds: float) -> int:
    window_start = now - float(window_seconds)
    try:
        return int(client.zcount(key, window_start, "+inf"))
    except Exception:
        return 0


def _scan_counts(
    client, *, prefix: str
) -> tuple[
    dict[int, int],
    dict[int, int],
    dict[int, int],
    dict[int, dict[str, int]],
    dict[int, dict[str, int]],
    dict[int, dict[str, int]],
    dict[int, dict[str, int]],
    dict[int, dict[str, int]],
    dict[int, dict[str, int]],
    dict[int, dict[str, int]],
]:
    now = _redis_now_seconds(client)
    llm_counts: dict[int, int] = {}
    api_counts: dict[int, int] = {}
    api_429_counts: dict[int, int] = {}
    api_by_model: dict[int, dict[str, int]] = {}
    api_429_by_model: dict[int, dict[str, int]] = {}
    llm_by_model: dict[int, dict[str, int]] = {}
    quota_tokens_by_model: dict[int, dict[str, int]] = {}
    rpm_used_by_model: dict[int, dict[str, int]] = {}
    rpd_used_by_model: dict[int, dict[str, int]] = {}
    api_daily_by_model: dict[int, dict[str, int]] = {}

    # LLM metric should represent *attempts* (including retries), so prefer :llm:reqs.
    # Fallbacks exist for older workers that didn't emit :llm:reqs.
    api_calls_pattern = f"{prefix}:*:k*:*:api:calls"
    llm_pattern = f"{prefix}:*:k*:*:llm:reqs"
    quota_pattern = f"{prefix}:*:k*:*:quota:reqs"
    api_pattern = f"{prefix}:*:k*:*:api:reqs"
    api_429_pattern = f"{prefix}:*:k*:*:api:429"
    tpm_freezed_pattern = f"{prefix}:*:k*:*:quota:tpm:freezed"
    tpm_locked_pattern = f"{prefix}:*:k*:*:quota:tpm:locked"
    rpm_freezed_pattern = f"{prefix}:*:k*:*:quota:rpm:freezed"
    rpm_locked_pattern = f"{prefix}:*:k*:*:quota:rpm:locked"
    rpd_freezed_pattern = f"{prefix}:*:k*:*:quota:rpd:freezed"
    rpd_locked_pattern = f"{prefix}:*:k*:*:quota:rpd:locked"

    llm_keys: set[str] = set()
    llm_bases: set[str] = set()
    api_metric_bases: set[str] = set()

    def _add_model_count(store: dict[int, dict[str, int]], idx: int, model: str, count: int) -> None:
        by_model = store.setdefault(idx, {})
        by_model[model] = by_model.get(model, 0) + int(count or 0)

    # Preferred API attempt metric: :api:reqs (HTTP attempts including retries).
    for key in client.scan_iter(match=api_pattern, count=1000):
        idx, model = _extract_key_index_and_model(key, pattern=_KEY_MODEL_API_REQS_RE)
        if idx is None or not model:
            continue
        base = str(key).removesuffix(":api:reqs")
        if base in api_metric_bases:
            continue
        api_metric_bases.add(base)
        count_1m = _zcount_1m(client, key, now)
        count_1d = _zcount_window(client, str(key), now, window_seconds=86400.0)
        api_counts[idx] = api_counts.get(idx, 0) + count_1m
        _add_model_count(api_by_model, idx, model, count_1m)
        _add_model_count(api_daily_by_model, idx, model, count_1d)

    # Preferred: llm:reqs (attempts, including retries)
    for key in client.scan_iter(match=llm_pattern, count=1000):
        idx, model = _extract_key_index_and_model(key, pattern=_KEY_MODEL_LLM_RE)
        if idx is None or not model:
            continue
        base = str(key).removesuffix(":llm:reqs")
        if base in llm_bases:
            continue
        llm_bases.add(base)
        llm_keys.add(key)
        count = _zcount_1m(client, key, now)
        llm_counts[idx] = llm_counts.get(idx, 0) + count
        _add_model_count(llm_by_model, idx, model, count)
        if base not in api_metric_bases:
            api_metric_bases.add(base)
            count_1d = _zcount_window(client, str(key), now, window_seconds=86400.0)
            api_counts[idx] = api_counts.get(idx, 0) + count
            _add_model_count(api_by_model, idx, model, count)
            _add_model_count(api_daily_by_model, idx, model, count_1d)

    # Fallback: api:calls (logical calls). This undercounts when retries happen, but is better than 0
    # when :llm:reqs isn't available.
    for key in client.scan_iter(match=api_calls_pattern, count=1000):
        idx, model = _extract_key_index_and_model(key, pattern=_KEY_MODEL_API_CALLS_RE)
        if idx is None or not model:
            continue
        base = str(key).removesuffix(":api:calls")
        if base in llm_bases:
            continue
        llm_bases.add(base)
        llm_keys.add(key)
        count = _zcount_1m(client, key, now)
        llm_counts[idx] = llm_counts.get(idx, 0) + count
        _add_model_count(llm_by_model, idx, model, count)
        if base not in api_metric_bases:
            api_metric_bases.add(base)
            count_1d = _zcount_window(client, str(key), now, window_seconds=86400.0)
            api_counts[idx] = api_counts.get(idx, 0) + count
            _add_model_count(api_by_model, idx, model, count)
            _add_model_count(api_daily_by_model, idx, model, count_1d)

    # Backward-compatible fallback: older workers only wrote :quota:reqs.
    for key in client.scan_iter(match=quota_pattern, count=1000):
        idx, model = _extract_key_index_and_model(key, pattern=_KEY_MODEL_QUOTA_RE)
        if idx is None or not model:
            continue
        base = str(key).removesuffix(":quota:reqs")
        if base in llm_bases:
            continue
        llm_bases.add(base)
        count = _zcount_1m(client, key, now)
        llm_counts[idx] = llm_counts.get(idx, 0) + count
        _add_model_count(llm_by_model, idx, model, count)
        if base not in api_metric_bases:
            api_metric_bases.add(base)
            count_1d = _zcount_window(client, str(key), now, window_seconds=86400.0)
            api_counts[idx] = api_counts.get(idx, 0) + count
            _add_model_count(api_by_model, idx, model, count)
            _add_model_count(api_daily_by_model, idx, model, count_1d)

    # Quota TPM usage: sum estimated tokens for active members in the last 60s.
    def _sum_tokens_for_zset(zset_key: str, *, token_hash_key: str, window_seconds: float) -> int:
        window_start = now - float(window_seconds)
        try:
            members = list(client.zrangebyscore(zset_key, window_start, "+inf"))
        except Exception:
            return 0
        if not members:
            return 0
        try:
            token_vals = client.hmget(token_hash_key, members)
        except Exception:
            token_vals = []
        total = 0
        for raw in token_vals or []:
            try:
                total += int(raw or 0)
            except (TypeError, ValueError):
                continue
        return max(0, int(total))

    # Central quota v2 preferred: TPM is split across freezed + locked.
    for key in client.scan_iter(match=tpm_freezed_pattern, count=1000):
        idx, model = _extract_key_index_and_model(key, pattern=_KEY_MODEL_TPM_FREEZED_RE)
        if idx is None or not model:
            continue
        token_key = f"{str(key).removesuffix(':quota:tpm:freezed')}:quota:tpm:freezed_tokens"
        total_tokens = _sum_tokens_for_zset(str(key), token_hash_key=token_key, window_seconds=60.0)
        if total_tokens <= 0:
            continue
        _add_model_count(quota_tokens_by_model, idx, model, total_tokens)

    for key in client.scan_iter(match=tpm_locked_pattern, count=1000):
        idx, model = _extract_key_index_and_model(key, pattern=_KEY_MODEL_TPM_LOCKED_RE)
        if idx is None or not model:
            continue
        token_key = f"{str(key).removesuffix(':quota:tpm:locked')}:quota:tpm:locked_tokens"
        total_tokens = _sum_tokens_for_zset(str(key), token_hash_key=token_key, window_seconds=60.0)
        if total_tokens <= 0:
            continue
        _add_model_count(quota_tokens_by_model, idx, model, total_tokens)

    # Backward-compatible fallback: older workers used :quota:reqs + :quota:tokens.
    for key in client.scan_iter(match=quota_pattern, count=1000):
        idx, model = _extract_key_index_and_model(key, pattern=_KEY_MODEL_QUOTA_RE)
        if idx is None or not model:
            continue
        token_key = f"{str(key).removesuffix(':quota:reqs')}:quota:tokens"
        total_tokens = _sum_tokens_for_zset(str(key), token_hash_key=token_key, window_seconds=60.0)
        if total_tokens <= 0:
            continue
        by_model = quota_tokens_by_model.setdefault(idx, {})
        by_model.setdefault(model, 0)
        # Only use fallback when v2 hasn't already populated it.
        if by_model[model] <= 0:
            by_model[model] = total_tokens

    # Central quota v2: RPM and RPD usage.
    for key in client.scan_iter(match=rpm_freezed_pattern, count=1000):
        idx, model = _extract_key_index_and_model(key, pattern=_KEY_MODEL_RPM_FREEZED_RE)
        if idx is None or not model:
            continue
        count = _zcount_window(client, str(key), now, window_seconds=60.0)
        if count <= 0:
            continue
        _add_model_count(rpm_used_by_model, idx, model, count)

    for key in client.scan_iter(match=rpm_locked_pattern, count=1000):
        idx, model = _extract_key_index_and_model(key, pattern=_KEY_MODEL_RPM_LOCKED_RE)
        if idx is None or not model:
            continue
        count = _zcount_window(client, str(key), now, window_seconds=60.0)
        if count <= 0:
            continue
        _add_model_count(rpm_used_by_model, idx, model, count)

    for key in client.scan_iter(match=rpd_freezed_pattern, count=1000):
        idx, model = _extract_key_index_and_model(key, pattern=_KEY_MODEL_RPD_FREEZED_RE)
        if idx is None or not model:
            continue
        count = _zcount_window(client, str(key), now, window_seconds=86400.0)
        if count <= 0:
            continue
        _add_model_count(rpd_used_by_model, idx, model, count)

    for key in client.scan_iter(match=rpd_locked_pattern, count=1000):
        idx, model = _extract_key_index_and_model(key, pattern=_KEY_MODEL_RPD_LOCKED_RE)
        if idx is None or not model:
            continue
        count = _zcount_window(client, str(key), now, window_seconds=86400.0)
        if count <= 0:
            continue
        _add_model_count(rpd_used_by_model, idx, model, count)

    for key in client.scan_iter(match=api_429_pattern, count=1000):
        idx, model = _extract_key_index_and_model_for_429(key)
        if idx is None or not model:
            continue
        count = _zcount_1m(client, key, now)
        api_429_counts[idx] = api_429_counts.get(idx, 0) + count
        by_model = api_429_by_model.setdefault(idx, {})
        by_model[model] = by_model.get(model, 0) + count

    return (
        api_counts,
        api_429_counts,
        llm_counts,
        api_by_model,
        api_429_by_model,
        llm_by_model,
        quota_tokens_by_model,
        rpm_used_by_model,
        rpd_used_by_model,
        api_daily_by_model,
    )


def ai_key_ps(*, filters: list[str] | None = None, filters_raw: list[str] | None = None) -> int:
    keys = _load_keys()
    cfg = _load_redis_cfg()
    client = _client(cfg)
    enabled_models = _load_enabled_models()
    tpm_limits_by_model, rpm_limits_by_model, rpd_limits_by_model = _load_model_limits()
    proxy_cfg = _load_proxy_gateway_cfg()

    filter_tokens = _parse_filter_values(filters or [])
    filter_raw_tokens = _parse_filter_values(filters_raw or [])
    selected, unknown_raw = _select_indices(keys, filter_tokens=filter_tokens, filter_raw_tokens=filter_raw_tokens)

    (
        api_counts,
        api_429_counts,
        llm_counts,
        api_by_model,
        api_429_by_model,
        llm_by_model,
        quota_tokens_by_model,
        rpm_used_by_model,
        rpd_used_by_model,
        api_daily_by_model,
    ) = _scan_counts(client, prefix=cfg.prefix)

    all_indices: set[int] = set(range(1, len(keys) + 1))
    all_indices.update(api_counts.keys())
    all_indices.update(api_429_counts.keys())
    all_indices.update(llm_counts.keys())
    all_indices.update(api_429_by_model.keys())
    all_indices.update(llm_by_model.keys())

    if selected is not None:
        all_indices = {idx for idx in all_indices if idx in selected}

    def _label(idx: int) -> str:
        if 1 <= idx <= len(keys):
            last4 = keys[idx - 1][-4:] if keys[idx - 1] else ""
            return f"k{idx}:{last4}"
        return f"k{idx}:"

    rows = []
    for idx in sorted(all_indices):
        model_counts = api_429_by_model.get(idx, {}) or {}
        # Keep the cell compact: only show models with non-zero counts.
        nonzero = [(m, c) for m, c in model_counts.items() if int(c) > 0]
        nonzero.sort(key=lambda item: (-int(item[1]), item[0]))
        api_429_models = ", ".join(f"{m}={c}" for m, c in nonzero)
        llm_model_counts = llm_by_model.get(idx, {}) or {}
        llm_nonzero = [(m, c) for m, c in llm_model_counts.items() if int(c) > 0]
        llm_nonzero.sort(key=lambda item: (-int(item[1]), item[0]))
        llm_models = ", ".join(f"{m}={c}" for m, c in llm_nonzero)
        rows.append(
            {
                "key": _label(idx),
                "api": str(int(api_counts.get(idx, 0))),
                "api_429": str(int(api_429_counts.get(idx, 0))),
                "api_429_models": api_429_models,
                "llm": str(int(llm_counts.get(idx, 0))),
                "llm_models": llm_models,
            }
        )

    if unknown_raw:
        print(f"warning: ignored {unknown_raw} unknown raw key(s)")

    if not rows:
        print("No keys matched.")
        return 0

    proxy_by_key_index: dict[int, str] = {}
    if proxy_cfg.enabled:
        if proxy_cfg.auto_discovery:
            proxy_list = _load_healthy_proxy_names(client, prefix=cfg.prefix)
        else:
            proxy_list = list(proxy_cfg.proxies or [])
        if proxy_list:
            for idx in all_indices:
                proxy = select_proxy_for_key_index(
                    key_index=idx,
                    proxies=proxy_list,
                    keys_per_proxy=int(proxy_cfg.keys_per_proxy or 3),
                )
                if proxy:
                    proxy_by_key_index[int(idx)] = proxy

    headers = [
        "KEY",
        "API_CALL_COUNT",
        "LLM_CALL_COUNT",
        "API_SUCCESS_COUNT",
        "API_429_COUNT",
        "PROXY",
        "MODEL_NAME",
        "LLM_CALL",
        "API_SUCCESS",
        "API_429",
        "RPM",
        "RPD",
        "TPM",
    ]

    models_to_show = list(enabled_models)
    # Safety: if enabled models isn't configured, fall back to whatever we observed.
    if not models_to_show:
        observed: set[str] = set()
        for by_model in (api_by_model or {}).values():
            observed.update(str(m) for m in (by_model or {}).keys())
        for by_model in (api_429_by_model or {}).values():
            observed.update(str(m) for m in (by_model or {}).keys())
        for by_model in (llm_by_model or {}).values():
            observed.update(str(m) for m in (by_model or {}).keys())
        models_to_show = [m for m in sorted(observed) if m]

    def _model_pairs_in_order(by_model: dict[str, int]) -> list[tuple[str, int]]:
        return [(m, int((by_model or {}).get(m, 0) or 0)) for m in models_to_show]

    displayed_indices = sorted(all_indices)
    total_api_by_model: dict[str, int] = {}
    total_api_429_by_model: dict[str, int] = {}
    total_llm_by_model: dict[str, int] = {}
    total_quota_tokens_by_model: dict[str, int] = {}
    total_rpm_used_by_model: dict[str, int] = {}
    total_rpd_used_by_model: dict[str, int] = {}
    for idx in displayed_indices:
        for model, count in (api_by_model.get(idx, {}) or {}).items():
            total_api_by_model[model] = total_api_by_model.get(model, 0) + int(count or 0)
        for model, count in (api_429_by_model.get(idx, {}) or {}).items():
            total_api_429_by_model[model] = total_api_429_by_model.get(model, 0) + int(count or 0)
        for model, count in (llm_by_model.get(idx, {}) or {}).items():
            total_llm_by_model[model] = total_llm_by_model.get(model, 0) + int(count or 0)
        for model, count in (quota_tokens_by_model.get(idx, {}) or {}).items():
            total_quota_tokens_by_model[model] = total_quota_tokens_by_model.get(model, 0) + int(count or 0)
        for model, count in (rpm_used_by_model.get(idx, {}) or {}).items():
            total_rpm_used_by_model[model] = total_rpm_used_by_model.get(model, 0) + int(count or 0)
        for model, count in (rpd_used_by_model.get(idx, {}) or {}).items():
            total_rpd_used_by_model[model] = total_rpd_used_by_model.get(model, 0) + int(count or 0)

    if not models_to_show:
        models_to_show = [""]

    display_rows: list[dict[str, str]] = []
    for idx in displayed_indices:
        key_label = _label(idx)
        api_val = str(int(api_counts.get(idx, 0)))
        api_429_val = str(int(api_429_counts.get(idx, 0)))
        llm_val = str(int(llm_counts.get(idx, 0)))
        api_success_count_val = str(max(int(api_val) - int(api_429_val), 0))
        for i, model in enumerate(models_to_show):
            llm_model_val = str(int((llm_by_model.get(idx, {}) or {}).get(model, 0) or 0))
            attempts = int((api_by_model.get(idx, {}) or {}).get(model, 0) or 0)
            rate_limited = int((api_429_by_model.get(idx, {}) or {}).get(model, 0) or 0)
            api_success = max(attempts - rate_limited, 0)
            quota_used = int((quota_tokens_by_model.get(idx, {}) or {}).get(model, 0) or 0)
            tpm_limit = int((tpm_limits_by_model or {}).get(model, 0) or 0)
            tpm_has_data = model in (quota_tokens_by_model.get(idx, {}) or {})
            api_attempts_1m = int((api_by_model.get(idx, {}) or {}).get(model, 0) or 0)
            quota_cell = f"{quota_used:,} / {tpm_limit:,}" if tpm_limit > 0 else f"{quota_used:,}"
            if not tpm_has_data and quota_used <= 0 and api_attempts_1m > 0 and tpm_limit > 0:
                quota_cell = f"- / {tpm_limit:,}"
            rpm_has_data = model in (rpm_used_by_model.get(idx, {}) or {})
            rpm_used = int((rpm_used_by_model.get(idx, {}) or {}).get(model, 0) or 0)
            if not rpm_has_data:
                rpm_used = api_attempts_1m
            rpm_limit = int((rpm_limits_by_model or {}).get(model, 0) or 0)
            rpm_cell = f"{rpm_used} / {rpm_limit}" if rpm_limit > 0 else str(rpm_used)
            rpd_has_data = model in (rpd_used_by_model.get(idx, {}) or {})
            rpd_used = int((rpd_used_by_model.get(idx, {}) or {}).get(model, 0) or 0)
            if not rpd_has_data:
                rpd_used = int((api_daily_by_model.get(idx, {}) or {}).get(model, 0) or 0)
            rpd_limit = int((rpd_limits_by_model or {}).get(model, 0) or 0)
            rpd_cell = f"{rpd_used} / {rpd_limit}" if rpd_limit > 0 else str(rpd_used)
            display_rows.append(
                {
                    "KEY": key_label if i == 0 else "",
                    "API_CALL_COUNT": api_val if i == 0 else "",
                    "LLM_CALL_COUNT": llm_val if i == 0 else "",
                    "API_SUCCESS_COUNT": api_success_count_val if i == 0 else "",
                    "API_429_COUNT": api_429_val if i == 0 else "",
                    "PROXY": proxy_by_key_index.get(int(idx), "") if i == 0 else "",
                    "MODEL_NAME": model,
                    "LLM_CALL": llm_model_val,
                    "API_SUCCESS": str(int(api_success)),
                    "API_429": str(int(rate_limited)),
                    "RPM": rpm_cell,
                    "RPD": rpd_cell,
                    "TPM": quota_cell,
                }
            )

    total_api = sum(int(api_counts.get(idx, 0) or 0) for idx in displayed_indices)
    total_api_429 = sum(int(api_429_counts.get(idx, 0) or 0) for idx in displayed_indices)
    total_llm = sum(int(llm_counts.get(idx, 0) or 0) for idx in displayed_indices)
    total_api_success_count = max(int(total_api) - int(total_api_429), 0)
    key_count = sum(1 for idx in displayed_indices if 1 <= idx <= len(keys))

    total_api_success_by_model: dict[str, int] = {}
    for model in models_to_show:
        attempts = int((total_api_by_model or {}).get(model, 0) or 0)
        rate_limited = int((total_api_429_by_model or {}).get(model, 0) or 0)
        total_api_success_by_model[model] = max(attempts - rate_limited, 0)

    total_rows: list[dict[str, str]] = []
    for i, model in enumerate(models_to_show):
        llm_model_val = str(int((total_llm_by_model or {}).get(model, 0) or 0))
        api_success_val = str(int((total_api_success_by_model or {}).get(model, 0) or 0))
        api_429_val = str(int((total_api_429_by_model or {}).get(model, 0) or 0))
        total_quota_used = int((total_quota_tokens_by_model or {}).get(model, 0) or 0)
        tpm_limit = int((tpm_limits_by_model or {}).get(model, 0) or 0)
        total_limit = tpm_limit * key_count if (tpm_limit > 0 and key_count > 0) else 0
        total_quota_cell = f"{total_quota_used:,} / {total_limit:,}" if total_limit > 0 else f"{total_quota_used:,}"
        total_api_attempts_1m = int((total_api_by_model or {}).get(model, 0) or 0)
        if total_quota_used <= 0 and total_api_attempts_1m > 0 and total_limit > 0:
            total_quota_cell = f"- / {total_limit:,}"
        total_rpm_used = int((total_rpm_used_by_model or {}).get(model, 0) or 0)
        if model not in total_rpm_used_by_model:
            total_rpm_used = total_api_attempts_1m
        rpm_limit = int((rpm_limits_by_model or {}).get(model, 0) or 0)
        total_rpm_limit = rpm_limit * key_count if (rpm_limit > 0 and key_count > 0) else 0
        total_rpm_cell = f"{total_rpm_used} / {total_rpm_limit}" if total_rpm_limit > 0 else str(total_rpm_used)
        total_rpd_used = int((total_rpd_used_by_model or {}).get(model, 0) or 0)
        if model not in total_rpd_used_by_model:
            total_rpd_used = sum(int((api_daily_by_model.get(idx, {}) or {}).get(model, 0) or 0) for idx in displayed_indices)
        rpd_limit = int((rpd_limits_by_model or {}).get(model, 0) or 0)
        total_rpd_limit = rpd_limit * key_count if (rpd_limit > 0 and key_count > 0) else 0
        total_rpd_cell = f"{total_rpd_used} / {total_rpd_limit}" if total_rpd_limit > 0 else str(total_rpd_used)
        total_rows.append(
            {
                "KEY": "TOTAL" if i == 0 else "",
                "API_CALL_COUNT": str(total_api) if i == 0 else "",
                "LLM_CALL_COUNT": str(total_llm) if i == 0 else "",
                "API_SUCCESS_COUNT": str(total_api_success_count) if i == 0 else "",
                "API_429_COUNT": str(total_api_429) if i == 0 else "",
                "PROXY": "",
                "MODEL_NAME": model,
                "LLM_CALL": llm_model_val,
                "API_SUCCESS": api_success_val,
                "API_429": api_429_val,
                "RPM": total_rpm_cell,
                "RPD": total_rpd_cell,
                "TPM": total_quota_cell,
            }
        )

    widths: dict[str, int] = {h: len(h) for h in headers}
    for r in (display_rows + total_rows):
        for h in headers:
            widths[h] = max(widths[h], len(r.get(h, "")))

    def _hr() -> str:
        return "+-" + "-+-".join("-" * widths[h] for h in headers) + "-+"

    def _dash_hr() -> str:
        # Separator between multi-line per-key blocks.
        return "+-" + "-+-".join("-" * widths[h] for h in headers) + "-+"

    def _row(values: dict[str, str]) -> str:
        cells = []
        for h in headers:
            val = values.get(h, "")
            if h in {
                "API_CALL_COUNT",
                "LLM_CALL_COUNT",
                "API_SUCCESS_COUNT",
                "API_429_COUNT",
                "LLM_CALL",
                "API_SUCCESS",
                "API_429",
                "RPM",
                "RPD",
                "TPM",
            }:
                cells.append(val.rjust(widths[h]))
            else:
                cells.append(val.ljust(widths[h]))
        return "| " + " | ".join(cells) + " |"

    print(_hr())
    print(_row({h: h for h in headers}))
    print(_hr())
    current_key = None
    for r in display_rows:
        row_key = (r.get("KEY") or "").strip()
        if row_key and current_key is not None:
            # Separator between key blocks (not before the first).
            print(_dash_hr())
        if row_key:
            current_key = row_key
        print(_row(r))

    print(_hr())
    for r in total_rows:
        print(_row(r))
    print(_hr())
    return 0
