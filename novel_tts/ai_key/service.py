from __future__ import annotations

import re
import time
from dataclasses import dataclass
from pathlib import Path

import redis
import yaml


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


def _scan_counts(
    client, *, prefix: str
) -> tuple[
    dict[int, int],
    dict[int, int],
    dict[int, int],
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

    # LLM metric should represent *attempts* (including retries), so prefer :llm:reqs.
    # Fallbacks exist for older workers that didn't emit :llm:reqs.
    api_calls_pattern = f"{prefix}:*:k*:*:api:calls"
    llm_pattern = f"{prefix}:*:k*:*:llm:reqs"
    quota_pattern = f"{prefix}:*:k*:*:quota:reqs"
    api_pattern = f"{prefix}:*:k*:*:api:reqs"
    api_429_pattern = f"{prefix}:*:k*:*:api:429"

    llm_keys: set[str] = set()
    llm_bases: set[str] = set()

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
        by_model = llm_by_model.setdefault(idx, {})
        by_model[model] = by_model.get(model, 0) + count

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
        by_model = llm_by_model.setdefault(idx, {})
        by_model[model] = by_model.get(model, 0) + count

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
        by_model = llm_by_model.setdefault(idx, {})
        by_model[model] = by_model.get(model, 0) + count

    for key in client.scan_iter(match=api_pattern, count=1000):
        idx, model = _extract_key_index_and_model(key, pattern=_KEY_MODEL_API_REQS_RE)
        if idx is None or not model:
            continue
        count = _zcount_1m(client, key, now)
        api_counts[idx] = api_counts.get(idx, 0) + count
        by_model = api_by_model.setdefault(idx, {})
        by_model[model] = by_model.get(model, 0) + count

    for key in client.scan_iter(match=api_429_pattern, count=1000):
        idx, model = _extract_key_index_and_model_for_429(key)
        if idx is None or not model:
            continue
        count = _zcount_1m(client, key, now)
        api_429_counts[idx] = api_429_counts.get(idx, 0) + count
        by_model = api_429_by_model.setdefault(idx, {})
        by_model[model] = by_model.get(model, 0) + count

    return api_counts, api_429_counts, llm_counts, api_by_model, api_429_by_model, llm_by_model


def ai_key_ps(*, filters: list[str] | None = None, filters_raw: list[str] | None = None) -> int:
    keys = _load_keys()
    cfg = _load_redis_cfg()
    client = _client(cfg)
    enabled_models = _load_enabled_models()

    filter_tokens = _parse_filter_values(filters or [])
    filter_raw_tokens = _parse_filter_values(filters_raw or [])
    selected, unknown_raw = _select_indices(keys, filter_tokens=filter_tokens, filter_raw_tokens=filter_raw_tokens)

    api_counts, api_429_counts, llm_counts, api_by_model, api_429_by_model, llm_by_model = _scan_counts(
        client, prefix=cfg.prefix
    )

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

    headers = [
        "KEY",
        "API_CALL_COUNT_1M",
        "LLM_CALL_COUNT_1M",
        "API_SUCCESS_COUNT_1M",
        "API_429_COUNT_1M",
        "MODEL_NAME",
        "LLM_CALL_1M",
        "API_SUCCESS_1M",
        "API_429_1M",
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
    for idx in displayed_indices:
        for model, count in (api_by_model.get(idx, {}) or {}).items():
            total_api_by_model[model] = total_api_by_model.get(model, 0) + int(count or 0)
        for model, count in (api_429_by_model.get(idx, {}) or {}).items():
            total_api_429_by_model[model] = total_api_429_by_model.get(model, 0) + int(count or 0)
        for model, count in (llm_by_model.get(idx, {}) or {}).items():
            total_llm_by_model[model] = total_llm_by_model.get(model, 0) + int(count or 0)

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
            display_rows.append(
                {
                    "KEY": key_label if i == 0 else "",
                    "API_CALL_COUNT_1M": api_val if i == 0 else "",
                    "LLM_CALL_COUNT_1M": llm_val if i == 0 else "",
                    "API_SUCCESS_COUNT_1M": api_success_count_val if i == 0 else "",
                    "API_429_COUNT_1M": api_429_val if i == 0 else "",
                    "MODEL_NAME": model,
                    "LLM_CALL_1M": llm_model_val,
                    "API_SUCCESS_1M": str(int(api_success)),
                    "API_429_1M": str(int(rate_limited)),
                }
            )
    widths: dict[str, int] = {h: len(h) for h in headers}
    for r in display_rows:
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
                "API_CALL_COUNT_1M",
                "LLM_CALL_COUNT_1M",
                "API_SUCCESS_COUNT_1M",
                "API_429_COUNT_1M",
                "LLM_CALL_1M",
                "API_SUCCESS_1M",
                "API_429_1M",
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

    total_api = sum(int(api_counts.get(idx, 0) or 0) for idx in displayed_indices)
    total_api_429 = sum(int(api_429_counts.get(idx, 0) or 0) for idx in displayed_indices)
    total_llm = sum(int(llm_counts.get(idx, 0) or 0) for idx in displayed_indices)
    total_api_success_count = max(int(total_api) - int(total_api_429), 0)

    total_api_success_by_model: dict[str, int] = {}
    for model in models_to_show:
        attempts = int((total_api_by_model or {}).get(model, 0) or 0)
        rate_limited = int((total_api_429_by_model or {}).get(model, 0) or 0)
        total_api_success_by_model[model] = max(attempts - rate_limited, 0)

    print(_hr())
    for i, model in enumerate(models_to_show):
        llm_model_val = str(int((total_llm_by_model or {}).get(model, 0) or 0))
        api_success_val = str(int((total_api_success_by_model or {}).get(model, 0) or 0))
        api_429_val = str(int((total_api_429_by_model or {}).get(model, 0) or 0))
        print(
            _row(
                {
                    "KEY": "TOTAL" if i == 0 else "",
                    "API_CALL_COUNT_1M": str(total_api) if i == 0 else "",
                    "LLM_CALL_COUNT_1M": str(total_llm) if i == 0 else "",
                    "API_SUCCESS_COUNT_1M": str(total_api_success_count) if i == 0 else "",
                    "API_429_COUNT_1M": str(total_api_429) if i == 0 else "",
                    "MODEL_NAME": model,
                    "LLM_CALL_1M": llm_model_val,
                    "API_SUCCESS_1M": api_success_val,
                    "API_429_1M": api_429_val,
                }
            )
        )
    print(_hr())
    return 0
