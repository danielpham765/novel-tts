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
            if re.fullmatch(r"k?\d+", token, flags=re.IGNORECASE):
                token_num = token[1:] if token.lower().startswith("k") else token
                try:
                    idx = int(token_num)
                except Exception:
                    idx = 0
                if idx > 0:
                    selected.add(idx)
                continue
            if len(token) == 4:
                for idx in last4_to_indices.get(token, []):
                    selected.add(idx)

    if not selected and not filter_tokens and not filter_raw_tokens:
        return None, 0
    return selected, unknown_raw


def _client(cfg: RedisCfg):
    return redis.Redis(host=cfg.host, port=cfg.port, db=cfg.database, decode_responses=True)


def _zcount_1m(client, key: str, now: float) -> int:
    window_start = now - 60.0
    try:
        return int(client.zcount(key, window_start, "+inf"))
    except Exception:
        return 0


def _scan_counts(client, *, prefix: str) -> tuple[dict[int, int], dict[int, int], dict[int, int]]:
    now = time.time()
    llm_counts: dict[int, int] = {}
    api_counts: dict[int, int] = {}
    api_429_counts: dict[int, int] = {}

    llm_pattern = f"{prefix}:*:k*:*:llm:reqs"
    quota_pattern = f"{prefix}:*:k*:*:quota:reqs"
    api_pattern = f"{prefix}:*:k*:*:api:reqs"
    api_429_pattern = f"{prefix}:*:k*:*:api:429"

    llm_keys: set[str] = set()
    for key in client.scan_iter(match=llm_pattern, count=1000):
        idx = _extract_key_index(key)
        if idx is None:
            continue
        llm_keys.add(key)
        llm_counts[idx] = llm_counts.get(idx, 0) + _zcount_1m(client, key, now)

    # Backward-compatible fallback: older workers only wrote :quota:reqs.
    # Avoid double-counting when both exist by skipping quota keys that have a corresponding llm key.
    for key in client.scan_iter(match=quota_pattern, count=1000):
        idx = _extract_key_index(key)
        if idx is None:
            continue
        candidate_llm_key = str(key).replace(":quota:reqs", ":llm:reqs")
        if candidate_llm_key in llm_keys:
            continue
        llm_counts[idx] = llm_counts.get(idx, 0) + _zcount_1m(client, key, now)

    for key in client.scan_iter(match=api_pattern, count=1000):
        idx = _extract_key_index(key)
        if idx is None:
            continue
        api_counts[idx] = api_counts.get(idx, 0) + _zcount_1m(client, key, now)

    for key in client.scan_iter(match=api_429_pattern, count=1000):
        idx = _extract_key_index(key)
        if idx is None:
            continue
        api_429_counts[idx] = api_429_counts.get(idx, 0) + _zcount_1m(client, key, now)

    return api_counts, api_429_counts, llm_counts


def ai_key_ps(*, filters: list[str] | None = None, filters_raw: list[str] | None = None) -> int:
    keys = _load_keys()
    cfg = _load_redis_cfg()
    client = _client(cfg)

    filter_tokens = _parse_filter_values(filters or [])
    filter_raw_tokens = _parse_filter_values(filters_raw or [])
    selected, unknown_raw = _select_indices(keys, filter_tokens=filter_tokens, filter_raw_tokens=filter_raw_tokens)

    api_counts, api_429_counts, llm_counts = _scan_counts(client, prefix=cfg.prefix)

    all_indices: set[int] = set(range(1, len(keys) + 1))
    all_indices.update(api_counts.keys())
    all_indices.update(api_429_counts.keys())
    all_indices.update(llm_counts.keys())

    if selected is not None:
        all_indices = {idx for idx in all_indices if idx in selected}

    def _label(idx: int) -> str:
        if 1 <= idx <= len(keys):
            last4 = keys[idx - 1][-4:] if keys[idx - 1] else ""
            return f"k{idx}:{last4}"
        return f"k{idx}:"

    rows = []
    for idx in sorted(all_indices):
        rows.append(
            {
                "key": _label(idx),
                "api": str(int(api_counts.get(idx, 0))),
                "api_429": str(int(api_429_counts.get(idx, 0))),
                "llm": str(int(llm_counts.get(idx, 0))),
            }
        )

    if unknown_raw:
        print(f"warning: ignored {unknown_raw} unknown raw key(s)")

    if not rows:
        print("No keys matched.")
        return 0

    headers = ["KEY", "API_CALL_COUNT_1M", "API_429_COUNT_1M", "LLM_CALL_COUNT_1M"]
    display_rows = [
        {
            "KEY": r["key"],
            "API_CALL_COUNT_1M": r["api"],
            "API_429_COUNT_1M": r["api_429"],
            "LLM_CALL_COUNT_1M": r["llm"],
        }
        for r in rows
    ]
    widths: dict[str, int] = {h: len(h) for h in headers}
    for r in display_rows:
        for h in headers:
            widths[h] = max(widths[h], len(r.get(h, "")))

    def _hr() -> str:
        return "+-" + "-+-".join("-" * widths[h] for h in headers) + "-+"

    def _row(values: dict[str, str]) -> str:
        cells = []
        for h in headers:
            val = values.get(h, "")
            if h in {"API_CALL_COUNT_1M", "API_429_COUNT_1M", "LLM_CALL_COUNT_1M"}:
                cells.append(val.rjust(widths[h]))
            else:
                cells.append(val.ljust(widths[h]))
        return "| " + " | ".join(cells) + " |"

    print(_hr())
    print(_row({h: h for h in headers}))
    print(_hr())
    for r in display_rows:
        print(_row(r))
    print(_hr())
    return 0
