from __future__ import annotations

import hashlib


def key_token_from_raw(raw_key: str) -> str:
    value = str(raw_key or "").strip()
    if not value:
        raise ValueError("raw_key is required")
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]
    return f"key_{digest}"


def build_key_prefix(*, prefix: str, novel_id: str, raw_key: str) -> str:
    safe_prefix = str(prefix or "").strip()
    safe_novel_id = str(novel_id or "").strip()
    if not safe_prefix:
        raise ValueError("prefix is required")
    if not safe_novel_id:
        raise ValueError("novel_id is required")
    return f"{safe_prefix}:{safe_novel_id}:{key_token_from_raw(raw_key)}"


def build_global_key_prefix(*, prefix: str, raw_key: str) -> str:
    """Build a key prefix without novel_id for shared (cross-novel) rate limit keys."""
    safe_prefix = str(prefix or "").strip()
    if not safe_prefix:
        raise ValueError("prefix is required")
    return f"{safe_prefix}:{key_token_from_raw(raw_key)}"

