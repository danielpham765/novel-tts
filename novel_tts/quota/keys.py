from __future__ import annotations


def alloc_queue_key(*, key_prefix: str, model: str) -> str:
    return f"{key_prefix}:{model}:quota:alloc:queue"


def alloc_reply_key(*, key_prefix: str, model: str, request_id: str) -> str:
    return f"{key_prefix}:{model}:quota:alloc:reply:{request_id}"


def tpm_freezed_key(*, key_prefix: str, model: str) -> str:
    return f"{key_prefix}:{model}:quota:tpm:freezed"


def tpm_freezed_tokens_key(*, key_prefix: str, model: str) -> str:
    return f"{key_prefix}:{model}:quota:tpm:freezed_tokens"


def tpm_locked_key(*, key_prefix: str, model: str) -> str:
    return f"{key_prefix}:{model}:quota:tpm:locked"


def tpm_locked_tokens_key(*, key_prefix: str, model: str) -> str:
    return f"{key_prefix}:{model}:quota:tpm:locked_tokens"


def rpm_freezed_key(*, key_prefix: str, model: str) -> str:
    return f"{key_prefix}:{model}:quota:rpm:freezed"


def rpm_locked_key(*, key_prefix: str, model: str) -> str:
    return f"{key_prefix}:{model}:quota:rpm:locked"


def rpd_freezed_key(*, key_prefix: str, model: str) -> str:
    return f"{key_prefix}:{model}:quota:rpd:freezed"


def rpd_locked_key(*, key_prefix: str, model: str) -> str:
    return f"{key_prefix}:{model}:quota:rpd:locked"


def penalty_until_key(*, key_prefix: str, model: str) -> str:
    return f"{key_prefix}:{model}:quota:penalty_until"
