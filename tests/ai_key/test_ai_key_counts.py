from __future__ import annotations

import fnmatch

from novel_tts.ai_key.service import _scan_counts


class FakeRedis:
    def __init__(self, *, now_seconds: float, zcount_by_key: dict[str, int]):
        self._now_seconds = float(now_seconds)
        self._zcount_by_key = dict(zcount_by_key)

    def time(self):
        sec = int(self._now_seconds)
        usec = int((self._now_seconds - sec) * 1_000_000)
        return sec, usec

    def scan_iter(self, *, match: str, count: int = 1000):
        del count
        for key in sorted(self._zcount_by_key.keys()):
            if fnmatch.fnmatch(key, match):
                yield key

    def zcount(self, key: str, min_score, max_score):
        del min_score, max_score
        return int(self._zcount_by_key.get(key, 0))

    def zrangebyscore(self, key: str, min_score, max_score, withscores: bool = False):
        del key, min_score, max_score, withscores
        return []

    def hmget(self, key: str, keys):
        del key, keys
        return []


def test_scan_counts_llm_prefers_llm_reqs_attempts_over_api_calls() -> None:
    client = FakeRedis(
        now_seconds=1000.0,
        zcount_by_key={
            # Same base has both llm:reqs (attempts) and api:calls (logical).
            "novel_tts:novel:k1:gemma:llm:reqs": 5,
            "novel_tts:novel:k1:gemma:api:calls": 2,
            # Also include api:reqs so API_CALL_COUNT_1M can be computed separately.
            "novel_tts:novel:k1:gemma:api:reqs": 5,
        },
    )

    (
        api_counts,
        api_429_counts,
        llm_counts,
        api_by_model,
        api_429_by_model,
        llm_by_model,
        api_success_by_model,
        quota_tokens_by_model,
        rpm_used_by_model,
        rpd_used_by_model,
        api_daily_by_model,
    ) = _scan_counts(client, prefix="novel_tts", key_token_to_index={"k1": 1})

    assert api_counts == {1: 5}
    assert api_429_counts == {}
    assert api_by_model == {1: {"gemma": 5}}
    assert api_429_by_model == {}
    assert llm_counts == {1: 5}
    assert llm_by_model == {1: {"gemma": 5}}
    assert api_success_by_model == {}
    assert quota_tokens_by_model == {}
    assert rpm_used_by_model == {}
    assert rpd_used_by_model == {}
    assert api_daily_by_model == {1: {"gemma": 5}}


def test_scan_counts_llm_falls_back_to_api_calls_when_no_llm_reqs() -> None:
    client = FakeRedis(
        now_seconds=1000.0,
        zcount_by_key={
            "novel_tts:novel:k2:gemma:api:calls": 3,
        },
    )

    _, _, llm_counts, _, _, llm_by_model, _, _, _, _, _ = _scan_counts(
        client,
        prefix="novel_tts",
        key_token_to_index={"k2": 2},
    )
    assert llm_counts == {2: 3}
    assert llm_by_model == {2: {"gemma": 3}}


def test_scan_counts_llm_falls_back_to_quota_reqs_when_no_llm_or_api_calls() -> None:
    client = FakeRedis(
        now_seconds=1000.0,
        zcount_by_key={
            "novel_tts:novel:k3:gemma:quota:reqs": 7,
        },
    )

    _, _, llm_counts, _, _, llm_by_model, _, _, _, _, _ = _scan_counts(
        client,
        prefix="novel_tts",
        key_token_to_index={"k3": 3},
    )
    assert llm_counts == {3: 7}
    assert llm_by_model == {3: {"gemma": 7}}


def test_scan_counts_central_quota_rpm_rpd_and_tpm_tokens() -> None:
    client = FakeRedis(
        now_seconds=1000.0,
        zcount_by_key={
            "novel_tts:novel:k4:gemma:quota:rpm:freezed": 2,
            "novel_tts:novel:k4:gemma:quota:rpm:locked": 3,
            "novel_tts:novel:k4:gemma:quota:rpd:freezed": 4,
            "novel_tts:novel:k4:gemma:quota:rpd:locked": 5,
            "novel_tts:novel:k4:gemma:quota:tpm:freezed": 1,
            "novel_tts:novel:k4:gemma:quota:tpm:locked": 1,
        },
    )

    # Provide members for token sum lookups.
    def _zrangebyscore(key: str, min_score, max_score, withscores: bool = False):
        del min_score, max_score, withscores
        if key.endswith(":quota:tpm:freezed"):
            return ["a"]
        if key.endswith(":quota:tpm:locked"):
            return ["b"]
        return []

    def _hmget(key: str, keys):
        if key.endswith(":quota:tpm:freezed_tokens") and keys == ["a"]:
            return ["10"]
        if key.endswith(":quota:tpm:locked_tokens") and keys == ["b"]:
            return ["20"]
        return ["0" for _ in keys]

    client.zrangebyscore = _zrangebyscore  # type: ignore[method-assign]
    client.hmget = _hmget  # type: ignore[method-assign]

    (
        _api_counts,
        _api_429_counts,
        _llm_counts,
        _api_by_model,
        _api_429_by_model,
        _llm_by_model,
        api_success_by_model,
        quota_tokens_by_model,
        rpm_used_by_model,
        rpd_used_by_model,
        _api_daily_by_model,
    ) = _scan_counts(client, prefix="novel_tts", key_token_to_index={"k4": 4})

    assert api_success_by_model == {4: {"gemma": 1}}
    assert quota_tokens_by_model == {4: {"gemma": 30}}
    assert rpm_used_by_model == {4: {"gemma": 5}}
    assert rpd_used_by_model == {4: {"gemma": 9}}


def test_scan_counts_does_not_count_success_without_locked_tokens() -> None:
    client = FakeRedis(
        now_seconds=1000.0,
        zcount_by_key={
            "novel_tts:novel:k1:gemini:api:reqs": 2,
            "novel_tts:novel:k1:gemini:llm:reqs": 2,
            "novel_tts:novel:k1:gemini:quota:tpm:locked": 2,
        },
    )

    (
        api_counts,
        _api_429_counts,
        llm_counts,
        api_by_model,
        _api_429_by_model,
        llm_by_model,
        api_success_by_model,
        quota_tokens_by_model,
        _rpm_used_by_model,
        _rpd_used_by_model,
        _api_daily_by_model,
    ) = _scan_counts(client, prefix="novel_tts", key_token_to_index={"k1": 1})

    assert api_counts == {1: 2}
    assert llm_counts == {1: 2}
    assert api_by_model == {1: {"gemini": 2}}
    assert llm_by_model == {1: {"gemini": 2}}
    assert api_success_by_model == {}
    assert quota_tokens_by_model == {}
