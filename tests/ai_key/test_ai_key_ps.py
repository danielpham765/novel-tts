from __future__ import annotations

from novel_tts.ai_key.service import (
    _extract_key_token,
    _extract_key_token_and_model_for_429,
    _parse_filter_values,
    _select_indices,
)


def test_parse_filter_values_splits_and_dedupes() -> None:
    assert _parse_filter_values(["k1,k2", " k2 ", ""]) == ["k1", "k2"]
    assert _parse_filter_values([" 1 , 2 ", "2,3"]) == ["1", "2", "3"]


def test_extract_key_token_from_redis_key() -> None:
    assert _extract_key_token("novel_tts:k1:gemma:quota:reqs") == "k1"
    assert _extract_key_token("novel_tts:k12:gemma:api:reqs") == "k12"
    assert _extract_key_token("novel_tts:k12:gemma:api:calls") == "k12"
    assert _extract_key_token("novel_tts:k12:gemma:api:429") == "k12"
    assert _extract_key_token("novel_tts:gemma:quota:reqs") == ""


def test_extract_key_token_and_model_for_429() -> None:
    token, model = _extract_key_token_and_model_for_429("novel_tts:k3:gemini-3.1:api:429")
    assert token == "k3"
    assert model == "gemini-3.1"


def test_select_indices_union_and_unknown_raw() -> None:
    keys = ["AAA1111", "BBB2222", "CCC3333"]
    selected, unknown = _select_indices(
        keys,
        filter_tokens=["k1", "3333"],
        filter_raw_tokens=["BBB2222", "MISSING"],
    )
    assert unknown == 1
    assert selected == {1, 2, 3}


def test_select_indices_all_when_no_filters() -> None:
    keys = ["AAA1111"]
    selected, unknown = _select_indices(keys, filter_tokens=[], filter_raw_tokens=[])
    assert unknown == 0
    assert selected is None
