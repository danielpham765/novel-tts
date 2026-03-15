from __future__ import annotations

from novel_tts.ai_key.service import (
    _extract_key_index,
    _parse_filter_values,
    _select_indices,
)


def test_parse_filter_values_splits_and_dedupes() -> None:
    assert _parse_filter_values(["k1,k2", " k2 ", ""]) == ["k1", "k2"]
    assert _parse_filter_values([" 1 , 2 ", "2,3"]) == ["1", "2", "3"]


def test_extract_key_index_from_redis_key() -> None:
    assert _extract_key_index("novel_tts:novel:k1:gemma:quota:reqs") == 1
    assert _extract_key_index("novel_tts:novel:k12:gemma:api:reqs") == 12
    assert _extract_key_index("novel_tts:novel:k12:gemma:api:429") == 12
    assert _extract_key_index("novel_tts:novel:gemma:quota:reqs") is None


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
