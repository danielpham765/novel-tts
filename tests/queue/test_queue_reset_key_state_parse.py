from __future__ import annotations

import pytest

from novel_tts.queue.translation_queue import _resolve_key_indices, _split_csv_flags


def test_split_csv_flags() -> None:
    assert _split_csv_flags(["k1,k2", " k3 "]) == ["k1", "k2", "k3"]
    assert _split_csv_flags(["", "  "]) == []


def test_resolve_key_indices_kN() -> None:
    keys = ["rawA", "rawB", "rawC"]
    assert _resolve_key_indices(["k1", "k3"], keys) == [1, 3]


def test_resolve_key_indices_raw_key_exact_match() -> None:
    keys = ["rawA", "rawB", "rawC"]
    assert _resolve_key_indices(["rawB"], keys) == [2]


def test_resolve_key_indices_dedupes_preserving_order() -> None:
    keys = ["rawA", "rawB", "rawC"]
    assert _resolve_key_indices(["k2", "rawB", "k2"], keys) == [2]


def test_resolve_key_indices_raises_on_unknown_raw() -> None:
    keys = ["rawA"]
    with pytest.raises(ValueError, match="Unknown raw key"):
        _resolve_key_indices(["missing"], keys)


def test_resolve_key_indices_raises_on_out_of_range() -> None:
    keys = ["rawA"]
    with pytest.raises(ValueError, match="expected 1..1"):
        _resolve_key_indices(["k2"], keys)

