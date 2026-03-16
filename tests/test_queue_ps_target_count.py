from __future__ import annotations

from novel_tts.queue.translation_queue import _unique_target_count


def test_unique_target_count_prefers_translate_chapter_targets() -> None:
    rows = [
        {"role": "worker", "target": "chuong_1-10:0004"},
        {"role": "translate-chapter", "target": "chuong_1-10:0004"},
        {"role": "translate-chapter", "target": "chuong_1-10:0004"},
        {"role": "translate-chapter", "target": "chuong_1-10:0005"},
        {"role": "worker", "target": "chuong_1-10:0005"},
    ]
    assert _unique_target_count(rows) == 2


def test_unique_target_count_falls_back_when_no_translate_chapter_rows() -> None:
    rows = [
        {"role": "worker", "target": "chuong_1-10:0004"},
        {"role": "worker", "target": "chuong_1-10:0004"},
        {"role": "worker", "target": "chuong_1-10:0005"},
        {"role": "monitor", "target": ""},
    ]
    assert _unique_target_count(rows) == 2

