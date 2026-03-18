from __future__ import annotations

from novel_tts.translate.polish import normalize_text


def test_normalize_text_dedupes_repeated_phrases() -> None:
    raw = (
        "Tập đoàn tập đoàn Trác Hàng do Lộ Thiên Chương sáng lập.\n\n"
        "Lộ Vũ Đồng hiện đang làm việc tại một khách sạn khách sạn năm sao thuộc tập đoàn tập đoàn Trác Hàng.\n"
    )
    out = normalize_text(raw, chapter_num="1")

    lowered = out.lower()
    assert "tập đoàn tập đoàn" not in lowered
    assert "khách sạn khách sạn" not in lowered
    assert "Tập đoàn Trác Hàng" in out
    assert "khách sạn năm sao" in lowered


def test_normalize_text_keeps_intentional_single_word_emphasis() -> None:
    raw = "Anh ấy rất rất vui.\n"
    out = normalize_text(raw, chapter_num="1")

    assert "rất rất" in out.lower()


def test_normalize_text_replaces_haizz() -> None:
    raw = "Haizz, đúng là mệt.\n"
    out = normalize_text(raw, chapter_num="1")

    assert "Hầy" in out
    assert "Haizz" not in out
