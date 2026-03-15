from __future__ import annotations

from novel_tts.queue.translation_queue import _extract_target_from_argv, _format_target


def test_format_target_pads_part_number() -> None:
    assert _format_target("chuong_1-10", "4") == "chuong_1-10:0004"
    assert _format_target("caption_cn.srt", "0001") == "caption_cn.srt:0001"
    assert _format_target("/a/b/caption_cn.srt", "12") == "caption_cn.srt:0012"
    assert _format_target("", "1") == ""
    assert _format_target("file", "") == ""


def test_extract_target_from_argv() -> None:
    argv = ["novel-tts", "translate", "chapter", "novel", "--file", "chuong_1-10", "--chapter", "4"]
    assert _extract_target_from_argv(argv) == "chuong_1-10:0004"

