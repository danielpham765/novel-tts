from novel_tts.translate.novel import (
    find_fake_virtual_fishing_rod_lines,
    repair_fake_virtual_fishing_rod_artifacts,
)


def test_detects_fake_virtual_fishing_rod_when_source_has_none():
    source_text = "有些事，我需要当面向你们说清楚。"
    translated = "“Có việc, ta cần câu Hư Không muốn nói thẳng với các ngươi.”"

    assert find_fake_virtual_fishing_rod_lines(translated, source_text=source_text) == [1]


def test_keeps_real_virtual_fishing_rod_mentions_up_to_source_count():
    source_text = "他取出了虚空鱼竿。随后又收起虚空鱼竿。"
    translated = "\n".join(
        [
            "Hắn lấy ra cần câu Hư Không.",
            "Sau đó hắn thu hồi cần câu Hư Không.",
            "Nhưng ta chỉ cần câu Hư Không một chút thời gian thôi.",
        ]
    )

    assert find_fake_virtual_fishing_rod_lines(translated, source_text=source_text) == [3]


def test_repairs_common_fake_virtual_fishing_rod_patterns():
    source_text = "\n".join(
        [
            "有些事，我需要当面向你们说清楚。",
            "不需要你当打手。",
            "我只需要你在我遇到不可抵抗的危险时出手救我即可！",
            "至少需要一顆七品中等大丹才行。",
        ]
    )
    translated = "\n".join(
        [
            "“Có việc, ta cần câu Hư Không muốn nói thẳng với các ngươi.”",
            "“Không cần câu Hư Không ngươi làm tay sai.”",
            "“Ta chỉ cần câu Hư Không, ngươi ra tay cứu ta là được!”",
            "“Ít nhất phải có một viên Đại Đan thất phẩm trung đẳng cần câu Hư Không.”",
        ]
    )

    fixed = repair_fake_virtual_fishing_rod_artifacts(translated, source_text=source_text)

    assert "cần câu Hư Không" not in fixed
    assert "Có việc, ta cần nói thẳng với các ngươi." in fixed
    assert "Không cần ngươi làm tay sai." in fixed
    assert "Ta chỉ cần ngươi ra tay cứu ta là được!" in fixed
