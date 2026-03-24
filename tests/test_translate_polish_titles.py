from novel_tts.translate.polish import normalize_text


def test_normalize_text_folds_short_chapter_title_into_heading() -> None:
    raw = "Chương 1\n\nTỷ thay gả\n\nPhương Thần bước vào đại điện."
    out = normalize_text(raw, chapter_num="1")
    assert out == "Chương 1: Tỷ thay gả.\n\nPhương Thần bước vào đại điện.\n"


def test_normalize_text_keeps_question_mark_for_title() -> None:
    raw = "Chương 1\n\nCái gì?\n\nPhương Thần ngẩng đầu nhìn lên."
    out = normalize_text(raw, chapter_num="1")
    assert out == "Chương 1: Cái gì?\n\nPhương Thần ngẩng đầu nhìn lên.\n"


def test_normalize_text_preserves_inline_heading_title() -> None:
    raw = "Chương 1041: Dùng máu viết chữ Ừm?\n\nPhương Thần trợn tròn mắt."
    out = normalize_text(raw, chapter_num="1041")
    assert out == "Chương 1041: Dùng máu viết chữ Ừm?\n\nPhương Thần trợn tròn mắt.\n"


def test_normalize_text_preserves_inline_heading_title_with_period() -> None:
    raw = "Chương 1: Tỷ Thay Giá.\n\nPhương Thần bước vào đại điện."
    out = normalize_text(raw, chapter_num="1")
    assert out == "Chương 1: Tỷ Thay Giá.\n\nPhương Thần bước vào đại điện.\n"


def test_normalize_text_dedupes_duplicate_inline_heading_title() -> None:
    raw = "Chương 71: Đắc tội người không nên đắc tội. Đắc tội người không nên đắc tội.\n\nNội dung tiếp theo."
    out = normalize_text(raw, chapter_num="71")
    assert out == "Chương 71: Đắc tội người không nên đắc tội.\n\nNội dung tiếp theo.\n"


def test_normalize_text_merges_split_inline_heading_title() -> None:
    raw = "Chương 222: Không Ai Là. Đối Thủ.\n\nNội dung tiếp theo."
    out = normalize_text(raw, chapter_num="222")
    assert out == "Chương 222: Không Ai Là Đối Thủ.\n\nNội dung tiếp theo.\n"


def test_normalize_text_keeps_first_variant_when_heading_has_two_declarative_titles() -> None:
    raw = "Chương 714: Đến Cực Kỳ Vô Sỉ. Đến mức vô sỉ tột độ.\n\nNội dung tiếp theo."
    out = normalize_text(raw, chapter_num="714")
    assert out == "Chương 714: Đến Cực Kỳ Vô Sỉ.\n\nNội dung tiếp theo.\n"


def test_normalize_text_merges_lowercase_continuation_before_deduping_title() -> None:
    raw = "Chương 68: Sự khác biệt trong cách. đối xử với hai tỷ muội. Đối đãi khác biệt của hai tỷ muội.\n\nNội dung tiếp theo."
    out = normalize_text(raw, chapter_num="68")
    assert out == "Chương 68: Sự khác biệt trong cách đối xử với hai tỷ muội.\n\nNội dung tiếp theo.\n"


def test_normalize_text_keeps_non_title_first_sentence_as_body() -> None:
    raw = "Chương 1\n\nPhương Thần đang suy tư.\n\nLàn sương tím dần tan đi."
    out = normalize_text(raw, chapter_num="1")
    assert out == "Chương 1\n\nPhương Thần đang suy tư.\n\nLàn sương tím dần tan đi.\n"


def test_normalize_text_splits_inline_title_from_body() -> None:
    raw = (
        "Chương 1\n\nNguyệt Minh Châu từ biệt Đối với Phương Thần hoàn toàn không biết gì về việc này, "
        "lén lút lấy ra Ngọc Điệp Đổi Mệnh.\n\nPhần tiếp theo."
    )
    out = normalize_text(raw, chapter_num="1")
    assert out == (
        "Chương 1: Nguyệt Minh Châu từ biệt.\n\n"
        "Đối với Phương Thần hoàn toàn không biết gì về việc này, lén lút lấy ra Ngọc Điệp Đổi Mệnh.\n\n"
        "Phần tiếp theo.\n"
    )


def test_normalize_text_keeps_long_body_line_after_heading() -> None:
    raw = (
        "Chương 1\n\nNguyệt Minh Châu từ biệt Đối với Phương Thần hoàn toàn không biết gì về việc này, "
        "lén lút lấy ra Ngọc Điệp Đổi Mệnh.\n\nPhần tiếp theo."
    )
    out = normalize_text(raw, chapter_num="1")
    assert out.startswith("Chương 1: Nguyệt Minh Châu từ biệt.")


def test_normalize_text_allows_question_title_with_comma() -> None:
    raw = "Chương 1\n\nBởi vì không tranh được Phương Thần, nên tức đến phát bệnh?\n\nPhần tiếp theo."
    out = normalize_text(raw, chapter_num="1")
    assert out == "Chương 1: Bởi vì không tranh được Phương Thần, nên tức đến phát bệnh?\n\nPhần tiếp theo.\n"
