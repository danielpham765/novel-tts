from __future__ import annotations

from novel_tts.translate.novel import PLACEHOLDER_LIKE_RE, PLACEHOLDER_TOKEN_RE, make_placeholders


def test_make_placeholders_skips_corrupted_target_with_placeholder_token() -> None:
    glossary = {
        "上京之变": "Biến cố ZXQ125QXZ",
        "上京": "Thượng Kinh",
    }
    masked, mapping = make_placeholders("上京之变 xảy ra ở 上京", glossary)

    # Corrupted entry must not be placeholdered (prevents poisoning restore_placeholders()).
    assert "Biến cố ZXQ125QXZ" not in mapping.values()
    assert all(not PLACEHOLDER_LIKE_RE.search(v) for v in mapping.values())

    # Normal entry is still placeholdered.
    assert PLACEHOLDER_TOKEN_RE.search(masked)
    assert any(v == "Thượng Kinh" for v in mapping.values())


def test_make_placeholders_skips_mangled_placeholder_variant() -> None:
    glossary = {
        "沧南二中": "ZXQ731QTrường Trung Học Số Hai",
        "上京": "Thượng Kinh",
    }
    masked, mapping = make_placeholders("沧南二中 ở 上京", glossary)
    assert all(not PLACEHOLDER_LIKE_RE.search(v) for v in mapping.values())
    assert any(v == "Thượng Kinh" for v in mapping.values())
    assert PLACEHOLDER_TOKEN_RE.search(masked)
