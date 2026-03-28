from __future__ import annotations

import re

HAN_RE = re.compile(r"[\u4e00-\u9fff]")
ALLOWED_SOURCE_PUNCT_RE = re.compile(r"[《》〈〉（）()·・「」『』、，,\s]")
SOURCE_VARIANT_CHAR_MAP = {
    "群": "羣",
    "羣": "群",
}

COMMON_SOURCE_TERMS = {
    "不清楚",
    "不明白",
    "故人",
    "故友",
    "友人",
    "熟人",
    "路人",
    "親人",
    "亲人",
    "爱人",
    "愛人",
    "恩人",
    "污血",
    "乌云",
    "烏雲",
    "鮮血",
    "鲜血",
    "淚水",
    "泪水",
    "滾滾",
    "滚滚",
    "圓滾滾",
    "圆滚滚",
    "办公室",
    "辦公室",
    "经理",
    "經理",
    "总经理",
    "總經理",
    "助理",
    "秘书",
    "秘書",
    "主任",
    "部长",
    "部長",
    "老师",
    "老師",
    "同学",
    "同學",
    "领导",
    "領導",
    "公司",
    "集团",
    "集團",
    "部门",
    "部門",
    "学校",
    "學校",
    "大学",
    "大學",
    "超市",
    "会员超市",
    "會員超市",
    "宿舍",
    "寝室",
    "寢室",
    "房间",
    "房間",
    "客厅",
    "客廳",
    "卧室",
    "臥室",
    "手机",
    "手機",
    "电梯",
    "電梯",
    "食堂",
    "饭店",
    "飯店",
    "酒店",
    "商品房",
    "客卧",
    "客臥",
    "宿舍樓",
    "宿舍楼",
    "宿舍樓下",
    "宿舍楼下",
    "宾馆",
    "賓館",
}

COMMON_TARGET_TERMS = {
    "văn phòng",
    "quản lý",
    "tổng giám đốc",
    "trợ lý",
    "chủ nhiệm",
    "bộ trưởng",
    "thầy",
    "cô",
    "đồng học",
    "lãnh đạo",
    "công ty",
    "tập đoàn",
    "bộ phận",
    "trường học",
    "đại học",
    "siêu thị",
    "siêu thị thành viên",
    "ký túc xá",
    "phòng ngủ",
    "phòng khách",
    "điện thoại",
    "thang máy",
    "căng tin",
    "nhà hàng",
    "khách sạn",
    "nhà ở thương mại",
    "phòng cho khách",
}

SUSPICIOUS_TARGET_FRAGMENTS = {
    "ám chỉ",
    "tên công ty",
    "quý khách hàng",
}

PLACEHOLDER_LIKE_TARGET_RE = re.compile(r"(?:ZXQ|QZX)\d{1,6}Q(?:XZ)?")
PROPER_NOUN_SUFFIXES = {
    "大学",
    "大學",
    "学院",
    "學院",
    "中学",
    "中學",
    "小学",
    "小學",
    "集团",
    "集團",
    "公司",
    "分公司",
    "俱乐部",
    "俱樂部",
    "酒店",
    "饭店",
    "飯店",
    "餐厅",
    "餐廳",
    "银行",
    "銀行",
    "广场",
    "廣場",
    "小区",
    "小區",
    "别墅",
    "別墅",
    "公园",
    "公園",
    "路",
    "街",
    "巷",
    "村",
    "镇",
    "鎮",
    "市",
    "省",
    "区",
    "區",
    "县",
    "縣",
    "山",
    "江",
    "湖",
    "湾",
    "灣",
    "岛",
    "島",
    "桥",
    "橋",
}

PERSON_TITLE_SUFFIXES = {
    "师弟",
    "師弟",
    "师兄",
    "師兄",
    "师姐",
    "師姐",
    "师妹",
    "師妹",
    "小姐",
    "先生",
    "公子",
    "少爷",
    "少爺",
    "少主",
    "宗主",
    "掌门",
    "掌門",
    "长老",
    "長老",
    "峰主",
    "堂主",
    "城主",
    "家主",
    "前辈",
    "前輩",
    "道友",
    "仙子",
}

TARGET_TITLE_HINTS = {
    "sư đệ",
    "sư huynh",
    "sư tỷ",
    "sư tỉ",
    "sư muội",
    "tiểu thư",
    "tiên sinh",
    "công tử",
    "thiếu gia",
    "thiếu chủ",
    "tông chủ",
    "chưởng môn",
    "trưởng lão",
    "phong chủ",
    "đường chủ",
    "thành chủ",
    "gia chủ",
    "tiền bối",
    "đạo hữu",
    "tiên tử",
}


def _word_starts_capitalized(word: str) -> bool:
    for ch in word:
        if ch.isalpha():
            return ch.isupper()
    return False


def _looks_like_blocked_target(target: str) -> bool:
    normalized = normalize_glossary_text(target)
    if not normalized:
        return False
    lowered = normalized.lower()
    if " của " in lowered:
        return True
    if any(hint in lowered for hint in TARGET_TITLE_HINTS):
        return True
    words = _target_words(normalized)
    if len(words) >= 2 and sum(1 for word in words[:3] if _word_starts_capitalized(word)) >= 2:
        return True
    return False


def blocked_glossary_targets(kept: dict[str, str], dropped: dict[str, str]) -> list[str]:
    kept_targets = {normalize_glossary_text(value) for value in kept.values() if normalize_glossary_text(value)}
    blocked: list[str] = []
    seen: set[str] = set()
    for target in dropped.values():
        normalized = normalize_glossary_text(target)
        if not normalized or normalized in kept_targets:
            continue
        if not _looks_like_blocked_target(normalized):
            continue
        if normalized in seen:
            continue
        seen.add(normalized)
        blocked.append(normalized)
    return blocked


def normalize_glossary_text(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def source_text_variants(text: str) -> list[str]:
    normalized = normalize_glossary_text(text)
    if not normalized:
        return []
    variants = {normalized}
    frontier = {normalized}
    while frontier:
        current = frontier.pop()
        for idx, ch in enumerate(current):
            alt = SOURCE_VARIANT_CHAR_MAP.get(ch, "")
            if not alt:
                continue
            candidate = current[:idx] + alt + current[idx + 1 :]
            if candidate in variants:
                continue
            variants.add(candidate)
            frontier.add(candidate)
    return sorted(variants, key=len, reverse=True)


def build_glossary_text(entries: dict[str, str]) -> str:
    lines = []
    for source, target in entries.items():
        source = normalize_glossary_text(source)
        target = normalize_glossary_text(target)
        if not source or not target:
            continue
        lines.append(f"- {source} = {target}")
    return "\n".join(lines)


def _is_han_dominant_source(source: str) -> bool:
    stripped = ALLOWED_SOURCE_PUNCT_RE.sub("", source)
    return bool(stripped) and all(HAN_RE.fullmatch(ch) for ch in stripped)


def _is_chapter_or_numeric_entry(source: str) -> bool:
    if any(ch.isdigit() for ch in source):
        return True
    if source.startswith("第") and "章" in source:
        return True
    return False


def _looks_like_proper_target(target: str) -> bool:
    words = [part for part in re.split(r"\s+", target) if part]
    capitalized_words = 0
    for word in words:
        for ch in word:
            if ch.isalpha():
                if ch.isupper():
                    capitalized_words += 1
                break
    if capitalized_words >= 2:
        return True
    if capitalized_words == 1 and len(words) == 1:
        return True
    if target.startswith("《") and target.endswith("》"):
        return True
    return False


def _looks_like_long_term_proper_noun(source: str, target: str) -> bool:
    if any(source.endswith(suffix) for suffix in PROPER_NOUN_SUFFIXES):
        return True
    if len(source) in {2, 3, 4} and _looks_like_proper_target(target):
        return True
    if len(source) <= 8 and _looks_like_proper_target(target):
        return True
    return False


def _target_head_word(target: str) -> str:
    parts = [part for part in re.split(r"\s+", target.strip()) if part]
    if not parts:
        return ""
    word = re.sub(r"^[^\wÀ-ỹ]+|[^\wÀ-ỹ]+$", "", parts[0], flags=re.UNICODE)
    return word


def _person_title_suffix(source: str) -> str:
    for suffix in sorted(PERSON_TITLE_SUFFIXES, key=len, reverse=True):
        if len(source) > len(suffix) and source.endswith(suffix):
            return suffix
    return ""


def _is_person_title_hybrid(source: str) -> bool:
    if not source or not _is_han_dominant_source(source):
        return False
    return bool(_person_title_suffix(source))


def _target_preserves_person_title(target: str) -> bool:
    lowered = normalize_glossary_text(target).lower()
    return any(hint in lowered for hint in TARGET_TITLE_HINTS)


def _build_name_head_votes(entries: dict[str, str]) -> dict[str, dict[str, int]]:
    votes: dict[str, dict[str, int]] = {}
    for source, target in entries.items():
        if not source or not target:
            continue
        if not _is_han_dominant_source(source):
            continue
        if len(source) < 2 or len(source) > 6:
            continue
        if _is_person_title_hybrid(source):
            continue
        if not _looks_like_proper_target(target):
            continue
        head = _target_head_word(target)
        if not head:
            continue
        bucket = votes.setdefault(source[0], {})
        bucket[head] = bucket.get(head, 0) + 1
    return votes


def _dominant_name_heads(entries: dict[str, str]) -> dict[str, str]:
    votes = _build_name_head_votes(entries)
    dominant: dict[str, str] = {}
    for first_char, bucket in votes.items():
        if not bucket:
            continue
        winner, winner_count = max(bucket.items(), key=lambda item: item[1])
        total = sum(bucket.values())
        if winner_count >= 3 and winner_count / max(1, total) >= 0.7:
            dominant[first_char] = winner
    return dominant


def _target_words(target: str) -> list[str]:
    return [part for part in re.split(r"\s+", normalize_glossary_text(target)) if part]


def _build_char_reading_votes(entries: dict[str, str]) -> dict[str, dict[str, int]]:
    votes: dict[str, dict[str, int]] = {}
    for source, target in entries.items():
        if not source or not target:
            continue
        if not _is_han_dominant_source(source):
            continue
        if _is_person_title_hybrid(source):
            continue
        if len(source) < 1 or len(source) > 4:
            continue
        words = _target_words(target)
        if len(words) != len(source):
            continue
        if not _looks_like_proper_target(target):
            continue
        for idx, ch in enumerate(source):
            word = _target_head_word(words[idx])
            if not word:
                continue
            bucket = votes.setdefault(ch, {})
            bucket[word] = bucket.get(word, 0) + 1
    return votes


def _dominant_char_readings(entries: dict[str, str]) -> dict[str, str]:
    votes = _build_char_reading_votes(entries)
    dominant: dict[str, str] = {}
    for ch, bucket in votes.items():
        if not bucket:
            continue
        winner, winner_count = max(bucket.items(), key=lambda item: item[1])
        total = sum(bucket.values())
        if winner_count >= 2 and winner_count / max(1, total) >= 0.75:
            dominant[ch] = winner
    return dominant


def _violates_name_head_consistency(source: str, target: str, dominant_heads: dict[str, str]) -> bool:
    if not source or not target:
        return False
    if not _is_han_dominant_source(source):
        return False
    if len(source) < 2 or len(source) > 6:
        return False
    if _is_person_title_hybrid(source):
        return False
    if not _looks_like_proper_target(target):
        return False
    expected = dominant_heads.get(source[0], "")
    actual = _target_head_word(target)
    if not expected or not actual:
        return False
    return actual != expected


def _violates_char_reading_consistency(source: str, target: str, char_readings: dict[str, str]) -> bool:
    if not source or not target:
        return False
    if not _is_han_dominant_source(source):
        return False
    if _is_person_title_hybrid(source):
        return False
    if len(source) < 2 or len(source) > 4:
        return False
    words = _target_words(target)
    if len(words) != len(source):
        return False
    known = 0
    mismatches = 0
    for idx, ch in enumerate(source):
        expected = char_readings.get(ch, "")
        actual = _target_head_word(words[idx])
        if not expected or not actual:
            continue
        known += 1
        if expected != actual:
            mismatches += 1
    if known < 2:
        return False
    return mismatches >= 2


def _is_suspicious_target(target: str) -> bool:
    lowered = target.lower()
    # Drop placeholder-like tokens that indicate glossary corruption (e.g. "ZXQ1156QXZ").
    # Also drop partially-mangled variants like "ZXQ731QTrường ..." (missing trailing "XZ").
    if PLACEHOLDER_LIKE_TARGET_RE.search(target):
        return True
    if "?" in target:
        return True
    if "(" in target or "（" in target:
        return True
    if "," in target or ";" in target:
        return True
    if any(fragment in lowered for fragment in SUSPICIOUS_TARGET_FRAGMENTS):
        return True
    return False


def is_common_glossary_entry(source: str, target: str) -> bool:
    source = normalize_glossary_text(source)
    target = normalize_glossary_text(target).lower()
    if source in COMMON_SOURCE_TERMS:
        return True
    if target in COMMON_TARGET_TERMS:
        return True
    return False


def sanitize_glossary_entries(
    entries: dict[str, str],
    *,
    mode: str = "default",
    context_entries: dict[str, str] | None = None,
) -> tuple[dict[str, str], dict[str, str]]:
    kept: dict[str, str] = {}
    dropped: dict[str, str] = {}
    normalized_mode = normalize_glossary_text(mode).lower() or "default"
    for source, target in entries.items():
        source = normalize_glossary_text(source)
        target = normalize_glossary_text(target)
        if not source or not target:
            dropped[source] = target
            continue
        if not _is_han_dominant_source(source):
            dropped[source] = target
            continue
        if _is_chapter_or_numeric_entry(source):
            dropped[source] = target
            continue
        if is_common_glossary_entry(source, target):
            dropped[source] = target
            continue
        if _is_suspicious_target(target):
            dropped[source] = target
            continue
        if _target_preserves_person_title(target) and not _is_person_title_hybrid(source):
            dropped[source] = target
            continue
        if normalized_mode == "auto" and _is_person_title_hybrid(source):
            if not _target_preserves_person_title(target):
                dropped[source] = target
                continue
            dropped[source] = target
            continue
        if normalized_mode == "runtime" and _is_person_title_hybrid(source):
            if not _target_preserves_person_title(target):
                dropped[source] = target
                continue
        if not _looks_like_long_term_proper_noun(source, target):
            dropped[source] = target
            continue
        kept[source] = target

    if kept and normalized_mode in {"auto", "runtime"}:
        context = dict(context_entries or {})
        context.update(kept)
        dominant_heads = _dominant_name_heads(context)
        char_readings = _dominant_char_readings(context)
        if dominant_heads:
            filtered: dict[str, str] = {}
            for source, target in kept.items():
                if _violates_name_head_consistency(source, target, dominant_heads):
                    dropped[source] = target
                    continue
                if char_readings and _violates_char_reading_consistency(source, target, char_readings):
                    dropped[source] = target
                    continue
                filtered[source] = target
            kept = filtered
    return kept, dropped
