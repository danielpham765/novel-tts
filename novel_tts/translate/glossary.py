from __future__ import annotations

import re

HAN_RE = re.compile(r"[\u4e00-\u9fff]")
ALLOWED_SOURCE_PUNCT_RE = re.compile(r"[《》〈〉（）()·・「」『』、，,\s]")

COMMON_SOURCE_TERMS = {
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


def normalize_glossary_text(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


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


def sanitize_glossary_entries(entries: dict[str, str]) -> tuple[dict[str, str], dict[str, str]]:
    kept: dict[str, str] = {}
    dropped: dict[str, str] = {}
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
        if not _looks_like_long_term_proper_noun(source, target):
            dropped[source] = target
            continue
        kept[source] = target
    return kept, dropped
