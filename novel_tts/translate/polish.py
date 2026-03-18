from __future__ import annotations

import re
from pathlib import Path

from novel_tts.common.logging import get_logger
from novel_tts.config.models import NovelConfig

LOGGER = get_logger(__name__)

POLISH_REPLACEMENTS = {
    "ChưCát": "Chư Cát",
    "quấy ràm": "quấy rầy",
    "fiancée": "hôn thê",
    "thê hôn thê": "hôn thê",
    "người người": "người",
    "một người người": "một người",
    "thực lực hàng ngũ": "hàng ngũ thực lực",
    "porád": "đàng hoàng",
    "Gia Cốt Nhược Trần": "Cố Nhược Trần",
    "Gỗ Nhược Trần": "Cố Nhược Trần",
    "Gúc Nhược Trần": "Cố Nhược Trần",
    "Gu Dược Trần": "Cố Nhược Trần",
    "Gu Nhược Trần": "Cố Nhược Trần",
    "Gỡ Nhược Trần": "Cố Nhược Trần",
    "Quý Nhược Trần": "Cố Nhược Trần",
    "Quý Nhược Đinh": "Cố Nhược Trần",
    "Cố Nhược Tuyền": "Cố Nhược Trần",
    "Túc Nhược Hàm": "Từ Nhược Hàm",
    "Song Chu Vy": "Tống Sở Vi",
    "Song Chư Vi": "Tống Sở Vi",
    "Tống Trứ Vi": "Tống Sở Vi",
    "Tống Trú Vi": "Tống Sở Vi",
    "Tống Trúc Vi": "Tống Sở Vi",
    "Tống Chúc Vy": "Tống Sở Vi",
    "Tống Chu Vy": "Tống Sở Vi",
    "Tống Thục Vy": "Tống Sở Vi",
    "Ouyang Kinh Lý": "Âu Dương Kinh lý",
    "Ouyang Kinh lý": "Âu Dương Kinh lý",
    "Ouyang Tuệ Tuyết": "Âu Dương Thụy Tuyết",
    "Âu Dương Tuyết Nhi": "Âu Dương Thụy Tuyết",
    "Châu Năng Sương": "Chu Ngưng Sương",
    "Châu Ngưng Sương": "Chu Ngưng Sương",
    "Chu Nùng Sương": "Chu Ngưng Sương",
    "Tổng Gúc": "Cố tổng",
    "Tổng Gỗ": "Cố tổng",
    "Cố Tổng": "Cố tổng",
    "Trương Kinh Lý": "Trương Kinh lý",
    "Tập đoàn Đông Giang": "tập đoàn Đông Giang",
    "Đông Giang Tập đoàn": "tập đoàn Đông Giang",
    "Tiểu Gỗ": "Tiểu Cố",
    "Gỗ Lão Bản": "Cố lão bản",
    "tiểu Quý": "tiểu Cố",
    "Lộ vũ đồng": "Lộ Vũ Đồng",
    "Ngô Ưu Kỳ": "Ngô Vũ Kỳ",
    "Nhụy Kỳ": "Vũ Kỳ",
    "Túc Nhược Hàm": "Từ Nhược Hàm",
    "ngư hương nhục丝": "ngư hương nhục ti",
    "“Gỗ... Cố tổng": "“Cố tổng",
    "Gu tiên sinh": "Cố tiên sinh",
    "Haizz": "Hầy",
    "haizz": "Hầy",
}

_ALLOWLIST_SINGLE_WORD_REPEATS = {
    # Common intentional emphasis/onomatopoeia in Vietnamese prose.
    "rất",
    "quá",
    "lắm",
    "thật",
    "đúng",
    "cực",
    "siêu",
    "hơi",
    "mãi",
}


def _dedupe_immediate_repeats(text: str) -> str:
    """
    Remove immediate duplicated words/phrases that commonly appear as LLM glitches.

    Examples:
      - "tập đoàn tập đoàn Trác Hàng" -> "tập đoàn Trác Hàng"
      - "khách sạn khách sạn năm sao" -> "khách sạn năm sao"

    Notes:
      - Multi-word repeats (>= 2 words) are always collapsed.
      - Single-word repeats are collapsed unless in an allowlist (e.g. "rất rất").
      - Only collapses repeats separated by spaces/tabs (won't cross newlines).
    """

    word = r"[A-Za-zÀ-Ỵà-ỹĐđ]+"

    # Multi-word phrases (2..6 words): collapse aggressively.
    multi_word_repeat = re.compile(
        rf"\b({word}(?:[ \t]+{word}){{1,5}})\b[ \t]+\1\b",
        flags=re.IGNORECASE,
    )
    while True:
        cleaned = multi_word_repeat.sub(r"\1", text)
        if cleaned == text:
            break
        text = cleaned

    # Single words: collapse with allowlist.
    single_word_repeat = re.compile(rf"\b({word})\b[ \t]+\1\b", flags=re.IGNORECASE)

    def _single_repl(match: re.Match[str]) -> str:
        token = match.group(1)
        if token.casefold() in _ALLOWLIST_SINGLE_WORD_REPEATS:
            return match.group(0)
        return token

    while True:
        cleaned = single_word_repeat.sub(_single_repl, text)
        if cleaned == text:
            break
        text = cleaned

    return text


def _chapter_numbers(raw: str, chapter_regex: str) -> list[str]:
    return [m.group(1) for m in re.finditer(chapter_regex, raw, flags=re.M)]


def _normalize_heading(text: str, chapter_num: str) -> str:
    text = text.replace("\r\n", "\n").replace("\r", "\n").strip()
    text = re.sub(r"^\s*Chương\s+\d+[^\n]*\n*", "", text, count=1)
    text = re.sub(r"^\s*第\s*\d+\s*章[^\n]*\n*", "", text, count=1)
    text = f"Chương {int(chapter_num)}\n\n{text.strip()}" if text.strip() else f"Chương {int(chapter_num)}"
    text = re.sub(
        rf"^Chương\s+{int(chapter_num)}[:：]?\s+(.+)$",
        rf"Chương {int(chapter_num)}\n\n\1",
        text,
        count=1,
    )
    text = re.sub(rf"^Chương\s+{int(chapter_num)}[:：]$", rf"Chương {int(chapter_num)}", text, flags=re.M)
    return text


def _rebalance_paragraph(line: str, max_len: int = 360) -> list[str]:
    line = line.strip()
    if len(line) <= max_len:
        return [line]
    sentences = re.split(r'(?<=[.!?…])\s+(?=[“"A-ZÀ-Ỵ])', line)
    if len(sentences) == 1:
        sentences = re.split(r'(?=\s*[“"])|(?<=[:;])\s+', line)
    chunks: list[str] = []
    current = ""
    for sentence in sentences:
        sentence = sentence.strip()
        if not sentence:
            continue
        if not current:
            current = sentence
            continue
        if len(current) + 1 + len(sentence) <= max_len:
            current = f"{current} {sentence}"
        else:
            chunks.append(current.strip())
            current = sentence
    if current:
        chunks.append(current.strip())
    final_chunks: list[str] = []
    for chunk in chunks:
        if len(chunk) <= max_len:
            final_chunks.append(chunk)
            continue
        parts = re.split(r'(?<=[.!?…])\s+|(?=[“"])', chunk)
        cur = ""
        for part in parts:
            part = part.strip()
            if not part:
                continue
            if not cur:
                cur = part
            elif len(cur) + 1 + len(part) <= max_len:
                cur = f"{cur} {part}"
            else:
                final_chunks.append(cur.strip())
                cur = part
        if cur:
            final_chunks.append(cur.strip())
    return final_chunks


def _merge_broken_paragraphs(lines: list[str]) -> list[str]:
    merged: list[str] = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        if not merged:
            merged.append(line)
            continue
        prev = merged[-1]
        if (
            prev.endswith((":", ",", "“", "(", "-", "…"))
            or re.match(r"^[a-zà-ỹ]", line)
            or (len(line) < 70 and not re.match(r"^[“\"A-ZÀ-Ỵ]", line))
        ):
            merged[-1] = f"{prev} {line}"
        else:
            merged.append(line)
    return merged


def normalize_text(text: str, chapter_num: str) -> str:
    text = text.replace("QZXBRQ", "\n\n")
    text = _normalize_heading(text, chapter_num)
    for src, dst in POLISH_REPLACEMENTS.items():
        text = text.replace(src, dst)
    text = re.sub(r"loadAdv\(\d+,\s*\d+\);\s*", "", text)
    text = re.sub(r"[“\"]?Gỗ\.\.\.\s*Cố tổng", "“Cố tổng", text)
    text = re.sub(r"([^\n])([“\"])", r"\1 \2", text)
    text = re.sub(r'(?m)^(chương)\s+(?=[“"\'(])', "", text)
    # Break glued narration/dialogue around closing quotes.
    text = re.sub(r'([.!?…])”([A-ZÀ-Ỵ])', r'\1”\n\n\2', text)
    text = re.sub(r'([.!?…])"([A-ZÀ-Ỵ])', r'\1"\n\n\2', text)
    text = re.sub(r'([.!?…])”([“"])', r'\1”\n\n\2', text)
    text = re.sub(r'([.!?…])"([“"])', r'\1"\n\n\2', text)
    text = _dedupe_immediate_repeats(text)

    text = re.sub(r"(?m)^[ \t]+", "", text)
    text = re.sub(r"[ \t]+\n", "\n", text)
    text = re.sub(r"([.!?…,:;])([A-Za-zÀ-ỹ“\"'])", r"\1 \2", text)
    text = re.sub(r"([^\n])\.\.\.\.\.\.+", r"\1……", text)
    text = re.sub(r"(?m)^[ \t]*[“\"]([^\n]+)$", r"“\1", text)
    text = re.sub(r"[ \t]{2,}", " ", text)
    text = re.sub(r"(?m)^\s+$", "", text)
    text = re.sub(rf"^Chương\s+{int(chapter_num)}\n(?!\n)", rf"Chương {int(chapter_num)}\n\n", text, flags=re.M)
    raw_lines = [line.strip() for line in text.splitlines() if line.strip()]
    lines: list[str] = []
    for idx, line in enumerate(raw_lines):
        if idx == 0 and re.fullmatch(rf"Chương\s+{int(chapter_num)}", line):
            lines.append(line)
            continue
        lines.extend(_rebalance_paragraph(line))
    lines = _merge_broken_paragraphs(lines)
    text = "\n\n".join(lines)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip() + "\n"


def polish_translations(config: NovelConfig, filenames: list[str] | None = None) -> tuple[int, int]:
    changed_parts = 0
    rebuilt_files = 0
    files = sorted(config.storage.origin_dir.glob("*.txt"))
    if filenames:
        wanted = set(filenames)
        files = [path for path in files if path.name in wanted]
    config.storage.translated_dir.mkdir(parents=True, exist_ok=True)
    for origin_path in files:
        chapter_nums = _chapter_numbers(origin_path.read_text(encoding="utf-8"), config.translation.chapter_regex)
        part_dir = config.storage.parts_dir / origin_path.stem
        merged_parts: list[str] = []
        for chapter_num in chapter_nums:
            part_path = part_dir / f"{int(chapter_num):04d}.txt"
            if not part_path.exists():
                continue
            original = part_path.read_text(encoding="utf-8")
            polished = normalize_text(original, chapter_num)
            if polished != original:
                part_path.write_text(polished, encoding="utf-8")
                changed_parts += 1
            merged_parts.append(polished.strip())
        if merged_parts:
            output = merged_parts[0].strip()
            if len(merged_parts) > 1:
                output += "\n\n\n" + "\n\n\n".join(part.strip() for part in merged_parts[1:])
            output = output.strip() + "\n"
            (config.storage.translated_dir / origin_path.name).write_text(output, encoding="utf-8")
            rebuilt_files += 1
    LOGGER.info("Polished translations | changed_parts=%s rebuilt_files=%s", changed_parts, rebuilt_files)
    return changed_parts, rebuilt_files
