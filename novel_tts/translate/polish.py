from __future__ import annotations

import re
from difflib import SequenceMatcher
from pathlib import Path

from novel_tts.common.logging import get_logger
from novel_tts.config.models import NovelConfig

LOGGER = get_logger(__name__)

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

_INLINE_TITLE_BODY_STARTERS = {
    "ai",
    "bởi",
    "chỉ",
    "chính",
    "cho",
    "đây",
    "đối",
    "hắn",
    "khi",
    "liền",
    "lúc",
    "lại",
    "mọi",
    "một",
    "ngay",
    "nàng",
    "nếu",
    "nhưng",
    "phía",
    "sau",
    "theo",
    "trên",
    "trong",
    "tuy",
    "vì",
    "vừa",
}

_TITLE_CONTINUATION_TAILS = {
    "ai",
    "các",
    "cho",
    "chỉ",
    "có",
    "của",
    "đây",
    "đến",
    "để",
    "đi",
    "gì",
    "kẻ",
    "khi",
    "là",
    "lại",
    "mọi",
    "một",
    "này",
    "người",
    "nên",
    "nơi",
    "ở",
    "ta",
    "thì",
    "về",
    "với",
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


def _split_glued_camelcase(text: str) -> str:
    """
    Split glued words where a token starts with an uppercase letter and contains a
    lower->upper boundary without whitespace, e.g. "HộiLâm" -> "Hội Lâm".

    This intentionally does not touch tokens starting with lowercase (e.g. "iPhone").
    """

    def _split_token(token: str) -> str:
        if not token:
            return token
        # Only touch tokens that start with uppercase (proper-noun-ish).
        if not token[0].isupper():
            return token
        # Common Western prefix that should remain glued: McDonald, McArthur, ...
        if token.startswith("Mc") and len(token) >= 3 and token[2].isupper():
            return token

        out: list[str] = [token[0]]
        for prev, cur in zip(token, token[1:]):
            if prev.isalpha() and cur.isalpha() and prev.islower() and cur.isupper():
                out.append(" ")
            out.append(cur)
        return "".join(out)

    # Letters-only tokens (unicode), so we don't accidentally split digits/punctuation.
    return re.sub(r"[^\W\d_]+", lambda m: _split_token(m.group(0)), text, flags=re.UNICODE)


def _chapter_numbers(raw: str, chapter_regex: str) -> list[str]:
    return [m.group(1) for m in re.finditer(chapter_regex, raw, flags=re.M)]


def _origin_chapter_titles(raw: str, chapter_regex: str) -> dict[str, str]:
    titles: dict[str, str] = {}
    for match in re.finditer(chapter_regex, raw, flags=re.M):
        chapter_num = match.group(1)
        title = match.group(2).strip()
        if title:
            titles[str(int(chapter_num))] = title
    return titles


def _title_clause_tokens(text: str) -> list[str]:
    return re.findall(r"[A-Za-zÀ-Ỵà-ỹĐđ0-9]+", text.casefold())


def _is_title_continuation(prev_clause: str, next_clause: str) -> bool:
    prev_tokens = _title_clause_tokens(prev_clause)
    next_tokens = _title_clause_tokens(next_clause)
    if not prev_tokens or not next_tokens:
        return False
    if next_clause and next_clause[0].islower():
        return True
    if prev_tokens[-1] in _TITLE_CONTINUATION_TAILS:
        return True
    if len(prev_tokens) <= 2 and len(next_tokens) <= 2:
        return True
    return False


def _is_title_duplicate_clause(prev_clause: str, next_clause: str) -> bool:
    prev_norm = " ".join(_title_clause_tokens(prev_clause))
    next_norm = " ".join(_title_clause_tokens(next_clause))
    if not prev_norm or not next_norm:
        return False
    if prev_norm == next_norm:
        return True
    if prev_norm in next_norm or next_norm in prev_norm:
        return True
    ratio = SequenceMatcher(None, prev_norm, next_norm).ratio()
    if ratio >= 0.72:
        return True
    prev_tokens = set(prev_norm.split())
    next_tokens = set(next_norm.split())
    overlap = prev_tokens & next_tokens
    if len(overlap) >= 2 and len(overlap) >= min(len(prev_tokens), len(next_tokens)) - 1:
        return True
    return False


def _clean_heading_title(title: str) -> str:
    title = title.strip()
    if not title:
        return title

    parts = re.findall(r"[^.?!…]+[.?!…]?", title)
    clauses = [part.strip() for part in parts if part.strip()]
    if len(clauses) <= 1:
        return title

    cleaned: list[str] = [clauses[0]]
    for clause in clauses[1:]:
        previous = cleaned[-1]
        if _is_title_continuation(previous.rstrip(".?!…"), clause.rstrip(".?!…")):
            merged = f"{previous.rstrip('.?!…')} {clause.lstrip()}"
            cleaned[-1] = merged
            continue
        if _is_title_duplicate_clause(previous.rstrip(".?!…"), clause.rstrip(".?!…")):
            continue
        cleaned.append(clause)

    if (
        len(cleaned) == 2
        and cleaned[0].endswith(".")
        and cleaned[1].endswith(".")
        and len(_title_clause_tokens(cleaned[0])) >= 2
        and len(_title_clause_tokens(cleaned[1])) >= 2
    ):
        cleaned = [cleaned[0]]

    result = " ".join(part.strip() for part in cleaned if part.strip())
    result = re.sub(r"\s+([.?!…])", r"\1", result)
    return result.strip()


def _normalize_heading(text: str, chapter_num: str) -> str:
    text = text.replace("\r\n", "\n").replace("\r", "\n").strip()
    lines = text.splitlines()
    if not lines:
        return f"Chương {int(chapter_num)}"

    first_line = lines[0].strip()
    remainder = "\n".join(lines[1:]).strip()

    vi_match = re.match(r"^\s*Chương\s+\d+\s*([:：])?\s*([^\n]*)$", first_line)
    if vi_match:
        inline_title = vi_match.group(2).strip() if vi_match.group(1) else ""
        inline_title = _clean_heading_title(inline_title)
        heading = f"Chương {int(chapter_num)}"
        if inline_title:
            heading = f"{heading}: {inline_title}"
        if remainder:
            return f"{heading}\n\n{remainder}"
        return heading

    zh_match = re.match(r"^\s*第\s*\d+\s*章\s*([^\n]*)$", first_line)
    if zh_match:
        inline_title = zh_match.group(1).strip()
        inline_title = _clean_heading_title(inline_title)
        heading = f"Chương {int(chapter_num)}"
        if inline_title:
            heading = f"{heading}: {inline_title}"
        if remainder:
            return f"{heading}\n\n{remainder}"
        return heading

    return f"Chương {int(chapter_num)}\n\n{text.strip()}" if text.strip() else f"Chương {int(chapter_num)}"


def _looks_like_chapter_title(line: str) -> bool:
    candidate = line.strip()
    if not candidate:
        return False
    if candidate.startswith(("“", '"', "'", "-", "(", "[")):
        return False

    words = re.findall(r"[A-Za-zÀ-Ỵà-ỹĐđ0-9]+", candidate)
    if not words or len(words) > 14 or len(candidate) > 90:
        return False

    # A completed declarative sentence is more likely to be body text.
    if candidate.endswith("."):
        return False
    # Multiple clause separators usually indicate narration rather than a title.
    if any(token in candidate for token in (";", ":")):
        return False
    if "," in candidate and not candidate.endswith(("?", "!")):
        if candidate.count(",") > 1 or len(words) > 8 or len(candidate) > 40:
            return False
    if " - " in candidate:
        return False
    if re.search(r"[.!?…].+[.!?…]", candidate):
        return False
    return True


def _split_inline_chapter_title(line: str) -> tuple[str, str] | None:
    words = list(re.finditer(r"[A-Za-zÀ-Ỵà-ỹĐđ0-9()]+", line))
    if len(words) < 2:
        return None

    candidates: list[tuple[str, str]] = []
    for idx in range(1, len(words)):
        split_pos = words[idx].start()
        prefix = line[:split_pos].strip()
        suffix = line[split_pos:].strip()
        if not prefix or not suffix:
            continue
        if not _looks_like_chapter_title(prefix):
            continue
        prefix_words = re.findall(r"[A-Za-zÀ-Ỵà-ỹĐđ0-9]+", prefix)
        if len(prefix_words) < 3:
            continue
        if not re.match(r'^[A-ZÀ-Ỵ“"]', suffix):
            continue
        suffix_first_word_match = re.match(r'[“"]?([A-Za-zÀ-Ỵà-ỹĐđ0-9]+)', suffix)
        if suffix_first_word_match is None:
            continue
        if suffix_first_word_match.group(1).casefold() not in _INLINE_TITLE_BODY_STARTERS:
            continue
        if len(suffix) < 25 and not re.search(r"[,.!?…:;]", suffix):
            continue
        candidates.append((prefix, suffix))

    if not candidates:
        return None
    return min(candidates, key=lambda item: len(item[0]))


def _fold_chapter_title(text: str, chapter_num: str) -> str:
    lines = text.split("\n")
    heading = f"Chương {int(chapter_num)}"
    if not lines or lines[0].strip() != heading:
        return text

    title_idx: int | None = None
    for idx in range(1, len(lines)):
        if lines[idx].strip():
            title_idx = idx
            break
    if title_idx is None:
        return text

    title = lines[title_idx].strip()
    body_lines = lines[title_idx + 1 :]
    if not _looks_like_chapter_title(title):
        inline_split = _split_inline_chapter_title(title)
        if inline_split is None:
            return text
        title, first_body_line = inline_split
        body_lines = [first_body_line, *body_lines]

    title = _clean_heading_title(title)
    title = title if title.endswith(("?", "!")) else f"{title}."
    while body_lines and not body_lines[0].strip():
        body_lines = body_lines[1:]
    body = "\n".join(body_lines).strip()
    if body:
        return f"{heading}: {title}\n\n{body}"
    return f"{heading}: {title}"


def _force_fold_heading_from_next_line(text: str, chapter_num: str) -> str:
    lines = text.splitlines()
    non_empty_positions = [idx for idx, line in enumerate(lines) if line.strip()]
    if len(non_empty_positions) < 2:
        return text

    first_idx = non_empty_positions[0]
    second_idx = non_empty_positions[1]
    heading = lines[first_idx].strip()
    title_line = lines[second_idx].strip()
    if heading != f"Chương {int(chapter_num)}":
        return text
    if not title_line or title_line.startswith(("“", '"', "'", "-", "(")):
        return text

    title_line = _clean_heading_title(title_line)
    title = title_line if title_line.endswith(("?", "!", ".")) else f"{title_line}."
    lines[first_idx] = f"Chương {int(chapter_num)}: {title}"
    lines[second_idx] = ""
    folded = "\n".join(lines)
    folded = re.sub(r"\n{3,}", "\n\n", folded).rstrip() + "\n"
    return folded


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
        if re.fullmatch(r"Chương\s+\d+(?::\s+.+)?", prev):
            merged.append(line)
            continue
        if (
            prev.endswith((":", ",", "“", "(", "-", "…"))
            or re.match(r"^[a-zà-ỹ]", line)
            or (len(line) < 70 and not re.match(r"^[“\"A-ZÀ-Ỵ]", line))
        ):
            merged[-1] = f"{prev} {line}"
        else:
            merged.append(line)
    return merged


def normalize_text(
    text: str,
    chapter_num: str,
    replacements: dict[str, str] | None = None,
    *,
    force_title_fold: bool = False,
) -> str:
    text = text.replace("QZXBRQ", "\n\n")
    text = _normalize_heading(text, chapter_num)
    for src, dst in (replacements or {}).items():
        text = text.replace(src, dst)
    text = _fold_chapter_title(text, chapter_num)
    if force_title_fold:
        text = _force_fold_heading_from_next_line(text, chapter_num)
    text = re.sub(r"loadAdv\(\d+,\s*\d+\);\s*", "", text)
    text = re.sub(r"[“\"]?Gỗ\.\.\.\s*Cố tổng", "“Cố tổng", text)
    text = re.sub(r"([^\n])([“\"])", r"\1 \2", text)
    text = re.sub(r'(?m)^(chương)\s+(?=[“"\'(])', "", text)
    # Break glued narration/dialogue around closing quotes.
    text = re.sub(r'([.!?…])”([A-ZÀ-Ỵ])', r'\1”\n\n\2', text)
    text = re.sub(r'([.!?…])"([A-ZÀ-Ỵ])', r'\1"\n\n\2', text)
    text = re.sub(r'([.!?…])”([“"])', r'\1”\n\n\2', text)
    text = re.sub(r'([.!?…])"([“"])', r'\1"\n\n\2', text)
    text = _split_glued_camelcase(text)
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
        if idx == 0 and (
            re.fullmatch(rf"Chương\s+{int(chapter_num)}", line)
            or re.fullmatch(rf"Chương\s+{int(chapter_num)}:\s+.+", line)
        ):
            lines.append(line)
            continue
        lines.extend(_rebalance_paragraph(line))
    lines = _merge_broken_paragraphs(lines)
    text = "\n\n".join(lines)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = text.replace("…", "...")
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
        origin_raw = origin_path.read_text(encoding="utf-8")
        chapter_nums = _chapter_numbers(origin_raw, config.translation.chapter_regex)
        origin_titles = _origin_chapter_titles(origin_raw, config.translation.chapter_regex)
        part_dir = config.storage.parts_dir / origin_path.stem
        merged_parts: list[str] = []
        for chapter_num in chapter_nums:
            part_path = part_dir / f"{int(chapter_num):04d}.txt"
            if not part_path.exists():
                continue
            original = part_path.read_text(encoding="utf-8")
            polished = normalize_text(original, chapter_num, config.translation.polish_replacements)
            if origin_titles.get(str(int(chapter_num))):
                polished = _force_fold_heading_from_next_line(polished, chapter_num)
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
