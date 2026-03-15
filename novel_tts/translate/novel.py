from __future__ import annotations

import fcntl
import json
import os
import re
import time
from pathlib import Path

from novel_tts.common.logging import get_logger
from novel_tts.config.models import NovelConfig

from .glossary import normalize_glossary_text, sanitize_glossary_entries
from .providers import PromptBlockedError, get_translation_provider

LOGGER = get_logger(__name__)
HAN_REGEX = re.compile(r"[\u4e00-\u9fff]")
JSON_BLOCK_REGEX = re.compile(r"```(?:json)?\s*(.*?)```", re.S)
PLACEHOLDER_TOKEN_RE = re.compile(r"(?:ZXQ|QZX)\d{1,6}QXZ")
PLACEHOLDER_LIKE_RE = re.compile(r"(?:ZXQ|QZX)\d{1,6}Q(?:XZ)?")
GLOSSARY_STATUS_PENDING = "pending"
GLOSSARY_STATUS_DONE = "done"


def make_placeholders(text: str, glossary: dict[str, str]) -> tuple[str, dict[str, str]]:
    mapping: dict[str, str] = {}
    for idx, key in enumerate(sorted(glossary, key=len, reverse=True)):
        token = f"ZXQ{idx:03d}QXZ"
        value = glossary.get(key, "")
        # Guard against glossary corruption where the "translation" contains placeholder tokens (e.g. "ZXQ1156QXZ"
        # or "Biến cố ZXQ125QXZ"). In that case we skip placeholdering so the model can translate from the original term.
        if isinstance(value, str) and PLACEHOLDER_LIKE_RE.search(value):
            continue
        if key in text:
            text = text.replace(key, token)
            mapping[token] = value
    return text, mapping


def split_chunks(text: str, max_len: int) -> list[str]:
    if max_len <= 0:
        return [text]
    blocks: list[str] = []
    for paragraph in re.split(r"\n\s*\n", text):
        if len(paragraph) <= max_len:
            blocks.append(paragraph)
            continue
        lines = paragraph.splitlines()
        buf: list[str] = []
        buf_len = 0
        for line in lines:
            extra = len(line) + 1
            if buf and buf_len + extra > max_len:
                blocks.append("\n".join(buf))
                buf = [line]
                buf_len = len(line)
            else:
                buf.append(line)
                buf_len += extra
        if buf:
            blocks.append("\n".join(buf))
    chunks: list[str] = []
    current: list[str] = []
    current_len = 0
    for idx, block in enumerate(blocks):
        if idx:
            block = "\n\n" + block
        block_len = len(block)
        if current and current_len + block_len > max_len:
            chunks.append("".join(current))
            current = [block]
            current_len = block_len
        else:
            current.append(block)
            current_len += block_len
    if current:
        chunks.append("".join(current))
    return chunks


def restore_placeholders(text: str, mapping: dict[str, str]) -> str:
    for token, value in mapping.items():
        text = text.replace(token, value)
        text = text.replace(token.replace("ZXQ", "QZX", 1), value)
    return text


def apply_rule_based_han_fixes(text: str, replacements: dict[str, str]) -> str:
    for src, dst in replacements.items():
        text = text.replace(src, dst)
    text = re.sub(r"第\s*(\d+)\s*章", r"Chương \1", text)
    text = re.sub(r"Chương\s+(\d+)\s*Chương\s*", r"Chương \1 ", text)
    text = re.sub(r"(?<=[A-Za-zÀ-ỹ0-9])[\u4e00-\u9fff](?=[A-Za-zÀ-ỹ0-9])", "", text)
    text = re.sub(r"(?<=\s)[\u4e00-\u9fff](?=\s)", "", text)
    text = re.sub(r"[ \t]{2,}", " ", text)
    return text


def strip_model_wrappers(text: str) -> str:
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```[a-zA-Z0-9_-]*\n?", "", text)
        text = re.sub(r"\n?```$", "", text)
    return text.strip()


def count_han_chars(text: str) -> int:
    return len(HAN_REGEX.findall(text))


def has_han(text: str) -> bool:
    return bool(HAN_REGEX.search(text))


def build_glossary(mapping: dict[str, str]) -> str:
    return "\n".join(f"- {token} = {value}" for token, value in mapping.items())


def glossary_path(config: NovelConfig) -> Path | None:
    if not config.translation.glossary_file:
        return None
    return config.storage.root / config.translation.glossary_file


def glossary_marker_path(config: NovelConfig, source_path: Path, chapter_num: str) -> Path:
    return config.storage.parts_dir / source_path.stem / f"{int(chapter_num):04d}.glossary.json"


def _load_glossary_marker(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}
    except Exception:
        # Corrupt marker should not permanently block work; treat as pending.
        return {"status": GLOSSARY_STATUS_PENDING, "last_error": "invalid-marker-json"}


def _write_glossary_marker(path: Path, *, status: str, last_error: str = "") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "status": str(status),
        "last_attempt_at": time.time(),
        "last_error": (last_error or "")[:2000],
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def is_glossary_pending(config: NovelConfig, source_path: Path, chapter_num: str) -> bool:
    if not config.translation.auto_update_glossary:
        return False
    marker = glossary_marker_path(config, source_path, chapter_num)
    payload = _load_glossary_marker(marker)
    return str(payload.get("status", "")).strip().lower() == GLOSSARY_STATUS_PENDING


def refresh_glossary(config: NovelConfig) -> None:
    path = glossary_path(config)
    if path is None or not path.exists():
        return
    try:
        glossary_raw = json.loads(path.read_text(encoding="utf-8"))
        glossary_clean, dropped = sanitize_glossary_entries(glossary_raw)
        if dropped:
            LOGGER.info("Ignored %s generic glossary entries while loading %s", len(dropped), path.name)
        config.translation.glossary = glossary_clean
    except Exception as exc:
        LOGGER.warning("Unable to refresh glossary from %s: %s", path, exc)


def _strip_json_wrappers(text: str) -> str:
    text = strip_model_wrappers(text)
    match = JSON_BLOCK_REGEX.search(text)
    if match:
        return match.group(1).strip()
    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end != -1 and end > start:
        return text[start : end + 1]
    start = text.find("[")
    end = text.rfind("]")
    if start != -1 and end != -1 and end > start:
        return text[start : end + 1]
    return text.strip()


def _parse_glossary_response(raw: str) -> dict[str, str]:
    payload = json.loads(_strip_json_wrappers(raw))
    if isinstance(payload, dict):
        items = payload.items()
    elif isinstance(payload, list):
        items = []
        for row in payload:
            if not isinstance(row, dict):
                continue
            src = row.get("source") or row.get("zh") or row.get("han")
            dst = row.get("target") or row.get("vi") or row.get("translation")
            if isinstance(src, str) and isinstance(dst, str):
                items.append((src, dst))
    else:
        raise ValueError("Unsupported glossary response shape")

    cleaned: dict[str, str] = {}
    for src, dst in items:
        if not isinstance(src, str) or not isinstance(dst, str):
            continue
        src = normalize_glossary_text(src)
        dst = normalize_glossary_text(dst)
        if len(src) < 2 or not src or not dst:
            continue
        if not HAN_REGEX.search(src) or HAN_REGEX.search(dst):
            continue
        if re.fullmatch(r"[\W\d_]+", src):
            continue
        cleaned[src] = dst
    cleaned, _dropped = sanitize_glossary_entries(cleaned)
    return cleaned


def _extract_glossary_updates(config: NovelConfig, provider, source_text: str, translated_text: str) -> dict[str, str]:
    # Never learn glossary from a translation that still contains placeholder tokens. This is a strong signal
    # that placeholder restoration (or glossary) is corrupted, and would poison future translations.
    if PLACEHOLDER_TOKEN_RE.search(translated_text):
        LOGGER.warning("Skipping glossary auto-update because translation still contains placeholder tokens")
        return {}
    max_source_chars_raw = os.environ.get("NOVEL_TTS_GLOSSARY_EXTRACT_MAX_SOURCE_CHARS", "").strip()
    max_translated_chars_raw = os.environ.get("NOVEL_TTS_GLOSSARY_EXTRACT_MAX_TRANSLATED_CHARS", "").strip()
    try:
        max_source_chars = int(max_source_chars_raw) if max_source_chars_raw else 2600
    except ValueError:
        max_source_chars = 2600
    try:
        max_translated_chars = int(max_translated_chars_raw) if max_translated_chars_raw else 4200
    except ValueError:
        max_translated_chars = 4200

    compact_source, compact_translated, was_compacted = _compact_glossary_context(
        source_text,
        translated_text,
        max_source_chars=max_source_chars,
        max_translated_chars=max_translated_chars,
    )
    prompt = (
        "Hãy trích xuất glossary thuật ngữ từ cặp văn bản sau.\n"
        "Mục tiêu: dùng cho các chương sau của cùng một truyện để giữ cách dịch nhất quán.\n"
        "Chỉ lấy mục thật sự nên tái sử dụng: tên người, tên trường, địa danh, tổ chức, chức danh riêng, biệt hiệu, thuật ngữ riêng.\n"
        "Không lấy đại từ, động từ, tính từ, câu hoàn chỉnh, từ thông dụng.\n"
        "Khóa phải là cụm chữ Hán xuất hiện nguyên văn trong bản gốc. Giá trị phải là đúng cách gọi tiếng Việt đã dùng trong bản dịch.\n"
        "Giá trị bắt buộc phải xuất hiện nguyên văn trong BẢN DỊCH (copy y nguyên), không được tự bịa hoặc tự suy diễn.\n"
        "Tuyệt đối không trả về mã placeholder dạng ZXQ123QXZ hoặc QZX123QXZ.\n"
        "Nếu chưa chắc chắn hoặc bản dịch không thể hiện rõ, bỏ qua.\n"
        "Ưu tiên cụm dài, tránh tạo mục con dư thừa khi đã có mục dài hơn cùng nghĩa.\n"
        "Chỉ trả về JSON object thuần, không markdown, không giải thích.\n\n"
        f"BẢN GỐC{' (TRÍCH)' if was_compacted else ''}:\n{compact_source}\n\n"
        f"BẢN DỊCH{' (TRÍCH)' if was_compacted else ''}:\n{compact_translated}\n"
    )
    # Prefer using the repair model for glossary extraction (more like a cleanup stage),
    # and to avoid competing with the primary translation model quota.
    model = config.translation.repair_model or config.translation.model
    if was_compacted:
        LOGGER.info(
            "Glossary extract using compacted context | source_chars=%s/%s translated_chars=%s/%s model=%s",
            len(compact_source),
            len((source_text or "").strip()),
            len(compact_translated),
            len((translated_text or "").strip()),
            model,
        )
    updates = _parse_glossary_response(_generate_once(provider, model, prompt))
    return _sanitize_extracted_glossary_updates(updates, translated_text)


GENERIC_GLOSSARY_TARGETS = {
    # Too generic to be useful and easy to mislearn from context.
    "tiên quân",
    "tiên tổ",
    "tiên đế",
    "ma tôn",
    "ma đế",
    "ma tổ",
    "tông chủ",
    "phó tông chủ",
    "gia chủ",
    "lão tổ",
}


def _sanitize_extracted_glossary_updates(updates: dict[str, str], translated_text: str) -> dict[str, str]:
    """
    Extra strict filtering for *auto-extracted* glossary updates (to avoid poisoning the glossary).
    This is intentionally stricter than sanitize_glossary_entries(), which is also used when loading
    curated/hand-edited glossary files.
    """
    if not updates:
        return {}
    translated_norm = normalize_glossary_text(translated_text).lower()
    kept: dict[str, str] = {}
    for src, dst in updates.items():
        if not isinstance(src, str) or not isinstance(dst, str):
            continue
        dst_norm = normalize_glossary_text(dst)
        if not dst_norm:
            continue
        # Never accept placeholder tokens.
        if PLACEHOLDER_TOKEN_RE.search(dst_norm):
            continue
        # Must be present verbatim in the translated text (avoid hallucinated targets).
        if normalize_glossary_text(dst_norm).lower() not in translated_norm:
            continue
        # Drop overly generic targets that frequently cause bad glossary entries.
        if dst_norm.lower() in GENERIC_GLOSSARY_TARGETS:
            continue
        kept[src] = dst_norm
    return kept


def _slice_head_tail(text: str, max_chars: int) -> str:
    text = (text or "").strip()
    if max_chars <= 0 or len(text) <= max_chars:
        return text
    head = max(0, int(max_chars * 0.7))
    tail = max(0, max_chars - head)
    if tail <= 0:
        return text[:max_chars].strip()
    if head <= 0:
        return text[-max_chars:].strip()
    if head + tail >= len(text):
        return text[:max_chars].strip()
    return (text[:head].rstrip() + "\n...\n" + text[-tail:].lstrip()).strip()


def _compact_glossary_context(
    source_text: str,
    translated_text: str,
    *,
    max_source_chars: int,
    max_translated_chars: int,
) -> tuple[str, str, bool]:
    source_text = (source_text or "").strip()
    translated_text = (translated_text or "").strip()

    # Source: keep only lines with Han (plus minimal context) to reduce noise.
    kept_lines: list[str] = []
    if source_text:
        lines = source_text.splitlines()
        keep_idx: set[int] = set()
        for idx, line in enumerate(lines):
            if HAN_REGEX.search(line):
                keep_idx.add(idx)
                if idx - 1 >= 0:
                    keep_idx.add(idx - 1)
                if idx + 1 < len(lines):
                    keep_idx.add(idx + 1)
        for idx in sorted(keep_idx):
            kept_lines.append(lines[idx])
    compact_source = "\n".join(kept_lines).strip() if kept_lines else source_text
    compact_source = _slice_head_tail(compact_source, max_source_chars)

    # Translation: keep head+tail to capture names introduced early/late.
    compact_translated = _slice_head_tail(translated_text, max_translated_chars)

    was_compacted = (compact_source != source_text) or (compact_translated != translated_text)
    return compact_source, compact_translated, was_compacted


def _merge_glossary_file(config: NovelConfig, updates: dict[str, str]) -> tuple[dict[str, str], int]:
    path = glossary_path(config)
    if path is None or not updates:
        return config.translation.glossary, 0
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = path.with_suffix(path.suffix + ".lock")
    lock_path.touch(exist_ok=True)
    added = 0
    with lock_path.open("r+", encoding="utf-8") as lock_handle:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
        current: dict[str, str] = {}
        if path.exists():
            try:
                current = json.loads(path.read_text(encoding="utf-8"))
            except Exception as exc:
                LOGGER.warning("Unable to read existing glossary %s: %s", path, exc)
        merged = dict(current)
        changed = False
        for key, value in updates.items():
            key = normalize_glossary_text(key)
            value = normalize_glossary_text(value)
            filtered, dropped = sanitize_glossary_entries({key: value})
            if dropped:
                continue
            key, value = next(iter(filtered.items()))
            existing = merged.get(key)
            if existing:
                if existing != value:
                    LOGGER.info("Keeping existing glossary entry %s=%s over new value %s", key, existing, value)
                continue
            merged[key] = value
            changed = True
            added += 1
        if changed:
            # Never persist placeholder tokens in glossary values. These tokens are internal translation placeholders
            # and will poison future translations if they survive into the glossary file.
            merged = {
                key: value for key, value in merged.items() if isinstance(value, str) and (not PLACEHOLDER_LIKE_RE.search(value))
            }
            ordered = {key: merged[key] for key in sorted(merged)}
            path.write_text(json.dumps(ordered, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            merged = ordered
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)
    config.translation.glossary = merged
    return merged, added


def update_glossary_from_chapter(
    config: NovelConfig,
    source_text: str,
    translated_text: str,
    *,
    marker_path: Path | None = None,
    unit_key: str = "",
) -> None:
    if not config.translation.auto_update_glossary:
        return
    provider = get_translation_provider(config.translation.provider)
    if unit_key:
        LOGGER.info("QUEUE_PHASE glossary | unit=%s", unit_key)
    if marker_path is not None:
        _write_glossary_marker(marker_path, status=GLOSSARY_STATUS_PENDING, last_error="")
    try:
        updates = _extract_glossary_updates(config, provider, source_text, translated_text)
        if updates:
            merged, added = _merge_glossary_file(config, updates)
            LOGGER.info("Updated glossary | added=%s total=%s", added, len(merged))
        if marker_path is not None:
            _write_glossary_marker(marker_path, status=GLOSSARY_STATUS_DONE, last_error="")
    except Exception as exc:
        if marker_path is not None:
            _write_glossary_marker(marker_path, status=GLOSSARY_STATUS_PENDING, last_error=str(exc))
        strict = os.environ.get("NOVEL_TTS_GLOSSARY_STRICT", "").strip().lower() in {"1", "true", "yes"}
        if strict:
            raise
        LOGGER.warning("Unable to extract glossary updates: %s", exc)


def strip_small_han_residue(line: str) -> str:
    if count_han_chars(line) > 2:
        return line
    line = re.sub(r"(?<=[A-Za-zÀ-ỹ0-9])[\u4e00-\u9fff]+", "", line)
    line = re.sub(r"[\u4e00-\u9fff]+(?=[A-Za-zÀ-ỹ0-9])", "", line)
    line = re.sub(r"(?<=\s)[\u4e00-\u9fff]+(?=\s|[,.!?:;])", "", line)
    line = re.sub(r"[ \t]{2,}", " ", line)
    return line.strip()


def scrub_tiny_han_residue(text: str, replacements: dict[str, str]) -> str:
    lines = []
    for line in text.splitlines():
        line = apply_rule_based_han_fixes(line, replacements)
        if count_han_chars(line) <= 2:
            line = re.sub(r"[\u4e00-\u9fff]+", "", line)
            line = re.sub(r"[ \t]{2,}", " ", line).strip()
        lines.append(line)
    return "\n".join(lines)


def split_repair_segments(line: str) -> list[str]:
    separators = r"([,;:(){}\[\]\"“”‘’]|[.!?…]+|\s+-\s+)"
    parts = re.split(separators, line)
    segments: list[str] = []
    current = ""
    for part in parts:
        if not part:
            continue
        if len(current) + len(part) > 160 and current:
            segments.append(current)
            current = part
        else:
            current += part
    if current:
        segments.append(current)
    return segments or [line]


def repair_obvious_errors(text: str) -> str:
    text = re.sub(
        r"Chương\s+(\d+)\s+[^.\n]{0,80}?Chương\s+\1\s+",
        lambda m: f"Chương {m.group(1)} ",
        text,
    )
    text = re.sub(r"^(Chương\s+\d+[^\n]*?)([A-ZÀ-Ỵ\"“])", r"\1\n\n\2", text, flags=re.MULTILINE)
    text = re.sub(r"(Chương\s+\d+)\s*\n(?!\n)", r"\1\n\n", text)
    text = re.sub(r"([.!?…])([A-ZÀ-Ỵ\"“])", r"\1 \2", text)
    return text


def post_process(text: str, replacements: dict[str, str]) -> str:
    for src, dst in replacements.items():
        text = text.replace(src, dst)
    text = repair_obvious_errors(text)
    text = re.sub(r"(?m)^[ \t]+", "", text)
    text = re.sub(r"[ \t]+\n", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip() + "\n"


def progress_path(config: NovelConfig, key: str) -> Path:
    return config.storage.progress_dir / f"{key}.json"


def load_progress(config: NovelConfig, key: str) -> list[str]:
    path = progress_path(config, key)
    if not path.exists():
        return []
    return json.loads(path.read_text(encoding="utf-8")).get("chunks", [])


def save_progress(config: NovelConfig, key: str, chunks: list[str]) -> None:
    config.storage.progress_dir.mkdir(parents=True, exist_ok=True)
    progress_path(config, key).write_text(json.dumps({"chunks": chunks}, ensure_ascii=False, indent=2), encoding="utf-8")


def clear_progress(config: NovelConfig, key: str) -> None:
    path = progress_path(config, key)
    if path.exists():
        path.unlink()


def chapter_part_path(config: NovelConfig, source_path: Path, chapter_num: str) -> Path:
    return config.storage.parts_dir / source_path.stem / f"{int(chapter_num):04d}.txt"


def split_source_chapters(raw: str, chapter_regex: str) -> list[tuple[str, str]]:
    matches = list(re.finditer(chapter_regex, raw, flags=re.M))
    if not matches:
        return [("0", raw.strip())]
    chapters: list[tuple[str, str]] = []
    current_num: str | None = None
    current_title = ""
    current_parts: list[str] = []

    def flush() -> None:
        nonlocal current_num, current_title, current_parts
        if current_num is None:
            return
        text = "\n".join(part.strip("\n") for part in current_parts if part.strip("\n")).strip()
        if text:
            chapters.append((current_num, text + "\n"))
        current_num = None
        current_title = ""
        current_parts = []

    for idx, match in enumerate(matches):
        start = match.start()
        end = matches[idx + 1].start() if idx + 1 < len(matches) else len(raw)
        block = raw[start:end].strip("\n")
        lines = block.splitlines()
        header = lines[0].strip()
        chapter_num = match.group(1)
        title = match.group(2).strip()

        if current_num == chapter_num:
            remainder_lines = lines[1:]
            extra = re.sub(rf"^第{int(chapter_num)}章\s*", "", header).strip()
            if current_title and extra.startswith(current_title):
                extra = extra[len(current_title):].strip()
            if extra:
                remainder_lines.insert(0, extra)
            merged = "\n".join(remainder_lines).strip()
            if merged:
                current_parts.append(merged)
            continue

        flush()
        current_num = chapter_num
        current_title = title
        current_parts = [header]
        body = "\n".join(lines[1:]).strip()
        if body:
            current_parts.append(body)

    flush()
    return chapters


def load_source_chapters(config: NovelConfig, source_path: Path) -> list[tuple[str, str]]:
    raw = source_path.read_text(encoding="utf-8")
    return split_source_chapters(raw, config.translation.chapter_regex)


def load_chapter_map(config: NovelConfig, source_path: Path) -> dict[str, str]:
    return {chapter_num: chapter_text for chapter_num, chapter_text in load_source_chapters(config, source_path)}


def _generate_once(provider, model: str, prompt: str) -> str:
    return strip_model_wrappers(provider.generate(model, prompt))


def _repair_model(config: NovelConfig) -> str:
    return config.translation.repair_model or config.translation.model


def _strip_placeholder_rules(base_rules: str) -> str:
    """
    The chapter translation prompt must preserve placeholder tokens (ZXQ...QXZ) while translating masked text.
    However, *repair* prompts operate on already-restored text and should not encourage models to emit tokens.
    """
    if not base_rules:
        return ""
    filtered: list[str] = []
    for line in str(base_rules).splitlines():
        lowered = line.lower()
        if "placeholder" in lowered or "zxq" in lowered or "qzxbrq" in lowered:
            continue
        filtered.append(line)
    return "\n".join(filtered).strip()


def _safe_literary_prompt(base_rules: str, glossary_text: str, line_token: str, text: str) -> str:
    return (
        f"{base_rules}\n"
        f"Glossary dùng bắt buộc nếu xuất hiện:\n{glossary_text}\n\n"
        "Đây là đoạn văn học hư cấu từ tiểu thuyết mạng. "
        "Nếu có cảnh thân mật hoặc nội dung người lớn, hãy chuyển ngữ bằng giọng văn trung tính, tiết chế, "
        "không thêm chi tiết nhạy cảm, không tăng mức độ gợi dục, nhưng vẫn giữ nguyên ý và mạch truyện. "
        "Chỉ trả về đúng bản dịch tiếng Việt.\n\n"
        f"Dịch đoạn sau sang tiếng Việt:\n{text.replace(chr(10), f' {line_token} ')}"
    )


def _generate_translation_chunk(provider, translation_cfg, glossary_text: str, chunk: str) -> str:
    primary_prompt = (
        f"{translation_cfg.base_rules}\n"
        f"Glossary dùng bắt buộc nếu xuất hiện:\n{glossary_text}\n\n"
        "Hãy tự kiểm tra và sửa ngay trong một lần trả lời trước khi xuất kết quả cuối cùng.\n\n"
        f"Dịch đoạn sau sang tiếng Việt:\n{chunk.replace(chr(10), f' {translation_cfg.line_token} ')}"
    )
    try:
        return _generate_once(provider, translation_cfg.model, primary_prompt)
    except PromptBlockedError as exc:
        LOGGER.warning("Provider blocked chunk, retrying with safe literary prompt | reason=%s", exc.reason)

    safe_prompt = _safe_literary_prompt(
        translation_cfg.base_rules,
        glossary_text,
        translation_cfg.line_token,
        chunk,
    )
    try:
        return _generate_once(provider, translation_cfg.model, safe_prompt)
    except PromptBlockedError as exc:
        LOGGER.warning("Provider still blocked chunk, retrying with smaller segments | reason=%s", exc.reason)

    segment_limit = max(180, min(translation_cfg.chunk_max_len // 3, 320))
    segment_texts = split_chunks(chunk, segment_limit)
    outputs: list[str] = []
    for idx, segment in enumerate(segment_texts, 1):
        segment_prompt = _safe_literary_prompt(
            translation_cfg.base_rules,
            glossary_text,
            translation_cfg.line_token,
            segment,
        )
        try:
            outputs.append(_generate_once(provider, translation_cfg.model, segment_prompt))
        except PromptBlockedError as exc:
            LOGGER.warning(
                "Provider blocked small segment, stripping sensitive wording in prompt | segment=%s/%s reason=%s",
                idx,
                len(segment_texts),
                exc.reason,
            )
            softened_prompt = (
                f"{translation_cfg.base_rules}\n"
                f"Glossary dùng bắt buộc nếu xuất hiện:\n{glossary_text}\n\n"
                "Đây là một đoạn đối thoại hoặc trần thuật trong tiểu thuyết hư cấu. "
                "Hãy chuyển ngữ sang tiếng Việt rõ nghĩa, trung tính, giữ nguyên diễn biến. "
                "Chỉ trả về bản dịch.\n\n"
                f"{segment.replace(chr(10), f' {translation_cfg.line_token} ')}"
            )
            outputs.append(_generate_once(provider, translation_cfg.model, softened_prompt))
    return "".join(outputs)


def final_cleanup(config: NovelConfig, provider, model: str, text: str, mapping: dict[str, str]) -> str:
    prompt = (
        f"{_strip_placeholder_rules(config.translation.base_rules)}\n"
        f"Glossary dùng bắt buộc nếu xuất hiện:\n{build_glossary(mapping)}\n\n"
        "Dưới đây là bản dịch tiếng Việt còn lỗi. "
        "Hãy chỉ sửa lỗi còn sót: chữ Hán chưa dịch, câu cú gượng, xuống dòng xấu, tiêu đề chương dính hoặc lặp. "
        "Không thêm ý mới. Chỉ trả về bản sửa cuối cùng.\n\n"
        f"{text}"
    )
    return _generate_once(provider, model, prompt)


def patch_remaining_han(config: NovelConfig, provider, model: str, text: str, mapping: dict[str, str]) -> str:
    translation_cfg = config.translation
    text = apply_rule_based_han_fixes(text, translation_cfg.han_fallback_replacements)
    if not has_han(text):
        return text
    glossary = build_glossary(mapping)
    lines = text.splitlines()
    for idx, line in enumerate(lines):
        line = apply_rule_based_han_fixes(line, translation_cfg.han_fallback_replacements)
        line = strip_small_han_residue(line)
        if not has_han(line):
            lines[idx] = line
            continue
        prompt = (
            f"{_strip_placeholder_rules(translation_cfg.base_rules)}\n"
            f"Glossary dùng bắt buộc nếu xuất hiện:\n{glossary}\n\n"
            "Chỉ dịch đúng dòng sau sang tiếng Việt tự nhiên. "
            "Nếu dòng chỉ là từ tượng thanh thì dịch thành từ tượng thanh tiếng Việt phù hợp. "
            "Chỉ trả về đúng một dòng đã dịch.\n\n"
            f"{line}"
        )
        fixed = _generate_once(provider, model, prompt)
        fixed = apply_rule_based_han_fixes(fixed.strip(), translation_cfg.han_fallback_replacements)
        lines[idx] = strip_small_han_residue(fixed)
    return "\n".join(lines)


def aggressive_repair_han(config: NovelConfig, provider, model: str, text: str, mapping: dict[str, str]) -> str:
    translation_cfg = config.translation
    glossary = build_glossary(mapping)
    repaired_lines: list[str] = []
    for line in text.splitlines():
        line = apply_rule_based_han_fixes(line, translation_cfg.han_fallback_replacements)
        if not has_han(line):
            repaired_lines.append(line)
            continue
        fixed_segments: list[str] = []
        for segment in split_repair_segments(line):
            segment = apply_rule_based_han_fixes(segment, translation_cfg.han_fallback_replacements)
            segment = strip_small_han_residue(segment)
            if not has_han(segment):
                fixed_segments.append(segment)
                continue
            prompt = (
                f"{_strip_placeholder_rules(translation_cfg.base_rules)}\n"
                f"Glossary dùng bắt buộc nếu xuất hiện:\n{glossary}\n\n"
                "Chỉ sửa đoạn văn sau: thay toàn bộ chữ Hán còn sót thành tiếng Việt tự nhiên. "
                "Giữ nguyên ý, không thêm bớt. Tuyệt đối không để sót chữ Hán. "
                "Chỉ trả về đúng đoạn đã sửa.\n\n"
                f"{segment}"
            )
            fixed = _generate_once(provider, model, prompt).strip()
            fixed = apply_rule_based_han_fixes(fixed, translation_cfg.han_fallback_replacements)
            fixed = strip_small_han_residue(fixed)
            if has_han(fixed) and count_han_chars(fixed) <= 6:
                fixed = re.sub(r"[\u4e00-\u9fff]+", "", fixed)
                fixed = re.sub(r"[ \t]{2,}", " ", fixed).strip()
            fixed_segments.append(fixed)
        repaired_lines.append("".join(fixed_segments))
    return "\n".join(repaired_lines)


def repair_against_source(config: NovelConfig, provider, model: str, source_text: str, translated_text: str) -> str:
    prompt = (
        f"{_strip_placeholder_rules(config.translation.base_rules)}\n"
        "Dưới đây là bản gốc tiếng Trung và bản dịch tiếng Việt hiện có của cùng một chương.\n"
        "Nhiệm vụ của ngươi:\n"
        "- Giữ nguyên toàn bộ nội dung và thứ tự theo bản gốc.\n"
        "- Chỉ xuất ra bản dịch tiếng Việt cuối cùng của cả chương.\n"
        "- Phải thay hết toàn bộ chữ Hán còn sót, kể cả chữ Hán lẻ bị trộn trong câu tiếng Việt.\n"
        "- Nếu bản dịch hiện có đã đúng ở chỗ nào thì giữ nguyên tinh thần, chỉ sửa phần lỗi.\n"
        "- Không để lại chữ Hán, không giải thích, không ghi chú.\n\n"
        f"BẢN GỐC:\n{source_text}\n\n"
        f"BẢN DỊCH HIỆN CÓ:\n{translated_text}"
    )
    return _generate_once(provider, model, prompt)


def repair_placeholder_tokens_against_source(
    config: NovelConfig,
    provider,
    model: str,
    source_text: str,
    translated_text: str,
) -> str:
    found = sorted(set(PLACEHOLDER_TOKEN_RE.findall(translated_text)))
    examples = ", ".join(found[:8])
    prompt = (
        f"{_strip_placeholder_rules(config.translation.base_rules)}\n"
        "Bản dịch tiếng Việt dưới đây đang bị lỗi: còn sót các mã placeholder dạng ZXQ123QXZ hoặc QZX123QXZ.\n"
        "Nhiệm vụ của ngươi:\n"
        "- Tuyệt đối không để lại bất kỳ mã ZXQ...QXZ/QZX...QXZ nào trong kết quả.\n"
        "- Dựa vào bản gốc tiếng Trung để khôi phục đúng tên người/địa danh/tổ chức/chức danh tương ứng.\n"
        "- Nếu không chắc cách Việt hóa, giữ nguyên chữ Hán của thuật ngữ trong bản gốc (nhưng vẫn không được để token).\n"
        "- Giữ nguyên nội dung và thứ tự theo bản gốc, không thêm ý, không xóa làm mất nghĩa.\n"
        "- Chỉ xuất ra bản dịch tiếng Việt cuối cùng của cả đoạn.\n\n"
        f"PLACEHOLDER ĐANG BỊ LỌT (ví dụ): {examples}\n\n"
        f"BẢN GỐC:\n{source_text}\n\n"
        f"BẢN DỊCH HIỆN CÓ:\n{translated_text}"
    )
    return _generate_once(provider, model, prompt)


def strip_all_remaining_han(text: str) -> str:
    text = re.sub(r"[\u4e00-\u9fff]+", "", text)
    text = re.sub(r"[ \t]{2,}", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def translate_unit(config: NovelConfig, unit_key: str, raw_text: str) -> str:
    translation_cfg = config.translation
    repair_model = _repair_model(config)
    refresh_glossary(config)
    provider = get_translation_provider(translation_cfg.provider)
    masked, mapping = make_placeholders(raw_text, translation_cfg.glossary)
    chunks = split_chunks(masked, translation_cfg.chunk_max_len)
    translated_chunks = load_progress(config, unit_key)
    LOGGER.info("QUEUE_PHASE translate | unit=%s", unit_key)
    glossary_text = "\n".join(f"- {token} = {value}" for token, value in mapping.items())
    for idx, chunk in enumerate(chunks[len(translated_chunks):], len(translated_chunks) + 1):
        LOGGER.info("Translating %s chunk %s/%s", unit_key, idx, len(chunks))
        started = time.perf_counter()
        result = _generate_translation_chunk(provider, translation_cfg, glossary_text, chunk)
        elapsed = time.perf_counter() - started
        LOGGER.info(
            "Translated %s chunk %s/%s in %.1fs (chars=%s)",
            unit_key,
            idx,
            len(chunks),
            elapsed,
            len(chunk),
        )
        translated_chunks.append(result.replace(translation_cfg.line_token, "\n"))
        save_progress(config, unit_key, translated_chunks)
        time.sleep(translation_cfg.chunk_sleep_seconds)
    LOGGER.info("QUEUE_PHASE repair | unit=%s", unit_key)
    merged = restore_placeholders("".join(translated_chunks), mapping)
    merged = post_process(merged, translation_cfg.post_replacements)
    merged = apply_rule_based_han_fixes(merged, translation_cfg.han_fallback_replacements)
    # Placeholder tokens should never survive placeholder restoration. If they do, we repair against source.
    if PLACEHOLDER_TOKEN_RE.search(merged):
        LOGGER.info("Placeholder tokens detected after merge; repairing | unit=%s", unit_key)
        try:
            started = time.perf_counter()
            merged = repair_placeholder_tokens_against_source(config, provider, repair_model, raw_text, merged)
            LOGGER.info(
                "placeholder-token repair done in %.1fs | unit=%s",
                time.perf_counter() - started,
                unit_key,
            )
            merged = restore_placeholders(merged, mapping)
            merged = post_process(merged, translation_cfg.post_replacements)
            merged = apply_rule_based_han_fixes(merged, translation_cfg.han_fallback_replacements)
        except Exception as exc:
            LOGGER.warning("placeholder token repair failed for %s: %s", unit_key, exc)
    if has_han(merged) and count_han_chars(merged) > 12:
        LOGGER.info("Han residue detected; running final_cleanup | unit=%s count=%s", unit_key, count_han_chars(merged))
        try:
            started = time.perf_counter()
            merged = restore_placeholders(final_cleanup(config, provider, repair_model, merged, mapping), mapping)
            LOGGER.info(
                "final_cleanup done in %.1fs | unit=%s",
                time.perf_counter() - started,
                unit_key,
            )
            merged = post_process(merged, translation_cfg.post_replacements)
            merged = apply_rule_based_han_fixes(merged, translation_cfg.han_fallback_replacements)
        except Exception as exc:
            LOGGER.warning("final_cleanup failed for %s: %s", unit_key, exc)
    if has_han(merged):
        LOGGER.info("Han residue detected; patching per-line | unit=%s count=%s", unit_key, count_han_chars(merged))
        started = time.perf_counter()
        merged = patch_remaining_han(config, provider, repair_model, merged, mapping)
        LOGGER.info(
            "patch_remaining_han done in %.1fs | unit=%s count=%s",
            time.perf_counter() - started,
            unit_key,
            count_han_chars(merged),
        )
        merged = post_process(merged, translation_cfg.post_replacements)
        merged = apply_rule_based_han_fixes(merged, translation_cfg.han_fallback_replacements)
    if has_han(merged) and count_han_chars(merged) <= 6:
        merged = scrub_tiny_han_residue(merged, translation_cfg.han_fallback_replacements)
        merged = post_process(merged, translation_cfg.post_replacements)
    if has_han(merged):
        LOGGER.info("Han residue detected; repairing against source | unit=%s count=%s", unit_key, count_han_chars(merged))
        started = time.perf_counter()
        merged = repair_against_source(config, provider, repair_model, raw_text, merged)
        LOGGER.info(
            "repair_against_source done in %.1fs | unit=%s count=%s",
            time.perf_counter() - started,
            unit_key,
            count_han_chars(merged),
        )
        merged = post_process(merged, translation_cfg.post_replacements)
        merged = apply_rule_based_han_fixes(merged, translation_cfg.han_fallback_replacements)
        if has_han(merged) and count_han_chars(merged) <= 12:
            merged = scrub_tiny_han_residue(merged, translation_cfg.han_fallback_replacements)
            merged = post_process(merged, translation_cfg.post_replacements)
    if has_han(merged):
        LOGGER.info("Han residue detected; aggressive repair | unit=%s count=%s", unit_key, count_han_chars(merged))
        started = time.perf_counter()
        merged = aggressive_repair_han(config, provider, repair_model, merged, mapping)
        LOGGER.info(
            "aggressive_repair_han done in %.1fs | unit=%s count=%s",
            time.perf_counter() - started,
            unit_key,
            count_han_chars(merged),
        )
        merged = post_process(merged, translation_cfg.post_replacements)
        merged = apply_rule_based_han_fixes(merged, translation_cfg.han_fallback_replacements)
        if has_han(merged) and count_han_chars(merged) <= 12:
            merged = scrub_tiny_han_residue(merged, translation_cfg.han_fallback_replacements)
            merged = post_process(merged, translation_cfg.post_replacements)
        if has_han(merged) and count_han_chars(merged) <= 64:
            merged = strip_all_remaining_han(merged)
            merged = post_process(merged, translation_cfg.post_replacements)
    if has_han(merged):
        remaining_han = count_han_chars(merged)
        if remaining_han <= 80:
            LOGGER.warning("Force stripping residual Han | unit=%s count=%s", unit_key, remaining_han)
            merged = strip_all_remaining_han(merged)
            merged = post_process(merged, translation_cfg.post_replacements)
        else:
            raise RuntimeError(f"Still contains Han characters after cleanup: {unit_key}")

    # Final safety: some repair stages call the model on already-restored text and may re-emit placeholder tokens.
    # Always attempt restoration one last time before writing chapter parts.
    merged = restore_placeholders(merged, mapping)
    merged = post_process(merged, translation_cfg.post_replacements)
    merged = apply_rule_based_han_fixes(merged, translation_cfg.han_fallback_replacements)
    if PLACEHOLDER_TOKEN_RE.search(merged):
        LOGGER.error("Placeholder tokens survived final restoration | unit=%s", unit_key)
        raise RuntimeError(f"Still contains placeholder tokens after restoration: {unit_key}")
    clear_progress(config, unit_key)
    return merged


def translate_chapter(config: NovelConfig, source_path: Path, chapter_num: str, force: bool = False) -> Path:
    chapter_map = load_chapter_map(config, source_path)
    if chapter_num not in chapter_map:
        raise ValueError(f"Chapter {chapter_num} not found in {source_path.name}")
    part_path = chapter_part_path(config, source_path, chapter_num)
    marker_path = glossary_marker_path(config, source_path, chapter_num)
    pending_glossary = is_glossary_pending(config, source_path, chapter_num)
    if part_path.exists() and not force and part_path.stat().st_mtime >= source_path.stat().st_mtime and not pending_glossary:
        return part_path
    part_path.parent.mkdir(parents=True, exist_ok=True)
    source_text = chapter_map[chapter_num]
    unit_key = f"{source_path.name}__{chapter_num}"

    # Force runs should not resume stale progress snapshots. Glossary changes can shift placeholder tokens and
    # leave orphan ZXQ...QXZ tokens in the merged output if we resume old chunks.
    if force:
        clear_progress(config, unit_key)

    needs_translate = force or (not part_path.exists()) or part_path.stat().st_mtime < source_path.stat().st_mtime
    if needs_translate:
        text = translate_unit(config, unit_key, source_text)
        part_path.write_text(text, encoding="utf-8")
    else:
        text = part_path.read_text(encoding="utf-8", errors="replace")

    if config.translation.auto_update_glossary and (pending_glossary or needs_translate):
        update_glossary_from_chapter(
            config,
            source_text,
            text,
            marker_path=marker_path,
            unit_key=unit_key,
        )
    return part_path


def rebuild_translated_file(config: NovelConfig, source_path: Path, require_complete: bool = True) -> Path | None:
    chapters = load_source_chapters(config, source_path)
    merged_parts: list[str] = []
    for chapter_num, _chapter_text in chapters:
        part_path = chapter_part_path(config, source_path, chapter_num)
        if not part_path.exists():
            if require_complete:
                return None
            continue
        merged_parts.append(part_path.read_text(encoding="utf-8").strip())
    if require_complete and len(merged_parts) != len(chapters):
        return None
    if not merged_parts:
        return None
    config.storage.translated_dir.mkdir(parents=True, exist_ok=True)
    target = config.storage.translated_dir / source_path.name
    target.write_text("\n\n".join(merged_parts).strip() + "\n", encoding="utf-8")
    return target


def translate_file(config: NovelConfig, source_path: Path, force: bool = False) -> Path:
    target = config.storage.translated_dir / source_path.name
    if target.exists() and not force:
        LOGGER.info("Skipping %s (already translated)", source_path.name)
        return target
    for chapter_num, _chapter_text in load_source_chapters(config, source_path):
        translate_chapter(config, source_path, chapter_num, force=force)
    rebuilt = rebuild_translated_file(config, source_path, require_complete=True)
    if rebuilt is None:
        raise RuntimeError(f"Unable to rebuild translated file for {source_path.name}")
    return rebuilt


def translate_novel(config: NovelConfig, force: bool = False, filenames: list[str] | None = None) -> list[Path]:
    files = sorted(config.storage.origin_dir.glob("*.txt"))
    if filenames:
        wanted = set(filenames)
        files = [path for path in files if path.name in wanted]
    outputs = []
    for path in files:
        outputs.append(translate_file(config, path, force=force))
    return outputs
