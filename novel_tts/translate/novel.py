from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import time
from pathlib import Path

from novel_tts.common.logging import get_logger
from novel_tts.common.errors import RateLimitExceededError
from novel_tts.config.models import NovelConfig

from .glossary import normalize_glossary_text, sanitize_glossary_entries
from .model import _clean_model_name, resolve_translation_model
from .providers import PromptBlockedError, get_translation_provider

LOGGER = get_logger(__name__)
HAN_REGEX = re.compile(r"[\u4e00-\u9fff]")
JSON_BLOCK_REGEX = re.compile(r"```(?:json)?\s*(.*?)```", re.S)
PLACEHOLDER_TOKEN_RE = re.compile(r"(?:ZXQ|QZX)\d{1,6}QXZ")
PLACEHOLDER_LIKE_RE = re.compile(r"(?:ZXQ|QZX)\d{1,6}Q(?:XZ)?")
GLOSSARY_STATUS_PENDING = "pending"
GLOSSARY_STATUS_DONE = "done"


def _get_model_cfg(config: NovelConfig, model: str):
    model = _clean_model_name(model)
    cfg = getattr(config, "models", None) and config.models.model_configs.get(model)
    if cfg is not None:
        return cfg
    return getattr(config, "queue", None) and config.queue.model_configs.get(model)


def _effective_chunk_max_len(config: NovelConfig, model: str) -> int:
    raw = (os.environ.get("CHUNK_MAX_LEN") or "").strip()
    if raw:
        try:
            value = int(raw)
        except ValueError:
            value = 0
        if value > 0:
            return max(400, value)
    cfg = _get_model_cfg(config, model)
    value = int(getattr(cfg, "chunk_max_len", 0) or 0) if cfg is not None else 0
    if value > 0:
        return value
    # Loader enforces chunk_max_len > 0 for enabled models, but keep a conservative fallback.
    return 800


def _effective_chunk_sleep_seconds(config: NovelConfig, model: str) -> float:
    raw = (os.environ.get("CHUNK_SLEEP_SECONDS") or "").strip()
    if raw:
        try:
            return float(raw)
        except ValueError:
            return 0.0
    cfg = _get_model_cfg(config, model)
    if cfg is None:
        return 0.1
    value = getattr(cfg, "chunk_sleep_seconds", None)
    if value is None:
        return 0.1
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.1


def _effective_repair_model(config: NovelConfig, default_model: str) -> str:
    env = _clean_model_name(os.environ.get("REPAIR_MODEL", ""))
    if env:
        return env
    cfg = _get_model_cfg(config, default_model)
    per_model = _clean_model_name(getattr(cfg, "repair_model", "")) if cfg is not None else ""
    if per_model:
        return per_model
    global_default = _clean_model_name(getattr(getattr(config, "models", None), "repair_model", ""))
    return global_default or _clean_model_name(default_model)


def _effective_glossary_model(config: NovelConfig, default_model: str) -> str:
    env = _clean_model_name(os.environ.get("GLOSSARY_MODEL", ""))
    if env:
        return env
    cfg = _get_model_cfg(config, default_model)
    per_model = _clean_model_name(getattr(cfg, "glossary_model", "")) if cfg is not None else ""
    if per_model:
        return per_model
    global_default = _clean_model_name(getattr(getattr(config, "models", None), "glossary_model", ""))
    return global_default or _clean_model_name(default_model)


def make_placeholders(text: str, glossary: dict[str, str]) -> tuple[str, dict[str, str]]:
    masked, mapping, _replacements = make_placeholders_with_replacements(text, glossary)
    return masked, mapping


def make_placeholders_with_replacements(
    text: str, glossary: dict[str, str]
) -> tuple[str, dict[str, str], list[dict[str, str]]]:
    """
    Replace glossary terms in `text` with stable placeholder tokens and return:
    - masked text
    - token->value mapping (for restoration)
    - ordered replacements snapshot [{src, token, value}, ...] for reproducible resume
    """

    mapping: dict[str, str] = {}
    replacements: list[dict[str, str]] = []
    for idx, key in enumerate(sorted(glossary, key=len, reverse=True)):
        token = f"ZXQ{idx:03d}QXZ"
        value = glossary.get(key, "")
        # Guard against glossary corruption where the "translation" contains placeholder tokens (e.g. "ZXQ1156QXZ"
        # or "Biến cố ZXQ125QXZ"). In that case we skip placeholdering so the model can translate from the original term.
        if isinstance(value, str) and PLACEHOLDER_LIKE_RE.search(value):
            continue
        if key in text:
            text = text.replace(key, token)
            value_str = value if isinstance(value, str) else str(value)
            mapping[token] = value_str
            replacements.append({"src": str(key), "token": token, "value": value_str})
    return text, mapping, replacements


def _placeholders_snapshot_key(unit_key: str) -> str:
    return f"placeholders__{unit_key}"


def _hash_text(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8", errors="replace")).hexdigest()


def _load_placeholders_snapshot(config: NovelConfig, unit_key: str) -> dict:
    key = _placeholders_snapshot_key(unit_key)
    path = progress_path(config, key)
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _save_placeholders_snapshot(config: NovelConfig, unit_key: str, payload: dict) -> None:
    key = _placeholders_snapshot_key(unit_key)
    config.storage.progress_dir.mkdir(parents=True, exist_ok=True)
    progress_path(config, key).write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def _clear_placeholders_snapshot(config: NovelConfig, unit_key: str) -> None:
    clear_progress(config, _placeholders_snapshot_key(unit_key))


def _apply_placeholders_snapshot(raw_text: str, replacements: list[dict[str, str]]) -> tuple[str, dict[str, str]]:
    masked = raw_text
    mapping: dict[str, str] = {}
    for item in replacements:
        if not isinstance(item, dict):
            continue
        src = item.get("src", "")
        token = item.get("token", "")
        value = item.get("value", "")
        if not isinstance(src, str) or not isinstance(token, str) or not isinstance(value, str):
            continue
        if not src or not token:
            continue
        masked = masked.replace(src, token)
        mapping[token] = value
    return masked, mapping


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


def _glossary_text_for_text(mapping: dict[str, str], text: str, *, max_chars: int) -> str:
    """
    Build a compact glossary string for a given `text`.

    `mapping` can contain placeholder tokens for an entire unit/chapter. If we include all entries in every
    per-chunk prompt, requests can become much larger than the configured `chunk_max_len`. To keep request
    sizes aligned with the configured chunking, only include glossary entries whose placeholder tokens
    actually appear in `text`.
    """

    if not mapping or not text:
        return ""
    budget = int(max_chars or 0)
    if budget <= 0:
        return ""
    budget = max(64, budget)

    tokens_raw = PLACEHOLDER_TOKEN_RE.findall(text)
    if not tokens_raw:
        return ""
    seen: set[str] = set()
    tokens: list[str] = []
    for tok in tokens_raw:
        if tok in seen:
            continue
        seen.add(tok)
        tokens.append(tok)

    lines: list[str] = []
    total = 0
    for tok in tokens:
        val = mapping.get(tok)
        if not isinstance(val, str) or not val:
            continue
        line = f"- {tok} = {val}"
        extra = len(line) + (1 if lines else 0)
        if lines and (total + extra) > budget:
            break
        lines.append(line)
        total += extra
        if total >= budget:
            break
    return "\n".join(lines)


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
    default_model = resolve_translation_model(config)
    model = _effective_glossary_model(config, default_model)
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


def _slice_center(text: str, *, max_chars: int, center_frac: float) -> str:
    text = (text or "").strip()
    if max_chars <= 0 or len(text) <= max_chars:
        return text
    frac = min(max(float(center_frac), 0.0), 1.0)
    center = int(round(frac * len(text)))
    half = max_chars // 2
    start = max(0, center - half)
    end = min(len(text), start + max_chars)
    # If clamped at the end, shift start back.
    start = max(0, end - max_chars)
    return text[start:end].strip()


def _compact_source_for_glossary(text: str, *, max_chars: int) -> str:
    text = (text or "").strip()
    if not text:
        return ""
    lines = text.splitlines()
    keep_idx: set[int] = set()
    for idx, line in enumerate(lines):
        if HAN_REGEX.search(line):
            keep_idx.add(idx)
            if idx - 1 >= 0:
                keep_idx.add(idx - 1)
            if idx + 1 < len(lines):
                keep_idx.add(idx + 1)
    kept_lines = [lines[idx] for idx in sorted(keep_idx)] if keep_idx else lines
    compact = "\n".join(kept_lines).strip()
    return _slice_head_tail(compact, max_chars)


def _glossary_progress_key(unit_key: str) -> str:
    return f"glossary__{unit_key}"


def _load_glossary_progress(config: NovelConfig, unit_key: str) -> dict:
    key = _glossary_progress_key(unit_key)
    path = progress_path(config, key)
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _save_glossary_progress(config: NovelConfig, unit_key: str, payload: dict) -> None:
    key = _glossary_progress_key(unit_key)
    config.storage.progress_dir.mkdir(parents=True, exist_ok=True)
    progress_path(config, key).write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def _clear_glossary_progress(config: NovelConfig, unit_key: str) -> None:
    key = _glossary_progress_key(unit_key)
    clear_progress(config, key)


def _repair_progress_key(prefix: str, unit_key: str) -> str:
    return f"{prefix}__{unit_key}"


def _load_repair_progress(config: NovelConfig, prefix: str, unit_key: str) -> list[str]:
    payload = load_progress(config, _repair_progress_key(prefix, unit_key))
    return payload


def _save_repair_progress(config: NovelConfig, prefix: str, unit_key: str, chunks: list[str]) -> None:
    save_progress(config, _repair_progress_key(prefix, unit_key), chunks)


def _clear_repair_progress(config: NovelConfig, prefix: str, unit_key: str) -> None:
    clear_progress(config, _repair_progress_key(prefix, unit_key))


def _clear_repair_progress_prefix(config: NovelConfig, prefix_base: str, unit_key: str) -> None:
    """
    Clear any repair progress files for a unit_key whose prefix starts with prefix_base.

    Used when repair progress keys include hash suffixes (to avoid stale resume after input changes).
    """

    progress_dir = config.storage.progress_dir
    if not progress_dir.exists():
        return
    pattern = f"{prefix_base}*__{unit_key}.json"
    for path in progress_dir.glob(pattern):
        try:
            path.unlink()
        except Exception:
            continue


def _repair_placeholder_tokens_chunked(
    config: NovelConfig,
    provider,
    model: str,
    *,
    unit_key: str,
    source_text: str,
    translated_chunks: list[str],
    mapping: dict[str, str],
) -> str:
    """
    Repair hallucinated placeholder tokens (ZXQ...QXZ / QZX...QXZ) using smaller chunk windows.

    This runs after the main translation phase and persists progress so quota releases can resume.
    """

    if not translated_chunks:
        return ""

    # Work on already-restored chunk text so the model sees real terms, not internal placeholders.
    stage_prefix = f"repair_placeholders_{_hash_text(source_text)[:12]}_{len(translated_chunks)}"
    repaired = _load_repair_progress(config, stage_prefix, unit_key)
    if repaired and len(repaired) > len(translated_chunks):
        repaired = repaired[: len(translated_chunks)]

    total = len(translated_chunks)
    if repaired:
        LOGGER.info(
            "placeholder-token repair (chunked) resuming | unit=%s repaired=%s total=%s model=%s",
            unit_key,
            len(repaired),
            total,
            model,
        )
    else:
        LOGGER.info(
            "placeholder-token repair (chunked) starting | unit=%s total=%s model=%s",
            unit_key,
            total,
            model,
        )
    for idx in range(len(repaired), total):
        center = (idx + 0.5) / max(1, total)
        src_window = _slice_center(source_text, max_chars=2400, center_frac=center)
        current = restore_placeholders(translated_chunks[idx], mapping)
        found = sorted(set(PLACEHOLDER_TOKEN_RE.findall(current)))
        if not found:
            # Still emit periodic progress so long chapters don't look stalled.
            if ((idx + 1) == total) or ((idx + 1) % 10 == 0):
                LOGGER.info(
                    "placeholder-token repair (chunked) progress | unit=%s chunk=%s/%s placeholders=0",
                    unit_key,
                    idx + 1,
                    total,
                )
            repaired.append(current)
            _save_repair_progress(config, stage_prefix, unit_key, repaired)
            continue
        examples = ", ".join(found[:8])
        LOGGER.info(
            "placeholder-token repair (chunked) fixing | unit=%s chunk=%s/%s placeholders=%s examples=%s chars=%s",
            unit_key,
            idx + 1,
            total,
            len(found),
            examples,
            len(current),
        )
        started = time.perf_counter()
        try:
            fixed = repair_placeholder_tokens_against_source(config, provider, model, src_window, current).strip()
        except RateLimitExceededError:
            LOGGER.warning(
                "placeholder-token repair (chunked) rate-limited | unit=%s chunk=%s/%s placeholders=%s",
                unit_key,
                idx + 1,
                total,
                len(found),
            )
            raise
        except Exception:
            LOGGER.exception(
                "placeholder-token repair (chunked) failed | unit=%s chunk=%s/%s placeholders=%s",
                unit_key,
                idx + 1,
                total,
                len(found),
            )
            raise
        elapsed = time.perf_counter() - started
        remaining = len(set(PLACEHOLDER_TOKEN_RE.findall(fixed)))
        LOGGER.info(
            "placeholder-token repair (chunked) fixed | unit=%s chunk=%s/%s in %.1fs remaining_placeholders=%s",
            unit_key,
            idx + 1,
            total,
            elapsed,
            remaining,
        )
        repaired.append(fixed)
        _save_repair_progress(config, stage_prefix, unit_key, repaired)

    _clear_repair_progress(config, stage_prefix, unit_key)
    return "".join(repaired)


def _repair_placeholder_tokens_in_text_chunked(
    config: NovelConfig,
    provider,
    model: str,
    *,
    unit_key: str,
    source_text: str,
    translated_text: str,
    prefix: str,
) -> str:
    """
    Repair hallucinated placeholder tokens in an already-merged translated text.

    This is used as a bounded retry pass when placeholder tokens survive the first chunked repair.
    Progress is persisted so queue workers can release on quota and resume later.
    """

    if not translated_text:
        return ""

    chunk_max_len_raw = os.environ.get("NOVEL_TTS_REPAIR_CHUNK_MAX_LEN", "").strip()
    try:
        chunk_max_len = int(chunk_max_len_raw) if chunk_max_len_raw else 0
    except ValueError:
        chunk_max_len = 0
    if chunk_max_len <= 0:
        model_chunk_raw = os.environ.get("CHUNK_MAX_LEN", "").strip()
        try:
            chunk_max_len = int(model_chunk_raw) if model_chunk_raw else 0
        except ValueError:
            chunk_max_len = 0
    if chunk_max_len <= 0:
        chunk_max_len = int(getattr(config.translation, "chunk_max_len", 0) or 0)
    if chunk_max_len <= 0:
        chunk_max_len = 1600
    chunk_max_len = max(400, int(chunk_max_len))

    input_fingerprint = _hash_text(f"{chunk_max_len}\n{source_text}\n{translated_text}")[:12]
    stage_prefix = f"{prefix}_{input_fingerprint}"

    chunks = split_chunks(translated_text, chunk_max_len)
    repaired = _load_repair_progress(config, stage_prefix, unit_key)
    if repaired and len(repaired) > len(chunks):
        repaired = repaired[: len(chunks)]

    total = len(chunks)
    if repaired:
        LOGGER.info(
            "placeholder-token repair (retry chunked) resuming | unit=%s pass=%s repaired=%s total=%s chunk_max_len=%s model=%s",
            unit_key,
            prefix,
            len(repaired),
            total,
            chunk_max_len,
            model,
        )
    else:
        LOGGER.info(
            "placeholder-token repair (retry chunked) starting | unit=%s pass=%s total=%s chunk_max_len=%s model=%s",
            unit_key,
            prefix,
            total,
            chunk_max_len,
            model,
        )

    source_window_chars = max(1200, int(chunk_max_len * 2.2))
    for idx in range(len(repaired), total):
        current = chunks[idx]
        found = sorted(set(PLACEHOLDER_TOKEN_RE.findall(current)))
        if not found:
            if ((idx + 1) == total) or ((idx + 1) % 10 == 0):
                LOGGER.info(
                    "placeholder-token repair (retry chunked) progress | unit=%s pass=%s chunk=%s/%s placeholders=0",
                    unit_key,
                    prefix,
                    idx + 1,
                    total,
                )
            repaired.append(current)
            _save_repair_progress(config, stage_prefix, unit_key, repaired)
            continue

        examples = ", ".join(found[:8])
        center = (idx + 0.5) / max(1, total)
        src_window = _slice_center(source_text, max_chars=source_window_chars, center_frac=center)
        LOGGER.info(
            "placeholder-token repair (retry chunked) fixing | unit=%s pass=%s chunk=%s/%s placeholders=%s examples=%s chars=%s",
            unit_key,
            prefix,
            idx + 1,
            total,
            len(found),
            examples,
            len(current),
        )
        started = time.perf_counter()
        try:
            fixed = repair_placeholder_tokens_against_source(config, provider, model, src_window, current).strip()
        except RateLimitExceededError:
            LOGGER.warning(
                "placeholder-token repair (retry chunked) rate-limited | unit=%s pass=%s chunk=%s/%s placeholders=%s",
                unit_key,
                prefix,
                idx + 1,
                total,
                len(found),
            )
            raise
        except Exception:
            LOGGER.exception(
                "placeholder-token repair (retry chunked) failed | unit=%s pass=%s chunk=%s/%s placeholders=%s",
                unit_key,
                prefix,
                idx + 1,
                total,
                len(found),
            )
            raise
        elapsed = time.perf_counter() - started
        remaining = len(set(PLACEHOLDER_TOKEN_RE.findall(fixed)))
        LOGGER.info(
            "placeholder-token repair (retry chunked) fixed | unit=%s pass=%s chunk=%s/%s in %.1fs remaining_placeholders=%s",
            unit_key,
            prefix,
            idx + 1,
            total,
            elapsed,
            remaining,
        )
        repaired.append(fixed)
        _save_repair_progress(config, stage_prefix, unit_key, repaired)
        time.sleep(_effective_chunk_sleep_seconds(config, model))

    _clear_repair_progress(config, stage_prefix, unit_key)
    return "".join(repaired)


def final_cleanup_chunked(config: NovelConfig, provider, model: str, *, unit_key: str, text: str, mapping: dict[str, str]) -> str:
    chunk_max_len_raw = os.environ.get("NOVEL_TTS_REPAIR_CHUNK_MAX_LEN", "").strip()
    try:
        chunk_max_len = int(chunk_max_len_raw) if chunk_max_len_raw else 0
    except ValueError:
        chunk_max_len = 0
    if chunk_max_len <= 0:
        model_chunk_raw = os.environ.get("CHUNK_MAX_LEN", "").strip()
        try:
            chunk_max_len = int(model_chunk_raw) if model_chunk_raw else 0
        except ValueError:
            chunk_max_len = 0
    if chunk_max_len <= 0:
        chunk_max_len = _effective_chunk_max_len(config, model)
    if chunk_max_len <= 0:
        chunk_max_len = 1600
    chunk_max_len = max(400, int(chunk_max_len))

    stage_fingerprint = _hash_text(str(chunk_max_len) + "\n" + text)[:12]
    stage_prefix = f"final_cleanup_{stage_fingerprint}"
    chunks = split_chunks(text, chunk_max_len)
    translated_chunks = _load_repair_progress(config, stage_prefix, unit_key)
    total = len(chunks)
    if translated_chunks:
        LOGGER.info(
            "final_cleanup (chunked) resuming | unit=%s repaired=%s total=%s chunk_max_len=%s model=%s",
            unit_key,
            len(translated_chunks),
            total,
            chunk_max_len,
            model,
        )
    else:
        LOGGER.info(
            "final_cleanup (chunked) starting | unit=%s total=%s chunk_max_len=%s model=%s",
            unit_key,
            total,
            chunk_max_len,
            model,
        )
    for idx, chunk in enumerate(chunks[len(translated_chunks):], len(translated_chunks) + 1):
        LOGGER.info(
            "final_cleanup (chunked) fixing | unit=%s chunk=%s/%s chars=%s",
            unit_key,
            idx,
            total,
            len(chunk),
        )
        started = time.perf_counter()
        glossary_text = _glossary_text_for_text(mapping, chunk, max_chars=chunk_max_len)
        prompt = (
            f"{_strip_placeholder_rules(config.translation.base_rules)}\n"
            f"Glossary dùng bắt buộc nếu xuất hiện:\n{glossary_text}\n\n"
            "Dưới đây là một đoạn bản dịch tiếng Việt còn lỗi. "
            "Hãy chỉ sửa lỗi còn sót: chữ Hán chưa dịch, câu cú gượng, xuống dòng xấu, tiêu đề chương dính hoặc lặp. "
            "Không thêm ý mới. Chỉ trả về đúng đoạn đã sửa.\n\n"
            f"{chunk}"
        )
        try:
            fixed = _generate_once(provider, model, prompt).strip()
        except RateLimitExceededError:
            LOGGER.warning(
                "final_cleanup (chunked) rate-limited | unit=%s chunk=%s/%s",
                unit_key,
                idx,
                total,
            )
            raise
        except Exception:
            LOGGER.exception(
                "final_cleanup (chunked) failed | unit=%s chunk=%s/%s",
                unit_key,
                idx,
                total,
            )
            raise
        LOGGER.info(
            "final_cleanup (chunked) fixed | unit=%s chunk=%s/%s in %.1fs",
            unit_key,
            idx,
            total,
            time.perf_counter() - started,
        )
        translated_chunks.append(fixed)
        _save_repair_progress(config, stage_prefix, unit_key, translated_chunks)
        time.sleep(_effective_chunk_sleep_seconds(config, model))
    _clear_repair_progress(config, stage_prefix, unit_key)
    return "".join(translated_chunks)


def repair_against_source_chunked(
    config: NovelConfig,
    provider,
    model: str,
    *,
    unit_key: str,
    source_text: str,
    translated_text: str,
) -> str:
    """
    Repair against source in multiple smaller windows to reduce TPM bursts.

    This is triggered only when we still detect Han residue after other cleanup stages.
    Progress is persisted so queue workers can release on quota and resume later.
    """

    chunk_max_len_raw = os.environ.get("NOVEL_TTS_REPAIR_CHUNK_MAX_LEN", "").strip()
    try:
        chunk_max_len = int(chunk_max_len_raw) if chunk_max_len_raw else 0
    except ValueError:
        chunk_max_len = 0
    if chunk_max_len <= 0:
        model_chunk_raw = os.environ.get("CHUNK_MAX_LEN", "").strip()
        try:
            chunk_max_len = int(model_chunk_raw) if model_chunk_raw else 0
        except ValueError:
            chunk_max_len = 0
    if chunk_max_len <= 0:
        chunk_max_len = int(getattr(config.translation, "chunk_max_len", 0) or 0)
    if chunk_max_len <= 0:
        chunk_max_len = 1600
    chunk_max_len = max(400, int(chunk_max_len))

    # Keep translated windows smaller; include a bit more source context.
    translated_windows = split_chunks(translated_text, chunk_max_len)
    source_window_chars = max(1200, int(chunk_max_len * 2.2))

    stage_fingerprint = _hash_text(str(chunk_max_len) + "\n" + source_text + "\n" + translated_text)[:12]
    stage_prefix = f"repair_against_source_{stage_fingerprint}"
    repaired = _load_repair_progress(config, stage_prefix, unit_key)
    if repaired and len(repaired) > len(translated_windows):
        repaired = repaired[: len(translated_windows)]

    total = len(translated_windows)
    if repaired:
        LOGGER.info(
            "repair_against_source (chunked) resuming | unit=%s repaired=%s total=%s chunk_max_len=%s model=%s",
            unit_key,
            len(repaired),
            total,
            chunk_max_len,
            model,
        )
    else:
        LOGGER.info(
            "repair_against_source (chunked) starting | unit=%s total=%s chunk_max_len=%s model=%s",
            unit_key,
            total,
            chunk_max_len,
            model,
        )
    for idx in range(len(repaired), total):
        center = (idx + 0.5) / max(1, total)
        src_window = _slice_center(source_text, max_chars=source_window_chars, center_frac=center)
        tr_window = translated_windows[idx]
        LOGGER.info(
            "repair_against_source (chunked) fixing | unit=%s chunk=%s/%s chars=%s han=%s",
            unit_key,
            idx + 1,
            total,
            len(tr_window),
            count_han_chars(tr_window),
        )
        # This stage exists to eliminate remaining Han residue. If a window has no Han at all,
        # avoid sending the (possibly sensitive) source excerpt to the provider unnecessarily.
        if not has_han(tr_window):
            repaired.append(tr_window)
            _save_repair_progress(config, stage_prefix, unit_key, repaired)
            continue
        started = time.perf_counter()
        prompt = (
            f"{_strip_placeholder_rules(config.translation.base_rules)}\n"
            "Dưới đây là bản gốc tiếng Trung (TRÍCH) và bản dịch tiếng Việt hiện có (TRÍCH) của cùng một đoạn.\n"
            "Nhiệm vụ của ngươi:\n"
            "- Chỉ sửa đoạn BẢN DỊCH HIỆN CÓ sao cho khớp với BẢN GỐC.\n"
            "- Giữ nguyên ý và thứ tự theo BẢN GỐC, không thêm ý, không cắt mất nội dung.\n"
            "- Phải thay hết toàn bộ chữ Hán còn sót (kể cả chữ Hán lẻ bị trộn trong câu tiếng Việt).\n"
            "- Không viết tiêu đề/ghi chú, không nhắc lại phần ngoài đoạn này.\n"
            "- Chỉ xuất ra đoạn tiếng Việt đã sửa (TRÍCH) tương ứng.\n\n"
            f"BẢN GỐC (TRÍCH):\n{src_window}\n\n"
            f"BẢN DỊCH HIỆN CÓ (TRÍCH):\n{tr_window}"
        )
        try:
            fixed = _generate_once(provider, model, prompt).strip()
        except PromptBlockedError as exc:
            prompt_feedback = {}
            try:
                prompt_feedback = (exc.payload or {}).get("promptFeedback") or {}
            except Exception:
                prompt_feedback = {}
            LOGGER.warning(
                "repair_against_source (chunked) prompt blocked | unit=%s chunk=%s/%s reason=%s feedback=%s",
                unit_key,
                idx + 1,
                total,
                getattr(exc, "reason", "UNKNOWN"),
                prompt_feedback,
            )
            # Fallback: do a best-effort "Han-only" cleanup without including the Chinese source excerpt.
            # This is less faithful than repairing against source, but is better than failing the entire unit.
            fallback_prompt = (
                f"{_strip_placeholder_rules(config.translation.base_rules)}\n"
                "Dưới đây là một đoạn tiếng Việt (TRÍCH) còn lẫn chữ Hán.\n"
                "Nhiệm vụ của ngươi:\n"
                "- Dịch hết toàn bộ chữ Hán còn sót sang tiếng Việt.\n"
                "- Giữ nguyên ý và thứ tự theo đoạn hiện có, không thêm ý, không cắt mất nội dung.\n"
                "- Không viết tiêu đề/ghi chú.\n"
                "- Chỉ xuất ra đoạn tiếng Việt đã sửa (TRÍCH).\n\n"
                f"ĐOẠN CẦN SỬA (TRÍCH):\n{tr_window}"
            )
            try:
                fixed = _generate_once(provider, model, fallback_prompt).strip()
            except Exception as exc2:
                LOGGER.warning(
                    "repair_against_source (chunked) fallback failed; scrubbing Han locally | unit=%s chunk=%s/%s err=%r",
                    unit_key,
                    idx + 1,
                    total,
                    exc2,
                )
                fixed = HAN_REGEX.sub("", tr_window)
        except RateLimitExceededError:
            LOGGER.warning(
                "repair_against_source (chunked) rate-limited | unit=%s chunk=%s/%s",
                unit_key,
                idx + 1,
                total,
            )
            raise
        except Exception:
            LOGGER.exception(
                "repair_against_source (chunked) failed | unit=%s chunk=%s/%s",
                unit_key,
                idx + 1,
                total,
            )
            raise
        LOGGER.info(
            "repair_against_source (chunked) fixed | unit=%s chunk=%s/%s in %.1fs han=%s",
            unit_key,
            idx + 1,
            total,
            time.perf_counter() - started,
            count_han_chars(fixed),
        )
        repaired.append(fixed)
        _save_repair_progress(config, stage_prefix, unit_key, repaired)
        time.sleep(_effective_chunk_sleep_seconds(config, model))

    _clear_repair_progress(config, stage_prefix, unit_key)
    return "".join(repaired)


def _extract_glossary_updates_chunked(
    config: NovelConfig,
    provider,
    source_text: str,
    translated_text: str,
    *,
    unit_key: str,
) -> dict[str, str]:
    """
    Extract glossary updates using multiple smaller windows to reduce per-call TPM.

    This is similar in spirit to translation chunking: we persist partial progress to
    `input/<novel>/.progress/glossary__<unit_key>.json` so quota releases can resume later.
    """

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

    # Per-window budgets (smaller than the one-shot defaults).
    # In queue mode, align window sizes with the per-model CHUNK_MAX_LEN injected by the worker, so glossary/repair
    # stays consistent with translation chunking for each model.
    chunk_max_len_raw = os.environ.get("CHUNK_MAX_LEN", "").strip()
    try:
        model_chunk_max_len = int(chunk_max_len_raw) if chunk_max_len_raw else 0
    except ValueError:
        model_chunk_max_len = 0
    if model_chunk_max_len <= 0:
        model_chunk_max_len = int(getattr(config.translation, "chunk_max_len", 0) or 0)

    win_source_raw = os.environ.get("NOVEL_TTS_GLOSSARY_EXTRACT_MAX_SOURCE_CHARS_PER_WINDOW", "").strip()
    win_translated_raw = os.environ.get("NOVEL_TTS_GLOSSARY_EXTRACT_MAX_TRANSLATED_CHARS_PER_WINDOW", "").strip()
    try:
        win_source_chars = (
            int(win_source_raw) if win_source_raw else max(400, min(max_source_chars, model_chunk_max_len))
        )
    except ValueError:
        win_source_chars = max(400, min(max_source_chars, model_chunk_max_len))
    try:
        win_translated_chars = (
            int(win_translated_raw)
            if win_translated_raw
            else max(500, min(max_translated_chars, int(model_chunk_max_len * 1.7) if model_chunk_max_len > 0 else 0))
        )
    except ValueError:
        win_translated_chars = max(500, min(max_translated_chars, int(model_chunk_max_len * 1.7) if model_chunk_max_len > 0 else 0))

    win_count_raw = os.environ.get("NOVEL_TTS_GLOSSARY_EXTRACT_WINDOW_COUNT", "").strip()
    try:
        window_count = int(win_count_raw) if win_count_raw else 3
    except ValueError:
        window_count = 3
    window_count = max(1, min(7, window_count))

    # If the chapter is small, fall back to one-shot behavior.
    compact_source, compact_translated, was_compacted = _compact_glossary_context(
        source_text,
        translated_text,
        max_source_chars=max_source_chars,
        max_translated_chars=max_translated_chars,
    )
    total_chars = len(compact_source) + len(compact_translated)
    if (not was_compacted) and (total_chars <= (win_source_chars + win_translated_chars)):
        return _extract_glossary_updates(config, provider, source_text, translated_text)

    default_model = resolve_translation_model(config)
    model = _effective_glossary_model(config, default_model)
    LOGGER.info(
        "Glossary extract chunked | unit=%s windows=%s src_win=%s tr_win=%s model=%s",
        unit_key,
        window_count,
        win_source_chars,
        win_translated_chars,
        model,
    )

    # Choose window centers: evenly spaced from head->tail, including both ends.
    if window_count == 1:
        centers = [0.5]
    else:
        centers = [i / (window_count - 1) for i in range(window_count)]

    progress = _load_glossary_progress(config, unit_key) if unit_key else {}
    start_index = int(progress.get("next_window_index", 0) or 0)
    extracted: dict[str, str] = {}
    raw_updates = progress.get("updates")
    if isinstance(raw_updates, dict):
        extracted = {str(k): str(v) for k, v in raw_updates.items() if isinstance(k, str) and isinstance(v, str)}

    if start_index > 0 or extracted:
        LOGGER.info(
            "Glossary extract chunked resuming | unit=%s next_window=%s/%s extracted=%s model=%s",
            unit_key,
            start_index + 1,
            len(centers),
            len(extracted),
            model,
        )
    else:
        LOGGER.info(
            "Glossary extract chunked starting | unit=%s windows=%s model=%s",
            unit_key,
            len(centers),
            model,
        )

    for idx, center in enumerate(centers[start_index:], start_index + 1):
        # Align slices by relative position in the raw texts.
        raw_src = _slice_center(source_text, max_chars=max_source_chars * 2, center_frac=center)
        raw_tr = _slice_center(translated_text, max_chars=win_translated_chars, center_frac=center)
        compact_src = _compact_source_for_glossary(raw_src, max_chars=win_source_chars)
        compact_tr = _slice_head_tail(raw_tr, win_translated_chars)
        LOGGER.info(
            "Glossary extract chunked extracting | unit=%s window=%s/%s center=%.2f src_chars=%s tr_chars=%s extracted=%s",
            unit_key,
            idx,
            len(centers),
            center,
            len(compact_src),
            len(compact_tr),
            len(extracted),
        )
        started = time.perf_counter()
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
            f"WINDOW {idx}/{len(centers)}:\n"
            f"BẢN GỐC (TRÍCH):\n{compact_src}\n\n"
            f"BẢN DỊCH (TRÍCH):\n{compact_tr}\n"
        )
        try:
            updates = _parse_glossary_response(_generate_once(provider, model, prompt))
        except RateLimitExceededError:
            LOGGER.warning(
                "Glossary extract chunked rate-limited | unit=%s window=%s/%s",
                unit_key,
                idx,
                len(centers),
            )
            raise
        except Exception:
            LOGGER.exception(
                "Glossary extract chunked failed | unit=%s window=%s/%s",
                unit_key,
                idx,
                len(centers),
            )
            raise
        updates = _sanitize_extracted_glossary_updates(updates, translated_text)
        before_count = len(extracted)
        added_count = 0
        if updates:
            for k, v in updates.items():
                if k not in extracted:
                    added_count += 1
                extracted[k] = v
        if unit_key:
            _save_glossary_progress(
                config,
                unit_key,
                {
                    "next_window_index": idx,
                    "updates": extracted,
                },
            )
        LOGGER.info(
            "Glossary extract chunked extracted | unit=%s window=%s/%s in %.1fs added=%s total=%s",
            unit_key,
            idx,
            len(centers),
            time.perf_counter() - started,
            added_count,
            max(before_count, len(extracted)),
        )

    if unit_key:
        _clear_glossary_progress(config, unit_key)
    return extracted


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
    provider = get_translation_provider(config.models.provider, config=config)
    if unit_key:
        default_model = resolve_translation_model(config)
        glossary_model = _effective_glossary_model(config, default_model)
        LOGGER.info("QUEUE_PHASE glossary | unit=%s model=%s", unit_key, glossary_model or "unknown")
    if marker_path is not None:
        _write_glossary_marker(marker_path, status=GLOSSARY_STATUS_PENDING, last_error="")
    try:
        if unit_key:
            updates = _extract_glossary_updates_chunked(
                config,
                provider,
                source_text,
                translated_text,
                unit_key=unit_key,
            )
        else:
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


def chapter_source_hash_path(config: NovelConfig, source_path: Path, chapter_num: str) -> Path:
    """
    Per-chapter source hash used to detect whether a chapter's origin text changed since it was translated.

    Stored alongside the chapter part in .parts/<origin_stem>/<NNNN>.source.sha256.
    """
    return chapter_part_path(config, source_path, chapter_num).with_suffix(".source.sha256")


def _normalize_source_text_for_hash(text: str) -> str:
    # Normalize newlines + trailing whitespace so "touch"/rewrites that don't change content
    # don't force pointless re-translation.
    normalized = (text or "").replace("\r\n", "\n").replace("\r", "\n")
    normalized = "\n".join(line.rstrip() for line in normalized.split("\n"))
    return normalized.strip()


def chapter_source_sha256(source_text: str) -> str:
    normalized = _normalize_source_text_for_hash(source_text)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def read_chapter_source_hash(config: NovelConfig, source_path: Path, chapter_num: str) -> str | None:
    path = chapter_source_hash_path(config, source_path, chapter_num)
    if not path.exists():
        return None
    try:
        value = path.read_text(encoding="utf-8", errors="replace").strip()
    except Exception:
        return None
    return value or None


def write_chapter_source_hash(config: NovelConfig, source_path: Path, chapter_num: str, sha256: str) -> None:
    path = chapter_source_hash_path(config, source_path, chapter_num)
    path.parent.mkdir(parents=True, exist_ok=True)
    sha256 = (sha256 or "").strip()
    if not sha256:
        return
    if path.exists():
        try:
            existing = path.read_text(encoding="utf-8", errors="replace").strip()
        except Exception:
            existing = ""
        if existing == sha256:
            return
    path.write_text(sha256 + "\n", encoding="utf-8")


def chapter_source_changed(
    config: NovelConfig,
    source_path: Path,
    chapter_num: str,
    *,
    source_text: str,
    baseline_if_missing: bool = True,
) -> bool:
    """
    Returns True when we can prove the chapter's source text changed since last translation.

    If the stored hash is missing and baseline_if_missing is True, this writes the current hash and returns False.
    """
    current = chapter_source_sha256(source_text)
    stored = read_chapter_source_hash(config, source_path, chapter_num)
    if stored is None:
        if baseline_if_missing:
            write_chapter_source_hash(config, source_path, chapter_num, current)
        return False
    return stored != current


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


def _source_chapter_has_title(config: NovelConfig, source_text: str) -> bool:
    match = re.search(config.translation.chapter_regex, source_text, flags=re.M)
    if match is None:
        return False
    try:
        title = match.group(2)
    except IndexError:
        return False
    return bool(str(title or "").strip())


def _normalize_translated_chapter_text(
    config: NovelConfig,
    chapter_num: str,
    source_text: str,
    translated_text: str,
) -> str:
    cleaned = (translated_text or "").strip()
    if not cleaned:
        return ""
    if not _source_chapter_has_title(config, source_text):
        return cleaned + "\n"

    from .polish import normalize_text

    return normalize_text(
        cleaned,
        chapter_num,
        config.translation.polish_replacements,
        force_title_fold=True,
    )


def _generate_once(provider, model: str, prompt: str) -> str:
    return strip_model_wrappers(provider.generate(model, prompt))


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


def _generate_translation_chunk(
    provider,
    translation_cfg,
    model: str,
    glossary_text: str,
    chunk: str,
    *,
    chunk_max_len: int,
) -> str:
    primary_prompt = (
        f"{translation_cfg.base_rules}\n"
        f"Glossary dùng bắt buộc nếu xuất hiện:\n{glossary_text}\n\n"
        "Hãy tự kiểm tra và sửa ngay trong một lần trả lời trước khi xuất kết quả cuối cùng.\n\n"
        f"Dịch đoạn sau sang tiếng Việt:\n{chunk.replace(chr(10), f' {translation_cfg.line_token} ')}"
    )
    try:
        return _generate_once(provider, model, primary_prompt)
    except PromptBlockedError as exc:
        LOGGER.warning("Provider blocked chunk, retrying with safe literary prompt | reason=%s", exc.reason)

    safe_prompt = _safe_literary_prompt(
        translation_cfg.base_rules,
        glossary_text,
        translation_cfg.line_token,
        chunk,
    )
    try:
        return _generate_once(provider, model, safe_prompt)
    except PromptBlockedError as exc:
        LOGGER.warning("Provider still blocked chunk, retrying with smaller segments | reason=%s", exc.reason)

    segment_limit = max(180, min(int(chunk_max_len) // 3, 320))
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
            outputs.append(_generate_once(provider, model, segment_prompt))
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
            outputs.append(_generate_once(provider, model, softened_prompt))
    return "".join(outputs)


def final_cleanup(config: NovelConfig, provider, model: str, text: str, mapping: dict[str, str]) -> str:
    chunk_max_len = _effective_chunk_max_len(config, model)
    glossary_text = _glossary_text_for_text(mapping, text, max_chars=chunk_max_len)
    prompt = (
        f"{_strip_placeholder_rules(config.translation.base_rules)}\n"
        f"Glossary dùng bắt buộc nếu xuất hiện:\n{glossary_text}\n\n"
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
    lines = text.splitlines()
    for idx, line in enumerate(lines):
        line = apply_rule_based_han_fixes(line, translation_cfg.han_fallback_replacements)
        line = strip_small_han_residue(line)
        if not has_han(line):
            lines[idx] = line
            continue
        glossary_text = _glossary_text_for_text(mapping, line, max_chars=_effective_chunk_max_len(config, model))
        prompt = (
            f"{_strip_placeholder_rules(translation_cfg.base_rules)}\n"
            f"Glossary dùng bắt buộc nếu xuất hiện:\n{glossary_text}\n\n"
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
            glossary_text = _glossary_text_for_text(
                mapping,
                segment,
                max_chars=int(getattr(translation_cfg, "chunk_max_len", 0) or 0),
            )
            prompt = (
                f"{_strip_placeholder_rules(translation_cfg.base_rules)}\n"
                f"Glossary dùng bắt buộc nếu xuất hiện:\n{glossary_text}\n\n"
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
    translate_model = resolve_translation_model(config)
    repair_model = _effective_repair_model(config, translate_model)
    refresh_glossary(config)
    provider = get_translation_provider(config.models.provider, config=config)
    raw_sha1 = _hash_text(raw_text)
    snapshot = _load_placeholders_snapshot(config, unit_key)
    snapshot_sha1 = snapshot.get("raw_sha1") if isinstance(snapshot, dict) else None
    snapshot_repls = snapshot.get("replacements") if isinstance(snapshot, dict) else None
    if isinstance(snapshot_sha1, str) and snapshot_sha1 == raw_sha1 and isinstance(snapshot_repls, list) and snapshot_repls:
        masked, mapping = _apply_placeholders_snapshot(raw_text, snapshot_repls)
        LOGGER.info(
            "Loaded placeholder snapshot | unit=%s replacements=%s",
            unit_key,
            len(snapshot_repls),
        )
    else:
        masked, mapping, replacements = make_placeholders_with_replacements(raw_text, translation_cfg.glossary)
        _save_placeholders_snapshot(
            config,
            unit_key,
            {
                "raw_sha1": raw_sha1,
                "replacements": replacements,
            },
        )
        LOGGER.info(
            "Saved placeholder snapshot | unit=%s replacements=%s",
            unit_key,
            len(replacements),
        )
    effective_chunk_max_len = _effective_chunk_max_len(config, translate_model)
    chunks = split_chunks(masked, effective_chunk_max_len)
    translate_fingerprint = _hash_text(
        f"{raw_sha1}\n{effective_chunk_max_len}\n{len(masked)}\n{len(mapping)}\n{translate_model}"
    )[:12]
    translate_progress_key = f"translate_{translate_fingerprint}__{unit_key}"
    translated_chunks = load_progress(config, translate_progress_key)
    if translated_chunks:
        LOGGER.info(
            "Loaded translate progress | unit=%s chunks=%s/%s chunk_max_len=%s",
            unit_key,
            len(translated_chunks),
            len(chunks),
            effective_chunk_max_len,
        )
    LOGGER.info("QUEUE_PHASE translate | unit=%s model=%s", unit_key, translate_model or "unknown")
    for idx, chunk in enumerate(chunks[len(translated_chunks):], len(translated_chunks) + 1):
        LOGGER.info("Translating %s chunk %s/%s", unit_key, idx, len(chunks))
        started = time.perf_counter()
        glossary_text = _glossary_text_for_text(mapping, chunk, max_chars=effective_chunk_max_len)
        result = _generate_translation_chunk(
            provider,
            translation_cfg,
            translate_model,
            glossary_text,
            chunk,
            chunk_max_len=effective_chunk_max_len,
        )
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
        save_progress(config, translate_progress_key, translated_chunks)
        time.sleep(_effective_chunk_sleep_seconds(config, translate_model))
    LOGGER.info("QUEUE_PHASE repair | unit=%s model=%s", unit_key, (repair_model or "unknown"))
    merged = restore_placeholders("".join(translated_chunks), mapping)
    # If any placeholder-like tokens survive restoration, they are either hallucinated tokens or stale progress.
    # Repair them chunk-by-chunk so TPM gating can resume instead of failing the whole job.
    if PLACEHOLDER_TOKEN_RE.search(merged):
        LOGGER.info("Placeholder tokens detected after merge; repairing (chunked) | unit=%s", unit_key)
        started = time.perf_counter()
        merged = _repair_placeholder_tokens_chunked(
            config,
            provider,
            repair_model,
            unit_key=unit_key,
            source_text=raw_text,
            translated_chunks=translated_chunks,
            mapping=mapping,
        )
        LOGGER.info(
            "placeholder-token repair (chunked) done in %.1fs | unit=%s",
            time.perf_counter() - started,
            unit_key,
        )
        merged = restore_placeholders(merged, mapping)

    merged = post_process(merged, translation_cfg.post_replacements)
    merged = apply_rule_based_han_fixes(merged, translation_cfg.han_fallback_replacements)
    if has_han(merged) and count_han_chars(merged) > 12:
        LOGGER.info("Han residue detected; running final_cleanup | unit=%s count=%s", unit_key, count_han_chars(merged))
        try:
            started = time.perf_counter()
            merged = restore_placeholders(
                final_cleanup_chunked(config, provider, repair_model, unit_key=unit_key, text=merged, mapping=mapping),
                mapping,
            )
            LOGGER.info(
                "final_cleanup done in %.1fs | unit=%s",
                time.perf_counter() - started,
                unit_key,
            )
            merged = post_process(merged, translation_cfg.post_replacements)
            merged = apply_rule_based_han_fixes(merged, translation_cfg.han_fallback_replacements)
        except RateLimitExceededError:
            raise
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
        merged = repair_against_source_chunked(
            config,
            provider,
            repair_model,
            unit_key=unit_key,
            source_text=raw_text,
            translated_text=merged,
        )
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
        remaining = sorted(set(PLACEHOLDER_TOKEN_RE.findall(merged)))
        LOGGER.warning(
            "Placeholder tokens survived final restoration; retrying repair up to 2 more passes | unit=%s count=%s examples=%s",
            unit_key,
            len(remaining),
            ", ".join(remaining[:8]),
        )
        for retry_idx, prefix in enumerate(("repair_placeholders_retry1", "repair_placeholders_retry2"), 1):
            if not PLACEHOLDER_TOKEN_RE.search(merged):
                break
            LOGGER.warning("Placeholder-token retry pass %s/2 | unit=%s", retry_idx, unit_key)
            merged = _repair_placeholder_tokens_in_text_chunked(
                config,
                provider,
                repair_model,
                unit_key=unit_key,
                source_text=raw_text,
                translated_text=merged,
                prefix=prefix,
            )
            merged = post_process(merged, translation_cfg.post_replacements)
            merged = apply_rule_based_han_fixes(merged, translation_cfg.han_fallback_replacements)

        if PLACEHOLDER_TOKEN_RE.search(merged):
            remaining = sorted(set(PLACEHOLDER_TOKEN_RE.findall(merged)))
            LOGGER.warning(
                "Accepting translation despite residual placeholder tokens (operator will run queue repair) | unit=%s count=%s examples=%s",
                unit_key,
                len(remaining),
                ", ".join(remaining[:8]),
            )
    clear_progress(config, translate_progress_key)
    return merged


def translate_chapter(config: NovelConfig, source_path: Path, chapter_num: str, force: bool = False) -> Path:
    chapter_map = load_chapter_map(config, source_path)
    if chapter_num not in chapter_map:
        raise ValueError(f"Chapter {chapter_num} not found in {source_path.name}")
    part_path = chapter_part_path(config, source_path, chapter_num)
    marker_path = glossary_marker_path(config, source_path, chapter_num)
    pending_glossary = is_glossary_pending(config, source_path, chapter_num)
    part_path.parent.mkdir(parents=True, exist_ok=True)
    source_text = chapter_map[chapter_num]
    unit_key = f"{source_path.name}__{chapter_num}"
    current_hash = chapter_source_sha256(source_text)

    if part_path.exists() and not force and not pending_glossary:
        stored_hash = read_chapter_source_hash(config, source_path, chapter_num)
        if stored_hash is not None and stored_hash == current_hash:
            return part_path
        # Migration fallback: if we don't have a stored hash yet, use mtime once to avoid
        # potentially skipping real edits that happened before this tracking was introduced.
        if stored_hash is None and part_path.stat().st_mtime >= source_path.stat().st_mtime:
            write_chapter_source_hash(config, source_path, chapter_num, current_hash)
            return part_path

    # Force runs should not resume stale progress snapshots. Glossary changes can shift placeholder tokens and
    # leave orphan ZXQ...QXZ tokens in the merged output if we resume old chunks.
    if force:
        clear_progress(config, unit_key)
        _clear_repair_progress_prefix(config, "translate", unit_key)
        _clear_placeholders_snapshot(config, unit_key)
        # Also clear repair-stage progress so we don't mix old windows with new masking.
        _clear_repair_progress_prefix(config, "repair_placeholders", unit_key)
        _clear_repair_progress_prefix(config, "repair_placeholders_retry1", unit_key)
        _clear_repair_progress_prefix(config, "repair_placeholders_retry2", unit_key)
        _clear_repair_progress_prefix(config, "final_cleanup", unit_key)
        _clear_repair_progress_prefix(config, "repair_against_source", unit_key)

    stored_hash = read_chapter_source_hash(config, source_path, chapter_num)
    needs_translate = force or (not part_path.exists())
    if not needs_translate:
        if stored_hash is not None:
            needs_translate = stored_hash != current_hash
        else:
            # No hash yet: fall back to mtime (migration behavior).
            needs_translate = part_path.stat().st_mtime < source_path.stat().st_mtime
    existing_text = part_path.read_text(encoding="utf-8", errors="replace") if part_path.exists() else ""
    if needs_translate:
        text = translate_unit(config, unit_key, source_text)
    else:
        text = existing_text
    text = _normalize_translated_chapter_text(config, chapter_num, source_text, text)
    if needs_translate or text != existing_text:
        part_path.write_text(text, encoding="utf-8")

    if config.translation.auto_update_glossary and (pending_glossary or needs_translate):
        update_glossary_from_chapter(
            config,
            source_text,
            text,
            marker_path=marker_path,
            unit_key=unit_key,
        )
    write_chapter_source_hash(config, source_path, chapter_num, current_hash)
    return part_path


def rebuild_translated_file(config: NovelConfig, source_path: Path, require_complete: bool = True) -> Path | None:
    chapters = load_source_chapters(config, source_path)
    merged_parts: list[str] = []
    for chapter_num, chapter_text in chapters:
        part_path = chapter_part_path(config, source_path, chapter_num)
        if not part_path.exists():
            if require_complete:
                return None
            continue
        part_text = part_path.read_text(encoding="utf-8")
        merged_parts.append(_normalize_translated_chapter_text(config, chapter_num, chapter_text, part_text).strip())
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
