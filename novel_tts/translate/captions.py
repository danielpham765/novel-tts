from __future__ import annotations

import json
import re
from pathlib import Path

from novel_tts.common.logging import get_logger
from novel_tts.config.models import NovelConfig

from .model import resolve_translation_model
from .glossary import build_glossary_text
from .novel import update_glossary_from_chapter
from .providers import get_translation_provider, is_queue_worker_env

LOGGER = get_logger(__name__)
HAN_REGEX = re.compile(r"[\u4e00-\u9fff]")


def collect_subtitle_text_line_indices(lines: list[str]) -> list[int]:
    indices: list[int] = []
    for idx in range(len(lines) - 1):
        is_index = lines[idx].strip().isdigit()
        is_time = "-->" in lines[idx + 1]
        if not is_index or not is_time:
            continue
        pointer = idx + 2
        while pointer < len(lines) and lines[pointer].strip():
            indices.append(pointer)
            pointer += 1
    return indices


def _srt_time_to_menu_time(ts: str) -> str:
    hh, mm, rest = ts.split(":")
    ss = rest.split(",")[0]
    if int(hh) > 0:
        return f"{int(hh)}:{mm.zfill(2)}:{ss.zfill(2)}"
    return f"{int(mm)}:{ss.zfill(2)}"


def _write_chapter_menu_from_srt(config: NovelConfig, srt_lines: list[str], output_name: str) -> Path | None:
    blocks = "\n".join(srt_lines).split("\n\n")
    seen: set[str] = set()
    items: list[str] = []
    for block in blocks:
        lines = [line.strip() for line in block.splitlines() if line.strip()]
        if len(lines) < 3:
            continue
        time_line = next((line for line in lines if "-->" in line), "")
        if not time_line:
            continue
        title_line = next((line for line in lines if re.match(r"^Chương\s+\d+", line, flags=re.I)), "")
        if not title_line or title_line in seen:
            continue
        seen.add(title_line)
        start = time_line.split("-->")[0].strip()
        items.append(f"{_srt_time_to_menu_time(start)} - {title_line}")
    if not items:
        return None
    config.storage.subtitle_dir.mkdir(parents=True, exist_ok=True)
    menu_path = config.storage.subtitle_dir / f"{output_name}_menu.txt"
    menu_path.write_text("\n".join(items), encoding="utf-8")
    return menu_path


def translate_captions(config: NovelConfig) -> Path:
    caption_cfg = config.captions
    input_path = config.storage.captions_dir / caption_cfg.input_file
    output_path = config.storage.captions_dir / caption_cfg.output_file
    if not input_path.exists():
        raise FileNotFoundError(f"Caption source not found: {input_path}")
    provider = get_translation_provider(config.models.provider, config=config)
    content = input_path.read_text(encoding="utf-8")
    eol = "\r\n" if "\r\n" in content else "\n"
    lines = content.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    indices = collect_subtitle_text_line_indices(lines)
    source_lines = [lines[idx] for idx in indices]
    translated_lines: list[str] = []
    model = resolve_translation_model(config)
    LOGGER.info("Translating captions | novel=%s model=%s input=%s output=%s", config.novel_id, model, input_path, output_path)
    glossary_text = build_glossary_text(config.translation.glossary)
    debug_dir = config.storage.root / caption_cfg.prompt_debug_dir
    response_dir = config.storage.root / caption_cfg.response_dump_dir
    debug_dir.mkdir(parents=True, exist_ok=True)
    response_dir.mkdir(parents=True, exist_ok=True)

    for chunk_index in range(0, len(source_lines), caption_cfg.chunk_size):
        batch = source_lines[chunk_index : chunk_index + caption_cfg.chunk_size]
        prompt_parts = [
            "Bạn là chuyên gia dịch phụ đề Trung -> Việt.",
            "Dịch tự nhiên theo phong cách phụ đề, ngắn gọn nhưng mượt, ưu tiên câu văn nghe như lời thoại thật.",
            "Tự động thêm dấu câu phù hợp khi cần, đặc biệt là dấu phẩy, dấu chấm, dấu hỏi và dấu chấm than.",
            "Nếu tên riêng hoặc thuật ngữ đã xuất hiện trong glossary bên dưới, giữ nhất quán đúng theo glossary, không tự đổi cách gọi.",
            'Bắt buộc trả về DUY NHẤT JSON object dạng: {"translations":["...", "..."]}.',
            "translations phải có đúng số phần tử như đầu vào, đúng thứ tự.",
            "Dịch toàn bộ sang tiếng Việt, không để lại tiếng Trung, bao gồm cả tên riêng.",
            "Giữ nguyên định dạng subtitle trong dòng nếu có: <i>, </i>, {\\an8}, dấu câu, ký hiệu.",
        ]
        if glossary_text:
            prompt_parts.append("GLOSSARY:")
            prompt_parts.append(glossary_text)
        prompt_parts.append(json.dumps({"lines": batch}, ensure_ascii=False))
        prompt = "\n".join(prompt_parts)
        raw = provider.generate(model, prompt)
        (debug_dir / f"chunk_{chunk_index // caption_cfg.chunk_size + 1}.txt").write_text(prompt, encoding="utf-8")
        (response_dir / f"chunk_{chunk_index // caption_cfg.chunk_size + 1}.txt").write_text(raw, encoding="utf-8")
        match = re.search(r"\{.*\}", raw, flags=re.S)
        if not match:
            raise RuntimeError("Could not parse caption translation response as JSON")
        payload = json.loads(match.group(0))
        batch_translations = payload.get("translations", [])
        if len(batch_translations) != len(batch):
            raise RuntimeError("Caption translation line count mismatch")
        translated_lines.extend([str(item).strip() for item in batch_translations])

    for index, line_idx in enumerate(indices):
        translated = translated_lines[index]
        if HAN_REGEX.search(translated):
            LOGGER.warning("Residual Han characters in caption line %s", index + 1)
        lines[line_idx] = translated

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(eol.join(lines), encoding="utf-8")
    _write_chapter_menu_from_srt(config, lines, output_path.stem)
    if config.translation.auto_update_glossary and not is_queue_worker_env():
        update_glossary_from_chapter(
            config,
            "\n".join(source_lines).strip(),
            "\n".join(translated_lines).strip(),
        )
    return output_path
