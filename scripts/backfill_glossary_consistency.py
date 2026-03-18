from __future__ import annotations

import argparse
import json
import os
import re
from pathlib import Path

from novel_tts.config import load_novel_config
from novel_tts.translate.novel import (
    _extract_glossary_updates,
    _merge_glossary_file,
    build_glossary,
    glossary_path,
    load_source_chapters,
    refresh_glossary,
    rebuild_translated_file,
)
from novel_tts.translate.providers import get_translation_provider


VN_CHAPTER_HEADER = re.compile(r"^(Chương|Đoạn)\s+(\d+)\s*[:：]?\s*(.*)$", re.M)


def _effective_translate_model(config) -> str:
    model = (
        os.environ.get("NOVEL_TTS_TRANSLATION_MODEL")
        or os.environ.get("NOVEL_TTS_TRANSLATE_MODEL")
        or os.environ.get("GEMINI_MODEL")
        or ""
    ).strip()
    if model:
        return model
    if getattr(config, "models", None) and config.models.enabled_models:
        return str(config.models.enabled_models[0]).strip()
    raise KeyError("Missing models.enabled_models[0]")


def _load_part_paths(config, filenames: list[str] | None) -> list[tuple[Path, list[tuple[str, Path]]]]:
    source_files = sorted(config.storage.origin_dir.glob("*.txt"))
    if filenames:
        wanted = set(filenames)
        source_files = [path for path in source_files if path.name in wanted]
    payload: list[tuple[Path, list[tuple[str, Path]]]] = []
    for source_path in source_files:
        chapter_paths: list[tuple[str, Path]] = []
        part_dir = config.storage.parts_dir / source_path.stem
        for chapter_num, _chapter_text in load_source_chapters(config, source_path):
            part_path = part_dir / f"{int(chapter_num):04d}.txt"
            if part_path.exists():
                chapter_paths.append((chapter_num, part_path))
        if chapter_paths:
            payload.append((source_path, chapter_paths))
    return payload


def _relevant_glossary(glossary: dict[str, str], source_text: str) -> dict[str, str]:
    return {
        key: value
        for key, value in sorted(glossary.items(), key=lambda item: len(item[0]), reverse=True)
        if key in source_text
    }


def _clean_polish_response(text: str) -> str:
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```[a-zA-Z0-9_-]*\n?", "", text)
        text = re.sub(r"\n?```$", "", text)
    return text.strip() + "\n"


def _polish_translation(config, provider, model: str, source_text: str, translated_text: str, glossary: dict[str, str]) -> str:
    if not glossary:
        return translated_text
    glossary_text = build_glossary(glossary)
    prompt = (
        f"{config.translation.base_rules}\n"
        f"Glossary dùng bắt buộc nếu xuất hiện trong bản gốc:\n{glossary_text}\n\n"
        "Dưới đây là bản gốc tiếng Trung và bản dịch tiếng Việt hiện có của cùng một chương.\n"
        "Nhiệm vụ của ngươi:\n"
        "- Chỉ chuẩn hóa tên riêng, địa danh, tổ chức, chức danh, thuật ngữ theo glossary.\n"
        "- Nếu bản dịch hiện có đã đúng glossary thì giữ nguyên.\n"
        "- Không rút gọn, không thêm ý, không đổi văn phong quá mức.\n"
        "- Giữ cấu trúc xuống dòng tự nhiên như bản dịch hiện có.\n"
        "- Chỉ trả về bản dịch tiếng Việt đã chuẩn hóa cuối cùng.\n\n"
        f"BẢN GỐC:\n{source_text}\n\n"
        f"BẢN DỊCH HIỆN CÓ:\n{translated_text}"
    )
    return _clean_polish_response(provider.generate(model, prompt))


def _normalize_chapter_header(text: str, chapter_num: str) -> str:
    lines = text.splitlines()
    if not lines:
        return text
    first = lines[0].strip()
    match = VN_CHAPTER_HEADER.match(first)
    if match:
        title = match.group(3).strip()
        lines[0] = f"Chương {int(chapter_num)}: {title}".rstrip()
        return "\n".join(lines).strip() + "\n"
    return text.strip() + "\n"


def _save_glossary_snapshot(config, target: Path) -> None:
    path = glossary_path(config)
    if path is None or not path.exists():
        return
    target.write_text(path.read_text(encoding="utf-8"), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("novel_id")
    parser.add_argument("--file", action="append", default=[])
    parser.add_argument("--skip-polish", action="store_true")
    parser.add_argument("--snapshot")
    args = parser.parse_args()

    config = load_novel_config(args.novel_id)
    provider = get_translation_provider(config.models.provider)
    model = _effective_translate_model(config)
    payload = _load_part_paths(config, args.file or None)
    total_added = 0
    total_polished = 0

    for source_path, chapter_paths in payload:
        chapter_map = dict(load_source_chapters(config, source_path))
        for chapter_num, part_path in chapter_paths:
            refresh_glossary(config)
            source_text = chapter_map[chapter_num]
            translated_text = part_path.read_text(encoding="utf-8")
            relevant = _relevant_glossary(config.translation.glossary, source_text)
            if relevant and not args.skip_polish:
                polished = _polish_translation(config, provider, model, source_text, translated_text, relevant)
                polished = _normalize_chapter_header(polished, chapter_num)
                if polished != translated_text:
                    part_path.write_text(polished, encoding="utf-8")
                    translated_text = polished
                    total_polished += 1
            updates = _extract_glossary_updates(config, provider, source_text, translated_text)
            _merged, added = _merge_glossary_file(config, updates)
            total_added += added
        rebuild_translated_file(config, source_path, require_complete=False)

    if args.snapshot:
        _save_glossary_snapshot(config, Path(args.snapshot))

    summary = {
        "files_processed": len(payload),
        "chapters_polished": total_polished,
        "glossary_entries_added": total_added,
        "glossary_size": len(config.translation.glossary),
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
