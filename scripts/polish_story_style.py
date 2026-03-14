from __future__ import annotations

import argparse
from pathlib import Path

from novel_tts.config import load_novel_config
from novel_tts.translate.novel import build_glossary, load_source_chapters, rebuild_translated_file
from novel_tts.translate.providers import get_translation_provider


def _relevant_glossary(glossary: dict[str, str], source_text: str) -> dict[str, str]:
    return {
        key: value
        for key, value in sorted(glossary.items(), key=lambda item: len(item[0]), reverse=True)
        if key in source_text
    }


def _clean_response(text: str) -> str:
    text = text.strip()
    if text.startswith("```"):
        lines = text.splitlines()
        if lines:
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        text = "\n".join(lines).strip()
    return text.strip() + "\n"


def _polish_chapter(config, provider, chapter_num: str, source_text: str, translated_text: str) -> str:
    glossary = _relevant_glossary(config.translation.glossary, source_text)
    glossary_text = build_glossary(glossary) if glossary else "- không có mục bắt buộc"
    prompt = (
        f"{config.translation.base_rules}\n"
        f"Glossary dùng bắt buộc nếu xuất hiện:\n{glossary_text}\n\n"
        "Dưới đây là bản gốc tiếng Trung và bản dịch tiếng Việt hiện có của cùng một chương truyện.\n"
        "Hãy biên tập lại bản dịch tiếng Việt theo các yêu cầu bắt buộc:\n"
        "- Giữ nguyên nội dung, ý nghĩa, diễn biến, không thêm bớt chi tiết.\n"
        "- Giữ nguyên toàn bộ tên riêng, địa danh, tổ chức, thuật ngữ theo glossary.\n"
        "- Chỉnh câu văn cho tự nhiên, mượt, đúng chất văn truyện.\n"
        "- Format lại đoạn văn gọn, dễ đọc.\n"
        "- Sửa xưng hô theo đúng ngữ cảnh quan hệ nhân vật. Đặc biệt tránh sửa sai kiểu một nữ lớn tuổi nói với nữ nhỏ tuổi thành 'anh-em'.\n"
        "- Nếu ngữ cảnh là nữ lớn tuổi với nữ nhỏ tuổi thì ưu tiên 'chị-em' hoặc cách tương xứng, không dùng 'anh-em'.\n"
        "- Nếu ngữ cảnh là nam với nữ yêu đương thì dùng cách gọi phù hợp mạch truyện, không đổi bừa sang quan hệ khác giới tính.\n"
        "- Không để sót từ Hán, không để sót cụm vô nghĩa, không để các âm Hán Việt sai kiểu 'Gia Cốt', 'Gỗ', 'Gu'.\n"
        "- Chỉ trả về bản dịch tiếng Việt hoàn chỉnh của chương, không giải thích.\n\n"
        f"BẢN GỐC:\n{source_text}\n\n"
        f"BẢN DỊCH HIỆN TẠI:\n{translated_text}\n\n"
        f"Chương cần biên tập: {int(chapter_num)}"
    )
    return _clean_response(provider.generate(config.translation.model, prompt))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("novel_id")
    parser.add_argument("--file", action="append", default=[])
    args = parser.parse_args()

    config = load_novel_config(args.novel_id)
    provider = get_translation_provider(config.translation.provider)
    files = sorted(config.storage.origin_dir.glob("*.txt"))
    if args.file:
        wanted = set(args.file)
        files = [path for path in files if path.name in wanted]

    changed_parts = 0
    rebuilt_files = 0
    for source_path in files:
        chapter_map = dict(load_source_chapters(config, source_path))
        part_dir = config.storage.parts_dir / source_path.stem
        any_part = False
        for chapter_num, source_text in chapter_map.items():
            part_path = part_dir / f"{int(chapter_num):04d}.txt"
            if not part_path.exists():
                continue
            any_part = True
            translated_text = part_path.read_text(encoding="utf-8")
            polished = _polish_chapter(config, provider, chapter_num, source_text, translated_text)
            if polished != translated_text:
                part_path.write_text(polished, encoding="utf-8")
                changed_parts += 1
        if any_part:
            rebuilt = rebuild_translated_file(config, source_path, require_complete=False)
            if rebuilt is not None:
                rebuilt_files += 1

    print(f"changed_parts={changed_parts} rebuilt_files={rebuilt_files}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
