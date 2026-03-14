from __future__ import annotations

import argparse
import re
from pathlib import Path


CHAPTER_PATTERN = re.compile(r"^第(\d+)章[^\n]*", flags=re.M)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("origin_dir")
    return parser.parse_args()


def _extract_chapters(text: str) -> dict[int, str]:
    matches = list(CHAPTER_PATTERN.finditer(text))
    chapters: dict[int, str] = {}
    for index, match in enumerate(matches):
        number = int(match.group(1))
        start = match.start()
        end = matches[index + 1].start() if index + 1 < len(matches) else len(text)
        chapter_text = text[start:end].strip()
        if chapter_text:
            chapters[number] = chapter_text
    return chapters


def _normalize_block(chapters: list[str]) -> str:
    return "\n\n".join(chapter.strip() for chapter in chapters if chapter.strip()).strip() + "\n"


def main() -> int:
    args = _parse_args()
    origin_dir = Path(args.origin_dir)
    all_chapters: dict[int, str] = {}
    for path in sorted(origin_dir.glob("chuong_*.txt")):
        text = path.read_text(encoding="utf-8", errors="ignore")
        for chapter_number, chapter_text in _extract_chapters(text).items():
            all_chapters.setdefault(chapter_number, chapter_text)

    if not all_chapters:
        raise SystemExit("No chapters found.")

    expected = list(range(min(all_chapters), max(all_chapters) + 1))
    missing = [number for number in expected if number not in all_chapters]
    if missing:
        raise SystemExit(f"Missing chapters remain: {missing}")

    for path in sorted(origin_dir.glob("chuong_*.txt")):
        path.unlink()

    max_number = max(all_chapters)
    for start in range(1, max_number + 1, 10):
        end = min(start + 9, max_number)
        chapters = [all_chapters[number] for number in range(start, end + 1)]
        output = origin_dir / f"chuong_{start}-{end}.txt"
        output.write_text(_normalize_block(chapters), encoding="utf-8")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
