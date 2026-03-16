from __future__ import annotations

import argparse
import shutil
import re
from pathlib import Path


CHAPTER_PATTERN = re.compile(r"^第(\d+)章[^\n]*", flags=re.M)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("origin_dir")
    parser.add_argument("--batch-size", type=int, default=10)
    parser.add_argument("--start-at", type=int, default=1)
    parser.add_argument("--require-complete", action="store_true")
    parser.add_argument("--backup-dir", default="")
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
    batch_size = max(1, int(args.batch_size))
    start_at = max(1, int(args.start_at))
    all_chapters: dict[int, str] = {}
    for path in sorted(origin_dir.glob("chuong_*.txt")):
        text = path.read_text(encoding="utf-8", errors="ignore")
        for chapter_number, chapter_text in _extract_chapters(text).items():
            all_chapters.setdefault(chapter_number, chapter_text)

    if not all_chapters:
        raise SystemExit("No chapters found.")

    if args.require_complete:
        expected = list(range(min(all_chapters), max(all_chapters) + 1))
        missing = [number for number in expected if number not in all_chapters]
        if missing:
            raise SystemExit(f"Missing chapters remain: {missing}")

    backup_dir = Path(args.backup_dir).expanduser() if args.backup_dir else (origin_dir / "_backup_original")
    backup_dir.mkdir(parents=True, exist_ok=True)
    moved_any = False
    for path in sorted(origin_dir.glob("chuong_*.txt")):
        target = backup_dir / path.name
        if target.exists():
            target = backup_dir / f"{path.stem}__dup{path.suffix}"
        shutil.move(str(path), str(target))
        moved_any = True

    max_number = max(all_chapters)
    for start in range(start_at, max_number + 1, batch_size):
        end = min(start + batch_size - 1, max_number)
        chapters = [all_chapters[number] for number in range(start, end + 1) if number in all_chapters]
        if not chapters:
            continue
        output = origin_dir / f"chuong_{start}-{end}.txt"
        output.write_text(_normalize_block(chapters), encoding="utf-8")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
