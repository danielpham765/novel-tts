from __future__ import annotations

from pathlib import Path

from novel_tts.config import load_novel_config
from novel_tts.translate.novel import (
    chapter_part_path,
    find_fake_virtual_fishing_rod_lines,
    load_source_chapters,
    rebuild_translated_file,
    repair_fake_virtual_fishing_rod_artifacts,
)


def main() -> int:
    config = load_novel_config("thai-hu-chi-ton")
    modified_parts = 0
    modified_batches: set[Path] = set()

    for source_path in sorted(config.storage.origin_dir.glob("*.txt")):
        batch_changed = False
        for chapter_num, source_text in load_source_chapters(config, source_path):
            part_path = chapter_part_path(config, source_path, chapter_num)
            if not part_path.exists():
                continue
            original = part_path.read_text(encoding="utf-8")
            flagged = find_fake_virtual_fishing_rod_lines(original, source_text=source_text)
            if not flagged:
                continue
            fixed = repair_fake_virtual_fishing_rod_artifacts(original, source_text=source_text)
            remaining = find_fake_virtual_fishing_rod_lines(fixed, source_text=source_text)
            if remaining:
                raise RuntimeError(
                    f"Still has fake virtual fishing rod lines in {part_path}: {remaining}"
                )
            if fixed != original:
                part_path.write_text(fixed.rstrip() + "\n", encoding="utf-8")
                modified_parts += 1
                batch_changed = True
                print(f"fixed {part_path} lines={flagged}")
        if batch_changed:
            rebuild_translated_file(config, source_path, require_complete=True)
            modified_batches.add(source_path)
            print(f"rebuilt {config.storage.translated_dir / source_path.name}")

    print(f"modified_parts={modified_parts}")
    print(f"modified_batches={len(modified_batches)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
