from __future__ import annotations

from pathlib import Path

from novel_tts.crawl.service import _write_merged_batch


def test_write_merged_batch_merges_overlapping_origin_files(tmp_path: Path) -> None:
    origin_dir = tmp_path / "origin"
    origin_dir.mkdir(parents=True, exist_ok=True)

    old_path = origin_dir / "chuong_1221-1222.txt"
    old_path.write_text(
        "\n\n\n".join(
            [
                "第1221章 标题\n\n甲章内容",
                "第1222章 标题\n\n乙章内容",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    merged_path = _write_merged_batch(
        origin_dir=origin_dir,
        start_chapter=1221,
        end_chapter=1230,
        blocks=[
            "第1223章 标题\n\n丙章内容",
            "第1224章 标题\n\n丁章内容",
            "第1230章 标题\n\n戊章内容",
        ],
        chapter_numbers=[1223, 1224, 1230],
        chapter_regex=r"^第(\d+)章([^\n]*)",
    )

    assert merged_path == origin_dir / "chuong_1221-1230.txt"
    assert merged_path.exists()
    assert not old_path.exists()

    merged_raw = merged_path.read_text(encoding="utf-8")
    assert "第1221章 标题" in merged_raw
    assert "第1222章 标题" in merged_raw
    assert "第1223章 标题" in merged_raw
    assert "第1224章 标题" in merged_raw
    assert "第1230章 标题" in merged_raw
