from __future__ import annotations

from pathlib import Path

from novel_tts.crawl.repair_config import generate_repair_config_from_research


def test_generate_repair_config_from_research_files(tmp_path: Path) -> None:
    root = tmp_path
    novel_id = "thai-hu-chi-ton"
    logs_dir = root / ".logs"
    input_dir = root / "input" / novel_id

    crawl_logs = logs_dir / novel_id / "crawl"
    crawl_logs.mkdir(parents=True, exist_ok=True)

    # Continuity file: missing chapters 1205 and 2289.
    (crawl_logs / "missing_chapter_continuity.md").write_text(
        "# report\n\n- Missing reported: 1205\n- Missing reported: 2289\n",
        encoding="utf-8",
    )

    # Garbage research: chapter 2200 replacement link.
    (crawl_logs / "garbage_chapter_reseach.md").write_text(
        "### Chapter 2200\n\n```text\nhttps://cn.ttkan.co/novel/pagea/x_2209.html\n```\n",
        encoding="utf-8",
    )

    # Missing research: chapter 2289 replacement links (story gap).
    (crawl_logs / "missing_chapter_reseach.md").write_text(
        "```text\nChapter 2289 — x\n- 1qxs (mobile): https://m.1qxs.com/xs_1/86279/2292\n```\n",
        encoding="utf-8",
    )

    cfg = generate_repair_config_from_research(root=root, novel_id=novel_id, logs_dir=logs_dir, input_dir=input_dir)
    # 2289 is a story gap (has replacement rule) => not an index gap.
    assert 1205 in cfg.index_gaps
    assert 2289 not in cfg.index_gaps
    assert 2200 in cfg.replacements
    assert 2289 in cfg.replacements

