from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from novel_tts.common import logrotate


def _write_sparse_file(path: Path, size_bytes: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as f:
        if size_bytes <= 0:
            return
        f.seek(size_bytes - 1)
        f.write(b"\0")


def test_rotate_large_logs_moves_to_today_and_truncates(tmp_path: Path) -> None:
    now = datetime(2026, 3, 17, 12, 0, 0, tzinfo=timezone.utc)
    logs_root = tmp_path / ".logs"
    logs_root.mkdir(parents=True, exist_ok=True)
    src = logs_root / "quota-supervisor.log"
    _write_sparse_file(src, 10 * 1024 * 1024 + 1)
    os.utime(src, (now.timestamp(), now.timestamp()))

    rotated = logrotate.rotate_large_logs_to_today(
        logs_root=logs_root,
        size_threshold_bytes=10 * 1024 * 1024,
        now=now,
    )
    assert rotated == 1
    assert (logs_root / "archived" / "today" / "quota-supervisor_000.log").exists()
    assert src.exists()
    assert src.stat().st_size == 0


def test_rotate_old_logs_moves_to_date_folder_and_truncates(tmp_path: Path) -> None:
    now = datetime(2026, 3, 17, 12, 0, 0, tzinfo=timezone.utc)
    yesterday = now - timedelta(days=1)
    logs_root = tmp_path / ".logs"
    logs_root.mkdir(parents=True, exist_ok=True)
    src = logs_root / "a.log"
    src.write_text("hello\n", encoding="utf-8")
    os.utime(src, (yesterday.timestamp(), yesterday.timestamp()))

    rotated = logrotate.rotate_old_logs_to_date_folders(logs_root=logs_root, now=now)
    assert rotated == 1
    assert (logs_root / "archived" / "2026-03-16" / "a_000.log").exists()
    assert src.stat().st_size == 0


def test_rotate_novel_logs_preserves_relative_path(tmp_path: Path) -> None:
    now = datetime(2026, 3, 17, 12, 0, 0, tzinfo=timezone.utc)
    logs_root = tmp_path / ".logs"
    logs_root.mkdir(parents=True, exist_ok=True)

    src = logs_root / "vo-cuc-thien-ton" / "queue" / "workers" / "k1-model-w1.log"
    src.parent.mkdir(parents=True, exist_ok=True)
    src.write_text("line\n", encoding="utf-8")
    os.utime(src, (now.timestamp(), now.timestamp()))

    rotated = logrotate.rotate_novel_logs_to_today(logs_root=logs_root, novel_id="vo-cuc-thien-ton", now=now)
    assert rotated == 1
    assert (
        logs_root
        / "archived"
        / "today"
        / "vo-cuc-thien-ton"
        / "queue"
        / "workers"
        / "k1-model-w1_000.log"
    ).exists()
    assert src.exists()
    assert src.stat().st_size == 0


def test_rotate_single_log_file_to_today_preserves_relative_path(tmp_path: Path) -> None:
    now = datetime(2026, 3, 17, 12, 0, 0, tzinfo=timezone.utc)
    logs_root = tmp_path / ".logs"
    logs_root.mkdir(parents=True, exist_ok=True)

    src = logs_root / "thai-hu-chi-ton" / "crawl" / "verify.log"
    src.parent.mkdir(parents=True, exist_ok=True)
    src.write_text("line\n", encoding="utf-8")
    os.utime(src, (now.timestamp(), now.timestamp()))

    moved = logrotate.rotate_log_file_to_today(logs_root=logs_root, src=src)
    assert moved is not None
    assert (logs_root / "archived" / "today" / "thai-hu-chi-ton" / "crawl" / "verify_000.log").exists()
    assert src.exists()
    assert src.stat().st_size == 0


def test_housekeeping_zips_old_folders_and_prunes_zip(tmp_path: Path) -> None:
    now = datetime(2026, 3, 17, 1, 2, 3, tzinfo=timezone.utc)
    logs_root = tmp_path / ".logs"
    logs_root.mkdir(parents=True, exist_ok=True)
    archived_root = logrotate.ensure_archived_layout(logs_root)

    # Create required keep-folders.
    (archived_root / "2026-03-16").mkdir(parents=True, exist_ok=True)
    (archived_root / "2026-03-15").mkdir(parents=True, exist_ok=True)

    # Old folder with non-empty log -> should become zip.
    old_nonempty = archived_root / "2026-03-14"
    (old_nonempty / "x.log").parent.mkdir(parents=True, exist_ok=True)
    (old_nonempty / "x.log").write_text("data\n", encoding="utf-8")

    # Old folder with empty log -> skip zip but remove folder.
    old_empty = archived_root / "2026-03-13"
    (old_empty / "y.log").parent.mkdir(parents=True, exist_ok=True)
    (old_empty / "y.log").write_text("", encoding="utf-8")

    logrotate.housekeeping_archived(logs_root=logs_root, now=now)

    dirs = {p.name for p in archived_root.iterdir() if p.is_dir()}
    assert dirs == {"today", "zip", "2026-03-16", "2026-03-15"}
    assert not old_nonempty.exists()
    assert not old_empty.exists()
    assert (archived_root / "zip" / "2026-03-14.zip").exists()
    assert not (archived_root / "zip" / "2026-03-13.zip").exists()

    # Create extra zip files and ensure prune keeps only 4 newest.
    zip_root = archived_root / "zip"
    for d in ["2026-03-10", "2026-03-11", "2026-03-12", "2026-03-08", "2026-03-09"]:
        (zip_root / f"{d}.zip").write_bytes(b"z")
    logrotate.prune_zip_folder(zip_root=zip_root, max_files=4)
    kept = sorted([p.name for p in zip_root.iterdir() if p.is_file() and p.name.endswith(".zip")])
    assert len(kept) <= 4
