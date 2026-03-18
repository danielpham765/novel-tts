from __future__ import annotations

from pathlib import Path

from novel_tts.config.loader import load_novel_config
from novel_tts.config.models import StorageConfig
from novel_tts.translate import novel as novel_mod


def _tmp_storage(tmp_path: Path) -> StorageConfig:
    root = tmp_path
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    image_dir = tmp_path / "image"
    logs_dir = tmp_path / ".logs"
    tmp_dir = tmp_path / "tmp"
    input_dir.mkdir(parents=True, exist_ok=True)
    (input_dir / "origin").mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    image_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)
    tmp_dir.mkdir(parents=True, exist_ok=True)
    return StorageConfig(
        root=root,
        input_dir=input_dir,
        output_dir=output_dir,
        image_dir=image_dir,
        logs_dir=logs_dir,
        tmp_dir=tmp_dir,
    )


def test_translate_chapter_force_clears_progress(monkeypatch, tmp_path: Path) -> None:
    cfg = load_novel_config("vo-cuc-thien-ton")
    cfg.storage = _tmp_storage(tmp_path)
    cfg.translation.auto_update_glossary = False

    source_path = cfg.storage.origin_dir / "chuong_1-10.txt"
    source_path.write_text("第1章\n陳天極\n", encoding="utf-8")

    cleared: list[str] = []

    monkeypatch.setattr(novel_mod, "translate_unit", lambda *_args, **_kwargs: "ok\n")
    monkeypatch.setattr(novel_mod, "clear_progress", lambda _cfg, key: cleared.append(key))

    out = novel_mod.translate_chapter(cfg, source_path, "1", force=True)

    assert set(cleared) == {"chuong_1-10.txt__1", "placeholders__chuong_1-10.txt__1"}
    assert out.exists()
    assert out.read_text(encoding="utf-8") == "ok\n"
