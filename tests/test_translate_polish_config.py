from __future__ import annotations

import json

import pytest
import yaml

from novel_tts.config import loader
from novel_tts.translate.polish import normalize_text


def _write_minimal_config_tree(tmp_path, *, novel_id: str = "n1") -> None:
    (tmp_path / "configs" / "novels").mkdir(parents=True)
    (tmp_path / "configs" / "sources").mkdir(parents=True)
    (tmp_path / "configs" / "polish_replacement").mkdir(parents=True)

    app_cfg = {
        "models": {
            "provider": "gemini_http",
            "enabled_models": ["m1"],
            "model_configs": {"m1": {"chunk_max_len": 100, "worker_count": 1, "rpm_limit": 1, "tpm_limit": 1}},
        },
        "queue": {"redis": {"host": "127.0.0.1", "port": 6379, "database": 1, "prefix": "novel_tts"}},
        "tts": {"provider": "gradio_vie_tts", "voice": "Ly"},
    }
    (tmp_path / "configs" / "app.yaml").write_text(yaml.safe_dump(app_cfg, sort_keys=False), encoding="utf-8")

    source_cfg = {"crawl": {"site_id": "test"}, "browser_debug": {}}
    (tmp_path / "configs" / "sources" / "s1.json").write_text(json.dumps(source_cfg), encoding="utf-8")

    novel_cfg = {
        "novel_id": novel_id,
        "title": "N1",
        "slug": novel_id,
        "storage": {
            "input_dir": f"input/{novel_id}",
            "output_dir": f"output/{novel_id}",
            "image_dir": f"image/{novel_id}",
            "logs_dir": ".logs",
            "tmp_dir": "tmp",
        },
        "crawl": {"sources": [{"source_id": "s1"}]},
        "translation": {"chapter": {"chapter_regex": "^$", "base_rules": "x"}},
        "models": {},
        "tts": {},
        "visual": {},
        "video": {},
    }
    (tmp_path / "configs" / "novels" / f"{novel_id}.json").write_text(json.dumps(novel_cfg), encoding="utf-8")


def test_normalize_text_uses_injected_replacements() -> None:
    raw = "Haizz, Họ Hứa tới rồi.\n"
    out = normalize_text(raw, chapter_num="1", replacements={"Haizz": "Hầy", "Họ Hứa": "họ Hứa"})

    assert "Hầy" in out
    assert "họ Hứa" in out
    assert "Haizz" not in out
    assert "Họ Hứa" not in out


def test_load_novel_config_merges_polish_replacements(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(loader, "_root_dir", lambda: tmp_path)
    _write_minimal_config_tree(tmp_path, novel_id="n1")
    (tmp_path / "configs" / "polish_replacement" / "common.json").write_text(
        json.dumps({"Haizz": "Hầy", "Ouyang": "Âu Dương"}, ensure_ascii=False),
        encoding="utf-8",
    )
    (tmp_path / "configs" / "polish_replacement" / "n1.json").write_text(
        json.dumps({"Ouyang": "Âu Dương chuẩn", "Họ Hứa": "họ Hứa"}, ensure_ascii=False),
        encoding="utf-8",
    )

    cfg = loader.load_novel_config("n1")

    assert cfg.translation.polish_replacements == {
        "Haizz": "Hầy",
        "Ouyang": "Âu Dương chuẩn",
        "Họ Hứa": "họ Hứa",
    }


def test_load_novel_config_missing_novel_polish_file_defaults_empty(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(loader, "_root_dir", lambda: tmp_path)
    _write_minimal_config_tree(tmp_path, novel_id="n1")
    (tmp_path / "configs" / "polish_replacement" / "common.json").write_text(
        json.dumps({"Haizz": "Hầy"}, ensure_ascii=False),
        encoding="utf-8",
    )

    cfg = loader.load_novel_config("n1")

    assert cfg.translation.polish_replacements == {"Haizz": "Hầy"}


def test_load_novel_config_rejects_invalid_polish_replacement_shape(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(loader, "_root_dir", lambda: tmp_path)
    _write_minimal_config_tree(tmp_path, novel_id="n1")
    (tmp_path / "configs" / "polish_replacement" / "common.json").write_text(
        json.dumps(["bad-shape"], ensure_ascii=False),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Invalid polish replacement config format"):
        loader.load_novel_config("n1")
