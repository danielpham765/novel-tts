from __future__ import annotations

import json
import importlib
from pathlib import Path

import pytest
import yaml

cli_main = importlib.import_module("novel_tts.cli.main")
from novel_tts.config import loader
from novel_tts.config.models import (
    BrowserDebugConfig,
    CaptionConfig,
    CrawlConfig,
    MediaConfig,
    MediaBatchConfig,
    MediaBatchRule,
    ModelsConfig,
    NovelConfig,
    QueueConfig,
    QueueModelConfig,
    SourceConfig,
    StorageConfig,
    TtsConfig,
    TranslationConfig,
    VideoConfig,
    VisualConfig,
)


def _write_minimal_loader_tree(tmp_path: Path, *, novel_cfg: dict, app_cfg: dict) -> None:
    (tmp_path / "configs" / "novels").mkdir(parents=True)
    (tmp_path / "configs" / "sources").mkdir(parents=True)
    (tmp_path / "configs" / "app.yaml").write_text(yaml.safe_dump(app_cfg, sort_keys=False), encoding="utf-8")
    (tmp_path / "configs" / "sources" / "s1.json").write_text(
        json.dumps({"crawl": {"site_id": "test", "browser_debug": {}}}),
        encoding="utf-8",
    )
    (tmp_path / "configs" / "novels" / "n1.yaml").write_text(
        yaml.safe_dump(novel_cfg, sort_keys=False),
        encoding="utf-8",
    )


def test_load_novel_config_reads_yaml_and_merges_media_batch(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(loader, "_root_dir", lambda: tmp_path)
    app_cfg = {
        "models": {
            "provider": "gemini_http",
            "enabled_models": ["m1"],
            "model_configs": {"m1": {"chunk_max_len": 100, "worker_count": 1, "rpm_limit": 1, "tpm_limit": 1}},
        },
        "queue": {"redis": {"host": "127.0.0.1", "port": 6379, "database": 1, "prefix": "novel_tts"}},
        "tts": {"provider": "gradio_vie_tts", "voice": "Ly"},
        "media": {
            "media_batch": {
                "default_chapter_batch_size": 10,
                "chapter_batch_overrides": [{"range": "1-100", "chapter_batch_size": 20}],
            },
        },
    }
    novel_cfg = {
        "novel_id": "n1",
        "title": "N1",
        "slug": "n1",
        "storage": {
            "input_dir": "input/n1",
            "output_dir": "output/n1",
            "image_dir": "image/n1",
            "logs_dir": ".logs",
            "tmp_dir": "tmp",
        },
        "crawl": {"sources": [{"source_id": "s1"}]},
        "translation": {"chapter": {"chapter_regex": "^$", "base_rules": "x"}},
        "models": {},
        "tts": {},
        "media": {
            "visual": {"background_video": "bg.mp4"},
            "video": {},
            "media_batch": {"chapter_batch_overrides": [{"range": "101-120", "chapter_batch_size": 5}]},
        },
    }
    _write_minimal_loader_tree(tmp_path, novel_cfg=novel_cfg, app_cfg=app_cfg)

    cfg = loader.load_novel_config("n1")

    assert cfg.media.media_batch.default_chapter_batch_size == 10
    assert [(item.range, item.chapter_batch_size) for item in cfg.media.media_batch.chapter_batch_overrides] == [
        ("1-100", 20),
        ("101-120", 5),
    ]


def test_load_novel_config_rejects_overlapping_media_batch_rules(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(loader, "_root_dir", lambda: tmp_path)
    app_cfg = {
        "models": {
            "provider": "gemini_http",
            "enabled_models": ["m1"],
            "model_configs": {"m1": {"chunk_max_len": 100, "worker_count": 1, "rpm_limit": 1, "tpm_limit": 1}},
        },
        "queue": {"redis": {"host": "127.0.0.1", "port": 6379, "database": 1, "prefix": "novel_tts"}},
        "tts": {"provider": "gradio_vie_tts", "voice": "Ly"},
    }
    novel_cfg = {
        "novel_id": "n1",
        "title": "N1",
        "slug": "n1",
        "storage": {
            "input_dir": "input/n1",
            "output_dir": "output/n1",
            "image_dir": "image/n1",
            "logs_dir": ".logs",
            "tmp_dir": "tmp",
        },
        "crawl": {"sources": [{"source_id": "s1"}]},
        "translation": {"chapter": {"chapter_regex": "^$", "base_rules": "x"}},
        "models": {},
        "tts": {},
        "media": {
            "visual": {"background_video": "bg.mp4"},
            "video": {},
            "media_batch": {
                "chapter_batch_overrides": [
                    {"range": "1-100", "chapter_batch_size": 20},
                    {"range": "50-120", "chapter_batch_size": 10},
                ]
            },
        },
    }
    _write_minimal_loader_tree(tmp_path, novel_cfg=novel_cfg, app_cfg=app_cfg)

    with pytest.raises(ValueError, match="overlapping ranges"):
        loader.load_novel_config("n1")


def _make_config(tmp_path: Path) -> NovelConfig:
    storage = StorageConfig(
        root=tmp_path,
        input_dir=tmp_path / "input",
        output_dir=tmp_path / "output",
        image_dir=tmp_path / "image",
        logs_dir=tmp_path / ".logs",
        tmp_dir=tmp_path / "tmp",
    )
    crawl = CrawlConfig(site_id="test")
    source = SourceConfig(
        source_id="test",
        resolver_id="test",
        crawl=crawl,
    )
    return NovelConfig(
        novel_id="novel",
        title="Novel",
        slug="novel",
        source_language="zh",
        target_language="vi",
        source_id="test",
        source=source,
        storage=storage,
        crawl=crawl,
        models=ModelsConfig(
            provider="gemini_http",
            enabled_models=["m1"],
            model_configs={"m1": QueueModelConfig(chunk_max_len=1000)},
        ),
        translation=TranslationConfig(chapter_regex=r"^$", base_rules="", glossary_file=""),
        captions=CaptionConfig(),
        queue=QueueConfig(),
        tts=TtsConfig(provider="local", voice="test"),
        media=MediaConfig(
            visual=VisualConfig(background_video="bg.mp4"),
            video=VideoConfig(),
            media_batch=MediaBatchConfig(
                default_chapter_batch_size=10,
                chapter_batch_overrides=[
                    MediaBatchRule(range="1-20", chapter_batch_size=5),
                    MediaBatchRule(range="21-30", chapter_batch_size=2),
                ],
            ),
        ),
    )


def test_cli_get_translated_ranges_uses_media_batch_rules(tmp_path: Path) -> None:
    config = _make_config(tmp_path)

    assert cli_main.get_translated_ranges(config, 4, 23) == [
        (1, 5, "chuong_1-5"),
        (6, 10, "chuong_6-10"),
        (11, 15, "chuong_11-15"),
        (16, 20, "chuong_16-20"),
        (21, 22, "chuong_21-22"),
        (23, 24, "chuong_23-24"),
    ]
