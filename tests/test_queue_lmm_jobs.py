from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from novel_tts.config.models import (
    BrowserDebugConfig,
    CaptionConfig,
    CrawlConfig,
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
from novel_tts.queue import translation_queue
from novel_tts.translate import novel as novel_translate


def _make_config(tmp_path: Path) -> NovelConfig:
    root = tmp_path
    input_dir = root / "input"
    output_dir = root / "output"
    storage = StorageConfig(
        root=root,
        input_dir=input_dir,
        output_dir=output_dir,
        image_dir=root / "image",
        logs_dir=root / "logs",
        tmp_dir=root / "tmp",
    )
    crawl = CrawlConfig(site_id="test")
    browser_debug = BrowserDebugConfig()
    source = SourceConfig(source_id="test", resolver_id="test", crawl=crawl, browser_debug=browser_debug)
    models = ModelsConfig(
        provider="gemini_http",
        enabled_models=["dummy"],
        model_configs={"dummy": QueueModelConfig(chunk_max_len=4000, chunk_sleep_seconds=0.0)},
    )
    translation = TranslationConfig(
        chapter_regex=r"^第(\d+)章([^\n]*)",
        base_rules="",
        auto_update_glossary=True,
        glossary_file="",
    )
    captions = CaptionConfig()
    queue = QueueConfig()
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
        browser_debug=browser_debug,
        models=models,
        translation=translation,
        captions=captions,
        queue=queue,
        tts=TtsConfig(provider="local", voice="test"),
        visual=VisualConfig(background_video=""),
        video=VideoConfig(),
    )


def _write_origin_ch1(config: NovelConfig) -> Path:
    config.storage.origin_dir.mkdir(parents=True, exist_ok=True)
    origin_path = config.storage.origin_dir / "book1.txt"
    origin_path.write_text("第1章 标题\n你好\n", encoding="utf-8")
    return origin_path


def _write_part_ch1(config: NovelConfig, origin_path: Path) -> Path:
    part_path = novel_translate.chapter_part_path(config, origin_path, "1")
    part_path.parent.mkdir(parents=True, exist_ok=True)
    part_path.write_text("Chương 1\n\nXin chào\n", encoding="utf-8")
    # Make the part look up-to-date vs origin.
    os.utime(part_path, (origin_path.stat().st_mtime + 10, origin_path.stat().st_mtime + 10))
    return part_path


def test_job_id_parsing_legacy_and_captions() -> None:
    assert translation_queue._parse_job_id("file.txt::0005") == ("file.txt", "5")
    assert translation_queue._is_captions_job("captions") is True
    assert translation_queue._is_captions_job("CAPTIONS") is True
    assert translation_queue._is_captions_job("file.txt::0005") is False


def test_chapter_needs_work_glossary_pending(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    origin_path = _write_origin_ch1(config)
    part_path = _write_part_ch1(config, origin_path)
    assert translation_queue._chapter_needs_work(config, origin_path, "1") is False

    marker_path = novel_translate.glossary_marker_path(config, origin_path, "1")
    marker_path.write_text(json.dumps({"status": "pending"}, ensure_ascii=False), encoding="utf-8")
    os.utime(marker_path, (part_path.stat().st_mtime + 1, part_path.stat().st_mtime + 1))
    assert translation_queue._chapter_needs_work(config, origin_path, "1") is True


def test_translate_chapter_skips_translation_but_runs_glossary_when_pending(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _make_config(tmp_path)
    origin_path = _write_origin_ch1(config)
    part_path = _write_part_ch1(config, origin_path)

    marker_path = novel_translate.glossary_marker_path(config, origin_path, "1")
    marker_path.write_text(json.dumps({"status": "pending"}, ensure_ascii=False), encoding="utf-8")

    def _fail_translate_unit(*_args, **_kwargs):
        raise AssertionError("translate_unit() should not run when part is up-to-date")

    monkeypatch.setattr(novel_translate, "translate_unit", _fail_translate_unit)

    class _DummyProvider:
        def generate(self, model: str, prompt: str, system_prompt: str = "") -> str:
            # Return empty glossary updates (valid JSON object).
            return "{}"

    monkeypatch.setattr(novel_translate, "get_translation_provider", lambda _name, *, config=None: _DummyProvider())

    out_path = novel_translate.translate_chapter(config, origin_path, "1", force=False)
    assert out_path == part_path

    payload = json.loads(marker_path.read_text(encoding="utf-8"))
    assert payload.get("status") == "done"
