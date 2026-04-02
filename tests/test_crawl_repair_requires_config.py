from __future__ import annotations

from pathlib import Path

import pytest

from novel_tts.config.models import (
    BrowserDebugConfig,
    CaptionConfig,
    CrawlConfig,
    MediaConfig,
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
from novel_tts.crawl.service import repair_crawled_content


def _make_config(tmp_path: Path) -> NovelConfig:
    root = tmp_path
    input_dir = root / "input"
    output_dir = root / "output"
    storage = StorageConfig(
        root=root,
        input_dir=input_dir,
        output_dir=output_dir,
        image_dir=root / "image",
        logs_dir=root / ".logs",
        tmp_dir=root / "tmp",
    )
    crawl = CrawlConfig(site_id="test")
    browser_debug = BrowserDebugConfig()
    source = SourceConfig(source_id="test", resolver_id="test", crawl=crawl)
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
        models=models,
        translation=translation,
        captions=captions,
        queue=queue,
        tts=TtsConfig(provider="local", voice="test"),
        media=MediaConfig(
            visual=VisualConfig(background_video=""),
            video=VideoConfig(),
        ),
    )


def test_repair_requires_repair_config_when_generation_disabled(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    config.storage.origin_dir.mkdir(parents=True, exist_ok=True)
    # No input/repair_config.yaml created.
    with pytest.raises(FileNotFoundError):
        repair_crawled_content(config, 1, 1, generate_repair_config_if_missing=False)
