from __future__ import annotations

import json
from pathlib import Path

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
from novel_tts.crawl.service import verify_crawled_content


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


def test_verify_filters_failure_manifest_to_checked_range_when_no_range_provided(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    config.storage.origin_dir.mkdir(parents=True, exist_ok=True)
    body = ("你好" * 120).strip()
    (config.storage.origin_dir / "chuong_1-1.txt").write_text(f"第1章 标题\n\n{body}\n", encoding="utf-8")

    # Failure manifest has an entry outside checked chapters (only chapter 1 exists in origin).
    manifest_path = config.storage.progress_dir / "crawl_failures.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(
            {
                "novel_id": config.novel_id,
                "source": config.source_id,
                "updated_at": "2026-03-13T00:00:00+00:00",
                "failures": {
                    "2": {
                        "chapter_number": 2,
                        "batch_start": 2,
                        "batch_end": 2,
                        "url": "",
                        "reason": "missing_entry",
                        "details": "Directory parser did not return an entry for this chapter",
                        "updated_at": "2026-03-13T00:00:01+00:00",
                    }
                },
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    report = verify_crawled_content(config, fix_stale_manifest=False)
    assert 1 in report.checked_chapters
    assert report.ok is True
