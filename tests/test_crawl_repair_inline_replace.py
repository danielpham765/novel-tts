from __future__ import annotations

from pathlib import Path

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


def test_crawl_repair_replaces_chapter_via_inline_candidate(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    config.storage.origin_dir.mkdir(parents=True, exist_ok=True)
    config.storage.input_dir.mkdir(parents=True, exist_ok=True)

    # Repair config: replace chapter 3 with inline content.
    (config.storage.input_dir / "repair_config.yaml").write_text(
        "version: 1\n"
        "index_gaps: []\n"
        "replacements:\n"
        "  - chapter: 3\n"
        "    candidates:\n"
        "      - kind: inline\n"
        "        title: 第3章 新内容\n"
        "        content: 这是一段替换后的章节内容。\n",
        encoding="utf-8",
    )

    body = ("这是正常章节内容。" * 20).strip()
    raw = f"第2章 标题\n\n{body}\n\n\n第3章 旧内容\n\n{body}\n"
    batch_path = config.storage.origin_dir / "chuong_2-3.txt"
    batch_path.write_text(raw, encoding="utf-8")

    report_path = tmp_path / "report.txt"
    report = repair_crawled_content(config, 2, 3, log_path=report_path, generate_repair_config_if_missing=False)
    assert report_path.exists()
    assert any(action.action in {"replaced_from_source", "fallback_replaced"} and action.chapter == 3 for action in report.actions)

    updated = batch_path.read_text(encoding="utf-8")
    assert "第3章 新内容" in updated
    assert "这是一段替换后的章节内容。" in updated
