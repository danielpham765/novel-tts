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


def test_crawl_repair_writes_report_and_inserts_placeholder(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    config.storage.origin_dir.mkdir(parents=True, exist_ok=True)

    # Provide a minimal repair config so the repair command knows which missing chapters are index gaps.
    config.storage.input_dir.mkdir(parents=True, exist_ok=True)
    (config.storage.input_dir / "repair_config.yaml").write_text(
        "version: 1\n"
        "index_gaps: [2]\n"
        "replacements: []\n",
        encoding="utf-8",
    )

    body = ("这是正常章节内容。" * 20).strip()
    raw = f"第1章 标题\n\n{body}\n\n\n第3章 标题\n\n{body}\n"
    batch_path = config.storage.origin_dir / "chuong_1-3.txt"
    batch_path.write_text(raw, encoding="utf-8")

    report_path = tmp_path / "report.txt"
    report = repair_crawled_content(config, 1, 3, log_path=report_path, generate_repair_config_if_missing=False)

    assert report.log_path == report_path
    assert report_path.exists()

    report_text = report_path.read_text(encoding="utf-8")
    assert "## Added placeholders" in report_text
    assert "chapter 2" in report_text
    assert any(action.action == "placeholder_added" and action.chapter == 2 for action in report.actions)

    updated = batch_path.read_text(encoding="utf-8")
    assert "第2章 略过" in updated
    assert updated.count("第2章 略过") == 1


def test_crawl_repair_run_without_range_infers_from_origin_batches(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    config.storage.origin_dir.mkdir(parents=True, exist_ok=True)
    config.storage.input_dir.mkdir(parents=True, exist_ok=True)
    (config.storage.input_dir / "repair_config.yaml").write_text(
        "version: 1\nindex_gaps: [2]\nreplacements: []\n",
        encoding="utf-8",
    )

    body = ("这是正常章节内容。" * 20).strip()
    raw = f"第1章 标题\n\n{body}\n\n\n第3章 标题\n\n{body}\n"
    batch_path = config.storage.origin_dir / "chuong_1-3.txt"
    batch_path.write_text(raw, encoding="utf-8")

    report_path = tmp_path / "report.txt"
    report = repair_crawled_content(config, None, None, log_path=report_path, generate_repair_config_if_missing=False)
    assert report.from_chapter == 1
    assert report.to_chapter == 3
    assert any(action.action == "placeholder_added" and action.chapter == 2 for action in report.actions)


def test_crawl_repair_rewrites_index_gap_placeholder_body(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    config.storage.origin_dir.mkdir(parents=True, exist_ok=True)
    config.storage.input_dir.mkdir(parents=True, exist_ok=True)
    (config.storage.input_dir / "repair_config.yaml").write_text(
        "version: 1\n"
        "index_gaps: [2]\n"
        "placeholder_title_suffix: 略过\n"
        "placeholder_content_zh: 本章内容与主线剧情无关。\n"
        "replacements: []\n",
        encoding="utf-8",
    )

    raw = "第1章 标题\n\n正文\n\n\n第2章 略过\n\n（占位）旧内容，与主线无关。\n\n\n第3章 标题\n\n正文\n"
    batch_path = config.storage.origin_dir / "chuong_1-3.txt"
    batch_path.write_text(raw, encoding="utf-8")

    report_path = tmp_path / "report.txt"
    report = repair_crawled_content(config, 1, 3, log_path=report_path, generate_repair_config_if_missing=False)
    updated = batch_path.read_text(encoding="utf-8")
    assert "本章内容与主线剧情无关。" in updated
    assert any(action.action == "replaced_from_source" and action.chapter == 2 for action in report.actions)


def test_crawl_repair_strips_watermark_lines(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    config.storage.origin_dir.mkdir(parents=True, exist_ok=True)
    config.storage.input_dir.mkdir(parents=True, exist_ok=True)
    (config.storage.input_dir / "repair_config.yaml").write_text(
        "version: 1\nindex_gaps: []\nreplacements: []\n",
        encoding="utf-8",
    )

    body = ("这是正常章节内容。" * 20).strip()
    raw = "\n".join(
        [
            "第1章 标题",
            "",
            body,
            "",
            "【記住本站域名 台灣小說網書庫多，t̲̲̅̅w̲̲̅̅k̲̲̅̅a̲̲̅̅n̲̲̅̅.c̲̲̅̅o̲̲̅̅m̲̲̅̅任你選 】",
        ]
    )
    batch_path = config.storage.origin_dir / "chuong_1-1.txt"
    batch_path.write_text(raw, encoding="utf-8")

    report_path = tmp_path / "report.txt"
    report = repair_crawled_content(config, 1, 1, log_path=report_path, generate_repair_config_if_missing=False)

    updated = batch_path.read_text(encoding="utf-8")
    assert "twkan" not in updated.lower()
    assert any(action.action == "watermark_removed" and action.chapter == 1 for action in report.actions)


def test_crawl_repair_strips_metadata_noise_lines(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    config.storage.origin_dir.mkdir(parents=True, exist_ok=True)
    config.storage.input_dir.mkdir(parents=True, exist_ok=True)
    (config.storage.input_dir / "repair_config.yaml").write_text(
        "version: 1\nindex_gaps: []\nreplacements: []\n",
        encoding="utf-8",
    )

    raw = "\n".join(
        [
            "第1章 标题",
            "",
            "2026-03-09 11:41:07",
            "作者： 七柒四十九",
            "宋楚薇起身離開辦公室後，顧若塵拿起手機看著照片上的喬喬微微一笑。",
        ]
    )
    batch_path = config.storage.origin_dir / "chuong_1-1.txt"
    batch_path.write_text(raw, encoding="utf-8")

    report_path = tmp_path / "report.txt"
    report = repair_crawled_content(config, 1, 1, log_path=report_path, generate_repair_config_if_missing=False)

    updated = batch_path.read_text(encoding="utf-8")
    assert "2026-03-09 11:41:07" not in updated
    assert "作者： 七柒四十九" not in updated
    assert "宋楚薇起身離開辦公室後" in updated
    assert any(action.action == "metadata_removed" and action.chapter == 1 for action in report.actions)
