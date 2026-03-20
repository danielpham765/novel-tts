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


def test_crawl_verify_flags_duplicated_blocks(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    config.storage.origin_dir.mkdir(parents=True, exist_ok=True)

    para = ("这是一个段落内容，用于测试章节内重复检测。" * 12).strip()
    raw = f"第1章 标题\n\n{para}\n\n{para}\n"
    (config.storage.origin_dir / "chuong_1-1.txt").write_text(raw, encoding="utf-8")

    report = verify_crawled_content(config, from_chapter=1, to_chapter=1, fix_stale_manifest=False)
    assert report.ok is False
    assert any(issue.code == "duplicated_content" and issue.chapter_number == 1 for issue in report.issues)


def test_crawl_verify_ok_for_non_duplicated_content(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    config.storage.origin_dir.mkdir(parents=True, exist_ok=True)

    body = ("这是正常章节内容。" * 20).strip()
    raw = f"第1章 标题\n\n{body}\n"
    (config.storage.origin_dir / "chuong_1-1.txt").write_text(raw, encoding="utf-8")

    report = verify_crawled_content(config, from_chapter=1, to_chapter=1, fix_stale_manifest=False)
    assert report.ok is True


def test_crawl_verify_flags_watermark_lines(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    config.storage.origin_dir.mkdir(parents=True, exist_ok=True)

    body = ("正常内容。" * 20).strip()
    raw = "\n".join(
        [
            "第1章 标题",
            "",
            body,
            "",
            "【記住本站域名 台灣小說網書庫多，t̲̲̅̅w̲̲̅̅k̲̲̅̅a̲̲̅̅n̲̲̅̅.c̲̲̅̅o̲̲̅̅m̲̲̅̅任你選 】",
        ]
    )
    (config.storage.origin_dir / "chuong_1-1.txt").write_text(raw, encoding="utf-8")

    report = verify_crawled_content(config, from_chapter=1, to_chapter=1, fix_stale_manifest=False)
    assert report.ok is False
    assert any(issue.code == "watermark_content" and issue.chapter_number == 1 for issue in report.issues)


def test_crawl_verify_ignores_partial_batch_edges(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    config.storage.origin_dir.mkdir(parents=True, exist_ok=True)

    body = ("这是正常章节内容。" * 20).strip()
    raw = "\n\n".join([f"第{chapter}章 标题\n\n{body}\n" for chapter in range(1223, 1231)])
    (config.storage.origin_dir / "chuong_1221-1230.txt").write_text(raw, encoding="utf-8")

    report = verify_crawled_content(config, fix_stale_manifest=False)
    assert report.ok is True


def test_crawl_verify_flags_metadata_noise_lines(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    config.storage.origin_dir.mkdir(parents=True, exist_ok=True)

    body = "\n".join(
        [
            "2026-03-09 11:41:07",
            "作者： 七柒四十九",
            "宋楚薇起身離開辦公室後，顧若塵拿起手機看著照片上的喬喬微微一笑。",
            "誰能想到以前的一個「醜小鴨」，以後有可能變成一個萬眾矚目聚光燈下的超模了。",
        ]
    )
    raw = f"第1章 标题\n\n{body}\n"
    (config.storage.origin_dir / "chuong_1-1.txt").write_text(raw, encoding="utf-8")

    report = verify_crawled_content(config, from_chapter=1, to_chapter=1, fix_stale_manifest=False)
    assert report.ok is False
    assert any(issue.code == "metadata_content" and issue.chapter_number == 1 for issue in report.issues)
