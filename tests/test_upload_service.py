from __future__ import annotations

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
    UploadConfig,
    UploadTikTokConfig,
    UploadYouTubeConfig,
    VideoConfig,
    VisualConfig,
)
from novel_tts.upload.service import run_upload


def _make_config(tmp_path: Path) -> NovelConfig:
    root = tmp_path
    storage = StorageConfig(
        root=root,
        input_dir=root / "input" / "novel",
        output_dir=root / "output" / "novel",
        image_dir=root / "image" / "novel",
        logs_dir=root / ".logs",
        tmp_dir=root / "tmp",
    )
    crawl = CrawlConfig(site_id="test")
    browser_debug = BrowserDebugConfig()
    source = SourceConfig(source_id="test", resolver_id="test", crawl=crawl, browser_debug=browser_debug)
    models = ModelsConfig(
        provider="gemini_http",
        enabled_models=["m1"],
        model_configs={"m1": QueueModelConfig(chunk_max_len=1000)},
    )
    translation = TranslationConfig(chapter_regex=r"^$", base_rules="", glossary_file="")
    upload = UploadConfig(
        default_platform="youtube",
        youtube=UploadYouTubeConfig(enabled=True),
        tiktok=UploadTikTokConfig(enabled=True, dry_run=True),
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
        browser_debug=browser_debug,
        models=models,
        translation=translation,
        captions=CaptionConfig(),
        queue=QueueConfig(),
        tts=TtsConfig(provider="local", voice="test"),
        visual=VisualConfig(background_video="bg.mp4"),
        video=VideoConfig(),
        upload=upload,
    )


def _prepare_output_files(config: NovelConfig, *, range_key: str = "chuong_1-10") -> None:
    config.storage.video_dir.mkdir(parents=True, exist_ok=True)
    config.storage.visual_dir.mkdir(parents=True, exist_ok=True)
    config.storage.subtitle_dir.mkdir(parents=True, exist_ok=True)
    config.storage.output_dir.mkdir(parents=True, exist_ok=True)

    (config.storage.video_dir / f"{range_key}.mp4").write_bytes(b"mp4")
    (config.storage.visual_dir / f"{range_key}.png").write_bytes(b"png")
    (config.storage.subtitle_dir / f"{range_key}_menu.txt").write_text("00:00:00 - Chương 1", encoding="utf-8")
    (config.storage.output_dir / "title.txt").write_text("Tieu de", encoding="utf-8")
    (config.storage.output_dir / "description.txt").write_text("Mo ta", encoding="utf-8")
    (config.storage.output_dir / "playlist.txt").write_text("PL1234567890", encoding="utf-8")


def test_run_upload_youtube_dry_run_builds_metadata(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    _prepare_output_files(config)

    result = run_upload(config, 1, 10, platform="youtube", dry_run=True)

    assert result["platform"] == "youtube"
    assert result["range_key"] == "chuong_1-10"
    assert result["status"] == "dry-run"


def test_run_upload_youtube_missing_menu_raises(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    _prepare_output_files(config)
    (config.storage.subtitle_dir / "chuong_1-10_menu.txt").unlink()

    with pytest.raises(FileNotFoundError, match="menu"):
        run_upload(config, 1, 10, platform="youtube", dry_run=True)


def test_run_upload_tiktok_dry_run(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    _prepare_output_files(config)

    result = run_upload(config, 1, 10, platform="tiktok", dry_run=False)

    assert result["platform"] == "tiktok"
    assert result["range_key"] == "chuong_1-10"
    assert result["status"] == "dry-run"


def test_run_upload_youtube_dry_run_does_not_require_media_files(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    _prepare_output_files(config)
    (config.storage.video_dir / "chuong_1-10.mp4").unlink()
    (config.storage.visual_dir / "chuong_1-10.png").unlink()

    result = run_upload(config, 1, 10, platform="youtube", dry_run=True)

    assert result["status"] == "dry-run"


def test_run_upload_tiktok_dry_run_does_not_require_media_files(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    _prepare_output_files(config)
    (config.storage.video_dir / "chuong_1-10.mp4").unlink()
    (config.storage.visual_dir / "chuong_1-10.png").unlink()

    result = run_upload(config, 1, 10, platform="tiktok", dry_run=True)

    assert result["status"] == "dry-run"


def test_run_upload_youtube_dry_run_rewrites_tap_index_from_range(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config = _make_config(tmp_path)
    _prepare_output_files(config, range_key="chuong_11-20")
    (config.storage.output_dir / "title.txt").write_text("Tập 1 | Vô Cực Thiên Tôn", encoding="utf-8")

    captured: dict[str, str] = {}

    def _capture(config, spec, *, dry_run):  # type: ignore[no-redef]
        captured["title"] = spec.title
        return {"status": "dry-run", "platform": "youtube", "range_key": spec.range_key}

    from novel_tts.upload import service as upload_service

    monkeypatch.setattr(upload_service, "_upload_youtube", _capture)
    result = run_upload(config, 11, 20, platform="youtube", dry_run=True)

    assert result["status"] == "dry-run"
    assert captured["title"].startswith("Tập 2")
