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
    VideoConfig,
    VisualConfig,
)
from novel_tts.media import service as media_service


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
        provider="dummy",
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
        visual=VisualConfig(background_video="background.mp4"),
        video=VideoConfig(),
    )


def test_generate_visual_requires_drawtext(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config = _make_config(tmp_path)
    monkeypatch.setattr(media_service, "ffmpeg_has_filter", lambda _name: False)

    with pytest.raises(RuntimeError, match="drawtext"):
        media_service.generate_visual(config, 1, 10)


def test_generate_visual_part_index_uses_video_episode_batch_size(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _make_config(tmp_path)
    config.video.episode_batch_size = 10
    config.storage.image_dir.mkdir(parents=True, exist_ok=True)
    (config.storage.image_dir / config.visual.background_video).write_bytes(b"fake")
    (config.storage.root / "image").mkdir(parents=True, exist_ok=True)
    (config.storage.root / "image" / "channel-name.png").write_bytes(b"fake")

    ffmpeg_calls: list[list[str]] = []

    monkeypatch.setattr(media_service, "ffmpeg_has_filter", lambda _name: True)
    monkeypatch.setattr(media_service, "run_ffmpeg", lambda args: ffmpeg_calls.append(args))

    media_service.generate_visual(config, 11, 20)

    assert ffmpeg_calls, "Expected ffmpeg to be invoked"
    first_call = ffmpeg_calls[0]
    filter_idx = first_call.index("-filter_complex")
    filters = first_call[filter_idx + 1]
    assert "Tập 2" in filters
    assert "scale=-1:114[channel]" in filters
    assert "overlay=x=W-w-10:y=35" in filters
    assert str(config.storage.root / "image" / "channel-name.png") in first_call
    assert "-an" in first_call
    assert "0:a?" not in first_call


def test_generate_visual_requires_channel_name_image(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config = _make_config(tmp_path)
    config.storage.image_dir.mkdir(parents=True, exist_ok=True)
    (config.storage.image_dir / config.visual.background_video).write_bytes(b"fake")

    monkeypatch.setattr(media_service, "ffmpeg_has_filter", lambda _name: True)

    with pytest.raises(FileNotFoundError, match="channel-name.png"):
        media_service.generate_visual(config, 1, 10)


def test_generate_visual_for_chapter_uses_background_cover(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config = _make_config(tmp_path)
    config.visual.background_cover = "background.jpg"
    config.visual.line1 = "Tập 1"
    config.visual.line2 = "Không Qua Phong Tuyết,"
    config.visual.line3 = "Làm Sao Thấy Cầu Vồng?"
    config.storage.image_dir.mkdir(parents=True, exist_ok=True)
    (config.storage.image_dir / config.visual.background_cover).write_bytes(b"fake")

    ffmpeg_calls: list[list[str]] = []

    monkeypatch.setattr(media_service, "ffmpeg_has_filter", lambda _name: True)
    monkeypatch.setattr(media_service, "run_ffmpeg", lambda args: ffmpeg_calls.append(args))

    media_service.generate_visual_for_chapter(config, 2)

    assert len(ffmpeg_calls) >= 1
    first_call = ffmpeg_calls[0]
    assert "-loop" in first_call
    assert str(config.storage.image_dir / config.visual.background_cover) in first_call
    vf_idx = first_call.index("-vf")
    filters = first_call[vf_idx + 1]
    assert "pad=iw:ih+220:0:0:color=black" not in filters
    assert "Tập 2" in filters
    assert "Không Qua Phong Tuyết," in filters
    assert "Làm Sao Thấy Cầu Vồng?" in filters


def test_generate_visual_for_chapter_requires_valid_cover_extension(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _make_config(tmp_path)
    config.visual.background_cover = "background.gif"
    config.storage.image_dir.mkdir(parents=True, exist_ok=True)
    (config.storage.image_dir / config.visual.background_cover).write_bytes(b"fake")
    monkeypatch.setattr(media_service, "ffmpeg_has_filter", lambda _name: True)

    with pytest.raises(ValueError, match=r"\.jpg, \.jpeg, or \.png"):
        media_service.generate_visual_for_chapter(config, 1)


def test_generate_visual_uses_cached_final_outputs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config = _make_config(tmp_path)
    config.storage.image_dir.mkdir(parents=True, exist_ok=True)
    (config.storage.image_dir / config.visual.background_video).write_bytes(b"background")
    (config.storage.root / "image").mkdir(parents=True, exist_ok=True)
    (config.storage.root / "image" / "channel-name.png").write_bytes(b"channel")

    ffmpeg_calls: list[list[str]] = []

    def _fake_run_ffmpeg(args: list[str]) -> None:
        ffmpeg_calls.append(args)
        Path(args[-1]).write_bytes(b"rendered")

    monkeypatch.setattr(media_service, "ffmpeg_has_filter", lambda _name: True)
    monkeypatch.setattr(media_service, "run_ffmpeg", _fake_run_ffmpeg)

    media_service.generate_visual(config, 1, 10)
    assert len(ffmpeg_calls) == 2
    cache_path = config.storage.visual_dir / ".cache" / "chuong_1-10.sha256"
    assert cache_path.exists()

    media_service.generate_visual(config, 1, 10)
    assert len(ffmpeg_calls) == 2

    media_service.generate_visual(config, 1, 10, force=True)
    assert len(ffmpeg_calls) == 4


def test_create_video_uses_cached_final_output_and_invalidates_on_input_change(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _make_config(tmp_path)
    visual_path = config.storage.visual_dir / "chuong_1-10.mp4"
    audio_path = config.storage.audio_dir / "chuong_1-10" / "chuong_1-10.mp3"
    visual_path.parent.mkdir(parents=True, exist_ok=True)
    audio_path.parent.mkdir(parents=True, exist_ok=True)
    visual_path.write_bytes(b"visual-v1")
    audio_path.write_bytes(b"audio-v1")

    ffmpeg_calls: list[list[str]] = []

    def _fake_run_ffmpeg(args: list[str]) -> None:
        ffmpeg_calls.append(args)
        Path(args[-1]).write_bytes(b"video")

    monkeypatch.setattr(media_service, "run_ffmpeg", _fake_run_ffmpeg)
    monkeypatch.setattr(media_service, "ffprobe_duration", lambda _path: 12.5)

    media_service.create_video(config, 1, 10)
    assert len(ffmpeg_calls) == 1
    cache_path = config.storage.video_dir / ".cache" / "chuong_1-10.sha256"
    assert cache_path.exists()

    media_service.create_video(config, 1, 10)
    assert len(ffmpeg_calls) == 1

    audio_path.write_bytes(b"audio-v2")
    media_service.create_video(config, 1, 10)
    assert len(ffmpeg_calls) == 2

    media_service.create_video(config, 1, 10, force=True)
    assert len(ffmpeg_calls) == 3
