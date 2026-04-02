from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

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
from novel_tts.tts import service as tts_service


def _make_config(tmp_path: Path) -> NovelConfig:
    storage = StorageConfig(
        root=tmp_path,
        input_dir=tmp_path / "input" / "novel",
        output_dir=tmp_path / "output" / "novel",
        image_dir=tmp_path / "image" / "novel",
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
            provider="dummy",
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
                chapter_batch_overrides=[MediaBatchRule(range="1-20", chapter_batch_size=20)],
            ),
        ),
    )


@dataclass
class _AudioResult:
    local_path: Path
    cleanup_target: object | None = None


class _DummyProvider:
    def __init__(self, tmp_path: Path) -> None:
        self.tmp_path = tmp_path
        self.calls: list[str] = []

    def connect(self):
        return object()

    def load_model(self, _client) -> None:
        return None

    def synthesize(self, _client, chunk: str, progress_callback=None):
        self.calls.append(chunk)
        if progress_callback is not None:
            progress_callback("DONE")
        return chunk

    def materialize_output_audio(self, _client, result: str) -> _AudioResult:
        wav_path = self.tmp_path / f"{len(self.calls)}.wav"
        wav_path.parent.mkdir(parents=True, exist_ok=True)
        wav_path.write_bytes(result.encode("utf-8"))
        return _AudioResult(local_path=wav_path)

    def cleanup_output_audio(self, _client, _cleanup_target) -> None:
        return None


def test_run_tts_reads_across_multiple_translated_batches(tmp_path: Path, monkeypatch) -> None:
    config = _make_config(tmp_path)
    config.storage.translated_dir.mkdir(parents=True, exist_ok=True)
    batch_1 = []
    for chapter in range(1, 11):
        batch_1.append(f"Chương {chapter}: Chapter {chapter}\nNoi dung {chapter}")
    batch_2 = []
    for chapter in range(11, 21):
        batch_2.append(f"Chương {chapter}: Chapter {chapter}\nNoi dung {chapter}")
    (config.storage.translated_dir / "chuong_1-10.txt").write_text(
        "\n\n".join(batch_1) + "\n",
        encoding="utf-8",
    )
    (config.storage.translated_dir / "chuong_11-20.txt").write_text(
        "\n\n".join(batch_2) + "\n",
        encoding="utf-8",
    )

    provider = _DummyProvider(tmp_path / "provider")
    monkeypatch.setattr(tts_service, "get_tts_provider", lambda _config: provider)
    monkeypatch.setattr(
        tts_service,
        "_merge_audio",
        lambda *, audio_files, merged_path, tempo, bitrate, workers, tmp_dir: merged_path.write_bytes(b"merged"),
    )

    merged = tts_service.run_tts(config, 1, 20)

    assert merged.name == "chuong_1-20.aac"
    assert merged.exists()
    parts = sorted(
        (config.storage.audio_dir / "chuong_1-20" / ".parts").glob("chapter_*.wav"),
        key=lambda path: int(path.stem.split("_")[1]),
    )
    assert [path.name for path in parts] == [
        *(f"chapter_{chapter}.wav" for chapter in range(1, 21))
    ]
    assert len(provider.calls) == 20
