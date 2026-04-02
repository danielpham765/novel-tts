from __future__ import annotations

import time
from pathlib import Path

from novel_tts.config.models import (
    BrowserDebugConfig,
    CaptionConfig,
    CrawlConfig,
    MediaConfig,
    ModelsConfig,
    NovelConfig,
    QueueConfig,
    RedisConfig,
    SourceConfig,
    StorageConfig,
    TranslationConfig,
    TtsConfig,
    VideoConfig,
    VisualConfig,
)
from novel_tts.queue.translation_queue import _pick_last_ms_key, _worker_is_recently_picking


class FakeRedis:
    def __init__(self) -> None:
        self.store: dict[str, str] = {}

    def get(self, key: str):
        return self.store.get(key)

    def set(self, key: str, value: str) -> None:
        self.store[key] = value


def _config(tmp_path: Path) -> NovelConfig:
    storage = StorageConfig(
        root=tmp_path,
        input_dir=tmp_path / "input",
        output_dir=tmp_path / "output",
        image_dir=tmp_path / "image",
        logs_dir=tmp_path / ".logs",
        tmp_dir=tmp_path / "tmp",
    )
    crawl = CrawlConfig(site_id="site")
    browser_debug = BrowserDebugConfig()
    source = SourceConfig(source_id="site", resolver_id="resolver", crawl=crawl)
    queue = QueueConfig(redis=RedisConfig(prefix="novel_tts"), min_pick_interval_seconds=2.0)
    return NovelConfig(
        novel_id="novel",
        title="Novel",
        slug="novel",
        source_language="zh",
        target_language="vi",
        source_id="site",
        source=source,
        storage=storage,
        crawl=crawl,
        models=ModelsConfig(provider="gemini_http"),
        translation=TranslationConfig(chapter_regex=r"^Chuong (\d+)", base_rules=""),
        captions=CaptionConfig(),
        queue=queue,
        tts=TtsConfig(provider="gradio_vie_tts", voice="voice"),
        media=MediaConfig(
            visual=VisualConfig(background_video="bg.mp4"),
            video=VideoConfig(),
        ),
    )


def test_worker_is_recently_picking_only_for_short_window(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    secrets = cfg.storage.root / ".secrets"
    secrets.mkdir(parents=True, exist_ok=True)
    (secrets / "gemini-keys.txt").write_text("key-1\n", encoding="utf-8")

    client = FakeRedis()
    now_ms = time.time() * 1000.0
    key = _pick_last_ms_key(cfg, 1)

    client.set(key, str(now_ms - 500.0))
    assert _worker_is_recently_picking(cfg, client, key_index=1) is True

    client.set(key, str(now_ms - 6000.0))
    assert _worker_is_recently_picking(cfg, client, key_index=1) is False
