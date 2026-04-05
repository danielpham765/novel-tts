from __future__ import annotations

import os
import tempfile
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
from novel_tts.queue.translation_queue import (
    _classify_process_state,
    _extract_target_from_argv,
    _format_countdown,
    _format_target,
    _pick_last_ms_key,
    _unique_target_count,
    _worker_is_recently_picking,
)


class _FakeRedis:
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


def test_format_countdown_under_3m_includes_seconds() -> None:
    assert _format_countdown(None) == ""
    assert _format_countdown(0) == ""
    assert _format_countdown(-1) == ""
    assert _format_countdown(47) == "47s"
    assert _format_countdown(60) == "1m:0s"
    assert _format_countdown(179) == "2m:59s"
    assert _format_countdown(180) == "3m:0s"


def test_format_countdown_over_3m_hides_seconds() -> None:
    assert _format_countdown(181) == "3m"
    assert _format_countdown(3485) == "58m"
    assert _format_countdown(3600) == "1h:0m"
    assert _format_countdown(3723) == "1h:2m"


def test_unique_target_count_prefers_translate_chapter_targets() -> None:
    rows = [
        {"role": "worker", "target": "chuong_1-10:0004"},
        {"role": "translate-chapter", "target": "chuong_1-10:0004"},
        {"role": "translate-chapter", "target": "chuong_1-10:0004"},
        {"role": "translate-chapter", "target": "chuong_1-10:0005"},
        {"role": "worker", "target": "chuong_1-10:0005"},
    ]
    assert _unique_target_count(rows) == 2


def test_unique_target_count_falls_back_when_no_translate_chapter_rows() -> None:
    rows = [
        {"role": "worker", "target": "chuong_1-10:0004"},
        {"role": "worker", "target": "chuong_1-10:0004"},
        {"role": "worker", "target": "chuong_1-10:0005"},
        {"role": "monitor", "target": ""},
    ]
    assert _unique_target_count(rows) == 2


def test_format_target_pads_part_number() -> None:
    assert _format_target("chuong_1-10", "4") == "chuong_1-10:0004"
    assert _format_target("caption_cn.srt", "0001") == "caption_cn.srt:0001"
    assert _format_target("/a/b/caption_cn.srt", "12") == "caption_cn.srt:0012"
    assert _format_target("", "1") == ""
    assert _format_target("file", "") == ""


def test_extract_target_from_argv() -> None:
    argv = ["novel-tts", "translate", "chapter", "novel", "--file", "chuong_1-10", "--chapter", "4"]
    assert _extract_target_from_argv(argv) == "chuong_1-10:0004"


def test_worker_is_recently_picking_only_for_short_window(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    secrets = cfg.storage.root / ".secrets"
    secrets.mkdir(parents=True, exist_ok=True)
    (secrets / "gemini-keys.txt").write_text("key-1\n", encoding="utf-8")

    client = _FakeRedis()
    now_ms = time.time() * 1000.0
    key = _pick_last_ms_key(cfg, 1)

    client.set(key, str(now_ms - 500.0))
    assert _worker_is_recently_picking(cfg, client, key_index=1) is True

    client.set(key, str(now_ms - 6000.0))
    assert _worker_is_recently_picking(cfg, client, key_index=1) is False


def test_worker_error_is_not_sticky_after_success() -> None:
    with tempfile.NamedTemporaryFile("w+", encoding="utf-8", delete=True) as fh:
        fh.write("2026-03-16 06:00:00,000 | ERROR | x | Traceback (most recent call last):\n")
        fh.write("2026-03-16 06:00:01,000 | INFO | x | Worker done: chuong_1-10.txt::0001\n")
        fh.flush()
        state, countdown = _classify_process_state("worker", is_busy=False, log_file=fh.name)
        assert state == "idle"
        assert countdown is None


def test_worker_error_is_held_briefly() -> None:
    import datetime as _dt

    now = _dt.datetime.now()
    stamp = now.strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
    with tempfile.NamedTemporaryFile("w+", encoding="utf-8", delete=True) as fh:
        fh.write(f"{stamp} | ERROR | x | Command failed\n")
        fh.flush()
        state, countdown = _classify_process_state("worker", is_busy=False, log_file=fh.name)
        assert state == "error"
        assert countdown is None


def test_worker_traceback_without_timestamp_is_not_sticky() -> None:
    import datetime as _dt

    with tempfile.NamedTemporaryFile("w+", encoding="utf-8", delete=True) as fh:
        fh.write("Traceback (most recent call last):\n")
        fh.write("  File \"x.py\", line 1, in <module>\n")
        fh.flush()
        past = (_dt.datetime.now() - _dt.timedelta(seconds=30)).timestamp()
        os.utime(fh.name, (past, past))
        state, countdown = _classify_process_state("worker", is_busy=False, log_file=fh.name)
        assert state == "idle"
        assert countdown is None


def test_translate_chapter_infers_translate_phase_from_chunk_logs() -> None:
    with tempfile.NamedTemporaryFile("w+", encoding="utf-8", delete=True) as fh:
        fh.write("2026-03-16 06:00:00,000 | INFO | x | Translating chuong_1-10.txt::0001 chunk 2/9\n")
        fh.flush()
        state, countdown = _classify_process_state("translate-chapter", is_busy=True, log_file=fh.name)
        assert state == "translate"
        assert countdown is None


def test_translate_chapter_infers_repair_phase_from_han_repair_logs() -> None:
    with tempfile.NamedTemporaryFile("w+", encoding="utf-8", delete=True) as fh:
        fh.write("2026-03-16 06:00:00,000 | INFO | x | Han residue detected; aggressive repair | unit=u\n")
        fh.flush()
        state, countdown = _classify_process_state("translate-chapter", is_busy=True, log_file=fh.name)
        assert state == "repair"
        assert countdown is None


def test_translate_chapter_infers_glossary_phase_from_glossary_logs() -> None:
    with tempfile.NamedTemporaryFile("w+", encoding="utf-8", delete=True) as fh:
        fh.write("2026-03-16 06:00:00,000 | INFO | x | Glossary extract chunked | unit=u windows=3\n")
        fh.flush()
        state, countdown = _classify_process_state("translate-chapter", is_busy=True, log_file=fh.name)
        assert state == "glossary"
        assert countdown is None


def test_translate_chapter_infers_upstream_timeout_from_requests_log() -> None:
    with tempfile.NamedTemporaryFile("w+", encoding="utf-8", delete=True) as fh:
        fh.write(
            "2026-03-16 06:00:00,000 | WARNING | novel_tts.translate.providers | "
            "Gemini API generation error (attempt 1/12): HTTPSConnectionPool(host='generativelanguage.googleapis.com', port=443): "
            "Read timed out. (read timeout=90)\n"
        )
        fh.flush()
        state, countdown = _classify_process_state("translate-chapter", is_busy=True, log_file=fh.name)
        assert state == "upstream-timeout"
        assert countdown is None
