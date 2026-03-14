from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class StorageConfig:
    root: Path
    input_dir: Path
    output_dir: Path
    image_dir: Path
    logs_dir: Path
    tmp_dir: Path

    @property
    def origin_dir(self) -> Path:
        return self.input_dir / "origin"

    @property
    def translated_dir(self) -> Path:
        return self.input_dir / "translated"

    @property
    def caption_dir(self) -> Path:
        return self.input_dir / "caption"

    @property
    def progress_dir(self) -> Path:
        return self.input_dir / ".progress"

    @property
    def parts_dir(self) -> Path:
        return self.input_dir / ".parts"

    @property
    def audio_dir(self) -> Path:
        return self.output_dir / "audio"

    @property
    def subtitle_dir(self) -> Path:
        return self.output_dir / "subtitle"

    @property
    def visual_dir(self) -> Path:
        return self.output_dir / "visual"

    @property
    def video_dir(self) -> Path:
        return self.output_dir / "video"


@dataclass
class CrawlConfig:
    site_id: str
    directory_url: str = ""
    chapter_batch_size: int = 10
    chapter_url_pattern: str | None = None
    selectors: dict[str, list[str]] = field(default_factory=dict)
    chapter_regex: str = r"^第(\d+)章([^\n]*)"
    preferred_fetch_mode: str = "auto"
    request_timeout_seconds: int = 120
    content_wait_timeout_seconds: int = 45
    delay_between_chapters_seconds: float = 1.5
    max_fetch_retries: int = 3
    retry_backoff_seconds: float = 15.0
    rate_limit_cooldown_seconds: float = 300.0


@dataclass
class BrowserDebugConfig:
    mode: str = "auto"
    remote_debugging_url: str = ""
    remote_debugging_port: int = 9222
    executable_path: str = ""
    user_data_dir: str = ""
    profile_directory: str = ""
    headless: bool = False
    debug_image_dir: str = "debug/img"


@dataclass
class TranslationConfig:
    provider: str
    model: str
    chunk_max_len: int
    chunk_sleep_seconds: float
    chapter_regex: str
    base_rules: str
    repair_model: str = ""
    glossary: dict[str, str] = field(default_factory=dict)
    post_replacements: dict[str, str] = field(default_factory=dict)
    han_fallback_replacements: dict[str, str] = field(default_factory=dict)
    line_token: str = "QZXBRQ"
    repair_mode: bool = False
    glossary_file: str = ""
    auto_update_glossary: bool = True


@dataclass
class CaptionConfig:
    provider: str
    model: str
    chunk_size: int = 120
    chunk_concurrency: int = 1
    request_timeout_ms: int = 90000
    prompt_debug_dir: str = "tmp/translate-captions-prompts"
    response_dump_dir: str = "tmp/translate-captions-responses"
    input_file: str = "caption_cn.srt"
    output_file: str = "caption_vn.srt"


@dataclass
class RedisConfig:
    host: str = "127.0.0.1"
    port: int = 6379
    database: int = 0
    prefix: str = "novel_tts"


@dataclass
class QueueModelConfig:
    worker_count: int = 1
    rpm_limit: int = 0
    tpm_limit: int = 0
    rpd_limit: int = 0
    repair_model: str = ""
    chunk_max_len: int = 0
    chunk_sleep_seconds: float = 0.0


@dataclass
class QueueConfig:
    redis: RedisConfig = field(default_factory=RedisConfig)
    max_retries: int = 3
    inflight_ttl_seconds: int = 3600
    supervisor_interval_seconds: int = 15
    status_interval_seconds: int = 60
    enabled_models: list[str] = field(default_factory=lambda: ["gemma-3-27b-it", "gemma-3-12b-it"])
    model_configs: dict[str, QueueModelConfig] = field(default_factory=dict)


@dataclass
class TtsConfig:
    provider: str
    voice: str
    server_name: str = "local"
    model_name: str = "macos"
    generation_mode: str = "Standard (Một lần)"
    use_batch: bool = True
    max_batch_size_run: int = 128
    temperature: float = 1.0
    max_chars_chunk: int = 512
    tempo: float = 1.15
    bitrate: str = "128k"


@dataclass
class VisualConfig:
    background_video: str
    font_file: str = ""
    tag_text: str = ""
    line1: str = ""
    line2: str = ""
    line3: str = ""
    render_width: int = 1280


@dataclass
class VideoConfig:
    video_codec: str = "libx264"
    audio_codec: str = "aac"
    preset: str = "veryfast"
    crf: int = 28
    audio_bitrate: str = "96k"


@dataclass
class SourceConfig:
    source_id: str
    resolver_id: str
    crawl: CrawlConfig
    browser_debug: BrowserDebugConfig


@dataclass
class NovelConfig:
    novel_id: str
    title: str
    slug: str
    source_language: str
    target_language: str
    source_id: str
    source: SourceConfig
    storage: StorageConfig
    crawl: CrawlConfig
    browser_debug: BrowserDebugConfig
    translation: TranslationConfig
    captions: CaptionConfig
    queue: QueueConfig
    tts: TtsConfig
    visual: VisualConfig
    video: VideoConfig
