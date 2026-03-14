from __future__ import annotations

import json
import os
from pathlib import Path

import yaml

from .models import (
    BrowserDebugConfig,
    CaptionConfig,
    CrawlConfig,
    NovelConfig,
    QueueConfig,
    QueueModelConfig,
    RedisConfig,
    SourceConfig,
    StorageConfig,
    TranslationConfig,
    TtsConfig,
    VideoConfig,
    VisualConfig,
)
from novel_tts.translate.glossary import sanitize_glossary_entries


def _root_dir() -> Path:
    return Path(__file__).resolve().parents[2]


def _config_path(novel_id: str) -> Path:
    return _root_dir() / "configs" / "novels" / f"{novel_id}.json"


def _app_config_path() -> Path:
    return _root_dir() / "configs" / "app.yaml"


def _source_config_path(source_id: str) -> Path:
    return _root_dir() / "configs" / "sources" / f"{source_id}.json"


def _glossary_path(novel_id: str) -> Path:
    return _root_dir() / "configs" / "glossaries" / f"{novel_id}.json"


def _deep_merge(base: dict, override: dict) -> dict:
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _load_app_config() -> dict:
    path = _app_config_path()
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}
    if not isinstance(payload, dict):
        raise ValueError(f"Invalid app config format: {path}")
    return payload


def _normalize_queue_config(queue_raw: dict) -> dict:
    normalized = dict(queue_raw)
    enabled_models = normalized.get("enabled_models")
    model_configs = {key: dict(value) for key, value in normalized.get("model_configs", {}).items()}

    legacy_worker_models = normalized.pop("worker_models", None)
    legacy_worker_counts = normalized.pop("model_worker_counts", {})
    legacy_rpm_limits = normalized.pop("model_rpm_limits", {})
    legacy_tpm_limits = normalized.pop("model_tpm_limits", {})
    legacy_chunk_max_len = normalized.pop("model_chunk_max_len", {})
    legacy_chunk_sleep_seconds = normalized.pop("model_chunk_sleep_seconds", {})

    if enabled_models is None:
        enabled_models = legacy_worker_models
    if enabled_models is None:
        enabled_models = ["gemma-3-27b-it", "gemma-3-12b-it"]

    for model in enabled_models:
        cfg = dict(model_configs.get(model, {}))
        if model in legacy_worker_counts:
            cfg.setdefault("worker_count", legacy_worker_counts[model])
        if model in legacy_rpm_limits:
            cfg.setdefault("rpm_limit", legacy_rpm_limits[model])
        if model in legacy_tpm_limits:
            cfg.setdefault("tpm_limit", legacy_tpm_limits[model])
        if model in legacy_chunk_max_len:
            cfg.setdefault("chunk_max_len", legacy_chunk_max_len[model])
        if model in legacy_chunk_sleep_seconds:
            cfg.setdefault("chunk_sleep_seconds", legacy_chunk_sleep_seconds[model])
        if cfg:
            model_configs[model] = cfg

    missing = [model for model in enabled_models if model not in model_configs]
    if missing:
        raise ValueError(
            "Queue config is missing model_configs for enabled_models: " + ", ".join(sorted(missing))
        )

    normalized["enabled_models"] = enabled_models
    normalized["model_configs"] = model_configs
    return normalized


def load_novel_config(novel_id: str) -> NovelConfig:
    path = _config_path(novel_id)
    if not path.exists():
        raise FileNotFoundError(f"Novel config not found: {path}")
    raw = json.loads(path.read_text(encoding="utf-8"))
    app_raw = _load_app_config()
    source_id = raw["source_id"]
    source_path = _source_config_path(source_id)
    if not source_path.exists():
        raise FileNotFoundError(f"Source config not found: {source_path}")
    source_raw = json.loads(source_path.read_text(encoding="utf-8"))
    root = _root_dir()
    storage_input_dir = root / raw["storage"]["input_dir"]
    storage_output_dir = root / raw["storage"]["output_dir"]
    storage_image_dir = root / raw["storage"]["image_dir"]
    storage_logs_dir = root / raw["storage"].get("logs_dir", ".logs")
    storage_tmp_dir = root / raw["storage"].get("tmp_dir", "tmp")
    storage = StorageConfig(
        root=root,
        input_dir=storage_input_dir,
        output_dir=storage_output_dir,
        image_dir=storage_image_dir,
        logs_dir=storage_logs_dir,
        tmp_dir=storage_tmp_dir,
    )
    merged_crawl_raw = _deep_merge(source_raw["crawl"], raw.get("crawl", {}))
    merged_browser_debug_raw = _deep_merge(source_raw.get("browser_debug", {}), raw.get("browser_debug", {}))
    merged_queue_raw = _deep_merge(app_raw.get("queue", {}), raw.get("queue", {}))
    merged_queue_raw = _normalize_queue_config(merged_queue_raw)
    if "redis" not in merged_queue_raw:
        merged_queue_raw["redis"] = {}
    legacy_redis = {
        "host": merged_queue_raw.pop("redis_host", None),
        "port": merged_queue_raw.pop("redis_port", None),
        "database": merged_queue_raw.pop("redis_database", None),
        "prefix": merged_queue_raw.pop("redis_prefix", None),
    }
    for key, value in legacy_redis.items():
        if value is not None and key not in merged_queue_raw["redis"]:
            merged_queue_raw["redis"][key] = value
    translation_raw = dict(raw["translation"])
    glossary_file = translation_raw.get("glossary_file", "")
    glossary_path = root / glossary_file if glossary_file else _glossary_path(novel_id)
    if glossary_path.exists():
        glossary_raw = json.loads(glossary_path.read_text(encoding="utf-8"))
        glossary_clean, _dropped = sanitize_glossary_entries(glossary_raw)
        translation_raw["glossary"] = glossary_clean
        translation_raw["glossary_file"] = str(glossary_path.relative_to(root))
    else:
        translation_raw.setdefault("glossary", {})
        translation_raw["glossary_file"] = glossary_file
    translation_raw["model"] = os.environ.get(
        "NOVEL_TTS_TRANSLATION_MODEL",
        os.environ.get("GEMINI_MODEL", translation_raw["model"]),
    )
    translation_raw["repair_model"] = os.environ.get("REPAIR_MODEL", translation_raw.get("repair_model", ""))
    if "CHUNK_MAX_LEN" in os.environ:
        translation_raw["chunk_max_len"] = int(os.environ["CHUNK_MAX_LEN"])
    if "CHUNK_SLEEP_SECONDS" in os.environ:
        translation_raw["chunk_sleep_seconds"] = float(os.environ["CHUNK_SLEEP_SECONDS"])
    if "REPAIR_MODE" in os.environ:
        translation_raw["repair_mode"] = os.environ["REPAIR_MODE"].strip().lower() in {"1", "true", "yes"}

    source = SourceConfig(
        source_id=source_id,
        resolver_id=source_raw.get("resolver_id", source_id),
        crawl=CrawlConfig(**merged_crawl_raw),
        browser_debug=BrowserDebugConfig(**merged_browser_debug_raw),
    )

    return NovelConfig(
        novel_id=raw["novel_id"],
        title=raw["title"],
        slug=raw["slug"],
        source_language=raw.get("source_language", "zh"),
        target_language=raw.get("target_language", "vi"),
        source_id=source_id,
        source=source,
        storage=storage,
        crawl=source.crawl,
        browser_debug=source.browser_debug,
        translation=TranslationConfig(**translation_raw),
        captions=CaptionConfig(**raw["captions"]),
        queue=QueueConfig(
            redis=RedisConfig(**merged_queue_raw.pop("redis", {})),
            model_configs={
                model: QueueModelConfig(**cfg)
                for model, cfg in merged_queue_raw.pop("model_configs", {}).items()
            },
            **merged_queue_raw,
        ),
        tts=TtsConfig(**raw["tts"]),
        visual=VisualConfig(**raw["visual"]),
        video=VideoConfig(**raw["video"]),
    )
