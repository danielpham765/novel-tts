from __future__ import annotations

import json
import logging
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

LOGGER = logging.getLogger(__name__)


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
    models_raw = _deep_merge(app_raw.get("models", {}), raw.get("models", {}))
    # New schema (preferred):
    # - translation: common settings shared by chapter + captions (provider, glossary_file, line_token, replacements, etc.)
    # - translation.chapter: settings specific to translate chapter (chapter_regex, base_rules, etc.)
    # - translation.captions: settings specific to translate captions (input_file, output_file, etc.)
    #
    # Legacy schema support:
    # - root.chapter (renamed from earlier refactor) or root.translation containing chapter fields.
    # - root.captions containing captions fields.
    app_translation_root = app_raw.get("translation", {})
    if app_translation_root is None:
        app_translation_root = {}
    if not isinstance(app_translation_root, dict):
        raise ValueError('Invalid app config "translation" (expected object)')

    novel_translation_root = raw.get("translation")
    if novel_translation_root is None:
        legacy = raw.get("chapter") if isinstance(raw.get("chapter"), dict) else raw.get("translation")
        if isinstance(legacy, dict):
            novel_translation_root = {"chapter": legacy, "captions": raw.get("captions", {})}
            LOGGER.warning('Using legacy config keys; please migrate to translation.chapter/translation.captions | novel=%s', novel_id)
        else:
            raise KeyError('Missing translation config (expected "translation")')
    if not isinstance(novel_translation_root, dict):
        raise ValueError('Invalid novel "translation" config (expected object)')

    translation_root = _deep_merge(app_translation_root, novel_translation_root)
    chapter_section = translation_root.get("chapter", {})
    if chapter_section is None:
        chapter_section = {}
    if not isinstance(chapter_section, dict):
        raise ValueError('Invalid translation.chapter config (expected object)')
    captions_section = translation_root.get("captions", {})
    if captions_section is None:
        captions_section = {}
    if not isinstance(captions_section, dict):
        raise ValueError('Invalid translation.captions config (expected object)')
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
    # Model pool settings are shared across queue and direct translate, so we support a top-level
    # "models" section in configs/app.yaml and per-novel overrides in configs/novels/*.json.
    # Keep backward compatibility with legacy queue.enabled_models / queue.model_configs.
    if "enabled_models" not in merged_queue_raw and isinstance(models_raw.get("enabled_models"), list):
        merged_queue_raw["enabled_models"] = models_raw["enabled_models"]
    elif "enabled_models" in merged_queue_raw and isinstance(models_raw.get("enabled_models"), list):
        if merged_queue_raw["enabled_models"] != models_raw["enabled_models"]:
            LOGGER.warning(
                "Conflicting enabled_models in queue vs models; using queue.enabled_models | novel=%s",
                novel_id,
            )
    if "model_configs" not in merged_queue_raw and isinstance(models_raw.get("model_configs"), dict):
        merged_queue_raw["model_configs"] = models_raw["model_configs"]
    elif "model_configs" in merged_queue_raw and isinstance(models_raw.get("model_configs"), dict):
        if merged_queue_raw["model_configs"] != models_raw["model_configs"]:
            LOGGER.warning(
                "Conflicting model_configs in queue vs models; using queue.model_configs | novel=%s",
                novel_id,
            )
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
    # Build TranslationConfig from translation(common) + translation.chapter(specific).
    translation_common_raw = {key: value for key, value in translation_root.items() if key not in {"chapter", "captions"}}
    translation_raw = _deep_merge(translation_common_raw, chapter_section)
    # Backward/forward compatible translation model key.
    # Default comes from the shared model pool: models.enabled_models[0].
    # Keep supporting historical nested keys in translation config.
    model_aliases = [
        (
            "models.enabled_models[0]",
            (models_raw.get("enabled_models") or [None])[0]
            if isinstance(models_raw.get("enabled_models"), list)
            else None,
        ),
        ("translation.model", translation_root.get("model")),
        ("translation.translate_model", translation_root.get("translate_model")),
        ("translation.translation_model", translation_root.get("translation_model")),
        ("translation.chapter.model", chapter_section.get("model")),
    ]
    model_values = [(k, v) for k, v in model_aliases if isinstance(v, str) and v.strip()]
    if not model_values:
        raise KeyError(
            'Missing translation model (expected one of: "models.enabled_models[0]", "translation.model")'
        )
    preferred_key, preferred_value = model_values[0]
    default_caption_model = preferred_value
    for other_key, other_value in model_values[1:]:
        if other_value != preferred_value:
            LOGGER.warning(
                'Conflicting translation model keys: %s="%s" vs %s="%s" (using %s)',
                preferred_key,
                preferred_value,
                other_key,
                other_value,
                preferred_key,
            )
            break
    translation_raw["model"] = preferred_value
    translation_raw.pop("translation_model", None)
    translation_raw.pop("translate_model", None)
    if "provider" not in translation_raw or not str(translation_raw.get("provider", "")).strip():
        translation_raw["provider"] = str(models_raw.get("provider", "")).strip() or "gemini_http"

    model_pool_cfg = models_raw.get("model_configs", {}) if isinstance(models_raw.get("model_configs"), dict) else {}
    resolved_model_cfg = model_pool_cfg.get(translation_raw["model"], {}) if isinstance(model_pool_cfg, dict) else {}
    if isinstance(resolved_model_cfg, dict):
        if ("chunk_max_len" not in translation_raw) or int(translation_raw.get("chunk_max_len") or 0) <= 0:
            if "chunk_max_len" in resolved_model_cfg and int(resolved_model_cfg.get("chunk_max_len") or 0) > 0:
                translation_raw["chunk_max_len"] = int(resolved_model_cfg["chunk_max_len"])
        if "chunk_sleep_seconds" not in translation_raw:
            if "chunk_sleep_seconds" in resolved_model_cfg:
                translation_raw["chunk_sleep_seconds"] = float(resolved_model_cfg["chunk_sleep_seconds"])
            else:
                translation_raw["chunk_sleep_seconds"] = 0.1
    if ("chunk_max_len" not in translation_raw) or int(translation_raw.get("chunk_max_len") or 0) <= 0:
        raise KeyError(
            f'Missing translation.chunk_max_len for model "{translation_raw["model"]}" (set in models.model_configs or translation.chunk_max_len)'
        )
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
        os.environ.get("NOVEL_TTS_TRANSLATE_MODEL", os.environ.get("GEMINI_MODEL", translation_raw["model"])),
    )
    translation_raw["repair_model"] = os.environ.get(
        "REPAIR_MODEL",
        str(translation_root.get("repair_model", "")).strip()
        or translation_raw.get("repair_model")
        or str(models_raw.get("repair_model", "")).strip(),
    )
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

    # Build CaptionConfig from translation.captions, defaulting to the same provider+model as chapter translation.
    captions_raw = dict(captions_section)
    if "provider" not in captions_raw or not str(captions_raw.get("provider", "")).strip():
        captions_raw["provider"] = translation_raw["provider"]
    if "model" not in captions_raw or not str(captions_raw.get("model", "")).strip():
        # In queue mode, GEMINI_MODEL is used to select the chapter translation model.
        # Captions should remain stable unless explicitly overridden.
        captions_raw["model"] = default_caption_model
    captions_model_override = os.environ.get("NOVEL_TTS_CAPTIONS_MODEL", os.environ.get("CAPTIONS_MODEL", "")).strip()
    if captions_model_override:
        captions_raw["model"] = captions_model_override

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
        captions=CaptionConfig(**captions_raw),
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
