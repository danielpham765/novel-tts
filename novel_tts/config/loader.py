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
    ModelsConfig,
    NovelConfig,
    ProxyGatewayConfig,
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


def _clean_text(value) -> str:
    """
    Normalize config/env values that should be treated as optional strings.
    - None / null -> ""
    - "none"/"null" (case-insensitive) -> ""
    - otherwise -> stripped string
    """

    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    if text.lower() in {"none", "null"}:
        return ""
    return text


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


def _normalize_queue_config(queue_raw: dict, *, strict: bool = True) -> dict:
    normalized = dict(queue_raw)
    enabled_models = normalized.get("enabled_models")
    model_configs = {key: dict(value) for key, value in normalized.get("model_configs", {}).items()}

    legacy_worker_models = normalized.pop("worker_models", None)
    legacy_worker_counts = normalized.pop("model_worker_counts", {})
    legacy_rpm_limits = normalized.pop("model_rpm_limits", {})
    legacy_tpm_limits = normalized.pop("model_tpm_limits", {})
    legacy_chunk_max_len = normalized.pop("model_chunk_max_len", {})
    legacy_chunk_sleep_seconds = normalized.pop("model_chunk_sleep_seconds", {})

    enabled_models_effective = enabled_models if enabled_models is not None else legacy_worker_models
    if enabled_models_effective is None:
        enabled_models_effective = ["gemma-3-27b-it", "gemma-3-12b-it"] if strict else []

    for model in enabled_models_effective:
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

    missing = [model for model in enabled_models_effective if model not in model_configs]
    if missing and strict:
        raise ValueError("Queue config is missing model_configs for enabled_models: " + ", ".join(sorted(missing)))

    normalized["enabled_models"] = enabled_models_effective
    normalized["model_configs"] = model_configs
    return normalized


def _clean_bool(value) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    text = str(value).strip().lower()
    if not text:
        return False
    return text in {"1", "true", "yes", "on", "y"}


def _normalize_proxy_gateway_config(proxy_raw: dict) -> ProxyGatewayConfig:
    if proxy_raw is None:
        proxy_raw = {}
    if not isinstance(proxy_raw, dict):
        raise ValueError('Invalid app config "proxy_gateway" (expected object)')

    enabled = _clean_bool(proxy_raw.get("enabled"))
    base_url = _clean_text(proxy_raw.get("base_url")) or "http://localhost:8888"
    if enabled and not base_url:
        raise ValueError('proxy_gateway.enabled=true requires non-empty "proxy_gateway.base_url"')

    mode = _clean_text(proxy_raw.get("mode")) or "direct"
    mode = mode.lower()
    if mode not in {"direct", "socket"}:
        raise ValueError('Invalid "proxy_gateway.mode" (expected "direct" or "socket")')

    auto_discovery = _clean_bool(proxy_raw.get("auto_discovery", True))

    keys_per_proxy_raw = proxy_raw.get("keys_per_proxy", 3)
    try:
        keys_per_proxy = int(keys_per_proxy_raw)
    except Exception:
        keys_per_proxy = 0
    if keys_per_proxy < 1:
        raise ValueError('Invalid "proxy_gateway.keys_per_proxy" (must be >= 1)')

    proxies_raw = proxy_raw.get("proxies", [])
    proxies: list[str] = []
    if proxies_raw is None:
        proxies_raw = []
    if not isinstance(proxies_raw, list):
        raise ValueError('Invalid "proxy_gateway.proxies" (expected list)')
    for item in proxies_raw:
        text = _clean_text(item)
        if text:
            proxies.append(text)

    direct_strategy = _clean_text(proxy_raw.get("direct_run_strategy")) or "proxy_1"
    direct_strategy = direct_strategy.lower()
    if direct_strategy not in {"proxy_1", "gateway_rr"}:
        raise ValueError('Invalid "proxy_gateway.direct_run_strategy" (expected "proxy_1" or "gateway_rr")')

    return ProxyGatewayConfig(
        enabled=enabled,
        base_url=base_url,
        mode=mode,
        auto_discovery=auto_discovery,
        keys_per_proxy=keys_per_proxy,
        proxies=proxies,
        direct_run_strategy=direct_strategy,
    )


def load_novel_config(novel_id: str) -> NovelConfig:
    path = _config_path(novel_id)
    if not path.exists():
        raise FileNotFoundError(f"Novel config not found: {path}")
    raw = json.loads(path.read_text(encoding="utf-8"))
    app_raw = _load_app_config()
    models_raw = _deep_merge(app_raw.get("models", {}), raw.get("models", {}))
    merged_tts_raw = _deep_merge(app_raw.get("tts", {}) or {}, raw.get("tts", {}) or {})
    if not isinstance(merged_tts_raw, dict):
        raise ValueError('Invalid "tts" config (expected object)')
    if not _clean_text(merged_tts_raw.get("provider")) or not _clean_text(merged_tts_raw.get("voice")):
        raise KeyError('Missing tts config (expected "tts.provider" and "tts.voice")')
    # New schema (preferred):
    # - translation: common settings shared by chapter + captions (provider, glossary_file, line_token, replacements, etc.)
    # - translation.chapter: settings specific to translate chapter (chapter_regex, base_rules, etc.)
    # - translation.captions: settings specific to translate captions (input_file, output_file, etc.)
    #
    app_translation_root = app_raw.get("translation", {})
    if app_translation_root is None:
        app_translation_root = {}
    if not isinstance(app_translation_root, dict):
        raise ValueError('Invalid app config "translation" (expected object)')

    novel_translation_root = raw.get("translation")
    if novel_translation_root is None:
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

    forbidden_translation_keys = {
        "provider",
        "model",
        "chunk_max_len",
        "chunk_sleep_seconds",
        "repair_model",
        "glossary_model",
    }
    forbidden_captions_keys = {"provider", "model"}
    for key in sorted(forbidden_translation_keys.intersection(translation_root.keys())):
        raise ValueError(f'Deprecated config key "translation.{key}" (use "models" and worker default model)')
    for key in sorted(forbidden_translation_keys.intersection(chapter_section.keys())):
        raise ValueError(f'Deprecated config key "translation.chapter.{key}" (use "models" and worker default model)')
    for key in sorted(forbidden_captions_keys.intersection(captions_section.keys())):
        raise ValueError(
            f'Deprecated config key "translation.captions.{key}" (captions uses the worker default model)'
        )

    if "source_id" in raw:
        raise ValueError('Deprecated config key "source_id" (move to crawl.source_id)')
    crawl_override_raw = raw.get("crawl", {})
    if crawl_override_raw is None:
        crawl_override_raw = {}
    if not isinstance(crawl_override_raw, dict):
        raise ValueError('Invalid novel "crawl" config (expected object)')

    if "source_id" in crawl_override_raw:
        raise ValueError('Deprecated config key "crawl.source_id" (migrate to crawl.sources[0].source_id)')
    sources_raw = crawl_override_raw.get("sources", None)
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
    browser_debug_override_raw = raw.get("browser_debug", {})
    if browser_debug_override_raw is None:
        browser_debug_override_raw = {}
    if not isinstance(browser_debug_override_raw, dict):
        raise ValueError('Invalid novel "browser_debug" config (expected object)')

    source_id = ""
    source_raw: dict[str, object] = {}
    if isinstance(sources_raw, list) and sources_raw:
        primary_source_raw = sources_raw[0]
        if not isinstance(primary_source_raw, dict):
            raise ValueError('Invalid crawl.sources[0] (expected object)')
        source_id = _clean_text(primary_source_raw.get("source_id"))
        if not source_id:
            raise KeyError('Missing source id (expected "crawl.sources[0].source_id")')
        source_path = _source_config_path(source_id)
        if not source_path.exists():
            raise FileNotFoundError(f"Source config not found: {source_path}")
        source_raw = json.loads(source_path.read_text(encoding="utf-8"))
        crawl_override_flat = dict(primary_source_raw)
        crawl_override_flat.pop("source_id", None)
        merged_crawl_raw = _deep_merge(source_raw["crawl"], crawl_override_flat)
        merged_browser_debug_raw = _deep_merge(source_raw.get("browser_debug", {}), browser_debug_override_raw)
    else:
        crawl_override_flat = dict(crawl_override_raw)
        crawl_override_flat.pop("sources", None)
        merged_crawl_raw = dict(crawl_override_flat)
        merged_crawl_raw.setdefault("site_id", "")
        merged_browser_debug_raw = dict(browser_debug_override_raw)
    merged_queue_raw = _deep_merge(app_raw.get("queue", {}), raw.get("queue", {}))
    # Normalize legacy queue keys first, but allow model configs to be injected from models.* later.
    merged_queue_raw = _normalize_queue_config(merged_queue_raw, strict=False)
    proxy_gateway_raw = _deep_merge(app_raw.get("proxy_gateway", {}) or {}, raw.get("proxy_gateway", {}) or {})
    proxy_gateway_cfg = _normalize_proxy_gateway_config(proxy_gateway_raw)
    # Model pool settings are shared across queue and direct translate, so we support a top-level
    # "models" section in configs/app.yaml and per-novel overrides in configs/novels/*.json.
    # In configs/app.yaml and configs/novels/*.json, model pool settings live in the top-level "models" section.
    # The queue config can still carry legacy enabled_models/model_configs, but models.* is canonical.
    if isinstance(models_raw.get("enabled_models"), list) and models_raw["enabled_models"]:
        if (
            "enabled_models" in merged_queue_raw
            and merged_queue_raw["enabled_models"]
            and merged_queue_raw["enabled_models"] != models_raw["enabled_models"]
        ):
            LOGGER.warning(
                "Conflicting enabled_models in queue vs models; using models.enabled_models | novel=%s",
                novel_id,
            )
        merged_queue_raw["enabled_models"] = models_raw["enabled_models"]
    if isinstance(models_raw.get("model_configs"), dict) and models_raw["model_configs"]:
        if (
            "model_configs" in merged_queue_raw
            and merged_queue_raw["model_configs"]
            and merged_queue_raw["model_configs"] != models_raw["model_configs"]
        ):
            LOGGER.warning(
                "Conflicting model_configs in queue vs models; using models.model_configs | novel=%s",
                novel_id,
            )
        merged_queue_raw["model_configs"] = models_raw["model_configs"]

    # Apply shared model defaults to queue model configs (unless a per-model override exists).
    # Precedence: models.model_configs.<model> > models.* defaults.
    models_glossary_model = _clean_text(models_raw.get("glossary_model", ""))
    models_repair_model = _clean_text(models_raw.get("repair_model", ""))
    if models_glossary_model and isinstance(merged_queue_raw.get("model_configs"), dict):
        for _model_name, _cfg in merged_queue_raw["model_configs"].items():
            if not isinstance(_cfg, dict):
                continue
            if str(_cfg.get("glossary_model", "")).strip():
                continue
            _cfg["glossary_model"] = models_glossary_model
    if models_repair_model and isinstance(merged_queue_raw.get("model_configs"), dict):
        for _model_name, _cfg in merged_queue_raw["model_configs"].items():
            if not isinstance(_cfg, dict):
                continue
            if str(_cfg.get("repair_model", "")).strip():
                continue
            _cfg["repair_model"] = models_repair_model
    merged_queue_raw = _normalize_queue_config(merged_queue_raw, strict=True)
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

    models_provider = _clean_text(models_raw.get("provider")) or "gemini_http"
    models_enabled = merged_queue_raw.get("enabled_models", [])
    if (
        (not isinstance(models_enabled, list))
        or (not models_enabled)
        or (not all(isinstance(item, str) and item.strip() for item in models_enabled))
    ):
        raise KeyError('Missing models.enabled_models (non-empty list required)')
    models_repair_default = _clean_text(models_raw.get("repair_model"))
    models_glossary_default = _clean_text(models_raw.get("glossary_model"))

    model_pool_cfg = merged_queue_raw.get("model_configs", {})
    if not isinstance(model_pool_cfg, dict):
        model_pool_cfg = {}
    for model_name in models_enabled:
        cfg = model_pool_cfg.get(model_name)
        if not isinstance(cfg, dict):
            raise KeyError(f"Missing models.model_configs for enabled model: {model_name}")
        if int(cfg.get('chunk_max_len') or 0) <= 0:
            raise KeyError(f"Missing models.model_configs.{model_name}.chunk_max_len")
    # Build TranslationConfig from translation(common) + translation.chapter(specific).
    translation_common_raw = {key: value for key, value in translation_root.items() if key not in {"chapter", "captions"}}
    translation_raw = _deep_merge(translation_common_raw, chapter_section)

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
    if "REPAIR_MODE" in os.environ:
        translation_raw["repair_mode"] = os.environ["REPAIR_MODE"].strip().lower() in {"1", "true", "yes"}

    source = SourceConfig(
        source_id=source_id,
        resolver_id=str(source_raw.get("resolver_id", source_id)) if source_raw else "",
        crawl=CrawlConfig(**merged_crawl_raw),
        browser_debug=BrowserDebugConfig(**merged_browser_debug_raw),
    )

    # Build CaptionConfig from translation.captions.
    captions_raw = dict(captions_section)

    raw_model_configs = merged_queue_raw.pop("model_configs", {})
    queue_model_configs = {
        model: QueueModelConfig(**cfg) for model, cfg in raw_model_configs.items() if isinstance(cfg, dict)
    }
    models_cfg = ModelsConfig(
        provider=models_provider,
        enabled_models=list(models_enabled),
        repair_model=models_repair_default,
        glossary_model=models_glossary_default,
        model_configs=queue_model_configs,
    )

    visual_raw = raw.get("visual", {})
    if visual_raw is None:
        visual_raw = {}
    if not isinstance(visual_raw, dict):
        raise ValueError('Invalid novel "visual" config (expected object)')
    visual_raw.setdefault("background_video", "")
    video_raw = raw.get("video", {})
    if video_raw is None:
        video_raw = {}
    if not isinstance(video_raw, dict):
        raise ValueError('Invalid novel "video" config (expected object)')

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
        models=models_cfg,
        translation=TranslationConfig(**translation_raw),
        captions=CaptionConfig(**captions_raw),
        queue=QueueConfig(
            redis=RedisConfig(**merged_queue_raw.pop("redis", {})),
            model_configs=queue_model_configs,
            **merged_queue_raw,
        ),
        proxy_gateway=proxy_gateway_cfg,
        tts=TtsConfig(**merged_tts_raw),
        visual=VisualConfig(**visual_raw),
        video=VideoConfig(**video_raw),
    )
