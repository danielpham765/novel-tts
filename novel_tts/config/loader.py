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
    MediaConfig,
    MediaBatchConfig,
    MediaBatchRule,
    ModelsConfig,
    NovelConfig,
    PipelineConfig,
    PipelineWatchConfig,
    ProxyGatewayConfig,
    QueueConfig,
    QueueModelConfig,
    RedisConfig,
    SourceConfig,
    StorageConfig,
    TranslationConfig,
    TtsConfig,
    UploadConfig,
    UploadTikTokConfig,
    UploadYouTubeConfig,
    VideoConfig,
    VisualConfig,
)
from novel_tts.common.text import parse_range
from novel_tts.translate.glossary import sanitize_glossary_entries

LOGGER = logging.getLogger(__name__)


def _root_dir() -> Path:
    return Path(__file__).resolve().parents[2]


def _config_path(novel_id: str) -> Path:
    return _root_dir() / "configs" / "novels" / f"{novel_id}.yaml"


def _app_config_path() -> Path:
    return _root_dir() / "configs" / "app.yaml"


def _app_local_config_path() -> Path:
    return _root_dir() / "configs" / "app.local.yaml"


def _source_config_path(source_id: str) -> Path:
    return _root_dir() / "configs" / "sources" / f"{source_id}.json"


def _glossary_path(novel_id: str) -> Path:
    return _root_dir() / "configs" / "glossaries" / novel_id / "glossary.json"


def _auto_glossary_path(path: Path) -> Path:
    if path.suffix:
        return path.with_name(f"{path.stem}.auto{path.suffix}")
    return path.with_name(path.name + ".auto.json")


def _polish_replacement_path(name: str) -> Path:
    return _root_dir() / "configs" / "polish_replacement" / f"{name}.json"


def _load_string_map(path: Path, *, allow_missing: bool = False, label: str = "config") -> dict[str, str]:
    if not path.exists():
        if allow_missing:
            return {}
        raise FileNotFoundError(f"{label} not found: {path}")

    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Invalid {label} format: expected JSON object in {path}")

    cleaned: dict[str, str] = {}
    for key, value in payload.items():
        if not isinstance(key, str) or not isinstance(value, str):
            raise ValueError(f"Invalid {label} entry: expected string-to-string map in {path}")
        cleaned[key] = value
    return cleaned


def _load_polish_replacements(novel_id: str) -> dict[str, str]:
    common = _load_string_map(
        _polish_replacement_path("common"),
        allow_missing=True,
        label="polish replacement config",
    )
    by_novel = _load_string_map(
        _polish_replacement_path(novel_id),
        allow_missing=True,
        label=f"polish replacement config for {novel_id}",
    )
    return {**common, **by_novel}


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
    payload: dict = {}
    for path in (_app_config_path(), _app_local_config_path()):
        if not path.exists():
            continue
        raw = _load_yaml_object(path, label="app config")
        payload = _deep_merge(payload, raw)
    return payload


def _load_yaml_object(path: Path, *, label: str) -> dict:
    with path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}
    if not isinstance(payload, dict):
        raise ValueError(f"Invalid {label} format: {path}")
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


def _clean_string_list(value, *, field_name: str) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError(f'Invalid "{field_name}" config (expected list)')
    items: list[str] = []
    for index, item in enumerate(value):
        text = _clean_text(item)
        if not text:
            raise ValueError(f'Invalid "{field_name}[{index}]" config (expected non-empty string)')
        items.append(text)
    return items


def _normalize_upload_youtube_config(youtube_raw: dict) -> dict:
    normalized = dict(youtube_raw)
    project_raw = normalized.get("project", "rotate")
    project_text = _clean_text(project_raw) or "rotate"
    if project_text.lower() == "rotate":
        normalized["project"] = "rotate"
    else:
        try:
            project_index = int(project_text)
        except Exception as exc:
            raise ValueError('Invalid "upload.youtube.project" (expected "rotate" or positive integer)') from exc
        if project_index < 1:
            raise ValueError('Invalid "upload.youtube.project" (expected "rotate" or positive integer)')
        normalized["project"] = str(project_index)
    normalized["credentials_path"] = _clean_string_list(
        normalized.get("credentials_path", [".secrets/youtube/client_secrets.json"]),
        field_name="upload.youtube.credentials_path",
    )
    normalized["token_path"] = _clean_string_list(
        normalized.get("token_path", [".secrets/youtube/token.json"]),
        field_name="upload.youtube.token_path",
    )
    if not normalized["credentials_path"]:
        raise ValueError('Missing "upload.youtube.credentials_path" (non-empty list required)')
    if not normalized["token_path"]:
        raise ValueError('Missing "upload.youtube.token_path" (non-empty list required)')
    if len(normalized["credentials_path"]) != len(normalized["token_path"]):
        raise ValueError(
            'Invalid YouTube account config: "upload.youtube.credentials_path" and '
            '"upload.youtube.token_path" must have the same number of entries'
        )
    return normalized


def _normalize_pipeline_watch_config(watch_raw: dict) -> dict:
    normalized = dict(watch_raw)
    normalized["novels"] = _clean_string_list(
        normalized.get("novels", []),
        field_name="pipeline.watch.novels",
    )
    try:
        interval_seconds = float(normalized.get("interval_seconds", 300.0) or 300.0)
    except Exception as exc:
        raise ValueError('Invalid "pipeline.watch.interval_seconds" (expected number)') from exc
    if interval_seconds <= 0:
        raise ValueError('Invalid "pipeline.watch.interval_seconds" (must be > 0)')
    normalized["interval_seconds"] = interval_seconds

    upload_platform = _clean_text(normalized.get("upload_platform"))
    if upload_platform and upload_platform not in {"youtube", "tiktok"}:
        raise ValueError('Invalid "pipeline.watch.upload_platform" (expected "youtube", "tiktok", or empty)')
    normalized["upload_platform"] = upload_platform
    normalized["restart_queue"] = _clean_bool(normalized.get("restart_queue", False))

    bootstrap_value = normalized.get("bootstrap_from")
    bootstrap_raw = _clean_text(bootstrap_value)
    if bootstrap_value in {0, "0"} or not bootstrap_raw:
        normalized["bootstrap_from"] = 0
    else:
        try:
            bootstrap_from = int(bootstrap_raw)
        except Exception as exc:
            raise ValueError('Invalid "pipeline.watch.bootstrap_from" (expected positive integer or empty)') from exc
        if bootstrap_from < 1:
            raise ValueError('Invalid "pipeline.watch.bootstrap_from" (expected positive integer or empty)')
        normalized["bootstrap_from"] = bootstrap_from
    return normalized


def _normalize_media_batch_config(media_batch_raw: dict | None) -> dict:
    if media_batch_raw is None:
        media_batch_raw = {}
    if not isinstance(media_batch_raw, dict):
        raise ValueError('Invalid "media_batch" config (expected object)')

    normalized = dict(media_batch_raw)
    try:
        default_size = int(normalized.get("default_chapter_batch_size", 10) or 10)
    except Exception as exc:
        raise ValueError('Invalid "media_batch.default_chapter_batch_size" (expected integer >= 1)') from exc
    if default_size < 1:
        raise ValueError('Invalid "media_batch.default_chapter_batch_size" (must be >= 1)')
    normalized["default_chapter_batch_size"] = default_size

    overrides_raw = normalized.get("chapter_batch_overrides", []) or []
    if not isinstance(overrides_raw, list):
        raise ValueError('Invalid "media_batch.chapter_batch_overrides" (expected list)')

    normalized_overrides: list[dict[str, object]] = []
    resolved_ranges: list[tuple[int, int]] = []
    for index, item in enumerate(overrides_raw):
        if not isinstance(item, dict):
            raise ValueError(f'Invalid "media_batch.chapter_batch_overrides[{index}]" (expected object)')
        range_text = _clean_text(item.get("range"))
        if not range_text:
            raise ValueError(f'Missing "media_batch.chapter_batch_overrides[{index}].range"')
        try:
            start, end = parse_range(range_text)
        except Exception as exc:
            raise ValueError(
                f'Invalid "media_batch.chapter_batch_overrides[{index}].range" (expected "<start>-<end>")'
            ) from exc
        try:
            batch_size = int(item.get("chapter_batch_size", 0) or 0)
        except Exception as exc:
            raise ValueError(
                f'Invalid "media_batch.chapter_batch_overrides[{index}].chapter_batch_size" (expected integer >= 1)'
            ) from exc
        if batch_size < 1:
            raise ValueError(f'Invalid "media_batch.chapter_batch_overrides[{index}].chapter_batch_size" (must be >= 1)')
        for existing_start, existing_end in resolved_ranges:
            if start <= existing_end and end >= existing_start:
                raise ValueError(
                    'Invalid "media_batch.chapter_batch_overrides" (overlapping ranges are not allowed)'
                )
        resolved_ranges.append((start, end))
        normalized_overrides.append({"range": range_text, "chapter_batch_size": batch_size})

    normalized["chapter_batch_overrides"] = normalized_overrides
    return normalized


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


def _build_source_configs(
    *,
    sources_raw,
    crawl_override_raw: dict,
) -> list[SourceConfig]:
    source_configs: list[SourceConfig] = []
    if isinstance(sources_raw, list) and sources_raw:
        for index, source_item in enumerate(sources_raw):
            if not isinstance(source_item, dict):
                raise ValueError(f'Invalid crawl.sources[{index}] (expected object)')
            source_id = _clean_text(source_item.get("source_id"))
            if not source_id:
                raise KeyError(f'Missing source id (expected "crawl.sources[{index}].source_id")')
            source_path = _source_config_path(source_id)
            if not source_path.exists():
                raise FileNotFoundError(f"Source config not found: {source_path}")
            source_raw = json.loads(source_path.read_text(encoding="utf-8"))
            crawl_override_flat = dict(source_item)
            crawl_override_flat.pop("source_id", None)
            merged_crawl_raw = _deep_merge(source_raw["crawl"], crawl_override_flat)
            browser_debug_raw = merged_crawl_raw.pop("browser_debug", {})
            if browser_debug_raw is None:
                browser_debug_raw = {}
            if not isinstance(browser_debug_raw, dict):
                raise ValueError(f'Invalid crawl.sources[{index}].browser_debug config (expected object)')
            source_configs.append(
                SourceConfig(
                    source_id=source_id,
                    resolver_id=str(source_raw.get("resolver_id", source_id)),
                    crawl=CrawlConfig(**merged_crawl_raw, browser_debug=BrowserDebugConfig(**browser_debug_raw)),
                )
            )
        return source_configs

    crawl_override_flat = dict(crawl_override_raw)
    crawl_override_flat.pop("sources", None)
    browser_debug_raw = crawl_override_flat.pop("browser_debug", {})
    if browser_debug_raw is None:
        browser_debug_raw = {}
    if not isinstance(browser_debug_raw, dict):
        raise ValueError('Invalid novel "crawl.browser_debug" config (expected object)')
    merged_crawl_raw = dict(crawl_override_flat)
    merged_crawl_raw.setdefault("site_id", "")
    source_configs.append(
        SourceConfig(
            source_id="",
            resolver_id="",
            crawl=CrawlConfig(**merged_crawl_raw, browser_debug=BrowserDebugConfig(**browser_debug_raw)),
        )
    )
    return source_configs


def load_novel_source_configs(novel_id: str) -> list[SourceConfig]:
    path = _config_path(novel_id)
    if not path.exists():
        raise FileNotFoundError(f"Novel config not found: {path}")
    raw = _load_yaml_object(path, label="novel config")
    crawl_override_raw = raw.get("crawl", {})
    if crawl_override_raw is None:
        crawl_override_raw = {}
    if not isinstance(crawl_override_raw, dict):
        raise ValueError('Invalid novel "crawl" config (expected object)')
    if "source_id" in crawl_override_raw:
        raise ValueError('Deprecated config key "crawl.source_id" (migrate to crawl.sources[0].source_id)')
    sources_raw = crawl_override_raw.get("sources", None)
    if "browser_debug" in raw:
        raise ValueError('Deprecated config key "browser_debug" (move to "crawl.browser_debug")')
    return _build_source_configs(
        sources_raw=sources_raw,
        crawl_override_raw=crawl_override_raw,
    )


def load_novel_config(novel_id: str) -> NovelConfig:
    path = _config_path(novel_id)
    if not path.exists():
        raise FileNotFoundError(f"Novel config not found: {path}")
    raw = _load_yaml_object(path, label="novel config")
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
    if "browser_debug" in raw:
        raise ValueError('Deprecated config key "browser_debug" (move to "crawl.browser_debug")')

    all_source_configs = _build_source_configs(
        sources_raw=sources_raw,
        crawl_override_raw=crawl_override_raw,
    )
    primary_source_cfg = all_source_configs[0]
    source_id = primary_source_cfg.source_id
    merged_queue_raw = _deep_merge(app_raw.get("queue", {}), raw.get("queue", {}))
    # Normalize legacy queue keys first, but allow model configs to be injected from models.* later.
    merged_queue_raw = _normalize_queue_config(merged_queue_raw, strict=False)
    proxy_gateway_raw = _deep_merge(app_raw.get("proxy_gateway", {}) or {}, raw.get("proxy_gateway", {}) or {})
    proxy_gateway_cfg = _normalize_proxy_gateway_config(proxy_gateway_raw)
    # Model pool settings are shared across queue and direct translate, so we support a top-level
    # "models" section in configs/app.yaml and per-novel overrides in configs/novels/*.yaml.
    # In configs/app.yaml and configs/novels/*.yaml, model pool settings live in the top-level "models" section.
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
        glossary_clean, dropped_curated = sanitize_glossary_entries(glossary_raw, mode="runtime")
        if dropped_curated:
            LOGGER.info(
                "Ignored %s risky glossary entries while loading %s",
                len(dropped_curated),
                glossary_path.name,
            )
        translation_raw["glossary"] = glossary_clean
        translation_raw["glossary_file"] = str(glossary_path.relative_to(root))
    else:
        translation_raw.setdefault("glossary", {})
        translation_raw["glossary_file"] = glossary_file
    blocked_targets_raw = translation_raw.get("blocked_glossary_targets", []) or []
    if not isinstance(blocked_targets_raw, list):
        raise ValueError('Invalid "translation.blocked_glossary_targets" config (expected list)')
    translation_raw["blocked_glossary_targets"] = [str(item).strip() for item in blocked_targets_raw if str(item).strip()]
    translation_raw["polish_replacements"] = _load_polish_replacements(novel_id)
    if "REPAIR_MODE" in os.environ:
        translation_raw["repair_mode"] = os.environ["REPAIR_MODE"].strip().lower() in {"1", "true", "yes"}

    source = primary_source_cfg

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

    if "visual" in raw:
        raise ValueError('Deprecated config key "visual" (move to "media.visual")')
    if "video" in raw:
        raise ValueError('Deprecated config key "video" (move to "media.video")')
    if "media_batch" in raw:
        raise ValueError('Deprecated config key "media_batch" (move to "media.media_batch")')
    app_media_raw = app_raw.get("media", {}) or {}
    novel_media_raw = raw.get("media", {}) or {}
    media_raw = _deep_merge(app_media_raw, novel_media_raw)
    if media_raw is None:
        media_raw = {}
    if not isinstance(media_raw, dict):
        raise ValueError('Invalid "media" config (expected object)')
    visual_raw = media_raw.get("visual", {})
    if visual_raw is None:
        visual_raw = {}
    if not isinstance(visual_raw, dict):
        raise ValueError('Invalid "media.visual" config (expected object)')
    visual_raw.setdefault("background_video", "")
    visual_raw.setdefault("background_cover", "")
    media_batch_base = app_media_raw.get("media_batch", {}) or {}
    media_batch_override = novel_media_raw.get("media_batch", {}) or {}
    media_batch_raw = _deep_merge(media_batch_base, media_batch_override)
    if (
        isinstance(media_batch_base, dict)
        and isinstance(media_batch_override, dict)
        and ("chapter_batch_overrides" in media_batch_base or "chapter_batch_overrides" in media_batch_override)
    ):
        media_batch_raw["chapter_batch_overrides"] = [
            *(media_batch_base.get("chapter_batch_overrides", []) or []),
            *(media_batch_override.get("chapter_batch_overrides", []) or []),
        ]
    media_batch_normalized = _normalize_media_batch_config(media_batch_raw)
    media_batch_cfg = MediaBatchConfig(
        **{
            **media_batch_normalized,
            "chapter_batch_overrides": [
                MediaBatchRule(**item)
                for item in media_batch_normalized["chapter_batch_overrides"]
            ],
        }
    )
    video_raw = media_raw.get("video", {})
    if video_raw is None:
        video_raw = {}
    if not isinstance(video_raw, dict):
        raise ValueError('Invalid "media.video" config (expected object)')
    upload_raw = _deep_merge(app_raw.get("upload", {}) or {}, raw.get("upload", {}) or {})
    if upload_raw is None:
        upload_raw = {}
    if not isinstance(upload_raw, dict):
        raise ValueError('Invalid novel "upload" config (expected object)')
    youtube_raw = upload_raw.get("youtube", {}) or {}
    tiktok_raw = upload_raw.get("tiktok", {}) or {}
    if not isinstance(youtube_raw, dict):
        raise ValueError('Invalid "upload.youtube" config (expected object)')
    if not isinstance(tiktok_raw, dict):
        raise ValueError('Invalid "upload.tiktok" config (expected object)')
    youtube_raw = _normalize_upload_youtube_config(youtube_raw)
    default_platform = _clean_text(upload_raw.get("default_platform")) or "youtube"
    if default_platform not in {"youtube", "tiktok"}:
        raise ValueError('Invalid "upload.default_platform" (expected "youtube" or "tiktok")')
    upload_cfg = UploadConfig(
        default_platform=default_platform,
        youtube=UploadYouTubeConfig(**youtube_raw),
        tiktok=UploadTikTokConfig(**tiktok_raw),
    )
    pipeline_raw = _deep_merge(app_raw.get("pipeline", {}) or {}, raw.get("pipeline", {}) or {})
    if pipeline_raw is None:
        pipeline_raw = {}
    if not isinstance(pipeline_raw, dict):
        raise ValueError('Invalid "pipeline" config (expected object)')
    watch_raw = pipeline_raw.get("watch", {}) or {}
    if not isinstance(watch_raw, dict):
        raise ValueError('Invalid "pipeline.watch" config (expected object)')
    pipeline_cfg = PipelineConfig(
        watch=PipelineWatchConfig(**_normalize_pipeline_watch_config(watch_raw)),
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
        media=MediaConfig(
            visual=VisualConfig(**visual_raw),
            video=VideoConfig(**video_raw),
            media_batch=media_batch_cfg,
        ),
        upload=upload_cfg,
        pipeline=pipeline_cfg,
    )


def load_queue_config() -> QueueConfig:
    """Load the shared queue config from configs/app.yaml (queue + models sections).

    This is used by global queue commands (supervisor, worker, launch, stop)
    that are not tied to a specific novel.
    """
    app_raw = _load_app_config()

    queue_raw = dict(app_raw.get("queue", {}) or {})
    models_raw = dict(app_raw.get("models", {}) or {})

    # Merge enabled_models and model_configs from models section into queue config.
    if isinstance(models_raw.get("enabled_models"), list) and models_raw["enabled_models"]:
        queue_raw["enabled_models"] = models_raw["enabled_models"]
    if isinstance(models_raw.get("model_configs"), dict) and models_raw["model_configs"]:
        queue_raw["model_configs"] = models_raw["model_configs"]

    # Apply shared model defaults (repair_model, glossary_model) to model_configs.
    models_glossary_model = _clean_text(models_raw.get("glossary_model", ""))
    models_repair_model = _clean_text(models_raw.get("repair_model", ""))
    raw_model_configs = queue_raw.get("model_configs", {})
    if isinstance(raw_model_configs, dict):
        for _cfg in raw_model_configs.values():
            if not isinstance(_cfg, dict):
                continue
            if models_glossary_model and not str(_cfg.get("glossary_model", "")).strip():
                _cfg["glossary_model"] = models_glossary_model
            if models_repair_model and not str(_cfg.get("repair_model", "")).strip():
                _cfg["repair_model"] = models_repair_model

    normalized = _normalize_queue_config(queue_raw, strict=True)
    raw_model_configs = normalized.pop("model_configs", {})
    queue_model_configs = {
        model: QueueModelConfig(**cfg) for model, cfg in raw_model_configs.items() if isinstance(cfg, dict)
    }
    return QueueConfig(
        redis=RedisConfig(**normalized.pop("redis", {})),
        model_configs=queue_model_configs,
        **normalized,
    )


def load_proxy_gateway_config() -> ProxyGatewayConfig:
    """Load proxy gateway config from configs/app.yaml."""
    app_raw = _load_app_config()
    proxy_raw = dict(app_raw.get("proxy_gateway", {}) or {})
    return ProxyGatewayConfig(**proxy_raw)
