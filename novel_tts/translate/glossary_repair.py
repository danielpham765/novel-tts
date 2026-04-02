from __future__ import annotations

import hashlib
import json
import os
import re
from pathlib import Path

from novel_tts.common.logging import get_logger
from novel_tts.config.models import NovelConfig

LOGGER = get_logger(__name__)

DEFAULT_CHUNK_SIZE = 100
_REPAIR_BASE = Path(__file__).resolve().parents[2] / "configs" / "glossaries" / ".repair"


def _repair_dir(config: NovelConfig) -> Path:
    return _REPAIR_BASE / config.novel_id


def get_repair_chunk_output_path(config: NovelConfig, chunk_index: int) -> Path:
    return _repair_dir(config) / f"chunk-{chunk_index:04d}.json"


def _meta_path(config: NovelConfig) -> Path:
    return _repair_dir(config) / "meta.json"


def _resolve_glossary_path(config: NovelConfig) -> Path:
    root = Path(__file__).resolve().parents[2]
    gfile = (config.translation.glossary_file or "").strip()
    if gfile:
        p = Path(gfile)
        return p if p.is_absolute() else (root / p)
    return root / "configs" / "glossaries" / f"{config.novel_id}.json"


def _glossary_hash(entries: dict[str, str]) -> str:
    canonical = json.dumps(sorted(entries.keys()), ensure_ascii=False)
    return hashlib.sha256(canonical.encode()).hexdigest()[:16]


def load_repair_meta(config: NovelConfig) -> dict | None:
    path = _meta_path(config)
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def init_repair(config: NovelConfig, chunk_size: int = DEFAULT_CHUNK_SIZE, *, force: bool = False) -> dict:
    """
    Chunk the glossary and write meta.json.
    Returns the meta dict with total_chunks.

    If force=True, removes existing chunk output files so all chunks are re-processed.
    Otherwise, existing chunk outputs are preserved (resume-friendly).
    """
    gpath = _resolve_glossary_path(config)
    if not gpath.exists():
        raise FileNotFoundError(f"Glossary not found: {gpath}")

    entries: dict[str, str] = json.loads(gpath.read_text(encoding="utf-8"))
    total_entries = len(entries)
    total_chunks = max(1, (total_entries + chunk_size - 1) // chunk_size)

    meta = {
        "total_chunks": total_chunks,
        "chunk_size": chunk_size,
        "total_entries": total_entries,
        "glossary_hash": _glossary_hash(entries),
        "glossary_path": str(gpath),
    }

    repair_dir = _repair_dir(config)
    repair_dir.mkdir(parents=True, exist_ok=True)

    if force:
        for p in repair_dir.glob("chunk-*.json"):
            p.unlink(missing_ok=True)

    _meta_path(config).write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    LOGGER.info(
        "Glossary repair initialized | novel=%s entries=%d chunks=%d chunk_size=%d",
        config.novel_id,
        total_entries,
        total_chunks,
        chunk_size,
    )
    return meta


def get_chunk_entries(config: NovelConfig, chunk_index: int) -> dict[str, str]:
    """Return the source glossary entries for chunk N (sorted by key)."""
    meta = load_repair_meta(config)
    if meta is None:
        raise RuntimeError("Repair not initialized — run 'glossary repair <novel_id>' first")

    gpath = Path(meta["glossary_path"])
    entries: dict[str, str] = json.loads(gpath.read_text(encoding="utf-8"))

    # Validate hash so we catch glossary changes mid-repair
    current_hash = _glossary_hash(entries)
    if current_hash != meta["glossary_hash"]:
        LOGGER.warning(
            "Glossary changed since repair was initialized (hash mismatch) | novel=%s",
            config.novel_id,
        )

    chunk_size = meta["chunk_size"]
    sorted_items = sorted(entries.items())
    start = chunk_index * chunk_size
    return dict(sorted_items[start : start + chunk_size])


def repair_glossary_chunk(config: NovelConfig, chunk_index: int) -> dict[str, str]:
    """Call the LLM to repair chunk_index. Returns the repaired entries."""
    from .model import resolve_translation_model
    from .novel import strip_model_wrappers
    from .prompts import render_prompt
    from .providers import get_translation_provider

    entries = get_chunk_entries(config, chunk_index)
    if not entries:
        LOGGER.info("Chunk %d is empty, skipping LLM call", chunk_index)
        return {}

    provider = get_translation_provider(config.models.provider, config=config)
    model = resolve_translation_model(config)

    glossary_model = os.environ.get("GLOSSARY_MODEL", "").strip()
    if not glossary_model or glossary_model.lower() in {"none", "null"}:
        glossary_model = model

    glossary_json = json.dumps(entries, ensure_ascii=False, indent=2)
    base_rules = (config.translation.base_rules or "").strip()
    if base_rules:
        base_rules = base_rules.rstrip() + "\n\n"

    prompt = render_prompt(
        "glossary-repair.txt",
        base_rules=base_rules,
        glossary_json=glossary_json,
    )

    LOGGER.info(
        "Repairing glossary chunk %d/%d (%d entries) | novel=%s model=%s",
        chunk_index,
        (load_repair_meta(config) or {}).get("total_chunks", "?"),
        len(entries),
        config.novel_id,
        glossary_model,
    )

    raw = provider.generate(glossary_model, prompt)
    raw = strip_model_wrappers(raw)

    # Parse JSON — try strict parse first, then regex extraction as fallback
    try:
        repaired: dict[str, str] = json.loads(raw)
    except json.JSONDecodeError:
        m = re.search(r"\{.*\}", raw, re.S)
        if m:
            try:
                repaired = json.loads(m.group(0))
            except json.JSONDecodeError:
                LOGGER.error(
                    "Failed to parse LLM response for chunk %d | novel=%s | response=%r",
                    chunk_index,
                    config.novel_id,
                    raw[:500],
                )
                raise
        else:
            LOGGER.error(
                "No JSON object found in LLM response for chunk %d | novel=%s | response=%r",
                chunk_index,
                config.novel_id,
                raw[:500],
            )
            raise

    if not isinstance(repaired, dict):
        raise ValueError(f"Expected JSON object, got {type(repaired).__name__}")

    repaired = {str(k): str(v) for k, v in repaired.items()}
    LOGGER.info(
        "Chunk %d repaired: %d/%d entries kept | novel=%s",
        chunk_index,
        len(repaired),
        len(entries),
        config.novel_id,
    )
    return repaired


def save_chunk_result(config: NovelConfig, chunk_index: int, entries: dict[str, str]) -> Path:
    out_path = get_repair_chunk_output_path(config, chunk_index)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(entries, ensure_ascii=False, indent=2), encoding="utf-8")
    return out_path


def merge_repair_results(config: NovelConfig, *, dry_run: bool = False) -> tuple[Path, dict[str, str]]:
    """
    Merge all chunk outputs into a single combined glossary and write it back.
    A backup of the original is saved as <glossary>.pre-repair.json.
    Returns (glossary_path, merged_entries).
    """
    meta = load_repair_meta(config)
    if meta is None:
        raise RuntimeError("No repair meta found. Run 'glossary repair <novel_id>' first.")

    total_chunks = meta["total_chunks"]
    missing = [i for i in range(total_chunks) if not get_repair_chunk_output_path(config, i).exists()]
    if missing:
        noun = "chunk" if len(missing) == 1 else "chunks"
        raise RuntimeError(
            f"{len(missing)}/{total_chunks} {noun} not yet repaired "
            f"(indices: {missing[:10]}{'...' if len(missing) > 10 else ''}). "
            "Wait for all queue jobs to complete."
        )

    merged: dict[str, str] = {}
    for i in range(total_chunks):
        chunk_data: dict[str, str] = json.loads(
            get_repair_chunk_output_path(config, i).read_text(encoding="utf-8")
        )
        merged.update(chunk_data)

    gpath = Path(meta["glossary_path"])
    if not dry_run:
        if gpath.exists():
            backup_path = gpath.with_suffix(".pre-repair.json")
            backup_path.write_text(gpath.read_text(encoding="utf-8"), encoding="utf-8")
            LOGGER.info("Backed up original glossary to %s", backup_path)
        # Write sorted for deterministic diffs
        sorted_merged = dict(sorted(merged.items()))
        gpath.write_text(json.dumps(sorted_merged, ensure_ascii=False, indent=2), encoding="utf-8")
        LOGGER.info(
            "Merged %d repaired entries into %s | novel=%s",
            len(merged),
            gpath.name,
            config.novel_id,
        )
        return gpath, sorted_merged

    return gpath, dict(sorted(merged.items()))


def get_repair_status(config: NovelConfig) -> dict:
    meta = load_repair_meta(config)
    if meta is None:
        return {"initialized": False}
    total_chunks = meta["total_chunks"]
    done = sum(1 for i in range(total_chunks) if get_repair_chunk_output_path(config, i).exists())
    return {
        "initialized": True,
        "total_chunks": total_chunks,
        "done_chunks": done,
        "pending_chunks": total_chunks - done,
        "chunk_size": meta["chunk_size"],
        "total_entries": meta["total_entries"],
        "glossary_path": meta["glossary_path"],
    }


def enqueue_glossary_repair_jobs(config: NovelConfig, meta: dict, client) -> int:
    """Push all pending repair-glossary chunk jobs into the Redis queue. Returns count enqueued."""
    from novel_tts.queue.translation_queue import (
        _key,
        _has_exhausted_retries,
        _repair_glossary_job_id,
    )

    total_chunks = meta["total_chunks"]
    enqueued = 0
    for chunk_index in range(total_chunks):
        if get_repair_chunk_output_path(config, chunk_index).exists():
            continue  # already done
        job_id = _repair_glossary_job_id(chunk_index)
        if client.hexists(_key(config, "inflight"), job_id):
            continue  # already running
        if _has_exhausted_retries(config, client, job_id):
            continue
        if client.sadd(_key(config, "queued"), job_id):
            client.rpush(_key(config, "pending"), job_id)
            enqueued += 1
    return enqueued
