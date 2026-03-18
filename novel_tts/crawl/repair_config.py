from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import yaml


@dataclass(frozen=True)
class RepairCandidate:
    kind: str  # "url" | "inline"
    source_id: str = ""
    url: str = ""
    title: str = ""
    content: str = ""


@dataclass(frozen=True)
class ChapterRepairRule:
    chapter: int
    candidates: list[RepairCandidate] = field(default_factory=list)


@dataclass
class RepairConfig:
    version: int = 1
    generated_at: str = ""
    generated_from: list[str] = field(default_factory=list)
    index_gaps: list[int] = field(default_factory=list)
    placeholder_title_suffix: str = "略过"
    placeholder_content_zh: str = "本章内容与主线剧情无关。"
    dedupe_repeated_blocks: bool = True
    replacements: dict[int, ChapterRepairRule] = field(default_factory=dict)


def repair_config_path(input_dir: Path) -> Path:
    return input_dir / "repair_config.yaml"


def load_repair_config(path: Path) -> RepairConfig:
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    cfg = RepairConfig()
    cfg.version = int(raw.get("version", cfg.version))
    cfg.generated_at = str(raw.get("generated_at", cfg.generated_at) or "")
    cfg.generated_from = list(raw.get("generated_from", []) or [])
    cfg.index_gaps = sorted({int(x) for x in (raw.get("index_gaps", []) or [])})
    cfg.placeholder_title_suffix = str(raw.get("placeholder_title_suffix", cfg.placeholder_title_suffix) or "略过")
    cfg.placeholder_content_zh = str(raw.get("placeholder_content_zh", cfg.placeholder_content_zh) or cfg.placeholder_content_zh)
    cfg.dedupe_repeated_blocks = bool(raw.get("dedupe_repeated_blocks", cfg.dedupe_repeated_blocks))

    replacements: dict[int, ChapterRepairRule] = {}
    rep_raw = raw.get("replacements", []) or []
    # Preferred YAML shape: list of {chapter: N, candidates: [...]}
    if isinstance(rep_raw, list):
        for item in rep_raw:
            if not isinstance(item, dict):
                continue
            try:
                chapter = int(item.get("chapter"))
            except Exception:
                continue
            candidates: list[RepairCandidate] = []
            for cand in item.get("candidates", []) or []:
                if not isinstance(cand, dict):
                    continue
                kind = str(cand.get("kind") or "url")
                candidates.append(
                    RepairCandidate(
                        kind=kind,
                        source_id=str(cand.get("source_id") or ""),
                        url=str(cand.get("url") or ""),
                        title=str(cand.get("title") or ""),
                        content=str(cand.get("content") or ""),
                    )
                )
            replacements[chapter] = ChapterRepairRule(chapter=chapter, candidates=candidates)
    # Backward-compatible: mapping keyed by chapter (string/int) -> {candidates: [...]}
    elif isinstance(rep_raw, dict):
        for key, payload in rep_raw.items():
            try:
                chapter = int(key)
            except Exception:
                continue
            candidates: list[RepairCandidate] = []
            if isinstance(payload, dict):
                for cand in payload.get("candidates", []) or []:
                    if not isinstance(cand, dict):
                        continue
                    kind = str(cand.get("kind") or "url")
                    candidates.append(
                        RepairCandidate(
                            kind=kind,
                            source_id=str(cand.get("source_id") or ""),
                            url=str(cand.get("url") or ""),
                            title=str(cand.get("title") or ""),
                            content=str(cand.get("content") or ""),
                        )
                    )
            replacements[chapter] = ChapterRepairRule(chapter=chapter, candidates=candidates)
    cfg.replacements = replacements
    return cfg


def save_repair_config(path: Path, cfg: RepairConfig) -> None:
    replacements_list: list[dict[str, object]] = []
    for chapter, rule in sorted(cfg.replacements.items(), key=lambda item: item[0]):
        candidates: list[dict[str, object]] = []
        for cand in rule.candidates:
            item: dict[str, object] = {"kind": cand.kind}
            if cand.source_id:
                item["source_id"] = cand.source_id
            if cand.url:
                item["url"] = cand.url
            if cand.title:
                item["title"] = cand.title
            if cand.content:
                item["content"] = cand.content
            candidates.append(item)
        replacements_list.append({"chapter": int(chapter), "candidates": candidates})

    payload: dict[str, object] = {
        "version": cfg.version,
        "generated_at": cfg.generated_at,
        "generated_from": list(cfg.generated_from),
        "index_gaps": list(sorted({int(x) for x in cfg.index_gaps})),
        "placeholder_title_suffix": cfg.placeholder_title_suffix,
        "placeholder_content_zh": cfg.placeholder_content_zh,
        "dedupe_repeated_blocks": bool(cfg.dedupe_repeated_blocks),
        "replacements": replacements_list,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump(payload, allow_unicode=True, sort_keys=False, width=120),
        encoding="utf-8",
    )


def _parse_numbers(text: str) -> list[int]:
    nums = []
    for token in re.split(r"[^0-9]+", text.strip()):
        if not token:
            continue
        try:
            nums.append(int(token))
        except Exception:
            continue
    return nums


def _source_id_for_url(url: str) -> str:
    lower = (url or "").lower()
    if "m.1qxs.com" in lower or ".1qxs.com" in lower:
        return "1qxs"
    if "wa01.com" in lower:
        return "wa01"
    if "ttkan.co" in lower or "ttkan.com" in lower:
        return "ttkan"
    return ""


def _parse_missing_numbers_from_continuity(md: str) -> set[int]:
    missing: set[int] = set()
    for line in md.splitlines():
        if line.startswith("- Missing reported:"):
            missing.update(_parse_numbers(line))
    return missing


def _parse_replacements_from_garbage_research(md: str) -> dict[int, list[str]]:
    """
    Returns chapter -> list of replacement URLs (in priority order as they appear).
    """
    replacements: dict[int, list[str]] = {}
    current_chapter: int | None = None
    in_code_block = False
    for line in md.splitlines():
        if line.strip().startswith("```"):
            in_code_block = not in_code_block
            continue
        header_match = re.match(r"^###\s+Chapter\s+(\d+)\b", line.strip(), flags=re.I)
        if header_match:
            current_chapter = int(header_match.group(1))
            continue
        if current_chapter is None:
            continue
        if in_code_block:
            url_match = re.match(r"^https?://\S+", line.strip())
            if url_match:
                url = url_match.group(0).strip()
                replacements.setdefault(current_chapter, []).append(url)
    return replacements


def _parse_story_gap_rules_from_missing_research(md: str) -> dict[int, list[str]]:
    """
    Returns chapter -> list of candidate URLs parsed from the "Replacement links" section.
    """
    rules: dict[int, list[str]] = {}
    current: int | None = None
    for line in md.splitlines():
        match = re.match(r"^Chapter\s+(\d+)\b", line.strip(), flags=re.I)
        if match:
            current = int(match.group(1))
            continue
        if current is None:
            continue
        url_match = re.search(r"(https?://\S+)", line.strip())
        if url_match:
            rules.setdefault(current, []).append(url_match.group(1))
    return rules


def generate_repair_config_from_research(*, root: Path, novel_id: str, logs_dir: Path, input_dir: Path) -> RepairConfig:
    crawl_logs = logs_dir / novel_id / "crawl"
    garbage_path = crawl_logs / "garbage_chapter_reseach.md"
    missing_research_path = crawl_logs / "missing_chapter_reseach.md"
    continuity_path = crawl_logs / "missing_chapter_continuity.md"

    generated_from: list[str] = []
    garbage_md = garbage_path.read_text(encoding="utf-8") if garbage_path.exists() else ""
    if garbage_md:
        generated_from.append(str(garbage_path))
    missing_md = missing_research_path.read_text(encoding="utf-8") if missing_research_path.exists() else ""
    if missing_md:
        generated_from.append(str(missing_research_path))
    continuity_md = continuity_path.read_text(encoding="utf-8") if continuity_path.exists() else ""
    if continuity_md:
        generated_from.append(str(continuity_path))

    # Research files are the source of truth for "what to replace" and "what is a true story gap".
    garbage_replacements = _parse_replacements_from_garbage_research(garbage_md) if garbage_md else {}
    missing_replacements = _parse_story_gap_rules_from_missing_research(missing_md) if missing_md else {}

    # Continuity file is optional: if present, use it only to enumerate "missing reported" chapters.
    missing_numbers = _parse_missing_numbers_from_continuity(continuity_md) if continuity_md else set()

    # Story gaps: if missing research identifies explicit chapters to patch, treat them as non-index gaps.
    story_gap_chapters = set(missing_replacements.keys())
    index_gaps = sorted(missing_numbers - story_gap_chapters)

    replacements: dict[int, ChapterRepairRule] = {}

    # Garbage replacements are high priority: always replace those chapters if present.
    for chapter, urls in sorted(garbage_replacements.items()):
        candidates: list[RepairCandidate] = []
        for url in urls:
            source_id = _source_id_for_url(url)
            if not source_id:
                continue
            candidates.append(RepairCandidate(kind="url", source_id=source_id, url=url))
        if candidates:
            replacements[chapter] = ChapterRepairRule(chapter=chapter, candidates=candidates)

    # Missing story gaps: add candidate URLs (from missing research), appended after any existing rules.
    for chapter, urls in sorted(missing_replacements.items()):
        rule = replacements.get(chapter) or ChapterRepairRule(chapter=chapter, candidates=[])
        for url in urls:
            source_id = _source_id_for_url(url)
            if not source_id:
                continue
            rule.candidates.append(RepairCandidate(kind="url", source_id=source_id, url=url))
        replacements[chapter] = rule

    cfg = RepairConfig(
        version=1,
        generated_at=datetime.now(timezone.utc).isoformat(),
        generated_from=generated_from,
        index_gaps=index_gaps,
        replacements=replacements,
    )

    # Ensure the input dir exists (we will write repair.config there).
    input_dir.mkdir(parents=True, exist_ok=True)
    return cfg
