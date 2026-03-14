from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from novel_tts.config.loader import load_novel_config
from novel_tts.config.models import (
    BrowserDebugConfig,
    CaptionConfig,
    CrawlConfig,
    NovelConfig,
    QueueConfig,
    SourceConfig,
    StorageConfig,
    TranslationConfig,
    TtsConfig,
    VideoConfig,
    VisualConfig,
)
from novel_tts.crawl.registry import build_default_registry
from novel_tts.crawl.service import _load_failure_manifest, _write_batch
from novel_tts.crawl.strategies import build_strategy_chain


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("novel_id")
    return parser.parse_args()


def _load_existing_chapters(origin_dir: Path) -> set[int]:
    chapter_numbers: set[int] = set()
    for path in sorted(origin_dir.glob("chuong_*.txt")):
        text = path.read_text(encoding="utf-8", errors="ignore")
        for match in re.finditer(r"^第(\d+)章", text, flags=re.M):
            chapter_numbers.add(int(match.group(1)))
    return chapter_numbers


def _extract_neighbor_urls(html: str, current_url: str) -> tuple[str | None, str | None]:
    soup = BeautifulSoup(html, "html.parser")
    prev_url = None
    next_url = None
    for link in soup.select("a[href]"):
        href = link.get("href", "").strip()
        text = " ".join(link.get_text(" ", strip=True).split())
        if not href:
            continue
        abs_url = urljoin(current_url, href)
        if "上一章" in text and not prev_url:
            prev_url = abs_url
        if "下一章" in text and not next_url:
            next_url = abs_url
    return prev_url, next_url


def _root_dir() -> Path:
    return Path(__file__).resolve().parents[1]


def _load_current_schema_config(novel_id: str) -> NovelConfig:
    root = _root_dir()
    path = root / "configs" / "novels" / f"{novel_id}.json"
    raw = json.loads(path.read_text(encoding="utf-8"))
    storage = StorageConfig(
        root=root,
        input_dir=root / raw["storage"]["input_dir"],
        output_dir=root / raw["storage"]["output_dir"],
        image_dir=root / raw["storage"]["image_dir"],
        logs_dir=root / raw["storage"].get("logs_dir", ".logs"),
        tmp_dir=root / raw["storage"].get("tmp_dir", "tmp"),
    )
    crawl = CrawlConfig(**raw["crawl"])
    browser_debug = BrowserDebugConfig(**raw.get("browser_debug", {}))
    source_id = crawl.site_id
    source = SourceConfig(
        source_id=source_id,
        resolver_id=source_id,
        crawl=crawl,
        browser_debug=browser_debug,
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
        crawl=crawl,
        browser_debug=browser_debug,
        translation=TranslationConfig(**raw["translation"]),
        captions=CaptionConfig(**raw["captions"]),
        queue=QueueConfig(**raw["queue"]),
        tts=TtsConfig(**raw["tts"]),
        visual=VisualConfig(**raw["visual"]),
        video=VideoConfig(**raw["video"]),
    )


def _load_config(novel_id: str) -> NovelConfig:
    try:
        return load_novel_config(novel_id)
    except KeyError as exc:
        if exc.args != ("source_id",):
            raise
        return _load_current_schema_config(novel_id)


def main() -> int:
    args = _parse_args()
    config = _load_config(args.novel_id)
    strategy_chain = build_strategy_chain(config.crawl, config.browser_debug)
    resolver = build_default_registry().get(config.crawl.site_id)
    manifest = _load_failure_manifest(config)
    existing = _load_existing_chapters(config.storage.origin_dir)

    failures = manifest.get("failures", {})
    missing_numbers = sorted(
        int(chapter_number)
        for chapter_number, item in failures.items()
        if item.get("reason") == "missing_entry" and int(chapter_number) not in existing
    )
    if not missing_numbers:
        print("No unresolved missing chapters to fill.")
        return 0

    directory_result = strategy_chain.fetch(config.crawl.directory_url, config.crawl.request_timeout_seconds)
    entries = resolver.parse_directory(directory_result.html, directory_result.final_url)
    pending = resolver.find_directory_page_urls(directory_result.html, directory_result.final_url)
    seen = {directory_result.final_url}
    while pending and max(entries) < max(missing_numbers):
        page_url = pending.pop(0)
        if page_url in seen:
            continue
        seen.add(page_url)
        page_result = strategy_chain.fetch(page_url, config.crawl.request_timeout_seconds)
        entries.update(resolver.parse_directory(page_result.html, page_result.final_url))
        for extra_url in resolver.find_directory_page_urls(page_result.html, page_result.final_url):
            if extra_url not in seen and extra_url not in pending:
                pending.append(extra_url)

    outputs: list[Path] = []
    unresolved: list[int] = []
    for chapter_number in missing_numbers:
        prev_entry = entries.get(chapter_number - 1)
        next_entry = entries.get(chapter_number + 1)
        candidate_urls: list[str] = []
        if prev_entry:
            try:
                prev_result = strategy_chain.fetch(prev_entry.url, config.crawl.request_timeout_seconds)
            except Exception:
                prev_result = None
            if prev_result is not None:
                _, next_url = _extract_neighbor_urls(prev_result.html, prev_result.final_url)
                if next_url:
                    candidate_urls.append(next_url)
        if next_entry:
            try:
                next_result = strategy_chain.fetch(next_entry.url, config.crawl.request_timeout_seconds)
            except Exception:
                next_result = None
            if next_result is not None:
                prev_url, _ = _extract_neighbor_urls(next_result.html, next_result.final_url)
                if prev_url:
                    candidate_urls.append(prev_url)

        filled = False
        for candidate_url in dict.fromkeys(candidate_urls):
            try:
                result = strategy_chain.fetch(candidate_url, config.crawl.request_timeout_seconds)
                parsed = resolver.parse_chapter(result.html, chapter_number, f"第{chapter_number}章")
            except Exception:
                continue
            if parsed.chapter_number != chapter_number or not parsed.content.strip():
                continue
            output = _write_batch(
                config.storage.origin_dir,
                chapter_number,
                chapter_number,
                [f"{parsed.title}\n\n{parsed.content}".strip()],
            )
            outputs.append(output)
            filled = True
            break
        if not filled:
            unresolved.append(chapter_number)

    print(
        json.dumps(
            {
                "filled": [path.name for path in outputs],
                "unresolved": unresolved,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
