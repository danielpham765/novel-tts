from __future__ import annotations

import argparse
import logging
import os
import re
import sys
from pathlib import Path

from novel_tts.common.logging import (
    configure_logging,
    get_logger,
    get_novel_log_path,
    install_exception_logging,
)
from novel_tts.common.text import parse_range
from novel_tts.config import load_novel_config, NovelConfig

LOGGER = get_logger(__name__)


def get_translated_ranges(config: NovelConfig, search_start: int, search_end: int) -> list[tuple[int, int, str]]:
    ranges = []
    pattern = re.compile(r"^chuong_(\d+)-(\d+)\.txt$")
    config_dir = config.storage.translated_dir
    
    if not config_dir.exists():
        return [(search_start, search_end, f"chuong_{search_start}-{search_end}")]
        
    for file_path in config_dir.iterdir():
        if not file_path.is_file():
            continue
        match = pattern.match(file_path.name)
        if match:
            start = int(match.group(1))
            end = int(match.group(2))
            
            overlap_start = max(start, search_start)
            overlap_end = min(end, search_end)
            
            if overlap_start <= overlap_end:
                ranges.append((overlap_start, overlap_end, f"chuong_{start}-{end}"))
                
    if not ranges:
        return [(search_start, search_end, f"chuong_{search_start}-{search_end}")]
        
    ranges.sort(key=lambda x: x[0])
    return ranges


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="novel-tts")
    parser.add_argument("--log-file")
    subparsers = parser.add_subparsers(dest="command", required=True)

    crawl_parser = subparsers.add_parser("crawl")
    crawl_sub = crawl_parser.add_subparsers(dest="crawl_command")
    crawl_run_parser = crawl_sub.add_parser("run")
    crawl_run_parser.add_argument("novel_id")
    crawl_run_parser.add_argument("--from", dest="from_chapter", type=int)
    crawl_run_parser.add_argument("--to", dest="to_chapter", type=int)
    crawl_run_parser.add_argument("--range")
    crawl_run_parser.add_argument("--dir-url")
    crawl_verify_parser = crawl_sub.add_parser("verify")
    crawl_verify_parser.add_argument("novel_id")
    crawl_verify_parser.add_argument("--from", dest="from_chapter", type=int)
    crawl_verify_parser.add_argument("--to", dest="to_chapter", type=int)
    crawl_verify_parser.add_argument("--range")
    crawl_verify_parser.add_argument("--file", action="append", default=[])

    translate_parser = subparsers.add_parser("translate")
    translate_sub = translate_parser.add_subparsers(dest="translate_command", required=True)
    translate_novel_parser = translate_sub.add_parser("novel")
    translate_novel_parser.add_argument("novel_id")
    translate_novel_parser.add_argument("--force", action="store_true")
    translate_novel_parser.add_argument("--file", action="append", default=[])
    translate_chapter_parser = translate_sub.add_parser("chapter")
    translate_chapter_parser.add_argument("novel_id")
    translate_chapter_parser.add_argument("--file", required=True)
    translate_chapter_parser.add_argument("--chapter", required=True)
    translate_chapter_parser.add_argument("--force", action="store_true")
    translate_polish_parser = translate_sub.add_parser("polish")
    translate_polish_parser.add_argument("novel_id")
    translate_polish_parser.add_argument("--file", action="append", default=[])

    translate_captions_parser = translate_sub.add_parser("captions")
    translate_captions_parser.add_argument("novel_id")

    queue_parser = subparsers.add_parser("queue")
    queue_sub = queue_parser.add_subparsers(dest="queue_command", required=True)
    queue_supervisor_parser = queue_sub.add_parser("supervisor")
    queue_supervisor_parser.add_argument("novel_id")
    queue_monitor_parser = queue_sub.add_parser("monitor")
    queue_monitor_parser.add_argument("novel_id")
    queue_ps_parser = queue_sub.add_parser("ps")
    queue_ps_parser.add_argument("novel_id")
    queue_ps_parser.add_argument(
        "--all",
        action="store_true",
        help="Include all roles (including verbose subprocess roles)",
    )
    # Backward-compatible alias (may be removed later).
    queue_ps_parser.add_argument(
        "--show-translate",
        action="store_true",
        help="Alias for --all (include translate-chapter subprocesses)",
    )

    queue_ps_all_parser = queue_sub.add_parser("ps-all")
    queue_ps_all_parser.add_argument(
        "--all",
        action="store_true",
        help="Include all roles (including verbose subprocess roles)",
    )
    # Backward-compatible alias (may be removed later).
    queue_ps_all_parser.add_argument(
        "--show-translate",
        action="store_true",
        help="Alias for --all (include translate-chapter subprocesses)",
    )
    queue_stop_parser = queue_sub.add_parser("stop")
    queue_stop_parser.add_argument("novel_id")
    queue_stop_parser.add_argument("--pid", type=int, help="PID of a specific queue process to stop")
    queue_stop_parser.add_argument(
        "--role",
        action="append",
        help=(
            "Role(s) of queue processes to stop. "
            "Can be specified multiple times (e.g. --role supervisor --role worker) "
            "or as a comma-separated list (e.g. --role supervisor,worker). "
            "Known roles include: supervisor, monitor, worker, translate-chapter."
        ),
    )
    queue_worker_parser = queue_sub.add_parser("worker")
    queue_worker_parser.add_argument("novel_id")
    queue_worker_parser.add_argument("--key-index", type=int, required=True)
    queue_worker_parser.add_argument("--model", required=True)
    queue_launch_parser = queue_sub.add_parser("launch")
    queue_launch_parser.add_argument("novel_id")
    queue_launch_parser.add_argument("--restart", action="store_true")

    tts_parser = subparsers.add_parser("tts")
    tts_parser.add_argument("novel_id")
    tts_parser.add_argument("--range", required=True)

    visual_parser = subparsers.add_parser("visual")
    visual_parser.add_argument("novel_id")
    visual_parser.add_argument("--range", required=True)

    video_parser = subparsers.add_parser("video")
    video_parser.add_argument("novel_id")
    video_parser.add_argument("--range", required=True)

    pipeline_parser = subparsers.add_parser("pipeline")
    pipeline_sub = pipeline_parser.add_subparsers(dest="pipeline_command", required=True)
    pipeline_run = pipeline_sub.add_parser("run")
    pipeline_run.add_argument("novel_id")
    pipeline_run.add_argument("--from", dest="from_chapter", type=int)
    pipeline_run.add_argument("--to", dest="to_chapter", type=int)
    pipeline_run.add_argument("--range")
    pipeline_run.add_argument("--skip-crawl", action="store_true")
    pipeline_run.add_argument("--skip-translate", action="store_true")
    pipeline_run.add_argument("--skip-captions", action="store_true")
    pipeline_run.add_argument("--skip-tts", action="store_true")
    pipeline_run.add_argument("--skip-visual", action="store_true")
    pipeline_run.add_argument("--skip-video", action="store_true")

    return parser


def _default_log_path(args) -> Path | None:
    if getattr(args, "log_file", None):
        return Path(args.log_file).expanduser().resolve()

    novel_id = getattr(args, "novel_id", None)
    if not novel_id:
        return None

    config = load_novel_config(novel_id)

    # Organize logs into per-command subdirectories under .logs/<novel_id>/...
    command = args.command
    log_name: str

    if command == "crawl":
        crawl_command = getattr(args, "crawl_command", None) or "run"
        log_name = f"crawl/{crawl_command}.log"
    elif command == "translate":
        translate_command = getattr(args, "translate_command", None) or "novel"
        log_name = f"translate/{translate_command}.log"
    elif command == "queue":
        queue_command = getattr(args, "queue_command", None) or "supervisor"
        if queue_command == "worker":
            safe_model = args.model.replace("-", "_")
            log_name = f"queue/workers/k{args.key_index}-{safe_model}.log"
        else:
            log_name = f"queue/{queue_command}.log"
    elif command == "tts":
        log_name = "tts/tts.log"
    elif command == "visual":
        log_name = "media/visual.log"
    elif command == "video":
        log_name = "media/video.log"
    elif command == "pipeline":
        pipeline_command = getattr(args, "pipeline_command", None) or "run"
        log_name = f"pipeline/{pipeline_command}.log"
    else:
        log_name = f"{command}.log"

    return get_novel_log_path(config.storage.logs_dir, novel_id, log_name)


def main(argv: list[str] | None = None) -> int:
    raw_argv = list(sys.argv[1:] if argv is None else argv)
    if len(raw_argv) >= 2 and raw_argv[0] == "crawl" and raw_argv[1] not in {"run", "verify", "-h", "--help"}:
        raw_argv.insert(1, "run")

    parser = _build_parser()
    args = parser.parse_args(raw_argv)
    log_path = _default_log_path(args)
    configure_logging(log_path)
    install_exception_logging(LOGGER)
    if log_path is not None:
        LOGGER.info("Logging to %s", log_path)

    try:
        if args.command == "crawl":
            from novel_tts.crawl import crawl_range, verify_crawled_content

            config = load_novel_config(args.novel_id)
            crawl_command = args.crawl_command or "run"
            if crawl_command == "run":
                if args.range:
                    start, end = parse_range(args.range)
                else:
                    start, end = args.from_chapter, args.to_chapter
                    if (start is None) or (end is None):
                        parser.error("crawl run requires --range or both --from and --to")
                outputs = crawl_range(config, start, end, args.dir_url)
                for output in outputs:
                    LOGGER.info("Crawled output: %s", output)
                return 0

            if crawl_command == "verify":
                if args.range:
                    start, end = parse_range(args.range)
                else:
                    start, end = args.from_chapter, args.to_chapter
                    if (start is None) ^ (end is None):
                        parser.error("crawl verify requires both --from and --to when not using --range")
                report = verify_crawled_content(
                    config,
                    from_chapter=start,
                    to_chapter=end,
                    filenames=args.file or None,
                )
                LOGGER.info(
                    "Crawl verify checked %s files and %s chapters",
                    len(report.checked_files),
                    len(report.checked_chapters),
                )
                if report.ok:
                    LOGGER.info("Crawl verify OK")
                    return 0
                for issue in report.issues:
                    location = f" [{issue.path}]" if issue.path is not None else ""
                    LOGGER.warning("Crawl verify issue | code=%s chapter=%s%s %s", issue.code, issue.chapter_number, location, issue.message)
                return 1

        if args.command == "translate":
            from novel_tts.translate import polish_translations, translate_captions, translate_novel
            from novel_tts.translate.novel import rebuild_translated_file, translate_chapter

            config = load_novel_config(args.novel_id)
            if args.translate_command == "novel":
                outputs = translate_novel(config, force=args.force, filenames=args.file or None)
                for output in outputs:
                    LOGGER.info("Translated file: %s", output)
                return 0
            if args.translate_command == "chapter":
                source_path = config.storage.origin_dir / args.file
                output = translate_chapter(config, source_path, args.chapter, force=args.force)
                rebuilt = rebuild_translated_file(config, source_path, require_complete=True)
                LOGGER.info("Translated chapter part: %s", output)
                if rebuilt is not None:
                    LOGGER.info("Rebuilt file: %s", rebuilt)
                return 0
            if args.translate_command == "polish":
                changed_parts, rebuilt_files = polish_translations(config, filenames=args.file or None)
                LOGGER.info("Polished translations | changed_parts=%s rebuilt_files=%s", changed_parts, rebuilt_files)
                return 0
            if args.translate_command == "captions":
                output = translate_captions(config)
                LOGGER.info("Translated captions: %s", output)
                return 0

        if args.command == "queue":
            from novel_tts.queue import (
                launch_queue_stack,
                list_all_queue_processes,
                list_queue_processes,
                run_status_monitor,
                run_supervisor,
                run_worker,
                stop_queue_processes,
            )

            novel_id = getattr(args, "novel_id", None)
            config = load_novel_config(novel_id) if novel_id else None
            if args.queue_command == "supervisor":
                if config is None:
                    parser.error("queue supervisor requires a novel_id")
                return run_supervisor(config)
            if args.queue_command == "monitor":
                if config is None:
                    parser.error("queue monitor requires a novel_id")
                return run_status_monitor(config)
            if args.queue_command == "ps":
                if config is None:
                    parser.error("queue ps requires a novel_id")
                include_all = bool(getattr(args, "all", False) or getattr(args, "show_translate", False))
                return list_queue_processes(config, include_all=include_all)
            if args.queue_command == "ps-all":
                include_all = bool(getattr(args, "all", False) or getattr(args, "show_translate", False))
                return list_all_queue_processes(include_all=include_all)
            if args.queue_command == "stop":
                if config is None:
                    parser.error("queue stop requires a novel_id")
                raw_roles = getattr(args, "role", None)
                roles: list[str] | None = None
                if raw_roles:
                    roles = []
                    for value in raw_roles:
                        for part in value.split(","):
                            part = part.strip()
                            if part:
                                roles.append(part)
                return stop_queue_processes(
                    config,
                    pid=getattr(args, "pid", None),
                    roles=roles,
                )
            if args.queue_command == "worker":
                if config is None:
                    parser.error("queue worker requires a novel_id")
                return run_worker(config, key_index=args.key_index, model=args.model)
            if args.queue_command == "launch":
                if config is None:
                    parser.error("queue launch requires a novel_id")
                return launch_queue_stack(config, restart=args.restart)

        if args.command == "tts":
            from novel_tts.tts import run_tts

            config = load_novel_config(args.novel_id)
            start, end = parse_range(args.range)
            for c_start, c_end, r_key in get_translated_ranges(config, start, end):
                LOGGER.info("Merged audio: %s", run_tts(config, c_start, c_end, r_key))
            return 0

        if args.command == "visual":
            from novel_tts.media import generate_visual

            config = load_novel_config(args.novel_id)
            start, end = parse_range(args.range)
            for c_start, c_end, _ in get_translated_ranges(config, start, end):
                visual, thumbnail = generate_visual(config, c_start, c_end)
                LOGGER.info("Visual video: %s", visual)
                LOGGER.info("Thumbnail: %s", thumbnail)
            return 0

        if args.command == "video":
            from novel_tts.media import create_video

            config = load_novel_config(args.novel_id)
            start, end = parse_range(args.range)
            for c_start, c_end, _ in get_translated_ranges(config, start, end):
                LOGGER.info("Video: %s", create_video(config, c_start, c_end))
            return 0

        if args.command == "pipeline":
            from novel_tts.crawl import crawl_range
            from novel_tts.media import create_video, generate_visual
            from novel_tts.translate import translate_captions, translate_novel
            from novel_tts.tts import run_tts

            config = load_novel_config(args.novel_id)
            if args.range:
                start, end = parse_range(args.range)
            else:
                if args.from_chapter is None or args.to_chapter is None:
                    parser.error("pipeline run requires --range or both --from and --to")
                start, end = args.from_chapter, args.to_chapter
            if not args.skip_crawl:
                crawl_range(config, start, end)
            if not args.skip_translate:
                translate_novel(config)
            if not args.skip_captions:
                try:
                    translate_captions(config)
                except FileNotFoundError:
                    LOGGER.warning("Caption source missing, skipping caption translation")
            if not args.skip_tts:
                for c_start, c_end, r_key in get_translated_ranges(config, start, end):
                    run_tts(config, c_start, c_end, r_key)
            if not args.skip_visual:
                for c_start, c_end, _ in get_translated_ranges(config, start, end):
                    generate_visual(config, c_start, c_end)
            if not args.skip_video:
                for c_start, c_end, _ in get_translated_ranges(config, start, end):
                    create_video(config, c_start, c_end)
            return 0

        parser.error("Unhandled command")
        return 2
    except Exception:
        LOGGER.exception("Command failed")
        return 1
