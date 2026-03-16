from __future__ import annotations

import argparse
import contextlib
import io
import logging
import os
import re
import select
import sys
import termios
import time
import tty
from datetime import datetime
from pathlib import Path

from novel_tts.common.logging import (
    configure_logging,
    get_logger,
    get_novel_log_path,
    install_exception_logging,
)
from novel_tts.common.text import parse_range
from novel_tts.common.errors import RateLimitExceededError
from novel_tts.config import load_novel_config, NovelConfig

LOGGER = get_logger(__name__)


@contextlib.contextmanager
def _stdin_cbreak_if_tty() -> bool:
    if not sys.stdin.isatty():
        yield False
        return
    fd = sys.stdin.fileno()
    old = termios.tcgetattr(fd)
    try:
        tty.setcbreak(fd)
        yield True
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, old)


def _read_stdin_byte_nonblocking() -> bytes | None:
    if not sys.stdin.isatty():
        return None
    try:
        r, _, _ = select.select([sys.stdin], [], [], 0)
    except (ValueError, OSError):
        return None
    if not r:
        return None
    try:
        return os.read(sys.stdin.fileno(), 1)
    except OSError:
        return None


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
    translate_polish_parser.add_argument("--range", help="Optional range of chapters, e.g. 101-500")

    translate_repair_parser = translate_sub.add_parser("repair")
    translate_repair_parser.add_argument("novel_id")
    translate_repair_parser.add_argument("--range", required=True, help="Chapter range to scan and enqueue for repair, e.g. 1401-1410")

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
    queue_ps_all_parser.add_argument(
        "-f",
        "--follow",
        action="store_true",
        help="Watch mode (refresh every 1s). Ctrl+P to pause/resume, Ctrl+C to stop.",
    )
    queue_reset_parser = queue_sub.add_parser("reset")
    queue_reset_parser.add_argument("novel_id")
    queue_reset_parser.add_argument(
        "--key",
        action="append",
        default=[],
        help=(
            "Key selector(s) to reset. Repeatable or comma-separated. "
            "Accepts kN (key index) or raw key (exact match in .secrets/gemini-keys.txt)."
        ),
    )
    queue_reset_parser.add_argument(
        "--model",
        action="append",
        default=[],
        help=(
            "Model(s) to reset. Repeatable or comma-separated. "
            "Defaults to all queue.enabled_models for the selected key(s)."
        ),
    )
    queue_stop_parser = queue_sub.add_parser("stop")
    queue_stop_parser.add_argument("novel_id")
    queue_stop_parser.add_argument(
        "--pid",
        action="append",
        default=[],
        help="PID(s) of specific queue process(es) to stop. Repeatable or comma-separated (e.g. --pid 123 --pid 456 or --pid 123,456).",
    )
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

    queue_add_parser = queue_sub.add_parser("add")
    queue_add_parser.add_argument("novel_id")
    queue_add_parser.add_argument("--range", required=True, help="Chapter range to enqueue, e.g. 2001-2500")
    queue_add_parser.add_argument("--force", action="store_true", help="Enqueue even if already translated (force re-translate)")

    tts_parser = subparsers.add_parser("tts")
    tts_parser.add_argument("novel_id")
    tts_parser.add_argument("--range", required=True)
    tts_parser.add_argument(
        "--force",
        action="store_true",
        help="Regenerate per-chapter audio even if cached (also refreshes cache when translated text changes).",
    )

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

    ai_key_parser = subparsers.add_parser("ai-key")
    ai_key_sub = ai_key_parser.add_subparsers(dest="ai_key_command", required=True)
    ai_key_ps = ai_key_sub.add_parser("ps")
    ai_key_ps.add_argument(
        "-f",
        "--follow",
        action="store_true",
        help="Watch mode (refresh every 1s). Ctrl+P to pause/resume, Ctrl+C to stop.",
    )
    ai_key_ps.add_argument(
        "--filter",
        action="append",
        default=[],
        help="Filter keys by kN/N/last4. Repeatable or comma-separated (e.g. --filter k1,k2 --filter 3).",
    )
    ai_key_ps.add_argument(
        "--filter-raw",
        action="append",
        default=[],
        help="Filter by raw API key(s) (exact match). Repeatable or comma-separated. Raw keys are never printed.",
    )

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


def _rate_limit_exit_code(message: str) -> int:
    """
    Map provider rate limit/quota messages to special exit codes consumed by queue workers.

    - 75: HTTP 429/backoff style rate limit (worker may enter long out-of-quota cooldown)
    - 76: quota gate (RPM/TPM/RPD) without necessarily any HTTP 429 (worker should wait briefly and retry)
    """
    msg = message or ""
    is_429 = "429" in msg or "too many requests" in msg.lower()
    return 75 if is_429 else 76


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
            from novel_tts.translate.repair import enqueue_repair_jobs, find_repair_jobs_in_range

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
                filenames = args.file or []
                if getattr(args, "range", None):
                    start, end = parse_range(args.range)
                    for _, _, r_key in get_translated_ranges(config, start, end):
                        fname = f"{r_key}.txt"
                        if fname not in filenames:
                            filenames.append(fname)
                
                changed_parts, rebuilt_files = polish_translations(config, filenames=filenames or None)
                LOGGER.info("Polished translations | changed_parts=%s rebuilt_files=%s", changed_parts, rebuilt_files)
                return 0
            if args.translate_command == "captions":
                output = translate_captions(config)
                LOGGER.info("Translated captions: %s", output)
                return 0
            if args.translate_command == "repair":
                start, end = parse_range(args.range)
                jobs = find_repair_jobs_in_range(config, start, end)
                if not jobs:
                    LOGGER.info("No repair needed in range %s-%s", start, end)
                    print(f"No repair needed for novel {config.novel_id} chapters {start}-{end}.")
                    return 0
                LOGGER.info("Translate repair found %s job(s) in range %s-%s", len(jobs), start, end)
                # Print a compact summary for operator visibility.
                for job in jobs[:50]:
                    print(f"- {job.job_id} reasons={','.join(job.reasons)}")
                if len(jobs) > 50:
                    print(f"... and {len(jobs) - 50} more")
                return enqueue_repair_jobs(config, jobs)

        if args.command == "queue":
            from novel_tts.queue import (
                add_jobs_to_queue,
                launch_queue_stack,
                list_all_queue_processes,
                list_queue_processes,
                run_status_monitor,
                run_supervisor,
                run_worker,
                reset_queue_key_state,
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
                if getattr(args, "follow", False):
                    CTRL_P = b"\x10"

                    def enter_alt_screen() -> None:
                        # Alternate screen + hide cursor to reduce flicker.
                        sys.stdout.write("\033[?1049h\033[?25l")
                        sys.stdout.flush()

                    def leave_alt_screen() -> None:
                        # Restore cursor + leave alternate screen.
                        sys.stdout.write("\033[?25h\033[?1049l")
                        sys.stdout.flush()

                    def show_cursor() -> None:
                        sys.stdout.write("\033[?25h")
                        sys.stdout.flush()

                    paused = False
                    in_alt_screen = False
                    last_frame = ""
                    last_table_frame = ""

                    # We buffer the full frame before writing once.
                    with _stdin_cbreak_if_tty() as can_read_keys:
                        enter_alt_screen()
                        in_alt_screen = True
                        try:
                            while True:
                                if can_read_keys:
                                    key = _read_stdin_byte_nonblocking()
                                    if key == CTRL_P:
                                        paused = not paused
                                        if paused and in_alt_screen:
                                            # Leave the alt screen so output becomes scrollback.
                                            leave_alt_screen()
                                            in_alt_screen = False
                                            if last_table_frame:
                                                sys.stdout.write(last_table_frame)
                                            sys.stdout.write(
                                                "paused: Ctrl+P to resume, Ctrl+C to stop (no refresh while paused)\n"
                                            )
                                            sys.stdout.flush()
                                        elif (not paused) and (not in_alt_screen):
                                            enter_alt_screen()
                                            in_alt_screen = True
                                        continue

                                if paused:
                                    time.sleep(0.1)
                                    continue

                                buf = io.StringIO()
                                buf.write(
                                    "watch: queue ps-all"
                                    f" --all={include_all} | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                                    "\n\n"
                                )
                                with contextlib.redirect_stdout(buf):
                                    rc = list_all_queue_processes(include_all=include_all)
                                if rc != 0:
                                    return rc
                                last_table_frame = buf.getvalue().rstrip("\n") + "\n"
                                last_frame = f"{last_table_frame}live: Ctrl+P to pause, Ctrl+C to stop (refresh 1s)\n"
                                # Move cursor home + clear-to-end, then write the full frame once.
                                sys.stdout.write("\033[H\033[J")
                                sys.stdout.write(last_frame)
                                sys.stdout.flush()
                                time.sleep(1.0)
                        except KeyboardInterrupt:
                            return 0
                        finally:
                            if in_alt_screen:
                                leave_alt_screen()
                            else:
                                show_cursor()
                return list_all_queue_processes(include_all=include_all)
            if args.queue_command == "reset":
                if config is None:
                    parser.error("queue reset requires a novel_id")
                raw_keys = getattr(args, "key", None) or []
                if not raw_keys:
                    parser.error("queue reset requires --key (kN or raw key)")
                try:
                    return reset_queue_key_state(
                        config,
                        key_selectors=raw_keys,
                        model_selectors=(getattr(args, "model", None) or []),
                    )
                except ValueError as exc:
                    parser.error(str(exc))
            if args.queue_command == "stop":
                if config is None:
                    parser.error("queue stop requires a novel_id")
                raw_pids = getattr(args, "pid", None) or []
                pids: list[int] | None = None
                if raw_pids:
                    parsed: list[int] = []
                    for value in raw_pids:
                        for part in str(value).split(","):
                            part = part.strip()
                            if not part:
                                continue
                            try:
                                parsed.append(int(part))
                            except ValueError:
                                parser.error(f"queue stop: invalid --pid value: {part!r}")
                    pids = parsed or None

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
                    pids=pids,
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
            if args.queue_command == "add":
                if config is None:
                    parser.error("queue add requires a novel_id")
                start, end = parse_range(args.range)
                return add_jobs_to_queue(config, start, end, force=bool(getattr(args, "force", False)))

        if args.command == "tts":
            from novel_tts.tts import run_tts

            config = load_novel_config(args.novel_id)
            start, end = parse_range(args.range)
            for c_start, c_end, r_key in get_translated_ranges(config, start, end):
                LOGGER.info(
                    "Merged audio: %s",
                    run_tts(config, c_start, c_end, range_key=r_key, force=bool(getattr(args, "force", False))),
                )
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

        if args.command == "ai-key":
            from novel_tts.ai_key import ai_key_ps

            if args.ai_key_command != "ps":
                parser.error("ai-key: unsupported subcommand")
            if getattr(args, "follow", False):
                CTRL_P = b"\x10"

                def enter_alt_screen() -> None:
                    sys.stdout.write("\033[?1049h\033[?25l")
                    sys.stdout.flush()

                def leave_alt_screen() -> None:
                    sys.stdout.write("\033[?25h\033[?1049l")
                    sys.stdout.flush()

                def show_cursor() -> None:
                    sys.stdout.write("\033[?25h")
                    sys.stdout.flush()

                paused = False
                in_alt_screen = False
                last_frame = ""
                last_table_frame = ""

                with _stdin_cbreak_if_tty() as can_read_keys:
                    enter_alt_screen()
                    in_alt_screen = True
                    try:
                        while True:
                            if can_read_keys:
                                key = _read_stdin_byte_nonblocking()
                                if key == CTRL_P:
                                    paused = not paused
                                    if paused and in_alt_screen:
                                        leave_alt_screen()
                                        in_alt_screen = False
                                        if last_table_frame:
                                            sys.stdout.write(last_table_frame)
                                        sys.stdout.write(
                                            "paused: Ctrl+P to resume, Ctrl+C to stop (no refresh while paused)\n"
                                        )
                                        sys.stdout.flush()
                                    elif (not paused) and (not in_alt_screen):
                                        enter_alt_screen()
                                        in_alt_screen = True
                                    continue

                            if paused:
                                time.sleep(0.1)
                                continue

                            buf = io.StringIO()
                            buf.write(f"watch: ai-key ps | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                            with contextlib.redirect_stdout(buf):
                                rc = ai_key_ps(
                                    filters=args.filter or [], filters_raw=getattr(args, "filter_raw", []) or []
                                )
                            if rc != 0:
                                return rc
                            last_table_frame = buf.getvalue().rstrip("\n") + "\n"
                            last_frame = f"{last_table_frame}live: Ctrl+P to pause, Ctrl+C to stop (refresh 1s)\n"
                            sys.stdout.write("\033[H\033[J")
                            sys.stdout.write(last_frame)
                            sys.stdout.flush()
                            time.sleep(1.0)
                    except KeyboardInterrupt:
                        return 0
                    finally:
                        if in_alt_screen:
                            leave_alt_screen()
                        else:
                            show_cursor()
            return ai_key_ps(filters=args.filter or [], filters_raw=getattr(args, "filter_raw", []) or [])

        parser.error("Unhandled command")
        return 2
    except RateLimitExceededError as exc:
        # Used by queue workers: treat as a transient condition so the worker can release/requeue the job.
        #
        # Exit code semantics:
        # - 75: HTTP 429/backoff style rate limit (worker may enter long out-of-quota cooldown)
        # - 76: quota gate (RPM/TPM/RPD) without necessarily any HTTP 429 (worker should wait briefly and retry)
        code = _rate_limit_exit_code(str(exc))
        LOGGER.warning("Rate limited (exit=%s): %s", code, exc)
        return code
    except Exception:
        LOGGER.exception("Command failed")
        return 1
