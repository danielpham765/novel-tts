from __future__ import annotations

import argparse
import contextlib
import io
import json
import logging
import os
import re
import select
import subprocess
import sys
import termios
import time
import tty
from collections.abc import Callable
from datetime import datetime
from pathlib import Path

from novel_tts.common.logging import (
    configure_logging,
    get_logger,
    get_novel_log_dir,
    get_novel_log_path,
    install_exception_logging,
)
from novel_tts.common.text import parse_range
from novel_tts.common.errors import RateLimitExceededError
from novel_tts.config import load_novel_config, NovelConfig

LOGGER = get_logger(__name__)
CRAWL_VERIFY_LOGGER = get_logger("crawl.verify")


def _format_log_path(path: Path | None) -> str:
    if path is None:
        return ""
    try:
        rel = path.resolve().relative_to(Path.cwd().resolve())
    except Exception:
        rel = path
    text = rel.as_posix()
    if text and not text.startswith("."):
        text = f"./{text}"
    return text


def _format_click_path(path: Path | None) -> str:
    """
    Prefer an absolute path token that Cmd+Click detectors can open reliably.
    """
    if path is None:
        return ""
    try:
        return path.expanduser().resolve().as_posix()
    except Exception:
        return str(path)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _shared_logs_root() -> Path:
    return _repo_root() / ".logs"


def _apply_tts_cli_overrides(config: NovelConfig, args: argparse.Namespace) -> NovelConfig:
    server_name = getattr(args, "tts_server_name", None)
    model_name = getattr(args, "tts_model_name", None)
    if server_name:
        config.tts.server_name = str(server_name)
    if model_name:
        config.tts.model_name = str(model_name)
    return config


def _parse_bool_arg(value: str) -> bool:
    text = str(value or "").strip().lower()
    if text in {"1", "true", "t", "yes", "y"}:
        return True
    if text in {"0", "false", "f", "no", "n"}:
        return False
    raise argparse.ArgumentTypeError(f"Invalid boolean value: {value!r}")


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


def _watch_table(*, title: str, render: Callable[[], int], refresh_seconds: float = 1.0) -> int:
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
                            sys.stdout.write("paused: Ctrl+P to resume, Ctrl+C to stop (no refresh while paused)\n")
                            sys.stdout.flush()
                        elif (not paused) and (not in_alt_screen):
                            enter_alt_screen()
                            in_alt_screen = True
                        continue

                if paused:
                    time.sleep(0.1)
                    continue

                buf = io.StringIO()
                buf.write(f"{title} | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                with contextlib.redirect_stdout(buf):
                    rc = render()
                if rc != 0:
                    return rc
                last_table_frame = buf.getvalue().rstrip("\n") + "\n"
                frame = f"{last_table_frame}live: Ctrl+P to pause, Ctrl+C to stop (refresh {int(refresh_seconds)}s)\n"
                sys.stdout.write("\033[H\033[J")
                sys.stdout.write(frame)
                sys.stdout.flush()
                time.sleep(float(refresh_seconds))
        except KeyboardInterrupt:
            return 0
        finally:
            if in_alt_screen:
                leave_alt_screen()
            else:
                show_cursor()


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


def _run_pipeline_per_stage(
    config: NovelConfig,
    *,
    translated_ranges: list[tuple[int, int, str]],
    skip_tts: bool,
    skip_visual: bool,
    skip_video: bool,
    skip_upload: bool,
    upload_platform: str,
) -> None:
    from novel_tts.media import create_video, generate_visual
    from novel_tts.tts import run_tts
    from novel_tts.upload import run_uploads

    if not skip_tts:
        for c_start, c_end, range_key in translated_ranges:
            run_tts(config, c_start, c_end, range_key)
    if not skip_visual:
        for c_start, c_end, _ in translated_ranges:
            generate_visual(config, c_start, c_end)
    if not skip_video:
        for c_start, c_end, _ in translated_ranges:
            create_video(config, c_start, c_end)
    if not skip_upload:
        upload_ranges = [(c_start, c_end) for c_start, c_end, _ in translated_ranges]
        run_uploads(config, upload_ranges, platform=upload_platform, dry_run=False)


def _run_pipeline_per_video(
    config: NovelConfig,
    *,
    translated_ranges: list[tuple[int, int, str]],
    skip_tts: bool,
    skip_visual: bool,
    skip_video: bool,
    skip_upload: bool,
    upload_platform: str,
) -> None:
    from novel_tts.media import create_video, generate_visual
    from novel_tts.tts import run_tts
    from novel_tts.upload import run_uploads

    for c_start, c_end, range_key in translated_ranges:
        if not skip_tts:
            run_tts(config, c_start, c_end, range_key)
        if not skip_visual:
            generate_visual(config, c_start, c_end)
        if not skip_video:
            create_video(config, c_start, c_end)
        if not skip_upload:
            run_uploads(config, [(c_start, c_end)], platform=upload_platform, dry_run=False)


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
    crawl_run_parser.add_argument(
        "--no-prune-manifest",
        action="store_true",
        help="Do not prune stale entries in crawl_failures.json after crawling.",
    )
    crawl_verify_parser = crawl_sub.add_parser("verify")
    crawl_verify_parser.add_argument("novel_id")
    crawl_verify_parser.add_argument("--from", dest="from_chapter", type=int)
    crawl_verify_parser.add_argument("--to", dest="to_chapter", type=int)
    crawl_verify_parser.add_argument("--range")
    crawl_verify_parser.add_argument("--file", action="append", default=[])
    crawl_verify_parser.add_argument(
        "--no-fix-manifest",
        action="store_true",
        help="Do not auto-prune stale entries in crawl_failures.json during verify.",
    )
    crawl_verify_parser.add_argument(
        "--keep-empty-manifest",
        action="store_true",
        help="Keep crawl_failures.json even if it becomes empty after pruning.",
    )
    crawl_verify_parser.add_argument(
        "--sync-repair-config",
        action="store_true",
        help="Create/update input/<novel_id>/repair_config.yaml by merging existing config with research-derived suggestions.",
    )

    crawl_repair_parser = crawl_sub.add_parser("repair")
    crawl_repair_parser.add_argument("novel_id")
    crawl_repair_parser.add_argument("--from", dest="from_chapter", type=int)
    crawl_repair_parser.add_argument("--to", dest="to_chapter", type=int)
    crawl_repair_parser.add_argument("--range")
    crawl_repair_parser.add_argument("--file", action="append", default=[])
    crawl_repair_parser.add_argument(
        "--report-file",
        default="",
        help="Override default report path under .logs/<novel_id>/crawl/addition-replacement_chapter_list.txt",
    )
    crawl_repair_parser.add_argument(
        "--generate-repair-config",
        action="store_true",
        help="Generate input/<novel_id>/repair_config.yaml from .logs/<novel_id>/crawl research files, then exit.",
    )
    crawl_repair_parser.add_argument(
        "--run",
        action="store_true",
        help="Run repair using input/<novel_id>/repair_config.yaml. If missing, generate a minimal config first. If no --range/--from/--to is provided, infer range from origin batch filenames.",
    )

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
    queue_ps_parser.add_argument(
        "-f",
        "--follow",
        action="store_true",
        help="Watch mode (refresh every 1s). Ctrl+P to pause/resume, Ctrl+C to stop.",
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
    queue_reset_key_parser = queue_sub.add_parser("reset-key")
    queue_reset_key_parser.add_argument("novel_id")
    queue_reset_key_group = queue_reset_key_parser.add_mutually_exclusive_group(required=True)
    queue_reset_key_group.add_argument(
        "--key",
        action="append",
        default=[],
        help=(
            "Key selector(s) to reset. Repeatable or comma-separated. "
            "Accepts kN (key index) or raw key (exact match in .secrets/gemini-keys.txt)."
        ),
    )
    queue_reset_key_group.add_argument(
        "--all",
        action="store_true",
        help="Reset all keys from .secrets/gemini-keys.txt (applies to selected models).",
    )
    queue_reset_key_parser.add_argument(
        "--model",
        action="append",
        default=[],
        help=(
            "Model(s) to reset. Repeatable or comma-separated. "
            "Defaults to all queue.enabled_models for the selected key(s)."
        ),
    )

    queue_repair_parser = queue_sub.add_parser("repair")
    queue_repair_parser.add_argument("novel_id")
    queue_repair_group = queue_repair_parser.add_mutually_exclusive_group(required=True)
    queue_repair_group.add_argument(
        "--range",
        help="Chapter range to scan and enqueue for repair, e.g. 1401-1410",
    )
    queue_repair_group.add_argument(
        "--all",
        action="store_true",
        help="Scan all chapters in origin files and enqueue only the ones that likely need repair.",
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
    queue_launch_parser.add_argument(
        "--add-queue",
        action="store_true",
        help="Scan origin files and enqueue all chapters that still need translation.",
    )

    queue_add_parser = queue_sub.add_parser("add")
    queue_add_parser.add_argument("novel_id")
    queue_add_group = queue_add_parser.add_mutually_exclusive_group(required=True)
    queue_add_group.add_argument("--range", help="Chapter range to enqueue, e.g. 2001-2500")
    queue_add_group.add_argument(
        "--chapters",
        action="append",
        default=[],
        help="Chapter numbers to enqueue. Repeatable or comma-separated, e.g. --chapters 1205,1214 --chapters 2200",
    )
    queue_add_group.add_argument(
        "--repair-report",
        help="Parse a crawl repair report (addition-replacement_chapter_list.txt) and enqueue those chapters.",
    )
    queue_add_group.add_argument(
        "--all",
        action="store_true",
        help="Scan origin files and enqueue all chapters that still need translation.",
    )
    queue_add_parser.add_argument("--force", action="store_true", help="Enqueue even if already translated (force re-translate)")

    tts_parser = subparsers.add_parser("tts")
    tts_parser.add_argument("novel_id")
    tts_parser.add_argument("--range", required=True)
    tts_parser.add_argument(
        "--tts-server-name",
        help="Override tts.server_name for this run only.",
    )
    tts_parser.add_argument(
        "--tts-model-name",
        help="Override tts.model_name for this run only.",
    )
    tts_parser.add_argument(
        "--force",
        action="store_true",
        help="Regenerate per-chapter audio even if cached (also refreshes cache when translated text changes).",
    )

    visual_parser = subparsers.add_parser("visual")
    visual_parser.add_argument("novel_id")
    visual_group = visual_parser.add_mutually_exclusive_group(required=True)
    visual_group.add_argument("--range")
    visual_group.add_argument("--chapter", type=int)
    visual_parser.add_argument(
        "--force",
        action="store_true",
        help="Regenerate visual assets even if the cached final outputs already match the current inputs.",
    )

    video_parser = subparsers.add_parser("video")
    video_parser.add_argument("novel_id")
    video_parser.add_argument("--range", required=True)
    video_parser.add_argument(
        "--force",
        action="store_true",
        help="Regenerate final video even if the cached output already matches the current visual/audio inputs.",
    )

    upload_parser = subparsers.add_parser("upload")
    upload_parser.add_argument("novel_id")
    upload_parser.add_argument("--platform", choices=["youtube", "tiktok"], required=True)
    upload_parser.add_argument("--range")
    upload_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate/build payload and log upload plan without publishing.",
    )
    upload_parser.add_argument(
        "--update-playlist-index",
        action="store_true",
        help="Rewrite uploaded YouTube video descriptions so the playlist line uses each video's own id.",
    )

    youtube_parser = subparsers.add_parser("youtube")
    youtube_sub = youtube_parser.add_subparsers(dest="youtube_command", required=True)
    youtube_playlist_parser = youtube_sub.add_parser("playlist")
    youtube_playlist_parser.add_argument(
        "playlist_action",
        nargs="?",
        choices=["update"],
        help='Optional subcommand. Use "update" to update playlist metadata.',
    )
    youtube_playlist_parser.add_argument(
        "--id",
        help="Playlist id or full YouTube playlist URL. If omitted, list all accessible playlists.",
    )
    youtube_playlist_parser.add_argument(
        "--title-only",
        action="store_true",
        help="List only playlist id and title.",
    )
    youtube_playlist_parser.add_argument("--title", help="Updated playlist title.")
    youtube_playlist_parser.add_argument("--description", help="Updated playlist description.")
    youtube_playlist_parser.add_argument(
        "--privacy-status",
        choices=["private", "public", "unlisted"],
        help="Updated playlist privacy status.",
    )
    youtube_video_parser = youtube_sub.add_parser("video")
    youtube_video_parser.add_argument(
        "video_action",
        nargs="?",
        choices=["update"],
        help='Optional subcommand. Use "update" to update video metadata.',
    )
    youtube_video_parser.add_argument(
        "--id",
        help="Video id. If omitted, list all videos from the authenticated channel uploads playlist.",
    )
    youtube_video_parser.add_argument(
        "--title-only",
        action="store_true",
        help="List only video id and title.",
    )
    youtube_video_parser.add_argument("--title", help="Updated video title.")
    youtube_video_parser.add_argument("--description", help="Updated video description.")
    youtube_video_parser.add_argument(
        "--privacy_status",
        "--privacy-status",
        dest="privacy_status",
        choices=["private", "public", "unlisted"],
        help="Updated video privacy status.",
    )
    youtube_video_parser.add_argument(
        "--made_for_kids",
        "--made-for-kids",
        dest="made_for_kids",
        type=_parse_bool_arg,
        help="Updated made-for-kids flag (true/false).",
    )
    youtube_video_parser.add_argument(
        "--playlist_position",
        "--playlist-position",
        dest="playlist_position",
        type=int,
        help="Updated position in the authenticated channel uploads playlist.",
    )

    pipeline_parser = subparsers.add_parser("pipeline")
    pipeline_sub = pipeline_parser.add_subparsers(dest="pipeline_command", required=True)
    pipeline_run = pipeline_sub.add_parser("run")
    pipeline_run.add_argument("novel_id")
    pipeline_run.add_argument("--from", dest="from_chapter", type=int)
    pipeline_run.add_argument("--to", dest="to_chapter", type=int)
    pipeline_run.add_argument("--range")
    pipeline_run.add_argument("--skip-crawl", action="store_true")
    pipeline_run.add_argument("--skip-translate", action="store_true")
    pipeline_run.add_argument("--skip-tts", action="store_true")
    pipeline_run.add_argument("--skip-visual", action="store_true")
    pipeline_run.add_argument("--skip-video", action="store_true")
    pipeline_run.add_argument("--skip-upload", action="store_true")
    pipeline_run.add_argument(
        "--mode",
        choices=["per-stage", "per-video"],
        default="per-stage",
        help="Pipeline execution order for media stages. 'per-stage' runs one stage across all ranges before the next; 'per-video' runs TTS -> visual -> video -> upload for each range in order.",
    )
    pipeline_run.add_argument(
        "--upload-platform",
        choices=["youtube", "tiktok"],
        help="Override upload platform for pipeline upload step. Defaults to upload.default_platform.",
    )
    pipeline_watch = pipeline_sub.add_parser("watch")
    pipeline_watch.add_argument("novel_ids", nargs="*")
    pipeline_watch.add_argument(
        "--all",
        action="store_true",
        help="Watch all novels from pipeline.watch.novels, or fallback to configs/novels/*.json when that list is empty.",
    )
    pipeline_watch.add_argument(
        "--interval-seconds",
        type=float,
        default=None,
        help="Polling interval between source scans (default: 300 seconds).",
    )
    pipeline_watch.add_argument(
        "--once",
        action="store_true",
        help="Run exactly one scan cycle, then exit.",
    )
    pipeline_watch.add_argument(
        "--upload-platform",
        choices=["youtube", "tiktok"],
        help="Override upload platform for downstream upload step. Defaults to upload.default_platform.",
    )
    pipeline_watch.add_argument(
        "--restart-queue",
        action="store_true",
        default=None,
        help="Restart the per-novel queue stack before enqueueing newly crawled chapters.",
    )
    pipeline_watch.add_argument(
        "--bootstrap-from",
        type=int,
        help="If a novel has no local crawled chapters yet, bootstrap crawl from this chapter instead of skipping.",
    )
    pipeline_watch.add_argument("--skip-crawl", action="store_true")
    pipeline_watch.add_argument("--skip-translate", action="store_true")
    pipeline_watch.add_argument("--skip-repair", action="store_true")
    pipeline_watch.add_argument("--skip-polish", action="store_true")
    pipeline_watch.add_argument("--skip-tts", action="store_true")
    pipeline_watch.add_argument("--skip-visual", action="store_true")
    pipeline_watch.add_argument("--skip-video", action="store_true")
    pipeline_watch.add_argument("--skip-upload", action="store_true")
    pipeline_watch.add_argument(
        "--until-crawl",
        action="store_true",
        help="Run through crawl, then skip translate, repair, polish, tts, visual, video, and upload.",
    )
    pipeline_watch.add_argument(
        "--until-translate",
        action="store_true",
        help="Run through translate, then skip repair, polish, tts, visual, video, and upload.",
    )
    pipeline_watch.add_argument(
        "--until-repair",
        action="store_true",
        help="Run through repair, then skip polish, tts, visual, video, and upload.",
    )
    pipeline_watch.add_argument(
        "--until-polish",
        action="store_true",
        help="Run through polish, then skip tts, visual, video, and upload.",
    )
    pipeline_watch.add_argument(
        "--until-tts",
        action="store_true",
        help="Run through tts, then skip visual, video, and upload.",
    )
    pipeline_watch.add_argument(
        "--until-visual",
        action="store_true",
        help="Run through visual, then skip video and upload.",
    )
    pipeline_watch.add_argument(
        "--until-video",
        action="store_true",
        help="Run through video, then skip upload.",
    )
    pipeline_watch.add_argument(
        "--until-upload",
        action="store_true",
        help="Run through upload. Equivalent to not setting an --until-* cutoff.",
    )

    quota_supervisor_parser = subparsers.add_parser("quota-supervisor")
    quota_supervisor_parser.add_argument(
        "--poll-interval-seconds",
        type=float,
        default=0.05,
        help="Supervisor loop sleep when no grants are made (seconds).",
    )
    quota_supervisor_parser.add_argument(
        "-d",
        "--daemon",
        action="store_true",
        help="Run in background (spawns a detached quota-supervisor process).",
    )
    quota_supervisor_parser.add_argument(
        "--stop",
        action="store_true",
        help="Stop background quota-supervisor process(es) started via -d (best-effort).",
    )
    quota_supervisor_parser.add_argument(
        "--restart",
        action="store_true",
        help="Restart background quota-supervisor process (equivalent to --stop then -d).",
    )

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
    if not novel_id and args.command != "youtube":
        return None

    command = args.command
    log_name: str
    logs_root: Path

    if command != "youtube":
        config = load_novel_config(novel_id)
        logs_root = get_novel_log_dir(config.storage.logs_dir, novel_id)
    else:
        logs_root = _shared_logs_root()

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
    elif command == "upload":
        upload_platform = getattr(args, "platform", None) or "unknown"
        log_name = f"upload/{upload_platform}.log"
    elif command == "youtube":
        youtube_command = getattr(args, "youtube_command", None) or "youtube"
        log_name = f"upload/youtube/{youtube_command}.log"
    elif command == "pipeline":
        pipeline_command = getattr(args, "pipeline_command", None) or "run"
        log_name = f"pipeline/{pipeline_command}.log"
    else:
        log_name = f"{command}.log"

    return logs_root / log_name


def _resolve_watch_stage_flags(args) -> dict[str, bool]:
    stage_order = [
        "crawl",
        "translate",
        "repair",
        "polish",
        "tts",
        "visual",
        "video",
        "upload",
    ]
    explicit_skip = {
        stage: bool(getattr(args, f"skip_{stage}", False))
        for stage in stage_order
    }
    until_indexes = [
        index
        for index, stage in enumerate(stage_order)
        if bool(getattr(args, f"until_{stage}", False))
    ]
    if not until_indexes:
        return explicit_skip

    cutoff_index = min(until_indexes)
    resolved = dict(explicit_skip)
    for index, stage in enumerate(stage_order):
        if index > cutoff_index:
            resolved[stage] = True
    return resolved


def _rotate_log_if_new_day(log_path: Path | None) -> None:
    if log_path is None or not log_path.exists():
        return
    try:
        st = log_path.stat()
    except FileNotFoundError:
        return
    if st.st_size <= 0:
        return
    today = datetime.now().astimezone().date()
    file_day = datetime.fromtimestamp(st.st_mtime, tz=datetime.now().astimezone().tzinfo).date()
    if file_day == today:
        return
    from novel_tts.common import logrotate

    logrotate.rotate_log_file_to_today(logs_root=log_path.parents[2], src=log_path)


def _rate_limit_exit_code(message: str) -> int:
    """
    Map provider rate limit/quota messages to special exit codes consumed by queue workers.

    - 75: HTTP 429/backoff style rate limit (worker may enter long out-of-quota cooldown)
    - 76: quota gate (RPM/TPM/RPD) without necessarily any HTTP 429 (worker should wait briefly and retry)
    """
    msg = message or ""
    is_429 = "429" in msg or "too many requests" in msg.lower()
    return 75 if is_429 else 76


def _reroute_tts_away_from_uv(raw_argv: list[str]) -> int | None:
    if not raw_argv or raw_argv[0] != "tts":
        return None
    if not os.environ.get("UV_RUN_RECURSION_DEPTH"):
        return None
    if os.environ.get("NOVEL_TTS_SKIP_UV_REROUTE") == "1":
        return None

    repo_root = Path(__file__).resolve().parents[2]
    entrypoint = repo_root / ".venv" / "bin" / "novel-tts"
    if not entrypoint.exists():
        return None

    env = dict(os.environ)
    env["NOVEL_TTS_SKIP_UV_REROUTE"] = "1"
    for key in list(env.keys()):
        if key == "UV" or key.startswith("UV_"):
            env.pop(key, None)
    LOGGER.info("Rerouting tts away from uv-run via %s", entrypoint)
    proc = subprocess.run([str(entrypoint), *raw_argv], cwd=str(repo_root), env=env, check=False)
    return int(proc.returncode)


def main(argv: list[str] | None = None) -> int:
    raw_argv = list(sys.argv[1:] if argv is None else argv)
    if len(raw_argv) >= 2 and raw_argv[0] == "crawl" and raw_argv[1] not in {"run", "verify", "repair", "-h", "--help"}:
        raw_argv.insert(1, "run")
    rerouted_rc = _reroute_tts_away_from_uv(raw_argv)
    if rerouted_rc is not None:
        return rerouted_rc

    parser = _build_parser()
    args = parser.parse_args(raw_argv)
    is_crawl_verify = args.command == "crawl" and (getattr(args, "crawl_command", None) or "run") == "verify"
    run_logger = CRAWL_VERIFY_LOGGER if is_crawl_verify else LOGGER
    log_path = _default_log_path(args)
    configure_logging(log_path)
    if (
        log_path is not None
        and args.command == "crawl"
        and (getattr(args, "crawl_command", None) or "run") == "verify"
        and not bool(getattr(args, "log_file", None))
    ):
        try:
            from novel_tts.common import logrotate

            # log_path is typically: <logs_root>/<novel_id>/crawl/verify.log
            # => logs_root is two parents above novel_id dir.
            logs_root = log_path.parents[2]
            logrotate.rotate_log_file_to_today(logs_root=logs_root, src=log_path)
        except Exception:
            # Best-effort: rotation should never block the verify command itself.
            pass
    if log_path is not None and args.command == "youtube" and not bool(getattr(args, "log_file", None)):
        try:
            _rotate_log_if_new_day(log_path)
        except Exception:
            pass
    install_exception_logging(LOGGER)
    if log_path is not None:
        run_logger.info("Logging to %s", log_path)

    try:
        if args.command == "crawl":
            from novel_tts.crawl import crawl_range, repair_crawled_content, verify_crawled_content

            config = load_novel_config(args.novel_id)
            crawl_command = args.crawl_command or "run"
            if crawl_command == "run":
                if args.range:
                    start, end = parse_range(args.range)
                else:
                    start, end = args.from_chapter, args.to_chapter
                    if (start is None) or (end is None):
                        parser.error("crawl run requires --range or both --from and --to")
                outputs = crawl_range(
                    config,
                    start,
                    end,
                    args.dir_url,
                    prune_failure_manifest=not bool(getattr(args, "no_prune_manifest", False)),
                )
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
                    fix_stale_manifest=not bool(getattr(args, "no_fix_manifest", False)),
                    delete_empty_manifest=not bool(getattr(args, "keep_empty_manifest", False)),
                )
                if bool(getattr(args, "sync_repair_config", False)):
                    from novel_tts.crawl.repair_config import (
                        generate_repair_config_from_research,
                        load_repair_config,
                        merge_repair_config,
                        repair_config_path,
                        save_repair_config,
                    )

                    rc_path = repair_config_path(config.storage.input_dir)
                    generated_cfg = generate_repair_config_from_research(
                        root=config.storage.root,
                        novel_id=config.novel_id,
                        logs_dir=config.storage.logs_dir,
                        input_dir=config.storage.input_dir,
                    )
                    if rc_path.exists():
                        existing_cfg = load_repair_config(rc_path)
                        merged_cfg = merge_repair_config(existing_cfg, generated_cfg)
                        save_repair_config(rc_path, merged_cfg)
                        LOGGER.info("Merged repair config: %s", _format_click_path(rc_path))
                    else:
                        save_repair_config(rc_path, generated_cfg)
                        LOGGER.info("Generated repair config: %s", _format_click_path(rc_path))
                issue_count = len(report.issues)
                if issue_count:
                    noun = "issue" if issue_count == 1 else "issues"
                    summary_suffix = f": found {issue_count} {noun}"
                else:
                    summary_suffix = ""
                CRAWL_VERIFY_LOGGER.info(
                    "Crawl verify checked %s files and %s chapters%s",
                    len(report.checked_files),
                    len(report.checked_chapters),
                    summary_suffix,
                )
                if report.stale_failures_removed:
                    CRAWL_VERIFY_LOGGER.info(
                        "Crawl verify pruned stale failure manifest entries | removed=%s deleted=%s",
                        report.stale_failures_removed,
                        report.failure_manifest_deleted,
                    )
                if report.ok:
                    CRAWL_VERIFY_LOGGER.info("Crawl verify OK")
                    return 0
                for issue in report.issues:
                    if issue.path is not None:
                        reason = (issue.message or "").strip().replace("\n", " ")
                        if reason:
                            CRAWL_VERIFY_LOGGER.warning(
                                "Crawl verify issue | code=%s chapter=%s | Reason: %s | file=%s",
                                issue.code,
                                issue.chapter_number,
                                reason,
                                _format_click_path(issue.path),
                            )
                        else:
                            CRAWL_VERIFY_LOGGER.warning(
                                "Crawl verify issue | code=%s chapter=%s | file=%s",
                                issue.code,
                                issue.chapter_number,
                                _format_click_path(issue.path),
                            )
                    else:
                        CRAWL_VERIFY_LOGGER.warning(
                            "Crawl verify issue | code=%s chapter=%s | %s",
                            issue.code,
                            issue.chapter_number,
                            issue.message,
                        )
                return 1

            if crawl_command == "repair":
                if bool(getattr(args, "generate_repair_config", False)) and bool(getattr(args, "run", False)):
                    parser.error("crawl repair: use only one of --generate-repair-config or --run")
                if not bool(getattr(args, "generate_repair_config", False)) and not bool(getattr(args, "run", False)):
                    parser.error("crawl repair: requires --generate-repair-config or --run")
                report_path = (
                    Path(args.report_file).expanduser().resolve()
                    if str(getattr(args, "report_file", "") or "").strip()
                    else None
                )
                if bool(getattr(args, "generate_repair_config", False)):
                    from novel_tts.crawl.repair_config import (
                        generate_repair_config_from_research,
                        repair_config_path,
                        save_repair_config,
                    )

                    cfg = generate_repair_config_from_research(
                        root=config.storage.root,
                        novel_id=config.novel_id,
                        logs_dir=config.storage.logs_dir,
                        input_dir=config.storage.input_dir,
                    )
                    out_path = repair_config_path(config.storage.input_dir)
                    save_repair_config(out_path, cfg)
                    LOGGER.info("Generated repair config: %s", _format_click_path(out_path))
                    return 0

                # --run mode
                if args.range:
                    start, end = parse_range(args.range)
                else:
                    start, end = args.from_chapter, args.to_chapter
                    if (start is None) ^ (end is None):
                        parser.error("crawl repair --run requires both --from and --to when using explicit bounds")
                repair_cfg_path = config.storage.input_dir / "repair_config.yaml"
                if not repair_cfg_path.exists():
                    from novel_tts.crawl.repair_config import (
                        generate_repair_config_from_research,
                        save_repair_config,
                    )

                    repair_cfg = generate_repair_config_from_research(
                        root=config.storage.root,
                        novel_id=config.novel_id,
                        logs_dir=config.storage.logs_dir,
                        input_dir=config.storage.input_dir,
                    )
                    save_repair_config(repair_cfg_path, repair_cfg)
                    LOGGER.info("Generated missing repair config: %s", _format_click_path(repair_cfg_path))
                repair_report = repair_crawled_content(
                    config,
                    start,
                    end,
                    filenames=args.file or None,
                    log_path=report_path,
                    generate_repair_config_if_missing=False,
                )
                LOGGER.info(
                    "Crawl repair completed | actions=%s modified_files=%s",
                    len(repair_report.actions),
                    len(repair_report.modified_files),
                )
                LOGGER.info("Crawl repair report: %s", _format_click_path(repair_report.log_path))
                for path in repair_report.modified_files:
                    LOGGER.info("Modified origin file: %s", _format_click_path(path))
                return 0

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

        if args.command == "queue":
            from novel_tts.queue import (
                add_all_jobs_to_queue,
                add_chapters_to_queue,
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
            from novel_tts.translate.repair import enqueue_repair_jobs, find_repair_jobs_all, find_repair_jobs_in_range

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
                if getattr(args, "follow", False):
                    return _watch_table(
                        title=f"watch: queue ps {config.novel_id} --all={include_all}",
                        render=lambda: list_queue_processes(config, include_all=include_all),
                    )
                return list_queue_processes(config, include_all=include_all)
            if args.queue_command == "ps-all":
                include_all = bool(getattr(args, "all", False) or getattr(args, "show_translate", False))
                if getattr(args, "follow", False):
                    return _watch_table(
                        title=f"watch: queue ps-all --all={include_all}",
                        render=lambda: list_all_queue_processes(include_all=include_all),
                    )
                return list_all_queue_processes(include_all=include_all)
            if args.queue_command == "reset-key":
                if config is None:
                    parser.error("queue reset-key requires a novel_id")
                try:
                    return reset_queue_key_state(
                        config,
                        key_selectors=(getattr(args, "key", None) or []),
                        all_keys=bool(getattr(args, "all", False)),
                        model_selectors=(getattr(args, "model", None) or []),
                    )
                except ValueError as exc:
                    parser.error(str(exc))
            if args.queue_command == "repair":
                if config is None:
                    parser.error("queue repair requires a novel_id")
                if getattr(args, "all", False):
                    jobs = find_repair_jobs_all(config)
                else:
                    if not getattr(args, "range", None):
                        parser.error("queue repair requires --all or --range")
                    start, end = parse_range(args.range)
                    jobs = find_repair_jobs_in_range(config, start, end)
                if not jobs:
                    if getattr(args, "all", False):
                        LOGGER.info("No repair needed (all chapters)")
                        print(f"No repair needed for novel {config.novel_id} (all chapters).")
                    else:
                        LOGGER.info("No repair needed in range %s-%s", start, end)
                        print(f"No repair needed for novel {config.novel_id} chapters {start}-{end}.")
                    return 0
                if getattr(args, "all", False):
                    LOGGER.info("Queue repair found %s job(s) (all chapters)", len(jobs))
                else:
                    LOGGER.info("Queue repair found %s job(s) in range %s-%s", len(jobs), start, end)
                for job in jobs[:50]:
                    print(f"- {job.job_id} reasons={','.join(job.reasons)}")
                if len(jobs) > 50:
                    print(f"... and {len(jobs) - 50} more")
                return enqueue_repair_jobs(config, jobs, label="queue repair")
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
                return launch_queue_stack(config, restart=args.restart, add_queue=bool(getattr(args, "add_queue", False)))
            if args.queue_command == "add":
                if config is None:
                    parser.error("queue add requires a novel_id")
                if getattr(args, "all", False):
                    return add_all_jobs_to_queue(config, force=bool(getattr(args, "force", False)))
                if getattr(args, "range", None):
                    start, end = parse_range(args.range)
                    return add_jobs_to_queue(config, start, end, force=bool(getattr(args, "force", False)))

                repair_report = getattr(args, "repair_report", None)
                if repair_report:
                    report_path = Path(repair_report).expanduser().resolve()
                    if not report_path.exists():
                        parser.error(f"queue add: repair report not found: {report_path}")
                    text = report_path.read_text(encoding="utf-8")
                    chapters = sorted({int(m.group(1)) for m in re.finditer(r"^- chapter\s+(\d+)\b", text, flags=re.M)})
                    if not chapters:
                        parser.error(f"queue add: no chapters found in repair report: {report_path}")
                    return add_chapters_to_queue(config, chapters, force=bool(getattr(args, "force", False)))

                raw_chapters = getattr(args, "chapters", None) or []
                chapters: list[int] = []
                for value in raw_chapters:
                    for part in str(value).split(","):
                        part = part.strip()
                        if not part:
                            continue
                        try:
                            chapters.append(int(part))
                        except ValueError:
                            parser.error(f"queue add: invalid chapter: {part!r}")
                if not chapters:
                    parser.error("queue add requires --all, --range, --chapters, or --repair-report")
                return add_chapters_to_queue(config, chapters, force=bool(getattr(args, "force", False)))

        if args.command == "tts":
            from novel_tts.tts import run_tts

            config = load_novel_config(args.novel_id)
            config = _apply_tts_cli_overrides(config, args)
            start, end = parse_range(args.range)
            for c_start, c_end, r_key in get_translated_ranges(config, start, end):
                LOGGER.info(
                    "Merged audio: %s",
                    run_tts(config, c_start, c_end, range_key=r_key, force=bool(getattr(args, "force", False))),
                )
            return 0

        if args.command == "visual":
            from novel_tts.media import generate_visual, generate_visual_for_chapter

            config = load_novel_config(args.novel_id)
            chapter = getattr(args, "chapter", None)
            if chapter is not None:
                visual, thumbnail = generate_visual_for_chapter(config, int(chapter), force=bool(getattr(args, "force", False)))
                LOGGER.info("Visual video: %s", visual)
                LOGGER.info("Thumbnail: %s", thumbnail)
                return 0

            start, end = parse_range(args.range)
            for c_start, c_end, _ in get_translated_ranges(config, start, end):
                visual, thumbnail = generate_visual(config, c_start, c_end, force=bool(getattr(args, "force", False)))
                LOGGER.info("Visual video: %s", visual)
                LOGGER.info("Thumbnail: %s", thumbnail)
            return 0

        if args.command == "video":
            from novel_tts.media import create_video

            config = load_novel_config(args.novel_id)
            start, end = parse_range(args.range)
            for c_start, c_end, _ in get_translated_ranges(config, start, end):
                LOGGER.info("Video: %s", create_video(config, c_start, c_end, force=bool(getattr(args, "force", False))))
            return 0

        if args.command == "upload":
            from novel_tts.upload import run_uploads, update_uploaded_youtube_playlist_index_descriptions

            config = load_novel_config(args.novel_id)
            if bool(getattr(args, "update_playlist_index", False)):
                if str(args.platform) != "youtube":
                    parser.error("--update-playlist-index is only supported with --platform youtube")
                start = end = None
                if getattr(args, "range", None):
                    start, end = parse_range(args.range)
                results = update_uploaded_youtube_playlist_index_descriptions(
                    config,
                    from_chapter=start,
                    to_chapter=end,
                    log_summary=False,
                )
                pretty_results = json.dumps(results, ensure_ascii=False, indent=2)
                LOGGER.info("Upload playlist-index update result: %s", pretty_results)
                unchanged_count = sum(1 for item in results if str(item.get("status", "")) == "unchanged")
                updated_count = sum(1 for item in results if str(item.get("status", "")) == "updated")
                LOGGER.info("Uploaded Video count: %s", len(results))
                LOGGER.info("Correct description - video count: %s", unchanged_count)
                LOGGER.info("Update description - video count: %s", updated_count)
                print(pretty_results)
                return 0

            if not getattr(args, "range", None):
                parser.error("upload requires --range unless --update-playlist-index is used")

            start, end = parse_range(args.range)
            upload_ranges = [(c_start, c_end) for c_start, c_end, _ in get_translated_ranges(config, start, end)]
            for result in run_uploads(
                config,
                upload_ranges,
                platform=str(args.platform),
                dry_run=bool(getattr(args, "dry_run", False)),
            ):
                LOGGER.info("Upload result: %s", result)
            return 0

        if args.command == "youtube":
            from novel_tts.upload import (
                get_youtube_playlist,
                get_youtube_video,
                list_youtube_playlists,
                list_youtube_videos,
                update_youtube_playlist,
                update_youtube_video,
            )

            if args.youtube_command == "playlist" and getattr(args, "playlist_action", None) == "update":
                playlist_id = str(getattr(args, "id", "") or "").strip()
                if not playlist_id:
                    parser.error("youtube playlist update requires --id")

                current = get_youtube_playlist(playlist_id)
                update_fields = {
                    "title": getattr(args, "title", None),
                    "description": getattr(args, "description", None),
                    "privacy_status": getattr(args, "privacy_status", None),
                }
                changed_fields = {
                    key: value
                    for key, value in update_fields.items()
                    if value is not None and value != current.get(key)
                }

                print("Current playlist metadata:")
                print(json.dumps(current, ensure_ascii=False, indent=2))
                print("Update playlist metadata:")
                if changed_fields:
                    print(json.dumps(changed_fields, ensure_ascii=False, indent=2))
                else:
                    print("Nothing changes.")

                confirm = input("Execute update? [y/N]: ").strip().lower()
                if confirm not in {"y", "yes"}:
                    print("Update cancelled.")
                    return 0

                result = update_youtube_playlist(
                    playlist_id,
                    title=getattr(args, "title", None),
                    description=getattr(args, "description", None),
                    privacy_status=getattr(args, "privacy_status", None),
                )
            elif args.youtube_command == "playlist":
                if getattr(args, "id", None):
                    result = get_youtube_playlist(str(args.id))
                else:
                    result = list_youtube_playlists()
                    if getattr(args, "title_only", False):
                        result = [{"id": item.get("id", ""), "title": item.get("title", "")} for item in result]
            elif args.youtube_command == "video" and getattr(args, "video_action", None) == "update":
                video_id = str(getattr(args, "id", "") or "").strip()
                if not video_id:
                    parser.error("youtube video update requires --id")

                current = get_youtube_video(video_id)
                update_fields = {
                    "title": getattr(args, "title", None),
                    "description": getattr(args, "description", None),
                    "privacy_status": getattr(args, "privacy_status", None),
                    "made_for_kids": getattr(args, "made_for_kids", None),
                    "playlist_position": getattr(args, "playlist_position", None),
                }
                changed_fields = {
                    key: value
                    for key, value in update_fields.items()
                    if value is not None and value != current.get(key)
                }

                LOGGER.info("Current video metadata: %s", json.dumps(current, ensure_ascii=False))
                LOGGER.info("Updated video fields: %s", json.dumps(changed_fields, ensure_ascii=False))
                print("Current video metadata:")
                print(json.dumps(current, ensure_ascii=False, indent=2))
                print("Update video metadata:")
                if changed_fields:
                    print(json.dumps(changed_fields, ensure_ascii=False, indent=2))
                else:
                    print("Nothing changes.")

                confirm = input("Execute update? [y/N]: ").strip().lower()
                if confirm not in {"y", "yes"}:
                    print("Update cancelled.")
                    return 0

                result = update_youtube_video(
                    video_id,
                    title=getattr(args, "title", None),
                    description=getattr(args, "description", None),
                    privacy_status=getattr(args, "privacy_status", None),
                    made_for_kids=getattr(args, "made_for_kids", None),
                    playlist_position=getattr(args, "playlist_position", None),
                )
            elif args.youtube_command == "video":
                if getattr(args, "id", None):
                    result = get_youtube_video(str(args.id))
                else:
                    result = list_youtube_videos()
                    if getattr(args, "title_only", False):
                        result = [{"id": item.get("id", ""), "title": item.get("title", "")} for item in result]
            else:
                parser.error("youtube: unsupported subcommand")
            LOGGER.info("YouTube %s result: %s", args.youtube_command, json.dumps(result, ensure_ascii=False))
            print(json.dumps(result, ensure_ascii=False, indent=2))
            return 0

        if args.command == "pipeline":
            from novel_tts.crawl import crawl_range
            from novel_tts.queue import add_jobs_to_queue, launch_queue_stack, wait_for_range_completion
            from novel_tts.pipeline import run_watch_pipeline

            if args.pipeline_command == "watch":
                watch_stage_flags = _resolve_watch_stage_flags(args)
                return run_watch_pipeline(
                    repo_root=_repo_root(),
                    novel_ids=list(getattr(args, "novel_ids", []) or []),
                    watch_all=bool(getattr(args, "all", False)),
                    interval_seconds=getattr(args, "interval_seconds", None),
                    once=bool(getattr(args, "once", False)),
                    upload_platform_override=getattr(args, "upload_platform", None),
                    restart_queue=getattr(args, "restart_queue", None),
                    bootstrap_from=getattr(args, "bootstrap_from", None),
                    skip_crawl=watch_stage_flags["crawl"],
                    skip_translate=watch_stage_flags["translate"],
                    skip_repair=watch_stage_flags["repair"],
                    skip_polish=watch_stage_flags["polish"],
                    skip_tts=watch_stage_flags["tts"],
                    skip_visual=watch_stage_flags["visual"],
                    skip_video=watch_stage_flags["video"],
                    skip_upload=watch_stage_flags["upload"],
                )

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
                launch_queue_stack(config, restart=False, add_queue=False)
                add_jobs_to_queue(config, start, end)
                wait_for_range_completion(config, start, end)
            translated_ranges = get_translated_ranges(config, start, end)
            upload_platform = str(
                getattr(args, "upload_platform", None) or getattr(config.upload, "default_platform", "youtube")
            )
            pipeline_mode = str(getattr(args, "mode", "per-stage") or "per-stage")
            if pipeline_mode == "per-video":
                _run_pipeline_per_video(
                    config,
                    translated_ranges=translated_ranges,
                    skip_tts=bool(args.skip_tts),
                    skip_visual=bool(args.skip_visual),
                    skip_video=bool(args.skip_video),
                    skip_upload=bool(args.skip_upload),
                    upload_platform=upload_platform,
                )
            else:
                _run_pipeline_per_stage(
                    config,
                    translated_ranges=translated_ranges,
                    skip_tts=bool(args.skip_tts),
                    skip_visual=bool(args.skip_visual),
                    skip_video=bool(args.skip_video),
                    skip_upload=bool(args.skip_upload),
                    upload_platform=upload_platform,
                )
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

        if args.command == "quota-supervisor":
            from novel_tts.quota.supervisor import run_quota_supervisor

            if getattr(args, "restart", False):
                # Implement as stop then start daemon so operators can pick up code changes quickly.
                args.stop = True
                args.daemon = True

            if getattr(args, "stop", False):
                import signal

                repo_root = Path(__file__).resolve().parents[2]
                pid_file = repo_root / ".logs" / "quota-supervisor.pid"
                killed: list[int] = []

                def _kill(pid: int) -> bool:
                    try:
                        os.kill(pid, signal.SIGTERM)
                        return True
                    except Exception:
                        return False

                if pid_file.exists():
                    try:
                        pid = int(pid_file.read_text(encoding="utf-8").strip() or "0")
                    except Exception:
                        pid = 0
                    if pid > 1 and _kill(pid):
                        killed.append(pid)
                        try:
                            pid_file.unlink(missing_ok=True)
                        except Exception:
                            pass
                else:
                    # Best-effort: scan process table for quota-supervisor instances.
                    try:
                        proc = subprocess.run(
                            ["ps", "ax", "-o", "pid=,command="],
                            cwd=str(repo_root),
                            check=False,
                            capture_output=True,
                            text=True,
                        )
                        if proc.returncode == 0:
                            for line in (proc.stdout or "").splitlines():
                                line = line.strip()
                                if not line:
                                    continue
                                try:
                                    pid_str, cmd = line.split(None, 1)
                                    pid = int(pid_str.strip())
                                except Exception:
                                    continue
                                if "novel_tts" not in cmd or "quota-supervisor" not in cmd:
                                    continue
                                if pid > 1 and _kill(pid):
                                    killed.append(pid)
                    except Exception:
                        pass

                if killed:
                    killed_sorted = " ".join(str(p) for p in sorted(set(killed)))
                    print(f"quota-supervisor stopped pid={killed_sorted}")
                    if not getattr(args, "daemon", False):
                        return 0
                else:
                    print("quota-supervisor: no running process found")
                    if not getattr(args, "daemon", False):
                        return 0

            if getattr(args, "daemon", False):
                repo_root = Path(__file__).resolve().parents[2]
                log_path = Path(getattr(args, "log_file", "") or "").expanduser().resolve() if getattr(args, "log_file", None) else None
                if log_path is None:
                    log_path = repo_root / ".logs" / "quota-supervisor.log"
                log_path.parent.mkdir(parents=True, exist_ok=True)
                # Spawn a detached process that re-enters the CLI without --daemon to avoid recursion.
                cmd = [
                    sys.executable,
                    "-m",
                    "novel_tts",
                    "--log-file",
                    str(log_path),
                    "quota-supervisor",
                    "--poll-interval-seconds",
                    str(float(args.poll_interval_seconds)),
                ]
                # The child process logs to --log-file via logging.FileHandler. Avoid also redirecting stdout/stderr
                # to the same file to prevent duplicate log lines (stream handler + file handler).
                proc = subprocess.Popen(
                    cmd,
                    cwd=str(repo_root),
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    start_new_session=True,
                )
                try:
                    pid_file = repo_root / ".logs" / "quota-supervisor.pid"
                    pid_file.parent.mkdir(parents=True, exist_ok=True)
                    pid_file.write_text(str(proc.pid) + "\n", encoding="utf-8")
                except Exception:
                    pass
                print(f"quota-supervisor started (daemon) pid={proc.pid} log={log_path}")
                return 0

            return run_quota_supervisor(poll_interval_seconds=float(args.poll_interval_seconds))

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
