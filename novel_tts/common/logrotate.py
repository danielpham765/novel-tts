from __future__ import annotations

import json
import os
import re
import shutil
import time
import zipfile
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, tzinfo
from pathlib import Path

from novel_tts.common.logging import get_logger

LOGGER = get_logger(__name__)

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_ZIP_RE = re.compile(r"^(?P<date>\d{4}-\d{2}-\d{2})\.zip$")


def _local_now() -> datetime:
    return datetime.now().astimezone()


def _date_str(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")


def _parse_date_str(value: str) -> datetime | None:
    if not value or not _DATE_RE.match(value.strip()):
        return None
    try:
        return datetime.strptime(value.strip(), "%Y-%m-%d")
    except Exception:
        return None


def _mtime_date_str(path: Path, *, tz: tzinfo) -> str:
    try:
        st = path.stat()
    except FileNotFoundError:
        return ""
    dt = datetime.fromtimestamp(st.st_mtime, tz=tz)
    return _date_str(dt)


def _is_log_file(path: Path) -> bool:
    return path.is_file() and path.name.endswith(".log")


def _is_under(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
        return True
    except Exception:
        return False


def _ensure_empty_file(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        f.write("")


def _compute_archived_name(src_name: str, idx: int) -> str:
    if src_name.endswith(".log"):
        stem = src_name[:-4]
        return f"{stem}_{idx:03}.log"
    return f"{src_name}_{idx:03}"


def _next_index(dest_parent: Path, src_name: str) -> int:
    """
    Find next available _NNN suffix under dest_parent for src_name.
    """
    if not dest_parent.exists():
        return 0
    base = src_name
    if base.endswith(".log"):
        base = base[:-4]
    prefix = f"{base}_"
    max_idx = -1
    try:
        for entry in dest_parent.iterdir():
            name = entry.name
            if not name.startswith(prefix) or not name.endswith(".log"):
                continue
            mid = name[len(prefix) : -4]
            if len(mid) != 3 or not mid.isdigit():
                continue
            max_idx = max(max_idx, int(mid))
    except FileNotFoundError:
        return 0
    return max_idx + 1


def _rotate_file_to_folder(
    *,
    src: Path,
    logs_root: Path,
    dest_folder: Path,
) -> Path | None:
    if not _is_log_file(src):
        return None
    if not src.exists():
        return None
    if _is_under(src, dest_folder):
        return None
    try:
        rel = src.relative_to(logs_root)
    except Exception:
        return None

    dest_parent = dest_folder / rel.parent
    idx = _next_index(dest_parent, src.name)
    dest_name = _compute_archived_name(src.name, idx)
    dest = dest_parent / dest_name
    dest.parent.mkdir(parents=True, exist_ok=True)

    try:
        os.replace(src, dest)
    except FileNotFoundError:
        return None
    except Exception as exc:
        LOGGER.warning("Failed to rotate log file %s -> %s: %s", src, dest, exc)
        return None

    try:
        _ensure_empty_file(src)
    except Exception as exc:
        LOGGER.warning("Failed to recreate empty log file after rotate: %s (%s)", src, exc)
    return dest


@contextmanager
def _file_lock(lock_path: Path):
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        import fcntl  # type: ignore
    except Exception:
        yield
        return

    f = lock_path.open("a", encoding="utf-8")
    try:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        yield
    finally:
        try:
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)
        except Exception:
            pass
        try:
            f.close()
        except Exception:
            pass


@dataclass(frozen=True)
class RotateState:
    today_folder_date: str
    last_housekeeping_date: str


def _state_path(archived_root: Path) -> Path:
    return archived_root / ".rotate_state.json"


def load_state(archived_root: Path, *, now: datetime | None = None) -> RotateState:
    now = now or _local_now()
    today = _date_str(now)
    path = _state_path(archived_root)
    if not path.exists():
        return RotateState(today_folder_date=today, last_housekeeping_date=today)
    try:
        payload = json.loads(path.read_text(encoding="utf-8") or "{}")
    except Exception:
        payload = {}
    tfd = str(payload.get("today_folder_date") or "").strip() or today
    lhd = str(payload.get("last_housekeeping_date") or "").strip() or today
    if not _DATE_RE.match(tfd):
        tfd = today
    if not _DATE_RE.match(lhd):
        lhd = today
    return RotateState(today_folder_date=tfd, last_housekeeping_date=lhd)


def save_state(archived_root: Path, state: RotateState) -> None:
    path = _state_path(archived_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "today_folder_date": state.today_folder_date,
        "last_housekeeping_date": state.last_housekeeping_date,
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def ensure_archived_layout(logs_root: Path) -> Path:
    archived_root = logs_root / "archived"
    (archived_root / "today").mkdir(parents=True, exist_ok=True)
    (archived_root / "zip").mkdir(parents=True, exist_ok=True)
    return archived_root


def _roll_today_folder(
    *,
    archived_root: Path,
    state: RotateState,
    now: datetime,
) -> RotateState:
    today_str = _date_str(now)
    if state.today_folder_date == today_str:
        return state
    prev_date = state.today_folder_date
    today_dir = archived_root / "today"
    if _DATE_RE.match(prev_date) and today_dir.exists():
        dest = archived_root / prev_date
        if not dest.exists():
            try:
                os.replace(today_dir, dest)
            except Exception:
                # If rename fails (e.g., cross-device), fallback to copytree+delete.
                try:
                    shutil.copytree(today_dir, dest, dirs_exist_ok=True)
                    shutil.rmtree(today_dir, ignore_errors=True)
                except Exception:
                    pass
        else:
            # Merge into existing date folder and reset today.
            try:
                shutil.copytree(today_dir, dest, dirs_exist_ok=True)
                shutil.rmtree(today_dir, ignore_errors=True)
            except Exception:
                pass
    (archived_root / "today").mkdir(parents=True, exist_ok=True)
    return RotateState(today_folder_date=today_str, last_housekeeping_date=state.last_housekeeping_date)


def _iter_log_files(logs_root: Path) -> list[Path]:
    archived_root = logs_root / "archived"
    out: list[Path] = []
    if not logs_root.exists():
        return out
    for root, dirs, files in os.walk(logs_root):
        root_path = Path(root)
        if _is_under(root_path, archived_root):
            dirs[:] = []
            continue
        for name in files:
            if not name.endswith(".log"):
                continue
            out.append(root_path / name)
    return out


def rotate_old_logs_to_date_folders(*, logs_root: Path, now: datetime | None = None) -> int:
    now = now or _local_now()
    tz = now.tzinfo or _local_now().tzinfo
    assert tz is not None
    archived_root = ensure_archived_layout(logs_root)
    rotated = 0
    today_str = _date_str(now)
    for path in _iter_log_files(logs_root):
        d = _mtime_date_str(path, tz=tz)
        if not d or d >= today_str:
            continue
        if not _DATE_RE.match(d):
            continue
        dest_folder = archived_root / d
        moved = _rotate_file_to_folder(src=path, logs_root=logs_root, dest_folder=dest_folder)
        if moved is not None:
            rotated += 1
    return rotated


def rotate_large_logs_to_today(*, logs_root: Path, size_threshold_bytes: int, now: datetime | None = None) -> int:
    now = now or _local_now()
    tz = now.tzinfo or _local_now().tzinfo
    assert tz is not None
    archived_root = ensure_archived_layout(logs_root)
    today_str = _date_str(now)
    rotated = 0
    today_folder = archived_root / "today"
    for path in _iter_log_files(logs_root):
        try:
            st = path.stat()
        except FileNotFoundError:
            continue
        d = _mtime_date_str(path, tz=tz)
        if d != today_str:
            continue
        if int(st.st_size) <= int(size_threshold_bytes):
            continue
        moved = _rotate_file_to_folder(src=path, logs_root=logs_root, dest_folder=today_folder)
        if moved is not None:
            rotated += 1
    return rotated


def rotate_novel_logs_to_today(*, logs_root: Path, novel_id: str, now: datetime | None = None) -> int:
    now = now or _local_now()
    archived_root = ensure_archived_layout(logs_root)
    today_folder = archived_root / "today"
    rotated = 0
    novel_root = logs_root / str(novel_id)
    if not novel_root.exists():
        return 0
    for root, dirs, files in os.walk(novel_root):
        root_path = Path(root)
        for name in files:
            if not name.endswith(".log"):
                continue
            src = root_path / name
            moved = _rotate_file_to_folder(src=src, logs_root=logs_root, dest_folder=today_folder)
            if moved is not None:
                rotated += 1
    return rotated


def rotate_log_file_to_today(*, logs_root: Path, src: Path) -> Path | None:
    """
    Rotate a single log file into archived/today and recreate an empty source file.

    Preserves relative path under logs_root, adding _NNN suffix to avoid collisions.
    """
    if not _is_log_file(src) or not src.exists():
        return None
    try:
        if src.stat().st_size <= 0:
            return None
    except FileNotFoundError:
        return None

    archived_root = ensure_archived_layout(logs_root)
    today_folder = archived_root / "today"
    return _rotate_file_to_folder(src=src, logs_root=logs_root, dest_folder=today_folder)


def _zip_date_folder(*, archived_root: Path, date_folder: Path, zip_root: Path) -> Path | None:
    date_str = date_folder.name
    if not _DATE_RE.match(date_str):
        return None
    if not date_folder.is_dir():
        return None

    files: list[Path] = []
    log_files: list[Path] = []
    any_log_nonempty = False
    for root, _dirs, names in os.walk(date_folder):
        root_path = Path(root)
        for name in names:
            p = root_path / name
            if not p.is_file():
                continue
            files.append(p)
            if p.name.endswith(".log"):
                log_files.append(p)
            try:
                if p.name.endswith(".log") and p.stat().st_size > 0:
                    any_log_nonempty = True
            except FileNotFoundError:
                continue

    # Skip zip when the folder has no log files, or when all log files are empty.
    if (not log_files) or (not any_log_nonempty):
        return None

    zip_root.mkdir(parents=True, exist_ok=True)
    target = zip_root / f"{date_str}.zip"
    tmp = zip_root / f".{date_str}.{int(time.time())}.zip.tmp"

    with zipfile.ZipFile(tmp, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for p in files:
            try:
                arcname = str(p.relative_to(date_folder))
            except Exception:
                arcname = p.name
            try:
                zf.write(p, arcname=arcname)
            except FileNotFoundError:
                continue
    os.replace(tmp, target)
    return target


def prune_zip_folder(*, zip_root: Path, max_files: int = 4) -> int:
    if not zip_root.exists():
        return 0
    items: list[tuple[datetime, Path]] = []
    for entry in zip_root.iterdir():
        if not entry.is_file():
            continue
        m = _ZIP_RE.match(entry.name)
        if not m:
            continue
        dt = _parse_date_str(m.group("date"))
        if dt is None:
            continue
        items.append((dt, entry))
    items.sort(key=lambda t: t[0], reverse=True)
    to_delete = items[max_files:]
    deleted = 0
    for _dt, path in to_delete:
        try:
            path.unlink(missing_ok=True)
            deleted += 1
        except Exception:
            continue
    return deleted


def housekeeping_archived(*, logs_root: Path, now: datetime | None = None) -> None:
    now = now or _local_now()
    archived_root = ensure_archived_layout(logs_root)
    lock_path = archived_root / ".rotate.lock"

    with _file_lock(lock_path):
        state = load_state(archived_root, now=now)
        state = _roll_today_folder(archived_root=archived_root, state=state, now=now)

        today = _date_str(now)
        yesterday = _date_str(now - timedelta(days=1))
        day_before = _date_str(now - timedelta(days=2))

        # Ensure yesterday/day_before exist so the archived folder set is stable.
        for d in (yesterday, day_before):
            if _DATE_RE.match(d):
                (archived_root / d).mkdir(parents=True, exist_ok=True)

        # Zip any date folders older than day_before, then remove them.
        zip_root = archived_root / "zip"
        keep = {"today", "zip", yesterday, day_before}
        for entry in list(archived_root.iterdir()):
            if not entry.is_dir():
                continue
            name = entry.name
            if name in keep:
                continue
            if not _DATE_RE.match(name):
                # Unknown folder; best-effort remove to keep invariant.
                try:
                    shutil.rmtree(entry, ignore_errors=True)
                except Exception:
                    pass
                continue

            # Only yesterday/day_before are allowed date folders. Everything else must go away:
            # - older than day_before => zip (unless empty), then delete
            # - newer (including "today" date) => merge into archived/today, then delete
            if name >= day_before:
                if name in {yesterday, day_before}:
                    continue
                try:
                    shutil.copytree(entry, archived_root / "today", dirs_exist_ok=True)
                except Exception:
                    pass
                try:
                    shutil.rmtree(entry, ignore_errors=True)
                except Exception:
                    pass
                continue

            created_zip: Path | None = None
            try:
                created_zip = _zip_date_folder(archived_root=archived_root, date_folder=entry, zip_root=zip_root)
            except Exception as exc:
                LOGGER.warning("Failed to zip archived folder %s: %s", entry, exc)
                created_zip = None

            # Remove folder regardless (even if zip was skipped due to empty logs).
            try:
                shutil.rmtree(entry, ignore_errors=True)
            except Exception:
                pass
            if created_zip is not None:
                LOGGER.info("Archived logs zipped: %s", created_zip)

        prune_zip_folder(zip_root=zip_root, max_files=4)
        state = RotateState(today_folder_date=state.today_folder_date, last_housekeeping_date=today)
        save_state(archived_root, state)


def logrotate_tick(
    *,
    repo_root: Path,
    now: datetime | None = None,
    size_threshold_bytes: int = 10 * 1024 * 1024,
    run_housekeeping: bool = False,
) -> dict[str, int]:
    """
    One best-effort maintenance tick.
    Intended to be called by quota-supervisor on a timer.
    """
    now = now or _local_now()
    logs_root = repo_root / ".logs"
    logs_root.mkdir(parents=True, exist_ok=True)
    result = {"rotated_old": 0, "rotated_large": 0, "housekeeping": 0}
    try:
        result["rotated_old"] = rotate_old_logs_to_date_folders(logs_root=logs_root, now=now)
    except Exception as exc:
        LOGGER.warning("rotate_old_logs_to_date_folders failed: %s", exc)
    try:
        result["rotated_large"] = rotate_large_logs_to_today(
            logs_root=logs_root,
            size_threshold_bytes=int(size_threshold_bytes),
            now=now,
        )
    except Exception as exc:
        LOGGER.warning("rotate_large_logs_to_today failed: %s", exc)
    if run_housekeeping:
        try:
            housekeeping_archived(logs_root=logs_root, now=now)
            result["housekeeping"] = 1
        except Exception as exc:
            LOGGER.warning("housekeeping_archived failed: %s", exc)
    return result
