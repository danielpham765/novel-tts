from __future__ import annotations

import json
import math
import os
import re
import shutil
import subprocess
import time
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, TypeVar
from urllib.parse import parse_qs, urlparse
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

import redis
from novel_tts.common.logging import get_logger
from novel_tts.config.loader import _load_app_config
from novel_tts.config.models import NovelConfig, UploadYouTubeConfig
from novel_tts.media_batch import count_media_batches_before, find_media_range_by_episode, media_range_key

LOGGER = get_logger(__name__)

YOUTUBE_BULK_UPDATE_BATCH_SIZE = 5
YOUTUBE_BULK_UPDATE_SLEEP_SECONDS = 2.0
YOUTUBE_PLAYLIST_REORDER_SLEEP_SECONDS = 1.0
YOUTUBE_RATE_LIMIT_REASONS = {
    "rateLimitExceeded",
    "uploadRateLimitExceeded",
    "userRateLimitExceeded",
}
YOUTUBE_ROTATE_ACCOUNT_REASONS = {
    "quotaExceeded",
}

YOUTUBE_UPLOAD_SCOPES = [
    "https://www.googleapis.com/auth/youtube.upload",
    "https://www.googleapis.com/auth/youtube",
]
YOUTUBE_QUOTA_BUCKET_ID = "youtube.googleapis.com|youtube.googleapis.com/default|1/d/{project}|"
YOUTUBE_QUOTA_CONSOLE_URL = "https://console.cloud.google.com/apis/api/youtube.googleapis.com/quotas?project={project_id}"
YOUTUBE_QUOTA_SESSION_FILE = ".secrets/youtube/quota_session.json"
YOUTUBE_QUOTA_COST_VIDEO_INSERT = 100
YOUTUBE_QUOTA_COST_THUMBNAIL_SET = 50
YOUTUBE_QUOTA_COST_PLAYLIST_ITEM_INSERT = 50
YOUTUBE_QUOTA_COST_PLAYLISTS_LIST = 1
YOUTUBE_QUOTA_COST_PLAYLIST_ITEMS_LIST = 1
YOUTUBE_QUOTA_COST_VIDEOS_LIST = 1
YOUTUBE_QUOTA_COST_CHANNELS_LIST = 1
YOUTUBE_QUOTA_COST_PLAYLIST_UPDATE = 50
YOUTUBE_QUOTA_COST_PLAYLIST_ITEM_UPDATE = 50
YOUTUBE_QUOTA_COST_VIDEO_UPDATE = 50
YOUTUBE_QUOTA_COST_VIDEO_DELETE = 50
YOUTUBE_QUOTA_COST_PLAYLIST_ITEM_DELETE = 50
YOUTUBE_QUOTA_REDIS_NAMESPACE = "youtube:quota:v1"
PACIFIC_TZ = ZoneInfo("America/Los_Angeles")
YOUTUBE_QUOTA_UPLOAD_COMMIT_COST = (
    YOUTUBE_QUOTA_COST_VIDEO_INSERT + YOUTUBE_QUOTA_COST_THUMBNAIL_SET + YOUTUBE_QUOTA_COST_PLAYLIST_ITEM_INSERT
)


def _range_key(start: int, end: int) -> str:
    return media_range_key(start, end)


def _read_required_text(path: Path, *, field_name: str) -> str:
    if not path.exists():
        raise FileNotFoundError(f"Missing {field_name} file: {path}")
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        raise ValueError(f"Empty {field_name} file: {path}")
    return text


def _resolve_output_file(config: NovelConfig, raw_path: str, *, field_name: str) -> Path:
    path = Path(str(raw_path or "").strip()).expanduser()
    if not str(path):
        raise ValueError(f'Missing upload config path for "{field_name}"')
    if path.is_absolute():
        return path
    return config.storage.output_dir / path


def _parse_playlist_id(raw: str) -> str:
    value = str(raw or "").strip()
    if not value:
        raise ValueError("Playlist value is empty")
    if "://" not in value:
        if "/" in value:
            raise ValueError(f"Invalid playlist id: {value!r}")
        return value
    parsed = urlparse(value)
    query = parse_qs(parsed.query or "")
    playlist = (query.get("list") or [""])[0].strip()
    if not playlist:
        raise ValueError(f"Playlist URL does not contain list=...: {value}")
    return playlist


def _resolve_title_with_index(config: NovelConfig, raw_title: str, start: int, end: int) -> str:
    title = str(raw_title or "").strip()
    if not title:
        return title
    index = count_media_batches_before(config, start) + 1
    # Preferred: explicit placeholders in title file.
    if "{index}" in title:
        return title.replace("{index}", str(index))
    if "{start}" in title:
        title = title.replace("{start}", str(start))
    if "{end}" in title:
        title = title.replace("{end}", str(end))
    # Backward compatible: rewrite leading "Tap/Tập <n>" token if present.
    return re.sub(r"\b(T(?:ập|ap))\s+\d+\b", rf"\1 {index}", title, count=1, flags=re.IGNORECASE)


def _normalize_title(value: str) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip()).casefold()


def _normalize_search_text(value: str) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.casefold()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _extract_episode_number(value: str) -> int | None:
    normalized = _normalize_search_text(value)
    match = re.search(r"\btap\s+(\d+)\b", normalized)
    if match is None:
        return None
    return int(match.group(1))


def _read_title_template(config: NovelConfig) -> str:
    title_path = _resolve_output_file(config, config.upload.youtube.title_file, field_name="upload.youtube.title_file")
    return _read_required_text(title_path, field_name="title")


def _youtube_quota_session_path(
    path: str | os.PathLike[str] | None = None,
    *,
    slot: int | None = None,
) -> Path:
    if path:
        return Path(path).expanduser()
    if slot is not None:
        safe_slot = int(slot)
        if safe_slot < 1:
            raise ValueError("session slot must be >= 1")
        return _repo_root() / ".secrets" / "youtube" / f"quota_session-{safe_slot}.json"
    return _repo_root() / YOUTUBE_QUOTA_SESSION_FILE


def _youtube_client_secret_path_for_slot(slot: int) -> Path:
    safe_slot = int(slot)
    if safe_slot < 1:
        raise ValueError("session slot must be >= 1")
    return _repo_root() / ".secrets" / "youtube" / f"client_secrets-{safe_slot}.json"


def _project_id_from_client_secret_slot(slot: int) -> str:
    path = _youtube_client_secret_path_for_slot(slot)
    if not path.exists():
        raise FileNotFoundError(f"Missing YouTube client secret file for slot {slot}: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or not payload:
        raise ValueError(f"Invalid YouTube client secret payload: {path}")
    root = next(iter(payload.values()))
    if not isinstance(root, dict):
        raise ValueError(f"Invalid YouTube client secret structure: {path}")
    project_id = str(root.get("project_id", "") or "").strip()
    if not project_id:
        raise ValueError(f"Missing project_id in YouTube client secret file: {path}")
    return project_id


@dataclass
class UploadSpec:
    platform: str
    range_key: str
    video_path: Path
    thumbnail_path: Path
    title: str
    description: str
    playlist_id: str



@dataclass(frozen=True)
class YouTubeAccountPaths:
    index: int
    credentials_path: Path
    token_path: Path

    @property
    def label(self) -> str:
        return self.token_path.name or f"account-{self.index}"


T = TypeVar("T")


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _parse_iso_datetime(value: object) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text)
    except Exception:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _next_youtube_quota_reset(after: datetime | None = None) -> datetime:
    base = (after or _utcnow()).astimezone(PACIFIC_TZ)
    candidate = base.replace(hour=0, minute=0, second=0, microsecond=0)
    if candidate <= base:
        from datetime import timedelta

        candidate = candidate + timedelta(days=1)
    return candidate.astimezone(timezone.utc)


def _load_youtube_redis_cfg() -> tuple[str, int, int, str]:
    app_raw = _load_app_config()
    queue_raw = app_raw.get("queue", {}) or {}
    redis_raw = queue_raw.get("redis", {}) or {}
    host = str(redis_raw.get("host") or "").strip() or "127.0.0.1"
    port = int(redis_raw.get("port") or 6379)
    database = int(redis_raw.get("database") or 0)
    prefix = str(redis_raw.get("prefix") or "").strip() or "novel_tts"
    return host, port, database, prefix


def _youtube_quota_redis_client() -> redis.Redis | None:
    try:
        host, port, database, _prefix = _load_youtube_redis_cfg()
        return redis.Redis(host=host, port=port, db=database, decode_responses=True)
    except Exception:
        return None


def _youtube_quota_cache_key(slot: int) -> str:
    _host, _port, _database, prefix = _load_youtube_redis_cfg()
    return f"{prefix}:{YOUTUBE_QUOTA_REDIS_NAMESPACE}:slot:{int(slot)}"


def _normalize_cached_quota_record(payload: dict[str, object]) -> dict[str, object]:
    normalized = dict(payload)
    now = _utcnow()
    next_reset = _parse_iso_datetime(normalized.get("next_reset_time"))
    if next_reset is None:
        next_reset = _next_youtube_quota_reset(now)
        normalized["next_reset_time"] = next_reset.isoformat()

    effective_limit = int(normalized.get("effective_limit", 0) or 0)
    current_usage = int(normalized.get("current_usage", 0) or 0)
    if now >= next_reset:
        current_usage = 0
        normalized["current_usage"] = 0
        normalized["remaining"] = effective_limit
        normalized["next_reset_time"] = _next_youtube_quota_reset(now).isoformat()
        normalized["source"] = "cache-reset"
        normalized["cache_reset_time"] = now.isoformat()
    else:
        normalized["remaining"] = max(0, effective_limit - current_usage)
    return normalized


def _read_cached_youtube_quota(slot: int) -> dict[str, object] | None:
    client = _youtube_quota_redis_client()
    if client is None:
        return None
    try:
        raw = client.get(_youtube_quota_cache_key(slot))
    except Exception:
        return None
    if not raw:
        return None
    try:
        payload = json.loads(raw)
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    normalized = _normalize_cached_quota_record(payload)
    if normalized != payload:
        _write_cached_youtube_quota(slot, normalized)
    return normalized


def _write_cached_youtube_quota(slot: int, payload: dict[str, object]) -> None:
    client = _youtube_quota_redis_client()
    if client is None:
        return
    data = dict(payload)
    if "next_reset_time" not in data:
        data["next_reset_time"] = _next_youtube_quota_reset().isoformat()
    try:
        client.set(_youtube_quota_cache_key(slot), json.dumps(data, ensure_ascii=False))
    except Exception:
        return


def get_youtube_quota_redis(*, session_slot: int) -> dict[str, object]:
    cached = _read_cached_youtube_quota(int(session_slot))
    if cached is None:
        raise FileNotFoundError(f"No cached YouTube quota found in Redis for slot {session_slot}")
    payload = dict(cached)
    payload["session_slot"] = int(session_slot)
    payload["estimated_uploads_remaining"] = (
        int(int(payload.get("remaining", 0) or 0) // YOUTUBE_QUOTA_UPLOAD_COMMIT_COST)
        if YOUTUBE_QUOTA_UPLOAD_COMMIT_COST > 0
        else 0
    )
    payload["reset_policy"] = "midnight Pacific Time (PT)"
    return payload


def get_all_youtube_quota_redis() -> dict[str, object]:
    app_raw = _load_app_config()
    upload_raw = app_raw.get("upload", {}) or {}
    youtube_raw = upload_raw.get("youtube", {}) or {}
    accounts = _youtube_accounts_from_raw(youtube_raw, root=_repo_root())
    results: list[dict[str, object]] = []
    for account in accounts:
        try:
            payload = _get_or_refresh_youtube_quota_for_slot(account.index)
            payload = dict(payload)
            payload["label"] = account.label
            payload["session_slot"] = account.index
            payload["reset_policy"] = "midnight Pacific Time (PT)"
            payload["estimated_uploads_remaining"] = (
                int(int(payload.get("remaining", 0) or 0) // YOUTUBE_QUOTA_UPLOAD_COMMIT_COST)
                if YOUTUBE_QUOTA_UPLOAD_COMMIT_COST > 0
                else 0
            )
            results.append(payload)
        except Exception as exc:
            LOGGER.error(
                "Failed to refetch YouTube quota for slot %s: %s. Falling back to Redis cache.",
                account.index,
                exc,
            )
            cached = _read_cached_youtube_quota(account.index)
            if cached is not None:
                payload = dict(cached)
                payload["label"] = account.label
                payload["session_slot"] = account.index
                payload["reset_policy"] = "midnight Pacific Time (PT)"
                payload["estimated_uploads_remaining"] = (
                    int(int(payload.get("remaining", 0) or 0) // YOUTUBE_QUOTA_UPLOAD_COMMIT_COST)
                    if YOUTUBE_QUOTA_UPLOAD_COMMIT_COST > 0
                    else 0
                )
                results.append(payload)
            else:
                results.append(
                    {
                        "session_slot": account.index,
                        "label": account.label,
                        "status": "missing",
                        "estimated_uploads_remaining": 0,
                        "reset_policy": "midnight Pacific Time (PT)",
                    }
                )
    total_estimated_uploads_remaining = sum(int(item.get("estimated_uploads_remaining", 0) or 0) for item in results)
    return {
        "projects": results,
        "total_estimated_uploads_remaining": total_estimated_uploads_remaining,
        "upload_commit_cost": YOUTUBE_QUOTA_UPLOAD_COMMIT_COST,
        "reset_policy": "midnight Pacific Time (PT)",
    }


def _build_cached_quota_payload(slot: int, summary: dict[str, object], *, source: str, sync_ok: bool) -> dict[str, object]:
    now = _utcnow()
    effective_limit = int(summary.get("effective_limit", 0) or 0)
    current_usage = int(summary.get("current_usage", 0) or 0)
    return {
        "slot": int(slot),
        "project_id": str(summary.get("project_id", "") or "").strip(),
        "effective_limit": effective_limit,
        "current_usage": current_usage,
        "remaining": max(0, effective_limit - current_usage),
        "last_sync_time": now.isoformat(),
        "captured_at": str(summary.get("captured_at", "") or "").strip(),
        "next_reset_time": _next_youtube_quota_reset(now).isoformat(),
        "source": source,
        "sync_ok": bool(sync_ok),
    }


def _apply_estimated_quota_spend(slot: int, *, spent_units: int, reason: str) -> dict[str, object] | None:
    cached = _read_cached_youtube_quota(slot)
    if cached is None:
        return None
    updated = dict(cached)
    updated = _normalize_cached_quota_record(updated)
    spent = max(0, int(spent_units or 0))
    current_usage = int(updated.get("current_usage", 0) or 0) + spent
    effective_limit = int(updated.get("effective_limit", 0) or 0)
    updated["current_usage"] = current_usage
    updated["remaining"] = max(0, effective_limit - current_usage)
    updated["last_estimated_update_time"] = _utcnow().isoformat()
    updated["last_estimated_spend_units"] = spent
    updated["last_estimated_reason"] = reason
    updated["source"] = "estimated"
    updated["sync_ok"] = False
    _write_cached_youtube_quota(slot, updated)
    return updated


def _resolve_repo_relative_path(root: Path, raw_path: str) -> Path:
    path = Path(str(raw_path or "").strip()).expanduser()
    if not str(path):
        raise ValueError("Path value is empty")
    if not path.is_absolute():
        path = root / path
    return path


def _youtube_accounts_from_raw(youtube_raw: dict[str, object], *, root: Path) -> list[YouTubeAccountPaths]:
    credentials_raw = youtube_raw.get("credentials_path", [".secrets/youtube/client_secrets.json"])
    token_raw = youtube_raw.get("token_path", [".secrets/youtube/token.json"])
    if not isinstance(credentials_raw, list) or not credentials_raw:
        raise ValueError('Missing "upload.youtube.credentials_path" in configs/app.yaml (non-empty list required)')
    if not isinstance(token_raw, list) or not token_raw:
        raise ValueError('Missing "upload.youtube.token_path" in configs/app.yaml (non-empty list required)')
    if len(credentials_raw) != len(token_raw):
        raise ValueError(
            'Invalid YouTube account config: "upload.youtube.credentials_path" and '
            '"upload.youtube.token_path" must have the same number of entries'
        )

    accounts: list[YouTubeAccountPaths] = []
    for index, (credentials_value, token_value) in enumerate(zip(credentials_raw, token_raw), start=1):
        credentials_text = str(credentials_value or "").strip()
        token_text = str(token_value or "").strip()
        if not credentials_text:
            raise ValueError(f'Invalid "upload.youtube.credentials_path[{index - 1}]" in configs/app.yaml')
        if not token_text:
            raise ValueError(f'Invalid "upload.youtube.token_path[{index - 1}]" in configs/app.yaml')
        accounts.append(
            YouTubeAccountPaths(
                index=index,
                credentials_path=_resolve_repo_relative_path(root, credentials_text),
                token_path=_resolve_repo_relative_path(root, token_text),
            )
        )
    return accounts


def _youtube_accounts_from_defaults() -> list[YouTubeAccountPaths]:
    app_raw = _load_app_config()
    upload_raw = app_raw.get("upload", {}) or {}
    youtube_raw = upload_raw.get("youtube", {}) or {}
    return _youtube_accounts_from_raw(youtube_raw, root=_repo_root())


def _youtube_project_selector_from_defaults() -> str:
    app_raw = _load_app_config()
    upload_raw = app_raw.get("upload", {}) or {}
    youtube_raw = upload_raw.get("youtube", {}) or {}
    return str(youtube_raw.get("project", "rotate") or "rotate")


def _youtube_upload_cfg_from_defaults() -> UploadYouTubeConfig:
    app_raw = _load_app_config()
    upload_raw = app_raw.get("upload", {}) or {}
    youtube_raw = upload_raw.get("youtube", {}) or {}
    return UploadYouTubeConfig(**dict(youtube_raw))


def _selected_youtube_accounts_from_defaults() -> list[YouTubeAccountPaths]:
    return _select_youtube_accounts(
        _youtube_accounts_from_defaults(),
        project_selector=_youtube_project_selector_from_defaults(),
    )


def _youtube_accounts_from_config(config: NovelConfig) -> list[YouTubeAccountPaths]:
    return _youtube_accounts_from_raw(
        {
            "credentials_path": list(config.upload.youtube.credentials_path),
            "token_path": list(config.upload.youtube.token_path),
        },
        root=config.storage.root,
    )


def _select_youtube_accounts(accounts: list[YouTubeAccountPaths], *, project_selector: str) -> list[YouTubeAccountPaths]:
    if not accounts:
        raise ValueError("No YouTube accounts configured")
    selector = str(project_selector or "rotate").strip().lower()
    if selector in {"", "rotate"}:
        return accounts
    try:
        account_index = int(selector)
    except Exception as exc:
        raise ValueError('Invalid "upload.youtube.project" (expected "rotate" or positive integer)') from exc
    if account_index < 1:
        raise ValueError('Invalid "upload.youtube.project" (expected "rotate" or positive integer)')
    if account_index > len(accounts):
        raise ValueError(
            f'Invalid "upload.youtube.project": {account_index} (configured projects: 1-{len(accounts)})'
        )
    return [accounts[account_index - 1]]


def _extract_quota_summary(response_payload: object) -> dict[str, object]:
    rows: list[object] = []
    if isinstance(response_payload, list):
        for item in response_payload:
            if not isinstance(item, dict):
                continue
            rows.extend((((item.get("successfulResult") or {}).get("resultData") or {}).get("row") or []))
    elif isinstance(response_payload, dict):
        rows = (((response_payload.get("successfulResult") or {}).get("resultData") or {}).get("row") or [])

    quota_payload: dict[str, object] | None = None
    for row in rows:
        if not isinstance(row, dict):
            continue
        payload = row.get("payload") or {}
        row_id = str(row.get("id", "")).strip()
        payload_id = str((payload or {}).get("id", "")).strip() if isinstance(payload, dict) else ""
        if row_id == YOUTUBE_QUOTA_BUCKET_ID or payload_id == YOUTUBE_QUOTA_BUCKET_ID:
            quota_payload = payload if isinstance(payload, dict) else None
            break
    if quota_payload is None and rows:
        first_row = rows[0]
        if isinstance(first_row, dict) and isinstance(first_row.get("payload"), dict):
            quota_payload = first_row["payload"]
    if quota_payload is None:
        raise ValueError("YouTube quota response did not contain a quota bucket row")

    effective_limit = int(str(quota_payload.get("effectiveLimit", "0") or "0"))
    current_usage = int(quota_payload.get("currentUsage", 0) or 0)
    return {
        "id": str(quota_payload.get("id", "")).strip(),
        "service_name": str(quota_payload.get("serviceName", "")).strip(),
        "display_name": str(quota_payload.get("displayName", "")).strip(),
        "limit_name": str(quota_payload.get("limitName", "")).strip(),
        "limit_unit": str(quota_payload.get("limitUnit", "")).strip(),
        "effective_limit": effective_limit,
        "current_usage": current_usage,
        "remaining": max(0, effective_limit - current_usage),
        "current_percent": quota_payload.get("currentPercent", 0),
        "seven_day_peak_usage": quota_payload.get("sevenDayPeakUsage", 0),
        "allows_quota_increase_request": bool(quota_payload.get("allowsQuotaIncreaseRequest", False)),
    }


_CHROME_CANDIDATES = [
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    "/Applications/Chromium.app/Contents/MacOS/Chromium",
    "google-chrome",
    "google-chrome-stable",
    "chromium",
    "chromium-browser",
]


def _find_chrome_binary() -> str | None:
    for candidate in _CHROME_CANDIDATES:
        if os.path.isfile(candidate):
            return candidate
        found = shutil.which(candidate)
        if found:
            return found
    return None


def _ensure_chrome_debug_running(remote_debugging_url: str) -> subprocess.Popen | None:
    """Start Chrome with remote debugging if nothing is listening on the debug port.

    Returns the Popen handle if Chrome was launched (caller should terminate it),
    or None if a browser was already running.
    """
    from urllib.parse import urlparse as _urlparse
    from urllib.request import urlopen as _urlopen

    parsed = _urlparse(remote_debugging_url)
    host = parsed.hostname or "127.0.0.1"
    port = parsed.port or 9222
    probe_url = f"http://{host}:{port}/json/version"

    try:
        _urlopen(probe_url, timeout=2)
        return None  # already running
    except Exception:
        pass

    binary = _find_chrome_binary()
    if not binary:
        return None

    user_data_dir = Path.home() / ".novel_tts_chrome_debug"
    user_data_dir.mkdir(parents=True, exist_ok=True)
    cmd = [
        binary,
        f"--remote-debugging-port={port}",
        f"--user-data-dir={user_data_dir}",
        "--no-first-run",
        "--no-default-browser-check",
    ]
    LOGGER.info("Launching Chrome for remote debugging on port %s: %s", port, binary)
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        stdin=subprocess.DEVNULL,
    )
    # Wait until the debug endpoint is ready (up to 10 s)
    deadline = time.time() + 10.0
    while time.time() < deadline:
        try:
            _urlopen(probe_url, timeout=1)
            LOGGER.info("Chrome remote debugging is ready on port %s", port)
            return proc
        except Exception:
            time.sleep(0.5)
    LOGGER.warning("Chrome launched but debug endpoint did not become ready within 10 s")
    return proc


def _capture_quota_request_from_browser(
    *,
    project_id: str,
    remote_debugging_url: str,
    timeout_seconds: float,
) -> dict[str, object]:
    try:
        from playwright.sync_api import sync_playwright
    except Exception as exc:
        raise RuntimeError(
            "Playwright is required for `youtube quota`. Install dependencies and run `uv run playwright install chromium`."
        ) from exc

    quota_url = YOUTUBE_QUOTA_CONSOLE_URL.format(project_id=project_id)
    captured: dict[str, object] = {}

    chrome_proc = _ensure_chrome_debug_running(remote_debugging_url)

    with sync_playwright() as playwright:
        browser = playwright.chromium.connect_over_cdp(remote_debugging_url)
        try:
            context = browser.contexts[0] if browser.contexts else browser.new_context()
            page = context.pages[0] if context.pages else context.new_page()

            def _handle_request(request) -> None:
                if "QUOTAS_LIST_FLATTENED_QUOTA_BUCKETS:get" not in request.url:
                    return
                try:
                    headers = dict(request.all_headers())
                except Exception:
                    headers = dict(request.headers)
                post_data = request.post_data or ""
                try:
                    body = json.loads(post_data) if post_data else {}
                except Exception:
                    body = {"raw": post_data}
                captured["request"] = {
                    "url": request.url,
                    "headers": headers,
                    "body": body,
                }

            page.on("request", _handle_request)
            LOGGER.info("Capturing YouTube quota request via debug browser attach for project %s", project_id)
            page.goto(quota_url, wait_until="domcontentloaded", timeout=120000)
            page.wait_for_timeout(3000)

            deadline = time.time() + max(10.0, float(timeout_seconds))
            login_notice_shown = False
            while time.time() < deadline:
                request_payload = captured.get("request")
                if request_payload is not None:
                    return request_payload

                current_url = str(page.url or "")
                if "accounts.google.com" in current_url or "ServiceLogin" in current_url:
                    if not login_notice_shown:
                        LOGGER.warning(
                            "Debug browser requires Google sign-in for Cloud Console. "
                            "Complete login in the attached browser window; capture will keep waiting."
                        )
                        login_notice_shown = True
                    page.wait_for_timeout(1000)
                    continue
                if "console.cloud.google.com" in current_url and "apis/api/youtube.googleapis.com/quotas" not in current_url:
                    try:
                        page.goto(quota_url, wait_until="domcontentloaded", timeout=30000)
                    except Exception:
                        pass
                page.wait_for_timeout(1000)

            raise TimeoutError(
                "Timed out waiting to capture the YouTube quota request from the attached debug browser. "
                "Open the YouTube quota page in that browser, complete Google sign-in if needed, and ensure it is not blocked."
            )
        finally:
            try:
                browser.close()
            except Exception:
                pass
            if chrome_proc is not None:
                try:
                    chrome_proc.terminate()
                except Exception:
                    pass


def capture_youtube_quota_session(
    *,
    project_id: str = "",
    remote_debugging_url: str = "",
    timeout_seconds: float = 180.0,
    session_file: str | os.PathLike[str] | None = None,
    session_slot: int | None = None,
) -> dict[str, object]:
    resolved_project_id = str(project_id or os.environ.get("NOVEL_TTS_YOUTUBE_GCP_PROJECT_ID", "")).strip()
    if not resolved_project_id and session_slot is not None:
        resolved_project_id = _project_id_from_client_secret_slot(int(session_slot))
    if not resolved_project_id:
        raise ValueError(
            "Missing project id. Pass --project-id, set NOVEL_TTS_YOUTUBE_GCP_PROJECT_ID, "
            "or use --session-slot so project_id can be inferred from client_secrets-<slot>.json."
        )
    resolved_debug_url = str(
        remote_debugging_url or os.environ.get("NOVEL_TTS_BROWSER_DEBUG_URL", "http://127.0.0.1:9222")
    ).strip()
    if not resolved_debug_url:
        raise ValueError("Missing remote debugging URL. Pass --remote-debugging-url or set NOVEL_TTS_BROWSER_DEBUG_URL.")
    request_payload = _capture_quota_request_from_browser(
        project_id=resolved_project_id,
        remote_debugging_url=resolved_debug_url,
        timeout_seconds=timeout_seconds,
    )
    destination = _youtube_quota_session_path(session_file, slot=session_slot)
    destination.parent.mkdir(parents=True, exist_ok=True)

    session_payload = {
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "project_id": resolved_project_id,
        "remote_debugging_url": resolved_debug_url,
        "quota_request": request_payload,
    }
    destination.write_text(json.dumps(session_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    try:
        os.chmod(destination, 0o600)
    except Exception:
        pass
    return {
        "session_file": str(destination),
        "project_id": resolved_project_id,
        "captured_at": session_payload["captured_at"],
        "session_slot": session_slot,
    }


def _load_youtube_quota_session(
    session_file: str | os.PathLike[str] | None = None,
    *,
    session_slot: int | None = None,
) -> dict[str, object]:
    path = _youtube_quota_session_path(session_file, slot=session_slot)
    if not path.exists():
        raise FileNotFoundError(
            f"Missing YouTube quota session file: {path}. Run `novel-tts youtube quota capture --project-id ...` first."
        )
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Invalid YouTube quota session payload: {path}")
    return payload


def _perform_saved_quota_http_request(session_payload: dict[str, object]) -> object:
    quota_request = session_payload.get("quota_request") or {}
    if not isinstance(quota_request, dict):
        raise ValueError("Invalid quota_request payload in saved session file")
    request_url = str(quota_request.get("url", "") or "").strip()
    request_headers = quota_request.get("headers") or {}
    request_body = quota_request.get("body") or {}
    if not request_url:
        raise ValueError("Saved quota session does not contain a request URL")
    if not isinstance(request_headers, dict):
        raise ValueError("Saved quota session has invalid headers")
    if not isinstance(request_body, dict):
        raise ValueError("Saved quota session has invalid request body")

    filtered_headers = {
        str(key): str(value)
        for key, value in request_headers.items()
        if str(key).lower()
        in {
            "accept",
            "accept-language",
            "authorization",
            "content-type",
            "cookie",
            "origin",
            "referer",
            "user-agent",
            "x-goog-authuser",
            "x-goog-ext-353267353-jspb",
            "x-goog-first-party-reauth",
            "x-server-token",
        }
    }
    filtered_headers.setdefault("accept", "*/*")
    filtered_headers.setdefault("content-type", "application/json")

    request = Request(
        request_url,
        data=json.dumps(request_body, ensure_ascii=False, separators=(",", ":")).encode("utf-8"),
        headers=filtered_headers,
        method="POST",
    )
    with urlopen(request, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))


def _get_youtube_quota_live(
    *,
    session_file: str | os.PathLike[str] | None = None,
    session_slot: int | None = None,
) -> tuple[dict[str, object], object]:
    session_payload = _load_youtube_quota_session(session_file, session_slot=session_slot)
    payload = _perform_saved_quota_http_request(session_payload)
    summary = _extract_quota_summary(payload)
    summary["project_id"] = str(session_payload.get("project_id", "")).strip()
    summary["captured_at"] = str(session_payload.get("captured_at", "")).strip()
    summary["last_sync_time"] = _utcnow().isoformat()
    summary["session_slot"] = session_slot
    # Replace {project} placeholder in the id field with actual project_id
    if summary.get("project_id"):
        id_str = str(summary.get("id", "")).strip()
        summary["id"] = id_str.replace("{project}", summary["project_id"])
        summary["usage_url"] = YOUTUBE_QUOTA_CONSOLE_URL.format(project_id=summary["project_id"])
    if session_slot is not None:
        _write_cached_youtube_quota(
            int(session_slot),
            _build_cached_quota_payload(int(session_slot), summary, source="live", sync_ok=True),
        )
    return summary, payload


def get_youtube_quota(
    *,
    raw: bool = False,
    session_file: str | os.PathLike[str] | None = None,
    session_slot: int | None = None,
) -> dict[str, object]:
    payload: object | None = None
    try:
        summary, payload = _get_youtube_quota_live(session_file=session_file, session_slot=session_slot)
    except Exception as exc:
        if session_slot is None:
            raise
        cached = _read_cached_youtube_quota(int(session_slot))
        if cached is None:
            raise
        LOGGER.warning(
            "YouTube quota live fetch failed for slot %s, using Redis cache (captured_at=%s). Reason: %s",
            session_slot,
            cached.get("captured_at", "unknown"),
            exc,
        )
        summary = dict(cached)
        summary["session_slot"] = session_slot
        summary["project_id"] = str(summary.get("project_id", "") or "").strip()
        summary["captured_at"] = str(summary.get("captured_at", "") or "").strip()
        summary["used_cached_quota"] = True
        # Replace {project} placeholder in the id field with actual project_id
        if summary.get("project_id"):
            id_str = str(summary.get("id", "")).strip()
            summary["id"] = id_str.replace("{project}", summary["project_id"])
            summary["usage_url"] = YOUTUBE_QUOTA_CONSOLE_URL.format(project_id=summary["project_id"])
    if raw:
        return {"summary": summary, "raw": payload}
    return summary


def _get_or_refresh_youtube_quota_for_slot(slot: int) -> dict[str, object]:
    try:
        return get_youtube_quota(session_slot=slot)
    except Exception as exc:
        LOGGER.warning(
            "YouTube quota session for slot %s is missing or expired. Recapturing via debug browser attach. Reason: %s",
            slot,
            exc,
        )
        try:
            capture_youtube_quota_session(session_slot=slot)
            return get_youtube_quota(session_slot=slot)
        except Exception as capture_exc:
            cached = _read_cached_youtube_quota(slot)
            if cached is not None:
                LOGGER.warning(
                    "Using cached Redis YouTube quota for slot %s because live sync and recapture both failed. Reason: %s",
                    slot,
                    capture_exc,
                )
                return cached
            raise RuntimeError(
                f"Unable to sync YouTube quota for slot {slot}: live sync failed ({exc}); recapture failed ({capture_exc})"
            ) from capture_exc


def _estimate_duplicate_check_quota_cost(playlist_item_count: int) -> dict[str, int]:
    safe_count = max(0, int(playlist_item_count))
    playlist_items_list_calls = max(1, math.ceil(safe_count / 50))
    videos_list_calls = math.ceil(safe_count / 50) if safe_count > 0 else 0
    return {
        "playlistItems.list": playlist_items_list_calls * YOUTUBE_QUOTA_COST_PLAYLIST_ITEMS_LIST,
        "videos.list": videos_list_calls * YOUTUBE_QUOTA_COST_VIDEOS_LIST,
    }


def _sum_quota_costs(costs: dict[str, int], *keys: str) -> int:
    return sum(int(costs.get(key, 0) or 0) for key in keys)


def _sync_or_estimate_after_spend(slot: int, *, spent_units: int, reason: str) -> dict[str, object] | None:
    if int(slot or 0) <= 0:
        return None
    try:
        result = get_youtube_quota(session_slot=slot)
        if result.get("used_cached_quota"):
            # Live sync failed; get_youtube_quota returned stale cache without raising.
            # Apply estimated spend so local Redis reflects the cost.
            return _apply_estimated_quota_spend(slot, spent_units=spent_units, reason=reason)
        return result
    except Exception:
        return _apply_estimated_quota_spend(slot, spent_units=spent_units, reason=reason)


def _estimate_youtube_upload_quota_cost(youtube, spec: UploadSpec, cfg, *, force: bool, slot: int) -> dict[str, object]:
    costs: dict[str, int] = {
        "videos.insert": YOUTUBE_QUOTA_COST_VIDEO_INSERT,
        "thumbnails.set": YOUTUBE_QUOTA_COST_THUMBNAIL_SET,
        "playlistItems.insert": YOUTUBE_QUOTA_COST_PLAYLIST_ITEM_INSERT,
    }
    playlist_item_count = 0
    if not force:
        response = _execute_youtube_request(
            youtube.playlists().list(part="contentDetails", id=spec.playlist_id, maxResults=1),
            cfg,
            operation_name=f"playlists.list {spec.playlist_id}",
        )
        items = response.get("items", []) or []
        if items:
            playlist_item_count = int(((items[0].get("contentDetails", {}) or {}).get("itemCount", 0)) or 0)
        costs["playlists.list"] = YOUTUBE_QUOTA_COST_PLAYLISTS_LIST
        costs.update(_estimate_duplicate_check_quota_cost(playlist_item_count))
    total_cost = sum(costs.values())
    return {
        "playlist_item_count": playlist_item_count,
        "costs": costs,
        "planning_cost": int(costs.get("playlists.list", 0) or 0),
        "duplicate_check_cost": _sum_quota_costs(costs, "playlistItems.list", "videos.list"),
        "commit_cost": _sum_quota_costs(costs, "videos.insert", "thumbnails.set", "playlistItems.insert"),
        "total_cost": total_cost,
    }


def _order_youtube_accounts_for_upload(
    config: NovelConfig,
    spec: UploadSpec,
    *,
    force: bool,
) -> tuple[list[YouTubeAccountPaths], dict[str, object]]:
    cfg = config.upload.youtube
    accounts = _youtube_accounts_from_config(config)
    if not accounts:
        raise ValueError("No YouTube accounts configured")

    scored_accounts: list[tuple[YouTubeAccountPaths, int, dict[str, object]]] = []
    for account in accounts:
        quota_summary = _get_or_refresh_youtube_quota_for_slot(account.index)
        remaining = int(quota_summary.get("remaining", 0) or 0)
        scored_accounts.append((account, remaining, quota_summary))

    ordered_by_remaining = sorted(scored_accounts, key=lambda item: (item[1], -item[0].index), reverse=True)
    planning_account = ordered_by_remaining[0][0]
    planning_youtube = _build_youtube_client_for_account(planning_account)
    estimate = _estimate_youtube_upload_quota_cost(planning_youtube, spec, cfg, force=force, slot=planning_account.index)
    required_quota = int(estimate["total_cost"])

    refreshed_scored_accounts: list[tuple[YouTubeAccountPaths, int, dict[str, object]]] = []
    for account in accounts:
        cached_or_live = _get_or_refresh_youtube_quota_for_slot(account.index)
        refreshed_scored_accounts.append(
            (
                account,
                int(cached_or_live.get("remaining", 0) or 0),
                cached_or_live,
            )
        )

    eligible = [item for item in refreshed_scored_accounts if item[1] >= required_quota]
    quota_snapshot = [
        {
            "slot": item[0].index,
            "label": item[0].label,
            "remaining": item[1],
            "current_usage": int(item[2].get("current_usage", 0) or 0),
            "effective_limit": int(item[2].get("effective_limit", 0) or 0),
            "estimated_uploads_remaining": (
                int(item[1] // YOUTUBE_QUOTA_UPLOAD_COMMIT_COST) if YOUTUBE_QUOTA_UPLOAD_COMMIT_COST > 0 else 0
            ),
            "status": "eligible" if item[1] >= required_quota else "insufficient",
        }
        for item in sorted(refreshed_scored_accounts, key=lambda item: (item[1], -item[0].index), reverse=True)
    ]
    LOGGER.info(
        "YouTube quota check for %s: required=%s planning_slot=%s costs=%s",
        spec.range_key,
        required_quota,
        planning_account.index,
        json.dumps(estimate, ensure_ascii=False),
    )
    for item in quota_snapshot:
        LOGGER.info(
            "YouTube quota slot %s (%s): remaining=%s usage=%s/%s uploads_remaining~=%s status=%s",
            item["slot"],
            item["label"],
            item["remaining"],
            item["current_usage"],
            item["effective_limit"],
            item["estimated_uploads_remaining"],
            item["status"],
        )
    if eligible:
        ordered = sorted(eligible, key=lambda item: (item[1], -item[0].index), reverse=True)
        ordered_accounts = [item[0] for item in ordered]
        chosen = ordered[0]
    else:
        ordered = sorted(refreshed_scored_accounts, key=lambda item: (item[1], -item[0].index), reverse=True)
        raise RuntimeError(
            f"No YouTube project has enough remaining quota for this upload. "
            f"required={required_quota} details={json.dumps(quota_snapshot, ensure_ascii=False)}"
        )

    LOGGER.info(
        "Selected YouTube project slot %s (%s) for %s: remaining=%s required=%s",
        chosen[0].index,
        chosen[0].label,
        spec.range_key,
        chosen[1],
        required_quota,
    )

    selection = {
        "required_quota": required_quota,
        "estimate": estimate,
        "selection_mode": "quota-auto",
        "planning_slot": planning_account.index,
        "chosen_slot": chosen[0].index,
        "chosen_label": chosen[0].label,
        "chosen_remaining": chosen[1],
        "ordered_slots": [item[0].index for item in ordered],
        "quota_by_slot": quota_snapshot,
    }
    return ordered_accounts, selection


def _build_youtube_client_from_paths(credentials_path: Path, token_path: Path):
    try:
        from google.auth.transport.requests import Request
        from google.auth.exceptions import RefreshError
        from google.oauth2.credentials import Credentials
        from google_auth_oauthlib.flow import InstalledAppFlow
        from googleapiclient.discovery import build
    except Exception as exc:
        raise RuntimeError(
            "Missing YouTube upload dependencies. Install project dependencies again to include Google API libs."
        ) from exc

    if not credentials_path.exists():
        raise FileNotFoundError(f"YouTube credentials file not found: {credentials_path}")

    creds = None
    if token_path.exists():
        creds = Credentials.from_authorized_user_file(str(token_path), YOUTUBE_UPLOAD_SCOPES)

    if not creds or not creds.valid:
        refreshed = False
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
                refreshed = True
            except RefreshError as exc:
                LOGGER.warning("Stored YouTube token refresh failed; starting a fresh OAuth flow: %s", exc)
                creds = None
        if not refreshed:
            flow = InstalledAppFlow.from_client_secrets_file(str(credentials_path), YOUTUBE_UPLOAD_SCOPES)
            creds = flow.run_local_server(port=0)
        token_path.parent.mkdir(parents=True, exist_ok=True)
        token_path.write_text(creds.to_json(), encoding="utf-8")

    return build("youtube", "v3", credentials=creds)


def _extract_error_reasons(exc: Exception) -> list[str]:
    content = getattr(exc, "content", None)
    if content is None:
        return []
    if isinstance(content, bytes):
        try:
            content = content.decode("utf-8", errors="ignore")
        except Exception:
            return []
    if not isinstance(content, str):
        return []
    try:
        payload = json.loads(content)
    except Exception:
        return []
    error = payload.get("error", {}) if isinstance(payload, dict) else {}
    errors = error.get("errors", []) if isinstance(error, dict) else []
    reasons: list[str] = []
    for item in errors:
        if not isinstance(item, dict):
            continue
        reason = str(item.get("reason", "")).strip()
        if reason:
            reasons.append(reason)
    return reasons


def _extract_error_status(exc: Exception) -> int | None:
    resp = getattr(exc, "resp", None)
    status = getattr(resp, "status", None)
    if status is None:
        status = getattr(exc, "status_code", None)
    try:
        return int(status) if status is not None else None
    except Exception:
        return None


def _youtube_quota_cost_for_operation(operation_name: str) -> int:
    op = str(operation_name or "").strip().split(" ", 1)[0]
    costs = {
        "channels.list": YOUTUBE_QUOTA_COST_CHANNELS_LIST,
        "playlistItems.list": YOUTUBE_QUOTA_COST_PLAYLIST_ITEMS_LIST,
        "playlistItems.insert": YOUTUBE_QUOTA_COST_PLAYLIST_ITEM_INSERT,
        "playlistItems.update": YOUTUBE_QUOTA_COST_PLAYLIST_ITEM_UPDATE,
        "playlistItems.delete": YOUTUBE_QUOTA_COST_PLAYLIST_ITEM_DELETE,
        "playlists.list": YOUTUBE_QUOTA_COST_PLAYLISTS_LIST,
        "playlists.get": YOUTUBE_QUOTA_COST_PLAYLISTS_LIST,
        "playlists.update": YOUTUBE_QUOTA_COST_PLAYLIST_UPDATE,
        "videos.list": YOUTUBE_QUOTA_COST_VIDEOS_LIST,
        "videos.get": YOUTUBE_QUOTA_COST_VIDEOS_LIST,
        "videos.insert": YOUTUBE_QUOTA_COST_VIDEO_INSERT,
        "videos.update": YOUTUBE_QUOTA_COST_VIDEO_UPDATE,
        "videos.delete": YOUTUBE_QUOTA_COST_VIDEO_DELETE,
        "thumbnails.set": YOUTUBE_QUOTA_COST_THUMBNAIL_SET,
    }
    return int(costs.get(op, 1))


def _is_youtube_quota_rotation_error(exc: Exception) -> bool:
    status = _extract_error_status(exc)
    if status != 403:
        return False
    return any(reason in YOUTUBE_ROTATE_ACCOUNT_REASONS for reason in _extract_error_reasons(exc))


def _is_youtube_rate_limit_error(exc: Exception) -> bool:
    status = _extract_error_status(exc)
    if status == 429:
        return True
    if status != 403:
        return False
    return any(reason in YOUTUBE_RATE_LIMIT_REASONS for reason in _extract_error_reasons(exc))


def _execute_youtube_request(request, cfg, *, operation_name: str, slot: int | None = None):
    max_attempts = max(1, int(getattr(cfg, "upload_retry_max_attempts", 5) or 5))
    base_sleep = max(0.0, float(getattr(cfg, "upload_retry_base_sleep_seconds", 15.0) or 15.0))
    max_sleep = max(base_sleep, float(getattr(cfg, "upload_retry_max_sleep_seconds", 300.0) or 300.0))
    cost_units = _youtube_quota_cost_for_operation(operation_name)

    attempt = 1
    while True:
        try:
            if slot is not None and int(slot) > 0:
                _get_or_refresh_youtube_quota_for_slot(int(slot))
            response = request.execute()
        except Exception as exc:
            if slot is not None and int(slot) > 0:
                _sync_or_estimate_after_spend(int(slot), spent_units=cost_units, reason=f"{operation_name}:attempt:{attempt}")
            if (not _is_youtube_rate_limit_error(exc)) or attempt >= max_attempts:
                raise
            delay = min(max_sleep, base_sleep * (2 ** (attempt - 1)))
            reasons = _extract_error_reasons(exc)
            reason_text = ", ".join(reasons) if reasons else f"http {(_extract_error_status(exc) or 'unknown')}"
            LOGGER.warning(
                "YouTube %s hit a rate limit (%s). Retrying in %.1fs (%s/%s).",
                operation_name,
                reason_text,
                delay,
                attempt + 1,
                max_attempts,
            )
            time.sleep(delay)
            attempt += 1
        else:
            if slot is not None and int(slot) > 0:
                _sync_or_estimate_after_spend(int(slot), spent_units=cost_units, reason=operation_name)
            return response


def _build_youtube_client_for_account(account: YouTubeAccountPaths):
    return _build_youtube_client_from_paths(account.credentials_path, account.token_path)


def _run_with_youtube_accounts(
    accounts: list[YouTubeAccountPaths],
    cfg,
    *,
    operation_name: str,
    action: Callable[[object, YouTubeAccountPaths], T],
) -> T:
    if not accounts:
        raise ValueError("No YouTube accounts configured")

    last_error: Exception | None = None
    for offset, account in enumerate(accounts):
        youtube = _build_youtube_client_for_account(account)
        try:
            return action(youtube, account)
        except Exception as exc:
            last_error = exc
            if (not _is_youtube_quota_rotation_error(exc)) or offset >= len(accounts) - 1:
                raise
            reasons = ", ".join(_extract_error_reasons(exc)) or f"http {(_extract_error_status(exc) or 'unknown')}"
            LOGGER.warning(
                "YouTube %s exhausted quota on account %s. Rotating to the next account (%s/%s). Reason: %s",
                operation_name,
                account.label,
                offset + 2,
                len(accounts),
                reasons,
            )
    assert last_error is not None
    raise last_error


def _build_upload_spec(config: NovelConfig, start: int, end: int, *, require_media_files: bool = True) -> UploadSpec:
    range_key = _range_key(start, end)
    video_path = config.storage.video_dir / f"{range_key}.mp4"
    if require_media_files and (not video_path.exists()):
        raise FileNotFoundError(f"Missing video file for upload: {video_path}")
    thumbnail_path = config.storage.visual_dir / f"{range_key}.png"
    if require_media_files and (not thumbnail_path.exists()):
        raise FileNotFoundError(f"Missing thumbnail file for upload: {thumbnail_path}")
    menu_path = config.storage.subtitle_dir / f"{range_key}_menu.txt"
    menu = _read_required_text(menu_path, field_name="menu")
    title_path = _resolve_output_file(config, config.upload.youtube.title_file, field_name="upload.youtube.title_file")
    description_path = _resolve_output_file(
        config,
        config.upload.youtube.description_file,
        field_name="upload.youtube.description_file",
    )
    playlist_path = _resolve_output_file(
        config,
        config.upload.youtube.playlist_file,
        field_name="upload.youtube.playlist_file",
    )
    title_raw = _read_required_text(title_path, field_name="title")
    title = _resolve_title_with_index(config, title_raw, start, end)
    description_base = _read_required_text(description_path, field_name="description")
    playlist_raw = _read_required_text(playlist_path, field_name="playlist")
    playlist_line = next((line.strip() for line in playlist_raw.splitlines() if line.strip()), "")
    playlist_id = _parse_playlist_id(playlist_line or playlist_raw)
    description = f"{description_base}\n\n{menu}"
    return UploadSpec(
        platform="youtube",
        range_key=range_key,
        video_path=video_path,
        thumbnail_path=thumbnail_path,
        title=title,
        description=description,
        playlist_id=playlist_id,
    )


def _read_playlist_id(config: NovelConfig) -> str:
    playlist_path = _resolve_output_file(
        config,
        config.upload.youtube.playlist_file,
        field_name="upload.youtube.playlist_file",
    )
    playlist_raw = _read_required_text(playlist_path, field_name="playlist")
    playlist_line = next((line.strip() for line in playlist_raw.splitlines() if line.strip()), "")
    return _parse_playlist_id(playlist_line or playlist_raw)


def _find_range_key_for_episode(config: NovelConfig, episode_index: int) -> str | None:
    """Return range_key whose media episode index matches the given index, or None."""
    item = find_media_range_by_episode(config, episode_index)
    if item is None:
        return None
    return item.range_key


def _video_in_chapter_range(video_title: str, config: NovelConfig, from_chapter: int, to_chapter: int) -> bool:
    """Return True if the video's episode index corresponds to a translated range that overlaps [from_chapter, to_chapter]."""
    m = re.search(r"\bT(?:ập|ap)\s+(\d+)\b", video_title, re.IGNORECASE)
    if not m:
        return False
    episode_index = int(m.group(1))
    range_key = _find_range_key_for_episode(config, episode_index)
    if range_key is None:
        return False
    rk_match = re.match(r"chuong_(\d+)-(\d+)", range_key)
    if not rk_match:
        return False
    start = int(rk_match.group(1))
    end = int(rk_match.group(2))
    return start <= to_chapter and end >= from_chapter


def _extract_menu_from_description(description: str) -> str:
    """Extract the timestamp chapter-menu block from the end of a description."""
    timestamp_re = re.compile(r"^\d{2}:\d{2}:\d{2}\b")
    lines = str(description or "").splitlines()
    i = len(lines) - 1
    while i >= 0 and not lines[i].strip():
        i -= 1
    if i < 0 or not timestamp_re.match(lines[i].strip()):
        return ""
    while i >= 0 and (timestamp_re.match(lines[i].strip()) or not lines[i].strip()):
        i -= 1
    menu_start = i + 1
    while menu_start < len(lines) and not lines[menu_start].strip():
        menu_start += 1
    return "\n".join(lines[menu_start:]).strip()


def _build_expected_description(
    config: NovelConfig,
    video_id: str,
    playlist_id: str,
    video_title: str,
    current_description: str,
) -> str:
    """Build the expected full description for an uploaded video.

    Composes three parts:
    1. Header (2 lines) — video watch URL + playlist link, with up-to-date video_id.
    2. Description body — from description.txt (everything after the 2 header lines).
       Falls back to the current body when the file is missing.
    3. Menu — from subtitle/<range_key>_menu.txt for the video's episode.
       Preserved from current_description when the file does not exist.
    """
    # Part 1: header lines (2 lines)
    line1 = f"Xem trong danh sách phát: https://www.youtube.com/watch?v={video_id}&list={playlist_id}"
    line2 = f"Link danh sách phát: https://www.youtube.com/playlist?list={playlist_id}"

    # Part 2: description body from description.txt (skip first 2 header lines)
    try:
        desc_path = _resolve_output_file(
            config, config.upload.youtube.description_file, field_name="upload.youtube.description_file"
        )
        raw = _read_required_text(desc_path, field_name="description")
        lines = raw.splitlines()
        if len(lines) > 2:
            body_suffix = "\n" + "\n".join(lines[2:])
        else:
            body_suffix = ""
    except Exception:
        current_lines = str(current_description or "").splitlines()
        if len(current_lines) > 2:
            body_suffix = "\n" + "\n".join(current_lines[2:])
        else:
            body_suffix = ""

    # Part 3: menu from subtitle file; fall back to current menu if file absent
    menu: str | None = None
    episode_index = _extract_episode_number(video_title)
    if episode_index is not None:
        range_key = _find_range_key_for_episode(config, episode_index)
        if range_key is not None:
            menu_path = config.storage.subtitle_dir / f"{range_key}_menu.txt"
            if menu_path.exists():
                menu = menu_path.read_text(encoding="utf-8").strip()
    if menu is None:
        menu = _extract_menu_from_description(current_description)

    base = f"{line1}\n{line2}{body_suffix}"
    return f"{base}\n\n{menu}" if menu else base


def _novel_title_signals(config: NovelConfig) -> list[str]:
    signals: list[str] = []
    candidates = [
        config.novel_id.replace("-", " "),
    ]
    try:
        title_template = _read_title_template(config)
        candidates.append(title_template)
        candidates.append(re.sub(r"\bT(?:ập|ap)\s*(?:\{index\}|\d+)\b", " ", title_template, flags=re.IGNORECASE))
    except Exception:
        pass

    seen: set[str] = set()
    for candidate in candidates:
        norm = _normalize_search_text(candidate)
        norm = re.sub(r"\b(full|tap|t a p|index)\b", " ", norm)
        norm = re.sub(r"\s+", " ", norm).strip()
        if len(norm) >= 6 and norm not in seen:
            seen.add(norm)
            signals.append(norm)
    return signals


def _video_matches_novel(config: NovelConfig, video_title: str) -> bool:
    normalized_title = _normalize_search_text(video_title)
    if not normalized_title:
        return False
    for signal in _novel_title_signals(config):
        if signal and signal in normalized_title:
            return True
    return False


def _update_youtube_video_description_only(
    youtube,
    current_video: dict[str, object],
    new_description: str,
    *,
    cfg,
    slot: int | None = None,
) -> dict[str, object]:
    normalized_id = str(current_video.get("id", "")).strip()
    if not normalized_id:
        raise ValueError("Video id is empty")
    body = {
        "id": normalized_id,
        "snippet": {
            "title": str(current_video.get("title", "") or ""),
            "description": str(new_description or ""),
            "categoryId": str(current_video.get("category_id", "") or ""),
        },
        "status": {
            "privacyStatus": str(current_video.get("privacy_status", "") or ""),
            "selfDeclaredMadeForKids": bool(current_video.get("made_for_kids")),
        },
    }
    response = _execute_youtube_request(
        youtube.videos().update(part="snippet,status", body=body),
        cfg,
        operation_name=f"videos.update {normalized_id}",
        slot=slot,
    )
    item = response if isinstance(response, dict) and response.get("id") else None
    if item is None:
        item = {
            "id": normalized_id,
            "snippet": {
                "title": str(current_video.get("title", "") or ""),
                "description": str(new_description or ""),
                "publishedAt": current_video.get("published_at", ""),
                "channelId": current_video.get("channel_id", ""),
                "channelTitle": current_video.get("channel_title", ""),
                "thumbnails": current_video.get("thumbnails", {}),
                "categoryId": current_video.get("category_id", ""),
                "tags": current_video.get("tags", []),
                "liveBroadcastContent": current_video.get("live_broadcast_content", ""),
            },
            "status": {
                "privacyStatus": current_video.get("privacy_status", ""),
                "uploadStatus": current_video.get("upload_status", ""),
                "license": current_video.get("license", ""),
                "embeddable": current_video.get("embeddable"),
                "publicStatsViewable": current_video.get("public_stats_viewable"),
                "selfDeclaredMadeForKids": bool(current_video.get("made_for_kids")),
            },
            "contentDetails": {
                "duration": current_video.get("duration", ""),
                "dimension": current_video.get("dimension", ""),
                "definition": current_video.get("definition", ""),
                "caption": current_video.get("caption", ""),
                "licensedContent": current_video.get("licensed_content"),
                "projection": current_video.get("projection", ""),
            },
            "statistics": {
                "viewCount": current_video.get("view_count", ""),
                "likeCount": current_video.get("like_count", ""),
                "favoriteCount": current_video.get("favorite_count", ""),
                "commentCount": current_video.get("comment_count", ""),
            },
        }
    return _video_to_metadata(
        item,
        playlist_item={
            "id": current_video.get("playlist_item_id", ""),
            "snippet": {
                "playlistId": current_video.get("uploads_playlist_id", ""),
                "position": current_video.get("playlist_position"),
                "publishedAt": current_video.get("playlist_published_at", ""),
            },
        }
        if current_video.get("playlist_item_id") or current_video.get("playlist_position") is not None
        else None,
    )


def _load_youtube_client(config: NovelConfig):
    account = _select_youtube_accounts(_youtube_accounts_from_config(config), project_selector=config.upload.youtube.project)[0]
    return _build_youtube_client_for_account(account)


def _load_youtube_client_from_defaults():
    return _build_youtube_client_for_account(_selected_youtube_accounts_from_defaults()[0])


def _playlist_to_metadata(item: dict) -> dict[str, object]:
    snippet = item.get("snippet", {}) or {}
    status = item.get("status", {}) or {}
    content_details = item.get("contentDetails", {}) or {}
    thumbnails = snippet.get("thumbnails", {}) or {}
    thumbnail_urls: dict[str, str] = {}
    for key, value in thumbnails.items():
        if isinstance(value, dict) and value.get("url"):
            thumbnail_urls[key] = str(value.get("url"))
        elif isinstance(value, str) and value:
            thumbnail_urls[key] = value
    playlist_id = str(item.get("id", "")).strip()
    return {
        "id": playlist_id,
        "title": snippet.get("title", ""),
        "description": snippet.get("description", ""),
        "published_at": snippet.get("publishedAt", ""),
        "channel_id": snippet.get("channelId", ""),
        "channel_title": snippet.get("channelTitle", ""),
        "privacy_status": status.get("privacyStatus", ""),
        "item_count": content_details.get("itemCount", 0),
        "thumbnails": thumbnail_urls,
        "url": f"https://www.youtube.com/playlist?list={playlist_id}" if playlist_id else "",
    }


def _video_to_metadata(item: dict, *, playlist_item: dict | None = None) -> dict[str, object]:
    snippet = item.get("snippet", {}) or {}
    status = item.get("status", {}) or {}
    content_details = item.get("contentDetails", {}) or {}
    statistics = item.get("statistics", {}) or {}
    thumbnails = snippet.get("thumbnails", {}) or {}
    thumbnail_urls: dict[str, str] = {}
    for key, value in thumbnails.items():
        if isinstance(value, dict) and value.get("url"):
            thumbnail_urls[key] = str(value.get("url"))
        elif isinstance(value, str) and value:
            thumbnail_urls[key] = value

    video_id = str(item.get("id", "")).strip()
    payload: dict[str, object] = {
        "id": video_id,
        "title": snippet.get("title", ""),
        "description": snippet.get("description", ""),
        "published_at": snippet.get("publishedAt", ""),
        "channel_id": snippet.get("channelId", ""),
        "channel_title": snippet.get("channelTitle", ""),
        "privacy_status": status.get("privacyStatus", ""),
        "upload_status": status.get("uploadStatus", ""),
        "license": status.get("license", ""),
        "embeddable": status.get("embeddable"),
        "public_stats_viewable": status.get("publicStatsViewable"),
        "made_for_kids": status.get("selfDeclaredMadeForKids", status.get("madeForKids")),
        "category_id": snippet.get("categoryId", ""),
        "tags": snippet.get("tags", []) or [],
        "live_broadcast_content": snippet.get("liveBroadcastContent", ""),
        "duration": content_details.get("duration", ""),
        "dimension": content_details.get("dimension", ""),
        "definition": content_details.get("definition", ""),
        "caption": content_details.get("caption", ""),
        "licensed_content": content_details.get("licensedContent"),
        "projection": content_details.get("projection", ""),
        "view_count": statistics.get("viewCount", ""),
        "like_count": statistics.get("likeCount", ""),
        "favorite_count": statistics.get("favoriteCount", ""),
        "comment_count": statistics.get("commentCount", ""),
        "thumbnails": thumbnail_urls,
        "url": f"https://www.youtube.com/watch?v={video_id}" if video_id else "",
    }
    if playlist_item is not None:
        payload["playlist_item_id"] = str(playlist_item.get("id", "")).strip()
        payload["uploads_playlist_id"] = str((playlist_item.get("snippet", {}) or {}).get("playlistId", "")).strip()
        payload["playlist_position"] = (playlist_item.get("snippet", {}) or {}).get("position")
        payload["playlist_published_at"] = (playlist_item.get("snippet", {}) or {}).get("publishedAt", "")
    return payload


def _get_uploads_playlist_id(youtube, cfg, *, slot: int | None = None) -> str:
    response = _execute_youtube_request(
        youtube.channels().list(part="contentDetails", mine=True, maxResults=1),
        cfg,
        operation_name="channels.list mine",
        slot=slot,
    )
    items = response.get("items", []) or []
    if not items:
        raise ValueError("Unable to resolve YouTube uploads playlist for the authenticated channel")
    related = ((items[0].get("contentDetails", {}) or {}).get("relatedPlaylists", {}) or {})
    uploads_id = str(related.get("uploads", "")).strip()
    if not uploads_id:
        raise ValueError("Authenticated YouTube channel does not expose an uploads playlist")
    return uploads_id


def _find_uploads_playlist_item_for_video(youtube, video_id: str, cfg, *, slot: int | None = None) -> tuple[str, dict | None]:
    uploads_playlist_id = _get_uploads_playlist_id(youtube, cfg, slot=slot)
    page_token = None
    normalized_id = str(video_id or "").strip()
    while True:
        response = _execute_youtube_request(
            youtube.playlistItems().list(
                part="snippet,contentDetails,status",
                playlistId=uploads_playlist_id,
                maxResults=50,
                pageToken=page_token,
            ),
            cfg,
            operation_name=f"playlistItems.list {uploads_playlist_id}",
            slot=slot,
        )
        for item in response.get("items", []) or []:
            item_video_id = str((item.get("contentDetails", {}) or {}).get("videoId", "")).strip()
            if item_video_id == normalized_id:
                return uploads_playlist_id, item
        page_token = str(response.get("nextPageToken", "")).strip() or None
        if page_token is None:
            break
    return uploads_playlist_id, None


def _list_playlist_videos(youtube, playlist_id: str, cfg, *, slot: int | None = None) -> list[dict[str, object]]:
    normalized_playlist_id = _parse_playlist_id(playlist_id)
    playlist_items: list[dict] = []
    page_token = None

    while True:
        response = _execute_youtube_request(
            youtube.playlistItems().list(
                part="snippet,contentDetails,status",
                playlistId=normalized_playlist_id,
                maxResults=50,
                pageToken=page_token,
            ),
            cfg,
            operation_name=f"playlistItems.list {normalized_playlist_id}",
            slot=slot,
        )
        playlist_items.extend(response.get("items", []) or [])
        page_token = str(response.get("nextPageToken", "")).strip() or None
        if page_token is None:
            break

    if not playlist_items:
        return []

    playlist_by_video_id: dict[str, dict] = {}
    ordered_ids: list[str] = []
    for item in playlist_items:
        video_id = str((item.get("contentDetails", {}) or {}).get("videoId", "")).strip()
        if not video_id:
            continue
        if video_id not in playlist_by_video_id:
            ordered_ids.append(video_id)
            playlist_by_video_id[video_id] = item

    videos_by_id: dict[str, dict] = {}
    for idx in range(0, len(ordered_ids), 50):
        batch_ids = ordered_ids[idx : idx + 50]
        response = _execute_youtube_request(
            youtube.videos().list(
                part="snippet,contentDetails,status,statistics",
                id=",".join(batch_ids),
                maxResults=50,
            ),
            cfg,
            operation_name=f"videos.list {normalized_playlist_id}",
            slot=slot,
        )
        for item in response.get("items", []) or []:
            video_id = str(item.get("id", "")).strip()
            if video_id:
                videos_by_id[video_id] = item

    return [
        _video_to_metadata(videos_by_id[video_id], playlist_item=playlist_by_video_id.get(video_id))
        for video_id in ordered_ids
        if video_id in videos_by_id
    ]


def _delete_youtube_video(youtube, video_id: str, cfg, *, slot: int | None = None) -> None:
    normalized_id = str(video_id or "").strip()
    if not normalized_id:
        raise ValueError("Video id is empty")
    _execute_youtube_request(
        youtube.videos().delete(id=normalized_id),
        cfg,
        operation_name=f"videos.delete {normalized_id}",
        slot=slot,
    )


def _delete_playlist_item(youtube, playlist_item_id: str, cfg, *, slot: int | None = None) -> None:
    normalized_id = str(playlist_item_id or "").strip()
    if not normalized_id:
        raise ValueError("Playlist item id is empty")
    _execute_youtube_request(
        youtube.playlistItems().delete(id=normalized_id),
        cfg,
        operation_name=f"playlistItems.delete {normalized_id}",
        slot=slot,
    )


def _find_playlist_video_by_title(
    youtube,
    playlist_id: str,
    title: str,
    cfg,
    *,
    slot: int | None = None,
) -> dict[str, object] | None:
    title_key = _normalize_title(title)
    if not title_key:
        return None
    for video in _list_playlist_videos(youtube, playlist_id, cfg, slot=slot):
        if _normalize_title(str(video.get("title", ""))) == title_key:
            return video
    return None


def _update_playlist_item_position(
    youtube,
    playlist_id: str,
    playlist_item_id: str,
    video_id: str,
    position: int,
    cfg,
    *,
    slot: int | None = None,
):
    return _execute_youtube_request(
        youtube.playlistItems().update(
            part="snippet",
            body={
                "id": playlist_item_id,
                "snippet": {
                    "playlistId": playlist_id,
                    "resourceId": {
                        "kind": "youtube#video",
                        "videoId": video_id,
                    },
                    "position": int(position),
                },
            },
        ),
        cfg,
        operation_name=f"playlistItems.update {video_id}",
        slot=slot,
    )


def list_youtube_videos() -> list[dict[str, object]]:
    youtube_cfg = _youtube_upload_cfg_from_defaults()

    def _action(youtube, account: YouTubeAccountPaths) -> list[dict[str, object]]:
        uploads_playlist_id = _get_uploads_playlist_id(youtube, youtube_cfg, slot=account.index)
        return _list_playlist_videos(youtube, uploads_playlist_id, youtube_cfg, slot=account.index)

    return _run_with_youtube_accounts(
        _selected_youtube_accounts_from_defaults(),
        None,
        operation_name="videos.list",
        action=_action,
    )


def get_youtube_video(video_id: str) -> dict[str, object]:
    normalized_id = str(video_id or "").strip()
    if not normalized_id:
        raise ValueError("Video id is empty")

    youtube_cfg = _youtube_upload_cfg_from_defaults()

    def _action(youtube, account: YouTubeAccountPaths) -> dict[str, object]:
        response = _execute_youtube_request(
            youtube.videos().list(
                part="snippet,contentDetails,status,statistics",
                id=normalized_id,
                maxResults=1,
            ),
            youtube_cfg,
            operation_name=f"videos.get {normalized_id}",
            slot=account.index,
        )
        items = response.get("items", []) or []
        if not items:
            raise ValueError(f"YouTube video not found: {normalized_id}")
        playlist_item = None
        try:
            _uploads_playlist_id, playlist_item = _find_uploads_playlist_item_for_video(
                youtube, normalized_id, youtube_cfg, slot=account.index
            )
        except Exception:
            playlist_item = None
        return _video_to_metadata(items[0], playlist_item=playlist_item)

    return _run_with_youtube_accounts(
        _selected_youtube_accounts_from_defaults(),
        None,
        operation_name=f"videos.get {normalized_id}",
        action=_action,
    )


def update_youtube_video(
    video_id: str,
    *,
    title: str | None = None,
    description: str | None = None,
    privacy_status: str | None = None,
    made_for_kids: bool | None = None,
    playlist_position: int | None = None,
) -> dict[str, object]:
    account = _selected_youtube_accounts_from_defaults()[0]
    youtube = _build_youtube_client_for_account(account)
    youtube_cfg = _youtube_upload_cfg_from_defaults()
    current = get_youtube_video(video_id)
    normalized_id = str(current.get("id", "")).strip() or str(video_id or "").strip()
    if not normalized_id:
        raise ValueError("Video id is empty")

    effective_title = current.get("title", "") if title is None else title
    effective_description = current.get("description", "") if description is None else description
    effective_privacy_status = current.get("privacy_status", "") if privacy_status is None else privacy_status
    effective_made_for_kids = current.get("made_for_kids") if made_for_kids is None else made_for_kids

    body = {
        "id": normalized_id,
        "snippet": {
            "title": effective_title,
            "description": effective_description,
            "categoryId": current.get("category_id", ""),
        },
        "status": {
            "privacyStatus": effective_privacy_status,
            "selfDeclaredMadeForKids": bool(effective_made_for_kids),
        },
    }
    video_response = _execute_youtube_request(
        youtube.videos().update(part="snippet,status", body=body),
        youtube_cfg,
        operation_name=f"videos.update {normalized_id}",
        slot=account.index,
    )

    updated_playlist_item = None
    if playlist_position is not None:
        uploads_playlist_id, playlist_item = _find_uploads_playlist_item_for_video(
            youtube, normalized_id, youtube_cfg, slot=account.index
        )
        updated_playlist_item = playlist_item
        if playlist_item is not None:
            playlist_body = {
                "id": str(playlist_item.get("id", "")).strip(),
                "snippet": {
                    "playlistId": uploads_playlist_id,
                    "resourceId": {
                        "kind": "youtube#video",
                        "videoId": normalized_id,
                    },
                    "position": int(playlist_position),
                },
            }
            updated_playlist_item = _execute_youtube_request(
                youtube.playlistItems().update(part="snippet", body=playlist_body),
                youtube_cfg,
                operation_name=f"playlistItems.update {normalized_id}",
                slot=account.index,
            )

    item = video_response if isinstance(video_response, dict) and video_response.get("id") else None
    if item is None:
        item = {
            "id": normalized_id,
            "snippet": {
                "title": effective_title,
                "description": effective_description,
                "publishedAt": current.get("published_at", ""),
                "channelId": current.get("channel_id", ""),
                "channelTitle": current.get("channel_title", ""),
                "thumbnails": current.get("thumbnails", {}),
                "categoryId": current.get("category_id", ""),
                "tags": current.get("tags", []),
                "liveBroadcastContent": current.get("live_broadcast_content", ""),
            },
            "status": {
                "privacyStatus": effective_privacy_status,
                "uploadStatus": current.get("upload_status", ""),
                "license": current.get("license", ""),
                "embeddable": current.get("embeddable"),
                "publicStatsViewable": current.get("public_stats_viewable"),
                "selfDeclaredMadeForKids": bool(effective_made_for_kids),
            },
            "contentDetails": {
                "duration": current.get("duration", ""),
                "dimension": current.get("dimension", ""),
                "definition": current.get("definition", ""),
                "caption": current.get("caption", ""),
                "licensedContent": current.get("licensed_content"),
                "projection": current.get("projection", ""),
            },
            "statistics": {
                "viewCount": current.get("view_count", ""),
                "likeCount": current.get("like_count", ""),
                "favoriteCount": current.get("favorite_count", ""),
                "commentCount": current.get("comment_count", ""),
            },
        }
    return _video_to_metadata(
        item,
        playlist_item=updated_playlist_item if updated_playlist_item is not None else {
            "id": current.get("playlist_item_id", ""),
            "snippet": {
                "playlistId": current.get("uploads_playlist_id", ""),
                "position": current.get("playlist_position"),
                "publishedAt": current.get("playlist_published_at", ""),
            },
        } if current.get("playlist_item_id") or current.get("playlist_position") is not None else None,
    )


def update_uploaded_youtube_playlist_index_descriptions(
    config: NovelConfig,
    *,
    from_chapter: int | None = None,
    to_chapter: int | None = None,
    log_summary: bool = True,
) -> list[dict[str, object]]:
    youtube_cfg = _youtube_upload_cfg_from_defaults()

    def _action(youtube, account: YouTubeAccountPaths) -> list[dict[str, object]]:
        playlist_id = _read_playlist_id(config)
        remote_videos = _list_playlist_videos(youtube, playlist_id, youtube_cfg, slot=account.index)
        results: list[dict[str, object]] = []
        updated_in_batch = 0
        matched_uploaded_count = 0
        unchanged_count = 0
        updated_count = 0
        matched_videos = [video for video in remote_videos if _video_matches_novel(config, str(video.get("title", "")))]
        if from_chapter is not None or to_chapter is not None:
            fc = from_chapter if from_chapter is not None else 1
            tc = to_chapter if to_chapter is not None else 2**31
            matched_videos = [v for v in matched_videos if _video_in_chapter_range(str(v.get("title", "")), config, fc, tc)]
        for idx, remote_video in enumerate(matched_videos):
            matched_uploaded_count += 1
            remote_video_id = str(remote_video.get("id", "")).strip()
            current_title = str(remote_video.get("title", "") or "")
            current_description = str(remote_video.get("description", "") or "")
            updated_description = _build_expected_description(
                config,
                remote_video_id,
                playlist_id,
                current_title,
                current_description,
            )
            if updated_description == current_description:
                results.append({"title": current_title, "video_id": remote_video_id, "status": "unchanged"})
                unchanged_count += 1
            else:
                _update_youtube_video_description_only(
                    youtube, remote_video, updated_description, cfg=youtube_cfg, slot=account.index
                )
                results.append({"title": current_title, "video_id": remote_video_id, "status": "updated"})
                updated_count += 1
                updated_in_batch += 1
                if updated_in_batch >= YOUTUBE_BULK_UPDATE_BATCH_SIZE and idx < len(matched_videos) - 1:
                    LOGGER.info(
                        "YouTube bulk description update cooldown: sleeping %.1fs after %s updates",
                        YOUTUBE_BULK_UPDATE_SLEEP_SECONDS,
                        updated_in_batch,
                    )
                    time.sleep(YOUTUBE_BULK_UPDATE_SLEEP_SECONDS)
                    updated_in_batch = 0

        if log_summary:
            LOGGER.info("Uploaded Video count: %s", matched_uploaded_count)
            LOGGER.info("Correct description - video count: %s", unchanged_count)
            LOGGER.info("Update description - video count: %s", updated_count)
        return results

    return _run_with_youtube_accounts(
        _selected_youtube_accounts_from_defaults(),
        None,
        operation_name="videos.bulk-update-description",
        action=_action,
    )


def update_uploaded_youtube_playlist_positions(
    config: NovelConfig,
    *,
    log_summary: bool = True,
) -> list[dict[str, object]]:
    cfg = config.upload.youtube
    accounts = _select_youtube_accounts(_youtube_accounts_from_config(config), project_selector=cfg.project)

    def _action(youtube, account: YouTubeAccountPaths) -> list[dict[str, object]]:
        playlist_id = _read_playlist_id(config)
        LOGGER.warning(
            "YouTube API does not expose playlist sort mode. If playlist %s is using an auto-sort mode "
            "(for example Older first), switch it to Manual in the YouTube UI before running reorder.",
            playlist_id,
        )
        current_videos = _list_playlist_videos(youtube, playlist_id, cfg, slot=account.index)
        if not current_videos:
            if log_summary:
                LOGGER.info("Playlist %s has no uploaded videos to reorder", playlist_id)
            return []

        initial_positions: dict[str, int] = {}
        playlist_item_by_video_id: dict[str, str] = {}
        title_by_video_id: dict[str, str] = {}
        episode_by_video_id: dict[str, int | None] = {}
        sortable_videos: list[dict[str, object]] = []
        for index, video in enumerate(current_videos):
            video_id = str(video.get("id", "")).strip()
            if not video_id:
                continue
            playlist_item_id = str(video.get("playlist_item_id", "")).strip()
            title = str(video.get("title", "") or "")
            current_position = video.get("playlist_position")
            try:
                normalized_position = int(current_position) if current_position is not None else index
            except Exception:
                normalized_position = index
            episode = _extract_episode_number(title)
            initial_positions[video_id] = normalized_position
            if playlist_item_id:
                playlist_item_by_video_id[video_id] = playlist_item_id
            title_by_video_id[video_id] = title
            episode_by_video_id[video_id] = episode
            sortable_videos.append(
                {
                    "video_id": video_id,
                    "episode": episode,
                    "current_position": normalized_position,
                    "original_index": index,
                }
            )

        sortable_videos.sort(
            key=lambda item: (
                item["episode"] is None,
                item["episode"] if item["episode"] is not None else 10**12,
                item["current_position"],
                item["original_index"],
            )
        )

        matched_count = sum(1 for item in sortable_videos if item["episode"] is not None)
        desired_order = [str(item["video_id"]) for item in sortable_videos]

        move_count = 0
        for desired_position, target_video_id in enumerate(desired_order):
            if desired_position >= len(current_videos):
                break
            current_video = current_videos[desired_position]
            current_video_id = str(current_video.get("id", "")).strip()
            if current_video_id == target_video_id:
                continue

            playlist_item_id = playlist_item_by_video_id.get(target_video_id, "")
            if not playlist_item_id:
                LOGGER.warning(
                    "Skipping playlist reorder for video %s because playlist_item_id is missing",
                    target_video_id,
                )
                continue

            _update_playlist_item_position(
                youtube,
                playlist_id,
                playlist_item_id,
                target_video_id,
                desired_position,
                cfg,
                slot=account.index,
            )
            move_count += 1

            if YOUTUBE_PLAYLIST_REORDER_SLEEP_SECONDS > 0:
                LOGGER.info(
                    "YouTube playlist reorder cooldown: sleeping %.1fs after move %s",
                    YOUTUBE_PLAYLIST_REORDER_SLEEP_SECONDS,
                    move_count,
                )
                time.sleep(YOUTUBE_PLAYLIST_REORDER_SLEEP_SECONDS)

            # Reload after each move because YouTube reindexes adjacent items automatically.
            current_videos = _list_playlist_videos(youtube, playlist_id, cfg, slot=account.index)
            playlist_item_by_video_id = {
                str(video.get("id", "")).strip(): str(video.get("playlist_item_id", "")).strip()
                for video in current_videos
                if str(video.get("id", "")).strip()
            }

        final_videos = _list_playlist_videos(youtube, playlist_id, cfg, slot=account.index)
        final_positions: dict[str, int] = {}
        final_playlist_item_by_video_id: dict[str, str] = {}
        for index, video in enumerate(final_videos):
            video_id = str(video.get("id", "")).strip()
            if not video_id:
                continue
            position = video.get("playlist_position")
            try:
                final_positions[video_id] = int(position) if position is not None else index
            except Exception:
                final_positions[video_id] = index
            final_playlist_item_by_video_id[video_id] = str(video.get("playlist_item_id", "")).strip()

        results: list[dict[str, object]] = []
        updated_count = 0
        unchanged_count = 0
        skipped_count = 0
        for desired_position, target_video_id in enumerate(desired_order):
            title = title_by_video_id.get(target_video_id, "")
            episode = episode_by_video_id.get(target_video_id)
            old_position = initial_positions.get(target_video_id)
            new_position = final_positions.get(target_video_id)
            playlist_item_id = final_playlist_item_by_video_id.get(target_video_id, "")

            if not playlist_item_id:
                results.append(
                    {
                        "title": title,
                        "video_id": target_video_id,
                        "episode": episode,
                        "old_position": old_position,
                        "new_position": new_position,
                        "desired_position": desired_position,
                        "status": "skipped",
                        "reason": "missing_playlist_item_id",
                    }
                )
                skipped_count += 1
                continue

            status = "updated" if old_position != new_position else "unchanged"
            if status == "updated":
                updated_count += 1
            else:
                unchanged_count += 1
            results.append(
                {
                    "title": title,
                    "video_id": target_video_id,
                    "episode": episode,
                    "old_position": old_position,
                    "new_position": new_position,
                    "desired_position": desired_position,
                    "status": status,
                }
            )

        if log_summary:
            LOGGER.info("Playlist %s uploaded video count: %s", playlist_id, len(final_videos))
            LOGGER.info("Playlist %s videos with detected tap number: %s", playlist_id, matched_count)
            LOGGER.info("Playlist %s correct position count: %s", playlist_id, unchanged_count)
            LOGGER.info("Playlist %s updated position count: %s", playlist_id, updated_count)
            LOGGER.info("Playlist %s skipped position count: %s", playlist_id, skipped_count)
        return results

    return _run_with_youtube_accounts(
        accounts,
        cfg,
        operation_name="playlistItems.bulk-update-position",
        action=_action,
    )


def remove_duplicated_uploaded_youtube_videos(
    config: NovelConfig,
    *,
    execute: bool = True,
    log_summary: bool = True,
) -> list[dict[str, object]]:
    cfg = config.upload.youtube
    accounts = _select_youtube_accounts(_youtube_accounts_from_config(config), project_selector=cfg.project)

    def _action(youtube, account: YouTubeAccountPaths) -> list[dict[str, object]]:
        playlist_id = _read_playlist_id(config)
        current_videos = _list_playlist_videos(youtube, playlist_id, cfg, slot=account.index)
        groups: dict[str, list[dict[str, object]]] = {}
        for video in current_videos:
            title_key = _normalize_title(str(video.get("title", "")))
            if not title_key:
                continue
            groups.setdefault(title_key, []).append(video)

        results: list[dict[str, object]] = []
        duplicate_group_count = 0
        delete_count = 0
        kept_count = 0

        def _video_sort_key(video: dict[str, object]) -> tuple[str, str]:
            return (
                str(video.get("published_at", "") or ""),
                str(video.get("id", "") or ""),
            )

        for videos in groups.values():
            if len(videos) <= 1:
                continue
            duplicate_group_count += 1
            ordered_videos = sorted(videos, key=_video_sort_key)
            kept_video = ordered_videos[-1]
            kept_video_id = str(kept_video.get("id", "")).strip()
            kept_title = str(kept_video.get("title", "") or "")
            kept_published_at = str(kept_video.get("published_at", "") or "")

            results.append(
                {
                    "title": kept_title,
                    "video_id": kept_video_id,
                    "published_at": kept_published_at,
                    "status": "kept" if execute else "would_keep",
                    "reason": "latest_upload",
                    "playlist_id": playlist_id,
                }
            )
            kept_count += 1

            for video in ordered_videos[:-1]:
                video_id = str(video.get("id", "")).strip()
                playlist_item_id = str(video.get("playlist_item_id", "")).strip()
                if not video_id:
                    results.append(
                        {
                            "title": kept_title,
                            "video_id": "",
                            "published_at": str(video.get("published_at", "") or ""),
                            "status": "skipped",
                            "reason": "missing_video_id",
                            "playlist_id": playlist_id,
                        }
                    )
                    continue
                if execute:
                    if playlist_item_id:
                        _delete_playlist_item(youtube, playlist_item_id, cfg, slot=account.index)
                    _delete_youtube_video(youtube, video_id, cfg, slot=account.index)
                results.append(
                    {
                        "title": str(video.get("title", "") or ""),
                        "video_id": video_id,
                        "playlist_item_id": playlist_item_id,
                        "published_at": str(video.get("published_at", "") or ""),
                        "status": "deleted" if execute else "would_delete",
                        "reason": "older_duplicate",
                        "kept_video_id": kept_video_id,
                        "playlist_id": playlist_id,
                    }
                )
                delete_count += 1

        if log_summary:
            LOGGER.info("Playlist %s uploaded video count: %s", playlist_id, len(current_videos))
            LOGGER.info("Playlist %s duplicate title groups: %s", playlist_id, duplicate_group_count)
            LOGGER.info("Playlist %s kept latest duplicate videos: %s", playlist_id, kept_count)
            LOGGER.info(
                "Playlist %s %s older duplicate videos: %s",
                playlist_id,
                "deleted" if execute else "would delete",
                delete_count,
            )
        return results

    return _run_with_youtube_accounts(
        accounts,
        cfg,
        operation_name="videos.bulk-delete-duplicates",
        action=_action,
    )


def list_youtube_playlists() -> list[dict[str, object]]:
    youtube_cfg = _youtube_upload_cfg_from_defaults()

    def _action(youtube, account: YouTubeAccountPaths) -> list[dict[str, object]]:
        playlists: list[dict[str, object]] = []
        page_token = None

        while True:
            response = _execute_youtube_request(
                youtube.playlists().list(
                    part="snippet,contentDetails,status",
                    mine=True,
                    maxResults=50,
                    pageToken=page_token,
                ),
                youtube_cfg,
                operation_name="playlists.list mine",
                slot=account.index,
            )
            for item in response.get("items", []) or []:
                playlists.append(_playlist_to_metadata(item))
            page_token = str(response.get("nextPageToken", "")).strip() or None
            if page_token is None:
                break
        return playlists

    return _run_with_youtube_accounts(
        _selected_youtube_accounts_from_defaults(),
        None,
        operation_name="playlists.list",
        action=_action,
    )


def get_youtube_playlist(playlist_id: str) -> dict[str, object]:
    normalized_id = _parse_playlist_id(playlist_id)
    youtube_cfg = _youtube_upload_cfg_from_defaults()

    def _action(youtube, account: YouTubeAccountPaths) -> dict[str, object]:
        response = _execute_youtube_request(
            youtube.playlists().list(
                part="snippet,contentDetails,status",
                id=normalized_id,
                maxResults=1,
            ),
            youtube_cfg,
            operation_name=f"playlists.get {normalized_id}",
            slot=account.index,
        )
        items = response.get("items", []) or []
        if not items:
            raise ValueError(f"YouTube playlist not found: {normalized_id}")
        return _playlist_to_metadata(items[0])

    return _run_with_youtube_accounts(
        _youtube_accounts_from_defaults(),
        None,
        operation_name=f"playlists.get {normalized_id}",
        action=_action,
    )


def update_youtube_playlist(
    playlist_id: str,
    *,
    title: str | None = None,
    description: str | None = None,
    privacy_status: str | None = None,
) -> dict[str, object]:
    account = _selected_youtube_accounts_from_defaults()[0]
    youtube = _build_youtube_client_for_account(account)
    youtube_cfg = _youtube_upload_cfg_from_defaults()
    current = get_youtube_playlist(playlist_id)
    normalized_id = str(current.get("id", "")).strip() or _parse_playlist_id(playlist_id)

    effective_title = current.get("title", "") if title is None else title
    effective_description = current.get("description", "") if description is None else description
    effective_privacy_status = current.get("privacy_status", "") if privacy_status is None else privacy_status

    body = {
        "id": normalized_id,
        "snippet": {
            "title": effective_title,
            "description": effective_description,
        },
        "status": {
            "privacyStatus": effective_privacy_status,
        },
    }
    response = _execute_youtube_request(
        youtube.playlists().update(part="snippet,status", body=body),
        youtube_cfg,
        operation_name=f"playlists.update {normalized_id}",
        slot=account.index,
    )
    item = response if isinstance(response, dict) and response.get("id") else None
    if item is None:
        # Some client stubs or partial API responses may not echo the full resource.
        item = {
            "id": normalized_id,
            "snippet": {
                "title": effective_title,
                "description": effective_description,
                "publishedAt": current.get("published_at", ""),
                "channelId": current.get("channel_id", ""),
                "channelTitle": current.get("channel_title", ""),
                "thumbnails": current.get("thumbnails", {}),
            },
            "status": {"privacyStatus": effective_privacy_status},
            "contentDetails": {"itemCount": current.get("item_count", 0)},
        }
    return _playlist_to_metadata(item)


def _upload_youtube(config: NovelConfig, spec: UploadSpec, *, dry_run: bool, force: bool = False) -> dict[str, str]:
    cfg = config.upload.youtube
    if not cfg.enabled:
        raise ValueError('YouTube upload is disabled. Set "upload.youtube.enabled=true" in config to enable it.')

    ordered_accounts, quota_selection = _order_youtube_accounts_for_upload(config, spec, force=force)

    preview = {
        "platform": "youtube",
        "range_key": spec.range_key,
        "project": cfg.project,
        "selected_project_slot": quota_selection.get("chosen_slot"),
        "selected_project_label": quota_selection.get("chosen_label"),
        "required_quota": quota_selection.get("required_quota"),
        "quota_estimate": quota_selection.get("estimate"),
        "quota_by_slot": quota_selection.get("quota_by_slot"),
        "video_path": str(spec.video_path),
        "thumbnail_path": str(spec.thumbnail_path),
        "title": spec.title,
        "description_preview": spec.description[:200],
        "playlist_id": spec.playlist_id,
        "privacy_status": cfg.privacy_status,
        "self_declared_made_for_kids": cfg.self_declared_made_for_kids,
        "dry_run": dry_run,
    }
    LOGGER.info("Upload spec: %s", json.dumps(preview, ensure_ascii=False))
    if dry_run:
        return {"platform": "youtube", "range_key": spec.range_key, "status": "dry-run"}

    from googleapiclient.http import MediaFileUpload

    body = {
        "snippet": {
            "title": spec.title,
            "description": spec.description,
            "categoryId": cfg.category_id,
        },
        "status": {
            "privacyStatus": cfg.privacy_status,
            "selfDeclaredMadeForKids": bool(cfg.self_declared_made_for_kids),
        },
    }
    accounts = ordered_accounts
    if not force:
        existing_video = _run_with_youtube_accounts(
            accounts,
            cfg,
            operation_name=f"playlistItems.list {spec.playlist_id}",
            action=lambda youtube, account: _find_playlist_video_by_title(
                youtube, spec.playlist_id, spec.title, cfg, slot=account.index
            ),
        )
        if existing_video is not None:
            existing_video_id = str(existing_video.get("id", "")).strip()
            LOGGER.info(
                "Skipping YouTube upload for %s because playlist %s already contains title %r (video_id=%s).",
                spec.range_key,
                spec.playlist_id,
                spec.title,
                existing_video_id or "unknown",
            )
            result = {
                "platform": "youtube",
                "range_key": spec.range_key,
                "status": "skipped",
            }
            if existing_video_id:
                result["video_id"] = existing_video_id
                result["video_url"] = f"https://www.youtube.com/watch?v={existing_video_id}"
            return result

    last_error: Exception | None = None
    for offset, account in enumerate(accounts):
        LOGGER.info(
            "Uploading %s with YouTube project %s (%s/%s, mode=%s)",
            spec.range_key,
            account.label,
            offset + 1,
            len(accounts),
            cfg.project,
        )
        youtube = _build_youtube_client_for_account(account)
        video_id = ""
        try:
            insert_request = youtube.videos().insert(
                part="snippet,status",
                body=body,
                media_body=MediaFileUpload(str(spec.video_path), chunksize=-1, resumable=True),
            )
            response = _execute_youtube_request(
                insert_request,
                cfg,
                operation_name=f"videos.insert {spec.range_key}",
                slot=account.index,
            )
            video_id = str(response.get("id", "")).strip()
            if not video_id:
                raise RuntimeError("YouTube upload completed but no video id was returned")

            _execute_youtube_request(
                youtube.thumbnails().set(videoId=video_id, media_body=MediaFileUpload(str(spec.thumbnail_path))),
                cfg,
                operation_name=f"thumbnails.set {video_id}",
                slot=account.index,
            )
            _execute_youtube_request(
                youtube.playlistItems().insert(
                    part="snippet",
                    body={
                        "snippet": {
                            "playlistId": spec.playlist_id,
                            "resourceId": {
                                "kind": "youtube#video",
                                "videoId": video_id,
                            },
                        }
                    },
                ),
                cfg,
                operation_name=f"playlistItems.insert {video_id}",
                slot=account.index,
            )
            return {
                "platform": "youtube",
                "range_key": spec.range_key,
                "status": "uploaded",
                "video_id": video_id,
                "video_url": f"https://www.youtube.com/watch?v={video_id}",
            }
        except Exception as exc:
            last_error = exc
            if video_id and _is_youtube_quota_rotation_error(exc):
                raise RuntimeError(
                    f"YouTube account {account.label} hit quota after uploading video {video_id}. "
                    "Automatic rotation was skipped to avoid cross-account ownership issues; "
                    "complete thumbnail/playlist steps manually or retry later with the same account."
                ) from exc
            if (not _is_youtube_quota_rotation_error(exc)) or offset >= len(accounts) - 1:
                raise
            reasons = ", ".join(_extract_error_reasons(exc)) or f"http {(_extract_error_status(exc) or 'unknown')}"
            LOGGER.warning(
                "YouTube videos.insert %s exhausted quota on account %s. Rotating to the next account (%s/%s). Reason: %s",
                spec.range_key,
                account.label,
                offset + 2,
                len(accounts),
                reasons,
            )
    assert last_error is not None
    raise last_error


def _run_tiktok_dry_run(config: NovelConfig, start: int, end: int, *, dry_run: bool) -> dict[str, str]:
    cfg = config.upload.tiktok
    effective_dry_run = bool(dry_run or cfg.dry_run)
    if not cfg.enabled:
        raise ValueError('TikTok upload is disabled. Set "upload.tiktok.enabled=true" in config to enable it.')
    if not effective_dry_run:
        raise NotImplementedError("TikTok real upload is not implemented yet. Use --dry-run.")

    range_key = _range_key(start, end)
    video_path = config.storage.video_dir / f"{range_key}.mp4"
    thumbnail_path = config.storage.visual_dir / f"{range_key}.png"
    menu_path = config.storage.subtitle_dir / f"{range_key}_menu.txt"
    title_path = _resolve_output_file(config, cfg.title_file, field_name="upload.tiktok.title_file")
    description_path = _resolve_output_file(config, cfg.description_file, field_name="upload.tiktok.description_file")
    title = _read_required_text(title_path, field_name="title")
    description_base = _read_required_text(description_path, field_name="description")
    menu = _read_required_text(menu_path, field_name="menu")
    if (not effective_dry_run) and (not video_path.exists()):
        raise FileNotFoundError(f"Missing video file for TikTok dry-run: {video_path}")
    if (not effective_dry_run) and (not thumbnail_path.exists()):
        raise FileNotFoundError(f"Missing thumbnail file for TikTok dry-run: {thumbnail_path}")

    payload = {
        "platform": "tiktok",
        "range_key": range_key,
        "video_path": str(video_path),
        "thumbnail_path": str(thumbnail_path),
        "title": title,
        "description": f"{description_base}\n\n{menu}",
        "privacy_level": cfg.privacy_level,
        "disable_comment": bool(cfg.disable_comment),
        "disable_duet": bool(cfg.disable_duet),
        "disable_stitch": bool(cfg.disable_stitch),
        "account_name": cfg.account_name,
        "dry_run": effective_dry_run,
    }
    LOGGER.info("TikTok dry-run upload payload: %s", json.dumps(payload, ensure_ascii=False))
    return {"platform": "tiktok", "range_key": range_key, "status": "dry-run"}


def run_upload(
    config: NovelConfig,
    start: int,
    end: int,
    *,
    platform: str,
    dry_run: bool = False,
    force: bool = False,
) -> dict[str, str]:
    platform_norm = str(platform or "").strip().lower()
    if platform_norm not in {"youtube", "tiktok"}:
        raise ValueError(f"Unsupported upload platform: {platform!r}")
    if platform_norm == "youtube":
        spec = _build_upload_spec(config, start, end, require_media_files=not dry_run)
        return _upload_youtube(config, spec, dry_run=dry_run, force=force)
    return _run_tiktok_dry_run(config, start, end, dry_run=dry_run)


def run_uploads(
    config: NovelConfig,
    ranges: list[tuple[int, int]] | tuple[tuple[int, int], ...],
    *,
    platform: str,
    dry_run: bool = False,
    force: bool = False,
) -> list[dict[str, str]]:
    platform_norm = str(platform or "").strip().lower()
    items = list(ranges)
    results: list[dict[str, str]] = []
    if not items:
        return results

    youtube_cfg = config.upload.youtube
    batch_size = max(1, int(getattr(youtube_cfg, "upload_batch_size", 1) or 1))
    batch_sleep = max(0.0, float(getattr(youtube_cfg, "upload_batch_sleep_seconds", 30.0) or 30.0))

    for index, (start, end) in enumerate(items, start=1):
        results.append(run_upload(config, start, end, platform=platform_norm, dry_run=dry_run, force=force))
        if (
            platform_norm == "youtube"
            and (not dry_run)
            and batch_sleep > 0
            and index < len(items)
            and index % batch_size == 0
        ):
            LOGGER.info(
                "Completed YouTube upload batch ending at %s/%s. Sleeping %.1fs before continuing.",
                index,
                len(items),
                batch_sleep,
            )
            time.sleep(batch_sleep)
    return results
