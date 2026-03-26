from __future__ import annotations

import json
import re
import time
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, TypeVar
from urllib.parse import parse_qs, urlparse

from novel_tts.common.logging import get_logger
from novel_tts.config.loader import _load_app_config
from novel_tts.config.models import NovelConfig

LOGGER = get_logger(__name__)

YOUTUBE_BULK_UPDATE_BATCH_SIZE = 5
YOUTUBE_BULK_UPDATE_SLEEP_SECONDS = 2.0
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


def _range_key(start: int, end: int) -> str:
    return f"chuong_{start}-{end}"


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


def _resolve_title_with_index(raw_title: str, start: int, end: int) -> str:
    title = str(raw_title or "").strip()
    if not title:
        return title
    # Range-local index (e.g. 1-10 => 1, 11-20 => 2).
    batch_size = max(1, end - start + 1)
    index = ((start - 1) // batch_size) + 1
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


@dataclass
class UploadSpec:
    platform: str
    range_key: str
    video_path: Path
    thumbnail_path: Path
    title: str
    description: str
    playlist_id: str


@dataclass
class PlaylistIndexTarget:
    range_key: str
    title: str
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


def _execute_youtube_request(request, cfg, *, operation_name: str):
    max_attempts = max(1, int(getattr(cfg, "upload_retry_max_attempts", 5) or 5))
    base_sleep = max(0.0, float(getattr(cfg, "upload_retry_base_sleep_seconds", 15.0) or 15.0))
    max_sleep = max(base_sleep, float(getattr(cfg, "upload_retry_max_sleep_seconds", 300.0) or 300.0))

    attempt = 1
    while True:
        try:
            return request.execute()
        except Exception as exc:
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
    title = _resolve_title_with_index(title_raw, start, end)
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


def _build_playlist_index_target(config: NovelConfig, start: int, end: int) -> PlaylistIndexTarget:
    range_key = _range_key(start, end)
    playlist_path = _resolve_output_file(
        config,
        config.upload.youtube.playlist_file,
        field_name="upload.youtube.playlist_file",
    )
    title_raw = _read_title_template(config)
    title = _resolve_title_with_index(title_raw, start, end)
    playlist_raw = _read_required_text(playlist_path, field_name="playlist")
    playlist_line = next((line.strip() for line in playlist_raw.splitlines() if line.strip()), "")
    playlist_id = _parse_playlist_id(playlist_line or playlist_raw)
    return PlaylistIndexTarget(range_key=range_key, title=title, playlist_id=playlist_id)


def _read_playlist_id(config: NovelConfig) -> str:
    playlist_path = _resolve_output_file(
        config,
        config.upload.youtube.playlist_file,
        field_name="upload.youtube.playlist_file",
    )
    playlist_raw = _read_required_text(playlist_path, field_name="playlist")
    playlist_line = next((line.strip() for line in playlist_raw.splitlines() if line.strip()), "")
    return _parse_playlist_id(playlist_line or playlist_raw)


def _iter_translated_playlist_index_targets(
    config: NovelConfig,
    *,
    from_chapter: int | None = None,
    to_chapter: int | None = None,
) -> list[PlaylistIndexTarget]:
    specs: list[PlaylistIndexTarget] = []
    pattern = re.compile(r"^chuong_(\d+)-(\d+)\.txt$")
    if not config.storage.translated_dir.exists():
        return specs
    for file_path in sorted(config.storage.translated_dir.iterdir()):
        if not file_path.is_file():
            continue
        match = pattern.match(file_path.name)
        if not match:
            continue
        start = int(match.group(1))
        end = int(match.group(2))
        if from_chapter is not None and end < from_chapter:
            continue
        if to_chapter is not None and start > to_chapter:
            continue
        specs.append(_build_playlist_index_target(config, start, end))
    return specs


def _update_playlist_line(description: str, *, video_id: str, playlist_id: str) -> str:
    desired_line = f"Danh sách phát: https://www.youtube.com/watch?v={video_id}&list={playlist_id}"
    lines = str(description or "").splitlines()
    if lines:
        first = lines[0].strip()
        if first.startswith("Danh sách phát:"):
            lines[0] = desired_line
            return "\n".join(lines)
    return desired_line if not description else f"{desired_line}\n{description}"


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


def _update_youtube_video_description_only(youtube, current_video: dict[str, object], new_description: str) -> dict[str, object]:
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
    response = youtube.videos().update(part="snippet,status", body=body).execute()
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


def _get_uploads_playlist_id(youtube) -> str:
    response = youtube.channels().list(part="contentDetails", mine=True, maxResults=1).execute()
    items = response.get("items", []) or []
    if not items:
        raise ValueError("Unable to resolve YouTube uploads playlist for the authenticated channel")
    related = ((items[0].get("contentDetails", {}) or {}).get("relatedPlaylists", {}) or {})
    uploads_id = str(related.get("uploads", "")).strip()
    if not uploads_id:
        raise ValueError("Authenticated YouTube channel does not expose an uploads playlist")
    return uploads_id


def _find_uploads_playlist_item_for_video(youtube, video_id: str) -> tuple[str, dict | None]:
    uploads_playlist_id = _get_uploads_playlist_id(youtube)
    page_token = None
    normalized_id = str(video_id or "").strip()
    while True:
        response = youtube.playlistItems().list(
            part="snippet,contentDetails,status",
            playlistId=uploads_playlist_id,
            maxResults=50,
            pageToken=page_token,
        ).execute()
        for item in response.get("items", []) or []:
            item_video_id = str((item.get("contentDetails", {}) or {}).get("videoId", "")).strip()
            if item_video_id == normalized_id:
                return uploads_playlist_id, item
        page_token = str(response.get("nextPageToken", "")).strip() or None
        if page_token is None:
            break
    return uploads_playlist_id, None


def _list_playlist_videos(youtube, playlist_id: str) -> list[dict[str, object]]:
    normalized_playlist_id = _parse_playlist_id(playlist_id)
    playlist_items: list[dict] = []
    page_token = None

    while True:
        response = youtube.playlistItems().list(
            part="snippet,contentDetails,status",
            playlistId=normalized_playlist_id,
            maxResults=50,
            pageToken=page_token,
        ).execute()
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
        response = youtube.videos().list(
            part="snippet,contentDetails,status,statistics",
            id=",".join(batch_ids),
            maxResults=50,
        ).execute()
        for item in response.get("items", []) or []:
            video_id = str(item.get("id", "")).strip()
            if video_id:
                videos_by_id[video_id] = item

    return [
        _video_to_metadata(videos_by_id[video_id], playlist_item=playlist_by_video_id.get(video_id))
        for video_id in ordered_ids
        if video_id in videos_by_id
    ]


def list_youtube_videos() -> list[dict[str, object]]:
    def _action(youtube, _account: YouTubeAccountPaths) -> list[dict[str, object]]:
        uploads_playlist_id = _get_uploads_playlist_id(youtube)
        return _list_playlist_videos(youtube, uploads_playlist_id)

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

    def _action(youtube, _account: YouTubeAccountPaths) -> dict[str, object]:
        response = youtube.videos().list(
            part="snippet,contentDetails,status,statistics",
            id=normalized_id,
            maxResults=1,
        ).execute()
        items = response.get("items", []) or []
        if not items:
            raise ValueError(f"YouTube video not found: {normalized_id}")
        playlist_item = None
        try:
            _uploads_playlist_id, playlist_item = _find_uploads_playlist_item_for_video(youtube, normalized_id)
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
    youtube = _load_youtube_client_from_defaults()
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
    video_response = youtube.videos().update(part="snippet,status", body=body).execute()

    updated_playlist_item = None
    if playlist_position is not None:
        uploads_playlist_id, playlist_item = _find_uploads_playlist_item_for_video(youtube, normalized_id)
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
            updated_playlist_item = youtube.playlistItems().update(part="snippet", body=playlist_body).execute()

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
    def _action(youtube, _account: YouTubeAccountPaths) -> list[dict[str, object]]:
        target_playlist_id = _read_playlist_id(config)
        remote_videos = _list_playlist_videos(youtube, target_playlist_id)
        results: list[dict[str, object]] = []
        updated_in_batch = 0
        matched_uploaded_count = 0
        unchanged_count = 0
        updated_count = 0
        if from_chapter is None and to_chapter is None:
            playlist_id = target_playlist_id
            matched_videos = [video for video in remote_videos if _video_matches_novel(config, str(video.get("title", "")))]
            for idx, remote_video in enumerate(matched_videos):
                matched_uploaded_count += 1
                remote_video_id = str(remote_video.get("id", "")).strip()
                current_title = str(remote_video.get("title", "") or "")
                current_description = str(remote_video.get("description", "") or "")
                updated_description = _update_playlist_line(
                    current_description,
                    video_id=remote_video_id,
                    playlist_id=playlist_id,
                )
                if updated_description == current_description:
                    results.append({"title": current_title, "video_id": remote_video_id, "status": "unchanged"})
                    unchanged_count += 1
                else:
                    _update_youtube_video_description_only(youtube, remote_video, updated_description)
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
        else:
            remote_by_title: dict[str, dict[str, object]] = {}
            for video in remote_videos:
                key = _normalize_title(str(video.get("title", "")))
                if key and key not in remote_by_title:
                    remote_by_title[key] = video

            specs = _iter_translated_playlist_index_targets(config, from_chapter=from_chapter, to_chapter=to_chapter)
            for idx, spec in enumerate(specs):
                title_key = _normalize_title(spec.title)
                remote_video = remote_by_title.get(title_key)
                if remote_video is None:
                    continue
                matched_uploaded_count += 1

                remote_video_id = str(remote_video.get("id", "")).strip()
                current_description = str(remote_video.get("description", "") or "")
                updated_description = _update_playlist_line(
                    current_description,
                    video_id=remote_video_id,
                    playlist_id=spec.playlist_id,
                )
                if updated_description == current_description:
                    results.append(
                        {
                            "title": spec.title,
                            "video_id": remote_video_id,
                            "status": "unchanged",
                        }
                    )
                    unchanged_count += 1
                    continue

                _update_youtube_video_description_only(youtube, remote_video, updated_description)
                results.append(
                    {
                        "title": spec.title,
                        "video_id": remote_video_id,
                        "status": "updated",
                    }
                )
                updated_count += 1
                updated_in_batch += 1
                if updated_in_batch >= YOUTUBE_BULK_UPDATE_BATCH_SIZE and idx < len(specs) - 1:
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


def list_youtube_playlists() -> list[dict[str, object]]:
    def _action(youtube, _account: YouTubeAccountPaths) -> list[dict[str, object]]:
        playlists: list[dict[str, object]] = []
        page_token = None

        while True:
            response = youtube.playlists().list(
                part="snippet,contentDetails,status",
                mine=True,
                maxResults=50,
                pageToken=page_token,
            ).execute()
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
    def _action(youtube, _account: YouTubeAccountPaths) -> dict[str, object]:
        response = youtube.playlists().list(
            part="snippet,contentDetails,status",
            id=normalized_id,
            maxResults=1,
        ).execute()
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
    youtube = _load_youtube_client_from_defaults()
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
    response = youtube.playlists().update(part="snippet,status", body=body).execute()
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


def _upload_youtube(config: NovelConfig, spec: UploadSpec, *, dry_run: bool) -> dict[str, str]:
    cfg = config.upload.youtube
    if not cfg.enabled:
        raise ValueError('YouTube upload is disabled. Set "upload.youtube.enabled=true" in config to enable it.')

    preview = {
        "platform": "youtube",
        "range_key": spec.range_key,
        "project": cfg.project,
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
    accounts = _select_youtube_accounts(_youtube_accounts_from_config(config), project_selector=cfg.project)
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
            response = _execute_youtube_request(insert_request, cfg, operation_name=f"videos.insert {spec.range_key}")
            video_id = str(response.get("id", "")).strip()
            if not video_id:
                raise RuntimeError("YouTube upload completed but no video id was returned")

            _execute_youtube_request(
                youtube.thumbnails().set(videoId=video_id, media_body=MediaFileUpload(str(spec.thumbnail_path))),
                cfg,
                operation_name=f"thumbnails.set {video_id}",
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


def run_upload(config: NovelConfig, start: int, end: int, *, platform: str, dry_run: bool = False) -> dict[str, str]:
    platform_norm = str(platform or "").strip().lower()
    if platform_norm not in {"youtube", "tiktok"}:
        raise ValueError(f"Unsupported upload platform: {platform!r}")
    if platform_norm == "youtube":
        spec = _build_upload_spec(config, start, end, require_media_files=not dry_run)
        return _upload_youtube(config, spec, dry_run=dry_run)
    return _run_tiktok_dry_run(config, start, end, dry_run=dry_run)


def run_uploads(
    config: NovelConfig,
    ranges: list[tuple[int, int]] | tuple[tuple[int, int], ...],
    *,
    platform: str,
    dry_run: bool = False,
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
        results.append(run_upload(config, start, end, platform=platform_norm, dry_run=dry_run))
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
