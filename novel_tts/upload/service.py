from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from novel_tts.common.logging import get_logger
from novel_tts.config.models import NovelConfig

LOGGER = get_logger(__name__)

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


@dataclass
class UploadSpec:
    platform: str
    range_key: str
    video_path: Path
    thumbnail_path: Path
    title: str
    description: str
    playlist_id: str


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


def _load_youtube_client(config: NovelConfig):
    try:
        from google.auth.transport.requests import Request
        from google.oauth2.credentials import Credentials
        from google_auth_oauthlib.flow import InstalledAppFlow
        from googleapiclient.discovery import build
    except Exception as exc:
        raise RuntimeError(
            "Missing YouTube upload dependencies. Install project dependencies again to include Google API libs."
        ) from exc

    credentials_path = Path(config.upload.youtube.credentials_path).expanduser()
    if not credentials_path.is_absolute():
        credentials_path = config.storage.root / credentials_path
    token_path = Path(config.upload.youtube.token_path).expanduser()
    if not token_path.is_absolute():
        token_path = config.storage.root / token_path

    if not credentials_path.exists():
        raise FileNotFoundError(f"YouTube credentials file not found: {credentials_path}")

    creds = None
    if token_path.exists():
        creds = Credentials.from_authorized_user_file(str(token_path), YOUTUBE_UPLOAD_SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(str(credentials_path), YOUTUBE_UPLOAD_SCOPES)
            creds = flow.run_local_server(port=0)
        token_path.parent.mkdir(parents=True, exist_ok=True)
        token_path.write_text(creds.to_json(), encoding="utf-8")

    return build("youtube", "v3", credentials=creds)


def _upload_youtube(config: NovelConfig, spec: UploadSpec, *, dry_run: bool) -> dict[str, str]:
    cfg = config.upload.youtube
    if not cfg.enabled:
        raise ValueError('YouTube upload is disabled. Set "upload.youtube.enabled=true" in config to enable it.')

    preview = {
        "platform": "youtube",
        "range_key": spec.range_key,
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

    youtube = _load_youtube_client(config)
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
    insert_request = youtube.videos().insert(
        part="snippet,status",
        body=body,
        media_body=MediaFileUpload(str(spec.video_path), chunksize=-1, resumable=True),
    )
    response = insert_request.execute()
    video_id = str(response.get("id", "")).strip()
    if not video_id:
        raise RuntimeError("YouTube upload completed but no video id was returned")

    youtube.thumbnails().set(videoId=video_id, media_body=MediaFileUpload(str(spec.thumbnail_path))).execute()
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
    ).execute()
    return {
        "platform": "youtube",
        "range_key": spec.range_key,
        "status": "uploaded",
        "video_id": video_id,
        "video_url": f"https://www.youtube.com/watch?v={video_id}",
    }


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
