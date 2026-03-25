from __future__ import annotations

from pathlib import Path

import pytest

from novel_tts.common.logging import configure_logging
from novel_tts.config.models import (
    BrowserDebugConfig,
    CaptionConfig,
    CrawlConfig,
    ModelsConfig,
    NovelConfig,
    QueueConfig,
    QueueModelConfig,
    SourceConfig,
    StorageConfig,
    TtsConfig,
    TranslationConfig,
    UploadConfig,
    UploadTikTokConfig,
    UploadYouTubeConfig,
    VideoConfig,
    VisualConfig,
)
from novel_tts.upload.service import (
    _execute_youtube_request,
    get_youtube_playlist,
    get_youtube_video,
    list_youtube_playlists,
    list_youtube_videos,
    run_upload,
    run_uploads,
    update_uploaded_youtube_playlist_index_descriptions,
    update_youtube_playlist,
    update_youtube_video,
)


def _make_config(tmp_path: Path) -> NovelConfig:
    root = tmp_path
    storage = StorageConfig(
        root=root,
        input_dir=root / "input" / "novel",
        output_dir=root / "output" / "novel",
        image_dir=root / "image" / "novel",
        logs_dir=root / ".logs",
        tmp_dir=root / "tmp",
    )
    crawl = CrawlConfig(site_id="test")
    browser_debug = BrowserDebugConfig()
    source = SourceConfig(source_id="test", resolver_id="test", crawl=crawl, browser_debug=browser_debug)
    models = ModelsConfig(
        provider="gemini_http",
        enabled_models=["m1"],
        model_configs={"m1": QueueModelConfig(chunk_max_len=1000)},
    )
    translation = TranslationConfig(chapter_regex=r"^$", base_rules="", glossary_file="")
    upload = UploadConfig(
        default_platform="youtube",
        youtube=UploadYouTubeConfig(enabled=True),
        tiktok=UploadTikTokConfig(enabled=True, dry_run=True),
    )
    return NovelConfig(
        novel_id="novel",
        title="Novel",
        slug="novel",
        source_language="zh",
        target_language="vi",
        source_id="test",
        source=source,
        storage=storage,
        crawl=crawl,
        browser_debug=browser_debug,
        models=models,
        translation=translation,
        captions=CaptionConfig(),
        queue=QueueConfig(),
        tts=TtsConfig(provider="local", voice="test"),
        visual=VisualConfig(background_video="bg.mp4"),
        video=VideoConfig(),
        upload=upload,
    )


def _prepare_output_files(config: NovelConfig, *, range_key: str = "chuong_1-10") -> None:
    config.storage.video_dir.mkdir(parents=True, exist_ok=True)
    config.storage.visual_dir.mkdir(parents=True, exist_ok=True)
    config.storage.subtitle_dir.mkdir(parents=True, exist_ok=True)
    config.storage.output_dir.mkdir(parents=True, exist_ok=True)

    (config.storage.video_dir / f"{range_key}.mp4").write_bytes(b"mp4")
    (config.storage.visual_dir / f"{range_key}.png").write_bytes(b"png")
    (config.storage.subtitle_dir / f"{range_key}_menu.txt").write_text("00:00:00 - Chương 1", encoding="utf-8")
    (config.storage.output_dir / "title.txt").write_text("Tieu de", encoding="utf-8")
    (config.storage.output_dir / "description.txt").write_text("Mo ta", encoding="utf-8")
    (config.storage.output_dir / "playlist.txt").write_text("PL1234567890", encoding="utf-8")


def _prepare_translated_file(config: NovelConfig, *, range_key: str = "chuong_1-10") -> None:
    config.storage.translated_dir.mkdir(parents=True, exist_ok=True)
    (config.storage.translated_dir / f"{range_key}.txt").write_text("Noi dung", encoding="utf-8")


def test_run_upload_youtube_dry_run_builds_metadata(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    _prepare_output_files(config)

    result = run_upload(config, 1, 10, platform="youtube", dry_run=True)

    assert result["platform"] == "youtube"
    assert result["range_key"] == "chuong_1-10"
    assert result["status"] == "dry-run"


def test_run_upload_youtube_missing_menu_raises(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    _prepare_output_files(config)
    (config.storage.subtitle_dir / "chuong_1-10_menu.txt").unlink()

    with pytest.raises(FileNotFoundError, match="menu"):
        run_upload(config, 1, 10, platform="youtube", dry_run=True)


def test_run_upload_tiktok_dry_run(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    _prepare_output_files(config)

    result = run_upload(config, 1, 10, platform="tiktok", dry_run=False)

    assert result["platform"] == "tiktok"
    assert result["range_key"] == "chuong_1-10"
    assert result["status"] == "dry-run"


def test_run_upload_youtube_dry_run_does_not_require_media_files(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    _prepare_output_files(config)
    (config.storage.video_dir / "chuong_1-10.mp4").unlink()
    (config.storage.visual_dir / "chuong_1-10.png").unlink()

    result = run_upload(config, 1, 10, platform="youtube", dry_run=True)

    assert result["status"] == "dry-run"


def test_run_upload_tiktok_dry_run_does_not_require_media_files(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    _prepare_output_files(config)
    (config.storage.video_dir / "chuong_1-10.mp4").unlink()
    (config.storage.visual_dir / "chuong_1-10.png").unlink()

    result = run_upload(config, 1, 10, platform="tiktok", dry_run=True)

    assert result["status"] == "dry-run"


def test_run_upload_youtube_dry_run_rewrites_tap_index_from_range(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config = _make_config(tmp_path)
    _prepare_output_files(config, range_key="chuong_11-20")
    (config.storage.output_dir / "title.txt").write_text("Tập 1 | Vô Cực Thiên Tôn", encoding="utf-8")

    captured: dict[str, str] = {}

    def _capture(config, spec, *, dry_run):  # type: ignore[no-redef]
        captured["title"] = spec.title
        return {"status": "dry-run", "platform": "youtube", "range_key": spec.range_key}

    from novel_tts.upload import service as upload_service

    monkeypatch.setattr(upload_service, "_upload_youtube", _capture)
    result = run_upload(config, 11, 20, platform="youtube", dry_run=True)

    assert result["status"] == "dry-run"
    assert captured["title"].startswith("Tập 2")


def test_run_uploads_batches_youtube_with_sleep(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config = _make_config(tmp_path)
    config.upload.youtube.upload_batch_size = 2
    config.upload.youtube.upload_batch_sleep_seconds = 7.5

    calls: list[tuple[int, int, str, bool]] = []
    sleeps: list[float] = []

    monkeypatch.setattr(
        "novel_tts.upload.service.run_upload",
        lambda _cfg, start, end, *, platform, dry_run=False: calls.append((start, end, platform, dry_run))
        or {"range_key": f"chuong_{start}-{end}", "status": "uploaded"},
    )
    monkeypatch.setattr("novel_tts.upload.service.time.sleep", lambda seconds: sleeps.append(seconds))

    result = run_uploads(config, [(1, 10), (11, 20), (21, 30)], platform="youtube", dry_run=False)

    assert [item["range_key"] for item in result] == ["chuong_1-10", "chuong_11-20", "chuong_21-30"]
    assert calls == [
        (1, 10, "youtube", False),
        (11, 20, "youtube", False),
        (21, 30, "youtube", False),
    ]
    assert sleeps == [7.5]


def test_run_uploads_skips_batch_sleep_for_dry_run(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config = _make_config(tmp_path)
    config.upload.youtube.upload_batch_size = 1
    config.upload.youtube.upload_batch_sleep_seconds = 9.0

    monkeypatch.setattr(
        "novel_tts.upload.service.run_upload",
        lambda _cfg, start, end, *, platform, dry_run=False: {"range_key": f"chuong_{start}-{end}", "status": "dry-run"},
    )
    monkeypatch.setattr(
        "novel_tts.upload.service.time.sleep",
        lambda _seconds: pytest.fail("batch sleep should not run during dry-run"),
    )

    result = run_uploads(config, [(1, 10), (11, 20)], platform="youtube", dry_run=True)

    assert [item["status"] for item in result] == ["dry-run", "dry-run"]


def test_execute_youtube_request_retries_rate_limit(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    config.upload.youtube.upload_retry_max_attempts = 4
    config.upload.youtube.upload_retry_base_sleep_seconds = 3.0
    config.upload.youtube.upload_retry_max_sleep_seconds = 10.0

    sleeps: list[float] = []

    class _FakeRequest:
        def __init__(self) -> None:
            self.calls = 0

        def execute(self):
            self.calls += 1
            if self.calls < 3:
                raise ExceptionWithHttpError(403, b'{"error":{"errors":[{"reason":"rateLimitExceeded"}]}}')
            return {"id": "vid123"}

    monkeypatch.setattr("novel_tts.upload.service.time.sleep", lambda seconds: sleeps.append(seconds))

    result = _execute_youtube_request(_FakeRequest(), config.upload.youtube, operation_name="videos.insert chuong_1-10")

    assert result == {"id": "vid123"}
    assert sleeps == [3.0, 6.0]


class ExceptionWithHttpError(Exception):
    def __init__(self, status: int, content: bytes) -> None:
        super().__init__("http error")
        self.content = content
        self.resp = type("Resp", (), {"status": status})()


def test_list_youtube_playlists_collects_all_pages(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Request:
        def __init__(self, payload):
            self._payload = payload

        def execute(self):
            return self._payload

    class _PlaylistsResource:
        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []

        def list(self, **kwargs):
            self.calls.append(kwargs)
            if kwargs.get("pageToken") == "NEXT":
                return _Request(
                    {
                        "items": [
                            {
                                "id": "PL2",
                                "snippet": {"title": "Playlist 2", "channelTitle": "Channel", "thumbnails": {}},
                                "contentDetails": {"itemCount": 3},
                                "status": {"privacyStatus": "public"},
                            }
                        ]
                    }
                )
            return _Request(
                {
                    "items": [
                        {
                            "id": "PL1",
                            "snippet": {
                                "title": "Playlist 1",
                                "description": "Desc",
                                "publishedAt": "2026-01-01T00:00:00Z",
                                "channelId": "UC123",
                                "channelTitle": "Channel",
                                "thumbnails": {"default": {"url": "https://img/1.jpg"}},
                            },
                            "contentDetails": {"itemCount": 10},
                            "status": {"privacyStatus": "private"},
                        }
                    ],
                    "nextPageToken": "NEXT",
                }
            )

    class _YoutubeClient:
        def __init__(self) -> None:
            self.resource = _PlaylistsResource()

        def playlists(self):
            return self.resource

    client = _YoutubeClient()
    monkeypatch.setattr("novel_tts.upload.service._load_youtube_client_from_defaults", lambda: client)

    result = list_youtube_playlists()

    assert [item["id"] for item in result] == ["PL1", "PL2"]
    assert result[0]["item_count"] == 10
    assert result[0]["privacy_status"] == "private"
    assert result[0]["thumbnails"] == {"default": "https://img/1.jpg"}
    assert result[0]["url"] == "https://www.youtube.com/playlist?list=PL1"
    assert client.resource.calls[0]["mine"] is True
    assert client.resource.calls[1]["pageToken"] == "NEXT"


def test_get_youtube_playlist_accepts_full_url(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Request:
        def execute(self):
            return {
                "items": [
                    {
                        "id": "PL123",
                        "snippet": {"title": "Playlist 123", "thumbnails": {}},
                        "contentDetails": {"itemCount": 7},
                        "status": {"privacyStatus": "public"},
                    }
                ]
            }

    class _PlaylistsResource:
        def __init__(self) -> None:
            self.last_kwargs: dict[str, object] = {}

        def list(self, **kwargs):
            self.last_kwargs = kwargs
            return _Request()

    class _YoutubeClient:
        def __init__(self) -> None:
            self.resource = _PlaylistsResource()

        def playlists(self):
            return self.resource

    client = _YoutubeClient()
    monkeypatch.setattr("novel_tts.upload.service._load_youtube_client_from_defaults", lambda: client)

    result = get_youtube_playlist("https://www.youtube.com/playlist?list=PL123")

    assert result["id"] == "PL123"
    assert result["title"] == "Playlist 123"
    assert client.resource.last_kwargs["id"] == "PL123"


def test_build_youtube_client_falls_back_to_oauth_when_refresh_scope_is_stale(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    credentials_path = tmp_path / "client_secrets.json"
    token_path = tmp_path / "token.json"
    credentials_path.write_text("{}", encoding="utf-8")
    token_path.write_text("{}", encoding="utf-8")

    class _FakeRefreshError(Exception):
        pass

    class _FakeCreds:
        def __init__(self) -> None:
            self.valid = False
            self.expired = True
            self.refresh_token = "refresh-token"

        def refresh(self, _request) -> None:
            raise _FakeRefreshError("invalid_scope")

        def to_json(self) -> str:
            return '{"token":"new"}'

    fake_creds = _FakeCreds()

    class _FakeFlow:
        called = False

        @classmethod
        def from_client_secrets_file(cls, _path, _scopes):
            return cls()

        def run_local_server(self, port=0):
            type(self).called = True
            return fake_creds

    monkeypatch.setitem(__import__("sys").modules, "google.auth.transport.requests", type("_M", (), {"Request": object}))
    monkeypatch.setitem(__import__("sys").modules, "google.auth.exceptions", type("_M", (), {"RefreshError": _FakeRefreshError}))
    monkeypatch.setitem(
        __import__("sys").modules,
        "google.oauth2.credentials",
        type("_M", (), {"Credentials": type("_C", (), {"from_authorized_user_file": staticmethod(lambda _p, _s: fake_creds)})}),
    )
    monkeypatch.setitem(
        __import__("sys").modules,
        "google_auth_oauthlib.flow",
        type("_M", (), {"InstalledAppFlow": _FakeFlow}),
    )
    monkeypatch.setitem(
        __import__("sys").modules,
        "googleapiclient.discovery",
        type("_M", (), {"build": lambda *_args, **_kwargs: "youtube-client"}),
    )

    from novel_tts.upload.service import _build_youtube_client_from_paths

    result = _build_youtube_client_from_paths(credentials_path, token_path)

    assert result == "youtube-client"
    assert _FakeFlow.called is True
    assert token_path.read_text(encoding="utf-8") == '{"token":"new"}'


def test_update_youtube_playlist_preserves_current_fields_when_not_overridden(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Request:
        def __init__(self, payload):
            self._payload = payload

        def execute(self):
            return self._payload

    class _PlaylistsResource:
        def __init__(self) -> None:
            self.last_update_kwargs: dict[str, object] = {}

        def list(self, **_kwargs):
            return _Request(
                {
                    "items": [
                        {
                            "id": "PL123",
                            "snippet": {
                                "title": "Current title",
                                "description": "Current description",
                                "publishedAt": "2026-01-01T00:00:00Z",
                                "channelId": "UC1",
                                "channelTitle": "Channel",
                                "thumbnails": {},
                            },
                            "contentDetails": {"itemCount": 5},
                            "status": {"privacyStatus": "private"},
                        }
                    ]
                }
            )

        def update(self, **kwargs):
            self.last_update_kwargs = kwargs
            return _Request(kwargs["body"])

    class _YoutubeClient:
        def __init__(self) -> None:
            self.resource = _PlaylistsResource()

        def playlists(self):
            return self.resource

    client = _YoutubeClient()
    monkeypatch.setattr("novel_tts.upload.service._load_youtube_client_from_defaults", lambda: client)

    result = update_youtube_playlist("PL123")

    assert result["id"] == "PL123"
    assert result["title"] == "Current title"
    assert result["privacy_status"] == "private"
    assert client.resource.last_update_kwargs["body"] == {
        "id": "PL123",
        "snippet": {
            "title": "Current title",
            "description": "Current description",
        },
        "status": {
            "privacyStatus": "private",
        },
    }


def test_list_youtube_videos_collects_uploads_playlist_with_batched_video_requests(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Request:
        def __init__(self, payload):
            self._payload = payload

        def execute(self):
            return self._payload

    class _ChannelsResource:
        def list(self, **kwargs):
            assert kwargs["mine"] is True
            return _Request(
                {
                    "items": [
                        {
                            "contentDetails": {
                                "relatedPlaylists": {"uploads": "UU123"}
                            }
                        }
                    ]
                }
            )

    class _PlaylistItemsResource:
        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []

        def list(self, **kwargs):
            self.calls.append(kwargs)
            if kwargs.get("pageToken") == "NEXT":
                return _Request(
                    {
                        "items": [
                            {
                                "id": "PLI2",
                                "snippet": {"position": 1, "publishedAt": "2026-03-01T00:01:00Z"},
                                "contentDetails": {"videoId": "vid2"},
                            }
                        ]
                    }
                )
            return _Request(
                {
                    "items": [
                        {
                            "id": "PLI1",
                            "snippet": {"position": 0, "publishedAt": "2026-03-01T00:00:00Z"},
                            "contentDetails": {"videoId": "vid1"},
                        }
                    ],
                    "nextPageToken": "NEXT",
                }
            )

    class _VideosResource:
        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []

        def list(self, **kwargs):
            self.calls.append(kwargs)
            return _Request(
                {
                    "items": [
                        {
                            "id": "vid1",
                            "snippet": {"title": "Video 1", "thumbnails": {}},
                            "contentDetails": {"duration": "PT1M"},
                            "status": {"privacyStatus": "public"},
                            "statistics": {"viewCount": "10"},
                        },
                        {
                            "id": "vid2",
                            "snippet": {"title": "Video 2", "thumbnails": {}},
                            "contentDetails": {"duration": "PT2M"},
                            "status": {"privacyStatus": "private"},
                            "statistics": {"viewCount": "20"},
                        },
                    ]
                }
            )

    class _YoutubeClient:
        def __init__(self) -> None:
            self._channels = _ChannelsResource()
            self._playlist_items = _PlaylistItemsResource()
            self._videos = _VideosResource()

        def channels(self):
            return self._channels

        def playlistItems(self):
            return self._playlist_items

        def videos(self):
            return self._videos

    client = _YoutubeClient()
    monkeypatch.setattr("novel_tts.upload.service._load_youtube_client_from_defaults", lambda: client)

    result = list_youtube_videos()

    assert [item["id"] for item in result] == ["vid1", "vid2"]
    assert result[0]["title"] == "Video 1"
    assert result[0]["playlist_position"] == 0
    assert result[1]["privacy_status"] == "private"
    assert client._playlist_items.calls[0]["maxResults"] == 50
    assert client._videos.calls[0]["id"] == "vid1,vid2"


def test_get_youtube_video_returns_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Request:
        def execute(self):
            return {
                "items": [
                    {
                        "id": "vid1",
                        "snippet": {"title": "Video 1", "thumbnails": {}},
                        "contentDetails": {"duration": "PT3M"},
                        "status": {"privacyStatus": "public"},
                        "statistics": {"viewCount": "42"},
                    }
                ]
            }

    class _VideosResource:
        def __init__(self) -> None:
            self.last_kwargs: dict[str, object] = {}

        def list(self, **kwargs):
            self.last_kwargs = kwargs
            return _Request()

    class _YoutubeClient:
        def __init__(self) -> None:
            self.resource = _VideosResource()

        def videos(self):
            return self.resource

    client = _YoutubeClient()
    monkeypatch.setattr("novel_tts.upload.service._load_youtube_client_from_defaults", lambda: client)

    result = get_youtube_video("vid1")

    assert result["id"] == "vid1"
    assert result["title"] == "Video 1"
    assert result["view_count"] == "42"
    assert client.resource.last_kwargs["id"] == "vid1"


def test_update_youtube_video_preserves_current_fields_when_not_overridden(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Request:
        def __init__(self, payload):
            self._payload = payload

        def execute(self):
            return self._payload

    class _VideosResource:
        def __init__(self) -> None:
            self.last_update_kwargs: dict[str, object] = {}

        def update(self, **kwargs):
            self.last_update_kwargs = kwargs
            return _Request(kwargs["body"])

    class _PlaylistItemsResource:
        def __init__(self) -> None:
            self.last_update_kwargs: dict[str, object] = {}

        def list(self, **_kwargs):
            return _Request(
                {
                    "items": [
                        {
                            "id": "PLI1",
                            "snippet": {
                                "playlistId": "UU123",
                                "position": 4,
                                "publishedAt": "2026-03-01T00:00:00Z",
                            },
                            "contentDetails": {"videoId": "vid1"},
                        }
                    ]
                }
            )

        def update(self, **kwargs):
            self.last_update_kwargs = kwargs
            return _Request(kwargs["body"])

    class _ChannelsResource:
        def list(self, **_kwargs):
            return _Request(
                {
                    "items": [
                        {
                            "contentDetails": {
                                "relatedPlaylists": {"uploads": "UU123"}
                            }
                        }
                    ]
                }
            )

    class _YoutubeClient:
        def __init__(self) -> None:
            self._videos = _VideosResource()
            self._playlist_items = _PlaylistItemsResource()
            self._channels = _ChannelsResource()

        def videos(self):
            return self._videos

        def playlistItems(self):
            return self._playlist_items

        def channels(self):
            return self._channels

    client = _YoutubeClient()
    monkeypatch.setattr("novel_tts.upload.service._load_youtube_client_from_defaults", lambda: client)
    monkeypatch.setattr(
        "novel_tts.upload.service.get_youtube_video",
        lambda _video_id: {
            "id": "vid1",
            "title": "Current title",
            "description": "Current description",
            "privacy_status": "private",
            "made_for_kids": False,
            "playlist_position": 4,
            "category_id": "22",
            "published_at": "2026-03-01T00:00:00Z",
            "channel_id": "UC1",
            "channel_title": "Channel",
            "thumbnails": {},
            "upload_status": "processed",
            "license": "youtube",
            "embeddable": True,
            "public_stats_viewable": True,
            "duration": "PT1M",
            "dimension": "2d",
            "definition": "hd",
            "caption": "false",
            "licensed_content": True,
            "projection": "rectangular",
            "view_count": "10",
            "like_count": "2",
            "favorite_count": "0",
            "comment_count": "1",
            "tags": [],
            "live_broadcast_content": "none",
        },
    )

    result = update_youtube_video("vid1")

    assert result["id"] == "vid1"
    assert result["title"] == "Current title"
    assert result["playlist_position"] == 4
    assert client._videos.last_update_kwargs["body"] == {
        "id": "vid1",
        "snippet": {
            "title": "Current title",
            "description": "Current description",
            "categoryId": "22",
        },
        "status": {
            "privacyStatus": "private",
            "selfDeclaredMadeForKids": False,
        },
    }
    assert client._playlist_items.last_update_kwargs == {}


def test_update_uploaded_youtube_playlist_index_descriptions_updates_first_line(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _make_config(tmp_path)
    _prepare_output_files(config, range_key="chuong_1-10")
    (config.storage.subtitle_dir / "chuong_1-10_menu.txt").unlink()
    _prepare_translated_file(config, range_key="chuong_1-10")
    (config.storage.output_dir / "title.txt").write_text(
        "Tập 1 | Thanh Niên Giả Câm 18 Năm | Thái Hư Chí Tôn | FULL",
        encoding="utf-8",
    )

    updates: list[dict[str, object]] = []
    class _Request:
        def __init__(self, payload):
            self._payload = payload

        def execute(self):
            return self._payload

    class _ChannelsResource:
        def list(self, **_kwargs):
            return _Request({"items": [{"contentDetails": {"relatedPlaylists": {"uploads": "UU123"}}}]})

    class _PlaylistItemsResource:
        def list(self, **_kwargs):
            return _Request(
                {
                    "items": [
                        {
                            "id": "PLI1",
                            "snippet": {"playlistId": "UU123", "position": 0, "publishedAt": "2026-03-01T00:00:00Z"},
                            "contentDetails": {"videoId": "vid123"},
                        }
                    ]
                }
            )

    class _VideosResource:
        def list(self, **_kwargs):
            return _Request(
                {
                    "items": [
                        {
                            "id": "vid123",
                            "snippet": {
                                "title": "Tập 1 | Thanh Niên Giả Câm 18 Năm | Thái Hư Chí Tôn | FULL",
                                "description": "Danh sách phát: https://www.youtube.com/watch?v=oldid&list=PL1234567890\nRest of desc",
                                "thumbnails": {},
                                "categoryId": "22",
                            },
                            "contentDetails": {},
                            "status": {"privacyStatus": "public"},
                            "statistics": {},
                        }
                    ]
                }
            )

        def update(self, **kwargs):
            updates.append({"video_id": kwargs["body"]["id"], "description": kwargs["body"]["snippet"]["description"]})
            return _Request(kwargs["body"])

    class _YoutubeClient:
        def channels(self):
            return _ChannelsResource()

        def playlistItems(self):
            return _PlaylistItemsResource()

        def videos(self):
            return _VideosResource()

    monkeypatch.setattr("novel_tts.upload.service._load_youtube_client_from_defaults", lambda: _YoutubeClient())

    result = update_uploaded_youtube_playlist_index_descriptions(config)

    assert result == [
        {
            "title": "Tập 1 | Thanh Niên Giả Câm 18 Năm | Thái Hư Chí Tôn | FULL",
            "video_id": "vid123",
            "status": "updated",
        }
    ]
    assert updates == [
        {
            "video_id": "vid123",
            "description": "Danh sách phát: https://www.youtube.com/watch?v=vid123&list=PL1234567890\nRest of desc",
        }
    ]


def test_update_uploaded_youtube_playlist_index_descriptions_sleeps_between_batches(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _make_config(tmp_path)
    _prepare_output_files(config, range_key="chuong_1-10")
    _prepare_translated_file(config, range_key="chuong_1-10")
    _prepare_translated_file(config, range_key="chuong_11-20")
    _prepare_translated_file(config, range_key="chuong_21-30")
    (config.storage.output_dir / "title.txt").write_text(
        "Tập {index} | Thanh Niên Giả Câm 18 Năm | Thái Hư Chí Tôn | FULL",
        encoding="utf-8",
    )

    monkeypatch.setattr("novel_tts.upload.service.YOUTUBE_BULK_UPDATE_BATCH_SIZE", 2)
    monkeypatch.setattr("novel_tts.upload.service.YOUTUBE_BULK_UPDATE_SLEEP_SECONDS", 0.5)

    updates: list[str] = []
    sleeps: list[float] = []
    class _Request:
        def __init__(self, payload):
            self._payload = payload

        def execute(self):
            return self._payload

    class _ChannelsResource:
        def list(self, **_kwargs):
            return _Request({"items": [{"contentDetails": {"relatedPlaylists": {"uploads": "UU123"}}}]})

    class _PlaylistItemsResource:
        def list(self, **_kwargs):
            return _Request(
                {
                    "items": [
                        {"id": "PLI1", "snippet": {"playlistId": "UU123", "position": 0}, "contentDetails": {"videoId": "vid1"}},
                        {"id": "PLI2", "snippet": {"playlistId": "UU123", "position": 1}, "contentDetails": {"videoId": "vid2"}},
                        {"id": "PLI3", "snippet": {"playlistId": "UU123", "position": 2}, "contentDetails": {"videoId": "vid3"}},
                    ]
                }
            )

    class _VideosResource:
        def list(self, **_kwargs):
            return _Request(
                {
                    "items": [
                        {"id": "vid1", "snippet": {"title": "Tập 1 | Thanh Niên Giả Câm 18 Năm | Thái Hư Chí Tôn | FULL", "description": "Danh sách phát: https://www.youtube.com/watch?v=old1&list=PL1234567890\nRest", "thumbnails": {}, "categoryId": "22"}, "contentDetails": {}, "status": {"privacyStatus": "public"}, "statistics": {}},
                        {"id": "vid2", "snippet": {"title": "Tập 2 | Thanh Niên Giả Câm 18 Năm | Thái Hư Chí Tôn | FULL", "description": "Danh sách phát: https://www.youtube.com/watch?v=old2&list=PL1234567890\nRest", "thumbnails": {}, "categoryId": "22"}, "contentDetails": {}, "status": {"privacyStatus": "public"}, "statistics": {}},
                        {"id": "vid3", "snippet": {"title": "Tập 3 | Thanh Niên Giả Câm 18 Năm | Thái Hư Chí Tôn | FULL", "description": "Danh sách phát: https://www.youtube.com/watch?v=old3&list=PL1234567890\nRest", "thumbnails": {}, "categoryId": "22"}, "contentDetails": {}, "status": {"privacyStatus": "public"}, "statistics": {}},
                    ]
                }
            )

        def update(self, **kwargs):
            updates.append(kwargs["body"]["id"])
            return _Request(kwargs["body"])

    class _YoutubeClient:
        def channels(self):
            return _ChannelsResource()

        def playlistItems(self):
            return _PlaylistItemsResource()

        def videos(self):
            return _VideosResource()

    monkeypatch.setattr("novel_tts.upload.service._load_youtube_client_from_defaults", lambda: _YoutubeClient())
    monkeypatch.setattr("novel_tts.upload.service.time.sleep", lambda seconds: sleeps.append(seconds))

    result = update_uploaded_youtube_playlist_index_descriptions(config)

    assert [item["status"] for item in result] == ["updated", "updated", "updated"]
    assert updates == ["vid1", "vid2", "vid3"]
    assert sleeps == [0.5]


def test_update_uploaded_youtube_playlist_index_descriptions_skips_non_uploaded_videos_quietly(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _make_config(tmp_path)
    _prepare_output_files(config, range_key="chuong_1-10")
    _prepare_translated_file(config, range_key="chuong_1-10")
    (config.storage.output_dir / "title.txt").write_text(
        "Tập 1 | Thanh Niên Giả Câm 18 Năm | Thái Hư Chí Tôn | FULL",
        encoding="utf-8",
    )

    class _Request:
        def __init__(self, payload):
            self._payload = payload

        def execute(self):
            return self._payload

    class _ChannelsResource:
        def list(self, **_kwargs):
            return _Request({"items": [{"contentDetails": {"relatedPlaylists": {"uploads": "UU123"}}}]})

    class _PlaylistItemsResource:
        def list(self, **_kwargs):
            return _Request({"items": []})

    class _VideosResource:
        def list(self, **_kwargs):
            return _Request({"items": []})

    class _YoutubeClient:
        def channels(self):
            return _ChannelsResource()

        def playlistItems(self):
            return _PlaylistItemsResource()

        def videos(self):
            return _VideosResource()

    monkeypatch.setattr("novel_tts.upload.service._load_youtube_client_from_defaults", lambda: _YoutubeClient())

    result = update_uploaded_youtube_playlist_index_descriptions(config)

    assert result == []


def test_update_uploaded_youtube_playlist_index_descriptions_summary_counts_only_matched_novel_videos(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _make_config(tmp_path)
    _prepare_output_files(config, range_key="chuong_1-10")
    _prepare_translated_file(config, range_key="chuong_1-10")
    (config.storage.output_dir / "title.txt").write_text(
        "Tập 1 | Thanh Niên Giả Câm 18 Năm | Thái Hư Chí Tôn | FULL",
        encoding="utf-8",
    )
    log_path = tmp_path / "summary.log"
    configure_logging(log_path)

    class _Request:
        def __init__(self, payload):
            self._payload = payload

        def execute(self):
            return self._payload

    class _ChannelsResource:
        def list(self, **_kwargs):
            return _Request({"items": [{"contentDetails": {"relatedPlaylists": {"uploads": "UU123"}}}]})

    class _PlaylistItemsResource:
        def list(self, **_kwargs):
            return _Request(
                {
                    "items": [
                        {"id": "PLI1", "snippet": {"playlistId": "UU123", "position": 0}, "contentDetails": {"videoId": "vid1"}},
                        {"id": "PLI2", "snippet": {"playlistId": "UU123", "position": 1}, "contentDetails": {"videoId": "other1"}},
                    ]
                }
            )

    class _VideosResource:
        def list(self, **_kwargs):
            return _Request(
                {
                    "items": [
                        {"id": "vid1", "snippet": {"title": "Tập 1 | Thanh Niên Giả Câm 18 Năm | Thái Hư Chí Tôn | FULL", "description": "Danh sách phát: https://www.youtube.com/watch?v=vid1&list=PL1234567890\nRest", "thumbnails": {}, "categoryId": "22"}, "contentDetails": {}, "status": {"privacyStatus": "public"}, "statistics": {}},
                        {"id": "other1", "snippet": {"title": "Some other uploaded video", "description": "Other description", "thumbnails": {}, "categoryId": "22"}, "contentDetails": {}, "status": {"privacyStatus": "public"}, "statistics": {}},
                    ]
                }
            )

    class _YoutubeClient:
        def channels(self):
            return _ChannelsResource()

        def playlistItems(self):
            return _PlaylistItemsResource()

        def videos(self):
            return _VideosResource()

    monkeypatch.setattr("novel_tts.upload.service._load_youtube_client_from_defaults", lambda: _YoutubeClient())

    result = update_uploaded_youtube_playlist_index_descriptions(config)

    assert result == [
        {
            "title": "Tập 1 | Thanh Niên Giả Câm 18 Năm | Thái Hư Chí Tôn | FULL",
            "video_id": "vid1",
            "status": "unchanged",
        }
    ]
    content = log_path.read_text(encoding="utf-8")
    assert "Uploaded Video count: 1" in content
    assert "Correct description - video count: 1" in content
    assert "Update description - video count: 0" in content


def test_update_uploaded_youtube_playlist_index_descriptions_without_range_filters_by_novel_title(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _make_config(tmp_path)
    config.novel_id = "khong-qua-phong-tuyet"
    config.title = "Vo Cuc Thien Ton"
    _prepare_output_files(config, range_key="chuong_1-10")
    (config.storage.output_dir / "title.txt").write_text(
        "Không qua phong tuyết, sao thấy cầu vồng? | FULL | Tập 1",
        encoding="utf-8",
    )

    updates: list[str] = []
    class _Request:
        def __init__(self, payload):
            self._payload = payload

        def execute(self):
            return self._payload

    class _ChannelsResource:
        def list(self, **_kwargs):
            return _Request({"items": [{"contentDetails": {"relatedPlaylists": {"uploads": "UU123"}}}]})

    class _PlaylistItemsResource:
        def list(self, **_kwargs):
            return _Request(
                {
                    "items": [
                        {"id": "PLI1", "snippet": {"playlistId": "UU123", "position": 0}, "contentDetails": {"videoId": "vid1"}},
                        {"id": "PLI2", "snippet": {"playlistId": "UU123", "position": 1}, "contentDetails": {"videoId": "other1"}},
                    ]
                }
            )

    class _VideosResource:
        def list(self, **_kwargs):
            return _Request(
                {
                    "items": [
                        {"id": "vid1", "snippet": {"title": "Không qua phong tuyết, sao thấy cầu vồng? | FULL | Tập 8", "description": "Danh sách phát: https://www.youtube.com/watch?v=old1&list=PL1234567890\nRest", "thumbnails": {}, "categoryId": "22"}, "contentDetails": {}, "status": {"privacyStatus": "public"}, "statistics": {}},
                        {"id": "other1", "snippet": {"title": "Some other uploaded video", "description": "Other description", "thumbnails": {}, "categoryId": "22"}, "contentDetails": {}, "status": {"privacyStatus": "public"}, "statistics": {}},
                    ]
                }
            )

        def update(self, **kwargs):
            updates.append(kwargs["body"]["id"])
            return _Request(kwargs["body"])

    class _YoutubeClient:
        def channels(self):
            return _ChannelsResource()

        def playlistItems(self):
            return _PlaylistItemsResource()

        def videos(self):
            return _VideosResource()

    monkeypatch.setattr("novel_tts.upload.service._load_youtube_client_from_defaults", lambda: _YoutubeClient())

    result = update_uploaded_youtube_playlist_index_descriptions(config)

    assert result == [
        {
            "title": "Không qua phong tuyết, sao thấy cầu vồng? | FULL | Tập 8",
            "video_id": "vid1",
            "status": "updated",
        }
    ]
    assert updates == ["vid1"]
