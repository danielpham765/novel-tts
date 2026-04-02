from __future__ import annotations

import hashlib
import re
from pathlib import Path

from novel_tts.common.ffmpeg import ffmpeg_has_filter, ffprobe_duration, run_ffmpeg
from novel_tts.config.models import NovelConfig
from novel_tts.media_batch import count_media_batches_before, get_media_batch_range, media_range_key


def _range_key(start: int, end: int) -> str:
    return media_range_key(start, end)


def _esc_drawtext(text: str) -> str:
    return str(text or "").replace("\\", "\\\\").replace(":", "\\:").replace("'", "\\'")


def _line1_for_chapter(template: str, chapter: int) -> str:
    text = str(template or "").strip()
    if not text:
        return f"Tập {chapter}"
    if "{chapter}" in text:
        return text.replace("{chapter}", str(chapter))
    if re.search(r"\d+", text):
        return re.sub(r"\d+", str(chapter), text, count=1)
    return f"{text} {chapter}"


def _cache_dir(output_dir: Path) -> Path:
    return output_dir / ".cache"


def _cache_path(output_dir: Path, range_key: str) -> Path:
    return _cache_dir(output_dir) / f"{range_key}.sha256"


def _file_signature(path: Path) -> str:
    stat = path.stat()
    return f"{path.resolve()}:{stat.st_size}:{stat.st_mtime_ns}"


def _cache_value(*parts: str) -> str:
    payload = "\n".join(parts)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _read_cache(output_dir: Path, range_key: str) -> str | None:
    path = _cache_path(output_dir, range_key)
    if not path.exists():
        return None
    try:
        return path.read_text(encoding="utf-8").strip() or None
    except Exception:
        return None


def _write_cache(output_dir: Path, range_key: str, value: str) -> None:
    path = _cache_path(output_dir, range_key)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value, encoding="utf-8")


def _visual_cache_value(
    *,
    mode: str,
    background: Path,
    channel_name_image: Path | None,
    line1: str,
    line2: str,
    line3: str,
    font_file: str,
    render_width: int,
    episode_index: int,
    start: int,
    end: int,
    use_gpu: bool = False,
) -> str:
    parts = [
        f"mode={mode}",
        f"background={_file_signature(background)}",
        f"channel_name={_file_signature(channel_name_image) if channel_name_image is not None else ''}",
        f"line1={line1}",
        f"line2={line2}",
        f"line3={line3}",
        f"font_file={font_file}",
        f"render_width={render_width}",
        f"episode_index={episode_index}",
        f"start={start}",
        f"end={end}",
        f"use_gpu={use_gpu}",
    ]
    return _cache_value(*parts)


def _video_cache_value(
    *,
    visual_path: Path,
    audio_path: Path,
    duration: float,
) -> str:
    return _cache_value(
        f"visual={_file_signature(visual_path)}",
        f"audio={_file_signature(audio_path)}",
        f"duration={duration}",
    )


def _visual_encode_args(config: NovelConfig) -> list[str]:
    if config.media.video.use_gpu:
        _nvenc_preset_map = {
            "ultrafast": "p1", "superfast": "p1", "veryfast": "p2",
            "faster": "p3", "fast": "p4", "medium": "p5",
            "slow": "p6", "slower": "p7", "veryslow": "p7",
        }
        nvenc_preset = _nvenc_preset_map.get(config.media.video.preset, config.media.video.preset)
        return ["-c:v", "h264_nvenc", "-preset", nvenc_preset, "-cq", str(config.media.video.crf), "-b:v", "0"]
    return ["-c:v", config.media.video.video_codec, "-preset", config.media.video.preset, "-crf", str(config.media.video.crf)]


def generate_visual(config: NovelConfig, start: int, end: int, force: bool = False) -> tuple[Path, Path]:
    if not ffmpeg_has_filter("drawtext"):
        raise RuntimeError(
            "ffmpeg filter 'drawtext' is unavailable. Install an ffmpeg build with drawtext/libfreetype "
            "support, then verify with: ffmpeg -hide_banner -filters | rg drawtext"
        )
    range_key = _range_key(start, end)
    background = config.storage.image_dir / config.media.visual.background_video
    if not background.exists():
        raise FileNotFoundError(f"Background video not found: {background}")
    channel_name_image = config.storage.root / "image" / "channel-name.png"
    if not channel_name_image.exists():
        raise FileNotFoundError(f"Channel name image not found: {channel_name_image}")
    output_dir = config.storage.visual_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    output_video = output_dir / f"{range_key}.mp4"
    thumbnail = output_dir / f"{range_key}.png"
    batch_start, batch_end = get_media_batch_range(config, start)
    episode_index = count_media_batches_before(config, batch_start) + 1
    expected_cache = _visual_cache_value(
        mode="range",
        background=background,
        channel_name_image=channel_name_image,
        line1=config.media.visual.line1,
        line2=config.media.visual.line2,
        line3=config.media.visual.line3,
        font_file=config.media.visual.font_file,
        render_width=int(config.media.visual.render_width),
        episode_index=episode_index,
        start=start,
        end=end,
        use_gpu=config.media.video.use_gpu,
    )
    cached_value = _read_cache(output_dir, range_key)
    outputs_exist = (
        output_video.exists()
        and output_video.stat().st_size > 0
        and thumbnail.exists()
        and thumbnail.stat().st_size > 0
    )
    if (not force) and outputs_exist and cached_value == expected_cache:
        return output_video, thumbnail
    font_arg = f":fontfile={config.media.visual.font_file}" if config.media.visual.font_file else ""
    drawtext_filters = ",".join(
        [
            f"drawtext=text='{_esc_drawtext(f'Tập {episode_index}')}'{font_arg}:fontcolor=#FFD200:fontsize=48:borderw=4:bordercolor=black:x=10:y=35",
            f"drawtext=text='{_esc_drawtext(f'Chương {start} -> {end}')}'{font_arg}:fontcolor=white:fontsize=32:borderw=4:bordercolor=black:x=10:y=95",
            f"drawtext=text='{_esc_drawtext(config.media.visual.line1)}'{font_arg}:fontcolor=#FFD200:fontsize=40:borderw=6:bordercolor=black:x=(w-text_w)/2:y=h-200",
            f"drawtext=text='{_esc_drawtext(config.media.visual.line2)}'{font_arg}:fontcolor=#FFD200:fontsize=40:borderw=6:bordercolor=black:x=(w-text_w)/2:y=h-130",
            f"drawtext=text='{_esc_drawtext(config.media.visual.line3)}'{font_arg}:fontcolor=white:fontsize=30:borderw=6:bordercolor=black:x=(w-text_w)/2:y=h-60",
        ]
    )
    render_width = int(config.media.visual.render_width)
    filter_complex = (
        f"[0:v]scale={render_width}:-2[scaled];"
        f"[scaled]{drawtext_filters}[base];"
        f"[1:v]scale=-1:80[channel];"
        f"[base][channel]overlay=x=W-w-5:y=10[v]"
    )
    decode_args = []
    run_ffmpeg(
        [
            "-y",
            *decode_args,
            "-i",
            str(background),
            "-i",
            str(channel_name_image),
            "-filter_complex",
            filter_complex,
            "-map",
            "[v]",
            "-an",
            *_visual_encode_args(config),
            str(output_video),
        ]
    )
    run_ffmpeg(["-y", "-i", str(output_video), "-vframes", "1", "-update", "1", str(thumbnail)])
    _write_cache(output_dir, range_key, expected_cache)
    return output_video, thumbnail


def generate_visual_for_chapter(config: NovelConfig, chapter: int, force: bool = False) -> tuple[Path, Path]:
    if not ffmpeg_has_filter("drawtext"):
        raise RuntimeError(
            "ffmpeg filter 'drawtext' is unavailable. Install an ffmpeg build with drawtext/libfreetype "
            "support, then verify with: ffmpeg -hide_banner -filters | rg drawtext"
        )
    if chapter < 1:
        raise ValueError("chapter must be >= 1")
    background_cover = config.storage.image_dir / config.media.visual.background_cover
    if not config.media.visual.background_cover:
        raise ValueError('Missing visual.background_cover in novel config for --chapter flow')
    if background_cover.suffix.lower() not in {".jpg", ".jpeg", ".png"}:
        raise ValueError("visual.background_cover must be a .jpg, .jpeg, or .png file")
    if not background_cover.exists():
        raise FileNotFoundError(f"Background cover image not found: {background_cover}")

    range_key = _range_key(chapter, chapter)
    output_dir = config.storage.visual_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    output_video = output_dir / f"{range_key}.mp4"
    thumbnail = output_dir / f"{range_key}.png"
    expected_cache = _visual_cache_value(
        mode="chapter",
        background=background_cover,
        channel_name_image=None,
        line1=_line1_for_chapter(config.media.visual.line1, chapter),
        line2=config.media.visual.line2,
        line3=config.media.visual.line3,
        font_file=config.media.visual.font_file,
        render_width=int(config.media.visual.render_width),
        episode_index=count_media_batches_before(config, chapter) + 1,
        start=chapter,
        end=chapter,
        use_gpu=config.media.video.use_gpu,
    )
    cached_value = _read_cache(output_dir, range_key)
    outputs_exist = (
        output_video.exists()
        and output_video.stat().st_size > 0
        and thumbnail.exists()
        and thumbnail.stat().st_size > 0
    )
    if (not force) and outputs_exist and cached_value == expected_cache:
        return output_video, thumbnail
    font_arg = f":fontfile={config.media.visual.font_file}" if config.media.visual.font_file else ""
    line1_text = _line1_for_chapter(config.media.visual.line1, chapter)

    # Render all lines inside the cover image area.
    filters = ",".join(
        [
            f"scale={int(config.media.visual.render_width)}:-2",
            f"drawtext=text='{_esc_drawtext(line1_text)}'{font_arg}:fontcolor=#FFD200:fontsize=56:borderw=6:bordercolor=black:x=(w-text_w)/2 - 150:y=20",
            f"drawtext=text='{_esc_drawtext(config.media.visual.line2)}'{font_arg}:fontcolor=#FFD200:fontsize=42:borderw=6:bordercolor=black:x=(w-text_w)/2:y=h-180",
            f"drawtext=text='{_esc_drawtext(config.media.visual.line3)}'{font_arg}:fontcolor=white:fontsize=36:borderw=6:bordercolor=black:x=(w-text_w)/2:y=h-120",
        ]
    )

    run_ffmpeg(
        [
            "-y",
            "-loop",
            "1",
            "-i",
            str(background_cover),
            "-vf",
            filters,
            "-t",
            "1",
            "-r",
            "30",
            *_visual_encode_args(config),
            "-pix_fmt",
            "yuv420p",
            str(output_video),
        ]
    )
    run_ffmpeg(["-y", "-i", str(output_video), "-vframes", "1", "-update", "1", str(thumbnail)])
    _write_cache(output_dir, range_key, expected_cache)
    return output_video, thumbnail


def create_video(config: NovelConfig, start: int, end: int, force: bool = False) -> Path:
    range_key = _range_key(start, end)
    visual_path = config.storage.visual_dir / f"{range_key}.mp4"
    audio_path = config.storage.audio_dir / range_key / f"{range_key}.aac"
    if not visual_path.exists():
        raise FileNotFoundError(f"Visual asset not found: {visual_path}")
    if not audio_path.exists():
        raise FileNotFoundError(f"Audio file not found: {audio_path}")
    duration = ffprobe_duration(audio_path)
    config.storage.video_dir.mkdir(parents=True, exist_ok=True)
    output_path = config.storage.video_dir / f"{range_key}.mp4"
    expected_cache = _video_cache_value(
        visual_path=visual_path,
        audio_path=audio_path,
        duration=duration,
    )
    cached_value = _read_cache(config.storage.video_dir, range_key)
    if (not force) and output_path.exists() and output_path.stat().st_size > 0 and cached_value == expected_cache:
        return output_path
    # Visual and audio are already encoded — copy both streams directly.
    # -bsf:a aac_adtstoasc converts the ADTS AAC headers to MP4-compatible format (lossless).
    run_ffmpeg(
        [
            "-y",
            "-stream_loop",
            "-1",
            "-i",
            str(visual_path),
            "-i",
            str(audio_path),
            "-c:v",
            "copy",
            "-c:a",
            "copy",
            "-bsf:a",
            "aac_adtstoasc",
            "-t",
            str(duration),
            str(output_path),
        ]
    )
    _write_cache(config.storage.video_dir, range_key, expected_cache)
    return output_path
