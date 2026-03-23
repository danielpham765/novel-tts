from __future__ import annotations

import re
from pathlib import Path

from novel_tts.common.ffmpeg import ffmpeg_has_filter, ffprobe_duration, run_ffmpeg
from novel_tts.config.models import NovelConfig


def _range_key(start: int, end: int) -> str:
    return f"chuong_{start}-{end}"


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


def generate_visual(config: NovelConfig, start: int, end: int) -> tuple[Path, Path]:
    if not ffmpeg_has_filter("drawtext"):
        raise RuntimeError(
            "ffmpeg filter 'drawtext' is unavailable. Install an ffmpeg build with drawtext/libfreetype "
            "support, then verify with: ffmpeg -hide_banner -filters | rg drawtext"
        )
    range_key = _range_key(start, end)
    background = config.storage.image_dir / config.visual.background_video
    if not background.exists():
        raise FileNotFoundError(f"Background video not found: {background}")
    output_dir = config.storage.visual_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    output_video = output_dir / f"{range_key}.mp4"
    thumbnail = output_dir / f"{range_key}.png"
    font_arg = f":fontfile={config.visual.font_file}" if config.visual.font_file else ""
    episode_batch_size = max(1, int(getattr(config.video, "episode_batch_size", 10) or 10))
    part_index = ((start - 1) // episode_batch_size) + 1
    filters = ",".join(
        [
            f"drawtext=text='{_esc_drawtext(f'Tập {part_index}')}'{font_arg}:fontcolor=#FFD200:fontsize=48:borderw=4:bordercolor=black:x=10:y=35",
            f"drawtext=text='{_esc_drawtext(f'Chương {start} -> {end}')}'{font_arg}:fontcolor=white:fontsize=32:borderw=4:bordercolor=black:x=10:y=95",
            f"drawtext=text='{_esc_drawtext(config.visual.tag_text)}'{font_arg}:fontcolor=#FFD200:fontsize=36:borderw=4:bordercolor=black:x=w-text_w-20:y=35",
            f"drawtext=text='{_esc_drawtext(config.visual.line1)}'{font_arg}:fontcolor=#FFD200:fontsize=40:borderw=6:bordercolor=black:x=(w-text_w)/2:y=h-200",
            f"drawtext=text='{_esc_drawtext(config.visual.line2)}'{font_arg}:fontcolor=#FFD200:fontsize=40:borderw=6:bordercolor=black:x=(w-text_w)/2:y=h-130",
            f"drawtext=text='{_esc_drawtext(config.visual.line3)}'{font_arg}:fontcolor=white:fontsize=30:borderw=6:bordercolor=black:x=(w-text_w)/2:y=h-60",
        ]
    )
    run_ffmpeg(["-y", "-i", str(background), "-vf", filters, "-c:a", "copy", str(output_video)])
    run_ffmpeg(["-y", "-i", str(output_video), "-vframes", "1", str(thumbnail)])
    return output_video, thumbnail


def generate_visual_for_chapter(config: NovelConfig, chapter: int) -> tuple[Path, Path]:
    if not ffmpeg_has_filter("drawtext"):
        raise RuntimeError(
            "ffmpeg filter 'drawtext' is unavailable. Install an ffmpeg build with drawtext/libfreetype "
            "support, then verify with: ffmpeg -hide_banner -filters | rg drawtext"
        )
    if chapter < 1:
        raise ValueError("chapter must be >= 1")
    background_cover = config.storage.image_dir / config.visual.background_cover
    if not config.visual.background_cover:
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
    font_arg = f":fontfile={config.visual.font_file}" if config.visual.font_file else ""
    line1_text = _line1_for_chapter(config.visual.line1, chapter)

    # Render all lines inside the cover image area.
    filters = ",".join(
        [
            f"scale={int(config.visual.render_width)}:-2",
            f"drawtext=text='{_esc_drawtext(line1_text)}'{font_arg}:fontcolor=#FFD200:fontsize=56:borderw=6:bordercolor=black:x=(w-text_w)/2 - 150:y=20",
            f"drawtext=text='{_esc_drawtext(config.visual.line2)}'{font_arg}:fontcolor=#FFD200:fontsize=42:borderw=6:bordercolor=black:x=(w-text_w)/2:y=h-180",
            f"drawtext=text='{_esc_drawtext(config.visual.line3)}'{font_arg}:fontcolor=white:fontsize=36:borderw=6:bordercolor=black:x=(w-text_w)/2:y=h-120",
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
            "-c:v",
            "libx264",
            "-pix_fmt",
            "yuv420p",
            str(output_video),
        ]
    )
    run_ffmpeg(["-y", "-i", str(output_video), "-vframes", "1", str(thumbnail)])
    return output_video, thumbnail


def create_video(config: NovelConfig, start: int, end: int) -> Path:
    range_key = _range_key(start, end)
    visual_path = config.storage.visual_dir / f"{range_key}.mp4"
    audio_path = config.storage.audio_dir / range_key / f"{range_key}.mp3"
    if not visual_path.exists():
        raise FileNotFoundError(f"Visual asset not found: {visual_path}")
    if not audio_path.exists():
        raise FileNotFoundError(f"Audio file not found: {audio_path}")
    duration = ffprobe_duration(audio_path)
    config.storage.video_dir.mkdir(parents=True, exist_ok=True)
    output_path = config.storage.video_dir / f"{range_key}.mp4"
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
            config.video.video_codec,
            "-c:a",
            config.video.audio_codec,
            "-preset",
            config.video.preset,
            "-crf",
            str(config.video.crf),
            "-b:a",
            config.video.audio_bitrate,
            "-t",
            str(duration),
            str(output_path),
        ]
    )
    return output_path
