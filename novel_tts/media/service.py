from __future__ import annotations

import shutil
from pathlib import Path

from novel_tts.common.ffmpeg import ffprobe_duration, run_ffmpeg
from novel_tts.config.models import NovelConfig


def _range_key(start: int, end: int) -> str:
    return f"chuong_{start}-{end}"


def _esc_drawtext(text: str) -> str:
    return str(text or "").replace("\\", "\\\\").replace(":", "\\:").replace("'", "\\'")


def generate_visual(config: NovelConfig, start: int, end: int) -> tuple[Path, Path]:
    range_key = _range_key(start, end)
    background = config.storage.image_dir / config.visual.background_video
    if not background.exists():
        raise FileNotFoundError(f"Background video not found: {background}")
    output_dir = config.storage.visual_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    output_video = output_dir / f"{range_key}.mp4"
    thumbnail = output_dir / f"{range_key}.png"
    font_arg = f":fontfile={config.visual.font_file}" if config.visual.font_file else ""
    part_index = ((start - 1) // 20) + 1
    filters = ",".join(
        [
            f"drawtext=text='{_esc_drawtext(f'Phần {part_index}')}'{font_arg}:fontcolor=#FFD200:fontsize=48:borderw=4:bordercolor=black:x=10:y=35",
            f"drawtext=text='{_esc_drawtext(f'Chương {start} -> {end}')}'{font_arg}:fontcolor=white:fontsize=32:borderw=4:bordercolor=black:x=10:y=95",
            f"drawtext=text='{_esc_drawtext(config.visual.tag_text)}'{font_arg}:fontcolor=#FFD200:fontsize=36:borderw=4:bordercolor=black:x=w-text_w-20:y=35",
            f"drawtext=text='{_esc_drawtext(config.visual.line1)}'{font_arg}:fontcolor=#FFD200:fontsize=60:borderw=6:bordercolor=black:x=(w-text_w)/2:y=h-350",
            f"drawtext=text='{_esc_drawtext(config.visual.line2)}'{font_arg}:fontcolor=#FFD200:fontsize=60:borderw=6:bordercolor=black:x=(w-text_w)/2:y=h-250",
            f"drawtext=text='{_esc_drawtext(config.visual.line3)}'{font_arg}:fontcolor=white:fontsize=50:borderw=6:bordercolor=black:x=(w-text_w)/2:y=h-120",
        ]
    )
    run_ffmpeg(["-y", "-i", str(background), "-vf", filters, "-c:a", "copy", str(output_video)])
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
