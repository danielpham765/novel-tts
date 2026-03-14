from __future__ import annotations

import shutil
import time
from pathlib import Path

from novel_tts.common.ffmpeg import ffprobe_duration, run_ffmpeg
from novel_tts.common.logging import get_logger
from novel_tts.config.models import NovelConfig

from .providers import get_tts_provider

LOGGER = get_logger(__name__)


def split_text_into_chunks(text: str) -> tuple[list[str], list[dict[str, object]]]:
    chunks: list[str] = []
    chapter_info: list[dict[str, object]] = []
    matches = list(__import__("re").finditer(r"Chương\s+\d+", text))
    if not matches:
        return [text], [{"number": 1, "title": ""}]
    for idx, match in enumerate(matches):
        start = match.start()
        end = matches[idx + 1].start() if idx + 1 < len(matches) else len(text)
        chunk = text[start:end].strip()
        if not chunk:
            continue
        chunks.append(chunk)
        first_line = chunk.splitlines()[0]
        title_match = __import__("re").match(r"Chương\s+(\d+)[:\-\s]+(.+)", first_line)
        if title_match:
            chapter_info.append({"number": int(title_match.group(1)), "title": title_match.group(2).strip()})
        else:
            num_match = __import__("re").match(r"Chương\s+(\d+)", first_line)
            chapter_info.append({"number": int(num_match.group(1)) if num_match else idx + 1, "title": ""})
    return chunks, chapter_info


def _range_key(start: int, end: int) -> str:
    return f"chuong_{start}-{end}"


def _translated_text_path(config: NovelConfig, start: int, end: int) -> Path:
    key = _range_key(start, end)
    direct = config.storage.translated_dir / f"{key}.txt"
    if direct.exists():
        return direct
    raise FileNotFoundError(f"Translated range file not found: {direct}")


def _generate_menu(config: NovelConfig, files: list[Path], chapter_info: list[dict[str, object]], range_key: str) -> Path:
    subtitle_dir = config.storage.subtitle_dir
    subtitle_dir.mkdir(parents=True, exist_ok=True)
    menu_path = subtitle_dir / f"{range_key}_menu.txt"
    lines: list[str] = []
    current_time = 0.0
    for idx, file in enumerate(files):
        duration = ffprobe_duration(file) / config.tts.tempo
        hours = int(current_time // 3600)
        minutes = int((current_time % 3600) // 60)
        seconds = int(current_time % 60)
        timestamp = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
        chapter = chapter_info[idx] if idx < len(chapter_info) else {"number": idx + 1, "title": ""}
        label = f"Chương {chapter['number']}"
        if chapter.get("title"):
            label += f" - {chapter['title']}"
        lines.append(f"{timestamp} - {label}")
        current_time += duration
    menu_path.write_text("\n".join(lines), encoding="utf-8")
    return menu_path


def run_tts(config: NovelConfig, start: int, end: int) -> Path:
    source_path = _translated_text_path(config, start, end)
    text = source_path.read_text(encoding="utf-8")
    chunks, chapter_info = split_text_into_chunks(text)
    range_key = _range_key(start, end)
    output_dir = config.storage.audio_dir / range_key
    output_dir.mkdir(parents=True, exist_ok=True)
    LOGGER.info(
        "TTS start | range=%s chapters=%s source=%s server=%s model=%s voice=%s",
        range_key,
        len(chunks),
        source_path,
        config.tts.server_name,
        config.tts.model_name,
        config.tts.voice,
    )
    provider = get_tts_provider(config)
    client = provider.connect()
    LOGGER.info("TTS load model | server=%s model=%s", config.tts.server_name, config.tts.model_name)
    provider.load_model(client)
    LOGGER.info("TTS model ready")
    audio_files: list[Path] = []
    for idx, chunk in enumerate(chunks):
        output_path = output_dir / f"{idx}.wav"
        chapter = chapter_info[idx] if idx < len(chapter_info) else {"number": idx + 1, "title": ""}
        chapter_label = f"chapter={chapter.get('number', idx + 1)}"
        if chapter.get("title"):
            chapter_label += f" title={chapter['title']}"
        if output_path.exists() and output_path.stat().st_size > 0:
            LOGGER.info(
                "TTS chapter cached | %s index=%s/%s path=%s size_bytes=%s",
                chapter_label,
                idx + 1,
                len(chunks),
                output_path,
                output_path.stat().st_size,
            )
            audio_files.append(output_path)
            continue
        LOGGER.info(
            "TTS chapter start | %s index=%s/%s chars=%s output=%s",
            chapter_label,
            idx + 1,
            len(chunks),
            len(chunk),
            output_path,
        )
        chapter_started_at = time.monotonic()
        result = provider.synthesize(
            client,
            chunk,
            progress_callback=lambda message, chapter_label=chapter_label, idx=idx, total=len(chunks): LOGGER.info(
                "TTS chapter progress | %s index=%s/%s %s",
                chapter_label,
                idx + 1,
                total,
                message,
            ),
        )
        if isinstance(result, dict) and "path" in result:
            source_audio = Path(result["path"])
        else:
            source_audio = Path(str(result))
        shutil.copyfile(source_audio, output_path)
        elapsed = time.monotonic() - chapter_started_at
        LOGGER.info(
            "TTS chapter done | %s index=%s/%s elapsed=%.1fs path=%s size_bytes=%s",
            chapter_label,
            idx + 1,
            len(chunks),
            elapsed,
            output_path,
            output_path.stat().st_size,
        )
        audio_files.append(output_path)

    file_list_path = output_dir / "file-list.txt"
    file_list_path.write_text("\n".join(f"file '{path.resolve()}'" for path in audio_files), encoding="utf-8")
    merged_path = output_dir / f"{range_key}.mp3"
    LOGGER.info("TTS merge start | inputs=%s file_list=%s output=%s", len(audio_files), file_list_path, merged_path)
    run_ffmpeg(
        [
            "-y",
            "-f",
            "concat",
            "-safe",
            "0",
            "-i",
            str(file_list_path),
            "-filter:a",
            f"atempo={config.tts.tempo}",
            "-acodec",
            "libmp3lame",
            "-b:a",
            config.tts.bitrate,
            str(merged_path),
        ]
    )
    menu_path = _generate_menu(config, audio_files, chapter_info, range_key)
    LOGGER.info("TTS merge done | output=%s size_bytes=%s", merged_path, merged_path.stat().st_size)
    LOGGER.info("TTS menu generated | path=%s", menu_path)
    return merged_path
