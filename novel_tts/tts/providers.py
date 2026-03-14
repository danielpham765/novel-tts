from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from typing import Callable

from novel_tts.common.logging import get_logger
from novel_tts.config.models import NovelConfig

LOGGER = get_logger(__name__)


def _server_configs(root: Path) -> dict[str, dict[str, str]]:
    path = root / "configs" / "providers" / "tts_servers.json"
    return json.loads(path.read_text(encoding="utf-8"))


def _model_configs(root: Path) -> dict[str, list[object]]:
    path = root / "configs" / "providers" / "tts_models.json"
    return json.loads(path.read_text(encoding="utf-8"))


class GradioTtsProvider:
    def __init__(self, config: NovelConfig) -> None:
        self.config = config
        servers = _server_configs(config.storage.root)
        models = _model_configs(config.storage.root)
        self.server_url = servers[config.tts.server_name]
        self.model_payload = models[config.tts.model_name]

    def _normalized_model_payload(self) -> list[object]:
        # Support legacy short config values and map them to the current
        # Gradio endpoint shape: backbone, codec, device, lmdeploy, custom id,
        # base model, hf token.
        if len(self.model_payload) == 7:
            return list(self.model_payload)

        if len(self.model_payload) == 4:
            _language, device_hint, _use_default_voice, _unused = self.model_payload
            if str(device_hint).lower() == "macos":
                return [
                    "VieNeu-TTS-0.3B-q8-gguf",
                    "NeuCodec (Distill)",
                    "MPS",
                    False,
                    "",
                    "VieNeu-TTS-0.3B (GPU)",
                    "",
                ]
            if str(device_hint).lower() == "cpu":
                return [
                    "VieNeu-TTS-0.3B-q4-gguf",
                    "NeuCodec ONNX (Fast CPU)",
                    "CPU",
                    False,
                    "",
                    "VieNeu-TTS-0.3B (GPU)",
                    "",
                ]

        raise ValueError(f"Unsupported TTS model payload: {self.model_payload!r}")

    def connect(self):
        from gradio_client import Client

        return Client(self.server_url, verbose=False)

    def load_model(self, client) -> None:
        client.predict(*self._normalized_model_payload(), api_name="/load_model")

    def _drain_job_updates(self, job, progress_callback: Callable[[str], None] | None) -> None:
        communicator = getattr(job, "communicator", None)
        updates = getattr(communicator, "updates", None)
        if updates is None:
            return
        while True:
            try:
                update = updates.get_nowait()
            except asyncio.QueueEmpty:
                return
            if getattr(update, "type", None) == "output":
                continue
            code = getattr(getattr(update, "code", None), "value", None) or str(getattr(update, "code", "unknown"))
            details: list[str] = []
            rank = getattr(update, "rank", None)
            queue_size = getattr(update, "queue_size", None)
            eta = getattr(update, "eta", None)
            progress_data = getattr(update, "progress_data", None) or []
            log_entry = getattr(update, "log", None)
            if (rank is not None) and (queue_size is not None):
                details.append(f"queue={rank + 1}/{queue_size}")
            elif queue_size is not None:
                details.append(f"queue_size={queue_size}")
            if eta is not None:
                details.append(f"eta={eta:.1f}s")
            for progress in progress_data:
                progress_bits: list[str] = []
                desc = getattr(progress, "desc", None)
                if desc:
                    progress_bits.append(str(desc))
                progress_value = getattr(progress, "progress", None)
                if progress_value is not None:
                    progress_bits.append(f"{progress_value * 100:.0f}%")
                index = getattr(progress, "index", None)
                length = getattr(progress, "length", None)
                unit = getattr(progress, "unit", None)
                if (index is not None) and (length is not None):
                    if unit:
                        progress_bits.append(f"{index}/{length} {unit}")
                    else:
                        progress_bits.append(f"{index}/{length}")
                if progress_bits:
                    details.append(" | ".join(progress_bits))
            if log_entry:
                details.append(f"log[{log_entry[0]}]={log_entry[1]}")
            message = code if not details else f"{code} | {' | '.join(details)}"
            if progress_callback is not None:
                progress_callback(message)
            else:
                LOGGER.info("TTS job update | %s", message)

    def synthesize(self, client, text: str, progress_callback: Callable[[str], None] | None = None) -> str:
        job = client.submit(
            text,
            self.config.tts.voice,
            None,
            "",
            self.config.tts.generation_mode,
            self.config.tts.use_batch,
            self.config.tts.max_batch_size_run,
            self.config.tts.temperature,
            self.config.tts.max_chars_chunk,
            api_name="/synthesize_speech",
        )
        last_heartbeat = time.monotonic()
        while not job.done():
            self._drain_job_updates(job, progress_callback)
            now = time.monotonic()
            if (now - last_heartbeat) >= 30:
                status = job.status()
                code = getattr(getattr(status, "code", None), "value", None) or str(getattr(status, "code", "unknown"))
                if progress_callback is not None:
                    progress_callback(f"WAITING | latest_status={code}")
                else:
                    LOGGER.info("TTS job waiting | latest_status=%s", code)
                last_heartbeat = now
            time.sleep(1)
        result = job.result()
        self._drain_job_updates(job, progress_callback)
        if not result or not result[0]:
            raise RuntimeError(f"Invalid TTS result: {result}")
        return result[0]


def get_tts_provider(config: NovelConfig) -> GradioTtsProvider:
    if config.tts.provider != "gradio_vie_tts":
        raise ValueError(f"Unsupported TTS provider: {config.tts.provider}")
    return GradioTtsProvider(config)
