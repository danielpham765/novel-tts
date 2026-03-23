from __future__ import annotations

import asyncio
import os
import socket
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, cast
from urllib.parse import urlparse

import yaml

from novel_tts.common.logging import get_logger
from novel_tts.config.models import NovelConfig

LOGGER = get_logger(__name__)


@dataclass(frozen=True)
class TtsModelConfig:
    backbone: str
    codec: str
    device: str
    use_lmdeploy: bool = False
    custom_model_id: str = ""
    base_model: str = ""
    hf_token: str = ""

    def as_gradio_payload(self) -> list[object]:
        return [
            self.backbone,
            self.codec,
            self.device,
            self.use_lmdeploy,
            self.custom_model_id,
            self.base_model,
            self.hf_token,
        ]


def _load_yaml_config(path: Path) -> Any:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _server_configs(root: Path) -> dict[str, str]:
    path = root / "configs" / "providers" / "tts_servers.yaml"
    return cast(dict[str, str], _load_yaml_config(path))


def _model_configs(root: Path) -> dict[str, Any]:
    path = root / "configs" / "providers" / "tts_models.yaml"
    return cast(dict[str, Any], _load_yaml_config(path))


def _network_debug_snapshot(server_url: str) -> str:
    parsed = urlparse(server_url)
    host = parsed.hostname or ""
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    bits: list[str] = [f"url={server_url}", f"host={host}", f"port={port}"]

    proxy_env = {
        key: value
        for key, value in os.environ.items()
        if key.lower() in {"http_proxy", "https_proxy", "all_proxy", "no_proxy"}
    }
    bits.append(f"proxy_env={proxy_env}")

    try:
        addrinfo = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
        normalized = sorted({str(item[4]) for item in addrinfo})
        bits.append(f"addrinfo={normalized}")
    except Exception as exc:
        bits.append(f"addrinfo_error={exc!r}")

    try:
        with socket.create_connection((host, port), timeout=3):
            bits.append("raw_tcp=ok")
    except Exception as exc:
        bits.append(f"raw_tcp_error={exc!r}")

    try:
        proc = subprocess.run(
            ["route", "-n", "get", host],
            check=False,
            capture_output=True,
            text=True,
            timeout=3,
        )
        route_output = (proc.stdout or proc.stderr or "").strip().replace("\n", " | ")
        if route_output:
            bits.append(f"route={route_output}")
        else:
            bits.append(f"route_rc={proc.returncode}")
    except Exception as exc:
        bits.append(f"route_error={exc!r}")

    return " ; ".join(bits)


def _require_text(value: Any, *, model_name: str, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f'Invalid TTS model config "{model_name}": missing "{field_name}"')
    return text


def _parse_model_config(model_name: str, raw: Any) -> TtsModelConfig:
    if not isinstance(raw, dict):
        raise ValueError(
            f'Invalid TTS model config "{model_name}": expected mapping in configs/providers/tts_models.yaml'
        )
    return TtsModelConfig(
        backbone=_require_text(raw.get("backbone"), model_name=model_name, field_name="backbone"),
        codec=_require_text(raw.get("codec"), model_name=model_name, field_name="codec"),
        device=_require_text(raw.get("device"), model_name=model_name, field_name="device"),
        use_lmdeploy=bool(raw.get("use_lmdeploy", False)),
        custom_model_id=str(raw.get("custom_model_id") or "").strip(),
        base_model=_require_text(raw.get("base_model"), model_name=model_name, field_name="base_model"),
        hf_token=str(raw.get("hf_token") or "").strip(),
    )


class GradioTtsProvider:
    def __init__(self, config: NovelConfig) -> None:
        self.config = config
        servers = _server_configs(config.storage.root)
        models = _model_configs(config.storage.root)
        self.server_url = servers[config.tts.server_name]
        raw_model = models[config.tts.model_name]
        self.model_config = _parse_model_config(config.tts.model_name, raw_model)

    def _normalized_model_payload(self) -> list[object]:
        return self.model_config.as_gradio_payload()

    def connect(self):
        from gradio_client import Client
        import httpcore
        import httpx

        attempts = 4
        delay_seconds = 1.0
        last_exc: Exception | None = None
        for attempt in range(1, attempts + 1):
            try:
                return Client(self.server_url, verbose=False)
            except (httpx.ConnectError, httpcore.ConnectError, OSError) as exc:
                last_exc = exc
                debug_snapshot = _network_debug_snapshot(self.server_url)
                if attempt >= attempts:
                    LOGGER.error(
                        "TTS connect diagnostics | server=%s attempt=%s/%s error=%s | %s",
                        self.server_url,
                        attempt,
                        attempts,
                        exc,
                        debug_snapshot,
                    )
                    break
                LOGGER.warning(
                    "TTS connect failed | server=%s attempt=%s/%s error=%s; retrying in %.1fs | %s",
                    self.server_url,
                    attempt,
                    attempts,
                    exc,
                    delay_seconds,
                    debug_snapshot,
                )
                time.sleep(delay_seconds)
                delay_seconds *= 2.0
        assert last_exc is not None
        raise last_exc

    def load_model(self, client) -> None:
        client.predict(*self._normalized_model_payload(), api_name="/load_model")

    def cleanup_output_audio(self, client, source_path: str) -> None:
        try:
            client.predict(source_path, api_name="/delete_output_audio")
        except Exception as exc:
            LOGGER.warning(
                "TTS cleanup API unavailable or failed | server=%s path=%s error=%s",
                self.server_url,
                source_path,
                exc,
            )

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
            if (now - last_heartbeat) >= 60:
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
