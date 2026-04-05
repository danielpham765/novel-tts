from __future__ import annotations

import asyncio
import os
import socket
import subprocess
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, cast
from urllib.parse import quote, urljoin, urlparse

import httpx
import yaml

from novel_tts.common.logging import get_logger
from novel_tts.config.models import NovelConfig

LOGGER = get_logger(__name__)

_STANDARD_GENERATION_MODE = "Standard (Một lần)"
_STREAMING_GENERATION_MODE = "Streaming (Real-time)"


class TtsModelNotReadyError(RuntimeError):
    """Raised when the TTS server says the model has not been loaded yet."""


class TtsModelLoadError(RuntimeError):
    """Raised when the TTS server fails to load the requested model."""


@dataclass(frozen=True)
class TtsAudioResult:
    local_path: Path
    cleanup_target: str | None = None


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

    @staticmethod
    def _client_httpx_kwargs() -> dict[str, object]:
        # Keep Gradio bootstrap traffic independent from ambient proxy env so
        # local/LAN TTS servers behave the same across shells and uv re-execs.
        return {"trust_env": False}

    @staticmethod
    def _normalize_generation_mode(value: object) -> str:
        text = str(value or "").strip()
        normalized = text.casefold()
        if not normalized:
            return _STANDARD_GENERATION_MODE
        if normalized in {
            _STANDARD_GENERATION_MODE.casefold(),
            "standard",
            "standard_once",
            "preset_mode",
        }:
            return _STANDARD_GENERATION_MODE
        if normalized in {
            _STREAMING_GENERATION_MODE.casefold(),
            "streaming",
            "real-time",
            "realtime",
        }:
            return _STREAMING_GENERATION_MODE
        return text

    def connect(self):
        from gradio_client import Client
        import httpcore

        attempts = 4
        delay_seconds = 1.0
        last_exc: Exception | None = None
        for attempt in range(1, attempts + 1):
            try:
                return Client(
                    self.server_url,
                    verbose=False,
                    download_files=False,
                    httpx_kwargs=self._client_httpx_kwargs(),
                )
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
        result = client.predict(*self._normalized_model_payload(), api_name="/load_model")
        status_message = self._extract_load_model_status(result)
        if status_message:
            normalized = status_message.lower()
            if ("✅ model đã tải thành công" in normalized) or ("backend:" in normalized and "codec:" in normalized):
                LOGGER.info(
                    "TTS load model response | server=%s model=%s status=%s",
                    self.config.tts.server_name,
                    self.config.tts.model_name,
                    status_message.replace("\n", " | "),
                )
                return
            if ("❌" in status_message) or ("lỗi" in normalized):
                raise TtsModelLoadError(status_message)
        LOGGER.warning(
            "TTS load model returned unexpected response | server=%s model=%s result=%r",
            self.config.tts.server_name,
            self.config.tts.model_name,
            result,
        )

    @staticmethod
    def _extract_load_model_status(result: Any) -> str | None:
        if isinstance(result, (list, tuple)) and result:
            text = str(result[0] or "").strip()
            return text or None
        if isinstance(result, str):
            text = result.strip()
            return text or None
        return None

    def _load_model_with_retry(self, client, *, reason: str | None = None) -> None:
        if reason:
            LOGGER.info(
                "TTS load model | server=%s model=%s reason=%s",
                self.config.tts.server_name,
                self.config.tts.model_name,
                reason,
            )
        else:
            LOGGER.info("TTS load model | server=%s model=%s", self.config.tts.server_name, self.config.tts.model_name)
        self.load_model(client)

    @staticmethod
    def _extract_audio_reference(result: Any) -> dict[str, Any] | str:
        if isinstance(result, dict):
            path = result.get("path")
            if path:
                return cast(dict[str, Any], result)
        elif isinstance(result, (list, tuple)) and result:
            path = result[0]
            if path:
                if isinstance(path, dict):
                    return cast(dict[str, Any], path)
                return str(path)
        elif isinstance(result, str) and result.strip():
            return result
        raise RuntimeError(f"Invalid TTS result: {result}")

    @staticmethod
    def _resolve_output_audio_url(client, audio_ref: dict[str, Any] | str) -> str | None:
        if isinstance(audio_ref, dict):
            url = str(audio_ref.get("url") or "").strip()
            if url:
                if url.startswith(("http://", "https://")):
                    return url
                return urljoin(client.src_prefixed, url.lstrip("/"))
            path = str(audio_ref.get("path") or "").strip()
        else:
            path = str(audio_ref).strip()
        if not path or Path(path).exists():
            return None
        return urljoin(client.src_prefixed, f"file={quote(path)}")

    def materialize_output_audio(self, client, audio_ref: dict[str, Any] | str) -> TtsAudioResult:
        if isinstance(audio_ref, dict):
            path = str(audio_ref.get("path") or "").strip()
            cleanup_target = self._build_cleanup_target(path)
            orig_name = str(audio_ref.get("orig_name") or "").strip()
        else:
            path = str(audio_ref).strip()
            cleanup_target = self._build_cleanup_target(path)
            orig_name = ""

        source_path = Path(path) if path else None
        if source_path is not None and source_path.exists():
            return TtsAudioResult(local_path=source_path, cleanup_target=cleanup_target)

        download_url = self._resolve_output_audio_url(client, audio_ref)
        if not download_url:
            raise RuntimeError(f"Unable to resolve output audio download URL from TTS result: {audio_ref!r}")

        suffix = Path(orig_name or path or "audio.wav").suffix or ".wav"
        temp_dir = self.config.storage.tmp_dir / "tts_downloads"
        temp_dir.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(dir=temp_dir, prefix="tts_", suffix=suffix, delete=False) as handle:
            temp_path = Path(handle.name)
            with httpx.stream(
                "GET",
                download_url,
                headers=client.headers,
                cookies=client.cookies,
                verify=client.ssl_verify,
                follow_redirects=True,
                **client.httpx_kwargs,
            ) as response:
                response.raise_for_status()
                for chunk in response.iter_bytes():
                    handle.write(chunk)
        return TtsAudioResult(local_path=temp_path, cleanup_target=cleanup_target)

    @staticmethod
    def _build_cleanup_target(raw_path: str) -> str | None:
        path = str(raw_path or "").strip()
        if not path:
            return None
        if "file=" in path:
            path = path.split("file=", 1)[1].strip()

        normalized = path.replace("\\", "/").strip("/")
        if not normalized:
            return None

        parts = [part for part in normalized.split("/") if part and part not in {".", ".."}]
        if not parts:
            return None

        filename = parts[-1]
        if not (filename.startswith("tts_output_") or filename.startswith("tts_stream_")):
            return None

        if len(parts) >= 3 and parts[-3] == "gradio":
            return f"{parts[-2]}/{filename}"
        if len(parts) >= 2 and parts[-2] == "output_audio":
            return filename
        if len(parts) >= 2:
            return f"{parts[-2]}/{filename}"
        return filename

    @staticmethod
    def _extract_model_not_ready_message(result: Any) -> str | None:
        values: list[Any]
        if isinstance(result, dict):
            values = list(result.values())
        elif isinstance(result, (list, tuple)):
            values = list(result)
        else:
            values = [result]
        for value in values:
            text = str(value or "").strip()
            normalized = text.lower()
            if ("tải model trước" in normalized) or ("load model" in normalized and "first" in normalized):
                return text
        return None

    def cleanup_output_audio(self, client, cleanup_target: str | None) -> None:
        if not cleanup_target:
            return
        try:
            client.predict(cleanup_target, api_name="/delete_output_audio")
        except Exception as exc:
            LOGGER.warning(
                "TTS cleanup API unavailable or failed | server=%s cleanup_target=%s error=%s",
                self.server_url,
                cleanup_target,
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

    def synthesize(
        self, client, text: str, progress_callback: Callable[[str], None] | None = None
    ) -> dict[str, Any] | str:
        max_attempts = 3
        retry_delay_seconds = 2.0
        generation_mode = self._normalize_generation_mode(self.config.tts.generation_mode)
        for attempt in range(1, max_attempts + 1):
            job = client.submit(
                text,
                self.config.tts.voice,
                None,
                "",
                generation_mode,
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
                    code = getattr(getattr(status, "code", None), "value", None) or str(
                        getattr(status, "code", "unknown")
                    )
                    if progress_callback is not None:
                        progress_callback(f"WAITING | latest_status={code}")
                    else:
                        LOGGER.info("TTS job waiting | latest_status=%s", code)
                    last_heartbeat = now
                time.sleep(1)
            result = job.result()
            self._drain_job_updates(job, progress_callback)
            try:
                return self._extract_audio_reference(result)
            except RuntimeError:
                model_not_ready_message = self._extract_model_not_ready_message(result)
                if model_not_ready_message is None:
                    raise
                if attempt >= max_attempts:
                    raise TtsModelNotReadyError(
                        f"TTS server still reported model not ready after {max_attempts} attempts: "
                        f"{model_not_ready_message}"
                    )
                LOGGER.warning(
                    "TTS synthesize requested before model became ready | server=%s model=%s attempt=%s/%s message=%s",
                    self.config.tts.server_name,
                    self.config.tts.model_name,
                    attempt,
                    max_attempts,
                    model_not_ready_message,
                )
                self._load_model_with_retry(client, reason="server-reported-model-not-ready")
                time.sleep(retry_delay_seconds)
        raise TtsModelNotReadyError(
            f"TTS server never became ready after {max_attempts} attempts for model {self.config.tts.model_name}"
        )


def get_tts_provider(config: NovelConfig) -> GradioTtsProvider:
    if config.tts.provider != "gradio_vie_tts":
        raise ValueError(f"Unsupported TTS provider: {config.tts.provider}")
    return GradioTtsProvider(config)
