# novel-tts

File-first Python CLI pipeline for crawling serialized web novels, translating them to Vietnamese, generating TTS audio, and rendering publishable video assets.

- Architecture: `docs/ARCHITECTURE.md`
- Agent notes (internal): `docs/agents/codex/AGENTS.md`

## Table of contents

- [Quick start](#quick-start)
- [Prerequisites](#prerequisites)
- [Configuration](#configuration)
- [Storage layout (important invariants)](#storage-layout-important-invariants)
- [Recommended workflow (queue translation)](#recommended-workflow-queue-translation)
- [Command reference](#command-reference)
- [Troubleshooting](#troubleshooting)

## Quick start

```bash
uv sync
uv run novel-tts --help
```

Tip: all commands support `--log-file /path/to/file.log` at the top level.

## Prerequisites

- Python: `3.10`
- Package manager: `uv`
- For crawl (when needed): Playwright + Chromium
  - `uv run playwright install chromium`
- For queue translation: Redis (defaults in `configs/app.yaml`: `127.0.0.1:6379`, db `1`, prefix `novel_tts`)
- For media rendering: `ffmpeg` + `ffprobe`
- For the current TTS path: a reachable Gradio TTS server (configured in your app/novel config)

## Configuration

### Config files

- Novel configs: `configs/novels/*.json`
- Source configs: `configs/sources/*.json`
- Per-novel glossaries: `configs/glossaries/*.json`
- App defaults (models/queue, etc.): `configs/app.yaml`

### Secrets / API keys

Queue workers read Gemini API keys from `.secrets/gemini-keys.txt` (one key per line).
For one-off direct translation runs, `GEMINI_API_KEY` from env still works. If it is unset, direct translate
commands fall back to the first non-empty key in `.secrets/gemini-keys.txt`.

If you use OpenAI as a provider, set `OPENAI_API_KEY`.

## Storage layout (important invariants)

This repo is intentionally file-first: stages communicate via the filesystem under `input/<novel_id>/` and `output/<novel_id>/`.

**Translation truth is on disk:**

- Canonical per-chapter state: `input/<novel_id>/.parts/<batch>/*.txt`
- Derived (rebuildable) merged outputs: `input/<novel_id>/translated/*.txt`

**Crawl outputs:**

- Crawled source batches: `input/<novel_id>/origin/*.txt`
- Resumable crawl/translation state: `input/<novel_id>/.progress/*`

**Media outputs:**

- Audio: `output/<novel_id>/audio/<range>/*`
- Visual layer: `output/<novel_id>/visual/*`
- Final MP4: `output/<novel_id>/video/*`

**Heading convention (do not casually change):**

- Crawl/origin headings are typically ASCII: `Chuong <n> ...`
- Translated/TTS headings are typically Vietnamese: `Chương <n> ...`

Changing headings affects chapter splitting, translation rebuild, and downstream TTS/media assets.

## Recommended workflow (queue translation)

Translation policy: use the Redis-backed queue for translation for all novels.
Avoid running direct `translate novel` except for debugging/small one-off experiments.

```bash
# 1) Crawl
uv run novel-tts crawl run <novel_id> --range <start>-<end>

# 2) Verify crawled files (does not recrawl)
uv run novel-tts crawl verify <novel_id> --range <start>-<end>

# 3) Start the global quota supervisor (required for waiting-quota to make progress)
uv run novel-tts quota-supervisor
# or: uv run novel-tts quota-supervisor -d

# 4) Launch queue stack for a novel (supervisor + workers)
uv run novel-tts queue launch <novel_id>

# 5) Enqueue chapters for translation (repeat as needed)
uv run novel-tts queue add <novel_id> --range <start>-<end>
uv run novel-tts queue add <novel_id> --all

# 6) Monitor progress / inspect process state
uv run novel-tts queue monitor <novel_id>
uv run novel-tts queue ps <novel_id>

# 7) TTS + media
uv run novel-tts tts <novel_id> --range <start>-<end>
uv run novel-tts visual <novel_id> --range <start>-<end>
uv run novel-tts visual <novel_id> --chapter <chapter>
uv run novel-tts video <novel_id> --range <start>-<end>

# 8) Upload
uv run novel-tts upload <novel_id> --platform youtube --range <start>-<end>

# Optional: end-to-end pipeline
uv run novel-tts pipeline run <novel_id> --range <start>-<end>

# Process downstream media stage-by-stage across the whole range (default)
uv run novel-tts pipeline run <novel_id> --range 1-2000 --mode per-stage

# Process each translated video batch end-to-end: tts -> visual -> video -> upload
uv run novel-tts pipeline run <novel_id> --range 1-2000 --mode per-video
```

For YouTube uploads, the uploader can pace requests in batches using `upload.youtube.upload_batch_size`
and `upload.youtube.upload_batch_sleep_seconds` in config. This is helpful because Google documents
daily quota-unit costs clearly, but shorter burst limits are not published as a simple RPM/RP5M rule.

## Command reference

### Crawl

Writes crawled batches to `input/<novel_id>/origin/*.txt` and resumable state under `input/<novel_id>/.progress/`.

Backward compatible: `novel-tts crawl <novel_id> ...` is treated as `novel-tts crawl run <novel_id> ...`.

```bash
# Crawl a chapter range
uv run novel-tts crawl run <novel_id> --range 1-10

# Same, but with explicit bounds
uv run novel-tts crawl run <novel_id> --from 1 --to 10

# Optional: override directory URL (useful when source changes)
uv run novel-tts crawl run <novel_id> --range 1-10 --dir-url 'https://...'
```

### Crawl verify

Sanity-checks already-crawled origin files (does not recrawl). Useful before translation/TTS.
`--file` is interpreted as a filename under `input/<novel_id>/origin/` and can be specified multiple times.

```bash
# Verify an entire range
uv run novel-tts crawl verify <novel_id> --range 1-10

# Verify a specific origin batch file (filename under input/<novel_id>/origin/)
uv run novel-tts crawl verify <novel_id> --file chuong_1-10.txt
```

### Translate (direct, debug only)

Direct translation translates `input/<novel_id>/origin/*.txt` chapter-by-chapter, writes canonical per-chapter outputs
under `input/<novel_id>/.parts/`, and rebuilds `input/<novel_id>/translated/*.txt`.

```bash
# Translate all discovered origin batches
uv run novel-tts translate novel <novel_id>

# Translate only one origin batch file (filename under input/<novel_id>/origin/)
uv run novel-tts translate novel <novel_id> --file chuong_1-10.txt

# Re-translate even if parts already exist (use with care)
uv run novel-tts translate novel <novel_id> --file chuong_1-10.txt --force
```

Translate a single chapter (used by queue workers; also useful for debugging):

```bash
uv run novel-tts translate chapter <novel_id> --file chuong_1-10.txt --chapter 7
```

Translate captions (SRT) when they exist under `input/<novel_id>/captions/`:

```bash
uv run novel-tts translate captions <novel_id>
```

Run a polish/cleanup pass on translated outputs:

```bash
uv run novel-tts translate polish <novel_id> --range 101-500
uv run novel-tts translate polish <novel_id> --file chuong_1-10.txt
```

`translate polish` loads exact-match replacements from `configs/polish_replacement/common.json`
plus `configs/polish_replacement/<novel_id>.json`, with novel-specific entries overriding common keys.

### Queue (distributed translation via Redis)

Queue translation produces the same on-disk artifacts as direct translation: `.parts` and rebuilt `translated` batch files.

`queue launch` reads `.secrets/gemini-keys.txt` and spawns a supervisor + workers for the configured models.

Queue workers use a centralized quota gate (central quota v2) to coordinate rate-limit / quota waits across processes.

### YouTube

Inspect accessible YouTube playlists using the configured OAuth credentials in `configs/app.yaml` / `.secrets/youtube/`.

```bash
# List all accessible playlists with metadata
uv run novel-tts youtube playlist

# List only playlist ids and titles
uv run novel-tts youtube playlist --title-only

# Fetch one playlist by id or full playlist URL
uv run novel-tts youtube playlist --id PLxxxxxxxx
uv run novel-tts youtube playlist --id 'https://www.youtube.com/playlist?list=PLxxxxxxxx'

# List all uploaded videos with metadata
uv run novel-tts youtube video

# List only video ids and titles
uv run novel-tts youtube video --title-only

# Fetch one video by id
uv run novel-tts youtube video --id xxxxxxxx

# Review current video metadata, preview changed fields, confirm y/n, then update
uv run novel-tts youtube video update --id xxxxxxxx --title 'New title'
uv run novel-tts youtube video update --id xxxxxxxx --description 'New description'
uv run novel-tts youtube video update --id xxxxxxxx --privacy_status private
uv run novel-tts youtube video update --id xxxxxxxx --made_for_kids true
uv run novel-tts youtube video update --id xxxxxxxx --playlist_position 7

# Touch the video without changing fields
uv run novel-tts youtube video update --id xxxxxxxx

# Rewrite the playlist link on uploaded YouTube video descriptions
uv run novel-tts upload <novel_id> --platform youtube --update-playlist-index
uv run novel-tts upload <novel_id> --platform youtube --update-playlist-index --range 1-150

# Review current metadata, preview the update, confirm y/n, then update
uv run novel-tts youtube playlist update --id PLxxxxxxxx --title 'New title'
uv run novel-tts youtube playlist update --id PLxxxxxxxx --description 'New description'
uv run novel-tts youtube playlist update --id PLxxxxxxxx --privacy-status private

# Touch the playlist without changing fields (forces an update request/re-index attempt)
uv run novel-tts youtube playlist update --id PLxxxxxxxx
```

#### Quota supervisor (global)

`quota-supervisor` is a global helper process (not per-novel). It manages Redis-backed quota grants/ETAs that queue workers rely on.

Run it in a separate terminal whenever you use queue mode. Without it, jobs can get stuck in `waiting-quota` and appear to “do nothing” even though the queue stack is running.

```bash
# Foreground (recommended while debugging)
uv run novel-tts quota-supervisor

# Background daemon (best-effort detach). Logs: .logs/quota-supervisor.log
uv run novel-tts quota-supervisor -d

# Stop / restart the background quota supervisor
uv run novel-tts quota-supervisor --stop
uv run novel-tts quota-supervisor --restart
```

#### Command semantics

- Process control commands (`launch`, `ps`, `monitor`, `stop`, etc.) manage the running queue stack.
- Scheduling commands (`add`, `repair`) only enqueue jobs into Redis. They do not start workers; if the queue stack is not running, nothing will change on disk.

```bash
# One-shot: launch the whole queue stack for a novel
uv run novel-tts queue launch <novel_id>
uv run novel-tts queue launch <novel_id> --restart
uv run novel-tts queue launch <novel_id> --add-queue   # also scans + enqueues all chapters that still need work

# Monitor/status
uv run novel-tts queue ps <novel_id>
uv run novel-tts queue ps <novel_id> --all
uv run novel-tts queue ps-all
uv run novel-tts queue ps-all --all -f
uv run novel-tts queue monitor <novel_id>

# Stop queue processes for a novel (keeps Redis state)
uv run novel-tts queue stop <novel_id>
uv run novel-tts queue stop <novel_id> --role supervisor,worker
uv run novel-tts queue stop <novel_id> --pid 1234
```

#### Queue add (enqueue chapters)

Enqueue chapters for translation. You must pass exactly one of `--range` or `--all`. Use `--force` to re-translate.

```bash
uv run novel-tts queue add <novel_id> --range 2001-2500
uv run novel-tts queue add <novel_id> --range 2004-2016 --force
uv run novel-tts queue add <novel_id> --all
```

#### Queue reset-key (clear stuck key state)

Reset per-key Redis state (cooldown/quota/throttle) when a key gets stuck.

```bash
uv run novel-tts queue reset-key <novel_id> --key k5
uv run novel-tts queue reset-key <novel_id> --key k5 --model gemini-3.1-flash-lite-preview
uv run novel-tts queue reset-key <novel_id> --key k5,k6 --model gemma-3-27b-it,gemma-3-12b-it
uv run novel-tts queue reset-key <novel_id> --all
```

#### Queue repair (scan + requeue broken chapters)

Scans a chapter range and enqueues only the broken chapters back into the queue (force re-translate).
This is designed for cases where outputs contain placeholder tokens like `ZXQ1156QXZ`/`QZX...QXZ`, residual Han,
missing/empty parts, or stale parts (origin is newer than the part).

If you don’t see any changes on disk after running this, ensure the queue stack is running (e.g. `queue launch`) and watch progress via `queue monitor`.

```bash
uv run novel-tts queue repair <novel_id> --range 1401-1410
uv run novel-tts queue repair <novel_id> --all
```

### AI key telemetry

Reads `.secrets/gemini-keys.txt` and inspects Redis metrics emitted by queue workers.
Raw keys are never printed.

```bash
uv run novel-tts ai-key ps
uv run novel-tts ai-key ps -f
uv run novel-tts ai-key ps --filter k1 --filter 1234
uv run novel-tts ai-key ps --filter-raw "$GEMINI_API_KEY"
```

### TTS

Reads `input/<novel_id>/translated/chuong_<start>-<end>.txt` and writes audio assets under `output/<novel_id>/audio/<range>/`.

- Per-chapter WAVs are written under `output/<novel_id>/audio/<range>/.parts/` (the range folder stays clean, containing mostly the merged MP3).
- TTS caches per-chapter text hashes under `output/<novel_id>/audio/<range>/.parts/.cache/` to avoid re-synthesizing when text is unchanged.
- If the translated text changes, the chapter will be re-synthesized even without `--force`.

```bash
uv run novel-tts tts <novel_id> --range 1-10
uv run novel-tts tts <novel_id> --range 1-10 --force
uv run novel-tts tts <novel_id> --range 701-800 --tts-server-name onPremise --tts-model-name cpu
```

### Visual

Generates the visual layer under `output/<novel_id>/visual/` (typically requires audio for the same range).

```bash
uv run novel-tts visual <novel_id> --range 1-10
uv run novel-tts visual <novel_id> --chapter 1
```

### Video

Writes final MP4s under `output/<novel_id>/video/`.

```bash
uv run novel-tts video <novel_id> --range 1-10
```

### Upload

Uploads rendered videos by range.

- `youtube`: real upload via OAuth local token + YouTube Data API.
- `tiktok`: dry-run payload/validation only (real API upload is not implemented yet).

YouTube metadata convention:

- `title`: `output/<novel_id>/title.txt`
- `description`: `output/<novel_id>/description.txt` + `output/<novel_id>/subtitle/chuong_<start>-<end>_menu.txt`
- `thumbnail`: `output/<novel_id>/visual/chuong_<start>-<end>.png`
- `playlist`: `output/<novel_id>/playlist.txt`
- audience: not made for kids
- visibility: public

```bash
uv run novel-tts upload <novel_id> --platform youtube --range 1-10
uv run novel-tts upload <novel_id> --platform youtube --range 1-10 --dry-run
uv run novel-tts upload <novel_id> --platform tiktok --range 1-10
```

#### YouTube OAuth setup

Required files:

- `.secrets/youtube/client_secrets.json`
- `.secrets/youtube/token.json`

Example templates are available at:

- `.secrets/youtube/client_secrets.example.json`
- `.secrets/youtube/token.example.json`

How to get `client_secrets.json`:

1. Open [Google Cloud Console](https://console.cloud.google.com/).
2. Create/select a project.
3. Enable **YouTube Data API v3** in “APIs & Services > Library”.
4. Configure OAuth consent screen (External/Internal as appropriate), add your account as test user if needed.
5. Create OAuth client credentials: “APIs & Services > Credentials > Create Credentials > OAuth client ID”.
6. Choose app type **Desktop app**, then download JSON and save as `.secrets/youtube/client_secrets.json`.

How to get `token.json`:

1. Ensure `upload.youtube.credentials_path` points to `.secrets/youtube/client_secrets.json`.
2. Run first upload/dry-run command:
   - `uv run novel-tts upload <novel_id> --platform youtube --range 1-10 --dry-run`
3. Browser login/consent will open once; after consent, CLI auto-creates `.secrets/youtube/token.json`.

### Pipeline (end-to-end orchestration)

Runs multiple stages in order for a given range, with optional `--skip-*` flags for iteration.

Note: the pipeline's translation step uses direct `translate novel`. With a queue-only translation policy, run pipeline
with `--skip-translate`, translate via `novel-tts queue ...`, and resume the remaining stages.

By default, pipeline now runs upload at the end (using `upload.default_platform`, default `youtube`).
Use `--skip-upload` to disable or `--upload-platform` to override.

```bash
uv run novel-tts pipeline run <novel_id> --range 1-10
uv run novel-tts pipeline run <novel_id> --range 1-10 --skip-crawl --skip-captions
uv run novel-tts pipeline run <novel_id> --range 1-10 --skip-translate
uv run novel-tts pipeline run <novel_id> --range 1-10 --skip-upload
uv run novel-tts pipeline run <novel_id> --range 1-10 --upload-platform tiktok
uv run novel-tts pipeline run <novel_id> --range 1-10 --skip-crawl --skip-translate --skip-captions --skip-tts
```

## Troubleshooting

### Crawl issues

- If a source blocks plain HTTP, use Playwright fallback by installing the runtime:
  - `uv run playwright install chromium`

### Queue issues

- If `waiting-quota` never progresses, ensure the global quota supervisor is running:
  - `uv run novel-tts quota-supervisor` (or `-d`)
- If a specific key gets stuck in cooldown/quota state:
  - `uv run novel-tts queue reset-key <novel_id> --key kN [--model ...]`
- If translated outputs look poisoned (placeholders / residual Han / missing parts):
  - `uv run novel-tts queue repair <novel_id> --range <start>-<end>`

### Command help

When in doubt, rely on the built-in CLI help (kept accurate by the codebase):

```bash
uv run novel-tts --help
uv run novel-tts queue --help
uv run novel-tts translate --help
```
