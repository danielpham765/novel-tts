# novel-tts

File-first Python CLI pipeline for crawling serialized web novels, translating them to Vietnamese, generating TTS audio, and rendering publishable video assets.

- Architecture: `docs/ARCHITECTURE.md`
- Agent notes (internal): `docs/agents/codex/AGENTS.md`

## Table of contents

- [What this repo does](#what-this-repo-does)
- [Quick start](#quick-start)
- [Requirements](#requirements)
- [Configuration](#configuration)
- [Storage contract and invariants](#storage-contract-and-invariants)
- [Recommended workflows](#recommended-workflows)
- [Command reference](#command-reference)
- [Troubleshooting](#troubleshooting)

## What this repo does

At a high level, `novel-tts` supports this pipeline:

1. Crawl source chapters into `input/<novel_id>/origin/*.txt`
2. Translate chapters into canonical per-chapter outputs under `input/<novel_id>/.parts/`
3. Rebuild merged translated files under `input/<novel_id>/translated/*.txt`
4. Generate TTS audio under `output/<novel_id>/audio/`
5. Generate visual assets under `output/<novel_id>/visual/`
6. Mux final MP4s under `output/<novel_id>/video/`
7. Upload outputs to supported platforms

Important operational note:

- translation policy should normally be queue-first
- direct `translate novel` is mainly for debugging and small one-off runs

## Quick start

```bash
uv sync
uv run novel-tts --help
```

Tip:

- all commands support top-level `--log-file /path/to/file.log`

## Requirements

Base requirements:

- Python `3.10`
- `uv`

Optional / stage-specific requirements:

- crawl:
  - Playwright + Chromium
  - install with `uv run playwright install chromium`
- queue translation:
  - Redis
  - default config in `configs/app.yaml`: host `127.0.0.1`, port `6379`, db `1`, prefix `novel_tts`
- media rendering:
  - `ffmpeg`
  - `ffprobe`
- current TTS path:
  - a reachable Gradio TTS server configured in app/novel config

## Configuration

### Main config files

- novel configs: `configs/novels/*.json`
- source configs: `configs/sources/*.json`
- per-novel glossaries: `configs/glossaries/*.json`
- app defaults: `configs/app.yaml`
- polish replacements:
  - `configs/polish_replacement/common.json`
  - `configs/polish_replacement/<novel_id>.json`
- TTS provider catalogs:
  - `configs/providers/tts_servers.yaml`
  - `configs/providers/tts_models.yaml`

### Secrets and API keys

Gemini:

- queue workers read API keys from `.secrets/gemini-keys.txt`
- use one key per line
- direct translation can still use `GEMINI_API_KEY`
- if `GEMINI_API_KEY` is unset, direct Gemini translation falls back to the first non-empty line in `.secrets/gemini-keys.txt`

OpenAI:

- set `OPENAI_API_KEY`

YouTube:

- OAuth secrets and token live under `.secrets/youtube/`

## Storage contract and invariants

This repo is intentionally file-first: stages communicate through files under `input/<novel_id>/` and `output/<novel_id>/`.

### Translation truth lives on disk

- canonical per-chapter state:
  - `input/<novel_id>/.parts/<batch>/*.txt`
- derived merged outputs:
  - `input/<novel_id>/translated/*.txt`

`translated/*.txt` is rebuildable from `.parts` and should not be treated as the primary source of truth.

### Crawl outputs

- crawled source batches:
  - `input/<novel_id>/origin/*.txt`
- resumable state:
  - `input/<novel_id>/.progress/*`

### Media outputs

- audio:
  - `output/<novel_id>/audio/<range>/*`
- visual:
  - `output/<novel_id>/visual/*`
- final video:
  - `output/<novel_id>/video/*`

### Heading convention

Do not casually change heading formats:

- crawl/origin headings are typically ASCII:
  - `Chuong <n> ...`
- translated/TTS headings are typically Vietnamese:
  - `Chương <n> ...`

Changing headings affects:

- chapter splitting
- translated file rebuild
- TTS chapter detection
- subtitle/menu generation
- downstream media assets

## Recommended workflows

### Recommended queue-first workflow

Use this as the default production workflow.

```bash
# 1) Crawl
uv run novel-tts crawl run <novel_id> --range <start>-<end>

# 2) Verify crawled files (does not recrawl)
uv run novel-tts crawl verify <novel_id> --range <start>-<end>

# 3) Start the global quota supervisor
uv run novel-tts quota-supervisor
# or:
uv run novel-tts quota-supervisor -d

# 4) Launch queue stack for a novel
uv run novel-tts queue launch <novel_id>

# 5) Enqueue chapters for translation
uv run novel-tts queue add <novel_id> --range <start>-<end>
# or:
uv run novel-tts queue add <novel_id> --all

# 6) Monitor progress
uv run novel-tts queue monitor <novel_id>
uv run novel-tts queue ps <novel_id>

# 7) TTS + media
uv run novel-tts tts <novel_id> --range <start>-<end>
uv run novel-tts visual <novel_id> --range <start>-<end>
uv run novel-tts video <novel_id> --range <start>-<end>

# 8) Upload
uv run novel-tts upload <novel_id> --platform youtube --range <start>-<end>
```

Queue-first guidance:

- prefer queue translation for normal work
- avoid direct `translate novel` except for debugging or very small runs
- `quota-supervisor` should be running whenever queue workers need quota progress

### Optional end-to-end pipeline workflow

`pipeline run` is useful for orchestration, but note:

- its translation step uses direct `translate novel`
- if you want queue-only translation, run queue translation separately and use `pipeline run --skip-translate`

Examples:

```bash
# End-to-end range
uv run novel-tts pipeline run <novel_id> --range <start>-<end>

# Queue-first style: skip translate in pipeline
uv run novel-tts pipeline run <novel_id> --range <start>-<end> --skip-translate

# Downstream media stage-by-stage across a large range
uv run novel-tts pipeline run <novel_id> --range 1-2000 --mode per-stage

# Process each translated batch end-to-end
uv run novel-tts pipeline run <novel_id> --range 1-2000 --mode per-video
```

## Command reference

### Crawl

Writes crawled batches to `input/<novel_id>/origin/*.txt` and resumable state under `input/<novel_id>/.progress/`.

Backward compatibility:

- `novel-tts crawl <novel_id> ...` is treated as `novel-tts crawl run <novel_id> ...`

#### `crawl run`

```bash
# Crawl a chapter range
uv run novel-tts crawl run <novel_id> --range 1-10

# Same, but with explicit bounds
uv run novel-tts crawl run <novel_id> --from 1 --to 10

# Override directory URL if needed
uv run novel-tts crawl run <novel_id> --range 1-10 --dir-url 'https://...'
```

#### `crawl verify`

Sanity-checks already-crawled origin files. It does not recrawl.

`--file` is interpreted as a filename under `input/<novel_id>/origin/` and can be repeated.

```bash
uv run novel-tts crawl verify <novel_id> --range 1-10
uv run novel-tts crawl verify <novel_id> --file chuong_1-10.txt
```

### Translate

#### Direct translation (`translate novel`) is debug-oriented

Direct translation:

- reads `input/<novel_id>/origin/*.txt`
- writes canonical chapter outputs under `input/<novel_id>/.parts/`
- rebuilds `input/<novel_id>/translated/*.txt`

```bash
# Translate all discovered origin batches
uv run novel-tts translate novel <novel_id>

# Translate one origin batch file
uv run novel-tts translate novel <novel_id> --file chuong_1-10.txt

# Re-translate even if parts already exist
uv run novel-tts translate novel <novel_id> --file chuong_1-10.txt --force
```

#### `translate chapter`

Used by queue workers, and also useful for debugging:

```bash
uv run novel-tts translate chapter <novel_id> --file chuong_1-10.txt --chapter 7
```

#### `translate captions`

Translates captions when they exist under `input/<novel_id>/captions/`:

```bash
uv run novel-tts translate captions <novel_id>
```

#### `translate polish`

Runs a cleanup pass on translated outputs:

```bash
uv run novel-tts translate polish <novel_id> --range 101-500
uv run novel-tts translate polish <novel_id> --file chuong_1-10.txt
```

`translate polish` loads exact-match replacements from:

- `configs/polish_replacement/common.json`
- `configs/polish_replacement/<novel_id>.json`

Novel-specific entries override common keys.

### Queue

Queue translation produces the same on-disk artifacts as direct translation:

- `.parts`
- rebuilt `translated` batch files

`queue launch` reads `.secrets/gemini-keys.txt` and spawns:

- a supervisor
- a status monitor
- workers for configured models/keys

Queue workers use the centralized quota gate (central quota v2) to coordinate rate-limit and quota waits across processes.

#### Quota supervisor (global)

`quota-supervisor` is global, not per-novel.

Run it in a separate terminal whenever you use queue mode. Without it, jobs can remain in `waiting-quota` and appear stalled.

```bash
# Foreground
uv run novel-tts quota-supervisor

# Background daemon
uv run novel-tts quota-supervisor -d

# Stop / restart the background daemon
uv run novel-tts quota-supervisor --stop
uv run novel-tts quota-supervisor --restart
```

#### Queue process control

Process-control commands manage running queue processes.

```bash
# Launch queue stack
uv run novel-tts queue launch <novel_id>
uv run novel-tts queue launch <novel_id> --restart
uv run novel-tts queue launch <novel_id> --add-queue

# Monitor/status
uv run novel-tts queue monitor <novel_id>
uv run novel-tts queue ps <novel_id>
uv run novel-tts queue ps <novel_id> --all
uv run novel-tts queue ps-all
uv run novel-tts queue ps-all --all -f

# Stop queue processes for a novel
uv run novel-tts queue stop <novel_id>
uv run novel-tts queue stop <novel_id> --role supervisor,worker
uv run novel-tts queue stop <novel_id> --pid 1234
```

#### Queue scheduling commands

Scheduling commands enqueue work into Redis. They do not start workers.

If the queue stack is not running, nothing will change on disk.

#### `queue add`

Enqueue chapters for translation. Pass exactly one of `--range`, `--chapters`, `--repair-report`, or `--all`.

Use `--force` to re-translate.

```bash
uv run novel-tts queue add <novel_id> --range 2001-2500
uv run novel-tts queue add <novel_id> --range 2004-2016 --force
uv run novel-tts queue add <novel_id> --chapters 1205,1214
uv run novel-tts queue add <novel_id> --all
```

#### `queue reset-key`

Reset per-key Redis state when a key gets stuck in cooldown/quota/throttle state.

```bash
uv run novel-tts queue reset-key <novel_id> --key k5
uv run novel-tts queue reset-key <novel_id> --key k5 --model gemini-3.1-flash-lite-preview
uv run novel-tts queue reset-key <novel_id> --key k5,k6 --model gemma-3-27b-it,gemma-3-12b-it
uv run novel-tts queue reset-key <novel_id> --all
```

#### `queue repair`

Scans a chapter range and enqueues only broken chapters back into the queue with force re-translate behavior.

Typical reasons:

- placeholder tokens like `ZXQ1156QXZ` / `QZX...QXZ`
- residual Han text
- missing or empty parts
- stale parts where origin is newer

If you do not see changes on disk after running this, ensure the queue stack is running and watch progress via `queue monitor`.

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

Behavior notes:

- per-chapter WAVs are written under `output/<novel_id>/audio/<range>/.parts/`
- per-chapter text hashes are cached under `output/<novel_id>/audio/<range>/.parts/.cache/`
- if translated text changes, the chapter will be re-synthesized even without `--force`

```bash
uv run novel-tts tts <novel_id> --range 1-10
uv run novel-tts tts <novel_id> --range 1-10 --force
uv run novel-tts tts <novel_id> --range 701-800 --tts-server-name onPremise --tts-model-name cpu
```

### Visual

Generates the visual layer under `output/<novel_id>/visual/`.

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

Platforms:

- `youtube`: real upload via OAuth local token + YouTube Data API
- `tiktok`: dry-run payload/validation only

YouTube metadata convention:

- title:
  - `output/<novel_id>/title.txt`
- description:
  - `output/<novel_id>/description.txt`
  - plus `output/<novel_id>/subtitle/chuong_<start>-<end>_menu.txt`
- thumbnail:
  - `output/<novel_id>/visual/chuong_<start>-<end>.png`
- playlist:
  - `output/<novel_id>/playlist.txt`

Default audience/visibility:

- not made for kids
- public

```bash
uv run novel-tts upload <novel_id> --platform youtube --range 1-10
uv run novel-tts upload <novel_id> --platform youtube --range 1-10 --dry-run
uv run novel-tts upload <novel_id> --platform tiktok --range 1-10
```

YouTube upload pacing can be tuned with:

- `upload.youtube.upload_batch_size`
- `upload.youtube.upload_batch_sleep_seconds`

This helps smooth bursts even though Google mainly documents daily quota units rather than short-window limits.

#### YouTube OAuth setup

Required files:

- `.secrets/youtube/client_secrets.json`
- `.secrets/youtube/token.json`

Example templates:

- `.secrets/youtube/client_secrets.example.json`
- `.secrets/youtube/token.example.json`

How to get `client_secrets.json`:

1. Open [Google Cloud Console](https://console.cloud.google.com/).
2. Create or select a project.
3. Enable `YouTube Data API v3`.
4. Configure the OAuth consent screen.
5. Create OAuth client credentials as a Desktop app.
6. Download the JSON and save it as `.secrets/youtube/client_secrets.json`.

How to get `token.json`:

1. Ensure `upload.youtube.credentials_path` points to `.secrets/youtube/client_secrets.json`.
2. Run a first upload or dry-run command:
   - `uv run novel-tts upload <novel_id> --platform youtube --range 1-10 --dry-run`
3. Complete the browser login/consent flow once.
4. The CLI will create `.secrets/youtube/token.json`.

### YouTube admin commands

Inspect accessible YouTube playlists and videos using configured OAuth credentials.

```bash
# Playlists
uv run novel-tts youtube playlist
uv run novel-tts youtube playlist --title-only
uv run novel-tts youtube playlist --id PLxxxxxxxx
uv run novel-tts youtube playlist --id 'https://www.youtube.com/playlist?list=PLxxxxxxxx'

# Playlist update
uv run novel-tts youtube playlist update --id PLxxxxxxxx --title 'New title'
uv run novel-tts youtube playlist update --id PLxxxxxxxx --description 'New description'
uv run novel-tts youtube playlist update --id PLxxxxxxxx --privacy-status private
uv run novel-tts youtube playlist update --id PLxxxxxxxx

# Videos
uv run novel-tts youtube video
uv run novel-tts youtube video --title-only
uv run novel-tts youtube video --id xxxxxxxx

# Video update
uv run novel-tts youtube video update --id xxxxxxxx --title 'New title'
uv run novel-tts youtube video update --id xxxxxxxx --description 'New description'
uv run novel-tts youtube video update --id xxxxxxxx --privacy_status private
uv run novel-tts youtube video update --id xxxxxxxx --made_for_kids true
uv run novel-tts youtube video update --id xxxxxxxx --playlist_position 7
uv run novel-tts youtube video update --id xxxxxxxx

# Rewrite playlist links in uploaded video descriptions
uv run novel-tts upload <novel_id> --platform youtube --update-playlist-index
uv run novel-tts upload <novel_id> --platform youtube --update-playlist-index --range 1-150
```

### Pipeline

Runs multiple stages in order for a given range, with optional `--skip-*` flags for iteration.

By default, pipeline runs upload at the end using `upload.default_platform` (default `youtube`).

Use:

- `--skip-upload` to disable upload
- `--upload-platform` to override platform

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

- if a source blocks plain HTTP, install Playwright runtime:
  - `uv run playwright install chromium`

### Queue issues

- if `waiting-quota` never progresses, ensure the global quota supervisor is running:
  - `uv run novel-tts quota-supervisor`
  - or `uv run novel-tts quota-supervisor -d`
- if a specific key gets stuck in cooldown/quota state:
  - `uv run novel-tts queue reset-key <novel_id> --key kN [--model ...]`
- if translated outputs look poisoned:
  - `uv run novel-tts queue repair <novel_id> --range <start>-<end>`

### Built-in help

When in doubt, rely on CLI help:

```bash
uv run novel-tts --help
uv run novel-tts queue --help
uv run novel-tts translate --help
```
