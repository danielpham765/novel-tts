# novel-tts

Generic Python CLI for novel crawling, translation, caption translation, TTS, visual generation, and video rendering.

## Quick start with uv

```bash
uv sync
uv run novel-tts --help
```

## Config

- Novel configs live in `configs/novels/*.json`.
- Source configs live in `configs/sources/*.json`.
- Per-novel glossaries live in `configs/glossaries/*.json`.

Gemini API keys for queue workers live in `.secrets/gemini-keys.txt`, one key per line.
For one-off translation runs, `GEMINI_API_KEY` from env still works.

## Commands

```bash
uv run novel-tts crawl vo-cuc-thien-ton --range 1-10
uv run novel-tts crawl verify vo-cuc-thien-ton --range 1-10
# Queue-only note for tram-than: do not use direct `translate novel`.
# uv run novel-tts translate novel tram-than
uv run novel-tts translate captions vo-cuc-thien-ton
uv run novel-tts queue supervisor vo-cuc-thien-ton
uv run novel-tts queue worker vo-cuc-thien-ton --key-index 1 --model gemma-3-27b-it
uv run novel-tts queue launch vo-cuc-thien-ton --restart
uv run novel-tts queue ps vo-cuc-thien-ton              # list queue processes + progress for a single novel
uv run novel-tts queue ps-all                           # list queue processes for all novels
uv run novel-tts queue stop vo-cuc-thien-ton            # stop all queue processes for a novel (keep Redis state)
uv run novel-tts queue stop vo-cuc-thien-ton --role supervisor,worker
uv run novel-tts queue stop vo-cuc-thien-ton --pid 1234 # stop one specific PID
uv run novel-tts tts vo-cuc-thien-ton --range 1-10
uv run novel-tts visual vo-cuc-thien-ton --range 1-10
uv run novel-tts video vo-cuc-thien-ton --range 1-10
```

## Notes

- Python target: `3.10`
- Install browser runtime for crawl mode when needed: `uv run playwright install chromium`
- `queue launch` reads `.secrets/gemini-keys.txt` and spawns `1 supervisor + 2 workers/key` by default (`gemma-3-27b-it`, `gemma-3-12b-it`)
