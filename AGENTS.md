# Agent Instructions (Codex/Cursor)

This repository is a file-first CLI pipeline (`novel-tts`) for crawling, translating, TTS, and rendering media assets.

## Read Order (Fast Onboarding)

1. `README.md` for the common CLI commands and quick start (`uv sync`, `uv run novel-tts ...`).
2. `docs/ARCHITECTURE.md` for the system shape, storage layout, and module map.
3. Agent-oriented architecture summaries:
   - `docs/agents/codex/AGENTS.md`
   - `docs/agents/cursor/AGENTS.md`

## Core Invariants (Do Not Break)

- Stages communicate via the filesystem under `input/<novel>/` and `output/<novel>/` (Redis is queue bookkeeping only).
- `input/<novel>/.parts/...` is canonical per-chapter translation state.
- `input/<novel>/translated/*.txt` is derived and rebuildable from `.parts`.
- Crawl/origin headings are typically ASCII `Chuong <n> ...`, while translated/TTS headings are typically `Chương <n> ...` (audit TTS/media if you change heading formats).

## AI Agent Read Rules (Repo Hygiene)

To keep context small and avoid accidentally ingesting large/generated assets, **do not read** the following folders (or files under them) unless the task explicitly requires it:

- `./input`
- `./output`
- `./image`
- `./tmp`
- `./.logs`
- `./.secrets`
- `./.venv`
- `./tests`

When you must inspect artifacts, prefer **targeted, minimal reads** (specific file paths and small excerpts) instead of directory-wide scans.

## AI Agent Change Rules (Refactor/Architecture)

When doing a **refactor**, **architecture change**, or **strategy change**:

- Avoid keeping alias paths or backward-compatible behavior by default.
- Prefer a **complete migration**: update all call sites, configs, and docs to the new shape so the codebase has *one* consistent design.
- Only keep backward-compatibility when explicitly required for a real operational reason; if you must, make it time-boxed with a clear deprecation/removal follow-up.

## Where Workflows Live

- Human-facing usage: `README.md`
- Architectural contracts: `docs/ARCHITECTURE.md`
- Detailed "what to read/touch" guidance for agents: `docs/agents/*/AGENTS.md`

If you add a new workflow, prefer:

- Add a runnable command in `README.md`
- Add deeper notes in `docs/` (keep `AGENTS.md` concise)
