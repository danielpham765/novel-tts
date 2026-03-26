# Workflow: Update Project Docs

Use this workflow when the user asks to refresh project documentation so it matches the current codebase.

## Goal

- update all relevant docs affected by the current code and workflow shape
- prefer current implementation over older docs wording
- keep docs practical, file-first, and operational rather than aspirational
- keep `README.md`, `docs/ARCHITECTURE.md`, and agent docs consistent with each other
- keep `docs/ARCHITECTURE.md` as the detailed current-state architecture reference, not a shallow summary

## Scope

Default docs to consider:

- `AGENTS.md`
- `README.md`
- `docs/ARCHITECTURE.md`
- `docs/agents/codex/AGENTS.md`
- `docs/agents/codex/workflows/*.md`
- `docs/agents/cursor/AGENTS.md`

Update only the docs actually affected by the current code changes or drift you find.

## Read Order

1. `AGENTS.md`
2. `README.md`
3. `docs/ARCHITECTURE.md`
4. `docs/agents/codex/AGENTS.md`
5. `docs/agents/cursor/AGENTS.md`
6. any existing workflow docs under `docs/agents/codex/workflows/`
7. `novel_tts/cli/main.py`
8. `novel_tts/config/loader.py`
9. `novel_tts/config/models.py`
10. stage service roots:
   - `novel_tts/crawl/service.py`
   - `novel_tts/translate/novel.py`
   - `novel_tts/translate/captions.py`
   - `novel_tts/queue/translation_queue.py`
   - `novel_tts/quota/client.py`
   - `novel_tts/quota/supervisor.py`
   - `novel_tts/ai_key/service.py`
   - `novel_tts/tts/service.py`
   - `novel_tts/media/service.py`
   - `novel_tts/upload/service.py`

## How To Inspect Efficiently

- prefer `rg --files`, `rg -n "^(def|class) "`, and targeted `sed -n` reads
- map public entrypoints, command families, config sections, artifact paths, and user-facing workflows
- avoid reading `input/`, `output/`, `image/`, or `tmp/` unless the task explicitly needs artifact examples
- treat docs as stale until verified against code
- compare docs against each other, not just against code

## What To Capture

Across the affected docs, make sure the current docs set covers:

- top-level purpose and system shape
- current command surface and recommended workflows
- config merge model and important runtime dataclasses
- filesystem contracts under `input/` and `output/`
- per-stage responsibilities and public entrypoints
- queue/quota responsibilities and Redis role
- TTS/media/upload expectations and artifact flow
- important invariants that downstream stages rely on
- agent-specific reading/touch guidance
- workflow docs for repeatable maintenance tasks when useful

## What To Avoid

- do not document deprecated behavior as if it is canonical
- do not infer features from old docs alone
- do not bulk-describe generated artifacts from `input/` or `output/`
- do not update one doc in a way that contradicts another
- do not leave root `AGENTS.md`, architecture docs, README, and agent docs half-migrated

## Definition Of Done

- all affected docs match current CLI/config/module structure
- references to commands, files, and modules use current names
- workflow docs and agent docs point to the right locations
- root `AGENTS.md` and agent docs stay aligned on shared repo rules
- storage/invariant sections align with current code contracts
- `README.md` stays task-oriented, while `docs/ARCHITECTURE.md` stays deeper and more structural
- `docs/ARCHITECTURE.md` is detailed enough to explain the current architecture, module boundaries, and storage/runtime contracts
- agent docs stay concise and point to the deeper docs instead of duplicating everything
- the updated doc set reads consistently as one system, not as separate stale snapshots
