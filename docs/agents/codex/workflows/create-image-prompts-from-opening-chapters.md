# Workflow: Create Image Prompts From Opening Chapters

Use this workflow when the user wants Codex to read the opening chapters of a translated novel batch and create a set of image-generation prompts for video production.

## Input

- `novel_id`
  - example: `tu-tieu-than-loi`

Default input file to inspect:

- `input/<novel_id>/translated/chuong_1-10.txt`

Default output folder to write:

- `image/<novel_id>/prompts/`

## Goal

- read the first 10 translated chapters for the given novel
- choose 10 visually strong, viewer-retaining scenes
- write 1 prompt per scene as separate `.txt` files under `image/<novel_id>/prompts/`
- keep main characters visually consistent across all prompts
- optimize prompts for Gemini image generation used in story-video production

## Read Order

1. `README.md`
2. `docs/ARCHITECTURE.md`
3. `docs/agents/codex/AGENTS.md`
4. existing prompt examples under:
   - `image/*/prompts/*.txt`
   - read only 2-4 representative examples, not the whole tree
5. the target translated file:
   - `input/<novel_id>/translated/chuong_1-10.txt`

## How To Inspect Efficiently

- do not scan all of `input/` or all of `image/`
- first check whether `image/<novel_id>/prompts/` already exists
- read only a few existing prompt files from other novels to infer the house style
- use `rg -n "^Chương|^Chuong"` on the target file to find chapter boundaries
- read the target file in chunks with `sed -n` instead of dumping everything at once

## Prompt Format

Prefer this structure for each prompt file:

1. `CHARACTER LOCK:`
2. `CONTENT:`
3. `ENDING:`

### CHARACTER LOCK

Include:

- main protagonist visual identity
- important recurring side characters only when needed for that scene set
- fixed weapon, outfit phase, aura color, and environment cues that should stay consistent
- scene continuity notes when multiple consecutive images share the same phase or setting

Keep this section practical and generation-oriented, not novel-summary prose.

### CONTENT

Describe one specific image only.

Focus on:

- strongest emotional or action beat from the scene
- exact composition and focal subject
- environment and time-of-day cues
- tension, spectacle, or emotional hook that helps retain viewers
- continuity with the established character lock
- when the image would be hard to understand without context, allow a small Vietnamese context hint inside the image:
  - short dialogue
  - short caption
  - signage, interface text, rank text, exam text, or other in-world readable text
- use context hints only when they materially improve clarity for viewers who have not read the chapter

Each prompt should aim for a thumbnail-worthy frame, not a generic illustration of the chapter.

## Vietnamese Context Hints

The image prompts may include small in-image Vietnamese text to help viewers immediately understand the beat.

Allowed forms:

- short speech bubbles
- one short caption line
- one short UI / ranking / exam / sign text block when the scene naturally supports it

Rules:

- use Vietnamese only
- keep text short and high-signal
- prefer 1 hint only for most scenes
- use 2 hints only when the scene is genuinely confusing without them
- do not force text into every image
- do not turn the image into a poster full of writing
- only add readable text when it strengthens story comprehension or dramatic payoff

Good use cases:

- shocking confession or declaration
- system awakening or exam announcement
- rank reveal or identity reveal
- emotionally important line that anchors the scene

Avoid:

- repeating exposition the image already shows clearly
- long narration blocks
- multiple unrelated text elements competing for attention
- adding speech bubbles to scenes that work better as pure cinematic visuals

### ENDING

Add a concise visual style tail suitable for Gemini image generation, for example:

- cinematic
- ultra detailed
- strong focal point
- emotional realism
- consistent faces
- no watermark
- no distorted anatomy

Adapt the ending style to the novel genre:

- urban supernatural / sci-fi
- xianxia / fantasy
- romance / drama
- post-apocalyptic action

## Scene Selection Rules

Pick 10 scenes that together create a compelling opening arc for video use.

Prioritize:

- first major hook
- first reveal of the system / cheat / destiny shift
- strongest transformation scene
- first display of overwhelming power
- first socially charged encounter with important side characters
- first high-stakes test, duel, or exam setup
- first monster kill or major combat beat
- first public payoff where others are shocked by the protagonist

Avoid:

- repetitive exposition-only scenes
- 2-3 prompts that all show the same emotional beat with no visual escalation
- low-energy walking or talking scenes unless they establish strong contrast or tension

## Naming Convention

Write exactly 10 prompt files:

- `01-<scene-slug>.txt`
- `02-<scene-slug>.txt`
- ...
- `10-<scene-slug>.txt`

Slug rules:

- lowercase
- ASCII only
- use hyphens
- make the scene easy to recognize later

## Writing Rules

- create `image/<novel_id>/prompts/` if it does not exist
- if prompt files already exist, read them before overwriting so you understand current naming/style
- unless the user explicitly asks for merge behavior, replace the prompt set with a fresh consistent 10-scene set
- keep language in the prompt files aligned with the repo's existing prompt style
- keep prompts concrete enough for direct paste into Gemini
- avoid vague phrases like "beautiful scene" or "epic moment" without actual composition details
- when adding dialogue or captions, explicitly say they must be readable Vietnamese text
- if the scene does not need text for clarity, prefer no text

## Definition Of Done

- `input/<novel_id>/translated/chuong_1-10.txt` has been read
- 10 representative scenes have been selected from those chapters
- prompt files exist under `image/<novel_id>/prompts/`
- each prompt is one image only
- all prompts maintain consistent protagonist design and recurring visual rules
- the set escalates well when used as a sequence for an audio-story video
