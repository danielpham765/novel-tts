# Workflow: Optimize Background Video

Use this workflow when the user wants Codex to shrink a novel background video while keeping image quality close to the approved "light" encode profile.

## Input

- `novel_id`
  - example: `tu-tieu-than-loi`

Default input file:

- `image/<novel_id>/background.mp4`

## Output

- overwrite `image/<novel_id>/background.mp4`
- target profile:
  - HEVC / H.265
  - `1920x1080`
  - `30fps`
  - around `1.7-1.9 Mbps`
  - visually close to the accepted light encode profile

## Goal

- reduce background video size aggressively enough for production use
- keep the perceived image quality suitable for background usage
- keep resolution and frame rate unchanged
- write the optimized result back to `background.mp4`

## Read Order

1. `README.md`
2. `docs/ARCHITECTURE.md`
3. `docs/agents/codex/AGENTS.md`
4. `novel_tts/background/service.py`
5. `novel_tts/cli/main.py`

## CLI Command

```bash
uv run novel-tts background optimize <novel_id>
```

Current default encode profile:

- codec: `libx265`
- preset: `slow`
- CRF: `24`
- pixel format: `yuv420p`
- MP4 tag: `hvc1`
- only keep the main video stream: `-map 0:v:0`
- move moov atom to the front with `+faststart`

## Implementation Rules

- resolve the target file from `config.storage.image_dir / "background.mp4"`
- encode to a temporary file in the same folder first
- only replace `background.mp4` after the optimized file exists and has non-zero size
- do not keep attached pictures or extra streams
- do not silently downscale or change FPS
- prefer deterministic, file-first behavior over interactive prompts

## Verification

- confirm the output file still exists at `image/<novel_id>/background.mp4`
- inspect codec, bitrate, duration, width, height, and frame rate with `ffprobe`
- confirm the final file is smaller than the original unless the source is already smaller than the target profile

## Definition Of Done

- `background.mp4` has been rewritten in place
- resulting video is HEVC/H.265
- resolution and FPS are unchanged
- output size is materially smaller than the input on oversized source files
- the command can be repeated with only `novel_id` as input
