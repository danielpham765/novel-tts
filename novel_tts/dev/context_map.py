from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import yaml


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _context_map_path() -> Path:
    return _repo_root() / "docs" / "agents" / "context-map.yaml"


def load_context_map() -> dict[str, Any]:
    path = _context_map_path()
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Invalid context map format: {path}")
    return payload


def _validate_file_ref(repo_root: Path, value: Any, *, label: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Invalid {label}: expected non-empty path string")
    path = repo_root / value
    if not path.exists():
        raise ValueError(f"Context map references missing path: {value}")


def validate_context_map(payload: dict[str, Any]) -> None:
    repo_root = _repo_root()
    version = payload.get("version")
    if version != 1:
        raise ValueError(f"Unsupported context map version: {version!r}")

    stable = payload.get("stable_repo_context")
    if not isinstance(stable, dict):
        raise ValueError('Missing "stable_repo_context" object')
    for item in stable.get("read_first", []):
        if not isinstance(item, dict):
            raise ValueError('Invalid "stable_repo_context.read_first" entry')
        _validate_file_ref(repo_root, item.get("path"), label="stable_repo_context.read_first.path")

    for item in stable.get("repeated_token_hotspots", []):
        if not isinstance(item, dict):
            raise ValueError('Invalid "stable_repo_context.repeated_token_hotspots" entry')
        _validate_file_ref(repo_root, item.get("path"), label="stable_repo_context.repeated_token_hotspots.path")

    tasks = payload.get("tasks")
    if not isinstance(tasks, dict) or not tasks:
        raise ValueError('Missing "tasks" object')
    for task_name, task_payload in tasks.items():
        if not isinstance(task_payload, dict):
            raise ValueError(f'Invalid task payload for "{task_name}"')
        for key in ("summary", "dynamic_context"):
            if key not in task_payload:
                raise ValueError(f'Missing "{key}" for task "{task_name}"')
        for group_name in ("read_first", "maybe_read"):
            for item in task_payload.get(group_name, []):
                if not isinstance(item, dict):
                    raise ValueError(f'Invalid "{group_name}" entry for task "{task_name}"')
                _validate_file_ref(repo_root, item.get("path"), label=f"{task_name}.{group_name}.path")


def _format_file_entries(title: str, items: list[dict[str, str]]) -> list[str]:
    lines = [title]
    for item in items:
        lines.append(f"- {item['path']}: {item['why']}")
    return lines


def render_overview(payload: dict[str, Any]) -> str:
    stable = payload["stable_repo_context"]
    tasks = payload["tasks"]
    lines: list[str] = [
        "Stable repo context:",
        *[f"- {item}" for item in stable["summary"]],
        "Do not scan:",
        *[f"- {item}" for item in stable["do_not_scan"]],
        "Tasks:",
        *[f"- {name}: {task['summary']}" for name, task in sorted(tasks.items())],
        "",
        "Use `novel-tts-context <task>` to print a narrow task map.",
    ]
    return "\n".join(lines)


def render_task(payload: dict[str, Any], task_name: str) -> str:
    tasks = payload["tasks"]
    if task_name not in tasks:
        available = ", ".join(sorted(tasks))
        raise ValueError(f'Unknown task "{task_name}". Available: {available}')

    stable = payload["stable_repo_context"]
    task = tasks[task_name]
    lines: list[str] = [
        f"Task: {task_name}",
        f"Summary: {task['summary']}",
        "",
        "Stable repo context:",
        *[f"- {item}" for item in stable["summary"]],
        "Do not scan:",
        *[f"- {item}" for item in stable["do_not_scan"]],
        "",
        *_format_file_entries("Read first:", task["read_first"]),
    ]
    maybe_read = task.get("maybe_read") or []
    if maybe_read:
        lines.extend(["", *_format_file_entries("Read only if needed:", maybe_read)])
    avoid_first = task.get("avoid_first") or []
    if avoid_first:
        lines.extend(["", "Avoid reading first:", *[f"- {item}" for item in avoid_first]])
    lines.extend(["", "Dynamic context to collect before widening scope:"])
    lines.extend(f"- {item}" for item in task["dynamic_context"])
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="novel-tts-context",
        description="Print a compact, task-specific coding context map for this repo.",
    )
    parser.add_argument("task", nargs="?", help="Task key to narrow context (translate, queue, tts, media, config_cli)")
    parser.add_argument("--check", action="store_true", help="Validate the context map and exit")
    parser.add_argument("--list", action="store_true", help="Print overview plus task names")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    payload = load_context_map()
    validate_context_map(payload)

    if args.check:
        print("Context map OK")
        return 0
    if args.list or not args.task:
        print(render_overview(payload))
        return 0

    print(render_task(payload, args.task))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
