from __future__ import annotations

import subprocess
from pathlib import Path


def run_command(args: list[str], cwd: Path | None = None, capture_output: bool = False) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        args,
        cwd=str(cwd) if cwd else None,
        check=True,
        capture_output=capture_output,
        text=True,
    )
