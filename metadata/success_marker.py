from __future__ import annotations

from pathlib import Path


def write_success_marker(output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(b"")

    return output_path
