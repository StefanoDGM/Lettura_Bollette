import json
import os
import time
from pathlib import Path
from typing import Optional


def ensure_directory(path: Path) -> None:
    """Ensure the directory exists."""
    path.parent.mkdir(parents=True, exist_ok=True)


def _find_file_upwards(filename: str, start_dir: Optional[Path] = None, max_levels: int = 5) -> Optional[Path]:
    """Search for a file by walking up parent directories."""
    current = (start_dir or Path.cwd()).resolve()
    for _ in range(max_levels + 1):
        candidate = current / filename
        if candidate.exists():
            return candidate
        if current.parent == current:
            break
        current = current.parent
    return None


def load_api_key(api_file: str = "api.txt") -> Optional[str]:
    """Load API key from file if exists."""
    api_path = _find_file_upwards(api_file)
    if not api_path:
        return None

    with open(api_path, "r", encoding="utf-8") as file_obj:
        content = file_obj.read().strip()
        lines = content.split("\n")
        for line in lines:
            if line.startswith("sk-"):
                return line.strip()
    return None

