"""Utilities for release management."""

from pathlib import Path

from materialize import MZ_ROOT


def doc_file_path(version: str) -> Path:
    return MZ_ROOT / "doc" / "user" / "content" / "releases" / f"{version}.md"
