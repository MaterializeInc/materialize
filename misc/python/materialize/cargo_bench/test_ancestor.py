# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import os
import subprocess
from pathlib import Path

import pytest

from materialize.cargo_bench.ancestor import clamp_to_baseline


def git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=repo, check=True, capture_output=True, text=True
    ).stdout.strip()


def commit(repo: Path, message: str) -> str:
    git(repo, "commit", "--allow-empty", "--quiet", "-m", message)
    return git(repo, "rev-parse", "HEAD")


@pytest.fixture
def history(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> tuple[Path, dict[str, str]]:
    """main: release -> baseline -> after, plus `old` branched from release."""
    # Git exports GIT_DIR to hooks and to `rebase --exec` in linked worktrees.
    # Left set, it redirects these commands and `clamp_to_baseline` to the
    # caller's repository, writing fixture commits and config there.
    for var in list(os.environ):
        if var.startswith("GIT_"):
            monkeypatch.delenv(var)
    git(tmp_path, "init", "--quiet")
    git(tmp_path, "config", "user.email", "test@example.com")
    git(tmp_path, "config", "user.name", "test")
    git(tmp_path, "config", "commit.gpgsign", "false")
    commits = {"release": commit(tmp_path, "release")}
    commits["baseline"] = commit(tmp_path, "baseline")
    commits["after"] = commit(tmp_path, "after")
    commits["head"] = commit(tmp_path, "head")
    git(tmp_path, "checkout", "--quiet", "-b", "old", commits["release"])
    commits["old"] = commit(tmp_path, "old")
    return tmp_path, commits


def test_ancestor_before_baseline_moves_to_baseline(
    history: tuple[Path, dict[str, str]],
) -> None:
    repo, c = history
    assert (
        clamp_to_baseline(c["release"], c["head"], repo, c["baseline"]) == c["baseline"]
    )


def test_ancestor_at_or_after_baseline_is_kept(
    history: tuple[Path, dict[str, str]],
) -> None:
    repo, c = history
    for ancestor in (c["baseline"], c["after"]):
        assert clamp_to_baseline(ancestor, c["head"], repo, c["baseline"]) == ancestor


def test_head_without_baseline_keeps_ancestor(
    history: tuple[Path, dict[str, str]],
) -> None:
    repo, c = history
    assert (
        clamp_to_baseline(c["release"], c["old"], repo, c["baseline"]) == c["release"]
    )


def test_unknown_baseline_raises(history: tuple[Path, dict[str, str]]) -> None:
    repo, c = history
    with pytest.raises(subprocess.CalledProcessError):
        clamp_to_baseline(c["release"], c["head"], repo, "0" * 40)
