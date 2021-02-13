# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Git utilities."""

import subprocess
import sys
from functools import lru_cache, total_ordering
from pathlib import Path
from typing import List, Optional, Set, Union, NamedTuple

import semver

from materialize import spawn
from materialize import errors


def rev_count(rev: str) -> int:
    """Count the commits up to a revision.

    Args:
        rev: A Git revision in any format know to the Git CLI.

    Returns:
        count: The number of commits in the Git repository starting from the
            initial commit and ending with the specified commit, inclusive.
    """
    return int(
        spawn.capture(["git", "rev-list", "--count", rev, "--"], unicode=True).strip()
    )


def rev_parse(rev: str, *, abbrev: bool = False) -> str:
    """Compute the hash for a revision.

    Args:
        rev: A Git revision in any format known to the Git CLI.
        abbrev: Return a branch or tag name instead of a git sha

    Returns:
        ref: A 40 character hex-encoded SHA-1 hash representing the ID of the
            named revision in Git's object database.

            With "abbrev=True" this will return an abbreviated ref, or throw an
            error if there is no abbrev.
    """
    a = ["--abbrev-ref"] if abbrev else []
    out = spawn.capture(["git", "rev-parse", *a, "--verify", rev], unicode=True).strip()
    if not out:
        raise errors.MzRuntimeError(f"No parsed rev for {rev}")
    return out


@lru_cache(maxsize=None)
def expand_globs(root: Path, *specs: Union[Path, str]) -> Set[str]:
    """Find unignored files within the specified paths."""
    # The goal here is to find all files in the working tree that are not
    # ignored by .gitignore. Naively using `git ls-files` doesn't work, because
    # it reports files that have been deleted in the working tree if they are
    # still present in the index. Using `os.walkdir` doesn't work because there
    # is no good way to evaluate .gitignore rules from Python. So we use a
    # combination of `git diff` and `git ls-files`.

    # `git diff` against the empty tree surfaces all tracked files that have
    # not been deleted.
    empty_tree = (
        "4b825dc642cb6eb9a060e54bf8d69288fbee4904"  # git hash-object -t tree /dev/null
    )
    diff_files = spawn.capture(
        ["git", "diff", "--name-only", "-z", empty_tree, "--", *specs],
        cwd=root,
        unicode=True,
    )

    # `git ls-files --others --exclude-standard` surfaces any non-ignored,
    # untracked files, which are not included in the `git diff` output above.
    ls_files = spawn.capture(
        ["git", "ls-files", "--others", "--exclude-standard", "-z", "--", *specs],
        cwd=root,
        unicode=True,
    )

    return set(f for f in (diff_files + ls_files).split("\0") if f.strip() != "")


def get_version_tags(*, fetch: bool = True) -> List[semver.VersionInfo]:
    """List all the version-like tags in the repo

    Args:
        fetch: If false, don't automatically run `git fetch --tags`.
    """
    if fetch:
        spawn.runv(["git", "fetch", "--tags"])
    tags = []
    for t in spawn.capture(["git", "tag"], unicode=True).splitlines():
        try:
            tags.append(semver.VersionInfo.parse(t.lstrip("v")))
        except ValueError as e:
            print(f"WARN: {e}", file=sys.stderr)

    return sorted(tags, reverse=True)


def is_ancestor(earlier: str, later: str) -> bool:
    """True if earlier is in an ancestor of later"""
    try:
        spawn.capture(["git", "merge-base", "--is-ancestor", earlier, later])
    except subprocess.CalledProcessError:
        return False
    return True


def is_dirty() -> bool:
    """Check if the working directory has modifications to tracked files"""
    proc = subprocess.run("git diff --no-ext-diff --quiet --exit-code".split())
    idx = subprocess.run("git diff --cached --no-ext-diff --quiet --exit-code".split())
    return proc.returncode != 0 or idx.returncode != 0


def first_remote_matching(pattern: str) -> Optional[str]:
    """Get the name of the remote that matches the pattern"""
    remotes = spawn.capture(["git", "remote", "-v"], unicode=True)
    for remote in remotes.splitlines():
        if pattern in remote:
            return remote.split()[0]

    return None


def describe() -> str:
    """Describe the relationship between the current commit and the most recent tag"""
    return spawn.capture(["git", "describe"], unicode=True).strip()


# Work tree mutation


def create_branch(name: str) -> None:
    spawn.runv(["git", "checkout", "-b", name])


def checkout(rev: str, branch: Optional[str] = None) -> None:
    """Git checkout the rev"""
    spawn.runv(["git", "checkout", rev])


def commit_all_changed(message: str) -> None:
    """Commit all changed files with the given message"""
    spawn.runv(["git", "commit", "-a", "-m", message])


def tag_annotated(tag: str) -> None:
    """Create an annotated tag on HEAD"""
    spawn.runv(["git", "tag", "-a", "-m", tag, tag])
