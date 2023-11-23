# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Git utilities."""

import functools
import subprocess
import sys
from pathlib import Path
from typing import TypeVar

from materialize import spawn
from materialize.mz_version import MzVersion, TypedVersionBase
from materialize.util import YesNoOnce
from materialize.version_list import INVALID_VERSIONS

VERSION_TYPE = TypeVar("VERSION_TYPE", bound=TypedVersionBase)

fetched_tags_in_remotes: set[str | None] = set()


def rev_count(rev: str) -> int:
    """Count the commits up to a revision.

    Args:
        rev: A Git revision in any format know to the Git CLI.

    Returns:
        count: The number of commits in the Git repository starting from the
            initial commit and ending with the specified commit, inclusive.
    """
    return int(spawn.capture(["git", "rev-list", "--count", rev, "--"]).strip())


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
    out = spawn.capture(["git", "rev-parse", *a, "--verify", rev]).strip()
    if not out:
        raise RuntimeError(f"No parsed rev for {rev}")
    return out


@functools.cache
def expand_globs(root: Path, *specs: Path | str) -> set[str]:
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
    )

    # `git ls-files --others --exclude-standard` surfaces any non-ignored,
    # untracked files, which are not included in the `git diff` output above.
    ls_files = spawn.capture(
        ["git", "ls-files", "--others", "--exclude-standard", "-z", "--", *specs],
        cwd=root,
    )

    return set(f for f in (diff_files + ls_files).split("\0") if f.strip() != "")


def get_version_tags(
    *, version_type: type[VERSION_TYPE], fetch: bool = True
) -> list[VERSION_TYPE]:
    """List all the version-like tags in the repo

    Args:
        fetch: If false, don't automatically run `git fetch --tags`.
        prefix: A prefix to strip from each tag before attempting to parse the
            tag as a version.
    """
    if fetch:
        _fetch(
            all_remotes=True, include_tags=YesNoOnce.ONCE, force=True, only_tags=True
        )
    tags = []
    for t in spawn.capture(["git", "tag"]).splitlines():
        if not t.startswith(version_type.get_prefix()):
            continue
        try:
            tags.append(version_type.parse_mz(t))
        except ValueError as e:
            print(f"WARN: {e}", file=sys.stderr)

    return sorted(tags, reverse=True)


def get_latest_version(
    version_type: type[VERSION_TYPE], excluded_versions: set[VERSION_TYPE] | None = None
) -> VERSION_TYPE:
    all_version_tags: list[VERSION_TYPE] = get_version_tags(
        version_type=version_type, fetch=True
    )

    if excluded_versions is not None:
        all_version_tags = [v for v in all_version_tags if v not in excluded_versions]

    return max(all_version_tags)


def get_tags_of_current_commit(include_tags: YesNoOnce = YesNoOnce.ONCE) -> list[str]:
    if include_tags:
        fetch(include_tags=include_tags, only_tags=True)

    result = spawn.capture(["git", "tag", "--points-at", "HEAD"])

    if len(result) == 0:
        return []

    return result.splitlines()


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


def first_remote_matching(pattern: str) -> str | None:
    """Get the name of the remote that matches the pattern"""
    remotes = spawn.capture(["git", "remote", "-v"])
    for remote in remotes.splitlines():
        if pattern in remote:
            return remote.split()[0]

    return None


def describe() -> str:
    """Describe the relationship between the current commit and the most recent tag"""
    return spawn.capture(["git", "describe"]).strip()


def fetch(
    remote: str | None = None,
    all_remotes: bool = False,
    include_tags: YesNoOnce = YesNoOnce.NO,
    force: bool = False,
    branch: str | None = None,
    only_tags: bool = False,
) -> str:
    """Fetch from remotes"""

    if remote is not None and all_remotes:
        raise RuntimeError("all_remotes must be false when a remote is specified")

    if branch is not None and remote is None:
        raise RuntimeError("remote must be specified when a branch is specified")

    if branch is not None and only_tags:
        raise RuntimeError("branch must not be specified if only_tags is set")

    command = ["git", "fetch"]

    if remote:
        command.append(remote)

    if branch:
        command.append(branch)

    if all_remotes:
        command.append("--all")

    fetch_tags = (
        include_tags == YesNoOnce.YES
        # fetch tags again if used with force (tags might have changed)
        or (include_tags == YesNoOnce.ONCE and force)
        or (
            include_tags == YesNoOnce.ONCE
            and remote not in fetched_tags_in_remotes
            and "*" not in fetched_tags_in_remotes
        )
    )

    if fetch_tags:
        command.append("--tags")

    if force:
        command.append("--force")

    if not fetch_tags and only_tags:
        return ""

    output = spawn.capture(command).strip()

    if fetch_tags:
        fetched_tags_in_remotes.add(remote)

        if all_remotes:
            fetched_tags_in_remotes.add("*")

    return output


_fetch = fetch  # renamed because an argument shadows the fetch name in get_tags


def try_get_remote_name_by_url(url: str) -> str | None:
    result = spawn.capture(["git", "remote", "--verbose"])
    for line in result.splitlines():
        remote, desc = line.split("\t")
        if desc.lower() == f"{url} (fetch)".lower():
            return remote
        if f"{desc.lower()}.git" == f"{url} (fetch)".lower():
            return remote
    return None


def get_remote(
    url: str = "https://github.com/MaterializeInc/materialize",
    default_remote_name: str = "origin",
) -> str:
    # Alternative syntax
    remote = try_get_remote_name_by_url(url) or try_get_remote_name_by_url(
        url.replace("https://github.com/", "git@github.com:")
    )
    if not remote:
        remote = default_remote_name
        print(f"Remote for URL {url} not found, using {remote}")

    return remote


def get_common_ancestor_commit(remote: str, branch: str, fetch_branch: bool) -> str:
    if fetch_branch:
        fetch(remote=remote, branch=branch)

    command = ["git", "merge-base", "HEAD", f"{remote}/{branch}"]
    return spawn.capture(command).strip()


def is_on_release_version() -> bool:
    git_tags = get_tags_of_current_commit()
    return any(MzVersion.is_valid_version_string(git_tag) for git_tag in git_tags)


def is_on_main_branch() -> bool:
    return get_branch_name() == "main"


def get_tagged_release_version(version_type: type[VERSION_TYPE]) -> VERSION_TYPE | None:
    """
    This returns the release version if exactly this commit is tagged.
    If multiple release versions are present, the highest one will be returned.
    None will be returned if the commit is not tagged.
    """
    git_tags = get_tags_of_current_commit()

    versions: list[VERSION_TYPE] = []

    for git_tag in git_tags:
        if version_type.is_valid_version_string(git_tag):
            versions.append(version_type.parse(git_tag))

    if len(versions) == 0:
        return None

    if len(versions) > 1:
        print(
            "Warning! Commit is tagged with multiple release versions! Returning the highest."
        )

    return max(versions)


def get_commit_message(commit_sha: str) -> str | None:
    try:
        command = ["git", "log", "-1", "--pretty=format:%s", commit_sha]
        return spawn.capture(command, stderr=subprocess.DEVNULL).strip()
    except subprocess.CalledProcessError:
        # Sometimes mz_version() will report a Git SHA that is not available
        # in the current repository
        return None


def get_branch_name() -> str:
    command = ["git", "branch", "--show-current"]
    return spawn.capture(command).strip()


def get_previous_version(
    version: VERSION_TYPE, excluded_versions: set[VERSION_TYPE] | None = None
) -> VERSION_TYPE:
    if excluded_versions is None:
        excluded_versions = set()

    if version.prerelease is not None and len(version.prerelease) > 0:
        # simply drop the prerelease, do not try to find a decremented version
        found_version = MzVersion.create(version.major, version.minor, version.patch)

        if found_version not in excluded_versions:
            return found_version
        else:
            # start searching with this version
            version = found_version

    all_versions: list[VERSION_TYPE] = get_version_tags(version_type=type(version))
    all_suitable_previous_versions = [
        v
        for v in all_versions
        if v < version
        and (v.prerelease is None or len(v.prerelease) == 0)
        and (not isinstance(version, MzVersion) or v not in INVALID_VERSIONS)
        and v not in excluded_versions
    ]
    return max(all_suitable_previous_versions)


# Work tree mutation


def create_branch(name: str) -> None:
    spawn.runv(["git", "checkout", "-b", name])


def checkout(rev: str, branch: str | None = None) -> None:
    """Git checkout the rev"""
    spawn.runv(["git", "checkout", rev])


def commit_all_changed(message: str) -> None:
    """Commit all changed files with the given message"""
    spawn.runv(["git", "commit", "-a", "-m", message])


def tag_annotated(tag: str) -> None:
    """Create an annotated tag on HEAD"""
    spawn.runv(["git", "tag", "-a", "-m", tag, tag])


def push(remote: str, remote_ref: str | None = None) -> None:
    if remote_ref:
        spawn.runv(["git", "push", remote, f"HEAD:{remote_ref}"])
    else:
        spawn.runv(["git", "push", remote])
