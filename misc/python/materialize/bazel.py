# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import pathlib
import subprocess

from materialize import MZ_ROOT, ui

"""Utilities for interacting with Bazel from python scripts"""

# Path where we put the current revision of the repo that we can side channel
# into Bazel.
MZ_GIT_HASH_FILE = "/tmp/mz_git_hash.txt"


def output_paths(target, options=[]) -> list[pathlib.Path]:
    """Returns the absolute path of outputs from the built Bazel target."""

    cmd_args = ["bazel", "cquery", f"{target}", *options, "--output=files"]
    paths = subprocess.check_output(
        cmd_args, text=True, stderr=subprocess.DEVNULL
    ).splitlines()
    return [pathlib.Path(path) for path in paths]


def write_git_hash():
    """
    Temporary file where we write the current git hash, so we can side channel
    it into Bazel.

    For production releases we stamp builds with the `workspace_status_command`
    but this workflow is not friendly to remote caching. Specifically, the
    "volatile status" of a workspace is not supposed to cause builds to get
    invalidated, and it doesn't when the result is cached locally, but it does
    when it's cached remotely.

    See: <https://bazel.build/docs/user-manual#workspace-status>
         <https://github.com/bazelbuild/bazel/issues/10075>
    """

    repo = MZ_ROOT / ".git"
    cmd_args = ["git", f"--git-dir={repo}", "rev-parse", "HEAD"]
    result = subprocess.run(
        cmd_args, text=True, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL
    )

    if result.returncode == 0:
        with open(MZ_GIT_HASH_FILE, "w") as f:
            f.write(result.stdout.strip())
    else:
        ui.warn(f"Failed to get current revision of {MZ_ROOT}, falling back to all 0s")
