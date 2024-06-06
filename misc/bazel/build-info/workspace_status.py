# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import subprocess
import sys

"""
Script executed by Bazel before every build via [`workspace_status_command`]
(https://bazel.build/docs/user-manual#workspace-status).

This script is run outside of the Bazel sandbox, so a system must have all of
the required dependencies pre-installed. Changes to this script that depend on
anything other than the Python standard library should be avoided.
"""

GIT_COMMIT_HASH_KEY_NAME = "GIT_COMMIT_HASH"
BUILD_TIME_KEY_NAME = "MZ_BUILD_TIME"


def main():
    git_hash = get_git_hash(".")
    build_time = get_build_time()

    # Bazel expects this program to print zero or more key value pairs to
    # stdout, one per-line. The first space after the key separates the key
    # names from the values, the rest of the line is considered to be the
    # value.
    #
    # There are two kinds of values Bazel supports "stable" and "volatile",
    # read the docs for more info.
    #
    # <https://bazel.build/docs/user-manual#workspace-status>

    print(f"{GIT_COMMIT_HASH_KEY_NAME} {git_hash}")
    print(f"{BUILD_TIME_KEY_NAME} {build_time}")


def get_git_hash(path):
    p = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=path, stdout=subprocess.PIPE, text=True
    )
    if p.returncode != 0:
        sys.exit(p.returncode)
    return p.stdout.strip()


def get_build_time():
    p = subprocess.run(
        ["date", "-u", "+%Y-%m-%dT%H:%M:%SZ"], stdout=subprocess.PIPE, text=True
    )
    if p.returncode != 0:
        sys.exit(p.returncode)
    return p.stdout.strip()


if __name__ == "__main__":
    main()
