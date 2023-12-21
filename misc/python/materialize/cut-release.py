# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Cut a new release and push it to the Materialize repo"""

import argparse
import sys

from semver.version import Version

from materialize import MZ_ROOT, spawn
from materialize.git import checkout, get_branch_name, tag_annotated


def main():
    parser = argparse.ArgumentParser(
        prog="cut_release",
        description="Creates a new release for Materialize.",
    )
    parser.add_argument(
        "--sha",
        help="Chosen SHA of the release",
        type=str,
        required=True,
    )
    parser.add_argument(
        "--version",
        help="Version of release",
        type=Version.parse,
        required=True,
    )
    parser.add_argument(
        "--remote",
        help="Git remote name of Materialize repo",
        type=str,
        required=True,
    )

    args = parser.parse_args()
    version = f"v{args.version}"
    current_branch = get_branch_name()

    try:
        print(f"Checking out SHA {args.sha}")
        checkout(args.sha)
        print(f"Bumping version to {version}")
        spawn.runv([MZ_ROOT / "bin" / "bump-version", version])
        print("Tagging version")
        tag_annotated(version)
        print("Pushing tag to Materialize repo")
        spawn.runv(["git", "push", args.remote, version])
    finally:
        # The caller may have started in a detached HEAD state.
        if current_branch:
            checkout(current_branch)


if __name__ == "__main__":
    sys.exit(main())
