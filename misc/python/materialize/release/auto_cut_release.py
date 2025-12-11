# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Start a new minor release series."""

import argparse
import sys

from materialize import MZ_ROOT, git, spawn
from materialize.mz_version import MzVersion
from materialize.release.util import doc_file_path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("release_version")
    parser.add_argument("next_version")
    parser.add_argument("next_date")
    args = parser.parse_args()

    remote = git.get_remote()
    latest_version = git.get_latest_version(version_type=MzVersion)
    release_version = MzVersion.parse_mz(f"{args.release_version}.0-rc.1")
    next_version = MzVersion.parse_mz(f"{args.next_version}.0-dev.0")

    if latest_version >= release_version:
        print(
            f"Latest version ({latest_version}) is greater than or equal to release version ({release_version}); nothing to do"
        )
        return 0

    print("Pulling latest main...")
    spawn.runv(["git", "pull", remote, "main"])

    print("Creating temporary release branch...")
    git.create_branch(f"release-{release_version}")

    print(f"Bumping version to {release_version}...")
    spawn.runv([MZ_ROOT / "bin" / "bump-version", str(release_version)])

    print("Tagging release...")
    git.tag_annotated(str(release_version))

    print(f"Pushing release tag ({release_version}) to {remote}...")
    spawn.runv(["git", "push", remote, str(release_version)])

    print("Checking out main...")
    git.checkout("main")

    print(f"Bumping version on main to {next_version}...")
    spawn.runv([MZ_ROOT / "bin" / "bump-version", str(next_version)])

    # Create the release page in the docs for the next version after the one
    # that was just cut.
    print(f"Creating {args.next_version}.md in the docs...")
    next_version_doc_file = doc_file_path(args.next_version)
    if not next_version_doc_file.exists():
        next_version_doc_file.write_text(
            f"""---
title: Materialize {args.next_version}
date: {args.next_date}
released: false
patch: 0
rc: 1
publish_helm_chart: true
_build:
  render: never
---
"""
        )
        git.add_file(str(next_version_doc_file))
        git.commit_all_changed(f"release: create doc file for {args.next_version}")

    print(f"Pushing to {remote}...")
    spawn.runv(["git", "push", remote, "main"])

    return 0


if __name__ == "__main__":
    sys.exit(main())
