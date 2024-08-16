# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Start a new minor release series."""

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from materialize import MZ_ROOT, git, spawn
from materialize.mz_version import MzVersion

VERSION_FILE_TEXT = """---
title: "Materialize $VERSION"
date: $DATE
released: false
_build:
  render: never
---
"""


def main():
    remote = git.get_remote()
    latest_version = git.get_latest_version(version_type=MzVersion)
    release_version = latest_version.bump_minor()
    next_version = MzVersion.parse_mz(f"{release_version.bump_minor()}-dev")

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

    next_version_final = str(next_version).removesuffix(".0-dev")
    print(f"Creating {next_version_final}.md in the docs")
    today = datetime.today()
    next_thursday = today + timedelta(days=((3 - today.weekday()) % 7 or 7))
    next_version_doc_file = Path(
        MZ_ROOT / "doc" / "user" / "content" / "releases" / f"{next_version_final}.md"
    )
    if not next_version_doc_file.exists():
        text = VERSION_FILE_TEXT.replace("$VERSION", str(next_version_final)).replace(
            "$DATE", next_thursday.strftime("%Y-%m-%d")
        )
        next_version_doc_file.write_text(text)

    print(f"Pushing to {remote}...")
    spawn.runv(["git", "push", remote, "main"])

    github_output = os.getenv("GITHUB_OUTPUT")
    if github_output:
        print("Emitting GitHub output...")
        Path(github_output).write_text(
            "".join(
                [
                    f"release_version={release_version}\n",
                    f"next_version={next_version}\n",
                ]
            )
        )


if __name__ == "__main__":
    sys.exit(main())
