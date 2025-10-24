# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Mark a minor release series as complete."""

import argparse
import sys

from materialize import git, spawn
from materialize.release.util import doc_file_path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("release_version")
    parser.add_argument("patch")
    args = parser.parse_args()

    remote = git.get_remote()

    print(f"Marking {args.release_version} as released in the docs...")
    release_version_doc_file = doc_file_path(args.release_version)
    release_version_doc_file.write_text(
        release_version_doc_file.read_text().replace(
            "released: false", f"released: true\npatch: {args.patch}"
        )
    )
    git.add_file(str(release_version_doc_file))
    git.commit_all_changed(f"release: mark {args.release_version} as released")

    print(f"Pushing to {remote}...")
    spawn.runv(["git", "push", remote, "main"])

    version = f"{args.release_version}.{args.patch}"
    print(f"Releasing self-managed weekly helm-chart {version}")
    spawn.runv(
        [
            "misc/helm-charts/publish-weekly.sh",
            "--version",
            version,
            "--remote",
            "origin",
        ]
    )


if __name__ == "__main__":
    sys.exit(main())
