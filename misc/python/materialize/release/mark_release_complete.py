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

import frontmatter

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
    metadata = frontmatter.load(release_version_doc_file)
    metadata["released"] = True
    metadata["patch"] = int(args.patch)
    with open(release_version_doc_file, "wb") as f:
        frontmatter.dump(metadata, f, sort_keys=False)

    git.add_file(str(release_version_doc_file))
    git.commit_all_changed(f"release: mark {args.release_version} as released")

    print(f"Pushing to {remote}...")
    spawn.runv(["git", "push", remote, "main"])


if __name__ == "__main__":
    sys.exit(main())
