# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Cut a new release and push it to the Materialize repo"""

import sys

from materialize import MZ_ROOT, spawn
from materialize.git import get_latest_version, get_remote, tag_annotated
from materialize.mz_version import MzVersion


def main():
    latest_version = get_latest_version(version_type=MzVersion)
    version = str(latest_version.bump_minor())
    print(f"Bumping version to {version}")
    spawn.runv([MZ_ROOT / "bin" / "bump-version", version])
    print("Tagging version")
    tag_annotated(version)
    print("Pushing tag to Materialize repo")
    remote = get_remote()
    print(f"Would push {version} to {remote}")
    #spawn.runv(["git", "push", remote, version])

if __name__ == "__main__":
    sys.exit(main())
