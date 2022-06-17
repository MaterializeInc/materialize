# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import os
from pathlib import Path

import semver

from materialize import git, mzbuild
from materialize.xcompile import Arch


def main() -> None:
    repos = [
        mzbuild.Repository(Path("."), Arch.X86_64),
        mzbuild.Repository(Path("."), Arch.AARCH64),
    ]
    buildkite_tag = os.environ["BUILDKITE_TAG"]

    print(f"--- Tagging Docker images")
    deps = [
        repo.resolve_dependencies(image for image in repo if image.publish)
        for repo in repos
    ]

    if buildkite_tag:
        # On tag builds, always tag the images as such.
        mzbuild.publish_multiarch_images(buildkite_tag, deps)

        # Also tag the images as `latest` if this is the latest version.
        version = semver.VersionInfo.parse(buildkite_tag.lstrip("v"))
        latest_version = next(t for t in git.get_version_tags() if t.prerelease is None)
        if version == latest_version:
            mzbuild.publish_multiarch_images("latest", deps)
    else:
        mzbuild.publish_multiarch_images("unstable", deps)
        mzbuild.publish_multiarch_images(f'unstable-{git.rev_parse("HEAD")}', deps)


if __name__ == "__main__":
    main()
