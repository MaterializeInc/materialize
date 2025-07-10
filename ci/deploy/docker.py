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

from materialize import ci_util, git, mzbuild, ui
from materialize.mz_version import MzVersion
from materialize.rustc_flags import Sanitizer
from materialize.version_list import get_all_mz_versions
from materialize.xcompile import Arch


def main() -> None:
    bazel = ui.env_is_truthy("CI_BAZEL_BUILD")
    bazel_remote_cache = os.getenv("CI_BAZEL_REMOTE_CACHE")
    bazel_lto = ui.env_is_truthy("CI_BAZEL_LTO")

    repos = [
        mzbuild.Repository(
            Path("."),
            Arch.X86_64,
            coverage=False,
            sanitizer=Sanitizer.none,
            bazel=bazel,
            bazel_remote_cache=bazel_remote_cache,
            bazel_lto=bazel_lto,
        ),
        mzbuild.Repository(
            Path("."),
            Arch.AARCH64,
            coverage=False,
            sanitizer=Sanitizer.none,
            bazel=bazel,
            bazel_remote_cache=bazel_remote_cache,
            bazel_lto=bazel_lto,
        ),
    ]
    buildkite_tag = os.environ["BUILDKITE_TAG"]

    def include_image(image: mzbuild.Image) -> bool:
        # Images must always be publishable to be tagged. Only mainline images
        # get tagged for releases, but even non-mainline images get `unstable`
        # tags.
        return image.publish and (not buildkite_tag or image.mainline)

    print("--- Tagging Docker images")
    deps = [
        repo.resolve_dependencies(image for image in repo if include_image(image))
        for repo in repos
    ]

    if buildkite_tag:
        # On tag builds, always tag the images as such.
        mzbuild.publish_multiarch_images(buildkite_tag, deps)

        # Also tag the images as `latest` if this is the latest version.
        version = MzVersion.parse_mz(buildkite_tag)
        latest_version = max(t for t in get_all_mz_versions() if t.prerelease is None)
        if version == latest_version:
            mzbuild.publish_multiarch_images("latest", deps)
    else:
        mz_version = ci_util.get_mz_version()
        mzbuild.publish_multiarch_images("unstable", deps)
        # Ideally we'd use SemVer metadata (e.g., `v1.0.0+metadata`), but `+`
        # is not a valid character in Docker tags, so we use `--` instead.
        mzbuild.publish_multiarch_images(
            f'v{mz_version}--main.g{git.rev_parse("HEAD")}', deps
        )

        # Sync image descriptions to Docker Hub. The image descriptions are the
        # same across architectures, so we arbitrarily choose the first
        # repository.
        for image in repos[0]:
            image.sync_description()


if __name__ == "__main__":
    main()
