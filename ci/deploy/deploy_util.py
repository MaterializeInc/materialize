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

from materialize import MZ_ROOT, cargo, mzbuild
from materialize.rustc_flags import Sanitizer
from materialize.xcompile import Arch


def rust_version() -> str:
    rust_version = cargo.Workspace(MZ_ROOT).rust_version
    assert rust_version is not None, "workspace missing rust version configuration"
    return rust_version


def release_repos() -> list[mzbuild.Repository]:
    return [
        mzbuild.Repository(
            Path("."),
            arch,
            coverage=False,
            sanitizer=Sanitizer.none,
            image_registry="materialize",
        )
        for arch in (Arch.X86_64, Arch.AARCH64)
    ]


def publishable_image_deps(
    repos: list[mzbuild.Repository],
) -> list[mzbuild.DependencySet]:
    """Return, per repository, the images this build tags on Docker Hub."""
    buildkite_tag = os.environ["BUILDKITE_TAG"]

    def include_image(image: mzbuild.Image) -> bool:
        # Images must always be publishable to be tagged. Only mainline images
        # get tagged for releases, but even non-mainline images get `unstable`
        # tags.
        return image.publish and (not buildkite_tag or image.mainline)

    return [
        repo.resolve_dependencies(image for image in repo if include_image(image))
        for repo in repos
    ]
