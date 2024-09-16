#!/usr/bin/env python3

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
from materialize.rustc_flags import Sanitizer
from materialize.xcompile import Arch


def main() -> None:
    bazel = ui.env_is_truthy("CI_BAZEL_BUILD")
    bazel_remote_cache = os.getenv("CI_BAZEL_REMOTE_CACHE")

    mz_version = ci_util.get_mz_version()
    repos = [
        mzbuild.Repository(
            Path("."),
            Arch.X86_64,
            coverage=False,
            sanitizer=Sanitizer.none,
            bazel=bazel,
            bazel_remote_cache=bazel_remote_cache,
        ),
        mzbuild.Repository(
            Path("."),
            Arch.AARCH64,
            coverage=False,
            sanitizer=Sanitizer.none,
            bazel=bazel,
            bazel_remote_cache=bazel_remote_cache,
        ),
    ]
    print("--- Tagging development Docker images")
    deps = [
        repo.resolve_dependencies(image for image in repo if image.publish)
        for repo in repos
    ]
    # Ideally we'd use SemVer metadata (e.g., `v1.0.0+metadata`), but `+` is not
    # a valid character in Docker tags, so we use `--` instead.
    mzbuild.publish_multiarch_images(
        f'v{mz_version}--pr.g{git.rev_parse("HEAD")}', deps
    )


if __name__ == "__main__":
    main()
