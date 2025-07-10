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

from ci import tarball_uploader
from materialize import mzbuild, ui
from materialize.rustc_flags import Sanitizer
from materialize.xcompile import Arch

from .deploy_util import MZ_CLI_VERSION


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

    print("--- Tagging Docker images")
    deps = [[repo.resolve_dependencies([repo.images["mz"]])["mz"]] for repo in repos]

    mzbuild.publish_multiarch_images(f"v{MZ_CLI_VERSION.str_without_prefix()}", deps)
    if tarball_uploader.is_latest_version(MZ_CLI_VERSION):
        mzbuild.publish_multiarch_images("latest", deps)


if __name__ == "__main__":
    main()
