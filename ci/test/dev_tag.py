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

from materialize import ci_util, git, mzbuild
from materialize.rustc_flags import Sanitizer
from materialize.xcompile import Arch


def main() -> None:
    mz_version = ci_util.get_mz_version()
    sanitizer = Sanitizer[os.getenv("CI_SANITIZER", "none")]

    repos = [
        mzbuild.Repository(
            Path("."),
            Arch.X86_64,
            coverage=False,
            sanitizer=sanitizer,
        ),
        mzbuild.Repository(
            Path("."),
            Arch.AARCH64,
            coverage=False,
            sanitizer=sanitizer,
        ),
    ]
    print("--- Tagging development Docker images")
    deps = [
        repo.resolve_dependencies(image for image in repo if image.publish)
        for repo in repos
    ]
    # Ideally we'd use SemVer metadata (e.g., `v1.0.0+metadata`), but `+` is not
    # a valid character in Docker tags, so we use `--` instead.
    suffix = "pr" if sanitizer == Sanitizer.none else f"pr-{sanitizer}"
    mzbuild.publish_multiarch_images(
        f'v{mz_version}--{suffix}.g{git.rev_parse("HEAD")}', deps
    )


if __name__ == "__main__":
    main()
