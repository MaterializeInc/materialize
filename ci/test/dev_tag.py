#!/usr/bin/env python3

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from pathlib import Path

from materialize import git, mzbuild
from materialize.xcompile import Arch


def main() -> None:
    repos = [
        mzbuild.Repository(Path("."), Arch.X86_64),
        mzbuild.Repository(Path("."), Arch.AARCH64),
    ]
    print(f"--- Tagging development Docker images")
    deps = [
        repo.resolve_dependencies(image for image in repo if image.publish)
        for repo in repos
    ]
    mzbuild.publish_multiarch_images(f'devel-{git.rev_parse("HEAD")}', deps)


if __name__ == "__main__":
    main()
