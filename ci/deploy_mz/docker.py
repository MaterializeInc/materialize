# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from pathlib import Path

from materialize import mzbuild
from materialize.xcompile import Arch

from . import deploy_util
from .deploy_util import VERSION


def main() -> None:
    repos = [
        mzbuild.Repository(Path("."), Arch.X86_64, coverage=False),
        mzbuild.Repository(Path("."), Arch.AARCH64, coverage=False),
    ]

    print(f"--- Tagging Docker images")
    deps = [[repo.resolve_dependencies([repo.images["mz"]])["mz"]] for repo in repos]

    mzbuild.publish_multiarch_images(f"v{VERSION}", deps)
    if deploy_util.is_latest_version():
        mzbuild.publish_multiarch_images("latest", deps)


if __name__ == "__main__":
    main()
