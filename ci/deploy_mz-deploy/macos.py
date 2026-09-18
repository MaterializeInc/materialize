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

from ci.tarball_uploader import TarballUploader
from materialize import spawn
from materialize.xcompile import Arch

from ..deploy.deploy_util import rust_version
from . import deploy_util


def main() -> None:
    target = f"{Arch.host()}-apple-darwin"

    print("--- Checking version")
    version = deploy_util.mz_deploy_version()

    # No macOS mzbuild image exists, so this is the one target that still builds
    # from source at deploy time.
    print("--- Building mz-deploy")
    spawn.runv(
        ["cargo", "build", "--bin", "mz-deploy", "--release"],
        env=dict(os.environ, RUSTUP_TOOLCHAIN=rust_version()),
    )

    uploader = TarballUploader(
        package_name="mz-deploy",
        version=version,
    )

    uploader.deploy_tarball(
        target,
        Path("target") / "release" / "mz-deploy",
        update_latest=deploy_util.should_update_latest(version),
    )


if __name__ == "__main__":
    main()
