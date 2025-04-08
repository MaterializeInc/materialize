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
from materialize import spawn
from materialize.xcompile import Arch

from ..deploy.deploy_util import rust_version
from . import deploy_util


def main() -> None:
    target = f"{Arch.host()}-apple-darwin"

    print("--- Building mz-lsp-server")
    spawn.runv(
        ["cargo", "build", "--bin", "mz-lsp-server", "--release"],
        env=dict(os.environ, RUSTUP_TOOLCHAIN=rust_version()),
    )

    print(f"--- Uploading {target} binary tarball")
    uploader = tarball_uploader.TarballUploader(
        package_name="mz-lsp-server",
        version=deploy_util.MZ_LSP_SERVER_VERSION,
    )
    uploader.deploy_tarball(target, Path("target") / "release" / "mz-lsp-server")


if __name__ == "__main__":
    main()
