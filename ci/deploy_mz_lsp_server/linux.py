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
from ci.deploy.deploy_util import rust_version
from materialize import mzbuild, spawn
from materialize.mz_version import MzLspServerVersion
from materialize.rustc_flags import Sanitizer

from . import deploy_util
from .deploy_util import MZ_LSP_SERVER_VERSION


def main() -> None:
    repo = mzbuild.Repository(
        Path("."),
        coverage=False,
        sanitizer=Sanitizer.none,
    )
    target = f"{repo.rd.arch}-unknown-linux-gnu"

    print("--- Checking version")
    assert (
        MzLspServerVersion.parse_without_prefix(
            repo.rd.cargo_workspace.crates["mz-lsp-server"].version_string
        )
        == MZ_LSP_SERVER_VERSION
    )

    print("--- Building mz-lsp-server")
    # The bin/ci-builder uses target-xcompile as the volume and
    # is where the binary release will be available.
    path = Path("target-xcompile") / "release" / "mz-lsp-server"
    spawn.runv(
        ["cargo", "build", "--bin", "mz-lsp-server", "--release"],
        env=dict(os.environ, RUSTUP_TOOLCHAIN=rust_version()),
    )
    mzbuild.chmod_x(path)

    print(f"--- Uploading {target} binary tarball")
    uploader = tarball_uploader.TarballUploader(
        package_name="mz-lsp-server",
        version=deploy_util.MZ_LSP_SERVER_VERSION,
    )
    uploader.deploy_tarball(target, path)


if __name__ == "__main__":
    main()
