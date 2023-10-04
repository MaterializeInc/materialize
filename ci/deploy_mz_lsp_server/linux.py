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

from ci.deploy.deploy_util import rust_version
from materialize import mzbuild, spawn

from . import deploy_util
from .deploy_util import APT_BUCKET, VERSION


def main() -> None:
    repo = mzbuild.Repository(Path("."), coverage=False)
    target = f"{repo.rd.arch}-unknown-linux-gnu"

    print("--- Checking version")
    assert repo.rd.cargo_workspace.crates["mz-lsp-server"].version == VERSION

    print("--- Building mz-lsp-server")
    print(f"Roost toolchain: {rust_version()}")
    spawn.runv(
        ["cargo", "build", "--bin", "mz-lsp-server", "--release"],
        env=dict(os.environ, RUSTUP_TOOLCHAIN=rust_version()),
    )

    print(f"--- Uploading {target} binary tarball")
    deploy_util.deploy_tarball(target, Path("target") / "release" / "mz-lsp-server")

    print("--- Publishing Debian package")
    filename = f"mz-lsp-server_{VERSION}_{repo.rd.arch.go_str()}.deb"
    print(f"Publishing {filename}")
    spawn.runv(
        [
            *repo.rd.cargo("deb", rustflags=[]),
            "--no-build",
            "--no-strip",
            "--deb-version",
            str(VERSION),
            "-p",
            "mz-lsp-server",
            "-o",
            filename,
        ],
        cwd=repo.root,
    )
    # Import our GPG signing key from the environment.
    spawn.runv(["gpg", "--import"], stdin=os.environ["GPG_KEY"].encode("ascii"))
    # Run deb-s3 to update the repository. No need to upload the file again.
    spawn.runv(
        [
            "deb-s3",
            "upload",
            "-p",
            "--sign",
            "-b",
            APT_BUCKET,
            "-c",
            "generic",
            filename,
        ]
    )


if __name__ == "__main__":
    main()
