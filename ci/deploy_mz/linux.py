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

from materialize import mzbuild, spawn

from . import deploy_util
from .deploy_util import APT_BUCKET, VERSION


def main() -> None:
    repo = mzbuild.Repository(Path("."), coverage=False)
    target = f"{repo.rd.arch}-unknown-linux-gnu"

    print(f"--- Checking version")
    assert repo.rd.cargo_workspace.crates["mz"].version == VERSION

    print(f"--- Building mz")
    deps = repo.resolve_dependencies([repo.images["mz"]])
    deps.ensure()
    # Extract the mz binary from the Docker image.
    mz = repo.rd.cargo_target_dir() / "release" / "mz"
    mz.parent.mkdir(parents=True, exist_ok=True)
    with open(mz, "wb") as f:
        spawn.runv(
            [
                "docker",
                "run",
                "--rm",
                "--entrypoint",
                "cat",
                deps["mz"].spec(),
                "/usr/local/bin/mz",
            ],
            stdout=f,
        )
    mzbuild.chmod_x(mz)

    print(f"--- Uploading {target} binary tarball")
    deploy_util.deploy_tarball(target, mz)

    print(f"--- Publishing Debian package")
    filename = f"mz_{VERSION}_{repo.rd.arch.go_str()}.deb"
    print(f"Publishing {filename}")
    spawn.runv(
        [
            *repo.rd.cargo("deb", rustflags=[]),
            "--no-build",
            "--no-strip",
            "--deb-version",
            str(VERSION),
            "-p",
            "mz",
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
