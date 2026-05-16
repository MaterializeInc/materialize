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
from materialize import mzbuild, spawn
from materialize.mz_version import MzCliVersion
from materialize.rustc_flags import Sanitizer

from . import deploy_util
from .deploy_util import APT_BUCKET, MZ_CLI_VERSION


def main() -> None:
    repo = mzbuild.Repository(
        Path("."),
        coverage=False,
        sanitizer=Sanitizer.none,
        image_registry="materialize",
    )
    target = f"{repo.rd.arch}-unknown-linux-gnu"

    print("--- Checking version")
    assert (
        MzCliVersion.parse_without_prefix(
            repo.rd.cargo_workspace.crates["mzx"].version_string
        )
        == MZ_CLI_VERSION
    )

    print("--- Building mzx")
    deps = repo.resolve_dependencies([repo.images["mzx"]])
    deps.ensure()
    # Extract the mzx binary from the Docker image.
    mz = repo.rd.cargo_target_dir() / "release" / "mzx"
    mz.parent.mkdir(parents=True, exist_ok=True)
    with open(mz, "wb") as f:
        spawn.runv(
            [
                "docker",
                "run",
                "--rm",
                "--entrypoint",
                "cat",
                deps["mzx"].spec(),
                "/usr/local/bin/mzx",
            ],
            stdout=f,
        )
    mzbuild.chmod_x(mz)

    print(f"--- Uploading {target} binary tarball")
    uploader = tarball_uploader.TarballUploader(
        package_name="mzx",
        version=deploy_util.MZ_CLI_VERSION,
    )
    uploader.deploy_tarball(target, mz)

    print("--- Publishing Debian package")
    filename = f"mzx_{MZ_CLI_VERSION.str_without_prefix()}_{repo.rd.arch.go_str()}.deb"
    print(f"Publishing {filename}")
    spawn.runv(
        [
            *repo.rd.build("deb", rustflags=[]),
            "--no-build",
            "--no-strip",
            "--deb-version",
            MZ_CLI_VERSION.str_without_prefix(),
            '--deb-revision=""',
            "-p",
            "mzx",
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
