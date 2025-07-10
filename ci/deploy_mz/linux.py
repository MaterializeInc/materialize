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
from materialize import mzbuild, spawn, ui
from materialize.mz_version import MzCliVersion
from materialize.rustc_flags import Sanitizer

from . import deploy_util
from .deploy_util import APT_BUCKET, MZ_CLI_VERSION


def main() -> None:
    bazel = ui.env_is_truthy("CI_BAZEL_BUILD")
    bazel_remote_cache = os.getenv("CI_BAZEL_REMOTE_CACHE")
    bazel_lto = ui.env_is_truthy("CI_BAZEL_LTO")

    repo = mzbuild.Repository(
        Path("."),
        coverage=False,
        sanitizer=Sanitizer.none,
        bazel=bazel,
        bazel_remote_cache=bazel_remote_cache,
        bazel_lto=bazel_lto,
    )
    target = f"{repo.rd.arch}-unknown-linux-gnu"

    print("--- Checking version")
    assert (
        MzCliVersion.parse_without_prefix(
            repo.rd.cargo_workspace.crates["mz"].version_string
        )
        == MZ_CLI_VERSION
    )

    print("--- Building mz")
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
    uploader = tarball_uploader.TarballUploader(
        package_name="mz",
        version=deploy_util.MZ_CLI_VERSION,
    )
    uploader.deploy_tarball(target, mz)

    print("--- Publishing Debian package")
    filename = f"mz_{MZ_CLI_VERSION.str_without_prefix()}_{repo.rd.arch.go_str()}.deb"
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
