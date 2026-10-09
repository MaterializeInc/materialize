# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from pathlib import Path

from ci import tarball_uploader
from materialize import mzbuild, spawn
from materialize.rustc_flags import Sanitizer

from . import deploy_util


def main() -> None:
    repo = mzbuild.Repository(
        Path("."),
        coverage=False,
        sanitizer=Sanitizer.none,
        image_registry="materialize",
    )
    target = f"{repo.rd.arch}-unknown-linux-gnu"

    print("--- Checking version")
    version = deploy_util.mz_deploy_version(repo.rd.cargo_workspace)

    print("--- Extracting mz-deploy")
    # Take the binary out of the image the release build already produced rather
    # than recompiling, so the tarball ships exactly what CI tested.
    deps = repo.resolve_dependencies([repo.images["mz-deploy"]])
    deps.ensure()
    mz_deploy = repo.rd.cargo_target_dir() / "release" / "mz-deploy"
    mz_deploy.parent.mkdir(parents=True, exist_ok=True)
    with open(mz_deploy, "wb") as f:
        spawn.runv(
            [
                "docker",
                "run",
                "--rm",
                "--entrypoint",
                "cat",
                deps["mz-deploy"].spec(),
                "/usr/local/bin/mz-deploy",
            ],
            stdout=f,
        )
    mzbuild.chmod_x(mz_deploy)

    print(f"--- Uploading {target} binary tarball")
    uploader = tarball_uploader.TarballUploader(
        package_name="mz-deploy",
        version=version,
    )
    uploader.deploy_tarball(
        target, mz_deploy, update_latest=deploy_util.should_update_latest(version)
    )


if __name__ == "__main__":
    main()
