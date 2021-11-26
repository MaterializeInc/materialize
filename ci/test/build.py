#!/usr/bin/env python3

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

import boto3

from materialize import cargo, ci_util, deb, mzbuild, spawn

from ..deploy.deploy_util import APT_BUCKET, apt_materialized_path


def main() -> None:
    repo = mzbuild.Repository(Path("."))
    workspace = cargo.Workspace(repo.root)

    # Acquire all the mzbuild images in the repository, while pushing any
    # images that we build to Docker Hub, where they will be accessible to
    # other build agents.
    print("--- Acquiring mzbuild images")
    deps = repo.resolve_dependencies(image for image in repo if image.publish)
    deps.acquire()
    deps.push()

    print("--- Staging Debian package")
    if os.environ["BUILDKITE_BRANCH"] == "main":
        stage_deb(repo, "materialized-unstable", deb.unstable_version(workspace))
    elif os.environ["BUILDKITE_TAG"]:
        version = workspace.crates["materialized"].version
        assert (
            f"v{version}" == os.environ["BUILDKITE_TAG"]
        ), f'materialized version {version} does not match tag {os.environ["BUILDKITE_TAG"]}'
        stage_deb(repo, "materialized", str(version))
    else:
        print("Not on main branch or tag; skipping")


def stage_deb(repo: mzbuild.Repository, package: str, version: str) -> None:
    """Stage a Debian package on S3.

    Note that this function does not cause anything to become public; a
    step to publish the files and add them to the apt packages index
    will be run during the deploy job.
    """

    print(f"Staging deb {package} {version}")

    # Extract the materialized binary from the Docker image. This avoids
    # an expensive rebuild if we're using a cached image.
    ci_util.acquire_materialized(
        repo, repo.rd.cargo_target_dir() / "release" / "materialized"
    )

    # Build the Debian package.
    deb_path = repo.rd.cargo_target_dir() / "debian" / f"materialized_{version}.deb"
    spawn.runv(
        [
            *repo.rd.cargo("deb", rustflags=[]),
            f"--variant={package}",
            "--no-build",
            "--no-strip",
            "--deb-version",
            version,
            "-p",
            "materialized",
            "-o",
            deb_path,
        ],
        cwd=repo.root,
    )

    # Stage the package on S3
    boto3.client("s3").upload_file(
        Filename=str(deb_path),
        Bucket=APT_BUCKET,
        Key=apt_materialized_path(repo.rd.arch, version),
    )


if __name__ == "__main__":
    main()
