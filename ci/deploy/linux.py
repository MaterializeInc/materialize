# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import boto3
import io
import os
import subprocess
from pathlib import Path

import semver

from materialize import cargo
from materialize import ci_util
from materialize import deb
from materialize import git
from materialize import mzbuild
from materialize import spawn
from . import deploy_util
from .deploy_util import apt_materialized_path, APT_BUCKET


def main() -> None:
    repo = mzbuild.Repository(Path("."))
    workspace = cargo.Workspace(repo.root)
    buildkite_tag = os.environ["BUILDKITE_TAG"]

    print(f"--- Publishing Debian package")
    if buildkite_tag:
        version = workspace.crates["materialized"].version
        if version.prerelease is None:
            publish_deb("materialized", str(version))
        else:
            print(f"Detected prerelease version ({version}); skipping...")
    else:
        publish_deb("materialized-unstable", deb.unstable_version(workspace))

    print(f"--- Tagging Docker images")
    deps = repo.resolve_dependencies(image for image in repo if image.publish)
    deps.acquire()

    if buildkite_tag:
        # On tag builds, always tag the images as such.
        deps.push_tagged(buildkite_tag)

        # Also tag the images as `latest` if this is the latest version.
        version = semver.VersionInfo.parse(buildkite_tag.lstrip("v"))
        latest_version = next(t for t in git.get_version_tags() if t.prerelease is None)
        if version == latest_version:
            deps.push_tagged("latest")

    print("--- Uploading binary tarball")
    mz_path = Path("materialized")
    ci_util.acquire_materialized(repo, mz_path)
    deploy_util.deploy_tarball("x86_64-unknown-linux-gnu", mz_path)


def publish_deb(package: str, version: str) -> None:
    print(f"{package} v{version}")

    # Download the staged package, as deb-s3 needs various metadata from it.
    boto3.client("s3").download_file(
        APT_BUCKET, apt_materialized_path(version), f"materialized-{version}.deb"
    )

    # Import our GPG signing key from the environment.
    spawn.runv(["gpg", "--import"], stdin=os.environ["GPG_KEY"].encode("ascii"))

    # Run deb-s3 to update the repository. No need to upload the file again.
    spawn.runv(
        [
            "deb-s3",
            "upload",
            "-p",
            "--skip-package-upload",
            "--sign",
            "-b",
            APT_BUCKET,
            "-c",
            "generic",
            f"./materialized-{version}.deb",
        ]
    )


if __name__ == "__main__":
    main()
