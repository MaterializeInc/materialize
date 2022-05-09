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
import semver

from materialize import cargo, ci_util, deb, git, mzbuild, spawn
from materialize.xcompile import Arch

from . import deploy_util
from .deploy_util import APT_BUCKET, apt_materialized_path


def main() -> None:
    repos = [
        mzbuild.Repository(Path("."), Arch.X86_64),
        mzbuild.Repository(Path("."), Arch.AARCH64),
    ]
    workspace = cargo.Workspace(Path("."))
    buildkite_tag = os.environ["BUILDKITE_TAG"]

    print(f"--- Publishing Debian package")
    if buildkite_tag:
        version = workspace.crates["materialized"].version
        if version.prerelease is None:
            for repo in repos:
                publish_deb(repo.rd.arch, str(version))
        else:
            print(f"Detected prerelease version ({version}); skipping...")
    else:
        for repo in repos:
            publish_deb(repo.rd.arch, deb.unstable_version(workspace))

    print(f"--- Tagging Docker images")
    deps = [
        repo.resolve_dependencies(image for image in repo if image.publish)
        for repo in repos
    ]

    if buildkite_tag:
        # On tag builds, always tag the images as such.
        mzbuild.publish_multiarch_images(buildkite_tag, deps)

        # Also tag the images as `latest` if this is the latest version.
        version = semver.VersionInfo.parse(buildkite_tag.lstrip("v"))
        latest_version = next(t for t in git.get_version_tags() if t.prerelease is None)
        if version == latest_version:
            mzbuild.publish_multiarch_images("latest", deps)
    else:
        mzbuild.publish_multiarch_images("unstable", deps)
        mzbuild.publish_multiarch_images(f'unstable-{git.rev_parse("HEAD")}', deps)

    print("--- Uploading binary tarball")
    for repo in repos:
        mz_path = Path(f"materialized-{repo.rd.arch}")
        ci_util.acquire_materialized(repo, mz_path)
        deploy_util.deploy_tarball(f"{repo.rd.arch}-unknown-linux-gnu", mz_path)


def publish_deb(arch: Arch, version: str) -> None:
    filename = f"materialized_{version}_{arch.go_str()}.deb"
    print(f"Publishing {filename}")

    # Download the staged package, as deb-s3 needs various metadata from it.
    boto3.client("s3").download_file(
        Bucket=APT_BUCKET,
        Key=apt_materialized_path(arch, version),
        Filename=filename,
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
            filename,
        ]
    )


if __name__ == "__main__":
    main()
