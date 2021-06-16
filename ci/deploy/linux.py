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
    if buildkite_tag:
        tag_docker(repo, buildkite_tag)
        tag_docker_latest_maybe(repo, buildkite_tag)
    else:
        tag_docker(repo, f'unstable-{git.rev_parse("HEAD")}')
        tag_docker(repo, "unstable")

    print("--- Uploading binary tarball")
    mz_path = Path("materialized")
    ci_util.acquire_materialized(repo, mz_path)
    deploy_util.deploy_tarball("x86_64-unknown-linux-gnu", mz_path)


def publish_deb(package: str, version: str) -> None:
    print(f"{package} v{version}")

    s3 = boto3.client("s3")
    bucket = "apt.materialize.com"
    object_key = f"pool/generic/m/ma/materialized-{version}.deb"
    # Download the staged package (deb-s3 needs to get various metadata from it)
    s3.download_file(bucket, object_key, f"materialized-{version}.deb")
    # Import the private key into GPG
    key = os.environ["GPG_KEY"]
    gpg = subprocess.Popen(["gpg", "--import"], stdin=subprocess.PIPE)
    gpg.communicate(key.encode("ascii"))

    # Make the package public
    s3.put_object_acl(ACL="public-read", Key=object_key, Bucket=bucket)
    # Run deb-s3 to update the repository (no need to upload the file again)
    spawn.runv(
        [
            "deb-s3",
            "upload",
            "-p",
            "--skip-package-upload",
            "--sign",
            "-b",
            bucket,
            "-c",
            "generic",
            f"./materialized-{version}.deb",
        ]
    )


def tag_docker(repo: mzbuild.Repository, tag: str) -> None:
    deps = repo.resolve_dependencies(image for image in repo if image.publish)
    deps.acquire()
    for dep in deps:
        if dep.publish:
            name = dep.image.docker_name(tag)
            spawn.runv(["docker", "tag", dep.spec(), name])
            spawn.runv(["docker", "push", name])


def tag_docker_latest_maybe(repo: mzbuild.Repository, tag: str) -> None:
    """If this tag is greater than all other tags, and is a release, tag it `latest`"""
    this_tag = semver.VersionInfo.parse(tag.lstrip("v"))
    if this_tag.prerelease is not None:
        return

    highest_release = next(t for t in git.get_version_tags() if t.prerelease is None)
    if this_tag == highest_release:
        tag_docker(repo, "latest")


if __name__ == "__main__":
    main()
