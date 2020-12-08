#!/usr/bin/env python3

# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import os
from pathlib import Path

import humanize

from materialize import bintray
from materialize import errors
from materialize import cargo
from materialize import ci_util
from materialize import deb
from materialize import git
from materialize import mzbuild
from materialize import spawn


def main() -> None:
    repo = mzbuild.Repository(Path("."))
    workspace = cargo.Workspace(repo.root)

    # Acquire all the mzbuild images in the repository, while pushing any
    # images that we build to Docker Hub, where they will be accessible to
    # other build agents.
    print("--- Acquiring mzbuild images")
    deps = repo.resolve_dependencies(image for image in repo if image.publish)
    deps.acquire()
    for d in deps:
        if not d.pushed():
            d.push()

    print("--- Staging Debian package")
    if os.environ["BUILDKITE_BRANCH"] == "main":
        stage_deb(repo, "materialized-unstable", deb.unstable_version(workspace))
    elif os.environ["BUILDKITE_TAG"]:
        version = workspace.crates["materialized"].version
        assert (
            f"v{version}" == os.environ["BUILDKITE_TAG"]
        ), f'materialized version {version} does not match tag {os.environ["BUILDKITE_TAG"]}'
        stage_deb(repo, "materialized", str(version))
    elif os.environ["BUILDKITE_BRANCH"] == "master":
        raise errors.MzError(f"Tried to build branch master {git.rev_parse('HEAD')}")
    else:
        print("Not on main branch or tag; skipping")


def stage_deb(repo: mzbuild.Repository, package: str, version: str) -> None:
    """Stage a Debian package on Bintray.

    Note that this function does not cause anything to become public; a
    step to publish the files will be run in the deploy job.
    """

    print(f"Staging deb {package} {version}")

    # Extract the materialized binary from the Docker image. This avoids
    # an expensive rebuild if we're using a cached image.
    ci_util.acquire_materialized(
        repo, repo.rd.xcargo_target_dir() / "release" / "materialized"
    )

    # Build the Debian package.
    deb_path = repo.rd.xcargo_target_dir() / "debian" / "materialized.deb"
    spawn.runv(
        [
            repo.rd.xcargo(),
            "deb",
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
    deb_size = deb_path.stat().st_size

    bt = bintray.Client(
        "materialize", user="ci@materialize", api_key=os.environ["BINTRAY_API_KEY"]
    )
    package = bt.repo("apt").package(package)
    try:
        print("Creating Bintray version...")
        commit_hash = git.rev_parse("HEAD")
        package.create_version(version, desc="git main", vcs_tag=commit_hash)
    except bintray.VersionAlreadyExistsError:
        # Ignore for idempotency.
        pass

    try:
        print(f"Uploading Debian package ({humanize.naturalsize(deb_size)})...")
        package.debian_upload(
            version,
            path=f"/{version}/materialized-{commit_hash}.deb",
            data=open(deb_path, "rb"),
            distributions=["generic"],
            components=["main"],
            architectures=["amd64"],
        )
    except bintray.DebAlreadyExistsError:
        # Ideally `cargo deb` would produce identical output for identical input
        # to give us idempotency for free, since Bintray won't produce a
        # DebAlreadyExistsError if you upload the identical .deb file twice. But
        # it doesn't, so instead we just assume the .deb that's already uploaded
        # is functionally equivalent to the one we just built.
        print("Debian package already exists; assuming it is valid and skipping upload")


if __name__ == "__main__":
    main()
