#!/usr/bin/env python3

# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize import bintray
from materialize import cargo
from materialize import ci_util
from materialize import deb
from materialize import git
from materialize import mzbuild
from materialize import spawn
from pathlib import Path
import humanize
import os


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
    if os.environ["BUILDKITE_BRANCH"] == "master":
        stage_deb(repo, "materialized-unstable", deb.unstable_version(workspace))
    elif os.environ["BUILDKITE_TAG"]:
        version = workspace.crates["materialized"].version
        assert (
            f"v{version}" == os.environ["BUILDKITE_TAG"]
        ), f'materialized version {version} does not match tag {os.environ["BUILDKITE_TAG"]}'
        stage_deb(repo, "materialized", str(version))
    else:
        print("Not on master branch or tag; skipping")


def stage_deb(repo: mzbuild.Repository, package: str, version: str) -> None:
    """Stage a Debian package on Bintray.

    Note that this function does not cause anything to become public; a
    step to publish the files will be run in the deploy job.
    """

    print(f"{package} {version}")

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
    package = bt.repo("apt").package("materialized-unstable")
    try:
        print("Creating Bintray version...")
        commit_hash = git.rev_parse("HEAD")
        package.create_version(version, desc="git master", vcs_tag=commit_hash)
    except bintray.VersionAlreadyExistsError:
        # Ignore for idempotency. Bintray won't allow us to overwite an existing
        # .deb below with a file whose checksum doesn't match, so this is safe.
        pass

    print(f"Uploading Debian package ({humanize.naturalsize(deb_size)})...")
    package.debian_upload(
        version,
        path=f"/{version}/materialized-{commit_hash}.deb",
        data=open(deb_path, "rb"),
        distributions=["generic"],
        components=["main"],
        architectures=["amd64"],
    )


if __name__ == "__main__":
    main()
