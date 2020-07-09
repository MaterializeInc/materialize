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
from . import deploy_util
import os


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
        # TODO(benesch): figure out how to push a latest tag. We want to be
        # careful to not overwrite a tag for a newer release if we are building
        # a historical release (e.g., don't overwrite v1.1.0 with v1.0.1).
    else:
        tag_docker(repo, f'unstable-{git.rev_parse("HEAD")}')
        tag_docker(repo, "unstable")

    print("--- Uploading binary tarball")
    mz_path = Path("materialized")
    ci_util.acquire_materialized(repo, mz_path)
    deploy_util.deploy_tarball("x86_64-unknown-linux-gnu", mz_path)


def publish_deb(package: str, version: str) -> None:
    print(f"{package} v{version}")
    bt = bintray.Client(
        "materialize", user="ci@materialize", api_key=os.environ["BINTRAY_API_KEY"]
    )
    bt.repo("apt").package(package).publish_uploads(version)


def tag_docker(repo: mzbuild.Repository, tag: str) -> None:
    deps = repo.resolve_dependencies(image for image in repo if image.publish)
    deps.acquire()
    for dep in deps:
        if dep.publish:
            name = dep.image.docker_name(tag)
            spawn.runv(["docker", "tag", dep.spec(), name])
            spawn.runv(["docker", "push", name])


if __name__ == "__main__":
    main()
