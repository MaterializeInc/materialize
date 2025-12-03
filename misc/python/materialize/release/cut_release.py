# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Cut a new release and push the tag to the upstream Materialize repository."""

import argparse
import os
import pathlib
import re
import shutil
import subprocess
import sys
import time

from semver.version import Version

from materialize import MZ_ROOT, spawn
from materialize.git import checkout, get_branch_name, tag_annotated


def parse_version(version: str) -> Version:
    return Version.parse(re.sub(r"^v", "", version))


def main():
    parser = argparse.ArgumentParser(
        prog="cut_release",
        description="Creates a new release for Materialize.",
    )
    parser.add_argument(
        "--sha",
        help="Chosen SHA of the release",
        type=str,
        required=True,
    )
    parser.add_argument(
        "--version",
        help="Version of release",
        type=parse_version,
        required=True,
    )
    parser.add_argument(
        "--remote",
        help="Git remote name of Materialize repo",
        type=str,
        required=True,
    )

    args = parser.parse_args()
    version = f"v{args.version}"
    current_branch = get_branch_name()

    try:
        print(f"Checking out SHA {args.sha}")
        checkout(args.sha)
        print("Cloning console repo")
        console_dir = MZ_ROOT / "console"
        if os.path.exists(console_dir):
            shutil.rmtree(console_dir)
        try:
            spawn.runv(
                [
                    "git",
                    "clone",
                    "https://github.com/MaterializeInc/console",
                    console_dir,
                ],
                env={**os.environ, "GIT_TERMINAL_PROMPT": "0"},
            )
        except subprocess.CalledProcessError:
            spawn.runv(
                ["git", "clone", "git@github.com:MaterializeInc/console", console_dir],
                env={**os.environ, "GIT_TERMINAL_PROMPT": "0"},
            )

        print(f"Bumping console version to {version}")
        spawn.runv(["git", "tag", "-a", version, "-m", version], cwd=console_dir)
        spawn.runv(["git", "push", "origin", version], cwd=console_dir)

        print("Waiting for console version to be released on DockerHub (~15 min)")
        console_version = version[1:]
        console_image = f"materialize/console:{console_version}"
        time.sleep(15 * 60)
        while True:
            try:
                spawn.runv(["docker", "manifest", "inspect", console_image])
            except subprocess.CalledProcessError:
                print(f"{console_image} not yet on DockerHub, sleeping 1 min")
                time.sleep(60)
                continue
            break
        print(f"{console_image} found on DockerHub")

        print(f"Bumping version to {version}")
        spawn.runv(
            [
                MZ_ROOT / "bin" / "ci-builder",
                "run",
                "stable",
                MZ_ROOT / "bin" / "bump-version",
                version,
                "--no-commit",
                "--sbom",
            ]
        )
        dockerfile = pathlib.Path("misc/images/materialized-base/Dockerfile")
        dockerfile_text = dockerfile.read_text()
        dockerfile.write_text(
            re.sub(
                r"FROM materialize/console:.*? AS console",
                f"FROM {console_image} AS console",
                dockerfile_text,
            )
        )
        deployment = pathlib.Path("misc/helm-charts/operator/templates/deployment.yaml")
        deployment_text = deployment.read_text()
        deployment.write_text(
            re.sub(
                r'"--console-image-tag-default=[^"]*"',
                f'"--console-image-tag-default={console_version}"',
                deployment_text,
            )
        )
        # Commit here instead of in bump-version so we have access to the correct git author
        spawn.runv(["git", "commit", "-am", f"release: bump to version {version}"])
        print("Tagging version")
        tag_annotated(version)
        print("Pushing tag to Materialize repo")
        spawn.runv(["git", "push", args.remote, version])
    finally:
        # The caller may have started in a detached HEAD state.
        if current_branch:
            checkout(current_branch)


if __name__ == "__main__":
    sys.exit(main())
