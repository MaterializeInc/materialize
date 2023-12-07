# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Docker utilities."""
import subprocess

from materialize.mz_version import MzVersion


def image_of_release_version_exists(version: MzVersion) -> bool:
    return mz_image_tag_exists(version_to_image_tag(version))


def image_of_commit_exists(commit_hash: str) -> bool:
    return mz_image_tag_exists(commit_to_image_tag(commit_hash))


def mz_image_tag_exists(image_tag: str) -> bool:
    image = f"materialize/materialized:{image_tag}"
    command = [
        "docker",
        "pull",
        image,
    ]

    print(f"Trying to pull image: {image}")

    try:
        subprocess.check_output(command, stderr=subprocess.STDOUT, text=True)
        return True
    except subprocess.CalledProcessError as e:
        print(f"Failed to pull image: {image}")
        return "not found: manifest unknown: manifest unknown" not in e.output


def commit_to_image_tag(commit_hash: str) -> str:
    return f"devel-{commit_hash}"


def version_to_image_tag(version: MzVersion) -> str:
    return str(version)
