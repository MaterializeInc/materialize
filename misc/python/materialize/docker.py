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

EXISTENCE_OF_IMAGE_NAMES_FROM_EARLIER_CHECK: dict[str, bool] = dict()


def image_of_release_version_exists(version: MzVersion) -> bool:
    return mz_image_tag_exists(version_to_image_tag(version))


def image_of_commit_exists(commit_hash: str) -> bool:
    return mz_image_tag_exists(commit_to_image_tag(commit_hash))


def mz_image_tag_exists(image_tag: str) -> bool:
    image_name = f"materialize/materialized:{image_tag}"

    if image_name in EXISTENCE_OF_IMAGE_NAMES_FROM_EARLIER_CHECK:
        image_exists = EXISTENCE_OF_IMAGE_NAMES_FROM_EARLIER_CHECK[image_name]
        print(
            f"Status of image {image_name} known from earlier check: {'exists' if image_exists else 'does not exist'}"
        )
        return image_exists

    command = [
        "docker",
        "manifest",
        "inspect",
        image_name,
    ]

    print(f"Checking existence of image manifest: {image_name}")

    try:
        subprocess.check_output(command, stderr=subprocess.STDOUT, text=True)
        EXISTENCE_OF_IMAGE_NAMES_FROM_EARLIER_CHECK[image_name] = True
        return True
    except subprocess.CalledProcessError as e:
        print(f"Failed to fetch image manifest: {image_name}")

        if "no such manifest:" in e.output:
            EXISTENCE_OF_IMAGE_NAMES_FROM_EARLIER_CHECK[image_name] = False
        else:
            print(f"Error when checking image manifest was: {e.output}")
            # do not cache the result of unknown error messages

        return False


def commit_to_image_tag(commit_hash: str) -> str:
    return f"devel-{commit_hash}"


def version_to_image_tag(version: MzVersion) -> str:
    return str(version)
