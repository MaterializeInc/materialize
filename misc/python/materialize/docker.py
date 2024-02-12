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
IMAGE_TAG_OF_VERSION_PREFIX = "v"
IMAGE_TAG_OF_COMMIT_PREFIX = "devel-"


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
        if "no such manifest:" in e.output:
            print(f"Failed to fetch image manifest '{image_name}' (does not exist)")
            EXISTENCE_OF_IMAGE_NAMES_FROM_EARLIER_CHECK[image_name] = False
        else:
            print(f"Failed to fetch image manifest '{image_name}' ({e.output})")
            # do not cache the result of unknown error messages

        return False


def commit_to_image_tag(commit_hash: str) -> str:
    return f"{IMAGE_TAG_OF_COMMIT_PREFIX}{commit_hash}"


def version_to_image_tag(version: MzVersion) -> str:
    return str(version)


def is_image_tag_of_version(image_tag: str) -> bool:
    return image_tag.startswith(IMAGE_TAG_OF_VERSION_PREFIX)


def is_image_tag_of_commit(image_tag: str) -> bool:
    return image_tag.startswith(IMAGE_TAG_OF_COMMIT_PREFIX)


def get_version_from_image_tag(image_tag: str) -> str:
    assert image_tag.startswith(IMAGE_TAG_OF_VERSION_PREFIX)
    # image tag is equal to the version
    return image_tag


def get_mz_version_from_image_tag(image_tag: str) -> MzVersion:
    return MzVersion.parse_mz(get_version_from_image_tag(image_tag))


def get_commit_from_image_tag(image_tag: str) -> str:
    assert image_tag.startswith(IMAGE_TAG_OF_COMMIT_PREFIX)
    return image_tag.removeprefix(IMAGE_TAG_OF_COMMIT_PREFIX)
