# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Docker utilities."""
import re
import subprocess
import time

import requests

from materialize import ui
from materialize.mz_version import MzVersion

CACHED_IMAGE_NAME_BY_COMMIT_HASH: dict[str, str] = dict()
EXISTENCE_OF_IMAGE_NAMES_FROM_EARLIER_CHECK: dict[str, bool] = dict()

IMAGE_TAG_OF_DEV_VERSION_METADATA_SEPARATOR = "--"
LATEST_IMAGE_TAG = "latest"
LEGACY_IMAGE_TAG_COMMIT_PREFIX = "devel-"
MZ_GHCR_DEFAULT = "1" if ui.env_is_truthy("CI") else "0"

# Examples:
# * v0.114.0
# * v0.114.0-dev
# * v0.114.0-dev.0--pr.g3d565dd11ba1224a41beb6a584215d99e6b3c576
VERSION_IN_IMAGE_TAG_PATTERN = re.compile(r"^(v\d+\.\d+\.\d+(-dev)?)")


def image_of_release_version_exists(version: MzVersion, quiet: bool = False) -> bool:
    if version.is_dev_version():
        raise ValueError(f"Version {version} is a dev version, not a release version")

    return mz_image_tag_exists(release_version_to_image_tag(version), quiet=quiet)


def image_of_commit_exists(commit_hash: str) -> bool:
    return mz_image_tag_exists(commit_to_image_tag(commit_hash))


def mz_image_tag_exists_cmdline(image_name: str) -> bool:
    command = [
        "docker",
        "manifest",
        "inspect",
        image_name,
    ]
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


def mz_image_tag_exists(image_tag: str, quiet: bool = False) -> bool:
    image_name = f"{image_registry()}/materialized:{image_tag}"

    if image_name in EXISTENCE_OF_IMAGE_NAMES_FROM_EARLIER_CHECK:
        image_exists = EXISTENCE_OF_IMAGE_NAMES_FROM_EARLIER_CHECK[image_name]
        if not quiet:
            print(
                f"Status of image {image_name} known from earlier check: {'exists' if image_exists else 'does not exist'}"
            )
        return image_exists

    if not quiet:
        print(f"Checking existence of image manifest: {image_name}")

    command_local = ["docker", "images", "--quiet", image_name]

    output = subprocess.check_output(command_local, stderr=subprocess.STDOUT, text=True)
    if output:
        # image found locally, can skip querying remote Docker Hub
        EXISTENCE_OF_IMAGE_NAMES_FROM_EARLIER_CHECK[image_name] = True
        return True

    # docker manifest inspect counts against the Docker Hub rate limits, even
    # when the image doesn't exist, see https://www.docker.com/increase-rate-limits/,
    # so use the API instead.

    if image_registry() != "materialize":
        return mz_image_tag_exists_cmdline(image_name)

    try:
        response = requests.get(
            f"https://hub.docker.com/v2/repositories/materialize/materialized/tags/{image_tag}"
        )
        result = response.json()
    except (requests.exceptions.ConnectionError, requests.exceptions.JSONDecodeError):
        return mz_image_tag_exists_cmdline(image_name)

    if result.get("images"):
        EXISTENCE_OF_IMAGE_NAMES_FROM_EARLIER_CHECK[image_name] = True
        return True
    if "not found" in result.get("message", ""):
        EXISTENCE_OF_IMAGE_NAMES_FROM_EARLIER_CHECK[image_name] = False
        return False
    if not quiet:
        print(f"Failed to fetch image info from API: {result}")
    # do not cache the result of unknown error messages
    return False


def commit_to_image_tag(commit_hash: str) -> str:
    return _resolve_image_name_by_commit_hash(commit_hash)


def release_version_to_image_tag(version: MzVersion) -> str:
    return str(version)


def is_image_tag_of_release_version(image_tag: str) -> bool:
    return (
        IMAGE_TAG_OF_DEV_VERSION_METADATA_SEPARATOR not in image_tag
        and not image_tag.startswith(LEGACY_IMAGE_TAG_COMMIT_PREFIX)
        and image_tag != LATEST_IMAGE_TAG
    )


def is_image_tag_of_commit(image_tag: str) -> bool:
    return (
        IMAGE_TAG_OF_DEV_VERSION_METADATA_SEPARATOR in image_tag
        or image_tag.startswith(LEGACY_IMAGE_TAG_COMMIT_PREFIX)
    )


def get_version_from_image_tag(image_tag: str) -> str:
    match = VERSION_IN_IMAGE_TAG_PATTERN.match(image_tag)
    assert match is not None, f"Invalid image tag: {image_tag}"

    return match.group(1)


def get_mz_version_from_image_tag(image_tag: str) -> MzVersion:
    return MzVersion.parse_mz(get_version_from_image_tag(image_tag))


def _resolve_image_name_by_commit_hash(commit_hash: str) -> str:
    if commit_hash in CACHED_IMAGE_NAME_BY_COMMIT_HASH.keys():
        return CACHED_IMAGE_NAME_BY_COMMIT_HASH[commit_hash]

    image_name_candidates = _search_docker_hub_for_image_name(search_value=commit_hash)
    image_name = _select_image_name_from_candidates(image_name_candidates, commit_hash)

    CACHED_IMAGE_NAME_BY_COMMIT_HASH[commit_hash] = image_name
    EXISTENCE_OF_IMAGE_NAMES_FROM_EARLIER_CHECK[image_name] = True

    return image_name


def _search_docker_hub_for_image_name(
    search_value: str, remaining_retries: int = 10
) -> list[str]:
    try:
        json_response = requests.get(
            f"https://hub.docker.com/v2/repositories/materialize/materialized/tags?name={search_value}"
        ).json()
    except (
        requests.exceptions.ConnectionError,
        requests.exceptions.JSONDecodeError,
    ) as _:
        if remaining_retries > 0:
            print("Searching Docker Hub for image name failed, retrying in 5 seconds")
            time.sleep(5)
            return _search_docker_hub_for_image_name(
                search_value, remaining_retries - 1
            )

        raise

    json_results = json_response.get("results")

    image_names = []

    for entry in json_results:
        image_name = entry.get("name")

        if image_name.startswith("unstable-"):
            # for images with the old version scheme favor "devel-" over "unstable-"
            continue

        image_names.append(image_name)

    return image_names


def _select_image_name_from_candidates(
    image_name_candidates: list[str], commit_hash: str
) -> str:
    if len(image_name_candidates) == 0:
        raise RuntimeError(f"No image found for commit hash {commit_hash}")

    if len(image_name_candidates) > 1:
        print(
            f"Multiple images found for commit hash {commit_hash}: {image_name_candidates}, picking first"
        )

    return image_name_candidates[0]


def image_registry() -> str:
    return (
        "ghcr.io/materializeinc/materialize"
        if ui.env_is_truthy("MZ_GHCR", "1")
        else "materialize"
    )
