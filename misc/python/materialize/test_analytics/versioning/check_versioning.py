#!/usr/bin/env python3

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from pathlib import Path

from materialize.test_analytics.versioning.benchmark.benchmark_version_skew_check import (
    BENCHMARK_FRAMEWORK_VERSION_SKEW_CHECK_CONFIG,
    BENCHMARK_SCENARIOS_VERSION_SKEW_CHECK_CONFIG,
)
from materialize.test_analytics.versioning.scalability.scalability_version_skew_check import (
    SCALABILITY_FRAMEWORK_VERSION_SKEW_CHECK_CONFIG,
    SCALABILITY_WORKLOADS_VERSION_SKEW_CHECK_CONFIG,
)
from materialize.test_analytics.versioning.versioning_config import (
    VersioningConfig,
)
from materialize.util import sha256_of_file, sha256_of_utf8_string

VERSION_SKEW_CHECK_CONFIGS = [
    SCALABILITY_FRAMEWORK_VERSION_SKEW_CHECK_CONFIG,
    SCALABILITY_WORKLOADS_VERSION_SKEW_CHECK_CONFIG,
    BENCHMARK_FRAMEWORK_VERSION_SKEW_CHECK_CONFIG,
    BENCHMARK_SCENARIOS_VERSION_SKEW_CHECK_CONFIG,
]

DEFAULT_EXCLUDED_FILE_NAMES = {"__pycache__"}


def main() -> None:
    errors = []

    for config in VERSION_SKEW_CHECK_CONFIGS:
        errors.extend(check_versioning(config))

    for error in errors:
        print(error)

    if len(errors) > 0:
        exit(1)


def check_versioning(config: VersioningConfig) -> list[str]:
    """
    :return: identified errors
    """
    errors = []

    hash_per_relative_path = compute_hashes(config)

    if config.group_hash_to_single_value():
        merged_sha_value = sha256_of_utf8_string(
            "-".join(hash_per_relative_path.values())
        )
        hash_per_relative_path.clear()
        hash_per_relative_path["*"] = merged_sha_value

    for relative_path, sha_value in hash_per_relative_path.items():
        error = _check_hash_entry(relative_path, sha_value, config)
        if error is not None:
            errors.append(error)

    return errors


def compute_hashes(
    config: VersioningConfig, override_directory: Path | None = None
) -> dict[str, str]:
    """
    :return: sha256 hash per relative file path
    """
    hash_per_file_path = dict()
    parent_directory = override_directory or config.root_directory

    for file_path in sorted(parent_directory.iterdir()):
        file_name = file_path.name

        if not _is_included_file(config, file_path, file_name):
            continue

        if file_path.is_dir():
            if config.recursive:
                hash_per_file_path.update(
                    compute_hashes(config, override_directory=file_path)
                )
            continue

        sha_value = sha256_of_file(file_path)

        relative_path = file_path.relative_to(config.root_directory)
        hash_per_file_path[str(relative_path)] = sha_value

    return hash_per_file_path


def _check_hash_entry(
    path: str, sha_value: str, config: VersioningConfig
) -> str | None:
    hash_definition_location = (
        f"'{config.sha256_per_entry_dict_name}' in '{config.sha256_definition_file}'"
    )
    if path not in config.sha256_per_entry.keys():
        return (
            f"Path '{path}' has no hash record in {hash_definition_location}.\n"
            f"Hint: Add an entry '{path}' with value '{sha_value}'."
        )
    elif config.sha256_per_entry[path] != sha_value:
        return (
            f"Path '{path}' has a divergent hash record in {hash_definition_location}!\n"
            f"Important: {config.task_on_hash_mismatch}\n"
            f"Afterwards: Update the entry '{path}' with value '{sha_value}'."
        )

    return None


def _is_included_file(
    config: VersioningConfig, file_path: Path, file_name: str
) -> bool:
    if file_name in DEFAULT_EXCLUDED_FILE_NAMES:
        return False

    if file_name in config.excluded_file_names:
        return False

    if file_path.is_file():
        matches_file_extension = False
        for extension in config.included_file_extensions:
            if file_name.endswith(extension):
                matches_file_extension = True

        if not matches_file_extension:
            return False

    relative_path = file_path.relative_to(config.root_directory)

    for excluded_sub_path in config.excluded_paths:
        if str(relative_path).startswith(excluded_sub_path):
            return False

    return True


if __name__ == "__main__":
    main()
