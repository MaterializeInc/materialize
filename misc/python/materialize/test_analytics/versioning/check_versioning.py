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
from materialize.util import compute_sha256_of_file, compute_sha256_of_utf8_string

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
        errors.extend(check_per_file_versioning(config))

    for error in errors:
        print(error)

    if len(errors) > 0:
        exit(1)


def check_per_file_versioning(
    config: VersioningConfig, override_directory: Path | None = None
) -> list[str]:
    errors = []

    parent_directory = override_directory or config.root_directory

    hashes_of_files_in_current_dir = []
    for file_path in sorted(parent_directory.iterdir()):
        file_name = file_path.name

        if not _is_included_file(config, file_path, file_name):
            continue

        if file_path.is_dir():
            if config.recursive:
                errors.extend(
                    check_per_file_versioning(config, override_directory=file_path)
                )
            continue

        sha_value = compute_sha256_of_file(file_path)

        if config.group_hash_by_directory():
            hashes_of_files_in_current_dir.append(sha_value)
        else:
            error = _check_hash_entry(file_name, sha_value, file_path, config)
            if error is not None:
                errors.append(error)

    if config.group_hash_by_directory():
        entry_key = str(parent_directory.relative_to(config.root_directory))
        sha_value = compute_sha256_of_utf8_string(
            "-".join(hashes_of_files_in_current_dir)
        )
        error = _check_hash_entry(entry_key, sha_value, parent_directory, config)
        if error is not None:
            errors.append(error)

    return errors


def _check_hash_entry(
    entry_key: str, sha_value: str, file_path: Path, config: VersioningConfig
) -> str | None:
    if entry_key not in config.sha256_per_entry.keys():
        return (
            f"Entry {file_path} has no hash record in '{config.sha256_per_entry_dict_name}'.\n"
            f"Hint: Add an entry '{entry_key}' with value '{sha_value}'."
        )
    elif config.sha256_per_entry[entry_key] != sha_value:
        return (
            f"File {file_path} has a divergent hash record in '{config.sha256_per_entry_dict_name}'.\n"
            f"{config.task_on_hash_mismatch}\n"
            f"Hint: Update the entry '{entry_key}' with value '{sha_value}'."
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
