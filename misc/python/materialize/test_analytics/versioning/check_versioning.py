#!/usr/bin/env python3

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.test_analytics.versioning.versioning_config import (
    PerFileVersioningConfig,
)
from materialize.util import compute_sha256_of_file

PER_FILE_VERSIONING_CONFIGS_TO_CHECK = []


def main() -> None:
    errors = []

    for config in PER_FILE_VERSIONING_CONFIGS_TO_CHECK:
        errors.extend(check_per_file_versioning(config))

    for error in errors:
        print(error)

    if len(errors) > 0:
        exit(1)


def check_per_file_versioning(config: PerFileVersioningConfig) -> list[str]:
    errors = []

    for file_path in config.directory.iterdir():
        if file_path.is_dir():
            continue

        file_name = file_path.name

        if _is_included_file(config, file_name):
            continue

        sha_value = compute_sha256_of_file(file_path)

        if file_name not in config.sha256_per_file.keys():
            errors.append(
                f"File {file_path} has no hash record in '{config.sha256_per_file_dict_name}'.\n"
                f"Hint: Add an entry '{file_name}' with value '{sha_value}'."
            )
        elif config.sha256_per_file[file_name] != sha_value:
            errors.append(
                f"File {file_path} has a divergent hash record in '{config.sha256_per_file_dict_name}'.\n"
                f"{config.task_on_hash_mismatch}\n"
                f"Hint: Update the entry '{file_name}' with value '{sha_value}'."
            )

    return errors


def _is_included_file(config: PerFileVersioningConfig, file_name: str) -> bool:
    if file_name in config.excluded_file_names:
        return False

    matches_file_extension = False

    for extension in config.included_file_extensions:
        if file_name.endswith(extension):
            matches_file_extension = True

    return matches_file_extension


if __name__ == "__main__":
    main()
