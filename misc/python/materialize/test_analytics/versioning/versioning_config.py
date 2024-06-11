# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class VersioningConfig:
    root_directory: Path
    sha256_definition_file: Path
    sha256_per_entry: dict[str, str]
    sha256_per_entry_dict_name: str
    task_on_hash_mismatch: str
    recursive: bool
    excluded_file_names: set[str] = field(default_factory=lambda: set())
    excluded_paths: set[str] = field(default_factory=lambda: set())
    included_file_extensions: set[str] = field(default_factory=lambda: {"py"})

    def group_hash_to_single_value(self) -> bool:
        raise NotImplementedError


@dataclass
class PerFileVersioningConfig(VersioningConfig):
    recursive: bool = False

    def group_hash_to_single_value(self) -> bool:
        return False


@dataclass
class DirectoryVersioningConfig(VersioningConfig):
    recursive: bool = True

    def group_hash_to_single_value(self) -> bool:
        return True
