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
class PerFileVersioningConfig:
    directory: Path
    excluded_file_names: set[str]
    sha256_per_file: dict[str, str]
    sha256_per_file_dict_name: str
    task_on_hash_mismatch: str
    included_file_extensions: set[str] = field(default_factory=lambda: {"py"})
