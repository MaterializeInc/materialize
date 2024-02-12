# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""File utilities."""
import glob
from pathlib import Path

from materialize import MZ_ROOT, spawn


def get_recursive_file_list(path: str | Path, root_dir: Path = MZ_ROOT) -> list[str]:
    result = spawn.capture(["find", path, "-type", "f"], cwd=root_dir)
    return _file_list_output_to_list(result)


def resolve_path_with_wildcard(path: str, root_dir: str | Path = MZ_ROOT) -> set[str]:
    return set(glob.glob(path, root_dir=root_dir))


def resolve_paths_with_wildcard(
    paths: set[str], root_dir: str | Path = MZ_ROOT
) -> set[str]:
    result = set()

    for path in paths:
        result = result.union(resolve_path_with_wildcard(path, root_dir))

    return result


def _file_list_output_to_list(output: str) -> list[str]:
    files = output.split("\n")
    return [file for file in files if file != ""]
