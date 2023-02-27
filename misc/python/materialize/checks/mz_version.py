# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import json
import subprocess
from functools import total_ordering
from typing import Any


@total_ordering
class MzVersion:
    """Version of Materialize based on mz_version() string."""

    def __init__(self, version: str) -> None:
        if not version[0] == "v":
            raise ValueError(f"Invalid mz version string: {version}")
        if " " in version:
            version, git_hash = version.split(" ")
            if not git_hash[0] == "(" or not git_hash[-1] == ")":
                raise ValueError(f"Invalid mz version string: {version}")
            self._hash = git_hash  # Currently ignored
        self._dev = version.endswith("-dev")  # Currently ignored
        version = version.removesuffix("-dev")
        self._version = tuple(int(part) for part in version[1:].split("."))
        if not len(self._version) == 3:
            raise ValueError(f"Invalid mz version string: {version}")

    def __lt__(self, other: object) -> Any:
        if not isinstance(other, MzVersion):
            raise TypeError(
                f"'<' not supported between instances of '{type(self).__name__}' and '{type(other).__name__}'"
            )
        return self._version < other._version

    def __eq__(self, other: object) -> Any:
        if not isinstance(other, MzVersion):
            return False
        return self._version == other._version

    def __str__(self) -> str:
        return f"v{self._version[0]}.{self._version[1]}.{self._version[2]}"


class MzVersionCargo(MzVersion):
    """Version determined based on the current cargo mz-environmentd version."""

    def __init__(self) -> None:
        metadata = json.loads(
            subprocess.check_output(
                ["cargo", "metadata", "--no-deps", "--format-version=1"]
            )
        )
        for package in metadata["packages"]:
            if package["name"] == "mz-environmentd":
                super().__init__(f"v{package['version']}")
                break
        else:
            raise ValueError("No mz-environmentd version found in cargo metadata")
