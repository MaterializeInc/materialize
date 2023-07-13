# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Various utilities"""

import json
import os
import random
import subprocess
from pathlib import Path

from semver import Version

from materialize.mzcompose import Composition

ROOT = Path(os.environ["MZ_ROOT"])


def nonce(digits: int) -> str:
    return "".join(random.choice("0123456789abcdef") for _ in range(digits))


class MzVersion(Version):
    """Version of Materialize, can be parsed from version string, SQL, cargo"""

    @classmethod
    def parse_mz(cls, version: str) -> "MzVersion":
        """Parses a Mz version string, for example:  v0.45.0-dev (f01773cb1)"""
        if not version[0] == "v":
            raise ValueError(f"Invalid mz version string: {version}")
        version = version[1:]
        if " " in version:
            version, git_hash = version.split(" ")
            if not git_hash[0] == "(" or not git_hash[-1] == ")":
                raise ValueError(f"Invalid mz version string: {version}")
            # Hash ignored
        return cls.parse(version)

    @classmethod
    def parse_sql(cls, c: Composition) -> "MzVersion":
        """Gets the Mz version from SQL query "SELECT mz_version()" and parses it"""
        return cls.parse_mz(c.sql_query("SELECT mz_version()")[0][0])

    @classmethod
    def parse_cargo(cls) -> "MzVersion":
        """Uses the cargo mz-environmentd package info to get the version of current source code state"""
        metadata = json.loads(
            subprocess.check_output(
                ["cargo", "metadata", "--no-deps", "--format-version=1"]
            )
        )
        for package in metadata["packages"]:
            if package["name"] == "mz-environmentd":
                return cls.parse(package["version"])
        else:
            raise ValueError("No mz-environmentd version found in cargo metadata")

    @classmethod
    def from_semver(cls, version: Version) -> "MzVersion":
        return cls.parse(str(version))

    def __str__(self) -> str:
        return "v" + super().__str__()
