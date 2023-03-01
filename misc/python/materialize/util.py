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
from typing import List

import frontmatter
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
        # TODO(def-) Remove type ignores when https://github.com/python-semver/python-semver/pull/396 is merged
        return cls.parse(version)  # type: ignore

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
                return cls.parse(package["version"])  # type: ignore
        else:
            raise ValueError("No mz-environmentd version found in cargo metadata")

    def __str__(self) -> str:
        return "v" + super().__str__()


def released_materialize_versions() -> List[MzVersion]:
    """Returns all released Materialize versions.

    The list is determined from the release notes files in the user
    documentation. Only versions that declare `released: true` in their
    frontmatter are considered.

    The list is returned in version order with newest versions first.
    """
    files = Path(ROOT / "doc" / "user" / "content" / "releases").glob("v*.md")
    versions = []
    for f in files:
        base = f.stem
        metadata = frontmatter.load(f)
        if metadata.get("released", False):
            patch = metadata.get("patch", 0)
            versions.append(MzVersion.parse_mz(f"{base}.{patch}"))
    versions.sort(reverse=True)
    return versions
