# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Various utilities"""

from __future__ import annotations

import json
import subprocess

try:
    from semver.version import Version
except ImportError:
    from semver import VersionInfo as Version  # type: ignore


class MzVersion(Version):
    """Version of Materialize, can be parsed from version string, SQL, cargo"""

    @classmethod
    def create_mz(
        cls, major: int, minor: int, patch: int, prerelease: str | None = None
    ) -> MzVersion:
        prerelease_suffix = f"-{prerelease}" if prerelease is not None else ""
        return cls.parse_mz(f"v{major}.{minor}.{patch}{prerelease_suffix}")

    @classmethod
    def parse_mz(cls, version: str, drop_dev_suffix: bool = False) -> MzVersion:
        """Parses a Mz version string, for example:  v0.45.0-dev (f01773cb1)"""
        if not version[0] == "v":
            raise ValueError(f"Invalid mz version string: {version}")
        version = version[1:]
        if " " in version:
            version, git_hash = version.split(" ")
            if not git_hash[0] == "(" or not git_hash[-1] == ")":
                raise ValueError(f"Invalid mz version string: {version}")
            # Hash ignored

        if drop_dev_suffix:
            version = version.replace("-dev", "")

        return cls.parse(version)

    @classmethod
    def try_parse_mz(
        cls, version: str, drop_dev_suffix: bool = False
    ) -> MzVersion | None:
        """Parses a Mz version string but returns empty if that fails"""
        try:
            return cls.parse_mz(version, drop_dev_suffix=drop_dev_suffix)
        except ValueError:
            return None

    @classmethod
    def is_mz_version_string(cls, version: str) -> bool:
        return cls.try_parse_mz(version) is not None

    @classmethod
    def parse_cargo(cls) -> MzVersion:
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
    def from_semver(cls, version: Version) -> MzVersion:
        return cls.parse(str(version))

    def to_semver(self) -> Version:
        return Version.parse(str(self).lstrip("v"))

    def __str__(self) -> str:
        return "v" + super().__str__()
