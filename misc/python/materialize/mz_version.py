# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Version types"""

from __future__ import annotations

import json
import subprocess
from typing import TypeVar

try:
    from semver.version import Version
except ImportError:
    from semver import VersionInfo as Version  # type: ignore

from materialize import MZ_ROOT

T = TypeVar("T", bound="TypedVersionBase")


class TypedVersionBase(Version):
    """Typed version, can be parsed from version string"""

    @classmethod
    def get_prefix(cls) -> str:
        raise NotImplementedError(f"Not implemented in {cls}")

    @classmethod
    def create(
        cls: type[T], major: int, minor: int, patch: int, prerelease: str | None = None
    ) -> T:
        prerelease_suffix = f"-{prerelease}" if prerelease is not None else ""
        return cls.parse(
            f"{cls.get_prefix()}{major}.{minor}.{patch}{prerelease_suffix}"
        )

    @classmethod
    def parse_without_prefix(
        cls: type[T], version_without_prefix: str, drop_dev_suffix: bool = False
    ) -> T:
        version = f"{cls.get_prefix()}{version_without_prefix}"
        return cls.parse(version, drop_dev_suffix=drop_dev_suffix)

    @classmethod
    def parse(cls: type[T], version: str, drop_dev_suffix: bool = False) -> T:
        """Parses a version string with prefix, for example: v0.45.0-dev (f01773cb1) or v0.115.0-dev.0"""
        expected_prefix = cls.get_prefix()
        if not version.startswith(expected_prefix):
            raise ValueError(
                f"Invalid version string '{version}', expected prefix '{expected_prefix}'"
            )
        version = version.removeprefix(expected_prefix)
        if " " in version:
            version, git_hash = version.split(" ")
            if not git_hash[0] == "(" or not git_hash[-1] == ")":
                raise ValueError(f"Invalid mz version string: {version}")
            # Hash ignored

        if drop_dev_suffix:
            version, _, _ = version.partition("-dev")

        return super().parse(version)

    @classmethod
    def try_parse(
        cls: type[T], version: str, drop_dev_suffix: bool = False
    ) -> T | None:
        """Parses a version string but returns empty if that fails"""
        try:
            return cls.parse(version, drop_dev_suffix=drop_dev_suffix)
        except ValueError:
            return None

    @classmethod
    def is_valid_version_string(cls, version: str) -> bool:
        return cls.try_parse(version) is not None

    def str_without_prefix(self) -> str:
        return super().__str__()

    def __str__(self) -> str:
        return f"{self.get_prefix()}{self.str_without_prefix()}"

    def is_dev_version(self) -> bool:
        return self.prerelease is not None


class MzVersion(TypedVersionBase):
    """Version of Materialize, can be parsed from version string, SQL, cargo"""

    @classmethod
    def get_prefix(cls) -> str:
        return "v"

    @classmethod
    def parse_mz(cls: type[T], version: str, drop_dev_suffix: bool = False) -> T:
        """Parses a version string with prefix, for example: v0.45.0-dev (f01773cb1) or v0.115.0-dev.0"""
        return cls.parse(version=version, drop_dev_suffix=drop_dev_suffix)

    @classmethod
    def parse_cargo(cls) -> MzVersion:
        """Uses the cargo mz-environmentd package info to get the version of current source code state"""
        metadata = json.loads(
            subprocess.check_output(
                ["cargo", "metadata", "--no-deps", "--format-version=1"],
                cwd=MZ_ROOT,
            )
        )
        for package in metadata["packages"]:
            if package["name"] == "mz-environmentd":
                return cls.parse_without_prefix(package["version"])
        else:
            raise ValueError("No mz-environmentd version found in cargo metadata")


class MzCliVersion(TypedVersionBase):
    """Version of Materialize APT"""

    @classmethod
    def get_prefix(cls) -> str:
        return "mz-v"


class MzLspServerVersion(TypedVersionBase):
    """Version of Materialize LSP Server"""

    @classmethod
    def get_prefix(cls) -> str:
        return "mz-lsp-server-v"


class MzDebugVersion(TypedVersionBase):
    """Version of Materialize Debug Tool"""

    @classmethod
    def get_prefix(cls) -> str:
        return "mz-debug-v"
