# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""An interface to Rust's Cargo package manager."""

from pathlib import Path
import semver
import toml


class Crate:
    """A Cargo crate.

    A crate directory must contain a `Cargo.toml` file with `package.name` and
    `package.version` keys.

    Args:
        path: The path to the crate directory.

    Attributes:
        name: The name of the crate.
        version: The version of the crate.
    """

    def __init__(self, path: Path):
        with open(path / "Cargo.toml") as f:
            config = toml.load(f)
        self.name = config["package"]["name"]
        self.version = semver.VersionInfo.parse(config["package"]["version"])


class Workspace:
    """A Cargo workspace.

    A workspace directory must contain a `Cargo.toml` file with a
    `workspace.members` key.

    Args:
        root: The path to the root of the workspace.
    """

    def __init__(self, root: Path):
        with open(root / "Cargo.toml") as f:
            config = toml.load(f)

        self.crates = {}
        for path in config["workspace"]["members"]:
            crate = Crate(Path(path))
            self.crates[crate.name] = crate
