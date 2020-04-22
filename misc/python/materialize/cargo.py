# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""A pure Python metadata parser for Cargo, Rust's package manager.

See the [Cargo] documentation for details. Only the features that are presently
necessary to support this repository are implemented.

[Cargo]: https://doc.rust-lang.org/cargo/
"""

from pathlib import Path
from typing import Set
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
        self.path = path
        self.path_dependencies: Set[str] = set()
        for dep_type in ["build-dependencies", "dependencies"]:
            if dep_type in config:
                self.path_dependencies.update(
                    name for name, c in config[dep_type].items() if "path" in c
                )
        self.bins = []
        if "bin" in config:
            for bin in config["bin"]:
                self.bins.append(bin["name"])
        if (path / "src" / "main.rs").exists():
            self.bins.append(self.name)


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
            crate = Crate(root / path)
            self.crates[crate.name] = crate

    def crate_for_bin(self, bin: str) -> Crate:
        """Find the crate containing the named binary.

        Args:
            bin: The name of the binary to find.

        Raises:
            ValueError: The named binary did not exist in exactly one crate in
                the Cargo workspace.
        """
        out = None
        for crate in self.crates.values():
            for b in crate.bins:
                if b == bin:
                    if out is not None:
                        raise ValueError(
                            f"bin {bin} appears more than once in cargo workspace"
                        )
                    out = crate
        if out is None:
            raise ValueError(f"bin {bin} does not exist in cargo workspace")
        return out

    def transitive_path_dependencies(self, crate: Crate) -> Set[Crate]:
        """Collects the transitive path dependencies of the requested crate.

        Note that only _path_ dependencies are collected. Other types of
        dependencies, like registry or Git dependencies, are not collected.

        Args:
            crate: The crate object from which to start the dependency crawl.

        Returns:
            crate_set: A set of all of the crates in this Cargo workspace upon
                which the input crate depended upon, whether directly or
                transitively.

        Raises:
            IndexError: The input crate did not exist.
        """
        deps = set()

        def visit(c: Crate) -> None:
            deps.add(c)
            for d in c.path_dependencies:
                visit(self.crates[d])

        visit(crate)
        return deps
