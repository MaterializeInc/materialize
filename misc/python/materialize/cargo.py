# Copyright Materialize, Inc. and contributors. All rights reserved.
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
from typing import Dict, Optional, Set

import semver
import toml

from materialize import git


class Crate:
    """A Cargo crate.

    A crate directory must contain a `Cargo.toml` file with `package.name` and
    `package.version` keys.

    Args:
        root: The path to the root of the workspace.
        path: The path to the crate directory.

    Attributes:
        name: The name of the crate.
        version: The version of the crate.
        features: The features of the crate.
        path: The path to the crate.
        path_build_dependencies: The build dependencies which are declared
            using paths.
        path_dev_dependencies: The dev dependencies which are declared using
            paths.
        path_dependencies: The dependencies which are declared using paths.
        rust_version: The minimum Rust version declared in the crate, if any.
        bins: The names of all binaries in the crate.
        examples: The names of all examples in the crate.
    """

    def __init__(self, root: Path, path: Path):
        self.root = root
        with open(path / "Cargo.toml") as f:
            config = toml.load(f)
        self.name = config["package"]["name"]
        self.version = semver.VersionInfo.parse(config["package"]["version"])
        self.features = config.get("features", {})
        self.path = path
        self.path_build_dependencies: Set[str] = set()
        self.path_dev_dependencies: Set[str] = set()
        self.path_dependencies: Set[str] = set()
        for (dep_type, field) in [
            ("build-dependencies", self.path_build_dependencies),
            ("dev-dependencies", self.path_dev_dependencies),
            ("dependencies", self.path_dependencies),
        ]:
            if dep_type in config:
                field.update(
                    c.get("package", name)
                    for name, c in config[dep_type].items()
                    if "path" in c
                )
        self.rust_version: Optional[str] = None
        if "package" in config:
            self.rust_version = config["package"].get("rust-version")
        self.bins = []
        if "bin" in config:
            for bin in config["bin"]:
                self.bins.append(bin["name"])
        if config["package"].get("autobins", True):
            if (path / "src" / "main.rs").exists():
                self.bins.append(self.name)
            for p in (path / "src" / "bin").glob("*.rs"):
                self.bins.append(p.stem)
            for p in (path / "src" / "bin").glob("*/main.rs"):
                self.bins.append(p.parent.stem)
        self.examples = []
        if "example" in config:
            for example in config["example"]:
                self.examples.append(example["name"])
        if config["package"].get("autoexamples", True):
            for p in (path / "examples").glob("*.rs"):
                self.examples.append(p.stem)
            for p in (path / "examples").glob("*/main.rs"):
                self.examples.append(p.parent.stem)

    def inputs(self) -> Set[str]:
        """Compute the files that can impact the compilation of this crate.

        Note that the returned list may have false positives (i.e., include
        files that do not in fact impact the compilation of this crate), but it
        is not believed to have false negatives.

        Returns:
            inputs: A list of input files, relative to the root of the
                Cargo workspace.
        """
        # NOTE(benesch): it would be nice to have fine-grained tracking of only
        # exactly the files that go into a Rust crate, but doing this properly
        # requires parsing Rust code, and we don't want to force a dependency on
        # a Rust toolchain for users running demos. Instead, we assume that all†
        # files in a crate's directory are inputs to that crate.
        #
        # † As a development convenience, we omit mzcompose configuration files
        # within a crate. This is technically incorrect if someone writes
        # `include!("mzcompose.py")`, but that seems like a crazy thing to do.
        return git.expand_globs(
            self.root,
            f"{self.path}/**",
            f":(exclude){self.path}/mzcompose",
            f":(exclude){self.path}/mzcompose.py",
        )


class Workspace:
    """A Cargo workspace.

    A workspace directory must contain a `Cargo.toml` file with a
    `workspace.members` key.

    Args:
        root: The path to the root of the workspace.

    Attributes:
        crates: A mapping from name to crate definition.
    """

    def __init__(self, root: Path):
        with open(root / "Cargo.toml") as f:
            config = toml.load(f)

        self.crates: Dict[str, Crate] = {}
        for path in config["workspace"]["members"]:
            crate = Crate(root, root / path)
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

    def crate_for_example(self, example: str) -> Crate:
        """Find the crate containing the named example.

        Args:
            example: The name of the example to find.

        Raises:
            ValueError: The named example did not exist in exactly one crate in
                the Cargo workspace.
        """
        out = None
        for crate in self.crates.values():
            for e in crate.examples:
                if e == example:
                    if out is not None:
                        raise ValueError(
                            f"example {example} appears more than once in cargo workspace"
                        )
                    out = crate
        if out is None:
            raise ValueError(f"example {example} does not exist in cargo workspace")
        return out

    def transitive_path_dependencies(
        self, crate: Crate, dev: bool = False
    ) -> Set[Crate]:
        """Collects the transitive path dependencies of the requested crate.

        Note that only _path_ dependencies are collected. Other types of
        dependencies, like registry or Git dependencies, are not collected.

        Args:
            crate: The crate object from which to start the dependency crawl.
            dev: Whether to consider dev dependencies in the root crate.

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
            for d in c.path_build_dependencies:
                visit(self.crates[d])

        visit(crate)
        if dev:
            for d in crate.path_dev_dependencies:
                visit(self.crates[d])
        return deps
