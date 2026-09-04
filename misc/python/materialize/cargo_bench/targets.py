# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Enumeration of criterion bench targets from `cargo metadata` output, and the
argv and JSON-artifact parsing needed to build all of them in one `cargo bench
--no-run` invocation."""

import json
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True, order=True)
class BenchTarget:
    """One `[[bench]]` target of a workspace package."""

    package: str
    name: str
    required_features: tuple[str, ...]


def bench_targets(metadata: dict[str, Any]) -> list[BenchTarget]:
    """Return every bench target in a `cargo metadata --no-deps` document, sorted by package and name."""
    targets = []
    for package in metadata["packages"]:
        for target in package["targets"]:
            if "bench" not in target["kind"]:
                continue
            targets.append(
                BenchTarget(
                    package=package["name"],
                    name=target["name"],
                    required_features=tuple(target.get("required-features", [])),
                )
            )
    return sorted(targets)


@dataclass(frozen=True)
class BuiltBench:
    """A compiled bench binary and the directory cargo would run it from."""

    package: str
    name: str
    executable: Path
    manifest_dir: Path


def package_manifests(metadata: dict[str, Any]) -> dict[str, str]:
    """Map each workspace package's manifest path to its package name."""
    return {
        package["manifest_path"]: package["name"] for package in metadata["packages"]
    }


def cargo_build_args(targets: Sequence[BenchTarget]) -> list[str]:
    """Build the `cargo bench --no-run` argv that compiles every given target in one invocation.

    Emits `--package` once per distinct package, `--bench` once per distinct
    target name, and required features as `package/feature` entries of a single
    `--features` flag, which is the only spelling cargo accepts when more than one
    package is selected. Output is `--message-format=json` so the caller can
    recover executable paths.
    """
    packages = sorted({t.package for t in targets})
    names = sorted({t.name for t in targets})
    features = sorted(
        f"{t.package}/{feature}" for t in targets for feature in t.required_features
    )
    args = ["cargo", "bench", "--no-run", "--message-format=json"]
    for package in packages:
        args += ["--package", package]
    for name in names:
        args += ["--bench", name]
    if features:
        args += ["--features", ",".join(features)]
    return args


def bench_executables(
    lines: Iterable[str], manifests: dict[str, str]
) -> list[BuiltBench]:
    """Extract bench binaries from `cargo --message-format=json` output.

    Keeps `compiler-artifact` messages whose target kind contains `bench` and
    that carry an `executable`. The package is resolved through `manifests`
    because bench target names are only unique within a package (`row` exists
    in both mz-repr and mz-storage-types). Lines that are not JSON or not
    artifacts are ignored. Sorted by (package, name).
    """
    built = []
    for line in lines:
        try:
            message = json.loads(line)
        except json.JSONDecodeError:
            continue
        if message.get("reason") != "compiler-artifact":
            continue
        target = message["target"]
        if "bench" not in target["kind"]:
            continue
        executable = message.get("executable")
        if executable is None:
            continue
        manifest_path = message["manifest_path"]
        built.append(
            BuiltBench(
                package=manifests[manifest_path],
                name=target["name"],
                executable=Path(executable),
                manifest_dir=Path(manifest_path).parent,
            )
        )
    return sorted(built, key=lambda b: (b.package, b.name))
