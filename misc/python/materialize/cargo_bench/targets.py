# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Enumeration of criterion bench targets from `cargo metadata` output."""

from collections.abc import Sequence
from dataclasses import dataclass
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


def cargo_bench_args(
    target: BenchTarget, criterion_args: Sequence[str]
) -> list[str]:
    """Build the `cargo bench` argv for one target, passing `criterion_args` through to the bench binary."""
    args = ["cargo", "bench", "--package", target.package, "--bench", target.name]
    # Cargo silently skips a target whose required features are off, so they
    # must be enabled explicitly for the target to run at all.
    if target.required_features:
        args += ["--features", ",".join(target.required_features)]
    args.append("--")
    args.extend(criterion_args)
    return args
