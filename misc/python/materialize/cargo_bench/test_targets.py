# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import json
from pathlib import Path

import pytest

from materialize.cargo_bench.targets import (
    BenchTarget,
    BuiltBench,
    bench_executables,
    bench_targets,
    cargo_build_args,
    package_manifests,
)

METADATA = {
    "packages": [
        {
            "name": "mz-ore",
            "targets": [
                {"name": "mz_ore", "kind": ["lib"], "required-features": []},
                {"name": "id_gen", "kind": ["bench"], "required-features": []},
                {
                    "name": "pager",
                    "kind": ["bench"],
                    "required-features": ["pager"],
                },
            ],
        },
        {
            "name": "mz-compute",
            "targets": [
                {
                    "name": "correction",
                    "kind": ["bench"],
                    "required-features": ["bench"],
                },
                {"name": "some_test", "kind": ["test"], "required-features": []},
            ],
        },
    ]
}


def test_bench_targets_filters_kind_and_sorts() -> None:
    assert bench_targets(METADATA) == [
        BenchTarget("mz-compute", "correction", ("bench",)),
        BenchTarget("mz-ore", "id_gen", ()),
        BenchTarget("mz-ore", "pager", ("pager",)),
    ]


def test_bench_targets_tolerates_missing_required_features_key() -> None:
    metadata = {
        "packages": [{"name": "p", "targets": [{"name": "b", "kind": ["bench"]}]}]
    }
    assert bench_targets(metadata) == [BenchTarget("p", "b", ())]


def test_cargo_build_args_single_package_no_features() -> None:
    targets = [
        BenchTarget("mz-ore", "a", ()),
        BenchTarget("mz-ore", "b", ()),
    ]
    assert cargo_build_args(targets) == [
        "cargo",
        "bench",
        "--no-run",
        "--message-format=json",
        "--package",
        "mz-ore",
        "--bench",
        "a",
        "--bench",
        "b",
    ]


def test_cargo_build_args_multi_package_features() -> None:
    # Given in a deliberately unsorted order: the output must sort packages,
    # bench names, and features regardless of input order. "region" is
    # required by both "bytes" and "region_probe" in mz-ore, so "mz-ore/region"
    # must still appear exactly once in the output.
    targets = [
        BenchTarget("mz-ore", "pager", ("pager",)),
        BenchTarget("mz-compute", "correction", ("bench",)),
        BenchTarget("mz-ore", "bytes", ("bytes", "region", "tracing")),
        BenchTarget("mz-ore", "region_probe", ("region",)),
        BenchTarget("mz-ore", "id_gen", ()),
    ]
    assert cargo_build_args(targets) == [
        "cargo",
        "bench",
        "--no-run",
        "--message-format=json",
        "--package",
        "mz-compute",
        "--package",
        "mz-ore",
        "--bench",
        "bytes",
        "--bench",
        "correction",
        "--bench",
        "id_gen",
        "--bench",
        "pager",
        "--bench",
        "region_probe",
        "--features",
        "mz-compute/bench,mz-ore/bytes,mz-ore/pager,mz-ore/region,mz-ore/tracing",
    ]


def test_cargo_build_args_rejects_empty() -> None:
    with pytest.raises(ValueError, match="no bench targets to build"):
        cargo_build_args([])


def test_bench_executables_filters_and_resolves_package() -> None:
    manifests = {
        "/ws/src/repr/Cargo.toml": "mz-repr",
        "/ws/src/storage-types/Cargo.toml": "mz-storage-types",
    }
    lines = [
        json.dumps(
            {
                "reason": "compiler-artifact",
                "manifest_path": "/ws/src/repr/Cargo.toml",
                "target": {"kind": ["lib"], "name": "mz_repr"},
                "executable": None,
            }
        ),
        json.dumps(
            {
                "reason": "compiler-artifact",
                "manifest_path": "/ws/src/repr/Cargo.toml",
                "target": {"kind": ["bench"], "name": "row"},
                "executable": "/ws/target/release/deps/row-aaa",
            }
        ),
        json.dumps(
            {
                "reason": "compiler-artifact",
                "manifest_path": "/ws/src/storage-types/Cargo.toml",
                "target": {"kind": ["bench"], "name": "row"},
                "executable": "/ws/target/release/deps/row-bbb",
            }
        ),
        json.dumps({"reason": "build-script-executed"}),
        "warning: unused import",
    ]
    assert bench_executables(lines, manifests) == [
        BuiltBench(
            package="mz-repr",
            name="row",
            executable=Path("/ws/target/release/deps/row-aaa"),
            manifest_dir=Path("/ws/src/repr"),
        ),
        BuiltBench(
            package="mz-storage-types",
            name="row",
            executable=Path("/ws/target/release/deps/row-bbb"),
            manifest_dir=Path("/ws/src/storage-types"),
        ),
    ]


def test_package_manifests() -> None:
    metadata = {
        "packages": [
            {"name": "mz-ore", "manifest_path": "/ws/src/ore/Cargo.toml"},
            {"name": "mz-repr", "manifest_path": "/ws/src/repr/Cargo.toml"},
        ]
    }
    assert package_manifests(metadata) == {
        "/ws/src/ore/Cargo.toml": "mz-ore",
        "/ws/src/repr/Cargo.toml": "mz-repr",
    }
