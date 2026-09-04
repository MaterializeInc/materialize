# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.cargo_bench.targets import (
    BenchTarget,
    bench_targets,
    cargo_bench_args,
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
        "packages": [
            {"name": "p", "targets": [{"name": "b", "kind": ["bench"]}]}
        ]
    }
    assert bench_targets(metadata) == [BenchTarget("p", "b", ())]


def test_cargo_bench_args_without_features() -> None:
    target = BenchTarget("mz-ore", "id_gen", ())
    assert cargo_bench_args(target, ["--save-baseline", "ancestor"]) == [
        "cargo",
        "bench",
        "--package",
        "mz-ore",
        "--bench",
        "id_gen",
        "--",
        "--save-baseline",
        "ancestor",
    ]


def test_cargo_bench_args_with_features() -> None:
    target = BenchTarget("mz-ore", "bytes", ("bytes", "region", "tracing"))
    assert cargo_bench_args(target, []) == [
        "cargo",
        "bench",
        "--package",
        "mz-ore",
        "--bench",
        "bytes",
        "--features",
        "bytes,region,tracing",
        "--",
    ]
