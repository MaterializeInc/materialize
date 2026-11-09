# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Partition symbolized sample weights once. Allocation paths are not object censuses."""

import collections
import json
import re
import sys
from pathlib import Path


def samples(run, role, kind):
    prefix = role + "-" + kind
    raw = (run / (prefix + "-raw.txt")).read_text()
    summary = json.loads((run / (prefix + "-analysis.json")).read_text())
    maps = {
        l.split()[0].rstrip(":")
        for l in raw.split("Mappings\n")[1].splitlines()
        if len(l.split()) >= 4 and l.split()[3] == summary["build_id"]
    }
    locations = [
        int(m[1])
        for m in re.finditer(r"^\s+(\d+): (0x[0-9a-f]+) M=(\d+)", raw, re.M)
        if m[3] in maps
    ]
    blocks = (run / (prefix + "-all-symbols.txt")).read_text().strip().split("\n\n")
    assert len(blocks) == len(locations), (role, kind, len(blocks), len(locations))
    symbols = {i: b.splitlines()[0] for i, b in zip(locations, blocks)}
    headers = raw.split("Samples:", 1)[1].strip().splitlines()[0].split()
    column = headers.index(summary["sample_type"])
    result = []
    for line in raw.split("Locations")[0].splitlines():
        m = re.match(r"\s+([0-9 ]+): ([0-9 ]+)\s*$", line)
        if m:
            values = [int(x) for x in m[1].split()]
            assert len(values) == len(headers)
            result.append(
                (values[column], [symbols.get(int(i), "?") for i in m[2].split()])
            )
    assert sum(w for w, _ in result) == summary["total_weight"]
    return summary, result


CPU_RULES = [
    (
        "read-protection publication",
        [
            "<mz_adapter::coord::Coordinator>::publish_read_protection",
            "<mz_clusterd::catalog_follower::execution::ReplicaEnactment>::publish",
        ],
    ),
    (
        "cooperative storage observations",
        [
            "mz_storage_controller::frontier_observations::",
            "<mz_storage_controller::frontier_observations::",
        ],
    ),
    (
        "replica observation publication",
        [
            "mz_clusterd::catalog_follower::execution::frontiers::",
            "<mz_clusterd::catalog_follower::execution::frontiers::",
        ],
    ),
    (
        "compute worker execution",
        [
            "<mz_compute::server::Worker>::run_commands",
            "<mz_compute::server::Config as mz_cluster::client::ClusterSpec>::run_worker",
        ],
    ),
    (
        "written-plan cache decoding",
        ["<mz_catalog::expr_cache::ExpressionCache>::read_plans"],
    ),
    (
        "catalog catch-up and application",
        [
            "<mz_catalog::catalog::Catalog>::sync_updates",
            "<mz_catalog::catalog::Catalog>::apply_next_committed_transaction",
        ],
    ),
    (
        "table WAL / group commit",
        [
            "<mz_adapter::coord::Coordinator>::stage_group_commit",
            "<mz_adapter::coord::Coordinator>::group_commit",
            "<mz_adapter::coord::Coordinator>::attempt_write_to_txns",
            "mz_txn_wal::",
            "<mz_txn_wal::",
        ],
    ),
    ("introspection collection manager", ["<mz_storage_controller::collection_mgmt::"]),
    (
        "Persist compaction",
        [
            "mz_persist_client::internal::compact::",
            "<mz_persist_client::internal::compact::",
        ],
    ),
    (
        "Persist GC",
        ["mz_persist_client::internal::gc::", "<mz_persist_client::internal::gc::"],
    ),
    (
        "PubSub dissemination and cache updates",
        ["mz_persist_client::rpc::", "<mz_persist_client::rpc::"],
    ),
    (
        "other replica catalog reconciliation",
        ["mz_clusterd::catalog_follower::", "<mz_clusterd::catalog_follower::"],
    ),
    (
        "other controller provisioning",
        ["mz_environmentd::controllerd::", "<mz_environmentd::controllerd::"],
    ),
    (
        "other coordinator work",
        ["<mz_adapter::coord::Coordinator>::serve", "mz_adapter::coord::serve"],
    ),
    (
        "command transport and services",
        [
            "mz_service::",
            "<mz_service::",
            "mz_compute_client::service::",
            "<mz_compute_client::service::",
        ],
    ),
    ("metadata PostgreSQL transport", ["tokio_postgres::", "<tokio_postgres::"]),
    (
        "other Persist work",
        [
            "mz_persist_client::",
            "<mz_persist_client::",
            "mz_persist::",
            "<mz_persist::",
        ],
    ),
]
HEAP_RULES = [
    (
        "async file-read buffers, caller detached",
        [
            "<tracing::instrument::Instrumented<tokio::runtime::blocking::task::BlockingTask<tokio::fs::read::read_spawn_blocking::"
        ],
    ),
    ("encoded Persist batches", ["mz_persist::indexed::columnar::parquet::"]),
    (
        "written/physical plan allocation",
        [
            "<mz_catalog::expr_cache::ExpressionCache>::read_plans",
            "<mz_compute_types::plan::LirRelation",
        ],
    ),
    (
        "read-protection bookkeeping",
        ["mz_catalog::read_protection::", "<mz_catalog::read_protection::"],
    ),
    (
        "catalog objects and SQL reconstruction",
        [
            "mz_catalog::catalog::state::",
            "<mz_catalog::memory::",
            "mz_catalog::memory::",
            "mz_sql::plan::",
            "<mz_sql::plan::",
        ],
    ),
    (
        "encoded blob fetch allocation",
        ["<mz_persist::file::FileBlob as mz_persist::location::Blob>::get"],
    ),
    (
        "compute data/arrangement allocation",
        [
            "mz_compute::render::",
            "<mz_compute::render::",
            "differential_dataflow::",
            "<differential_dataflow::",
            "mz_timely_util::",
            "<mz_timely_util::",
            "timely::",
            "<timely::",
            "mz_compute::logging::",
            "<mz_compute::logging::",
        ],
    ),
    (
        "Persist shard state / maintenance",
        ["mz_persist_client::internal::", "<mz_persist_client::internal::"],
    ),
    ("other catalog allocation", ["mz_catalog::", "<mz_catalog::"]),
    (
        "other Persist allocation",
        [
            "mz_persist_client::",
            "<mz_persist_client::",
            "mz_persist::",
            "<mz_persist::",
        ],
    ),
]


def classify(frames, kind):
    for label, prefixes in CPU_RULES if kind == "cpu" else HEAP_RULES:
        if any(f.startswith(tuple(prefixes)) for f in frames):
            return label
    if any(
        f.startswith("<mz_compute_types::plan::") and "deserialize" in f for f in frames
    ):
        return "plan deserialization, caller unclassified"
    return "runtime / other / unmapped"


def reduce(run, role, kind):
    summary, rows = samples(run, role, kind)
    totals = collections.Counter()
    limited = collections.Counter()
    examples = {}
    for weight, frames in rows:
        label = classify(frames, kind)
        totals[label] += weight
        if len(frames) >= 128:
            limited[label] += weight
        examples.setdefault(label, frames)
    assert sum(totals.values()) == summary["total_weight"]
    result = {
        "role": role,
        "kind": kind,
        "total_weight": summary["total_weight"],
        "sample_type": summary["sample_type"],
        "rules": CPU_RULES if kind == "cpu" else HEAP_RULES,
        "categories": [
            {
                "component": k,
                "weight": v,
                "fraction": v / summary["total_weight"],
                "depth_at_least_128_weight": limited[k],
            }
            for k, v in totals.most_common()
        ],
        "example_stacks": examples,
        "limits": [
            "Exclusive first-match classification by explicit stack frames, not per-object ownership.",
            "128-frame samples may omit callers. Unmatched samples remain visible.",
            "CPU samples are post-window. Their fractions are not measured per-component idle core counts.",
            "Heap samples identify allocation paths, not all retained references or RSS. Cache gauges overlap these allocations.",
        ],
    }
    (run / (role + "-" + kind + "-components.json")).write_text(
        json.dumps(result, indent=2) + "\n"
    )
    print(
        role,
        kind,
        [(r["component"], round(100 * r["fraction"], 1)) for r in result["categories"]],
    )


if __name__ == "__main__":
    reduce(Path(sys.argv[1]), sys.argv[2], sys.argv[3])
