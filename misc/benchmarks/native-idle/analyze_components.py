# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Disjoint logical dataset/activity accounting from reconciled event groups."""

import collections
import json
import sys
from pathlib import Path

LEGACY = {
    "mz_internal.mz_frontiers",
    "mz_catalog.mz_cluster_replica_frontiers",
    "mz_internal.mz_frontiers_raw",
    "mz_internal.mz_cluster_replica_frontiers_raw",
}


def reduce(run):
    audit = json.loads((run / "backend-analysis.json").read_text())
    names = {
        r[1]: ".".join(r[2:])
        for r in json.loads((run / "object-names.json").read_text())
    }
    shards = {
        r[1]: names.get(r[0], r[0])
        for r in json.loads((run / "shards.json").read_text())["rows"]
    }
    mv = set()
    wal = set()
    cache = set()
    for g in audit["all_groups"]:
        purpose = g["example"].get("purpose") or ""
        if purpose.startswith("MV sink "):
            mv.add(g["shard"])
        if purpose.startswith(("commit txns", "read txns")):
            wal.add(g["shard"])
        if "expression" in purpose.lower() or "durable cache" in purpose.lower():
            cache.add(g["shard"])

    def component(g):
        name = shards.get(g["shard"], "")
        if name == "mz_internal.mz_catalog_raw":
            return "catalog journal"
        if name == "mz_internal.mz_replica_frontier_observations":
            return "replica frontier observations"
        if name == "mz_internal.mz_storage_frontier_observations":
            return "cooperative storage observations"
        if name in LEGACY:
            return "ready-made / legacy frontier relations"
        if g["shard"] in mv and name.startswith(
            ("mz_catalog.", "mz_internal.", "pg_catalog.")
        ):
            return "durable catalog projection outputs"
        if g["shard"] in wal:
            return "table transaction WAL"
        if g["shard"] in cache:
            return "expression / plan cache"
        if name.startswith(
            ("mz_catalog.", "mz_internal.", "mz_introspection.", "pg_catalog.")
        ):
            return "other builtin introspection"
        if "." in name:
            return "user collections"
        if g["work_class"] == "liveness":
            return "blob liveness check"
        return "unmapped dataset"

    totals = collections.defaultdict(
        lambda: {
            "calls_per_second": 0.0,
            "bytes_per_second": 0.0,
            "events": 0,
            "bytes": 0,
        }
    )
    rows = []
    checks = []
    for w in audit["windows"]:
        accounted = collections.defaultdict(lambda: [0, 0])
        for g in w["groups"]:
            role = g["role"]
            op = g["op"]
            elapsed = w["roles"][role]["elapsed"]
            name = shards.get(g["shard"], g["shard"])
            label = component(g)
            key = (
                label,
                g["work_class"],
                role,
                op,
                g["append_kind"],
                g["command"],
                g["outcome"],
            )
            v = totals[key]
            v["calls_per_second"] += g["events"] / elapsed / len(audit["windows"])
            v["bytes_per_second"] += g["bytes_count"] / elapsed / len(audit["windows"])
            v["events"] += g["events"]
            v["bytes"] += g["bytes_count"]
            accounted[role, op][0] += g["events"]
            accounted[role, op][1] += g["bytes_count"]
            rows.append({"window": w["after"], "component": label, "object": name, **g})
        for role, v in w["roles"].items():
            for op, q in v["operations"].items():
                assert accounted[role, op] == [
                    q["nominal"]["events"],
                    q["nominal"]["bytes_count"],
                ], (role, op, accounted[role, op], q["nominal"])
                for field, c in q["reconciliation"].items():
                    if c["reconciled"] is not None:
                        assert c["reconciled"], (role, op, field, c)
                checks.append(
                    {
                        "window": w["after"],
                        "role": role,
                        "op": op,
                        "events": accounted[role, op][0],
                        "bytes": accounted[role, op][1],
                    }
                )
    report = {
        "run": str(run),
        "scope": "Backend completion events within nominal scrape bounds. No event is dropped or counted twice. Kind bytes in catalog counters are a different encoding measure.",
        "groups": [
            dict(
                zip(
                    [
                        "component",
                        "activity",
                        "role",
                        "op",
                        "append_kind",
                        "command",
                        "outcome",
                    ],
                    k,
                ),
                **v,
            )
            for k, v in totals.items()
        ],
        "conservation_checks": checks,
        "dataset_identities": shards,
        "classified_window_groups": rows,
    }
    (run / "logical-backend.json").write_text(json.dumps(report, indent=2) + "\n")
    for op in ["consensus_cas", "blob_get", "blob_set", "blob_delete"]:
        summary = collections.defaultdict(lambda: [0.0, 0.0])
        for g in report["groups"]:
            if g["op"] == op:
                summary[g["component"]][0] += g["calls_per_second"]
                summary[g["component"]][1] += g["bytes_per_second"] / 1024
        print(op, dict(summary))


if __name__ == "__main__":
    reduce(Path(sys.argv[1]))
