# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Certify observed record kinds by matching complete prefixes to committed appends."""

import collections
import gzip
import json
import re
import sys
from pathlib import Path


def reduce(run):
    phase = json.loads((run / "publication-result.json").read_text())
    assert phase["status"] == "passed"
    shards = json.loads((run / "logical-backend.json").read_text())[
        "dataset_identities"
    ]
    raw = next(k for k, v in shards.items() if v == "mz_internal.mz_catalog_raw")
    commits = {}
    with gzip.open(run / "backend-events.jsonl.gz", "rt") as f:
        for line in f:
            e = json.loads(line)
            if (
                e.get("op") != "consensus_cas"
                or e["shard"] != raw
                or e["outcome"] != "committed"
                or e["append_kind"] != "data"
            ):
                continue
            values = {v["value"] for v in e["context_fields"]["upper"]}
            assert len(values) == 1
            upper = int(
                re.fullmatch(r"Antichain \{ elements: \[(\d+)\] \}", values.pop())[1]
            )
            assert upper not in commits
            commits[upper] = {
                "updates": e["updates"],
                "role": e["role"],
                "time": e["time"],
                "line": e["line"],
            }
    covered = collections.defaultdict(set)
    accepted = []
    rejected = []
    equations = []
    with (run / "runtime.log").open() as f:
        for lineno, line in enumerate(f, 1):
            if "catalog committed prefix applied" not in line:
                continue
            m = re.search(
                r"from_position=.*upper: (\d+) \}\) upper=(\d+).*update_kinds=\{(.*?)\} grant_incarnations=",
                line,
            )
            assert m, line
            lo, hi = int(m[1]), int(m[2])
            counts = collections.Counter()
            for kind, diff, count in re.findall(
                r'\("(\w+)", (Retraction|Addition)\): (\d+)', m[3]
            ):
                counts[kind] += int(count)
            ids = {u for u in commits if lo < u <= hi}
            expected = sum(commits[u]["updates"] for u in ids)
            record = {
                "line": lineno,
                "from": lo,
                "through": hi,
                "commits": sorted(ids),
                "counts": dict(counts),
                "committed_rows": expected,
            }
            if expected != sum(counts.values()):
                rejected.append(record)
                continue
            accepted.append(record)
            equations.append((ids, counts))
            for u in ids:
                covered[u].update(counts)
    selected = {
        u: e
        for u, e in commits.items()
        if phase["start_wall"] < e["time"] < phase["end_wall"]
    }
    solved = {}
    changed = True
    while changed:
        changed = False
        for ids, counts in equations:
            rest = ids - solved.keys()
            remaining = counts.copy()
            for u in ids & solved.keys():
                remaining.subtract(solved[u])
            assert all(v >= 0 for v in remaining.values())
            if len(rest) == 1:
                u = rest.pop()
                assert sum(remaining.values()) == commits[u]["updates"]
                solved[u] = remaining
                changed = True
    by_role = collections.defaultdict(collections.Counter)
    for u in selected:
        if u in solved:
            by_role[selected[u]["role"]].update(solved[u])
    result = {
        "scope": "Separate post-window DEBUG diagnostic. Prefixes are validated against committed upper ranges, never summed across followers.",
        "selected_commits": len(selected),
        "selected_rows": sum(e["updates"] for e in selected.values()),
        "covered_commits": sum(u in covered for u in selected),
        "solved_commits": sum(u in solved for u in selected),
        "possible_kinds": sorted(
            set.union(set(), *(covered.get(u, set()) for u in selected))
        ),
        "solved_kinds_by_role": {k: dict(v) for k, v in by_role.items()},
        "uncovered": [e for u, e in selected.items() if not covered.get(u)],
        "unsolved": [e for u, e in selected.items() if u not in solved],
        "accepted_prefixes": accepted,
        "rejected_prefixes": rejected,
    }
    (run / "publication-coverage.json").write_text(json.dumps(result, indent=2) + "\n")
    print(
        {
            k: v
            for k, v in result.items()
            if k not in ["accepted_prefixes", "rejected_prefixes"]
        }
    )
    print("rejected", len(rejected))


if __name__ == "__main__":
    reduce(Path(sys.argv[1]))
