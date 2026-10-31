# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Independent marker obligations for multi-table commit snapshots."""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass


@dataclass(frozen=True)
class Plan:
    shards: tuple[int, ...]
    value: int
    outcome: str
    retired: bool = False


def violations(
    plans: Mapping[int, Plan],
    rows: Iterable[tuple[int, int, int]],
    *,
    uncommitted: int | None = None,
) -> list[str]:
    """Check one common snapshot against attempted, acknowledged and retired writes."""
    seen: dict[int, set[int]] = defaultdict(set)
    errors = []
    counts: Counter[tuple[int, int]] = Counter()
    for shard, marker, value in rows:
        counts[(shard, marker)] += 1
        plan = plans.get(marker)
        if plan is None:
            errors.append(f"unattempted marker {marker}")
            continue
        seen[marker].add(shard)
        if shard not in plan.shards or value != plan.value:
            errors.append(f"changed transaction payload {marker}")
    if any(count != 1 for count in counts.values()):
        errors.append("duplicate transaction marker")
    for marker, plan in plans.items():
        visible = seen[marker]
        if visible and visible != set(plan.shards) and not plan.retired:
            errors.append(f"torn transaction {marker}")
        if visible and (plan.outcome == "rejected" or marker == uncommitted):
            errors.append(f"uncommitted or rolled-back transaction {marker}")
        if (
            plan.outcome in ("ok", "observed")
            and not plan.retired
            and visible != set(plan.shards)
        ):
            errors.append(
                f"lost acknowledged or previously observed transaction {marker}"
            )
    return errors
