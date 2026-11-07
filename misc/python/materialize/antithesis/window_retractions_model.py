# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Input-derived window semantics and generated partition-changing mutations."""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

Input = tuple[int, int | None, int | None, int | None, int | None, int]
Output = tuple[int | None, ...]
INPUT_COLUMNS = "id,g,o,v,off,def"
OUTPUT_COLUMNS = (
    INPUT_COLUMNS + ",rn,rk,dr,lag_v,lead_v,lag_nn,lead_nn,n,s,fst,lst,peer_s"
)
WIDTH = len(OUTPUT_COLUMNS.split(","))


@dataclass(frozen=True)
class Spec:
    descending: bool
    nulls_first: bool
    lower: int
    upper: int

    def order(self, *, unique: bool) -> str:
        return (
            f"o {'DESC' if self.descending else 'ASC'}"
            f" NULLS {'FIRST' if self.nulls_first else 'LAST'}"
            + (", id" if unique else "")
        )

    @property
    def frame(self) -> str:
        def bound(offset: int) -> str:
            return (
                "CURRENT ROW"
                if offset == 0
                else f"{abs(offset)} {'PRECEDING' if offset < 0 else 'FOLLOWING'}"
            )

        return f"ROWS BETWEEN {bound(self.lower)} AND {bound(self.upper)}"


@dataclass(frozen=True)
class Mutation:
    key: int
    row: Input | None


@dataclass(frozen=True)
class Plan:
    spec: Spec
    seed: tuple[Input, ...]
    phases: tuple[tuple[Mutation, ...], ...]


def query(relation: str, spec: Spec) -> str:
    window = f"(PARTITION BY g ORDER BY {spec.order(unique=True)})"
    peer = f"(PARTITION BY g ORDER BY {spec.order(unique=False)})"
    frame = f"(PARTITION BY g ORDER BY {spec.order(unique=True)} {spec.frame})"
    return (
        f"SELECT {INPUT_COLUMNS}, row_number() OVER {window} AS rn,"
        f" rank() OVER {peer} AS rk, dense_rank() OVER {peer} AS dr,"
        f" lag(v,off,def) OVER {window} AS lag_v, lead(v,off,def) OVER {window} AS lead_v,"
        f" lag(v,off,def) IGNORE NULLS OVER {window} AS lag_nn,"
        f" lead(v,off,def) IGNORE NULLS OVER {window} AS lead_nn,"
        f" count(v) OVER {frame} AS n, sum(v) OVER {frame} AS s,"
        f" first_value(v) OVER {frame} AS fst, last_value(v) OVER {frame} AS lst,"
        f" sum(v) OVER {peer} AS peer_s FROM {relation}"
    )


def apply(rows: Sequence[Input], mutations: Sequence[Mutation]) -> tuple[Input, ...]:
    current = {row[0]: row for row in rows}
    for mutation in mutations:
        if mutation.row is None:
            current.pop(mutation.key, None)
        else:
            if mutation.row[0] != mutation.key:
                raise ValueError("mutation identity differs from its row")
            current[mutation.key] = mutation.row
    return tuple(current[key] for key in sorted(current))


def evaluate(rows: Sequence[Input], spec: Spec) -> Counter[Output]:
    partitions: dict[int | None, list[Input]] = defaultdict(list)
    for row in rows:
        partitions[row[1]].append(row)
    result: Counter[Output] = Counter()
    for partition in partitions.values():
        partition.sort(
            key=lambda row: (
                (row[2] is not None) if spec.nulls_first else (row[2] is None),
                0 if row[2] is None else (-row[2] if spec.descending else row[2]),
                row[0],
            )
        )
        peers = list(dict.fromkeys(row[2] for row in partition))
        for index, row in enumerate(partition):
            peer = row[2]
            rank = next(i + 1 for i, other in enumerate(partition) if other[2] == peer)
            dense = peers.index(peer) + 1

            def neighbor(lead: bool, ignore_nulls: bool) -> int | None:
                offset = row[4]
                if offset is None:
                    return None
                distance = offset if lead else -offset
                if not ignore_nulls:
                    target = index + distance
                    return (
                        partition[target][3] if 0 <= target < len(partition) else row[5]
                    )
                if not distance:
                    raise ValueError(
                        "IGNORE NULLS zero-offset semantics are unspecified"
                    )
                step = 1 if distance > 0 else -1
                candidates = range(
                    index + step, len(partition) if step > 0 else -1, step
                )
                nonnull = [
                    partition[i][3] for i in candidates if partition[i][3] is not None
                ]
                return (
                    nonnull[abs(distance) - 1]
                    if len(nonnull) >= abs(distance)
                    else row[5]
                )

            frame = partition[
                max(0, index + spec.lower) : max(
                    0, min(len(partition), index + spec.upper + 1)
                )
            ]
            values = [member[3] for member in frame if member[3] is not None]
            end_peer = max(i for i, member in enumerate(partition) if member[2] == peer)
            peer_values = [
                member[3]
                for member in partition[: end_peer + 1]
                if member[3] is not None
            ]
            result[
                (
                    *row,
                    index + 1,
                    rank,
                    dense,
                    neighbor(False, False),
                    neighbor(True, False),
                    neighbor(False, True),
                    neighbor(True, True),
                    len(values),
                    sum(values) if values else None,
                    frame[0][3] if frame else None,
                    frame[-1][3] if frame else None,
                    sum(peer_values) if peer_values else None,
                )
            ] += 1
    return result


def generate(draw: Any) -> Plan:
    lower, upper = draw.choice(((-3, -1), (1, 3), (-2, 1)))
    spec = Spec(draw.choice((False, True)), draw.choice((False, True)), lower, upper)
    rows: list[Input] = []
    for group in (None, 0, 1):
        for position in range(draw.randint(4, 7)):
            key = len(rows) + 1
            rows.append(
                (
                    key,
                    group,
                    None if position < 2 else draw.choice((None, -1, 0, 1)),
                    None if position < 2 else draw.randint(-20, 20),
                    draw.choice((None, -2, -1, 1, 2)),
                    100 + key,
                )
            )
    seed = tuple(rows)
    values = tuple(
        Mutation(
            row[0],
            (
                row[0],
                row[1],
                row[2],
                draw.randint(-20, 20) if row[3] is None else None,
                row[4],
                -row[5],
            ),
        )
        for row in rows[::3]
    )
    current = apply(seed, values)
    moved = tuple(
        Mutation(
            row[0],
            (
                row[0],
                (0 if row[1] is None else (1 if row[1] == 0 else None)),
                draw.choice((None, -2, 2)),
                row[3],
                row[4],
                row[5],
            ),
        )
        for row in current[::4]
    )
    current = apply(current, moved)
    deleted = tuple(Mutation(row[0], None) for row in current if row[1] is None)
    removed = [row for row in current if row[1] is None]
    inserted = tuple(
        Mutation(
            row[0],
            (
                row[0],
                row[1],
                draw.choice((None, -1, 1)),
                draw.choice((None, draw.randint(-20, 20))),
                row[4],
                row[5],
            ),
        )
        for row in removed
    )
    restored = tuple(Mutation(row[0], row) for row in seed)
    return Plan(spec, seed, (values, moved, deleted, inserted, restored))


@dataclass
class Stream:
    frontier: int | None = None
    start: int | None = None
    bag: Counter[Output] = field(default_factory=Counter)
    pending: dict[int, Counter[Output]] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)
    rows: int = 0
    retracted: bool = False

    def feed(self, batch: Sequence[Sequence[Any]]) -> None:
        def integer(value: Any) -> int:
            if value is None or isinstance(value, bool) or int(value) != value:
                raise ValueError("not an integer")
            return int(value)

        for values in batch:
            self.rows += 1
            if self.rows > 8192:
                raise OverflowError("window subscription capture exceeded its bound")
            try:
                if len(values) != WIDTH + 3 or type(values[1]) is not bool:
                    raise ValueError("invalid subscription shape")
                timestamp = integer(values[0])
                progressed = values[1]
                if timestamp < 0:
                    raise ValueError("negative timestamp")
                if progressed:
                    if self.start is None:
                        self.start = timestamp
                    if self.frontier is not None and timestamp < self.frontier:
                        self.errors.append("subscription progress regressed")
                    for stamp in sorted(list(self.pending)):
                        if stamp < timestamp:
                            self.bag.update(self.pending.pop(stamp))
                            if any(count < 0 for count in self.bag.values()):
                                self.errors.append(
                                    "closed output has negative multiplicity"
                                )
                    self.bag = +self.bag
                    self.frontier = timestamp
                    continue
                diff = integer(values[2])
                row = tuple(
                    None if value is None else integer(value) for value in values[3:]
                )
                if row[0] is None or diff == 0:
                    raise ValueError("invalid data identity or multiplicity")
                if self.start is None:
                    self.errors.append("subscription data preceded initial progress")
                if self.frontier is not None and timestamp < self.frontier:
                    self.errors.append(
                        "subscription data arrived below closed progress"
                    )
                self.retracted |= diff < 0
                self.pending.setdefault(timestamp, Counter())[row] += diff
            except (TypeError, ValueError, OverflowError):
                self.errors.append("malformed subscription row")
