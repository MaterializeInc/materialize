# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Reference model for the incremental view maintenance oracle in `drivers/ivm.py`.

The oracle subscribes to one query that unions the base tables and every
maintained view, each row tagged with its relation, so a single
`SUBSCRIBE ... WITH (PROGRESS)` gives a consistent cut of inputs and outputs
at every timestamp. `Stream` accumulates that output and steps through each
closed timestamp; `ViewChecker` evaluates every view kind in Python over the
base table contents at that timestamp and compares.

Each view's SQL and its Python evaluator live next to each other in `VIEWS`,
and must agree exactly, including SQL's NULL, integer and multiset semantics.
Orderings in TopK cover every output column, so ties are between identical
rows and the expected multiset is deterministic.

Nothing here imports the Antithesis SDK or a database driver, so it is unit
tested on synthetic streams (`test_ivm_model.py`).
"""

from __future__ import annotations

import bisect
from collections import Counter, defaultdict
from collections.abc import Callable, Iterable, Iterator, Mapping
from dataclasses import dataclass, field
from typing import Any

SCHEMA = "ivm"
WIDTH = 5
"""Payload columns of the tagged union; narrower relations pad with NULL."""
TEMPORAL_LEAD_MS = 20_000
"""A row of `ev` is visible from `expires - TEMPORAL_LEAD_MS` through `expires`."""

Row = tuple[Any, ...]
Bag = Counter[Row]
Base = Mapping[str, Bag]


def sql_mod(a: int, b: int) -> int:
    """`a % b` for SQL integers: the result takes the sign of the dividend."""
    r = abs(a) % abs(b)
    return -r if a < 0 else r


def pad(row: Row) -> Row:
    return tuple(row) + (None,) * (WIDTH - len(row))


@dataclass(frozen=True)
class Table:
    name: str
    columns: tuple[str, ...]
    ddl: str


TABLES = (
    Table(
        "t",
        ("k", "g", "v", "w"),
        "k int NOT NULL, g int NOT NULL, v int NOT NULL, w bigint NOT NULL",
    ),
    Table("d", ("g", "label"), "g int NOT NULL, label bigint NOT NULL"),
    Table("ev", ("k", "expires"), "k int NOT NULL, expires bigint NOT NULL"),
    Table("e", ("src", "dst"), "src int NOT NULL, dst int NOT NULL"),
)
TABLE_NAMES = frozenset(t.name for t in TABLES)
ARITY = {t.name: len(t.columns) for t in TABLES}


def _filter_map(base: Base, _t: int) -> Bag:
    out: Bag = Counter()
    for (k, g, v, _w), n in base["t"].items():
        if sql_mod(v, 3) != 0:
            out[(k, v * 2 + g)] += n
    return out


def _by_group(bag: Bag) -> dict[Any, list[tuple[Row, int]]]:
    groups: dict[Any, list[tuple[Row, int]]] = defaultdict(list)
    for row, n in bag.items():
        groups[row[0]].append((row, n))
    return groups


def _inner_join(base: Base, _t: int) -> Bag:
    dims = _by_group(base["d"])
    out: Bag = Counter()
    for (k, g, v, _w), n in base["t"].items():
        for (_g, label), m in dims.get(g, []):
            out[(k, v, label)] += n * m
    return out


def _left_join(base: Base, _t: int) -> Bag:
    dims = _by_group(base["d"])
    out: Bag = Counter()
    for (k, g, _v, _w), n in base["t"].items():
        matches = dims.get(g, [])
        if not matches:
            out[(k, g, None)] += n
        for (_g, label), m in matches:
            out[(k, g, label)] += n * m
    return out


def _full_join(base: Base, _t: int) -> Bag:
    dims = _by_group(base["d"])
    facts = {row[1] for row in base["t"]}
    out: Bag = Counter()
    for (k, g, _v, _w), n in base["t"].items():
        matches = dims.get(g, [])
        if not matches:
            out[(k, g, None, None)] += n
        for (dg, label), m in matches:
            out[(k, g, dg, label)] += n * m
    for (dg, label), m in base["d"].items():
        if dg not in facts:
            out[(None, None, dg, label)] += m
    return out


def _reduce(base: Base, _t: int) -> Bag:
    groups: dict[int, list[tuple[int, int]]] = defaultdict(list)
    for (_k, g, v, _w), n in base["t"].items():
        groups[g].append((v, n))
    out: Bag = Counter()
    for g, vs in groups.items():
        count = sum(n for _, n in vs)
        total = sum(v * n for v, n in vs)
        out[(g, count, total, min(v for v, _ in vs), max(v for v, _ in vs))] += 1
    return out


def _global_reduce(base: Base, _t: int) -> Bag:
    rows = base["t"]
    count = sum(rows.values())
    total = sum(v * n for (_k, _g, v, _w), n in rows.items()) if count else None
    return Counter({(count, total): 1})


def _distinct(base: Base, _t: int) -> Bag:
    return Counter({(g, abs(v) % 4): 1 for (_k, g, v, _w) in base["t"]})


TOPK_LIMIT = 2


def _topk(base: Base, _t: int) -> Bag:
    groups: dict[int, list[Row]] = defaultdict(list)
    for (k, g, v, w), n in base["t"].items():
        groups[g].extend([(k, v, w)] * n)
    out: Bag = Counter()
    for g, rows in groups.items():
        rows.sort(key=lambda r: (-r[1], r[0], r[2]))
        for k, v, w in rows[:TOPK_LIMIT]:
            out[(g, k, v, w)] += 1
    return out


def _temporal(base: Base, t: int) -> Bag:
    out: Bag = Counter()
    for (k, expires), n in base["ev"].items():
        if expires - TEMPORAL_LEAD_MS <= t <= expires:
            out[(k, expires)] += n
    return out


def _recursive(base: Base, _t: int) -> Bag:
    edges = set(base["e"])
    succ: dict[int, set[int]] = defaultdict(set)
    for src, dst in edges:
        succ[src].add(dst)
    reach: set[Row] = set()
    for src in {s for s, _ in edges}:
        seen: set[int] = set()
        frontier = list(succ[src])
        while frontier:
            node = frontier.pop()
            if node in seen:
                continue
            seen.add(node)
            frontier.extend(succ[node])
        reach.update((src, dst) for dst in seen)
    return Counter({r: 1 for r in reach})


@dataclass(frozen=True)
class ViewKind:
    kind: str
    columns: tuple[str, ...]
    """Output columns, in the order the evaluator returns them."""
    sql: str
    inputs: frozenset[str]
    evaluate: Callable[[Base, int], Bag]
    time_dependent: bool = False
    """The view's contents depend on `mz_now()`, so they change at timestamps
    that carry no base table update."""


S = SCHEMA
VIEWS = (
    ViewKind(
        "filter_map",
        ("k", "x"),
        f"SELECT k, v * 2 + g AS x FROM {S}.t WHERE v % 3 <> 0",
        frozenset({"t"}),
        _filter_map,
    ),
    ViewKind(
        "inner_join",
        ("k", "v", "label"),
        f"SELECT t.k, t.v, d.label FROM {S}.t AS t JOIN {S}.d AS d ON t.g = d.g",
        frozenset({"t", "d"}),
        _inner_join,
    ),
    ViewKind(
        "left_join",
        ("k", "g", "label"),
        f"SELECT t.k, t.g, d.label FROM {S}.t AS t LEFT JOIN {S}.d AS d ON t.g = d.g",
        frozenset({"t", "d"}),
        _left_join,
    ),
    ViewKind(
        "full_join",
        ("k", "tg", "dg", "label"),
        f"SELECT t.k, t.g AS tg, d.g AS dg, d.label"
        f" FROM {S}.t AS t FULL OUTER JOIN {S}.d AS d ON t.g = d.g",
        frozenset({"t", "d"}),
        _full_join,
    ),
    ViewKind(
        "reduce",
        ("g", "n", "s", "lo", "hi"),
        f"SELECT g, count(*) AS n, sum(v) AS s, min(v) AS lo, max(v) AS hi"
        f" FROM {S}.t GROUP BY g",
        frozenset({"t"}),
        _reduce,
    ),
    ViewKind(
        "global_reduce",
        ("n", "s"),
        f"SELECT count(*) AS n, sum(v) AS s FROM {S}.t",
        frozenset({"t"}),
        _global_reduce,
    ),
    ViewKind(
        "distinct",
        ("g", "b"),
        f"SELECT DISTINCT g, abs(v) % 4 AS b FROM {S}.t",
        frozenset({"t"}),
        _distinct,
    ),
    ViewKind(
        "topk",
        ("g", "k", "v", "w"),
        f"SELECT grp.g, lat.k, lat.v, lat.w FROM (SELECT DISTINCT g FROM {S}.t) AS grp,"
        f" LATERAL (SELECT i.k, i.v, i.w FROM {S}.t AS i WHERE i.g = grp.g"
        f" ORDER BY i.v DESC, i.k, i.w LIMIT {TOPK_LIMIT}) AS lat",
        frozenset({"t"}),
        _topk,
    ),
    ViewKind(
        "temporal",
        ("k", "expires"),
        f"SELECT k, expires FROM {S}.ev"
        f" WHERE mz_now() >= expires - {TEMPORAL_LEAD_MS} AND mz_now() <= expires",
        frozenset({"ev"}),
        _temporal,
        time_dependent=True,
    ),
    ViewKind(
        "recursive",
        ("src", "dst"),
        f"WITH MUTUALLY RECURSIVE r (src int, dst int) AS ("
        f"SELECT src, dst FROM {S}.e UNION SELECT r.src, e.dst FROM r JOIN {S}.e AS e ON r.dst = e.src"
        f") SELECT src, dst FROM r",
        frozenset({"e"}),
        _recursive,
    ),
)
VIEWS_BY_KIND = {v.kind: v for v in VIEWS}

VARIANTS = ("index", "mv")
"""How each view kind is maintained: an indexed view, or a materialized view."""


def object_name(kind: str, variant: str) -> str:
    return f"{kind}_v" if variant == "index" else f"{kind}_mv"


def tag(kind: str, variant: str) -> str:
    return f"{kind}:{variant}"


VIEW_TAGS = tuple(tag(v.kind, variant) for v in VIEWS for variant in VARIANTS)


def _branch(tag_name: str, columns: Iterable[str], relation: str) -> str:
    cols = list(columns)
    exprs = [f"{c}::int8 AS c{i + 1}" for i, c in enumerate(cols)]
    exprs += [f"NULL::int8 AS c{i + 1}" for i in range(len(cols), WIDTH)]
    return (
        f"SELECT '{tag_name}'::text AS tag, {', '.join(exprs)} FROM {SCHEMA}.{relation}"
    )


def subscribe_query() -> str:
    """Union of every base table and every maintained view, tagged by relation.

    Output columns: `tag, c1, ..., c5`.
    """
    branches = [_branch(t.name, t.columns, t.name) for t in TABLES]
    for v in VIEWS:
        for variant in VARIANTS:
            branches.append(
                _branch(tag(v.kind, variant), v.columns, object_name(v.kind, variant))
            )
    return " UNION ALL ".join(branches)


def base_from_state(state: Mapping[str, Bag]) -> dict[str, Bag]:
    return {
        name: Counter(
            {row[: ARITY[name]]: n for row, n in state.get(name, Counter()).items()}
        )
        for name in TABLE_NAMES
    }


def reference(kind: str, base: Base, t: int) -> Bag:
    """Expected contents of a view kind at timestamp `t`, padded to `WIDTH`."""
    return Counter(
        {pad(r): n for r, n in VIEWS_BY_KIND[kind].evaluate(base, t).items() if n}
    )


def bag_diff(expected: Bag, observed: Bag, limit: int = 10) -> dict[str, Any]:
    missing = list((expected - observed).elements())
    extra = list((observed - expected).elements())
    return {
        "missing_count": len(missing),
        "extra_count": len(extra),
        "missing": [list(r) for r in missing[:limit]],
        "extra": [list(r) for r in extra[:limit]],
    }


@dataclass(frozen=True)
class Step:
    """The accumulated state is complete through `time`."""

    time: int
    changed: frozenset[str]
    """Tags with an update at exactly `time`."""


class Stream:
    """Accumulates `SUBSCRIBE ... AS OF as_of WITH (PROGRESS)` output.

    A progress row at `p` promises that no later row has a timestamp below `p`.
    `late` records data rows that break that promise, or that precede `as_of`;
    `regressions` records progress rows below an earlier one. Late rows are
    still accumulated, at the next `advance`, so later steps include them.
    """

    def __init__(self, as_of: int) -> None:
        self.as_of = as_of
        self.frontier: int | None = None
        self.pending: dict[int, Counter[tuple[str, Row]]] = defaultdict(Counter)
        self.state: dict[str, Bag] = defaultdict(Counter)
        self.late: list[dict[str, Any]] = []
        self.regressions: list[dict[str, Any]] = []

    def update(self, ts: int, tag_name: str, row: Row, diff: int) -> None:
        bound = self.as_of if self.frontier is None else max(self.as_of, self.frontier)
        if ts < bound:
            self.late.append(
                {
                    "ts": ts,
                    "frontier": self.frontier,
                    "as_of": self.as_of,
                    "tag": tag_name,
                }
            )
        self.pending[ts][(tag_name, row)] += diff

    def advance(self, ts: int) -> Iterator[Step]:
        """Apply every pending update below `ts`, one timestamp at a time.

        Yields after applying each timestamp, then once more at `ts - 1` if no
        update fell there, so time-dependent views are checked at the frontier.
        The caller must exhaust the iterator before feeding more rows.
        """
        if self.frontier is not None and ts < self.frontier:
            self.regressions.append({"progress": ts, "frontier": self.frontier})
            return
        self.frontier = ts
        last = None
        for time in sorted(t for t in self.pending if t < ts):
            updates = self.pending.pop(time)
            changed = set()
            for (tag_name, row), diff in updates.items():
                if diff == 0:
                    continue
                bag = self.state[tag_name]
                bag[row] += diff
                if bag[row] == 0:
                    del bag[row]
                changed.add(tag_name)
            last = time
            yield Step(time, frozenset(changed))
        if ts - 1 >= self.as_of and last != ts - 1:
            yield Step(ts - 1, frozenset())


def negatives(bag: Bag) -> list[tuple[Row, int]]:
    return [(r, n) for r, n in bag.items() if n < 0]


@dataclass
class TagStats:
    compared: int = 0
    nonempty: int = 0
    """Comparisons where the expected contents were non-empty."""
    changed_steps: int = 0
    """Comparisons at a timestamp that carried an update to this tag."""
    time_driven_changes: int = 0
    """Updates to this tag at a timestamp with no update to its inputs."""
    null_extended: int = 0
    """Comparisons where the expected contents held a NULL."""
    mismatch: dict[str, Any] | None = None
    negative: dict[str, Any] | None = None


@dataclass
class ViewChecker:
    """Compares every maintained view with its reference at each `Step`.

    A view is evaluated when its inputs or its own output changed, when it is
    time dependent, or on its first step; otherwise its comparison could not
    have changed since the last one.
    """

    stats: dict[str, TagStats] = field(
        default_factory=lambda: {
            t: TagStats() for t in (*sorted(TABLE_NAMES), *VIEW_TAGS)
        }
    )
    steps: int = 0
    skipped_negative_base: int = 0
    first_mismatch_time: int | None = None
    _seen: set[str] = field(default_factory=set)

    def check(self, state: Mapping[str, Bag], step: Step) -> None:
        self.steps += 1
        negative_base = False
        for name in sorted(TABLE_NAMES):
            neg = negatives(state.get(name, Counter()))
            if neg:
                negative_base = True
                self._note_negative(name, step.time, neg)
        if negative_base:
            self.skipped_negative_base += 1
            return
        base = base_from_state(state)
        changed_inputs = step.changed & TABLE_NAMES
        for view in VIEWS:
            expected: Bag | None = None
            for variant in VARIANTS:
                tag_name = tag(view.kind, variant)
                if not (
                    view.time_dependent
                    or tag_name in step.changed
                    or changed_inputs & view.inputs
                    or tag_name not in self._seen
                ):
                    continue
                self._seen.add(tag_name)
                if expected is None:
                    expected = reference(view.kind, base, step.time)
                observed = state.get(tag_name, Counter())
                s = self.stats[tag_name]
                s.compared += 1
                if expected:
                    s.nonempty += 1
                if any(None in r[: len(view.columns)] for r in expected):
                    s.null_extended += 1
                if tag_name in step.changed:
                    s.changed_steps += 1
                    if not changed_inputs & view.inputs:
                        s.time_driven_changes += 1
                neg = negatives(observed)
                if neg:
                    self._note_negative(tag_name, step.time, neg)
                if observed != expected and s.mismatch is None:
                    if self.first_mismatch_time is None:
                        self.first_mismatch_time = step.time
                    s.mismatch = {
                        "time": step.time,
                        "changed": sorted(step.changed),
                        "diff": bag_diff(expected, observed),
                        "expected_rows": sum(expected.values()),
                        "observed_rows": sum(observed.values()),
                    }

    def _note_negative(
        self, tag_name: str, time: int, neg: list[tuple[Row, int]]
    ) -> None:
        s = self.stats[tag_name]
        if s.negative is None:
            s.negative = {"time": time, "rows": [[list(r), n] for r, n in neg[:10]]}


def state_diff(
    expected: Mapping[str, Bag], observed: Mapping[str, Bag]
) -> dict[str, dict[str, Any]]:
    """Per tag, the difference between two accumulated states; empty if equal."""
    out = {}
    for name in sorted(set(expected) | set(observed)):
        e = +Counter(expected.get(name, Counter()))
        o = +Counter(observed.get(name, Counter()))
        if e != o:
            out[name] = bag_diff(e, o)
    return out


def encode_state(state: Mapping[str, Bag]) -> list[list[Any]]:
    return [
        [name, list(row), n]
        for name in sorted(state)
        for row, n in sorted(state[name].items(), key=repr)
        if n
    ]


def decode_state(encoded: Iterable[list[Any]]) -> dict[str, Bag]:
    out: dict[str, Bag] = defaultdict(Counter)
    for name, row, n in encoded:
        out[str(name)][tuple(row)] += int(n)
    return dict(out)


@dataclass(frozen=True)
class WriteOp:
    op_id: int
    outcome: str
    """`pending`, `ok`, `rejected`, or `indeterminate`."""
    invoke_rt: float
    complete_rt: float | None
    commit_ts: int | None


def realtime_order_violations(
    ops: Iterable[WriteOp], limit: int = 5
) -> list[dict[str, Any]]:
    """Pairs where a write acknowledged before another began committed later.

    `invoke_rt` and `complete_rt` are on one monotonic clock. A write that was
    not definitely rejected and has a commit timestamp counts as the later
    write even if its own outcome is unknown, because it committed after it
    was invoked.
    """
    committed = [o for o in ops if o.commit_ts is not None and o.outcome != "rejected"]
    acked = sorted(
        (o for o in committed if o.outcome == "ok" and o.complete_rt is not None),
        key=lambda o: o.complete_rt or 0.0,
    )
    completes = [o.complete_rt or 0.0 for o in acked]
    prefix: list[WriteOp] = []
    for o in acked:
        if not prefix or (o.commit_ts or 0) > (prefix[-1].commit_ts or 0):
            prefix.append(o)
        else:
            prefix.append(prefix[-1])
    out = []
    for b in committed:
        i = bisect.bisect_left(completes, b.invoke_rt)
        if i == 0:
            continue
        a = prefix[i - 1]
        assert a.commit_ts is not None and b.commit_ts is not None
        if a.commit_ts > b.commit_ts:
            out.append(
                {
                    "earlier": a.op_id,
                    "earlier_commit_ts": a.commit_ts,
                    "earlier_complete_rt": a.complete_rt,
                    "later": b.op_id,
                    "later_commit_ts": b.commit_ts,
                    "later_invoke_rt": b.invoke_rt,
                }
            )
            if len(out) >= limit:
                break
    return out


def written_ids(tag_name: str, row: Row) -> int | None:
    """The write id a base table row carries, if its table records one."""
    if tag_name == "t":
        return int(row[3])
    if tag_name == "d":
        return int(row[1])
    return None


def write_id_violations(
    seen: Mapping[int, set[int | None]], ledger: Mapping[int, WriteOp], limit: int = 10
) -> dict[str, list[Any]]:
    """Write ids seen in rows that no write could have produced.

    `seen` maps an id to the timestamps it was inserted at, `None` for an
    insertion folded into the `AS OF` snapshot. `rejected` lists ids of writes
    that failed with a definite error; `unknown` ids the ledger never
    allocated; `commit_ts_conflicts` ids inserted at two timestamps, or at a
    timestamp other than the one an earlier subscribe recorded.
    """
    rejected, unknown, conflicts = [], [], []
    for op_id in sorted(seen):
        op = ledger.get(op_id)
        if op is None:
            unknown.append(op_id)
            continue
        if op.outcome == "rejected":
            rejected.append(op_id)
        times = {t for t in seen[op_id] if t is not None}
        if op.commit_ts is not None:
            times.add(op.commit_ts)
        if len(times) > 1:
            conflicts.append({"op_id": op_id, "timestamps": sorted(times)})
    return {
        "rejected": rejected[:limit],
        "unknown": unknown[:limit],
        "commit_ts_conflicts": conflicts[:limit],
    }
