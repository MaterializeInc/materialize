# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from __future__ import annotations

from collections import Counter

from materialize.antithesis import ivm_model as m


def base(**tables: list[tuple]) -> dict[str, Counter]:
    out = {name: Counter() for name in m.TABLE_NAMES}
    for name, rows in tables.items():
        out[name] = Counter(rows)
    return out


def ref(kind: str, b: dict[str, Counter], t: int = 0) -> Counter:
    return m.VIEWS_BY_KIND[kind].evaluate(b, t)


def test_sql_mod_takes_sign_of_dividend() -> None:
    assert m.sql_mod(7, 3) == 1
    assert m.sql_mod(-7, 3) == -1
    assert m.sql_mod(7, -3) == 1
    assert m.sql_mod(-6, 3) == 0


def test_filter_map_uses_sql_modulo() -> None:
    b = base(t=[(1, 2, -4, 9), (2, 0, 3, 9), (3, 1, 5, 9), (3, 1, 5, 9)])
    assert ref("filter_map", b) == Counter({(1, -6): 1, (3, 11): 2})


def test_joins_multiply_and_null_extend() -> None:
    b = base(t=[(1, 0, 10, 1), (2, 1, 20, 2)], d=[(0, 100), (0, 101), (5, 102)])
    assert ref("inner_join", b) == Counter({(1, 10, 100): 1, (1, 10, 101): 1})
    assert ref("left_join", b) == Counter(
        {(1, 0, 100): 1, (1, 0, 101): 1, (2, 1, None): 1}
    )
    assert ref("full_join", b) == Counter(
        {
            (1, 0, 0, 100): 1,
            (1, 0, 0, 101): 1,
            (2, 1, None, None): 1,
            (None, None, 5, 102): 1,
        }
    )


def test_reduce_counts_multiplicities() -> None:
    b = base(t=[(1, 0, 5, 1), (1, 0, 5, 1), (2, 0, -3, 2), (3, 1, 7, 3)])
    assert ref("reduce", b) == Counter({(0, 3, 7, -3, 5): 1, (1, 1, 7, 7, 7): 1})
    assert ref("global_reduce", b) == Counter({(4, 14): 1})


def test_global_reduce_of_empty_input_is_one_row() -> None:
    assert ref("global_reduce", base()) == Counter({(0, None): 1})


def test_distinct_collapses_duplicates() -> None:
    b = base(t=[(1, 0, 5, 1), (2, 0, -5, 2), (3, 0, 9, 3)])
    assert ref("distinct", b) == Counter({(0, 1): 1})


def test_topk_breaks_ties_on_every_column() -> None:
    b = base(
        t=[
            (5, 0, 10, 1),
            (4, 0, 10, 2),
            (4, 0, 10, 1),
            (1, 0, 3, 1),
            (7, 1, 2, 1),
            (7, 1, 2, 1),
            (8, 1, 2, 1),
        ]
    )
    assert ref("topk", b) == Counter(
        {(0, 4, 10, 1): 1, (0, 4, 10, 2): 1, (1, 7, 2, 1): 2}
    )


def test_temporal_bounds_are_inclusive() -> None:
    lead = m.TEMPORAL_LEAD_MS
    b = base(ev=[(1, 100_000), (2, 100_000), (3, 200_000)])
    assert ref("temporal", b, 100_000 - lead - 1) == Counter()
    assert ref("temporal", b, 100_000 - lead) == Counter(
        {(1, 100_000): 1, (2, 100_000): 1}
    )
    assert ref("temporal", b, 100_000) == Counter({(1, 100_000): 1, (2, 100_000): 1})
    assert ref("temporal", b, 100_001) == Counter()


def test_recursive_is_transitive_closure_set() -> None:
    b = base(e=[(0, 1), (1, 2), (1, 2), (2, 0), (3, 4)])
    expected = {(a, c) for a in (0, 1, 2) for c in (0, 1, 2)} | {(3, 4)}
    assert ref("recursive", b) == Counter({r: 1 for r in expected})


def test_subscribe_query_covers_every_relation() -> None:
    q = m.subscribe_query()
    for t in m.TABLES:
        assert f"FROM {m.SCHEMA}.{t.name}" in q
    for v in m.VIEWS:
        assert f"'{v.kind}:index'::text" in q and f"FROM {m.SCHEMA}.{v.kind}_v" in q
        assert f"'{v.kind}:mv'::text" in q and f"FROM {m.SCHEMA}.{v.kind}_mv" in q
        assert len(v.columns) <= m.WIDTH
    assert q.count(" UNION ALL ") == len(m.TABLES) + 2 * len(m.VIEWS) - 1


def feed(stream: m.Stream, events: list[tuple]) -> list[m.Step]:
    """Events are `("p", ts)` for progress and `(ts, tag, row, diff)` for data."""
    steps = []
    for e in events:
        if e[0] == "p":
            steps.extend(stream.advance(e[1]))
        else:
            stream.update(e[0], e[1], m.pad(e[2]), e[3])
    return steps


def test_stream_steps_through_each_closed_timestamp() -> None:
    s = m.Stream(as_of=10)
    steps = feed(
        s,
        [
            ("p", 10),
            (10, "t", (1, 0, 5, 1), 1),
            (12, "t", (2, 0, 5, 2), 1),
            (12, "t", (2, 0, 5, 2), 1),
            (13, "t", (1, 0, 5, 1), -1),
            ("p", 13),
        ],
    )
    assert [st.time for st in steps] == [10, 12]
    assert steps[1].changed == frozenset({"t"})
    assert s.state["t"] == Counter({m.pad((1, 0, 5, 1)): 1, m.pad((2, 0, 5, 2)): 2})
    steps = feed(s, [("p", 20)])
    assert [st.time for st in steps] == [13, 19]
    assert steps[1].changed == frozenset()
    assert s.state["t"] == Counter({m.pad((2, 0, 5, 2)): 2})
    assert not s.late and not s.regressions


def test_stream_first_progress_at_as_of_closes_nothing() -> None:
    s = m.Stream(as_of=10)
    assert feed(s, [("p", 10)]) == []


def test_stream_flags_late_updates_and_regressions() -> None:
    s = m.Stream(as_of=10)
    feed(s, [(9, "t", (1, 0, 0, 1), 1), ("p", 15), (14, "d", (1, 2), 1)])
    assert [e["ts"] for e in s.late] == [9, 14]
    feed(s, [(15, "d", (1, 3), 1), ("p", 12)])
    assert len(s.late) == 2
    assert s.regressions == [{"progress": 12, "frontier": 15}]
    assert s.frontier == 15


def test_stream_drops_consolidated_rows() -> None:
    s = m.Stream(as_of=0)
    feed(s, [(1, "t", (1, 0, 0, 1), 1), (1, "t", (1, 0, 0, 1), -1), ("p", 2)])
    assert s.state["t"] == Counter()


def view_rows(kind: str, b: dict[str, Counter], t: int) -> list[tuple]:
    out = []
    for variant in m.VARIANTS:
        for row, n in m.reference(kind, b, t).items():
            out.extend([(m.tag(kind, variant), row)] * n)
    return out


def snapshot_events(ts: int, b: dict[str, Counter]) -> list[tuple]:
    events = []
    for name, bag in b.items():
        for row, n in bag.items():
            events.append((ts, name, row, n))
    for v in m.VIEWS:
        for tag_name, row in view_rows(v.kind, b, ts):
            events.append((ts, tag_name, row, 1))
    return events


def run_checker(events: list[tuple], as_of: int) -> tuple[m.Stream, m.ViewChecker]:
    s = m.Stream(as_of)
    c = m.ViewChecker()
    for e in events:
        if e[0] == "p":
            for step in s.advance(e[1]):
                c.check(s.state, step)
        else:
            s.update(e[0], e[1], m.pad(e[2]), e[3])
    return s, c


def test_checker_accepts_correct_stream() -> None:
    b = base(
        t=[(1, 0, 5, 1), (2, 1, -4, 2)],
        d=[(0, 7)],
        ev=[(1, 15)],
        e=[(0, 1), (1, 2)],
    )
    events = [("p", 10), *snapshot_events(10, b), ("p", 11)]
    s, c = run_checker(events, 10)
    assert c.steps == 1
    assert c.first_mismatch_time is None
    assert all(st.compared == 1 for t, st in c.stats.items() if ":" in t)
    assert c.stats["global_reduce:mv"].nonempty == 1
    assert c.stats["left_join:index"].null_extended == 1
    assert c.stats["temporal:index"].nonempty == 1


def test_checker_finds_missing_view_update() -> None:
    b = base(t=[(1, 0, 5, 1)])
    events = [("p", 10), *snapshot_events(10, b), ("p", 11)]
    # The base row changes at 12 and only the indexed reduce view follows.
    events += [
        (12, "t", (1, 0, 5, 1), -1),
        (12, "t", (1, 0, 8, 3), 1),
        (12, "reduce:index", m.pad((0, 1, 5, 5, 5)), -1),
        (12, "reduce:index", m.pad((0, 1, 8, 8, 8)), 1),
        ("p", 13),
    ]
    _, c = run_checker(events, 10)
    assert c.stats["reduce:index"].mismatch is None
    mismatch = c.stats["reduce:mv"].mismatch
    assert mismatch is not None and mismatch["time"] == 12
    assert mismatch["diff"]["missing"] == [[0, 1, 8, 8, 8]]
    assert c.first_mismatch_time == 12
    assert c.stats["filter_map:mv"].mismatch is not None


def test_checker_catches_missing_temporal_retraction() -> None:
    lead = m.TEMPORAL_LEAD_MS
    b = base(ev=[(1, lead + 100)])
    events = [("p", lead + 50), *snapshot_events(lead + 50, b), ("p", lead + 51)]
    # Only the MV retracts the row when it expires; no base table changes.
    events += [
        (lead + 101, "temporal:mv", m.pad((1, lead + 100)), -1),
        ("p", lead + 150),
    ]
    _, c = run_checker(events, lead + 50)
    assert c.stats["temporal:mv"].mismatch is None
    assert c.stats["temporal:mv"].time_driven_changes == 1
    mismatch = c.stats["temporal:index"].mismatch
    assert mismatch is not None and mismatch["time"] == lead + 101


def test_checker_reports_negative_multiplicity() -> None:
    events = [
        ("p", 0),
        *snapshot_events(0, base()),
        (1, "distinct:index", (3, 1), -1),
        ("p", 2),
    ]
    _, c = run_checker(events, 0)
    neg = c.stats["distinct:index"].negative
    assert neg is not None and neg["time"] == 1


def test_checker_skips_views_over_negative_base() -> None:
    events = [("p", 0), (0, "t", (1, 0, 0, 1), -1), ("p", 1)]
    _, c = run_checker(events, 0)
    assert c.skipped_negative_base == 1
    assert c.stats["t"].negative is not None
    assert c.stats["reduce:index"].compared == 0


def test_state_round_trips_through_json_encoding() -> None:
    st = {
        "t": Counter({m.pad((1, 0, 5, 1)): 2}),
        "left_join:mv": Counter({m.pad((1, 0, None)): 1}),
    }
    assert m.state_diff(st, m.decode_state(m.encode_state(st))) == {}
    other = {"t": Counter({m.pad((1, 0, 5, 1)): 1})}
    diff = m.state_diff(st, other)
    assert set(diff) == {"t", "left_join:mv"}


def op(
    op_id: int, outcome: str, invoke: float, complete: float | None, ts: int | None
) -> m.WriteOp:
    return m.WriteOp(op_id, outcome, invoke, complete, ts)


def test_realtime_order_accepts_overlapping_writes() -> None:
    ops = [
        op(1, "ok", 0.0, 2.0, 200),
        op(2, "ok", 1.0, 3.0, 100),
        op(3, "ok", 4.0, 5.0, 200),
        op(4, "indeterminate", 6.0, None, 300),
    ]
    assert m.realtime_order_violations(ops) == []


def test_realtime_order_flags_later_write_committed_earlier() -> None:
    ops = [
        op(1, "ok", 0.0, 2.0, 200),
        op(2, "pending", 3.0, None, 150),
        op(3, "rejected", 3.0, 4.0, 100),
    ]
    out = m.realtime_order_violations(ops)
    assert [(v["earlier"], v["later"]) for v in out] == [(1, 2)]


def test_write_ids_flag_rejected_unknown_and_conflicting() -> None:
    ledger = {
        1: op(1, "ok", 0.0, 1.0, None),
        2: op(2, "rejected", 0.0, 1.0, None),
        3: op(3, "indeterminate", 0.0, None, 50),
        4: op(4, "ok", 0.0, 1.0, 70),
    }
    seen: dict[int, set[int | None]] = {
        1: {None, 10},
        2: {None},
        3: {60},
        4: {None, 70},
        9: {None},
    }
    out = m.write_id_violations(seen, ledger)
    assert out["rejected"] == [2]
    assert out["unknown"] == [9]
    assert out["commit_ts_conflicts"] == [{"op_id": 3, "timestamps": [50, 60]}]


def test_written_ids() -> None:
    assert m.written_ids("t", m.pad((1, 2, 3, 44))) == 44
    assert m.written_ids("d", m.pad((1, 55))) == 55
    assert m.written_ids("ev", m.pad((1, 2))) is None
