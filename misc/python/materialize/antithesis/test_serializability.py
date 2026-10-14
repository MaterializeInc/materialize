# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from __future__ import annotations

import random
import time
from dataclasses import dataclass, field, replace

from materialize.antithesis.serializability import (
    ANOMALIES,
    RW,
    SERIALIZABLE,
    STRICT,
    STRONG_SESSION,
    WR,
    WW,
    Kind,
    Observation,
    Op,
    Outcome,
    Report,
    check,
    classify_component,
    realtime_edges,
    realtime_timestamp_order,
    session_timestamp_order,
    strongly_connected_components,
)

OK = Outcome.OK
FAIL = Outcome.FAIL
INFO = Outcome.INFO


def append(
    op_id: int,
    key: int,
    invoke: float,
    complete: float | None,
    outcome: Outcome = OK,
    isolation: str = STRICT,
    session: str = "",
    endpoint: str = "service",
) -> Op:
    return Op(
        op_id,
        Kind.APPEND,
        outcome,
        invoke,
        complete,
        session=session or f"s{op_id}",
        isolation=isolation,
        write_key=key,
        endpoint=endpoint,
    )


def rmw(
    op_id: int,
    write_key: int,
    read_key: int,
    invoke: float,
    complete: float | None,
    outcome: Outcome = OK,
) -> Op:
    return Op(
        op_id,
        Kind.RMW,
        outcome,
        invoke,
        complete,
        session=f"s{op_id}",
        write_key=write_key,
        read_key=read_key,
    )


def read(
    op_id: int,
    invoke: float,
    complete: float,
    observed: dict[int, list[int]],
    counts: dict[int, int] | None = None,
    ts: int | None = None,
    isolation: str = STRICT,
    session: str = "",
    path: str = "table",
    endpoint: str = "service",
) -> Op:
    counts = counts or {}
    observations = tuple(
        Observation(
            key,
            path,
            tuple(values),
            tuple((v, counts[v]) for v in values if v in counts),
        )
        for key, values in observed.items()
    )
    return Op(
        op_id,
        Kind.READ,
        OK,
        invoke,
        complete,
        session=session or f"s{op_id}",
        isolation=isolation,
        observations=observations,
        ts=ts,
        endpoint=endpoint,
    )


def problems(report: Report) -> dict[str, object]:
    found: dict[str, object] = {
        name: getattr(report, name)
        for name in (
            "phantom_reads",
            "aborted_reads",
            "duplicates",
            "internal",
            "incompatible_orders",
            "rmw_self_reads",
            "stale_reads",
            "lost_writes",
            "timestamp_regressions",
            "indeterminate_regressions",
            "same_timestamp_mismatches",
        )
        if getattr(report, name)
    }
    found.update({k: v for k, v in report.anomaly_counts.items() if v})
    return found


def assert_clean(report: Report) -> None:
    assert problems(report) == {}


def only_anomaly(report: Report) -> str:
    fired = [name for name, count in report.anomaly_counts.items() if count]
    assert len(fired) == 1, report.anomaly_counts
    return fired[0]


def test_sequential_history_is_clean() -> None:
    report = check(
        [
            append(1, 0, 0, 1),
            read(2, 2, 3, {0: [1]}, ts=10),
            append(3, 1, 4, 5),
            read(4, 6, 7, {0: [1], 1: [3]}, ts=20),
        ]
    )
    assert_clean(report)
    assert report.stats.multi_key_strict_reads == 1
    assert report.stats.strict_reads_after_ack == 3


def test_concurrent_appends_in_one_layer_are_unordered() -> None:
    report = check(
        [
            append(1, 0, 0, 3),
            append(2, 0, 0, 3),
            read(3, 1, 2, {0: []}),
            read(4, 4, 5, {0: [2, 1]}),
        ]
    )
    assert_clean(report)


def test_indeterminate_write_may_commit_after_the_error() -> None:
    report = check(
        [
            append(1, 0, 0, 1, outcome=INFO),
            read(2, 2, 3, {0: []}, ts=10),
            read(3, 4, 5, {0: [1]}, ts=20),
        ]
    )
    assert_clean(report)
    assert report.stats.indeterminate_observed == 1


def test_unobserved_indeterminate_and_pending_writes_are_ignored() -> None:
    report = check(
        [
            append(1, 0, 0, 1, outcome=INFO),
            append(2, 0, 0, None, outcome=INFO),
            read(3, 2, 3, {0: []}),
            read(4, 4, 5, {0: [2]}),
        ]
    )
    assert_clean(report)


def test_rejected_write_never_observed_is_clean() -> None:
    report = check([append(1, 0, 0, 1, outcome=FAIL), read(2, 2, 3, {0: []})])
    assert_clean(report)


def test_observed_rejected_write_is_g1a() -> None:
    report = check([append(1, 0, 0, 1, outcome=FAIL), read(2, 2, 3, {0: [1]})])
    assert [a["value"] for a in report.aborted_reads] == [1]


def test_phantom_values() -> None:
    report = check(
        [
            append(1, 1, 0, 1),
            append(2, 0, 10, 11),
            read(3, 2, 3, {0: [1, 2, 99]}),
        ]
    )
    assert sorted(p["value"] for p in report.phantom_reads) == [1, 2, 99]


def test_duplicate_value_in_one_read() -> None:
    report = check([append(1, 0, 0, 1), read(2, 2, 3, {0: [1, 1]})])
    assert report.duplicates == [{"read": 2, "key": 0, "path": "table", "values": [1]}]


def test_incompatible_orders() -> None:
    report = check(
        [
            append(1, 0, 0, 1),
            append(2, 0, 0, 1),
            read(3, 2, 3, {0: [1]}, isolation=SERIALIZABLE),
            read(4, 2, 3, {0: [2]}, isolation=SERIALIZABLE),
        ]
    )
    assert len(report.incompatible_orders) == 1
    assert report.incompatible_orders[0]["key"] == 0


def test_paths_disagree_within_one_read() -> None:
    op = Op(
        3,
        Kind.READ,
        OK,
        2,
        3,
        observations=(
            Observation(0, "table", (1, 2)),
            Observation(0, "mv", (1,)),
        ),
    )
    report = check([append(1, 0, 0, 1), append(2, 0, 0, 1), op])
    assert len(report.internal) == 1
    assert report.internal[0]["paths"] == ["table", "mv"]


def test_g0_on_a_write_cycle() -> None:
    graph = {1: {2: WW}, 2: {3: WW}, 3: {1: WW | RW}}
    found = classify_component(graph, {1, 2, 3})
    assert found is not None
    assert found[0].name == "G0"
    assert sorted(found[1]) == [1, 2, 3]


def test_classification_prefers_fewer_rw_edges() -> None:
    graph = {1: {2: RW}, 2: {1: WR, 3: RW}, 3: {1: RW}}
    found = classify_component(graph, {1, 2, 3})
    assert found is not None
    assert found[0].name == "G-single"
    assert found[1] == [1, 2]


def test_g1c_two_read_modify_writes_read_each_other() -> None:
    # A appends to key 1 after counting key 0, B the other way round, and
    # each saw the other's append.
    report = check(
        [
            rmw(1, 1, 0, 0, 5),
            rmw(2, 0, 1, 0, 5),
            read(3, 6, 7, {0: [2], 1: [1]}, counts={1: 1, 2: 1}),
        ]
    )
    assert only_anomaly(report) == "G1c"


def test_g_single_read_modify_write_misses_a_write_before_it() -> None:
    # The read-modify-write counted an empty key, yet its append lands after
    # the blind append: one rw edge, closed by ww.
    report = check(
        [
            rmw(1, 0, 0, 0, 10),
            append(2, 0, 0, 10),
            read(3, 1, 9, {0: [2]}, isolation=SERIALIZABLE),
            read(4, 11, 12, {0: [2, 1]}, counts={1: 0}),
        ]
    )
    assert only_anomaly(report) == "G-single"


def test_g2_lost_update() -> None:
    report = check(
        [
            rmw(1, 0, 0, 0, 5),
            rmw(2, 0, 0, 0, 5),
            read(3, 6, 7, {0: [1, 2]}, counts={1: 0, 2: 0}),
        ]
    )
    assert only_anomaly(report) == "G2"


def test_g2_long_fork() -> None:
    report = check(
        [
            append(1, 0, 0, 10),
            append(2, 1, 0, 10),
            read(3, 1, 9, {0: [1], 1: []}),
            read(4, 1, 9, {0: [], 1: [2]}),
            read(5, 11, 12, {0: [1], 1: [2]}),
        ]
    )
    assert only_anomaly(report) == "G2"
    assert report.cycles["G2"][0]["length"] == 4


def test_stale_strict_read_is_g_single_realtime() -> None:
    report = check(
        [
            append(1, 0, 0, 1),
            read(2, 2, 3, {0: []}),
            read(3, 4, 5, {0: [1]}),
        ]
    )
    assert only_anomaly(report) == "G-single-realtime"
    assert [s["read"] for s in report.stale_reads] == [2]
    assert report.lost_writes == []
    cycle = report.cycles["G-single-realtime"][0]
    assert sorted(step["edge"] for step in cycle["steps"]) == ["realtime", "rw"]


def test_write_lost_after_ack() -> None:
    report = check(
        [
            append(1, 0, 0, 1, endpoint="other:2"),
            read(2, 2, 3, {0: []}, endpoint="active:3"),
        ]
    )
    assert report.lost_writes == [
        {
            "key": 0,
            "latest_read": 2,
            "latest_read_endpoint": "active:3",
            "lost": [1],
            "lost_endpoints": ["other:2"],
        }
    ]
    assert only_anomaly(report) == "G-single-realtime"


def test_g0_realtime() -> None:
    # Append 2 was acknowledged before append 1 started, but a serializable
    # read saw 1 without 2.
    report = check(
        [
            append(2, 0, 0, 1),
            append(1, 0, 10, 11),
            read(3, 10.5, 12, {0: [1]}, isolation=SERIALIZABLE),
            read(4, 13, 14, {0: [1, 2]}),
        ]
    )
    assert only_anomaly(report) == "G0-realtime"


def test_g1c_realtime() -> None:
    # The read-modify-write counted append 2, which was invoked only after
    # the read-modify-write completed.
    report = check(
        [
            rmw(1, 1, 0, 0, 1),
            append(2, 0, 2, 3),
            read(3, 4, 5, {0: [2], 1: [1]}, counts={1: 1}),
        ]
    )
    assert only_anomaly(report) == "G1c-realtime"


def test_g2_realtime() -> None:
    report = check(
        [
            read(2, 0, 10, {0: [], 1: [4]}),
            append(3, 0, 1, 2),
            read(1, 3, 4, {1: []}),
            append(4, 1, 0, 5),
            read(5, 11, 12, {0: [3], 1: [4]}),
        ]
    )
    assert only_anomaly(report) == "G2-realtime"


def test_strong_session_must_read_its_own_write() -> None:
    report = check(
        [
            append(1, 0, 0, 1, isolation=STRONG_SESSION, session="s"),
            read(2, 2, 3, {0: []}, isolation=STRONG_SESSION, session="s"),
            read(3, 4, 5, {0: [1]}),
        ]
    )
    assert only_anomaly(report) == "G-single-realtime"
    steps = report.cycles["G-single-realtime"][0]["steps"]
    assert sorted(step["edge"] for step in steps) == ["process", "rw"]
    assert report.stale_reads == []


def test_stale_serializable_read_is_allowed() -> None:
    report = check(
        [
            append(1, 0, 0, 1),
            read(2, 2, 3, {0: []}, ts=5, isolation=SERIALIZABLE),
            read(3, 4, 5, {0: [1]}, ts=10),
        ]
    )
    assert_clean(report)


def test_read_modify_write_placed_in_version_order() -> None:
    report = check(
        [
            append(1, 0, 0, 1),
            append(2, 0, 2, 3),
            append(3, 0, 2, 3),
            read(4, 1.5, 1.8, {0: [1]}),
            rmw(5, 1, 0, 2, 6),
            read(6, 7, 8, {0: [1, 2, 3], 1: [5]}, counts={5: 2}),
        ]
    )
    assert_clean(report)
    assert report.stats.rmw_reads_placed == 1


def test_read_modify_write_counting_past_every_observed_set() -> None:
    report = check(
        [
            append(1, 0, 0, 1),
            rmw(2, 1, 0, 2, 3),
            append(3, 0, 0, 1, outcome=INFO),
            read(4, 4, 5, {0: [1], 1: [2]}, counts={2: 2}, isolation=SERIALIZABLE),
        ]
    )
    assert_clean(report)


def test_read_modify_write_observes_its_own_append() -> None:
    report = check(
        [
            rmw(1, 0, 0, 0, 1),
            read(2, 2, 3, {0: [1]}, counts={1: 1}),
        ]
    )
    assert len(report.rmw_self_reads) == 1


def test_timestamp_regression() -> None:
    report = check(
        [
            append(1, 0, 0, 1),
            append(2, 0, 0, 1, outcome=INFO),
            read(3, 2, 3, {0: [1, 2]}, ts=20, isolation=SERIALIZABLE),
            read(4, 2, 3, {0: []}, ts=30, isolation=SERIALIZABLE),
        ]
    )
    assert [r["lost"] for r in report.timestamp_regressions] == [[1]]
    assert [r["lost"] for r in report.indeterminate_regressions] == [[2]]


def test_same_timestamp_mismatch() -> None:
    report = check(
        [
            append(1, 0, 0, 1),
            read(2, 2, 3, {0: [1]}, ts=20, isolation=SERIALIZABLE),
            read(3, 2, 3, {0: []}, ts=20, isolation=SERIALIZABLE),
        ]
    )
    assert len(report.same_timestamp_mismatches) == 1


def test_realtime_edges_are_a_transitive_reduction() -> None:
    ops = [
        append(1, 0, 0, 1),
        append(2, 0, 2, 3),
        append(3, 0, 4, 5),
        append(4, 0, 2.5, 6),
        append(5, 0, 5, 7, outcome=INFO),
        append(6, 0, 8, 9),
    ]
    assert sorted(realtime_edges(ops)) == [
        (1, 2),
        (1, 4),
        (2, 3),
        (2, 5),
        (3, 6),
        (4, 6),
    ]


def test_equal_instants_do_not_order() -> None:
    assert realtime_edges([append(1, 0, 0, 1), append(2, 0, 1, 2)]) == []


def test_strongly_connected_components() -> None:
    graph = {1: [2], 2: [3], 3: [1], 4: [1], 5: []}
    components = strongly_connected_components([1, 2, 3, 4, 5], lambda u: graph[u])
    assert sorted(sorted(c) for c in components) == [[1, 2, 3], [4], [5]]


def test_deep_chain_does_not_recurse() -> None:
    n = 50_000
    components = strongly_connected_components(
        range(n), lambda u: [u + 1] if u + 1 < n else [0]
    )
    assert len(components) == 1


def test_realtime_timestamp_order() -> None:
    reads = [
        read(1, 0, 1, {}, ts=20),
        read(2, 2, 3, {}, ts=10, session="other"),
        read(3, 0.5, 4, {}, ts=5),
        read(4, 5, 6, {}, ts=1, isolation=SERIALIZABLE),
    ]
    result = realtime_timestamp_order(reads)
    assert [r["read"] for r in result.regressions] == [2]
    assert result.regressions[0]["earlier_read"] == 1
    assert realtime_timestamp_order(reads, only={3}).regressions == []


def test_realtime_timestamp_order_across_restart() -> None:
    later = replace(read(2, 100, 101, {}, ts=30), uptime_s=10.0)
    result = realtime_timestamp_order([read(1, 0, 1, {}, ts=20), later])
    assert result.regressions == []
    assert result.across_restart == 1


def test_session_timestamp_order() -> None:
    reads = [
        read(1, 0, 1, {}, ts=20, isolation=STRONG_SESSION, session="s"),
        read(2, 2, 3, {}, ts=10, isolation=STRONG_SESSION, session="s"),
        read(3, 2, 3, {}, ts=5, isolation=STRONG_SESSION, session="t"),
    ]
    result = session_timestamp_order(reads)
    assert [(r["earlier"], r["later"]) for r in result.regressions] == [(1, 2)]


@dataclass
class Simulation:
    """A correct system: each operation takes effect at one instant inside its
    interval, against a single copy of the data."""

    rng: random.Random
    keys: int = 6
    ops: list[Op] = field(default_factory=list)

    def run(self, processes: int, per_process: int) -> list[Op]:
        planned = []
        next_id = 1
        for p in range(processes):
            now = self.rng.uniform(0, 1)
            isolation = self.rng.choice([STRICT, STRICT, STRONG_SESSION, SERIALIZABLE])
            session = f"p{p}"
            for _ in range(per_process):
                invoke = now + self.rng.uniform(0.001, 0.5)
                point = invoke + self.rng.uniform(0.001, 1.0)
                complete = point + self.rng.uniform(0.001, 1.0)
                now = complete
                planned.append((point, next_id, invoke, complete, isolation, session))
                next_id += 1
        state: dict[int, list[int]] = {k: [] for k in range(self.keys)}
        counts: dict[int, int] = {}
        history: list[dict[int, list[int]]] = []
        for point, op_id, invoke, complete, isolation, session in sorted(planned):
            choice = self.rng.random()
            if isolation == SERIALIZABLE or choice < 0.45:
                self.ops.append(
                    self._read(
                        op_id,
                        invoke,
                        complete,
                        isolation,
                        session,
                        state,
                        history,
                        counts,
                    )
                )
                history.append({k: list(v) for k, v in state.items()})
                continue
            key = self.rng.randrange(self.keys)
            outcome = self.rng.choices([OK, INFO, FAIL], weights=[85, 10, 5])[0]
            applied = outcome is OK or (outcome is INFO and self.rng.random() < 0.5)
            if choice < 0.8:
                op = append(op_id, key, invoke, complete, outcome, isolation, session)
            else:
                read_key = self.rng.randrange(self.keys)
                op = replace(
                    append(op_id, key, invoke, complete, outcome, isolation, session),
                    kind=Kind.RMW,
                    read_key=read_key,
                )
                counts[op_id] = len(state[read_key])
            if applied:
                state[key].append(op_id)
            if outcome is INFO and self.rng.random() < 0.5:
                op = replace(op, complete=None)
            self.ops.append(op)
            history.append({k: list(v) for k, v in state.items()})
        return self.ops

    def _read(
        self,
        op_id: int,
        invoke: float,
        complete: float,
        isolation: str,
        session: str,
        state: dict[int, list[int]],
        history: list[dict[int, list[int]]],
        counts: dict[int, int],
    ) -> Op:
        snapshot, ts = state, len(history)
        if isolation == SERIALIZABLE and history:
            # Serializable reads may be served from any earlier snapshot.
            ts = self.rng.randrange(len(history))
            snapshot = history[ts]
        keys = self.rng.sample(range(self.keys), self.rng.randint(1, self.keys))
        observations = []
        for k in keys:
            values = list(snapshot[k])
            self.rng.shuffle(values)
            observations.append(
                Observation(
                    k,
                    self.rng.choice(["table", "mv"]),
                    tuple(values),
                    tuple((v, counts[v]) for v in values if v in counts),
                )
            )
        return Op(
            op_id,
            Kind.READ,
            OK,
            invoke,
            complete,
            session=session,
            isolation=isolation,
            observations=tuple(observations),
            ts=ts,
        )


def simulate(seed: int, processes: int = 8, per_process: int = 40) -> list[Op]:
    return Simulation(random.Random(seed)).run(processes, per_process)


def test_simulated_correct_histories_are_clean() -> None:
    for seed in range(40):
        ops = simulate(seed)
        report = check(ops)
        assert problems(report) == {}, seed
        assert realtime_timestamp_order(ops).regressions == []
        assert session_timestamp_order(ops).regressions == []
        assert report.stats.graph_nodes > 0


def test_simulated_history_with_a_dropped_acked_value_is_caught() -> None:
    caught = 0
    for seed in range(40):
        ops = simulate(seed)
        acked = {op.op_id: op for op in ops if op.is_write and op.acked}
        for i in range(len(ops) - 1, -1, -1):
            op = ops[i]
            if op.kind is not Kind.READ or op.isolation != STRICT:
                continue
            victims = [
                (n, v)
                for n, obs in enumerate(op.observations)
                for v in obs.values
                if v in acked
                and acked[v].isolation == STRICT
                and (acked[v].complete or 0) < op.invoke
            ]
            if not victims:
                continue
            n, victim = victims[0]
            obs = op.observations[n]
            dropped = replace(
                obs,
                values=tuple(v for v in obs.values if v != victim),
                counts=tuple(c for c in obs.counts if c[0] != victim),
            )
            observations = list(op.observations)
            observations[n] = dropped
            ops[i] = replace(op, observations=tuple(observations))
            report = check(ops)
            assert report.stale_reads, seed
            assert sum(report.anomaly_counts.values()) > 0 or report.incompatible_orders
            caught += 1
            break
    assert caught > 30


def test_simulated_history_with_a_rejected_value_is_caught() -> None:
    for seed in range(10):
        ops = simulate(seed)
        writes = [op for op in ops if op.is_write and op.outcome is OK]
        target = writes[len(writes) // 2]
        ops = [
            replace(op, outcome=FAIL) if op.op_id == target.op_id else op for op in ops
        ]
        report = check(ops)
        observed = any(
            target.op_id in obs.values
            for op in ops
            if op.kind is Kind.READ
            for obs in op.observations
        )
        assert bool(report.aborted_reads) == observed


def test_stale_reads_labelled_strict_are_g_single_realtime() -> None:
    # Serializable reads may be served from old snapshots. Labelling them
    # strict serializable turns them into stale reads in one large component,
    # which must still be classified by its single-rw cycles.
    for seed in range(5):
        rng = random.Random(seed)
        ops = [
            (
                replace(op, isolation=STRICT)
                if op.isolation == SERIALIZABLE and rng.random() < 0.5
                else op
            )
            for op in simulate(seed, processes=12, per_process=150)
        ]
        report = check(ops)
        assert report.stale_reads, seed
        assert report.anomaly_counts["G-single-realtime"] >= 1, seed
        assert report.incompatible_orders == []


def test_large_history_is_checked_quickly() -> None:
    ops = simulate(7, processes=12, per_process=150)
    started = time.monotonic()
    report = check(ops)
    elapsed = time.monotonic() - started
    assert problems(report) == {}
    assert report.stats.graph_nodes > 1000
    assert elapsed < 30, elapsed


def test_every_anomaly_class_is_reported_by_name() -> None:
    assert [a.name for a in ANOMALIES] == [
        "G0",
        "G1c",
        "G-single",
        "G2",
        "G0-realtime",
        "G1c-realtime",
        "G-single-realtime",
        "G2-realtime",
    ]
    assert WR != RW
