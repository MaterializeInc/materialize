# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from textwrap import dedent

from materialize.feature_benchmark.action import Action, TdAction
from materialize.feature_benchmark.measurement_source import MeasurementSource, Td
from materialize.feature_benchmark.scenario import Scenario


class MvSink(Scenario):
    """Feature benchmarks for the materialized view sink's write path.

    These exercise the `write` operator and its correction buffer with the
    input shapes that hurt: catching up through many distinct timestamps
    after a restart, retraction-heavy steady state, and a large far-future
    update mass behind a temporal filter.

    Two dyncfgs select alternative implementations of this path,
    `enable_compute_sync_mv_sink` (sync Timely operators feeding Tokio tasks
    instead of async operators) and `enable_compute_correction_v2` (the
    correction buffer implementation). The nightly run compares against the
    merge base with identical settings, so it catches code regressions and a
    flag flip, but not a difference between the two settings themselves. To
    compare the settings, run the same build against itself with different
    parameters:

        bin/mzcompose --find feature-benchmark run default \\
          --root-scenario=MvSink \\
          --other-tag=mzbuild-$(bin/mzimage fingerprint materialized) \\
          --this-params=enable_compute_sync_mv_sink=true \\
          --other-params=enable_compute_sync_mv_sink=false

    Memory matters as much as wall-clock here: the sink buffers every
    desired update that persist has not absorbed yet, so a write path that
    falls behind shows up as replica memory before it shows up as latency.
    Memory is sampled after each measurement, so a backlog that was released
    before the trailing `SELECT` returned only shows if the allocator retains
    it.
    """


class MvSinkCatchUp(MvSink):
    """Measure how long the MV sink takes to catch up through many distinct timestamps.

    The view's cluster is taken offline while its input table absorbs many
    separate updates, one timestamp each. Bringing the cluster back makes the
    sink replay the desired collection from the view's old as-of through every
    one of those timestamps while persist writes trail behind, so the
    correction buffer holds the whole backlog and is drained one batch at a
    time. Work per drained step must stay proportional to that step, otherwise
    the catch-up is quadratic in the number of buffered timestamps.

    Runs on a dedicated cluster so its replica can be taken offline. That
    replica is a process inside the `materialized` container, so its memory
    is accounted to MEMORY_MZ rather than MEMORY_CLUSTERD.
    """

    SCALE = 5
    # Distinct timestamps the sink has to replay when it comes back.
    UPDATES = 500
    # Every update touches one row in STRIDE, so each timestamp carries
    # 2 * n / STRIDE updates: one retraction and one addition per row.
    STRIDE = 100

    def init(self) -> list[Action]:
        return [
            self.table_ten(),
            TdAction(dedent(f"""
                    > CREATE CLUSTER mv_sink_cluster SIZE 'scale=1,workers=1', REPLICATION FACTOR 1

                    > CREATE TABLE t (key INTEGER, v INTEGER)

                    > INSERT INTO t SELECT {self.unique_values()}, 0 FROM {self.join()}
                    """)),
        ]

    def before(self) -> Action:
        updates = "\n".join(
            f"> UPDATE t SET v = v + 1 WHERE key % {self.STRIDE} = 0"
            for _ in range(self.UPDATES)
        )
        # The view must be created fresh and hydrated before the cluster goes
        # offline, so the sink's as-of sits below all of the buffered updates.
        return TdAction(dedent("""
                > DROP MATERIALIZED VIEW IF EXISTS mv

                > UPDATE t SET v = 0

                > ALTER CLUSTER mv_sink_cluster SET (REPLICATION FACTOR 1)

                > CREATE MATERIALIZED VIEW mv IN CLUSTER mv_sink_cluster AS SELECT key, v FROM t

                > SELECT SUM(v) FROM mv
                0

                > ALTER CLUSTER mv_sink_cluster SET (REPLICATION FACTOR 0)
                """) + updates + "\n")

    def benchmark(self) -> MeasurementSource:
        return Td(dedent(f"""
                > SELECT 1
                  /* A */
                1

                > ALTER CLUSTER mv_sink_cluster SET (REPLICATION FACTOR 1)

                > SELECT SUM(v) FROM mv
                  /* B */
                {self.UPDATES * (self.n() // self.STRIDE)}
                """))


class MvSinkRetractions(MvSink):
    """Measure MV sink latency under retraction-heavy updates to a hydrated view.

    Every `UPDATE` rewrites all rows, so each timestamp carries one addition
    and one retraction per row. Each written batch cancels against the next
    round of updates, which is where a correction buffer that fails to
    consolidate what it has emitted regresses fastest.

    Runs on the default cluster, which the feature-benchmark composition puts
    on the external `clusterd` container, so MEMORY_CLUSTERD reflects the
    sink's buffering.
    """

    SCALE = 5
    # Full-table rewrites per measurement.
    UPDATES = 10

    def init(self) -> list[Action]:
        return [
            self.table_ten(),
            TdAction(dedent(f"""
                    > CREATE TABLE t (key INTEGER, v INTEGER)

                    > INSERT INTO t SELECT {self.unique_values()}, 0 FROM {self.join()}

                    > CREATE MATERIALIZED VIEW mv AS SELECT key, v FROM t

                    > SELECT SUM(v) FROM mv
                    0
                    """)),
        ]

    def before(self) -> Action:
        return TdAction(dedent("""
                > UPDATE t SET v = 0

                > SELECT SUM(v) FROM mv
                0
                """))

    def benchmark(self) -> MeasurementSource:
        updates = "\n".join("> UPDATE t SET v = v + 1" for _ in range(self.UPDATES))
        # Multi-line fragments are appended after dedenting: an indented `>`
        # line would read as a continuation of the previous statement.
        return Td(
            dedent("""
                > SELECT 1
                  /* A */
                1

                """)
            + updates
            + dedent(f"""

                > SELECT SUM(v) FROM mv
                  /* B */
                {self.UPDATES * self.n()}
                """)
        )


class MvSinkTemporalFilter(MvSink):
    """Measure MV sink latency for a temporal filter view with a large far-future update mass.

    Every row's retraction lands at its own far-future time when its window
    closes, and deleting a row cancels that retraction at yet another future
    time. No batch can ever write those updates, so the correction buffer
    holds an ever-growing mass of them. Draining the present, here small
    insert-and-delete rounds, must not touch that mass.

    Runs on the default cluster, which the feature-benchmark composition puts
    on the external `clusterd` container, so MEMORY_CLUSTERD reflects the
    sink's buffering.
    """

    SCALE = 6
    # Insert-and-delete rounds per measurement.
    ROUNDS = 10
    # Rows inserted per round. The previous round's rows are deleted alongside.
    BATCH = 1000

    def init(self) -> list[Action]:
        return [
            TdAction(dedent(f"""
                    > CREATE TABLE events (key INTEGER, event_ts TIMESTAMP)

                    > INSERT INTO events
                      SELECT key, TIMESTAMP '2100-01-01' + INTERVAL '1 second' * key
                      FROM generate_series(1, {self.n()}) AS key

                    > CREATE MATERIALIZED VIEW mv AS
                      SELECT key, event_ts FROM events
                      WHERE mz_now() <= event_ts + INTERVAL '30 days'

                    > SELECT COUNT(*) FROM mv
                    {self.n()}
                    """)),
        ]

    def before(self) -> Action:
        return TdAction(dedent(f"""
                > DELETE FROM events WHERE key > {self.n()}

                > SELECT COUNT(*) FROM mv
                {self.n()}
                """))

    def benchmark(self) -> MeasurementSource:
        rounds = []
        for r in range(self.ROUNDS):
            lo = self.n() + r * self.BATCH + 1
            hi = self.n() + (r + 1) * self.BATCH
            rounds.append(
                f"> INSERT INTO events "
                f"SELECT key, TIMESTAMP '2100-01-01' + INTERVAL '1 second' * key "
                f"FROM generate_series({lo}, {hi}) AS key"
            )
            if r > 0:
                rounds.append(
                    f"> DELETE FROM events WHERE key >= {lo - self.BATCH} AND key < {lo}"
                )
        return Td(
            dedent("""
                > SELECT 1
                  /* A */
                1

                """)
            + "\n".join(rounds)
            + dedent(f"""

                > SELECT COUNT(*) FROM mv
                  /* B */
                {self.n() + self.BATCH}
                """)
        )
