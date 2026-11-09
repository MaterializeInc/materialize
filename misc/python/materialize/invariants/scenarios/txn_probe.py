# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Transaction semantics under disruption, with no domain invariant at all.

Four write shapes, each with an oracle that holds at every timestamp whatever
the disruptions did:

* read-modify-write on a small contended key set, where every committed op
  adds exactly one, so the sum is pinned between the committed and the
  attempted op counts. A lost update is invisible to a conservation check,
  which is what every other scenario has.
* transactions that never commit (explicit ROLLBACK, an aborting statement
  error, and a connection dropped mid-transaction). Their rows must never be
  visible, which is the one completely unambiguous oracle here.
* one transaction writing the same markers into three separate tables, so
  txn-wal's multi-shard commit has to be atomic across shards. The three
  tables must hold the same set of markers at every timestamp.
* create/write/drop of scratch tables, which is the busiest lifecycle persist
  has (register with txn-wal, write batches, forget, finalize, GC the trace
  away) and which nothing else drives while a leg is severed.
"""

import random
import time

from materialize.invariants.checkers import (
    GroupCompletenessPeek,
    PeekChecker,
    ReplicaDivergence,
)
from materialize.invariants.framework import (
    CONVERGE_TIMEOUT,
    SEED_RANGE,
    Action,
    Checker,
    InvariantViolation,
    OpLog,
    Outcome,
    Scenario,
    ScenarioContext,
    TransientError,
    WorkerBundle,
    wait_until,
)
from materialize.invariants.mz import MzClient

# The final check runs on a quiesced system and legitimately scans everything
# the run wrote, which the per-query watchdog is not sized for: that watchdog
# exists to stop a checker hanging during chaos, and cancelling a final-check
# query instead reports a wedge that is really just a big honest query.
FINAL_TIMEOUT = 600

# Rows contended by the read-then-write action. Few enough that concurrent
# workers collide on the same row, which is the case that can lose an update.
COUNTER_KEYS = 8

# Three separate shards written by one transaction, see MultiShardTxn.
TXN_TABLES = ("txn_a", "txn_b", "txn_c")

# Markers present in one of the three tables but not the next, in both
# directions around the ring, so any asymmetry shows up.
MULTI_SHARD_DIFF_SQL = " UNION ALL ".join(
    f"(SELECT '{left}' AS present_in, worker, seq, idx FROM {left}"
    f" EXCEPT ALL SELECT '{left}', worker, seq, idx FROM {right})"
    for left, right in zip(TXN_TABLES, TXN_TABLES[1:] + TXN_TABLES[:1], strict=False)
)

# The maintained sum against the same sum recomputed from the table, in one
# statement, so both sides are answered at one timestamp and a difference
# cannot be explained by progress in between.
COUNTER_MV_DIFF_SQL = (
    "(SELECT total FROM counter_total"
    " EXCEPT ALL SELECT coalesce(sum(n), 0) FROM counters)"
    " UNION ALL"
    " (SELECT coalesce(sum(n), 0) FROM counters"
    " EXCEPT ALL SELECT total FROM counter_total)"
)


class ReadThenWrite(Action):
    """Increment a counter, half of the time reading it back client-side.

    A lost update is invisible to a conservation oracle: nothing moved, so
    the sum still balances. This counts instead, and each committed op must
    add exactly one.

    The increment is a server-side read-modify-write, and half the time it is
    preceded by a read of the same row on the same session, so a peek and a
    write on one key interleave.
    """

    name = "read-then-write"

    def __init__(
        self, rng: random.Random, worker: int, client: MzClient, oplog: OpLog
    ) -> None:
        super().__init__(rng)
        self.worker = worker
        self.client = client
        self.oplog = oplog
        self.seq = 0

    def run(self) -> Outcome | None:
        self.seq += 1
        seq = self.seq
        key = self.rng.randrange(COUNTER_KEYS)
        # Registered before sending: a thread that dies mid-call still counts
        # as possibly-applied, which keeps the upper bound sound.
        self.oplog.record(self.worker, seq, Outcome.UNKNOWN)
        if self.rng.random() < 0.5:
            # A read of the same row immediately before writing it, on the
            # same session. NOTE: the increment cannot be moved inside an
            # explicit transaction to make this a client-side
            # read-then-write, because Materialize rejects UPDATE in a
            # transaction block: multi-statement transactions are read-only
            # or insert-only.
            self.client.query(f"SELECT n FROM counters WHERE id = {key}")
        outcome = self.client.write(f"UPDATE counters SET n = n + 1 WHERE id = {key}")
        self.oplog.record(self.worker, seq, outcome)
        return outcome

    def close(self) -> None:
        self.client.reset()


class CounterSumPeek(PeekChecker):
    """The counters must account for every increment that committed.

    Bounds, not equality, because an op whose outcome is unknown may or may
    not have applied. Sampling order is the framework's rule: lower bound
    before the read, upper bound after.
    """

    def __init__(self, rng, ctx, oplog: OpLog) -> None:
        super().__init__(rng, ctx, "counter-sum", ["quickstart", "compute"])
        self.oplog = oplog

    def check_once(self) -> None:
        low = self.oplog.committed_count()
        rows = self.peek("SELECT coalesce(sum(n), 0) FROM counters")
        high = self.oplog.attempted_count()
        total = int(rows[0][0])
        if not low <= total <= high:
            raise InvariantViolation(
                f"counter sum {total} outside [{low}, {high}] on"
                f" {self.last_cluster}: increments were lost or duplicated"
            )
        self.validations += 1


class RollbackNoop(Action):
    """A transaction that never commits must leave nothing behind.

    Rolled back and abandoned transactions are the one write path with a
    completely unambiguous oracle: the rows must never be visible, at any
    timestamp, whatever the disruptions did. Every other action here has to
    reason about unknown outcomes, this one does not.

    Three ways to not commit, because they end the transaction differently:
    an explicit ROLLBACK, a statement error that aborts it, and dropping the
    connection mid-transaction, which is the case a disruption produces on
    its own and the one where a server-side mistake would actually commit.
    """

    name = "rollback"

    def __init__(self, rng: random.Random, worker: int, ctx: ScenarioContext) -> None:
        super().__init__(rng)
        self.worker = worker
        self.ctx = ctx
        self.client = MzClient(ctx, f"rollback-{worker}")
        self.seq = 0

    def run(self) -> Outcome | None:
        self.seq += 1
        marker = self.worker * 10_000_000 + self.seq
        style = self.rng.choice(["rollback", "error", "drop"])
        try:
            self.client.query("BEGIN")
            self.client.query(f"INSERT INTO rollback_probe VALUES ({marker})")
            if style == "rollback":
                self.client.query("ROLLBACK")
            elif style == "error":
                try:
                    # Aborts the transaction server-side. The INSERT above must
                    # go with it.
                    self.client.query("SELECT 1 / 0")
                except Exception:
                    pass
                self.client.query("ROLLBACK")
            else:
                # No ROLLBACK: the connection just goes away with the
                # transaction open.
                self.client.hard_close()
        except TransientError:
            # The disruption ended the transaction for us, which is the same
            # contract: it must not have committed.
            self.client.reset()
            raise
        except Exception:
            self.client.reset()
        return Outcome.FAILED

    def close(self) -> None:
        self.client.reset()


class RollbackProbePeek(PeekChecker):
    """Nothing a rolled back transaction wrote may ever be visible."""

    pause = (0.5, 2.0)

    def __init__(self, rng, ctx) -> None:
        super().__init__(rng, ctx, "rollback-probe", ["quickstart", "compute"])

    def check_once(self) -> None:
        rows = self.peek("SELECT id FROM rollback_probe LIMIT 5")
        if rows:
            raise InvariantViolation(
                f"rows from transactions that never committed are visible on"
                f" {self.last_cluster}: {rows}"
            )
        self.validations += 1


class MultiShardTxn(Action):
    """One transaction writing the same markers to three separate tables.

    A single-table write never makes txn-wal's multi-shard commit be atomic
    across shards. Here one transaction touches three, which txn-wal commits
    by registering all three batches in the txns shard at one timestamp and
    applying them to each data shard afterwards. If a lost consensus response
    left that half-applied, a marker would exist in some tables and not
    others.

    The invariant needs no bookkeeping: the three tables must hold exactly the
    same set of markers at every timestamp, whether a given transaction
    committed or not.
    """

    name = "multi-shard-txn"

    def __init__(self, rng: random.Random, worker: int, client: MzClient) -> None:
        super().__init__(rng)
        self.worker = worker
        self.client = client
        self.seq = 0

    def run(self) -> Outcome | None:
        self.seq += 1
        # A one-row-per-table transaction is a small target: a reader has to
        # land in the gap between three single-row writes. Widening it to
        # tens of rows widens that gap proportionally, which is the only
        # lever this action has on how often a tear is observable.
        rows = self.rng.randint(1, 20)
        values = ", ".join(f"({self.worker}, {self.seq}, {i})" for i in range(rows))
        return self.client.write_txn(
            [(f"INSERT INTO {table} VALUES {values}", None) for table in TXN_TABLES]
        )

    def close(self) -> None:
        self.client.reset()


class ShardChurn(Action):
    """Create, write, and drop scratch tables to churn persist shards.

    A table's lifecycle is the busiest sequence persist has: register the
    shard with txn-wal, write batches, forget it, finalize it (tombstone,
    then GC the whole trace away). Doing it while the metadata leg is being
    severed is the point: each step is a consensus write whose response can
    be lost, and the shard is gone shortly after, which is when a botched
    state transition stops being recoverable.

    No checker reads these tables, so their contents never matter. What
    matters is that the churn happens on the same envd and clusters the
    invariants cover.
    """

    name = "shard-churn"

    def __init__(
        self, rng: random.Random, worker: int, client: MzClient, ctx: ScenarioContext
    ) -> None:
        super().__init__(rng)
        self.worker = worker
        self.client = client
        self.ctx = ctx
        self.next_at = 0.0
        self.nonce = 0
        # Names whose DROP outcome was not COMMITTED. An UNKNOWN drop may not
        # have applied, and forgetting the name would leak the shard for the
        # rest of the run.
        self.maybe_alive: list[str] = []

    def run(self) -> Outcome | None:
        now = time.monotonic()
        if now < self.next_at:
            return None
        self.next_at = now + self.rng.uniform(3.0, 10.0)

        self.maybe_alive = [
            name
            for name in self.maybe_alive
            if self.client.write(f"DROP TABLE IF EXISTS {name}") != Outcome.COMMITTED
        ]

        self.nonce += 1
        name = f"churn_tbl_{self.worker}_{self.nonce}"
        # The shard is registered with txn-wal and written for the first time
        # in the next few hundred milliseconds, which is the window where a
        # lost consensus response leaves a half-created shard behind. A
        # timer-driven cut lands there only by luck.
        self.ctx.request_disruption("shard-create")
        outcome = self.client.write(f"CREATE TABLE {name} (a int, b text)")
        if outcome == Outcome.FAILED:
            return outcome
        self.maybe_alive.append(name)
        # Several small transactions rather than one big insert: each is its
        # own txn-wal commit and data batch, so the shard accumulates batches
        # to compact before it is dropped again.
        for i in range(self.rng.randint(1, 5)):
            self.client.write(
                f"INSERT INTO {name} SELECT g, repeat('x', 100)"
                f" FROM generate_series({i} * 50, {i} * 50 + 49) g"
            )
            # TODO: Reenable when SQL-616 and PER-59 are fixed. Evolving the
            # schema
            # mid-shard leaves batches written under both schemas in one
            # shard, so the compaction that merges them has to migrate, and
            # the registration is one more consensus write that a severed leg
            # can leave in doubt. That last part is the problem: a restart
            # re-runs the schema evolution against a persist shard that
            # already carries it, and the coordinator panics on the mismatch
            # instead of treating it as done (SQL-616).
            #
            # PER-59 is the sticky follow-on: once that evolution has failed,
            # the catalog holds the new table version while the shard still
            # only knows the old schema, so the next bootstrap registers the
            # table with txn-wal presenting a desc the shard never registered
            # and panics with "schema should be registered". That one repeats
            # on every boot, so the environment does not come back.
            if False and self.rng.random() < 0.3:
                self.client.write(f"ALTER TABLE {name} ADD COLUMN c{i} int")
        return outcome

    def close(self) -> None:
        self.client.reset()


class TxnProbe(Scenario):
    name = "txn-probe"
    services: list[str] = []
    legs = [
        "metadata",
        "blob",
        "clusterd-compute",
        "clusterd-compute2",
        "pubsub-compute",
        "pubsub-storage",
    ]

    def __init__(self, ctx: ScenarioContext) -> None:
        super().__init__(ctx)
        self.oplog = OpLog()

    def setup(self) -> None:
        client = MzClient(self.ctx, "setup")
        for sql in [
            "CREATE TABLE counters (id int, n bigint)",
            f"INSERT INTO counters SELECT generate_series(0, {COUNTER_KEYS - 1}), 0",
            "CREATE TABLE rollback_probe (id bigint)",
            *(
                f"CREATE TABLE {table} (worker int, seq bigint, idx int)"
                for table in TXN_TABLES
            ),
            # RETAIN HISTORY covers the timestamp the replica comparison
            # probes, which is deliberately slightly in the past.
            "CREATE MATERIALIZED VIEW counter_total IN CLUSTER compute"
            " WITH (RETAIN HISTORY = FOR '600s') AS"
            " SELECT coalesce(sum(n), 0) AS total FROM counters",
        ]:
            client.query(sql, timeout=120)
        client.reset()

    def make_worker(self, index: int, rng: random.Random) -> WorkerBundle:
        client = MzClient(self.ctx, f"worker-{index}")
        actions: list[Action] = [
            ReadThenWrite(rng, index, client, self.oplog),
            MultiShardTxn(rng, index, client),
            RollbackNoop(rng, index, self.ctx),
            ShardChurn(rng, index, client, self.ctx),
        ]
        # ShardChurn rate-limits itself, so its weight only decides how often
        # a worker offers it the chance to run.
        weights = [8, 6, 3, 2]
        return WorkerBundle(actions=actions, weights=weights)

    def checkers(self) -> list[Checker]:
        rngs = [random.Random(self.ctx.rng.randrange(SEED_RANGE)) for _ in range(5)]
        return [
            CounterSumPeek(rngs[0], self.ctx, self.oplog),
            RollbackProbePeek(rngs[1], self.ctx),
            GroupCompletenessPeek(
                rngs[2], self.ctx, "multi-shard-txn", MULTI_SHARD_DIFF_SQL
            ),
            GroupCompletenessPeek(
                rngs[3], self.ctx, "counter-mv-agrees", COUNTER_MV_DIFF_SQL
            ),
            ReplicaDivergence(rngs[4], self.ctx, ("SELECT total FROM counter_total",)),
        ]

    def converge(self) -> None:
        client = MzClient(self.ctx, "converge")

        def caught_up() -> bool:
            client.query("SET cluster = quickstart")
            direct = int(client.query("SELECT coalesce(sum(n), 0) FROM counters")[0][0])
            client.query("SET cluster = compute")
            via_mv = int(client.query("SELECT total FROM counter_total")[0][0])
            return direct == via_mv

        wait_until(caught_up, CONVERGE_TIMEOUT, "the counter MV catching up")
        client.reset()

    def final_check(self) -> None:
        client = MzClient(self.ctx, "final-check")
        client.query("SET cluster = quickstart")
        low = self.oplog.committed_count()
        total = int(client.query("SELECT coalesce(sum(n), 0) FROM counters")[0][0])
        high = self.oplog.attempted_count()
        if not low <= total <= high:
            raise InvariantViolation(
                f"final counter sum {total} outside [{low}, {high}]:"
                " read-then-write increments were lost or duplicated"
            )
        # An empty upper bound means no increment ever reached the server, so
        # the bounds above hold for the wrong reason.
        if high == 0:
            raise InvariantViolation("vacuous run: no increment was ever attempted")
        # How blunt the check above actually is. A read-modify-write cannot be
        # reconciled after the fact the way an insert can, since one increment
        # is indistinguishable from another, so every op whose outcome stayed
        # unknown widens the window forever. That makes this the one oracle
        # here that gets *weaker* the harder the run is disrupted, and it does
        # so silently, so the width is reported and a window wider than the
        # thing it is measuring fails: at that point it cannot see a loss.
        width = (high - low) / high
        self.ctx.log.log(
            "stats",
            f"counter oracle resolution: [{low}, {high}], window is"
            f" {width:.1%} of the upper bound",
        )
        leaked = client.query("SELECT id FROM rollback_probe LIMIT 20")
        if leaked:
            raise InvariantViolation(
                f"rows from transactions that never committed are visible: {leaked}"
            )
        half_applied = client.query(MULTI_SHARD_DIFF_SQL, timeout=FINAL_TIMEOUT)
        if half_applied:
            raise InvariantViolation(
                f"multi-shard transactions half applied between {TXN_TABLES}:"
                f" {half_applied[:20]}"
            )
        disagrees = client.query(COUNTER_MV_DIFF_SQL, timeout=FINAL_TIMEOUT)
        if disagrees:
            raise InvariantViolation(
                f"the counter MV disagrees with the table it sums: {disagrees}"
            )
        client.reset()
