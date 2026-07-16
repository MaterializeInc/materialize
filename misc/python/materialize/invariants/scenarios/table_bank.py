# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Bank transfers on Materialize tables.

Write paths, all atomic per Materialize's documented guarantees:
single-statement read-then-write UPDATEs of an accounts table, INSERT-only
ledger transactions (a debit and a credit row that sum to zero, tagged with
a unique (worker, seq) op id for exact reconciliation), COPY FROM STDIN of
balanced pairs, and a registry table driven through idempotent
INSERT/UPDATE/DELETE per single-owner key.

Invariant: sum(accounts.balance) + sum(ledger.amount) equals the initial
total at every timestamp, no matter which subset of concurrent transfers
committed, failed, or is in an unknown state. The total is verified through
many documented, result-equivalent read paths (maintained MV, ad-hoc query,
join, window function, recursive CTE, LATERAL, multi-statement read-only
transactions, COPY TO STDOUT, COPY TO S3 exports, a REFRESH EVERY view, a
blue/green schema pair cut over via ALTER SCHEMA SWAP, and replacement
materialized views applied in place).
"""

import random
import time
from datetime import UTC, datetime
from typing import Any

from materialize.invariants.checkers import PeekChecker, SubscribeChecker
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
    Watermark,
    WorkerBundle,
    wait_until,
)
from materialize.invariants.mz import MzClient, UnexpectedQueryError

BALANCE_PER_ACCOUNT = 1000
# Each ledger transfer writes one debit and one credit row.
ROWS_PER_TRANSFER = 2
# Registry keys per worker, each owned and driven by exactly one thread.
REGISTRY_KEYS = 8


class UpdateTransfer(Action):
    """Move money between two accounts in one atomic UPDATE statement."""

    name = "update-transfer"

    def __init__(self, rng: random.Random, accounts: int, client: MzClient) -> None:
        super().__init__(rng)
        self.accounts = accounts
        self.client = client

    def run(self) -> Outcome | None:
        src, dst = self.rng.sample(range(self.accounts), 2)
        amount = self.rng.randint(1, 100)
        return self.client.write(
            f"UPDATE accounts SET balance = balance +"
            f" CASE id WHEN {src} THEN {-amount} ELSE {amount} END"
            f" WHERE id IN ({src}, {dst})"
        )

    def close(self) -> None:
        self.client.reset()


class LedgerTransfer(Action):
    """Append a balanced (debit, credit) pair to the ledger table."""

    name = "ledger-transfer"

    def __init__(
        self,
        rng: random.Random,
        worker: int,
        accounts: int,
        client: MzClient,
        oplog: OpLog,
    ) -> None:
        super().__init__(rng)
        self.worker = worker
        self.accounts = accounts
        self.client = client
        self.oplog = oplog
        self.seq = 0
        # Committed transfers not yet reversed, consumed by ReversalTransfer.
        self.reversible: list[int] = []

    def run(self) -> Outcome | None:
        self.seq += 1
        seq = self.seq
        src, dst = self.rng.sample(range(self.accounts), 2)
        amount = self.rng.randint(1, 100)
        debit = f"({self.worker}, {seq}, {src}, {-amount}, now())"
        credit = f"({self.worker}, {seq}, {dst}, {amount}, now())"
        # Register before sending: if this thread dies mid-call the op still
        # counts as possibly-applied.
        self.oplog.record(self.worker, seq, Outcome.UNKNOWN)
        if self.rng.random() < 0.5:
            outcome = self.client.write(f"INSERT INTO ledger VALUES {debit}, {credit}")
        else:
            # The same pair as an explicit INSERT-only transaction, which
            # Materialize commits atomically at one timestamp.
            outcome = self.client.write_txn(
                [
                    (f"INSERT INTO ledger VALUES {debit}", None),
                    (f"INSERT INTO ledger VALUES {credit}", None),
                ]
            )
        self.oplog.record(self.worker, seq, outcome)
        if outcome == Outcome.COMMITTED:
            self.reversible.append(seq)
        return outcome

    def close(self) -> None:
        self.client.reset()


class ReversalTransfer(Action):
    """Reverse one of this worker's committed transfers via INSERT..SELECT.

    Exercises the read-then-write insert path. The reversal writes the
    negated pair under op id (worker, -seq), so conservation and the exact
    reconciliation keep holding for any outcome.
    """

    name = "reversal"

    def __init__(
        self,
        rng: random.Random,
        worker: int,
        client: MzClient,
        oplog: OpLog,
        forward: "LedgerTransfer",
    ) -> None:
        super().__init__(rng)
        self.worker = worker
        self.client = client
        self.oplog = oplog
        self.forward = forward

    def run(self) -> Outcome | None:
        if not self.forward.reversible:
            return None
        seq = self.forward.reversible.pop(
            self.rng.randrange(len(self.forward.reversible))
        )
        self.oplog.record(self.worker, -seq, Outcome.UNKNOWN)
        outcome = self.client.write(
            "INSERT INTO ledger SELECT worker, -seq, account, -amount, now()"
            f" FROM ledger WHERE worker = {self.worker} AND seq = {seq}"
        )
        self.oplog.record(self.worker, -seq, outcome)
        return outcome

    def close(self) -> None:
        self.client.reset()


class CopyTransfer(Action):
    """Append a balanced (debit, credit) pair via COPY FROM STDIN.

    One COPY is one atomic write statement, so conservation and the exact
    (worker, seq) reconciliation hold for it like for INSERTs. Shares the
    forward transfer's seq counter (both actions run on the same worker
    thread) so op ids stay unique per worker.
    """

    name = "copy-transfer"

    def __init__(
        self,
        rng: random.Random,
        worker: int,
        client: MzClient,
        oplog: OpLog,
        forward: "LedgerTransfer",
    ) -> None:
        super().__init__(rng)
        self.worker = worker
        self.client = client
        self.oplog = oplog
        self.forward = forward

    def run(self) -> Outcome | None:
        self.forward.seq += 1
        seq = self.forward.seq
        src, dst = self.rng.sample(range(self.forward.accounts), 2)
        amount = self.rng.randint(1, 100)
        # The `at` value is the host clock: COPY has no server-side defaults
        # here, and the temporal-filter windows are wide enough to absorb
        # clock skew between host and server.
        at = datetime.now(UTC).isoformat()
        data = (
            f"{self.worker}\t{seq}\t{src}\t{-amount}\t{at}\n"
            f"{self.worker}\t{seq}\t{dst}\t{amount}\t{at}\n"
        )
        self.oplog.record(self.worker, seq, Outcome.UNKNOWN)
        outcome = self.client.copy_in(
            "COPY ledger (worker, seq, account, amount, at) FROM STDIN", data
        )
        self.oplog.record(self.worker, seq, outcome)
        if outcome == Outcome.COMMITTED:
            self.forward.reversible.append(seq)
        return outcome

    def close(self) -> None:
        self.client.reset()


class RegistryOp(Action):
    """Drive one owned registry key through INSERT/UPDATE/DELETE.

    All statements are idempotent (guarded insert, absolute-value update,
    keyed delete), so an UNKNOWN outcome never risks a duplicate row, and
    versions written by the single owner only increase. Every op is appended
    to the scenario's per-key log for the final admissible-state check.
    """

    name = "registry"

    def __init__(
        self, rng: random.Random, worker: int, client: MzClient, scenario: "TableBank"
    ) -> None:
        super().__init__(rng)
        self.worker = worker
        self.client = client
        self.scenario = scenario
        self.ver = 0

    def run(self) -> Outcome | None:
        key = self.rng.randrange(REGISTRY_KEYS)
        self.ver += 1
        ver = self.ver
        where = f"worker = {self.worker} AND key = {key}"
        roll = self.rng.random()
        if roll < 0.4:
            op = ("insert", ver)
            sql = (
                f"INSERT INTO registry SELECT {self.worker}, {key}, {ver}"
                f" WHERE NOT EXISTS (SELECT 1 FROM registry WHERE {where})"
            )
        elif roll < 0.8:
            op = ("update", ver)
            sql = f"UPDATE registry SET ver = {ver} WHERE {where}"
        else:
            op = ("delete", None)
            sql = f"DELETE FROM registry WHERE {where}"
        entry: dict[str, Any] = {"op": op, "outcome": Outcome.UNKNOWN}
        log = self.scenario.registry_log[(self.worker, key)]
        log.append(entry)
        entry["outcome"] = self.client.write(sql)
        return entry["outcome"]

    def close(self) -> None:
        self.client.reset()


class SchemaSwap(Action):
    """Blue/green cutover: atomically swap two schemas holding identical MVs.

    Consumers resolve the checked MV through the stable name
    blue.total_swap, so the documented atomicity of ALTER SCHEMA .. SWAP is
    what keeps their reads exact through every cutover.
    """

    name = "schema-swap"

    def __init__(self, rng: random.Random, client: MzClient) -> None:
        super().__init__(rng)
        self.client = client
        self.next_at = 0.0

    def run(self) -> Outcome | None:
        now = time.monotonic()
        if now < self.next_at:
            return None
        self.next_at = now + self.rng.uniform(8.0, 20.0)
        return self.client.write("ALTER SCHEMA blue SWAP WITH green")

    def close(self) -> None:
        self.client.reset()


class ReplacementChurn(Action):
    """Replace the checked total MV in place with an identical definition.

    The documented flow: CREATE REPLACEMENT MATERIALIZED VIEW hydrates in
    the background while the original keeps serving, then ALTER .. APPLY
    REPLACEMENT switches the definition while preserving the name and all
    downstream objects. Because the definition is identical, every existing
    total checker must keep seeing the exact total through the switch.
    """

    name = "replacement"

    # Must match the definition of the `total` MV exactly.
    TOTAL_DEF = (
        "SELECT (SELECT coalesce(sum(balance), 0) FROM accounts)"
        " + (SELECT coalesce(sum(amount), 0) FROM ledger) AS total"
    )

    def __init__(self, rng: random.Random, client: MzClient) -> None:
        super().__init__(rng)
        self.client = client
        self.next_at = time.monotonic() + 20.0
        self.supported = True

    def run(self) -> Outcome | None:
        now = time.monotonic()
        if not self.supported or now < self.next_at:
            return None
        self.next_at = now + self.rng.uniform(30.0, 60.0)
        # A leftover replacement from a cycle whose APPLY outcome was
        # UNKNOWN. Dropping an unapplied replacement is documented as safe.
        if (
            self.client.write("DROP MATERIALIZED VIEW IF EXISTS total_repl")
            != Outcome.COMMITTED
        ):
            return Outcome.UNKNOWN
        try:
            outcome = self.client.write(
                "CREATE REPLACEMENT MATERIALIZED VIEW total_repl FOR total"
                f" IN CLUSTER compute WITH (RETAIN HISTORY = FOR '600s')"
                f" AS {self.TOTAL_DEF}"
            )
        except UnexpectedQueryError as e:
            # Not available on this version, e.g. the pre-upgrade half of an
            # --upgrade-from run. Disabling (instead of failing) keeps the
            # rest of the scenario meaningful there.
            self.supported = False
            raise TransientError(f"replacement MVs unsupported: {e}") from e
        if outcome != Outcome.COMMITTED:
            return outcome
        # Apply only once the replacement hydrated on every replica, per the
        # documented workflow.
        deadline = time.monotonic() + 90.0
        while time.monotonic() < deadline:
            try:
                rows = self.client.query(
                    "SELECT bool_and(h.hydrated)"
                    " FROM mz_internal.mz_hydration_statuses h"
                    " JOIN mz_catalog.mz_materialized_views v"
                    " ON h.object_id = v.id WHERE v.name = 'total_repl'"
                )
            except TransientError:
                return Outcome.UNKNOWN
            if rows and rows[0][0]:
                break
            time.sleep(1.0)
        else:
            # Hydration did not finish, e.g. the compute leg is cut. Leave
            # the replacement for the next cycle's drop.
            return Outcome.UNKNOWN
        return self.client.write(
            "ALTER MATERIALIZED VIEW total APPLY REPLACEMENT total_repl"
        )

    def close(self) -> None:
        self.client.reset()


class DdlChurn(Action):
    """Catalog and dataflow churn on the compute cluster.

    Creates and drops a scratch index and MV over the ledger. The checked
    objects never reference them, so the invariant checkers stay unaffected
    by the churn itself, but dataflow (un)installation keeps happening on
    the same cluster and table the invariants cover, also while that
    cluster's leg is disrupted. Every statement is idempotent, and index
    names carry a nonce, because an UNKNOWN outcome leaves the catalog state
    uncertain.
    """

    name = "ddl-churn"

    def __init__(self, rng: random.Random, worker: int, client: MzClient) -> None:
        super().__init__(rng)
        self.worker = worker
        self.client = client
        self.next_at = 0.0
        self.present = False
        self.nonce = 0
        self.maybe_alive: list[str] = []

    def run(self) -> Outcome | None:
        now = time.monotonic()
        if now < self.next_at:
            return None
        self.next_at = now + self.rng.uniform(5.0, 15.0)
        if self.present:
            outcome = self.client.write(
                f"DROP MATERIALIZED VIEW IF EXISTS churn_mv_{self.worker}"
            )
            for name in self.maybe_alive:
                self.client.write(f"DROP INDEX IF EXISTS {name}")
            self.maybe_alive = []
            self.present = False
        else:
            outcome = self.client.write(
                f"CREATE OR REPLACE MATERIALIZED VIEW churn_mv_{self.worker}"
                " IN CLUSTER compute AS SELECT count(*) AS cnt FROM ledger"
            )
            self.nonce += 1
            name = f"churn_idx_{self.worker}_{self.nonce}"
            self.maybe_alive.append(name)
            self.client.write(
                f"CREATE INDEX {name} IN CLUSTER compute ON ledger (account)"
            )
            # Also churn an index on the checked MV itself: peeks against
            # total race plan selection against the concurrent drops, which
            # legitimately error ("was dropped") but must never be wrong.
            total_idx = f"churn_total_idx_{self.worker}_{self.nonce}"
            self.maybe_alive.append(total_idx)
            self.client.write(
                f"CREATE INDEX {total_idx} IN CLUSTER compute ON total (total)"
            )
            self.present = True
        return outcome

    def close(self) -> None:
        self.client.reset()


class BankTotalPeek(PeekChecker):
    """The grand total must be exact on every cluster, at every time.

    Alternates between the maintained MV, an ad-hoc query over the base
    tables, a single query spanning both, and result-equivalent
    formulations through a join, a window function, a recursive CTE, and a
    LATERAL subquery. The same invariant is verified through many
    documented plans, and in the combined forms as one consistent snapshot
    across objects. The ad-hoc forms also stay live on quickstart while the
    compute cluster (which maintains the MV) is disrupted.
    """

    BASE_TOTAL = (
        "SELECT (SELECT coalesce(sum(balance), 0) FROM accounts)"
        " + (SELECT coalesce(sum(amount), 0) FROM ledger)"
    )
    # Every ledger row references an existing account id, so the inner join
    # keeps all ledger rows.
    JOIN_TOTAL = (
        "SELECT (SELECT coalesce(sum(balance), 0) FROM accounts)"
        " + coalesce(sum(l.amount), 0)"
        " FROM ledger l JOIN accounts a ON l.account = a.id"
    )
    WINDOW_TOTAL = (
        "SELECT DISTINCT sum(balance) OVER ()"
        " + (SELECT coalesce(sum(amount), 0) FROM ledger) FROM accounts"
    )
    # Log-depth pairwise-halving sum: level k holds per-id/2^k partial sums,
    # level 20 collapses everything to id 0 for up to 2^20 accounts.
    RECURSIVE_TOTAL = (
        "WITH MUTUALLY RECURSIVE lvl (l int, id int, bal numeric) AS ("
        " SELECT 0, id, balance::numeric FROM accounts"
        " UNION ALL"
        " SELECT l + 1, id / 2, sum(bal) FROM lvl WHERE l < 20"
        " GROUP BY l + 1, id / 2"
        ") SELECT bal + (SELECT coalesce(sum(amount), 0) FROM ledger)"
        " FROM lvl WHERE l = 20 AND id = 0"
    )
    LATERAL_TOTAL = (
        "SELECT sub.total FROM (VALUES (0)) v (z), LATERAL ("
        " SELECT (SELECT coalesce(sum(balance), 0) FROM accounts)"
        " + (SELECT coalesce(sum(amount), 0) FROM ledger) + v.z AS total"
        ") sub"
    )
    QUERIES = [
        ("SELECT total FROM total", 1),
        (BASE_TOTAL, 1),
        (f"SELECT total FROM total UNION ALL {BASE_TOTAL}", 2),
        (JOIN_TOTAL, 1),
        (WINDOW_TOTAL, 1),
        (RECURSIVE_TOTAL, 1),
        (LATERAL_TOTAL, 1),
    ]

    def __init__(self, rng, ctx, scenario: "TableBank") -> None:
        super().__init__(rng, ctx, "total-peek", ["compute", "quickstart"])
        self.scenario = scenario
        self.isolations = [
            "strict serializable",
            "serializable",
            "bounded staleness 5s",
        ]

    def check_once(self) -> None:
        # Conservation is timestamp-free, so it must hold under every
        # isolation level: staleness changes the chosen timestamp, never the
        # value of a timestamp-free invariant. A bounded staleness read with
        # no readable timestamp in bound errors with SQLSTATE 40001, which
        # the client classifies as a transient skip.
        isolation = self.rng.choice(self.isolations)
        try:
            self.client.query(f"SET transaction_isolation = '{isolation}'")
        except UnexpectedQueryError:
            if isolation.startswith("bounded staleness"):
                # Not available on this version, e.g. the pre-upgrade half
                # of an --upgrade-from run.
                self.isolations = [
                    i for i in self.isolations if not i.startswith("bounded")
                ]
                raise TransientError(
                    "bounded staleness unsupported, dropped from rotation"
                ) from None
            raise
        recent = self.scenario.recent_ts.get()
        if recent > 0 and self.rng.random() < 0.25:
            # Time travel into retained history: compaction must preserve
            # the invariant at already-validated timestamps.
            as_of = self.rng.randint(max(recent - 120_000, 1), recent)
            query, expected_rows = f"SELECT total FROM total AS OF {as_of}", 1
        else:
            # rng, not round-robin: the cluster rotation has the same period
            # as the query list, round-robin would pin each form to one
            # cluster.
            query, expected_rows = self.rng.choice(self.QUERIES)
        try:
            rows = self.peek(query)
        except UnexpectedQueryError as e:
            # The history probe can race compaction: while checkers skip
            # through long disruptions the recent_ts watermark goes stale,
            # and the probed timestamp may fall behind the retained window.
            # An unreadable timestamp is a skipped round, wrong data at a
            # readable one stays fatal.
            if "could not find a valid timestamp" in str(e):
                raise TransientError(str(e)) from e
            raise
        if len(rows) != expected_rows or any(
            int(row[0]) != self.scenario.total for row in rows
        ):
            raise InvariantViolation(
                f"total mismatch via {query!r}: expected {expected_rows}x"
                f" {self.scenario.total}, got {rows}"
            )
        mz_now = self.client.query("SELECT mz_now()::text")
        self.scenario.recent_ts.advance(int(mz_now[0][0]))
        self.validations += 1


class LedgerDirectPeek(PeekChecker):
    """Direct table reads: conservation, bounds, and monotonic row count.

    Table reads stay live on quickstart even while the compute cluster's leg
    is disrupted, so this checker provides coverage during those windows.
    """

    def __init__(self, rng, ctx, scenario: "TableBank") -> None:
        super().__init__(rng, ctx, "ledger-peek", ["quickstart"])
        self.scenario = scenario

    def check_once(self) -> None:
        oplog = self.scenario.oplog
        # Sampling order matters: lower bounds before the read, upper bounds
        # after, and the read is compared against the watermark as it was
        # before the read was issued.
        low = ROWS_PER_TRANSFER * oplog.committed_count()
        watermark = self.scenario.ledger_rows.get()
        rows = self.peek("SELECT count(*), coalesce(sum(amount), 0) FROM ledger")
        high = ROWS_PER_TRANSFER * oplog.attempted_count()
        count, amount_sum = int(rows[0][0]), int(rows[0][1])
        if amount_sum != 0:
            raise InvariantViolation(f"ledger sum {amount_sum} != 0 (count {count})")
        if count < watermark:
            raise InvariantViolation(
                f"ledger count went backwards: {count} < watermark {watermark}"
            )
        if count < low:
            raise InvariantViolation(
                f"ledger count {count} misses committed transfers (>= {low} expected)"
            )
        if count > high:
            raise InvariantViolation(
                f"ledger count {count} exceeds attempted transfers (<= {high} expected)"
            )
        self.scenario.ledger_rows.advance(count)
        self.validations += 1


class SnapshotTxnPeek(PeekChecker):
    """Multi-statement read-only transactions see one consistent snapshot.

    The first SELECT picks the transaction's timestamp and every subsequent
    statement reads at that same timestamp, so the accounts sum and the
    ledger sum from separate statements must still add up to the conserved
    total. Alternates the transaction form with a one-shot COPY TO STDOUT
    read of the same invariant, and ends transactions with COMMIT or
    ROLLBACK (equivalent for reads).
    """

    def __init__(self, rng, ctx, scenario: "TableBank") -> None:
        super().__init__(rng, ctx, "txn-peek", ["quickstart", "compute"])
        self.scenario = scenario

    def check_once(self) -> None:
        if self.rng.random() < 0.3:
            rows = self.client.copy_out(
                "COPY (SELECT (SELECT coalesce(sum(balance), 0) FROM accounts)"
                " + (SELECT coalesce(sum(amount), 0) FROM ledger)) TO STDOUT"
            )
            if len(rows) != 1 or int(rows[0][0]) != self.scenario.total:
                raise InvariantViolation(
                    f"total via COPY TO STDOUT: expected {self.scenario.total},"
                    f" got {rows}"
                )
            self.validations += 1
            return
        oplog = self.scenario.oplog
        # Bounds sampling order as in LedgerDirectPeek: lower bounds before
        # the transaction picks its timestamp, upper bounds after it ends.
        low = ROWS_PER_TRANSFER * oplog.committed_count()
        watermark = self.scenario.ledger_rows.get()
        cluster = self.clusters[self._round % len(self.clusters)]
        self._round += 1
        self.client.query(f"SET cluster = {cluster}")
        self.client.query("SET transaction_isolation = 'strict serializable'")
        try:
            self.client.query("BEGIN")
            accounts_sum = int(
                self.client.query("SELECT coalesce(sum(balance), 0) FROM accounts")[0][
                    0
                ]
            )
            ledger_sum, count = (
                int(v)
                for v in self.client.query(
                    "SELECT coalesce(sum(amount), 0), count(*) FROM ledger"
                )[0]
            )
        finally:
            # Read-only transactions end equivalently via COMMIT or
            # ROLLBACK. Failures drop the connection, which also ends it.
            try:
                self.client.query("COMMIT" if self.rng.random() < 0.5 else "ROLLBACK")
            except TransientError:
                pass
        high = ROWS_PER_TRANSFER * oplog.attempted_count()
        if accounts_sum + ledger_sum != self.scenario.total:
            raise InvariantViolation(
                "read-only transaction saw a torn snapshot:"
                f" accounts {accounts_sum} + ledger {ledger_sum}"
                f" != {self.scenario.total}"
            )
        if count < watermark or count < low or count > high:
            raise InvariantViolation(
                f"ledger count {count} in transaction out of bounds"
                f" [{max(low, watermark)}, {high}]"
            )
        # Shared with LedgerDirectPeek: both read under strict
        # serializable, where the monotonic-read guarantee spans sessions.
        self.scenario.ledger_rows.advance(count)
        self.validations += 1


class TemporalPeek(PeekChecker):
    """Temporal filters keep exactly the rows inside their mz_now() window.

    The wide window is larger than any run, so its count must equal the
    full ledger count at the same timestamp. The short window is a subset
    at every timestamp, and the final check verifies its rows are retracted
    on schedule after the workload quiesces.
    """

    QUERY = (
        "SELECT (SELECT count(*) FROM ledger),"
        " (SELECT cnt FROM recent_wide), (SELECT cnt FROM recent_short)"
    )

    def __init__(self, rng, ctx) -> None:
        super().__init__(rng, ctx, "temporal-peek", ["compute", "quickstart"])

    def check_once(self) -> None:
        direct, wide, short = (int(v) for v in self.peek(self.QUERY)[0])
        if wide != direct:
            raise InvariantViolation(
                f"wide temporal window dropped live rows: {wide} != {direct}"
            )
        if short > direct:
            raise InvariantViolation(
                f"short temporal window exceeds the full count:" f" {short} > {direct}"
            )
        self.validations += 1


class RefreshMvPeek(PeekChecker):
    """A REFRESH EVERY view serves some past snapshot, never a torn one.

    Conservation is timestamp-free, so any data the view returns must be
    the exact total. Reads that block around a refresh are transient skips
    via the client watchdog.
    """

    pause = (1.0, 4.0)

    def __init__(self, rng, ctx, scenario: "TableBank") -> None:
        super().__init__(rng, ctx, "refresh-peek", ["compute", "quickstart"])
        self.scenario = scenario

    def check_once(self) -> None:
        isolation = self.rng.choice(["strict serializable", "serializable"])
        self.client.query(f"SET transaction_isolation = '{isolation}'")
        rows = self.peek("SELECT total FROM total_refresh")
        if not rows:
            # The documented unavailability window around a refresh.
            raise TransientError("refresh view unavailable")
        if len(rows) != 1 or int(rows[0][0]) != self.scenario.total:
            raise InvariantViolation(
                f"REFRESH EVERY view shows a torn snapshot: {rows}"
            )
        self.validations += 1


class SwapTotalPeek(PeekChecker):
    """The blue/green MV must be exact through every ALTER SCHEMA SWAP.

    The stable name blue.total_swap resolves to either schema's MV, both
    identically defined, so the documented atomicity of the swap means a
    read either errors transiently (racing the catalog change) or returns
    the exact total. Wrong data would mean a non-atomic cutover.
    """

    pause = (1.0, 3.0)

    def __init__(self, rng, ctx, scenario: "TableBank") -> None:
        super().__init__(rng, ctx, "swap-peek", ["compute", "quickstart"])
        self.scenario = scenario

    def check_once(self) -> None:
        rows = self.peek("SELECT total FROM blue.total_swap")
        if len(rows) != 1 or int(rows[0][0]) != self.scenario.total:
            raise InvariantViolation(
                f"total via swapped schema: expected {self.scenario.total},"
                f" got {rows}"
            )
        self.validations += 1


class RegistryPeek(PeekChecker):
    """Registry state machine: at most one live row per key, versions only
    move forward, and the TopK (DISTINCT ON) read matches the plain read."""

    def __init__(self, rng, ctx, scenario: "TableBank") -> None:
        super().__init__(rng, ctx, "registry-peek", ["quickstart", "compute"])
        self.scenario = scenario
        self.high_water: dict[tuple[int, int], int] = {}

    def check_once(self) -> None:
        duplicated = self.peek(
            "SELECT worker, key FROM registry GROUP BY worker, key"
            " HAVING count(*) > 1"
        )
        if duplicated:
            raise InvariantViolation(
                f"registry holds multiple rows per key: {duplicated[:20]}"
            )
        # Sampled before the read, per the watermark rule.
        watermarks = dict(self.high_water)
        rows = self.peek(
            "SELECT r.worker, r.key, r.ver, t.cnt FROM registry r,"
            " (SELECT count(*) AS cnt FROM"
            "  (SELECT DISTINCT ON (worker, key) worker, key FROM registry"
            "   ORDER BY worker, key, ver DESC)) t"
        )
        limit = self.ctx.complexity.workers * REGISTRY_KEYS
        if len(rows) > limit:
            raise InvariantViolation(
                f"registry holds {len(rows)} rows, only {limit} keys exist"
            )
        for row in rows:
            worker, key, ver, topk_cnt = (int(v) for v in row)
            if topk_cnt != len(rows):
                raise InvariantViolation(
                    f"DISTINCT ON read disagrees with plain read:"
                    f" {topk_cnt} != {len(rows)}"
                )
            if watermarks.get((worker, key), -1) > ver:
                raise InvariantViolation(
                    f"registry key ({worker}, {key}) version moved backwards:"
                    f" {ver} < {watermarks[(worker, key)]}"
                )
            current = self.high_water.get((worker, key), -1)
            if ver > current:
                self.high_water[(worker, key)] = ver
        self.validations += 1


class CopyExportPeek(Checker):
    """COPY TO S3 exports are consistent snapshots of the ledger.

    Each export must sum to zero and contain every transfer completely
    (exactly two rows per op id, summing to zero), with the row count
    bounded by the op ledger. Alternates the documented csv and parquet
    formats and reads the files back through the S3 API.
    """

    name = "copy-export"
    pause = (20.0, 45.0)

    def __init__(self, rng, ctx, scenario: "TableBank") -> None:
        super().__init__(rng)
        self.ctx = ctx
        self.scenario = scenario
        self.client = MzClient(ctx, "copy-export")
        self.exports = 0
        self._s3 = None

    def _s3_client(self):
        import boto3

        if self._s3 is None:
            self._s3 = boto3.client(
                "s3",
                endpoint_url=f"http://127.0.0.1:{self.ctx.endpoints.minio_port}",
                aws_access_key_id="minioadmin",
                aws_secret_access_key="minioadmin",
                region_name="us-east-1",
            )
        return self._s3

    def check_once(self) -> None:
        self.exports += 1
        fmt = "csv" if self.exports % 2 else "parquet"
        prefix = f"{self.ctx.seed}/{self.exports}-{fmt}"
        oplog = self.scenario.oplog
        low = ROWS_PER_TRANSFER * oplog.committed_count()
        self.client.query(
            f"COPY (SELECT worker, seq, amount FROM ledger)"
            f" TO 's3://copytos3/{prefix}'"
            f" WITH (AWS CONNECTION = aws_conn, FORMAT = '{fmt}')",
            timeout=max(120.0, self.ctx.complexity.query_timeout),
        )
        high = ROWS_PER_TRANSFER * oplog.attempted_count()
        rows = self._read_back(prefix, fmt)
        amount_sum = sum(amount for _, _, amount in rows)
        if amount_sum != 0:
            raise InvariantViolation(f"S3 export ({fmt}) sums to {amount_sum} != 0")
        if not low <= len(rows) <= high:
            raise InvariantViolation(
                f"S3 export ({fmt}) has {len(rows)} rows, outside" f" [{low}, {high}]"
            )
        per_op: dict[tuple[int, int], list[int]] = {}
        for worker, seq, amount in rows:
            per_op.setdefault((worker, seq), []).append(amount)
        broken = {
            op: amounts
            for op, amounts in per_op.items()
            if len(amounts) != ROWS_PER_TRANSFER or sum(amounts) != 0
        }
        if broken:
            raise InvariantViolation(
                f"S3 export ({fmt}) tore transfers apart:"
                f" {dict(list(broken.items())[:10])}"
            )
        self.validations += 1

    def _read_back(self, prefix: str, fmt: str) -> list[tuple[int, int, int]]:
        try:
            s3 = self._s3_client()
            listing = s3.list_objects_v2(Bucket="copytos3", Prefix=prefix)
            rows: list[tuple[int, int, int]] = []
            for entry in listing.get("Contents", []):
                if entry["Key"].endswith("INCOMPLETE"):
                    # The sentinel is deleted on completion, seeing it here
                    # would mean an incomplete export. The COPY statement
                    # already returned success, so treat it as not-yet-listed
                    # rather than failing.
                    raise TransientError("INCOMPLETE sentinel still present")
                body = s3.get_object(Bucket="copytos3", Key=entry["Key"])["Body"].read()
                if fmt == "csv":
                    for line in body.decode().splitlines():
                        worker, seq, amount = line.split(",")
                        rows.append((int(worker), int(seq), int(amount)))
                else:
                    import pyarrow as pa
                    import pyarrow.parquet as pq

                    table = pq.read_table(pa.BufferReader(body))
                    for worker, seq, amount in zip(
                        table.column("worker").to_pylist(),
                        table.column("seq").to_pylist(),
                        table.column("amount").to_pylist(),
                    ):
                        if worker is None or seq is None or amount is None:
                            raise InvariantViolation(
                                "NULL in exported ledger row:"
                                f" ({worker}, {seq}, {amount})"
                            )
                        rows.append((int(worker), int(seq), int(amount)))
            return rows
        except InvariantViolation:
            raise
        except Exception as e:
            # The S3 read-back path is harness-side infrastructure, not the
            # system under test.
            raise TransientError(f"export read-back failed: {e}") from e

    def close(self) -> None:
        self.client.reset()


class BankTotalSubscribe(SubscribeChecker):
    """The total MV must show the exact total at every progress boundary.

    The durable variant carries its state across reconnects by resuming
    from the last validated timestamp, verifying the documented durable
    subscription pattern under disruptions.
    """

    def __init__(
        self,
        rng,
        ctx,
        scenario: "TableBank",
        name: str = "total-subscribe",
        durable: bool = False,
    ) -> None:
        super().__init__(rng, ctx, name, "SELECT total FROM total", durable=durable)
        self.scenario = scenario

    def validate_state(self, state: dict[tuple, int], ts: int) -> None:
        expected = {(self.scenario.total,): 1}
        got = {(int(k[0]),): v for k, v in state.items()}
        if got != expected:
            raise InvariantViolation(
                f"total via SUBSCRIBE at {ts}: expected {expected}, got {got}"
            )


class LedgerCountAudit(SubscribeChecker):
    """Replays the ledger count's dense history: it must never decrease."""

    def __init__(self, rng, ctx) -> None:
        super().__init__(rng, ctx, "history-audit", "SELECT cnt FROM ledger_agg")
        self.last_cnt = -1

    def validate_state(self, state: dict[tuple, int], ts: int) -> None:
        if len(state) != 1 or list(state.values()) != [1]:
            raise InvariantViolation(
                f"ledger count history at {ts}: expected one row, got {state}"
            )
        cnt = int(list(state)[0][0])
        if cnt < self.last_cnt:
            raise InvariantViolation(
                f"ledger count went backwards in history at {ts}:"
                f" {cnt} < {self.last_cnt}"
            )
        self.last_cnt = cnt


class TableBank(Scenario):
    name = "table-bank"
    services: list[str] = []
    legs = ["metadata", "blob", "clusterd-compute", "clusterd-compute2"]

    def __init__(self, ctx: ScenarioContext) -> None:
        super().__init__(ctx)
        self.accounts = ctx.complexity.accounts
        self.total = self.accounts * BALANCE_PER_ACCOUNT
        self.oplog = OpLog()
        self.ledger_rows = Watermark()
        # Recent mz timestamps observed by peeks, the AS OF history probes
        # stay within RETAIN HISTORY of this.
        self.recent_ts = Watermark()
        # (worker, key) -> ordered op log, appended by the owning worker
        # thread only and read after all workers stopped.
        self.registry_log: dict[tuple[int, int], list[dict[str, Any]]] = {
            (worker, key): []
            for worker in range(ctx.complexity.workers)
            for key in range(REGISTRY_KEYS)
        }

    # The same definition in both blue/green schemas: the checked object of
    # the ALTER SCHEMA SWAP cutover.
    SWAP_TOTAL_DEF = (
        "SELECT (SELECT coalesce(sum(balance), 0) FROM accounts)"
        " + (SELECT coalesce(sum(amount), 0) FROM ledger) AS total"
    )

    def setup(self) -> None:
        client = MzClient(self.ctx, "setup")
        for sql in [
            "CREATE TABLE accounts (id int, balance bigint)",
            f"INSERT INTO accounts SELECT generate_series(0, {self.accounts - 1}),"
            f" {BALANCE_PER_ACCOUNT}",
            # RETAIN HISTORY on the base table keeps direct AS OF reads of
            # the ledger itself possible, besides the MVs.
            "CREATE TABLE ledger (worker int, seq bigint, account int,"
            " amount bigint, at timestamptz) WITH (RETAIN HISTORY = FOR '600s')",
            "CREATE TABLE registry (worker int, key int, ver bigint)",
            # RETAIN HISTORY keeps recent timestamps readable so the durable
            # subscribe checker can resume where it left off.
            "CREATE MATERIALIZED VIEW total IN CLUSTER compute"
            " WITH (RETAIN HISTORY = FOR '600s') AS"
            " SELECT (SELECT coalesce(sum(balance), 0) FROM accounts)"
            " + (SELECT coalesce(sum(amount), 0) FROM ledger) AS total",
            "CREATE INDEX total_idx IN CLUSTER compute ON total (total)",
            "CREATE MATERIALIZED VIEW ledger_agg IN CLUSTER compute"
            " WITH (RETAIN HISTORY = FOR '600s') AS"
            " SELECT count(*) AS cnt FROM ledger",
            # Temporal filters: the wide window outlasts any run, the short
            # window drains after quiescing.
            "CREATE MATERIALIZED VIEW recent_wide IN CLUSTER compute AS"
            " SELECT count(*) AS cnt FROM ledger"
            " WHERE mz_now() <= at + INTERVAL '4 hours'",
            "CREATE MATERIALIZED VIEW recent_short IN CLUSTER compute AS"
            " SELECT count(*) AS cnt FROM ledger"
            " WHERE mz_now() <= at + INTERVAL '60 seconds'",
            # REFRESH EVERY serves the last refresh's snapshot in between.
            "CREATE MATERIALIZED VIEW total_refresh IN CLUSTER compute"
            " WITH (REFRESH EVERY '10 seconds') AS SELECT total FROM total",
            # Blue/green pair for the ALTER SCHEMA SWAP cutover.
            "CREATE SCHEMA blue",
            "CREATE SCHEMA green",
            "CREATE MATERIALIZED VIEW blue.total_swap IN CLUSTER compute AS"
            f" {self.SWAP_TOTAL_DEF}",
            "CREATE MATERIALIZED VIEW green.total_swap IN CLUSTER compute AS"
            f" {self.SWAP_TOTAL_DEF}",
            # COPY TO S3 exports go to the MinIO container directly (the
            # toxiproxied blob leg is persist's, not this connection's).
            "CREATE SECRET miniopass AS 'minioadmin'",
            "CREATE CONNECTION aws_conn TO AWS (ENDPOINT 'http://minio:9000/',"
            " REGION 'us-east-1', ACCESS KEY ID 'minioadmin',"
            " SECRET ACCESS KEY SECRET miniopass)",
        ]:
            client.query(sql, timeout=120)
        # Anchor for the end-of-run history audit.
        self.setup_ts = int(client.query("SELECT mz_now()::text")[0][0])
        client.reset()

    def make_worker(self, index: int, rng: random.Random) -> WorkerBundle:
        client = MzClient(self.ctx, f"worker-{index}")
        forward = LedgerTransfer(rng, index, self.accounts, client, self.oplog)
        actions: list[Action] = [
            UpdateTransfer(rng, self.accounts, client),
            forward,
            ReversalTransfer(rng, index, client, self.oplog, forward),
            CopyTransfer(rng, index, client, self.oplog, forward),
            RegistryOp(rng, index, client, self),
            DdlChurn(rng, index, client),
        ]
        weights = [10, 10, 3, 3, 6, 1]
        if index == 0:
            # Single-instance churns: concurrent swaps of the same schema
            # pair or replacements of the same MV would only race each other
            # in the catalog, without adding coverage.
            actions += [SchemaSwap(rng, client), ReplacementChurn(rng, client)]
            weights += [2, 2]
        return WorkerBundle(actions=actions, weights=weights)

    def checkers(self) -> list[Checker]:
        rngs = [random.Random(self.ctx.rng.randrange(SEED_RANGE)) for _ in range(10)]
        return [
            BankTotalPeek(rngs[0], self.ctx, self),
            LedgerDirectPeek(rngs[1], self.ctx, self),
            BankTotalSubscribe(rngs[2], self.ctx, self),
            BankTotalSubscribe(
                rngs[3], self.ctx, self, name="total-subscribe-durable", durable=True
            ),
            SnapshotTxnPeek(rngs[4], self.ctx, self),
            TemporalPeek(rngs[5], self.ctx),
            RefreshMvPeek(rngs[6], self.ctx, self),
            SwapTotalPeek(rngs[7], self.ctx, self),
            RegistryPeek(rngs[8], self.ctx, self),
            CopyExportPeek(rngs[9], self.ctx, self),
        ]

    def converge(self) -> None:
        client = MzClient(self.ctx, "converge")

        def caught_up() -> bool:
            client.query("SET cluster = quickstart")
            direct = int(client.query("SELECT count(*) FROM ledger")[0][0])
            client.query("SET cluster = compute")
            via_mv = int(client.query("SELECT cnt FROM ledger_agg")[0][0])
            return direct == via_mv

        wait_until(caught_up, CONVERGE_TIMEOUT, "compute MVs catching up to tables")
        client.reset()

    def final_check(self) -> None:
        client = MzClient(self.ctx, "final-check")
        client.query("SET cluster = quickstart")
        total = int(client.query("SELECT total FROM total")[0][0])
        if total != self.total:
            raise InvariantViolation(f"final total {total} != {self.total}")
        broken = client.query(
            "SELECT worker, seq, count(*), sum(amount) FROM ledger"
            " GROUP BY worker, seq HAVING count(*) <> 2 OR sum(amount) <> 0"
        )
        if broken:
            raise InvariantViolation(f"non-atomic ledger transfers: {broken[:20]}")
        for worker in range(self.ctx.complexity.workers):
            present = {
                int(row[0])
                for row in client.query(
                    f"SELECT seq FROM ledger WHERE worker = {worker} GROUP BY seq"
                )
            }
            committed = self.oplog.seqs(worker, Outcome.COMMITTED)
            unknown = self.oplog.seqs(worker, Outcome.UNKNOWN)
            missing = committed - present
            if missing:
                raise InvariantViolation(
                    f"worker {worker}: committed transfers lost: {sorted(missing)[:20]}"
                )
            phantom = present - committed - unknown
            if phantom:
                raise InvariantViolation(
                    f"worker {worker}: transfers present that never committed:"
                    f" {sorted(phantom)[:20]}"
                )
        swap_total = int(client.query("SELECT total FROM blue.total_swap")[0][0])
        if swap_total != self.total:
            raise InvariantViolation(
                f"final swapped-schema total {swap_total} != {self.total}"
            )
        self._check_registry(client)
        # Temporal filter drain: with the workload quiesced, the short
        # window's rows must all be retracted once the window has passed.
        # Bounded by the 60s window plus host/server clock skew slack.
        wait_until(
            lambda: int(client.query("SELECT cnt FROM recent_short")[0][0]) == 0,
            180,
            "temporal filter retracting expired rows",
        )
        client.reset()
        self._history_audit(client_end_ts=None)

    def _check_registry(self, client: MzClient) -> None:
        """The final row per key must be admissible given its op log.

        Replays each key's ops over the set of possible states: COMMITTED
        ops apply to every possible state, UNKNOWN ops fork it. All
        statements are idempotent, so applying an op is a function of the
        current state only.
        """
        actual: dict[tuple[int, int], int] = {}
        for row in client.query("SELECT worker, key, ver FROM registry"):
            worker, key, ver = (int(v) for v in row)
            if (worker, key) in actual:
                raise InvariantViolation(
                    f"registry key ({worker}, {key}) has multiple rows"
                )
            actual[(worker, key)] = ver

        def apply(op: tuple, state: int | None) -> int | None:
            kind, ver = op
            if kind == "insert":
                return state if state is not None else ver
            if kind == "update":
                return ver if state is not None else None
            return None

        for (worker, key), log in self.registry_log.items():
            possible: set[int | None] = {None}
            for entry in log:
                if entry["outcome"] == Outcome.FAILED:
                    continue
                applied = {apply(entry["op"], s) for s in possible}
                if entry["outcome"] == Outcome.COMMITTED:
                    possible = applied
                else:
                    possible |= applied
            got = actual.get((worker, key))
            if got not in possible:
                raise InvariantViolation(
                    f"registry key ({worker}, {key}): final state {got} not"
                    f" admissible ({sorted(str(s) for s in possible)[:20]})"
                )

    def _history_audit(self, client_end_ts: int | None) -> None:
        """Replay the entire retained history, validating every boundary.

        Live checkers legitimately skip rounds during disruptions, this
        audit retroactively closes those gaps: a subscribe from the earliest
        retained timestamp must show the conserved total at every boundary
        of the whole run.
        """
        client = MzClient(self.ctx, "history-audit-now")
        end_ts = int(client.query("SELECT mz_now()::text")[0][0])
        client.reset()
        rng = random.Random(self.ctx.rng.randrange(SEED_RANGE))
        # The ledger count has a dense history (unlike the constant total),
        # so its replay meaningfully covers every timestamp of the run.
        audit = LedgerCountAudit(rng, self.ctx)
        # An exact AS OF: AS OF AT LEAST would let the server pick the
        # current timestamp and skip the history. Clamped into the RETAIN
        # HISTORY window for runs longer than the retention.
        as_of = max(self.setup_ts, end_ts - 480_000)
        audit.as_of_clause = f"AS OF {as_of}"
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            try:
                audit.check_once()
            except TransientError:
                continue
            if (
                audit.last_validated_ts is not None
                and audit.last_validated_ts >= end_ts
            ):
                audit.close()
                self.ctx.log.log(
                    "phase",
                    f"history audit: {audit.validations} boundaries replayed",
                )
                # Far fewer boundaries than the audited window must contain
                # means the audit did not actually start in the past.
                if audit.validations < 50:
                    raise InvariantViolation(
                        f"vacuous history audit: only {audit.validations}"
                        " boundaries replayed"
                    )
                self._up_to_replay(as_of, end_ts)
                return
        audit.close()
        raise InvariantViolation(
            "liveness: the history audit did not reach the current timestamp"
        )

    def _up_to_replay(self, as_of: int, end_ts: int) -> None:
        """A bounded SUBSCRIBE .. UP TO terminates, stays inside the bound,
        and folds to the state at the bound.

        Executed as a direct statement, not a cursor: UP TO makes the
        subscription finite, so the statement completing at all is the
        termination check. NOTE: deliberately run without PROGRESS, since
        with UP TO no progress message is ever emitted, the fold is
        validated against a direct read instead.
        TODO: Reenable when SQL-528 is fixed: run the history audit's
        per-boundary validation WITH (PROGRESS) UP TO instead of this
        end-state fold.
        The workload is quiesced and end_ts postdates it, so the folded
        count must equal the current one.
        """
        client = MzClient(self.ctx, "up-to-replay")
        try:
            rows = client.query(
                f"SUBSCRIBE (SELECT cnt FROM ledger_agg)"
                f" AS OF {as_of} UP TO {end_ts}",
                timeout=120,
            )
        except TransientError as e:
            raise InvariantViolation(
                f"liveness: SUBSCRIBE UP TO {end_ts} did not terminate: {e}"
            ) from e
        if not rows:
            raise InvariantViolation("SUBSCRIBE UP TO emitted no snapshot")
        state: dict[int, int] = {}
        for row in rows:
            ts, diff, cnt = int(row[0]), int(row[1]), int(row[2])
            if ts >= end_ts:
                raise InvariantViolation(
                    f"update at {ts} at or beyond the exclusive UP TO bound"
                    f" {end_ts}"
                )
            state[cnt] = state.get(cnt, 0) + diff
            if state[cnt] == 0:
                del state[cnt]
        current = int(client.query("SELECT cnt FROM ledger_agg")[0][0])
        client.reset()
        if state != {current: 1}:
            raise InvariantViolation(
                f"bounded replay folded to {state}, current count is {current}"
            )
        self.ctx.log.log("phase", f"bounded UP TO replay: {len(rows)} updates folded")

    def diagnostics(self) -> None:
        client = MzClient(self.ctx, "post-heal")
        for cluster in ("quickstart", "compute"):
            try:
                client.query(f"SET cluster = {cluster}")
                rows = client.query("SELECT total FROM total", timeout=60)
                verdict = (
                    "still wrong" if int(rows[0][0]) != self.total else "converged"
                )
                self.ctx.log.log(
                    "diag", f"post-heal total on {cluster}: {rows} ({verdict})"
                )
            except Exception as e:
                self.ctx.log.log("diag", f"post-heal total on {cluster}: {e}")
        client.reset()
