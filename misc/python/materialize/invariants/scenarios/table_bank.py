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

import math
import random
import time
from datetime import UTC, date, datetime, timedelta
from typing import Any

from materialize.invariants.checkers import (
    GroupCompletenessPeek,
    PeekChecker,
    ReplicaDivergence,
    SubscribeChecker,
    TernaryPartitionPeek,
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
    Watermark,
    WorkerBundle,
    wait_until,
)
from materialize.invariants.mz import MzClient, UnexpectedQueryError

# The final check runs on a quiesced system and legitimately scans everything
# the run wrote, which the per-query watchdog is not sized for: that watchdog
# exists to stop a checker hanging during chaos, and cancelling a final-check
# query instead reports a wedge that is really just a big honest query.
FINAL_TIMEOUT = 600

BALANCE_PER_ACCOUNT = 1000
# Each ledger transfer writes one debit and one credit row.
ROWS_PER_TRANSFER = 2
# Registry keys per worker, each owned and driven by exactly one thread.
REGISTRY_KEYS = 8

# The conserved grand total. One shared definition: the checked `total` MV,
# its in-place replacement, and both blue/green swap MVs must be identical
# for the checkers to stay exact through every churn.
TOTAL_DEF = (
    "SELECT (SELECT coalesce(sum(balance), 0) FROM accounts)"
    " + (SELECT coalesce(sum(amount), 0) FROM ledger) AS total"
)

# Values a summed invariant cannot see. `tag` and `amount_dec` are derived
# from the op id, so every row is checkable on its own, which catches a
# corruption that keeps the count and the sum intact. `flt` carries the float
# values whose comparison and statistics handling has been wrong before
# (NaN, negative zero, infinities), and `day` is nullable, so predicates over
# both pull persist's filter pushdown into the checked path.
FLOAT_TEXTS = ["0", "-0", "1e-320", "NaN", "Infinity", "-Infinity", "0.5"]
# Must match the literal in PredicateDifferentialPeek.PREDICATES.
DAY_CUT = date(2024, 1, 1)


def _derived(worker: int, seq: int, amount: int) -> tuple[str, str, str, str | None]:
    """The derived columns of one ledger row, as text: dec, tag, flt, day."""
    dec = f"{amount / 1_000_000:.6f}"
    tag = f"w{worker}-s{seq}"
    flt = FLOAT_TEXTS[abs(seq) % len(FLOAT_TEXTS)]
    # NULL every fifth row, so IS NULL predicates have something to find.
    day = (
        None
        if abs(seq) % 5 == 0
        else (date(2020, 1, 1) + timedelta(days=abs(seq) % 3000)).isoformat()
    )
    return dec, tag, flt, day


# Only table-bank's ledger has the derived columns. The other scenarios reuse
# these actions against a plain (worker, seq, account, amount, at) table, so
# every write path takes the shape as a flag rather than assuming the wide one.
LEDGER_COLS = "worker, seq, account, amount, at"
LEDGER_COLS_DERIVED = f"{LEDGER_COLS}, amount_dec, tag, flt, day"


def ledger_values(
    worker: int, seq: int, account: int, amount: int, derived: bool
) -> str:
    """One ledger row literal, with the derived columns when the table has them."""
    base = f"{worker}, {seq}, {account}, {amount}, now()"
    if not derived:
        return f"({base})"
    dec, tag, flt, day = _derived(worker, seq, amount)
    day_sql = "NULL" if day is None else f"DATE '{day}'"
    return f"({base}, {dec}, '{tag}', '{flt}'::double, {day_sql})"


def ledger_copy_row(
    worker: int, seq: int, account: int, amount: int, at: str, derived: bool
) -> str:
    """The same row in COPY's text format, NULL spelled the way COPY wants."""
    base = f"{worker}\t{seq}\t{account}\t{amount}\t{at}"
    if not derived:
        return f"{base}\n"
    dec, tag, flt, day = _derived(worker, seq, amount)
    # COPY's text format spells NULL as \N. Bound outside the f-string, whose
    # expression part may not contain a backslash before Python 3.12.
    day_text = "\\N" if day is None else day
    return f"{base}\t{dec}\t{tag}\t{flt}\t{day_text}\n"


# The row-level contract of the derived columns, as SQL over `ledger`. Checked
# per row rather than in aggregate: a swapped or rewritten row keeps both the
# count and the sum, and only this notices.
DERIVED_ROW_CONTRACT = (
    "tag <> 'w' || worker::text || '-s' || seq::text"
    " OR amount_dec <> amount * 0.000001"
)


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
        derived: bool = False,
    ) -> None:
        super().__init__(rng)
        self.worker = worker
        self.accounts = accounts
        self.client = client
        self.ctx = client.ctx
        self.oplog = oplog
        self.derived = derived
        self.seq = 0
        # Committed transfers not yet reversed, consumed by ReversalTransfer.
        self.reversible: list[int] = []

    def run(self) -> Outcome | None:
        self.seq += 1
        seq = self.seq
        src, dst = self.rng.sample(range(self.accounts), 2)
        amount = self.rng.randint(1, 100)
        debit = ledger_values(self.worker, seq, src, -amount, self.derived)
        credit = ledger_values(self.worker, seq, dst, amount, self.derived)
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
            self._read_back(seq)
        return outcome

    def _read_back(self, seq: int) -> None:
        """A committed write must be visible to the session that wrote it.

        Read-your-writes is a guarantee no timestamp-free invariant covers, and
        it is the one a stale read breaks first. The read goes through this
        action's own client, on purpose: another session may legitimately be
        behind. A read that fails or times out under disruption raises
        TransientError, which the worker loop treats as a skipped op.
        """
        if self.rng.random() >= 0.25 or not self.ctx.checking.is_set():
            return
        rows = self.client.query(
            f"SELECT count(*) FROM ledger WHERE worker = {self.worker}"
            f" AND seq = {seq}"
        )
        if int(rows[0][0]) != ROWS_PER_TRANSFER:
            raise InvariantViolation(
                f"read-your-writes: worker {self.worker} committed op {seq} and"
                f" its own session then saw {rows[0][0]} of"
                f" {ROWS_PER_TRANSFER} rows"
            )

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
        derived_cols = (
            ", -amount_dec, 'w' || worker::text || '-s' || (-seq)::text, flt, day"
            if self.forward.derived
            else ""
        )
        outcome = self.client.write(
            "INSERT INTO ledger SELECT worker, -seq, account, -amount, now()"
            f"{derived_cols}"
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
        derived = self.forward.derived
        data = ledger_copy_row(
            self.worker, seq, src, -amount, at, derived
        ) + ledger_copy_row(self.worker, seq, dst, amount, at, derived)
        cols = LEDGER_COLS_DERIVED if derived else LEDGER_COLS
        self.oplog.record(self.worker, seq, Outcome.UNKNOWN)
        outcome = self.client.copy_in(f"COPY ledger ({cols}) FROM STDIN", data)
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

    def __init__(
        self, rng: random.Random, client: MzClient, ctx: ScenarioContext
    ) -> None:
        super().__init__(rng)
        self.client = client
        self.ctx = ctx
        self.next_at = 0.0

    def run(self) -> Outcome | None:
        now = time.monotonic()
        if now < self.next_at:
            return None
        self.next_at = now + self.rng.uniform(8.0, 20.0)
        # The cutover is one catalog transaction, and the swap-total checker
        # asserts that readers never see a torn one. Cutting the coordinator
        # off while it commits is the only way to test that against a
        # transaction that may or may not have landed.
        self.ctx.request_disruption("schema-swap")
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

    def __init__(
        self, rng: random.Random, client: MzClient, ctx: ScenarioContext
    ) -> None:
        super().__init__(rng)
        self.client = client
        self.ctx = ctx
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
                " IN CLUSTER compute WITH (RETAIN HISTORY = FOR '600s')"
                f" AS {TOTAL_DEF}"
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
        # documented workflow. Also bail on stop: this poll can outlast the
        # executor's join ladder and would read as a stuck thread.
        deadline = time.monotonic() + 90.0
        while time.monotonic() < deadline and not self.ctx.stop.is_set():
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
            self.ctx.stop.wait(1.0)
        else:
            # Hydration did not finish, e.g. the compute leg is cut or the
            # run is shutting down. Leave the replacement for the next
            # cycle's drop.
            return Outcome.UNKNOWN
        # An interrupted apply is the interesting case: the cutover finalizes
        # the collection it replaces while the replacement still holds a read
        # hold on it, and whatever that leaves behind has to survive a
        # bootstrap. The next cycle's DROP cleans up an apply whose outcome
        # was unknown.
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
            # Keep names whose drop outcome is uncertain: an UNKNOWN drop may
            # not have applied, and untracking such an index would leak its
            # dataflow for the rest of the run.
            self.maybe_alive = [
                name
                for name in self.maybe_alive
                if self.client.write(f"DROP INDEX IF EXISTS {name}")
                != Outcome.COMMITTED
            ]
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


def ledger_predicate(rng: random.Random) -> str:
    """A random total predicate over the ledger, for the partition oracle.

    Total is the requirement: no division, no fallible cast, nothing that can
    raise, or the three branches of the partition fail differently instead of
    disagreeing. Everything here is a comparison, a NULL test, or a boolean
    combination of them.

    The columns are chosen for the values that make filters and statistics
    pushdown awkward. `flt` carries NaN, both infinities, negative zero and a
    denormal, `day` is nullable, so predicates over it are the ones that put
    rows in the IS NULL branch at all, and `amount` is signed.
    """
    atoms = [
        f"amount > {rng.randint(-100, 100)}",
        f"amount < {rng.randint(-100, 100)}",
        f"amount = {rng.randint(-100, 100)}",
        f"account >= {rng.randint(0, 32)}",
        f"seq % {rng.randint(2, 9)} = 0",
        "flt > 0",
        "flt < 0",
        "flt = 'NaN'::double precision",
        "flt = 'Infinity'::double precision",
        f"amount_dec > {rng.randint(-100, 100)}",
        "tag LIKE 'w%'",
        f"tag LIKE '%s{rng.randint(0, 9)}'",
        "day IS NULL",
        "day > DATE '2024-01-01'",
        f"day < DATE '20{rng.randint(20, 30)}-06-01'",
    ]
    p = rng.choice(atoms)
    # Compound predicates, since a filter can be right on each half and wrong
    # on the combination, and NULL propagation through AND/OR is its own trap.
    if rng.random() < 0.4:
        p = f"({p}) {rng.choice(['AND', 'OR'])} ({rng.choice(atoms)})"
    if rng.random() < 0.2:
        p = f"NOT ({p})"
    return p


class CrossObjectTxn(Action):
    """Read a compute-maintained MV and write a table, in one transaction.

    Everything else here reads and writes within one side of the system. This
    crosses it: the SELECT is served by the compute cluster and the INSERT
    lands in storage, inside a single transaction, which is where write-lock
    scope and timestamp selection have to agree with each other.

    The invariant is that the value copied is the conserved total, so a read
    that ever observes a torn or stale state records a wrong number durably.
    Unlike a peek, which sees a transient inconsistency only if it happens to
    look at that instant, this keeps the evidence.
    """

    name = "cross-object-txn"

    def __init__(self, rng: random.Random, client: MzClient, scenario) -> None:
        super().__init__(rng)
        self.client = client
        self.scenario = scenario

    def run(self) -> Outcome | None:
        self.client.query(f"SET cluster = {self.rng.choice(['quickstart', 'compute'])}")
        # A read and a write in one transaction, which is only expressible
        # this way: Materialize rejects both UPDATE and INSERT .. SELECT
        # inside a transaction block, so the read has to be its own statement
        # and the write has to carry the value the client saw.
        try:
            self.client.query("BEGIN")
            rows = self.client.query("SELECT total FROM total")
            self.client.query(f"INSERT INTO cross_probe VALUES ({int(rows[0][0])})")
            self.client.query("COMMIT")
        except TransientError:
            self.client.reset()
            raise
        except Exception:
            self.client.reset()
            return Outcome.UNKNOWN
        return Outcome.COMMITTED

    def close(self) -> None:
        self.client.reset()


# One transfer writes exactly two ledger rows, a debit and its credit, in one
# transaction. Any other group size means the transaction became visible in
# pieces, and a non-zero sum means the pieces do not belong together.
LEDGER_TORN_SQL = (
    "SELECT worker, seq, count(*), sum(amount) FROM ledger"
    " GROUP BY worker, seq HAVING count(*) <> 2 OR sum(amount) <> 0"
)


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

    def _context(self, isolation: str) -> str:
        """What the next occurrence needs in order to classify itself.

        A peek that returns the wrong rows says nothing on its own about where
        it went wrong. Naming the cluster and the isolation the read used, and
        reading the object again on every cluster with the timestamp each read
        picks, separates a wrong shard from one wrong replica, and a window
        that has passed from one that is still open.
        """
        parts = [f"cluster={self.last_cluster}", f"isolation={isolation}"]
        for cluster in self.clusters:
            try:
                self.client.query(f"SET cluster = {cluster}")
                # mz_now() over an input relation is the read's own timestamp,
                # unlike the input-free form (CPU-197).
                rows = self.client.query(
                    "SELECT total, mz_now()::text FROM total", timeout=20
                )
                parts.append(f"re-read on {cluster}: {rows}")
            except Exception as e:
                parts.append(f"re-read on {cluster} failed: {e}")
        return "; ".join(parts)

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
                f" {self.scenario.total}, got {rows} [{self._context(isolation)}]"
            )
        # Sample the watermark that feeds the history probe. `SELECT mz_now()`
        # has no inputs, and for such a constant query the coordinator answers
        # at Timestamp::MAX under serializable and bounded staleness, so only
        # strict serializable yields a usable time. The watermark never goes
        # back down, so a single MAX sample would point every later probe at
        # the end of time.
        # TODO: Reenable for all isolation levels when CPU-197 is fixed.
        if isolation == "strict serializable":
            self.scenario.recent_ts.advance(
                int(self.client.query("SELECT mz_now()::text")[0][0])
            )
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
        rows = self.peek(
            "SELECT count(*), coalesce(sum(amount), 0),"
            " coalesce(sum(amount_dec), 0) FROM ledger"
        )
        high = ROWS_PER_TRANSFER * oplog.attempted_count()
        count, amount_sum, dec_sum = int(rows[0][0]), int(rows[0][1]), rows[0][2]
        if amount_sum != 0:
            raise InvariantViolation(f"ledger sum {amount_sum} != 0 (count {count})")
        # The same conservation in numeric arithmetic, which has its own
        # encoding and scale handling.
        if float(dec_sum) != 0.0:
            raise InvariantViolation(f"ledger decimal sum {dec_sum} != 0")
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


class LedgerIdentityPeek(PeekChecker):
    """Row-level reconciliation of a window of one worker's ledger rows.

    The conserved sum and the counted bounds are blind to a corruption that
    keeps both intact: a lost transfer masked by a duplicated one, a rewritten
    account or tag, a pair that is present twice. This checks the identities
    instead, over a bounded window so it stays cheap, and it remembers what it
    saw: an op id that was there once must never be gone again, which no
    aggregate can express.
    """

    # Ops per worker examined per round. The newest ones are where a write
    # that is still settling would show up.
    WINDOW = 200

    def __init__(self, rng, ctx, scenario: "TableBank") -> None:
        super().__init__(rng, ctx, "ledger-identity", ["quickstart", "compute"])
        self.scenario = scenario
        # Op ids this checker has already observed, per worker. Nothing ever
        # deletes ledger rows, so these may never disappear.
        self.seen: dict[int, set[int]] = {}

    def check_once(self) -> None:
        oplog = self.scenario.oplog
        worker = self.rng.randrange(self.ctx.complexity.workers)
        # Sampling order as in LedgerDirectPeek: what must be present is
        # sampled before the read, what may at most be present after it. An op
        # issued while the read is in flight is legitimately in its result, and
        # comparing it against a pre-read sample would report it as a phantom.
        committed = oplog.seqs(worker, Outcome.COMMITTED)
        issued = oplog.issued(worker)
        if not issued:
            return
        # Bound the window by op id magnitude, so reversals (negative ids) of
        # transfers in the window come along.
        floor = max(abs(seq) for seq in issued) - self.WINDOW
        rows = self.peek(
            f"SELECT seq, count(*), coalesce(sum(amount), 0),"
            f" count(*) FILTER (WHERE {DERIVED_ROW_CONTRACT})"
            f" FROM ledger WHERE worker = {worker} AND abs(seq) > {floor}"
            f" GROUP BY seq"
        )
        # Everything the read may legitimately contain, including ops issued
        # while it was in flight.
        issued_after = oplog.issued(worker)
        present = set()
        for seq, count, amount_sum, bad_rows in rows:
            seq, count = int(seq), int(count)
            present.add(seq)
            if count != ROWS_PER_TRANSFER or int(amount_sum) != 0:
                raise InvariantViolation(
                    f"worker {worker} op {seq}: {count} rows summing to"
                    f" {amount_sum}, expected {ROWS_PER_TRANSFER} summing to 0"
                )
            if int(bad_rows) != 0:
                raise InvariantViolation(
                    f"worker {worker} op {seq}: {bad_rows} rows whose derived"
                    " columns do not match their op id"
                )
        missing = {seq for seq in committed if abs(seq) > floor} - present
        if missing:
            raise InvariantViolation(
                f"worker {worker}: committed ops absent: {sorted(missing)[:20]}"
            )
        phantom = present - issued_after
        if phantom:
            raise InvariantViolation(
                f"worker {worker}: ops never issued: {sorted(phantom)[:20]}"
            )
        seen = self.seen.setdefault(worker, set())
        vanished = {seq for seq in seen if abs(seq) > floor} - present
        if vanished:
            raise InvariantViolation(
                f"worker {worker}: ops disappeared after being observed:"
                f" {sorted(vanished)[:20]}"
            )
        seen |= present
        self.validations += 1


class LedgerRowsSubscribe(SubscribeChecker):
    """Row-level validation of the ledger's change stream.

    Subscribes without a snapshot, so the cost is independent of how large the
    table has grown, and judges the stream itself rather than an aggregate of
    it: the ledger is append-only, so a retraction is a bug outright, and both
    rows of a transfer are written in one statement, so every op id that
    appears at a completed timestamp must appear exactly twice and sum to zero.
    """

    def __init__(self, rng, ctx, scenario: "TableBank") -> None:
        super().__init__(
            rng,
            ctx,
            "ledger-rows-subscribe",
            "SELECT worker, seq, account, amount FROM ledger",
            snapshot=False,
            append_only=True,
        )
        self.scenario = scenario

    # Rows to carry before starting a fresh subscription, so the per-round
    # validation stays cheap however long the run is.
    STATE_LIMIT = 4000

    def check_once(self) -> None:
        super().check_once()
        if len(self._state) > self.STATE_LIMIT:
            self.recycle()

    def validate_state(self, state: dict[tuple, int], ts: int) -> None:
        per_op: dict[tuple[int, int], tuple[int, int]] = {}
        for row, count in state.items():
            worker, seq, _account, amount = row
            if count != 1:
                raise InvariantViolation(f"row {row} present {count} times at {ts}")
            rows, total = per_op.get((int(worker), int(seq)), (0, 0))
            per_op[(int(worker), int(seq))] = (rows + 1, total + int(amount))
        for (worker, seq), (rows, total) in per_op.items():
            if rows != ROWS_PER_TRANSFER or total != 0:
                raise InvariantViolation(
                    f"worker {worker} op {seq} at {ts}: {rows} rows summing to"
                    f" {total} in the change stream"
                )
            if not self.scenario.oplog.was_issued(worker, seq):
                raise InvariantViolation(
                    f"worker {worker} op {seq} at {ts} was never issued"
                )


class PredicateDifferentialPeek(PeekChecker):
    """Predicates over the float and date columns, judged two ways.

    An aggregate with a predicate lets persist skip parts by their statistics,
    and getting that wrong is silent: the answer is simply too small. So the
    same predicate is also evaluated on the client, over the rows the same
    transaction returns, and the two must agree. The values chosen are the ones
    whose statistics handling has been wrong before: NaN, negative zero, the
    infinities, a denormal, and a nullable date.

    Only predicates whose truth does not depend on where NaN sorts are used,
    so this checks statistics handling and not float ordering semantics.
    """

    # Ops per worker read back per round, as in LedgerIdentityPeek.
    WINDOW = 200

    PREDICATES: list[tuple[str, Any]] = [
        (
            "flt < 0",
            lambda flt, day: flt is not None and not math.isnan(flt) and flt < 0,
        ),
        (
            "flt = 0",
            lambda flt, day: flt is not None and not math.isnan(flt) and flt == 0,
        ),
        (
            "flt = 'NaN'::double precision",
            lambda flt, day: flt is not None and math.isnan(flt),
        ),
        ("day IS NULL", lambda flt, day: day is None),
        (
            "day > DATE '2024-01-01'",
            lambda flt, day: day is not None and day > DAY_CUT,
        ),
    ]

    def __init__(self, rng, ctx, scenario: "TableBank") -> None:
        super().__init__(rng, ctx, "predicate-differential", ["quickstart", "compute"])
        self.scenario = scenario

    def check_once(self) -> None:
        oplog = self.scenario.oplog
        worker = self.rng.randrange(self.ctx.complexity.workers)
        issued = oplog.issued(worker)
        if not issued:
            return
        floor = max(abs(seq) for seq in issued) - self.WINDOW
        predicate, on_client = self.rng.choice(self.PREDICATES)
        window = f"FROM ledger WHERE worker = {worker} AND abs(seq) > {floor}"
        cluster = self.clusters[self._round % len(self.clusters)]
        self._round += 1
        self.client.query(f"SET cluster = {cluster}")
        # One read-only transaction, so both statements read at the same
        # timestamp and a disagreement cannot be staleness.
        self.client.query("BEGIN")
        try:
            counted = int(
                self.client.query(f"SELECT count(*) {window} AND ({predicate})")[0][0]
            )
            rows = self.client.query(f"SELECT flt, day {window}")
        finally:
            try:
                self.client.query("COMMIT")
            except Exception:
                pass
        recomputed = sum(1 for flt, day in rows if on_client(flt, day))
        if counted != recomputed:
            raise InvariantViolation(
                f"worker {worker}: `{predicate}` counted {counted} of"
                f" {len(rows)} rows, recomputing it over the same read gives"
                f" {recomputed}"
            )
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

    def setup(self) -> None:
        client = MzClient(self.ctx, "setup")
        for sql in [
            "CREATE TABLE accounts (id int, balance bigint)",
            f"INSERT INTO accounts SELECT generate_series(0, {self.accounts - 1}),"
            f" {BALANCE_PER_ACCOUNT}",
            # RETAIN HISTORY on the base table keeps direct AS OF reads of
            # the ledger itself possible, besides the MVs.
            # amount_dec, tag, flt and day are derived from the op id, see
            # ledger_values: they make every row checkable on its own and put
            # float and nullable-date predicates on the checked path.
            "CREATE TABLE ledger (worker int, seq bigint, account int,"
            " amount bigint, at timestamptz, amount_dec numeric(38, 6),"
            " tag text, flt double precision, day date)"
            " WITH (RETAIN HISTORY = FOR '600s')",
            "CREATE TABLE registry (worker int, key int, ver bigint)",
            "CREATE TABLE cross_probe (total bigint)",
            # RETAIN HISTORY keeps recent timestamps readable so the durable
            # subscribe checker can resume where it left off.
            "CREATE MATERIALIZED VIEW total IN CLUSTER compute"
            f" WITH (RETAIN HISTORY = FOR '600s') AS {TOTAL_DEF}",
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
            f" {TOTAL_DEF}",
            "CREATE MATERIALIZED VIEW green.total_swap IN CLUSTER compute AS"
            f" {TOTAL_DEF}",
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
        forward = LedgerTransfer(
            rng, index, self.accounts, client, self.oplog, derived=True
        )
        actions: list[Action] = [
            UpdateTransfer(rng, self.accounts, client),
            forward,
            ReversalTransfer(rng, index, client, self.oplog, forward),
            CopyTransfer(rng, index, client, self.oplog, forward),
            RegistryOp(rng, index, client, self),
            DdlChurn(rng, index, client),
            CrossObjectTxn(rng, MzClient(self.ctx, f"cross-{index}"), self),
        ]
        # The transfer actions carry the conservation and ledger-identity
        # oracles, which are the strongest ones here, so they keep the bulk of
        # the op budget. The rest only need to happen often enough to produce
        # the states their checkers watch continuously.
        weights = [10, 10, 3, 3, 6, 1, 1]
        if index == 0:
            # Single-instance churns: concurrent swaps of the same schema
            # pair or replacements of the same MV would only race each other
            # in the catalog, without adding coverage.
            actions += [
                SchemaSwap(rng, client, self.ctx),
                ReplacementChurn(rng, client, self.ctx),
            ]
            weights += [2, 2]
        return WorkerBundle(actions=actions, weights=weights)

    def checkers(self) -> list[Checker]:
        rngs = [random.Random(self.ctx.rng.randrange(SEED_RANGE)) for _ in range(17)]
        return [
            BankTotalPeek(rngs[0], self.ctx, self),
            LedgerDirectPeek(rngs[1], self.ctx, self),
            LedgerIdentityPeek(rngs[2], self.ctx, self),
            LedgerRowsSubscribe(rngs[3], self.ctx, self),
            PredicateDifferentialPeek(rngs[4], self.ctx, self),
            BankTotalSubscribe(rngs[5], self.ctx, self),
            BankTotalSubscribe(
                rngs[6], self.ctx, self, name="total-subscribe-durable", durable=True
            ),
            SnapshotTxnPeek(rngs[7], self.ctx, self),
            TemporalPeek(rngs[8], self.ctx),
            RefreshMvPeek(rngs[9], self.ctx, self),
            SwapTotalPeek(rngs[10], self.ctx, self),
            RegistryPeek(rngs[11], self.ctx, self),
            CopyExportPeek(rngs[12], self.ctx, self),
            ReplicaDivergence(
                rngs[13],
                self.ctx,
                ("SELECT total FROM total", "SELECT cnt FROM ledger_agg"),
            ),
            TernaryPartitionPeek(
                rngs[14],
                self.ctx,
                "ledger-partition",
                "ledger",
                ledger_predicate,
                "amount",
                history=self.recent_ts,
            ),
            GroupCompletenessPeek(
                rngs[15],
                self.ctx,
                "cross-object-total",
                f"SELECT total FROM cross_probe WHERE total <> {self.total} LIMIT 5",
                history=self.recent_ts,
            ),
            GroupCompletenessPeek(
                rngs[16],
                self.ctx,
                "ledger-atomicity",
                LEDGER_TORN_SQL,
                history=self.recent_ts,
            ),
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
        broken = client.query(LEDGER_TORN_SQL, timeout=FINAL_TIMEOUT)
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
            issued = self.oplog.issued(worker)
            missing = committed - present
            if missing:
                raise InvariantViolation(
                    f"worker {worker}: committed transfers lost: {sorted(missing)[:20]}"
                )
            phantom = present - issued
            if phantom:
                raise InvariantViolation(
                    f"worker {worker}: transfers present that never committed:"
                    f" {sorted(phantom)[:20]}"
                )
        wrong = client.query(
            f"SELECT total FROM cross_probe WHERE total <> {self.total} LIMIT 20"
        )
        if wrong:
            raise InvariantViolation(
                f"a transaction reading the total MV recorded {wrong}, not {self.total}"
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
