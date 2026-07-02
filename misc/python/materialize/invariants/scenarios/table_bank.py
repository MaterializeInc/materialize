# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Bank transfers on Materialize tables.

Two write paths, both atomic per Materialize's documented guarantees:
single-statement read-then-write UPDATEs of an accounts table, and
INSERT-only ledger transactions (a debit and a credit row that sum to zero,
tagged with a unique (worker, seq) op id for exact reconciliation).

Invariant: sum(accounts.balance) + sum(ledger.amount) equals the initial
total at every timestamp, no matter which subset of concurrent transfers
committed, failed, or is in an unknown state.
"""

import random
import time

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
        debit = f"({self.worker}, {seq}, {src}, {-amount})"
        credit = f"({self.worker}, {seq}, {dst}, {amount})"
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
            "INSERT INTO ledger SELECT worker, -seq, account, -amount"
            f" FROM ledger WHERE worker = {self.worker} AND seq = {seq}"
        )
        self.oplog.record(self.worker, -seq, outcome)
        return outcome

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
    tables, and a single query spanning both, so the same invariant is
    verified through different plans and, in the combined form, as one
    consistent snapshot across objects. The ad-hoc form also stays live on
    quickstart while the compute cluster (which maintains the MV) is
    disrupted.
    """

    BASE_TOTAL = (
        "SELECT (SELECT coalesce(sum(balance), 0) FROM accounts)"
        " + (SELECT coalesce(sum(amount), 0) FROM ledger)"
    )
    QUERIES = [
        ("SELECT total FROM total", 1),
        (BASE_TOTAL, 1),
        (f"SELECT total FROM total UNION ALL {BASE_TOTAL}", 2),
    ]

    def __init__(self, rng, ctx, scenario: "TableBank") -> None:
        super().__init__(rng, ctx, "total-peek", ["compute", "quickstart"])
        self.scenario = scenario

    def check_once(self) -> None:
        # Conservation is timestamp-free, so it must hold under either
        # isolation level.
        isolation = self.rng.choice(["strict serializable", "serializable"])
        self.client.query(f"SET transaction_isolation = '{isolation}'")
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

    def setup(self) -> None:
        client = MzClient(self.ctx, "setup")
        for sql in [
            "CREATE TABLE accounts (id int, balance bigint)",
            f"INSERT INTO accounts SELECT generate_series(0, {self.accounts - 1}),"
            f" {BALANCE_PER_ACCOUNT}",
            "CREATE TABLE ledger (worker int, seq bigint, account int, amount bigint)",
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
        ]:
            client.query(sql, timeout=120)
        # Anchor for the end-of-run history audit.
        self.setup_ts = int(client.query("SELECT mz_now()::text")[0][0])
        client.reset()

    def make_worker(self, index: int, rng: random.Random) -> WorkerBundle:
        client = MzClient(self.ctx, f"worker-{index}")
        forward = LedgerTransfer(rng, index, self.accounts, client, self.oplog)
        return WorkerBundle(
            actions=[
                UpdateTransfer(rng, self.accounts, client),
                forward,
                ReversalTransfer(rng, index, client, self.oplog, forward),
                DdlChurn(rng, index, client),
            ],
            weights=[10, 10, 3, 1],
        )

    def checkers(self) -> list[Checker]:
        rngs = [random.Random(self.ctx.rng.randrange(SEED_RANGE)) for _ in range(4)]
        return [
            BankTotalPeek(rngs[0], self.ctx, self),
            LedgerDirectPeek(rngs[1], self.ctx, self),
            BankTotalSubscribe(rngs[2], self.ctx, self),
            BankTotalSubscribe(
                rngs[3], self.ctx, self, name="total-subscribe-durable", durable=True
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
        client.reset()
        self._history_audit(client_end_ts=None)

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
                return
        audit.close()
        raise InvariantViolation(
            "liveness: the history audit did not reach the current timestamp"
        )

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
