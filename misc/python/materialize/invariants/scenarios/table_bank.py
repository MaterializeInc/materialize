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
    Watermark,
    WorkerBundle,
    wait_until,
)
from materialize.invariants.mz import MzClient

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
        # rng, not round-robin: the cluster rotation has the same period as
        # the query list, round-robin would pin each form to one cluster.
        query, expected_rows = self.rng.choice(self.QUERIES)
        rows = self.peek(query)
        if len(rows) != expected_rows or any(
            int(row[0]) != self.scenario.total for row in rows
        ):
            raise InvariantViolation(
                f"total mismatch via {query!r}: expected {expected_rows}x"
                f" {self.scenario.total}, got {rows}"
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


class TableBank(Scenario):
    name = "table-bank"
    services: list[str] = []
    legs = ["metadata", "blob", "clusterd-compute"]

    def __init__(self, ctx: ScenarioContext) -> None:
        super().__init__(ctx)
        self.accounts = ctx.complexity.accounts
        self.total = self.accounts * BALANCE_PER_ACCOUNT
        self.oplog = OpLog()
        self.ledger_rows = Watermark()

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
            "CREATE MATERIALIZED VIEW ledger_agg IN CLUSTER compute AS"
            " SELECT count(*) AS cnt FROM ledger",
        ]:
            client.query(sql, timeout=120)
        client.reset()

    def make_worker(self, index: int, rng: random.Random) -> WorkerBundle:
        client = MzClient(self.ctx, f"worker-{index}")
        return WorkerBundle(
            actions=[
                UpdateTransfer(rng, self.accounts, client),
                LedgerTransfer(rng, index, self.accounts, client, self.oplog),
                DdlChurn(rng, index, client),
            ],
            weights=[10, 10, 1],
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
