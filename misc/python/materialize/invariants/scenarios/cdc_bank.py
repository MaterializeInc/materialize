# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Bank transfers in an upstream database, ingested via a CDC source.

Transfers are real upstream transactions (two balance UPDATEs plus a row in
a transfers log). Materialize documents transactional consistency for
PostgreSQL and MySQL sources: an upstream transaction is applied at a single
Materialize timestamp, so sum(balance) must equal the initial total at every
timestamp, including while the source connection, the clusterd connections,
or the metadata store are disrupted. The transfers log carries (worker, seq)
op ids for exact final reconciliation against the upstream oracle.
"""

import random
import time
from abc import abstractmethod
from typing import Any

from materialize.invariants.checkers import (
    GroupCompletenessPeek,
    PeekChecker,
    ProgressPeek,
    SubscribeChecker,
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

BALANCE_PER_ACCOUNT = 1000

# Worker id used for real-time recency marker rows in the transfers log.
# Negative so the per-worker reconciliation and count bounds never see them.
RTR_MARKER_WORKER = -2

# Purification of CREATE TABLE .. FROM SOURCE connects to the upstream database
# over the (possibly disrupted) source leg to validate the reference. These
# plan-time connect failures are expected chaos, not a rejected write.
UPSTREAM_CONNECT_ERROR_SNIPPETS = (
    "failed to connect to PostgreSQL database",
    "failed to connect to MySQL database",
    "failed to connect to SQL Server database",
)


class UpstreamTransfer(Action):
    """One upstream transaction: debit, credit, and a transfers-log row.

    Accounts are locked in id order so concurrent transfers cannot deadlock.
    Outcome contract: any failure before COMMIT is FAILED (the upstream
    database aborts the transaction), a failed COMMIT is UNKNOWN.
    """

    name = "cdc-transfer"

    def __init__(self, rng: random.Random, worker: int, scenario: "CdcBank") -> None:
        super().__init__(rng)
        self.worker = worker
        self.scenario = scenario
        self.conn: Any = None
        self.seq = 0

    def _ensure_conn(self) -> Any:
        if self.conn is None:
            try:
                self.conn = self.scenario.connect_upstream()
            except Exception as e:
                raise TransientError(f"upstream connect failed: {e}") from e
        return self.conn

    def _drop_conn(self) -> None:
        conn, self.conn = self.conn, None
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

    def run(self) -> Outcome | None:
        conn = self._ensure_conn()
        self.seq += 1
        seq = self.seq
        src, dst = self.rng.sample(range(self.scenario.accounts), 2)
        amount = self.rng.randint(1, 100)
        self.scenario.oplog.record(self.worker, seq, Outcome.UNKNOWN)
        try:
            cur = conn.cursor()
            for account in sorted((src, dst)):
                delta = -amount if account == src else amount
                cur.execute(
                    f"UPDATE accounts SET balance = balance + {delta}"
                    f" WHERE id = {account}"
                )
            cur.execute(
                "INSERT INTO transfers (worker, seq, src, dst, amount)"
                f" VALUES ({self.worker}, {seq}, {src}, {dst}, {amount})"
            )
        except Exception:
            try:
                conn.rollback()
            except Exception:
                self._drop_conn()
            self.scenario.oplog.record(self.worker, seq, Outcome.FAILED)
            return Outcome.FAILED
        try:
            conn.commit()
        except Exception:
            self._drop_conn()
            return Outcome.UNKNOWN
        self.scenario.oplog.record(self.worker, seq, Outcome.COMMITTED)
        return Outcome.COMMITTED

    def close(self) -> None:
        self._drop_conn()


class SourceTableChurn(Action):
    """Source versioning: add and drop tables on the running source.

    CREATE TABLE .. FROM SOURCE snapshots just the new table while the
    existing tables keep serving (their ingestion may pause during the
    snapshot, which surfaces as skipped checker rounds). Reads on the new
    table block until its snapshot completes, so the first value a read
    returns is a transactionally consistent state and must show the
    conserved total.
    """

    name = "source-table"

    def __init__(
        self, rng: random.Random, client: MzClient, scenario: "CdcBank"
    ) -> None:
        super().__init__(rng)
        self.client = client
        self.scenario = scenario
        self.nonce = 0
        self.next_at = time.monotonic() + 20.0

    def run(self) -> Outcome | None:
        now = time.monotonic()
        if now < self.next_at:
            return None
        self.next_at = now + self.rng.uniform(45.0, 90.0)
        # A leftover from a cycle whose drop had an UNKNOWN outcome.
        if self.nonce > 0:
            self.client.write(f"DROP TABLE IF EXISTS accounts_v{self.nonce}")
        self.nonce += 1
        name = f"accounts_v{self.nonce}"
        try:
            outcome = self.client.write(
                f"CREATE TABLE {name} FROM SOURCE bank_source"
                f" (REFERENCE {self.scenario.accounts_reference})"
            )
        except UnexpectedQueryError as e:
            # Purification reaches the upstream over the disrupted source leg,
            # so a plan-time connect failure is expected chaos, not a bug.
            if any(s in str(e) for s in UPSTREAM_CONNECT_ERROR_SNIPPETS):
                raise TransientError(str(e)) from e
            raise
        if outcome != Outcome.COMMITTED:
            return outcome
        # Also bail on stop: this poll can outlast the executor's join
        # ladder and would read as a stuck thread.
        deadline = time.monotonic() + 120.0
        validated = False
        while time.monotonic() < deadline and not self.scenario.ctx.stop.is_set():
            try:
                rows = self.client.query(
                    f"SELECT coalesce(sum(balance), 0) FROM {name}", timeout=30
                )
            except TransientError:
                continue
            total = int(rows[0][0])
            if total != self.scenario.total:
                if not self.scenario.snapshot_conservation_checked:
                    self.scenario.ctx.log.log(
                        "op",
                        f"SS-427: snapshot of {name} shows a non-conserved"
                        f" total: {total} != {self.scenario.total}",
                    )
                    break
                raise InvariantViolation(
                    f"snapshot of {name} shows a non-conserved total:"
                    f" {total} != {self.scenario.total}"
                )
            validated = True
            break
        if not validated:
            self.scenario.ctx.log.log(
                "op", f"{name} snapshot did not become readable in time"
            )
        self.client.write(f"DROP TABLE IF EXISTS {name}")
        return Outcome.COMMITTED if validated else Outcome.UNKNOWN

    def close(self) -> None:
        self.client.reset()


class UpstreamSchemaChurn(Action):
    """Documented-compatible upstream schema changes on the transfers table.

    Adding a column upstream, and dropping a column that was added after
    source creation, must not disturb ingestion (the column is simply not
    ingested). Upstream connection trouble is oracle-side and transient.
    """

    name = "upstream-ddl"

    def __init__(self, rng: random.Random, scenario: "CdcBank") -> None:
        super().__init__(rng)
        self.scenario = scenario
        self.nonce = 0
        self.next_at = time.monotonic() + 30.0

    def run(self) -> Outcome | None:
        now = time.monotonic()
        if now < self.next_at:
            return None
        self.next_at = now + self.rng.uniform(30.0, 60.0)
        self.nonce += 1
        statements = [f"ALTER TABLE transfers ADD COLUMN extra_{self.nonce} int"]
        if self.nonce > 1:
            statements.insert(
                0, f"ALTER TABLE transfers DROP COLUMN extra_{self.nonce - 1}"
            )
        try:
            conn = self.scenario.connect_upstream()
            try:
                cur = conn.cursor()
                for sql in statements:
                    cur.execute(sql)
                conn.commit()
            finally:
                conn.close()
        except Exception as e:
            # The previous cycle's ADD may not have applied (its connection
            # died), making this DROP fail. The next cycle moves on to a
            # fresh column name either way.
            raise TransientError(f"upstream DDL failed: {e}") from e
        return Outcome.COMMITTED


class CdcTotalPeek(PeekChecker):
    """Verifies the total through the maintained MV, an ad-hoc query, and a
    single query spanning both (which must observe one consistent snapshot
    across the two objects)."""

    QUERIES = [
        ("SELECT total FROM bank_total", 1),
        ("SELECT coalesce(sum(balance), 0) AS total FROM accounts_tbl", 1),
        (
            "SELECT total FROM bank_total UNION ALL"
            " SELECT coalesce(sum(balance), 0) FROM accounts_tbl",
            2,
        ),
    ]

    def __init__(self, rng, ctx, scenario: "CdcBank") -> None:
        super().__init__(rng, ctx, "total-peek", ["compute", "quickstart"])
        self.scenario = scenario

    def check_once(self) -> None:
        # rng, not round-robin: the cluster rotation has the same period as
        # the query list, round-robin would pin each form to one cluster.
        query, expected_rows = self.rng.choice(self.QUERIES)
        recent = self.scenario.recent_ts.get()
        if recent > 0 and self.rng.random() < 0.25:
            # Time travel into retained history. Conservation is
            # timestamp-free, so it must hold at every past timestamp too,
            # and a tear that the live state has already healed is still
            # recorded there.
            as_of = self.rng.randint(max(recent - 120_000, 1), recent)
            query, expected_rows = f"SELECT total FROM bank_total AS OF {as_of}", 1
        try:
            rows = self.peek(query)
        except UnexpectedQueryError as e:
            if "could not find a valid timestamp" in str(e):
                raise TransientError(f"{as_of} no longer readable") from None
            raise
        if len(rows) != expected_rows or any(
            int(row[0]) != self.scenario.total for row in rows
        ):
            raise InvariantViolation(
                f"upstream-transaction atomicity broken via {query!r}: expected"
                f" {expected_rows}x total {self.scenario.total}, got {rows}"
            )
        self.scenario.recent_ts.advance(
            int(self.client.query("SELECT mz_now()::text")[0][0])
        )
        self.validations += 1


class TransferCountPeek(PeekChecker):
    """Transfer count: monotonic, and bounded by the op ledger."""

    def __init__(self, rng, ctx, scenario: "CdcBank") -> None:
        super().__init__(rng, ctx, "transfers-peek", ["quickstart"])
        self.scenario = scenario

    def check_once(self) -> None:
        oplog = self.scenario.oplog
        watermark = self.scenario.transfer_rows.get()
        rows = self.peek("SELECT count(*) FROM transfers_tbl WHERE worker >= 0")
        high = oplog.attempted_count()
        count = int(rows[0][0])
        if count < watermark:
            raise InvariantViolation(
                f"transfer count went backwards: {count} < watermark {watermark}"
            )
        if count > high:
            raise InvariantViolation(
                f"transfer count {count} exceeds attempted transfers {high}"
            )
        # NOTE: no lower bound against committed transfers: ingestion lags the
        # upstream commit (linearizability does not cover source data), so
        # committed transfers are only guaranteed visible after converge.
        self.scenario.transfer_rows.advance(count)
        self.validations += 1


class CdcTotalSubscribe(SubscribeChecker):
    def __init__(self, rng, ctx, scenario: "CdcBank") -> None:
        super().__init__(rng, ctx, "total-subscribe", "SELECT total FROM bank_total")
        self.scenario = scenario

    def validate_state(self, state: dict[tuple, int], ts: int) -> None:
        expected = {(self.scenario.total,): 1}
        got = {(int(k[0]),): v for k, v in state.items()}
        if got != expected:
            raise InvariantViolation(
                f"total via SUBSCRIBE at {ts}: expected {expected}, got {got}"
                " (an upstream transaction was applied non-atomically)"
            )


class RtrPeek(Checker):
    """Real-time recency: a read issued after an upstream commit was
    acknowledged must include that commit.

    This is RTR's documented "at least" guarantee under strict serializable.
    RTR queries legitimately error or time out while the source connection
    is disrupted, which skips the round, but a read that succeeds while
    missing acknowledged upstream data is a violation.
    """

    name = "rtr-peek"
    pause = (2.0, 6.0)

    def __init__(self, rng, ctx, scenario: "CdcBank") -> None:
        super().__init__(rng)
        self.ctx = ctx
        self.scenario = scenario
        self.client = MzClient(ctx, "rtr-peek")
        self.markers = 0
        self._session_ready = False

    def check_once(self) -> None:
        conn = None
        marker = self.markers + 1
        try:
            conn = self.scenario.connect_upstream()
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO transfers (worker, seq, src, dst, amount)"
                f" VALUES ({RTR_MARKER_WORKER}, {marker}, 0, 0, 0)"
            )
            conn.commit()
        except Exception as e:
            raise TransientError(f"upstream marker insert failed: {e}") from e
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass
        # The marker is acknowledged upstream. If the commit had failed with
        # an unknown outcome we would not get here, and an unknown commit
        # that still landed only increases the count, which the >= check
        # tolerates.
        self.markers = marker
        try:
            if not self._session_ready:
                self.client.query("SET real_time_recency = true")
                self._session_ready = True
            rows = self.client.query(
                "SELECT count(*) FROM transfers_tbl"
                f" WHERE worker = {RTR_MARKER_WORKER}"
            )
        except TransientError:
            # The connection was dropped, so the session setting is gone.
            self._session_ready = False
            raise
        except UnexpectedQueryError as e:
            self._session_ready = False
            # The recency probe contacts the upstream database over the
            # disrupted leg, so besides the RTR timeout it can surface the
            # storage layer's connection failures. Both are expected chaos,
            # only a successful read with missing data is a violation.
            if "timed out before ingesting" in str(e) or "storage error" in str(e):
                raise TransientError(str(e)) from e
            raise
        count = int(rows[0][0])
        if count < self.markers:
            raise InvariantViolation(
                f"real-time recency violated: read issued after upstream ack"
                f" saw {count} markers, expected at least {self.markers}"
            )
        self.validations += 1

    def close(self) -> None:
        self.client.reset()


# A transaction's rows say how many of them there should be, so a group of
# any other size means the transaction became visible in pieces.
WIDE_TXN_TORN_SQL = (
    "SELECT txn_id, count(*), txn_rows FROM wide_txn_tbl"
    " GROUP BY txn_id, txn_rows HAVING count(*) <> txn_rows"
)


class WideTransaction(Action):
    """One upstream transaction writing many self-describing rows.

    The transfer workload commits two rows at a time, and a two-row
    transaction is a small target: an interrupted snapshot or a resumption has
    to land exactly between them to tear. These transactions are tens of rows
    wide, so the window a reader can fall into is correspondingly wider.

    Every row carries the id of its transaction and the number of rows that
    transaction wrote, which makes the atomicity invariant self-contained: no
    checker state, no knowledge of which transactions committed, and it holds
    at every timestamp, including ones in retained history.
    """

    name = "wide-txn"

    def __init__(self, rng: random.Random, worker: int, scenario: "CdcBank") -> None:
        super().__init__(rng)
        self.worker = worker
        self.scenario = scenario
        self.conn: Any = None
        self.seq = 0

    def run(self) -> Outcome | None:
        if self.conn is None:
            try:
                self.conn = self.scenario.connect_upstream()
            except Exception as e:
                raise TransientError(f"upstream connect failed: {e}") from e
        self.seq += 1
        # Unique across workers without coordination.
        txn_id = self.worker * 10_000_000 + self.seq
        rows = self.rng.randint(5, 50)
        values = ", ".join(f"({txn_id}, {rows}, {i})" for i in range(rows))
        try:
            self.conn.cursor().execute(
                f"INSERT INTO wide_txn (txn_id, txn_rows, idx) VALUES {values}"
            )
        except Exception:
            try:
                self.conn.rollback()
            except Exception:
                self._drop()
            return Outcome.FAILED
        try:
            self.conn.commit()
        except Exception:
            self._drop()
            return Outcome.UNKNOWN
        return Outcome.COMMITTED

    def _drop(self) -> None:
        conn, self.conn = self.conn, None
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

    def close(self) -> None:
        self._drop()


class CdcBank(Scenario):
    # Real-time recency is documented for Kafka, PostgreSQL, and MySQL
    # sources only.
    rtr_supported = True
    # Whether the documented-compatible upstream schema changes (add
    # column, drop later-added column) are exercised. SQL Server is
    # excluded: its CDC capture instances pin the captured column list, so
    # column changes are a different, heavier operation there.
    upstream_ddl_supported = True
    # The CREATE TABLE .. FROM SOURCE reference for the accounts table.
    accounts_reference: str
    # TODO: Reenable when SS-427 is fixed. A MySQL snapshot taken while the
    # upstream connection is being truncated can expose half of an upstream
    # transaction, which shows up here as a non-conserved total.
    snapshot_conservation_checked = True

    def __init__(self, ctx: ScenarioContext) -> None:
        super().__init__(ctx)
        self.accounts = ctx.complexity.accounts
        self.total = self.accounts * BALANCE_PER_ACCOUNT
        self.oplog = OpLog()
        self.transfer_rows = Watermark()
        # Recent timestamps observed by the conservation peeks, so the same
        # invariant can be re-asked of retained history.
        self.recent_ts = Watermark()

    @abstractmethod
    def connect_upstream(self) -> Any:
        """A new connection to the upstream database (direct, not proxied)."""

    @abstractmethod
    def setup_upstream(self) -> None:
        pass

    @abstractmethod
    def mz_setup_sql(self) -> list[str]:
        pass

    def setup(self) -> None:
        self.setup_upstream()
        client = MzClient(self.ctx, "setup")
        for sql in self.mz_setup_sql() + [
            # RETAIN HISTORY so the conservation oracle can be asked of past
            # timestamps: a torn upstream transaction is durable in the
            # shard's history even once the live state has converged, which
            # is how SS-427 stayed findable after the fact.
            "CREATE MATERIALIZED VIEW bank_total IN CLUSTER compute"
            " WITH (RETAIN HISTORY = FOR '600s') AS"
            " SELECT coalesce(sum(balance), 0) AS total FROM accounts_tbl",
            "CREATE INDEX bank_total_idx IN CLUSTER compute ON bank_total (total)",
        ]:
            client.query(sql, timeout=120)

        def snapshot_done() -> bool:
            rows = client.query("SELECT count(*) FROM accounts_tbl")
            return int(rows[0][0]) == self.accounts

        wait_until(snapshot_done, CONVERGE_TIMEOUT, "source snapshot")
        client.reset()

    def make_worker(self, index: int, rng: random.Random) -> WorkerBundle:
        actions: list[Action] = [
            UpstreamTransfer(rng, index, self),
            WideTransaction(rng, index, self),
        ]
        weights = [20, 4]
        if index == 0:
            actions.append(
                SourceTableChurn(rng, MzClient(self.ctx, "source-table"), self)
            )
            weights.append(1)
            if self.upstream_ddl_supported:
                actions.append(UpstreamSchemaChurn(rng, self))
                weights.append(1)
        return WorkerBundle(actions=actions, weights=weights)

    def checkers(self) -> list[Checker]:
        rngs = [random.Random(self.ctx.rng.randrange(SEED_RANGE)) for _ in range(6)]
        checkers: list[Checker] = [
            CdcTotalPeek(rngs[0], self.ctx, self),
            GroupCompletenessPeek(
                rngs[5],
                self.ctx,
                "wide-txn-atomicity",
                WIDE_TXN_TORN_SQL,
                clusters=["quickstart"],
                history=self.recent_ts,
            ),
            TransferCountPeek(rngs[1], self.ctx, self),
            CdcTotalSubscribe(rngs[2], self.ctx, self),
            ProgressPeek(rngs[3], self.ctx, "bank_source"),
        ]
        if self.rtr_supported:
            checkers.append(RtrPeek(rngs[4], self.ctx, self))
        return checkers

    def _upstream_state(self) -> tuple[list[tuple[int, int]], int]:
        conn = self.connect_upstream()
        try:
            cur = conn.cursor()
            cur.execute("SELECT id, balance FROM accounts ORDER BY id")
            accounts = [(int(r[0]), int(r[1])) for r in cur.fetchall()]
            cur.execute("SELECT count(*) FROM transfers")
            transfers = int(cur.fetchone()[0])
            return accounts, transfers
        finally:
            conn.close()

    def _mz_state(self, client: MzClient) -> tuple[list[tuple[int, int]], int]:
        client.query("SET cluster = quickstart")
        accounts = [
            (int(r[0]), int(r[1]))
            for r in client.query("SELECT id, balance FROM accounts_tbl ORDER BY id")
        ]
        transfers = int(client.query("SELECT count(*) FROM transfers_tbl")[0][0])
        return accounts, transfers

    def converge(self) -> None:
        client = MzClient(self.ctx, "converge")
        upstream = self._upstream_state()

        def caught_up() -> bool:
            return self._mz_state(client) == upstream

        wait_until(caught_up, CONVERGE_TIMEOUT, "CDC ingestion catching up")
        client.reset()

    def final_check(self) -> None:
        client = MzClient(self.ctx, "final-check")
        upstream_accounts, upstream_transfers = self._upstream_state()
        mz_accounts, mz_transfers = self._mz_state(client)
        if mz_accounts != upstream_accounts:
            diff = [
                (up, mz) for up, mz in zip(upstream_accounts, mz_accounts) if up != mz
            ]
            raise InvariantViolation(
                f"account state diverged from upstream: {diff[:20]}"
            )
        if mz_transfers != upstream_transfers:
            raise InvariantViolation(
                f"transfer count diverged: mz {mz_transfers} !="
                f" upstream {upstream_transfers}"
            )
        if sum(balance for _, balance in mz_accounts) != self.total:
            raise InvariantViolation(f"final balances do not sum to {self.total}")
        for worker in range(self.ctx.complexity.workers):
            present = {
                int(row[0])
                for row in client.query(
                    f"SELECT seq FROM transfers_tbl WHERE worker = {worker}"
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
        client.reset()

    def diagnostics(self) -> None:
        client = MzClient(self.ctx, "post-heal")
        try:
            mz_accounts, mz_transfers = self._mz_state(client)
            upstream_accounts, upstream_transfers = self._upstream_state()
            verdict = (
                "converged"
                if (mz_accounts, mz_transfers)
                == (upstream_accounts, upstream_transfers)
                else "still diverged"
            )
            self.ctx.log.log(
                "diag",
                f"post-heal: mz transfers={mz_transfers}"
                f" upstream={upstream_transfers}, accounts {verdict}",
            )
        except Exception as e:
            self.ctx.log.log("diag", f"post-heal comparison failed: {e}")
        client.reset()


class PgCdcBank(CdcBank):
    name = "pg-cdc-bank"
    accounts_reference = "accounts"
    services = ["postgres"]
    legs = [
        "metadata",
        "blob",
        "clusterd-storage",
        "clusterd-compute",
        "clusterd-compute2",
        "pg",
    ]

    def connect_upstream(self) -> Any:
        import psycopg

        return psycopg.connect(
            host=self.ctx.endpoints.mz_host,
            port=self.ctx.endpoints.pg_port,
            user="postgres",
            password="postgres",
            dbname="postgres",
            connect_timeout=10,
        )

    def setup_upstream(self) -> None:
        conn = self.connect_upstream()
        cur = conn.cursor()
        cur.execute("CREATE TABLE accounts (id int PRIMARY KEY, balance bigint)")
        cur.execute(
            "CREATE TABLE transfers (worker int, seq bigint, src int, dst int,"
            " amount bigint)"
        )
        cur.execute("CREATE TABLE wide_txn (txn_id bigint, txn_rows int, idx int)")
        cur.execute("ALTER TABLE accounts REPLICA IDENTITY FULL")
        cur.execute("ALTER TABLE transfers REPLICA IDENTITY FULL")
        cur.execute("ALTER TABLE wide_txn REPLICA IDENTITY FULL")
        cur.execute(
            "INSERT INTO accounts SELECT generate_series(0, %s), %s",
            (self.accounts - 1, BALANCE_PER_ACCOUNT),
        )
        cur.execute(
            "CREATE PUBLICATION bank_pub FOR TABLE accounts, transfers, wide_txn"
        )
        conn.commit()
        conn.close()

    def mz_setup_sql(self) -> list[str]:
        return [
            "CREATE SECRET pgpass AS 'postgres'",
            "CREATE CONNECTION pg_conn TO POSTGRES (HOST 'toxiproxy', PORT 5432,"
            " DATABASE postgres, USER postgres, PASSWORD SECRET pgpass)",
            "CREATE SOURCE bank_source IN CLUSTER storage"
            " FROM POSTGRES CONNECTION pg_conn (PUBLICATION 'bank_pub')",
            "CREATE TABLE accounts_tbl FROM SOURCE bank_source (REFERENCE accounts)",
            "CREATE TABLE transfers_tbl FROM SOURCE bank_source (REFERENCE transfers)",
            "CREATE TABLE wide_txn_tbl FROM SOURCE bank_source (REFERENCE wide_txn)",
        ]


class MySqlCdcBank(CdcBank):
    name = "mysql-cdc-bank"
    snapshot_conservation_checked = False
    accounts_reference = "bank.accounts"
    services = ["mysql"]
    legs = [
        "metadata",
        "blob",
        "clusterd-storage",
        "clusterd-compute",
        "clusterd-compute2",
        "mysql",
    ]

    def connect_upstream(self) -> Any:
        import pymysql

        from materialize.mzcompose.services.mysql import MySql

        assert self.ctx.endpoints.mysql_port is not None
        return pymysql.connect(
            host=self.ctx.endpoints.mz_host,
            port=self.ctx.endpoints.mysql_port,
            user="root",
            password=MySql.DEFAULT_ROOT_PASSWORD,
            database="bank",
            connect_timeout=10,
        )

    def setup_upstream(self) -> None:
        import pymysql

        from materialize.mzcompose.services.mysql import MySql

        assert self.ctx.endpoints.mysql_port is not None
        admin = pymysql.connect(
            host=self.ctx.endpoints.mz_host,
            port=self.ctx.endpoints.mysql_port,
            user="root",
            password=MySql.DEFAULT_ROOT_PASSWORD,
            connect_timeout=10,
        )
        admin.cursor().execute("CREATE DATABASE bank")
        admin.close()
        conn = self.connect_upstream()
        cur = conn.cursor()
        cur.execute("CREATE TABLE accounts (id int PRIMARY KEY, balance bigint)")
        cur.execute(
            "CREATE TABLE transfers (worker int, seq bigint, src int, dst int,"
            " amount bigint)"
        )
        cur.execute("CREATE TABLE wide_txn (txn_id bigint, txn_rows int, idx int)")
        # Batched so the large complexity does not pay one round trip per row.
        for start in range(0, self.accounts, 1000):
            values = ", ".join(
                f"({account}, {BALANCE_PER_ACCOUNT})"
                for account in range(start, min(start + 1000, self.accounts))
            )
            cur.execute(f"INSERT INTO accounts VALUES {values}")
        conn.commit()
        conn.close()

    def mz_setup_sql(self) -> list[str]:
        from materialize.mzcompose.services.mysql import MySql

        return [
            f"CREATE SECRET mysqlpass AS '{MySql.DEFAULT_ROOT_PASSWORD}'",
            "CREATE CONNECTION mysql_conn TO MYSQL (HOST 'toxiproxy', PORT 3306,"
            " USER root, PASSWORD SECRET mysqlpass)",
            "CREATE SOURCE bank_source IN CLUSTER storage"
            " FROM MYSQL CONNECTION mysql_conn",
            "CREATE TABLE accounts_tbl FROM SOURCE bank_source"
            " (REFERENCE bank.accounts)",
            "CREATE TABLE transfers_tbl FROM SOURCE bank_source"
            " (REFERENCE bank.transfers)",
            "CREATE TABLE wide_txn_tbl FROM SOURCE bank_source"
            " (REFERENCE bank.wide_txn)",
        ]


class SqlServerCdcBank(CdcBank):
    name = "sqlserver-cdc-bank"
    rtr_supported = False
    upstream_ddl_supported = False
    # SQL Server captures changes with a polling agent that scans the
    # transaction log on a schedule, rather than streaming it the way Postgres
    # logical replication and the MySQL binlog do. Its capture rate is
    # therefore a ceiling the writers can exceed, and once they do the lag
    # grows for the rest of the run and converge has to drain all of it.
    # Fewer writers keeps the bottleneck inside Materialize, which is what
    # this test is about.
    max_workers = 6
    accounts_reference = "dbo.accounts"
    services = ["sql-server"]
    legs = [
        "metadata",
        "blob",
        "clusterd-storage",
        "clusterd-compute",
        "clusterd-compute2",
        "sql-server",
    ]

    def _connect(self, database: str, autocommit: bool) -> Any:
        import pymssql

        from materialize.mzcompose.services.sql_server import SqlServer

        assert self.ctx.endpoints.sqlserver_port is not None
        return pymssql.connect(
            server=self.ctx.endpoints.mz_host,
            port=str(self.ctx.endpoints.sqlserver_port),
            user=SqlServer.DEFAULT_USER,
            password=SqlServer.DEFAULT_SA_PASSWORD,
            database=database,
            autocommit=autocommit,
            login_timeout=10,
            timeout=60,
        )

    def connect_upstream(self) -> Any:
        return self._connect("bank", autocommit=False)

    def setup_upstream(self) -> None:
        admin = self._connect("master", autocommit=True)
        cur = admin.cursor()
        # SQL Server caps a VALUES constructor at 1000 rows.
        inserts = [
            "INSERT INTO accounts VALUES "
            + ", ".join(
                f"({account}, {BALANCE_PER_ACCOUNT})"
                for account in range(start, min(start + 1000, self.accounts))
            )
            for start in range(0, self.accounts, 1000)
        ]
        for sql in [
            "CREATE DATABASE bank",
            "USE bank",
            "EXEC sys.sp_cdc_enable_db",
            "ALTER DATABASE bank SET ALLOW_SNAPSHOT_ISOLATION ON",
            "CREATE TABLE accounts (id int PRIMARY KEY, balance bigint NOT NULL)",
            "CREATE TABLE transfers (worker int NOT NULL, seq bigint NOT NULL,"
            " src int, dst int, amount bigint, PRIMARY KEY (worker, seq))",
            "CREATE TABLE wide_txn (txn_id bigint NOT NULL, txn_rows int NOT NULL,"
            " idx int NOT NULL)",
            *inserts,
            "EXEC sys.sp_cdc_enable_table @source_schema = 'dbo',"
            " @source_name = 'accounts', @role_name = 'SA',"
            " @supports_net_changes = 0",
            "EXEC sys.sp_cdc_enable_table @source_schema = 'dbo',"
            " @source_name = 'transfers', @role_name = 'SA',"
            " @supports_net_changes = 0",
            "EXEC sys.sp_cdc_enable_table @source_schema = 'dbo',"
            " @source_name = 'wide_txn', @role_name = 'SA',"
            " @supports_net_changes = 0",
            # SQL Server CDC only advances its LSN watermark with new log
            # activity. Without a ticker the source frontier stalls as soon
            # as the workers quiesce and converge can never finish, so an
            # agent job keeps updating a dummy CDC-enabled table, mirroring
            # test/sql-server-cdc/setup/setup.td.
            "CREATE TABLE ticker (t datetime)",
            "INSERT INTO ticker VALUES (CURRENT_TIMESTAMP)",
            "EXEC sys.sp_cdc_enable_table @source_schema = 'dbo',"
            " @source_name = 'ticker', @role_name = 'SA',"
            " @supports_net_changes = 0",
            # NOTE: the procedure name must differ from the ticker table's,
            # SQL Server names are case-insensitive under the default
            # collation.
            "CREATE OR ALTER PROCEDURE dbo.TickerJob AS BEGIN SET NOCOUNT ON;"
            " WHILE 1 = 1 BEGIN UPDATE dbo.ticker SET t = CURRENT_TIMESTAMP;"
            " WAITFOR DELAY '00:00:01'; END END",
            "USE msdb",
            "EXEC sp_add_job @job_name = N'TickerJob', @enabled = 1",
            "EXEC sp_add_jobstep @job_name = N'TickerJob', @step_name = N'run',"
            " @subsystem = N'TSQL', @database_name = N'bank',"
            " @command = N'EXEC dbo.TickerJob;'",
            "EXEC sp_add_jobserver @job_name = N'TickerJob'",
            "EXEC msdb.dbo.sp_start_job N'TickerJob'",
        ]:
            cur.execute(sql)
        admin.close()

    def mz_setup_sql(self) -> list[str]:
        from materialize.mzcompose.services.sql_server import SqlServer

        return [
            f"CREATE SECRET sqlserverpass AS '{SqlServer.DEFAULT_SA_PASSWORD}'",
            "CREATE CONNECTION sqlserver_conn TO SQL SERVER (HOST 'toxiproxy',"
            " PORT 1433, DATABASE bank, USER 'SA',"
            " PASSWORD = SECRET sqlserverpass)",
            "CREATE SOURCE bank_source IN CLUSTER storage"
            " FROM SQL SERVER CONNECTION sqlserver_conn",
            "CREATE TABLE accounts_tbl FROM SOURCE bank_source"
            " (REFERENCE dbo.accounts)",
            "CREATE TABLE transfers_tbl FROM SOURCE bank_source"
            " (REFERENCE dbo.transfers)",
            "CREATE TABLE wide_txn_tbl FROM SOURCE bank_source"
            " (REFERENCE dbo.wide_txn)",
        ]
