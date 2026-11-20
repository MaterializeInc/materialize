# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""MySQL source, a `cdc.Dialect`.

Positions are transaction numbers of the GTIDs of the server's own UUID. The
driver is the server's only writer, so the largest number in
`gtid_executed` right after a commit is that commit's. The source's progress
holds, per UUID, the next number it expects.
"""

from __future__ import annotations

from typing import Any
from urllib.parse import unquote, urlparse

from antithesis.assertions import (  # pyright: ignore[reportMissingModuleSource]
    always,
    sometimes,
)

from materialize.antithesis_driver import cdc
from materialize.antithesis_driver.context import Context

DATABASE = "drv"
SCHEMA = "mysqlsrc"
UNKNOWN_THREAD = 1094


def gtid_max(gtid_set: str, uuid: str) -> int:
    """The largest transaction number of `uuid` in a GTID set such as
    `3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5:7, ...`, or 0."""
    best = 0
    for member in gtid_set.replace("\n", "").split(","):
        parts = member.strip().split(":")
        if parts[0].lower() != uuid.lower():
            continue
        for interval in parts[1:]:
            best = max(best, int(interval.split("-")[-1]))
    return best


class MySql:
    schema = SCHEMA
    ledger = f"{DATABASE}.ledger"
    rtr_needs_capture = False

    def __init__(self, url: str, mz_host: str | None) -> None:
        import pymysql

        self.errors: tuple[type[BaseException], ...] = (pymysql.err.Error, OSError)
        u = urlparse(url)
        self.host = u.hostname or "localhost"
        self.port = u.port or 3306
        self.user = unquote(u.username or "root")
        self.password = unquote(u.password or "")
        mz = urlparse(f"//{mz_host or u.netloc.split('@')[-1]}")
        self.mz_host, self.mz_port = mz.hostname, mz.port or 3306
        self.server_uuid = ""
        """Read at setup. A restarted server keeps its UUID."""

    def connect(self) -> Any:
        import pymysql

        return pymysql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            autocommit=True,
            charset="utf8mb4",
            connect_timeout=10,
            read_timeout=30,
            write_timeout=30,
        )

    def setup_upstream(self) -> None:
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute(f"DROP DATABASE IF EXISTS {DATABASE}")
            cur.execute(f"CREATE DATABASE {DATABASE}")
            for j in range(cdc.TABLES):
                cur.execute(f"CREATE TABLE {DATABASE}.t{j} (k int PRIMARY KEY, v text)")
            cur.execute(
                f"CREATE TABLE {self.ledger}"
                " (txn bigint, tbl int, PRIMARY KEY (txn, tbl))"
            )
            cur.execute("SELECT @@server_uuid")
            self.server_uuid = cur.fetchone()[0]

    def setup_statements(self) -> list[str]:
        password = self.password.replace("'", "''")
        return [
            f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}",
            f"CREATE SECRET IF NOT EXISTS {SCHEMA}.password AS '{password}'",
            f"CREATE CONNECTION IF NOT EXISTS {SCHEMA}.conn TO MYSQL"
            f" (HOST '{self.mz_host}', PORT {self.mz_port}, USER '{self.user}',"
            f" PASSWORD SECRET {SCHEMA}.password)",
            f"CREATE SOURCE IF NOT EXISTS {SCHEMA}.src IN CLUSTER {cdc.CLUSTER}"
            f" FROM MYSQL CONNECTION {SCHEMA}.conn",
            f"CREATE TABLE IF NOT EXISTS {SCHEMA}.ledger FROM SOURCE {SCHEMA}.src"
            f" (REFERENCE {self.ledger})",
            *(
                f"CREATE TABLE IF NOT EXISTS {SCHEMA}.t{j} FROM SOURCE {SCHEMA}.src"
                f" (REFERENCE {DATABASE}.t{j})"
                for j in range(cdc.TABLES)
            ),
        ]

    def session(self, cur: Any) -> Any:
        cur.execute("SELECT CONNECTION_ID()")
        return int(cur.fetchone()[0])

    def end_session(self, cur: Any, session: Any) -> bool:
        import pymysql

        # A KILL that finds the thread returns before the thread has rolled
        # back, so only the error for a missing thread proves it gone. Ids
        # restart with the server, and a restart ended the old transaction.
        try:
            cur.execute(f"KILL {int(session)}")
        except pymysql.err.OperationalError as e:
            if e.args[0] == UNKNOWN_THREAD:
                return True
            raise
        return False

    def _executed(self, cur: Any) -> int:
        cur.execute("SELECT @@global.gtid_executed")
        return gtid_max(cur.fetchone()[0], self.server_uuid)

    def begin(self, cur: Any) -> int:
        before = self._executed(cur)
        cur.execute("START TRANSACTION")
        return before + 1

    def statement(self, plan: cdc.Plan) -> tuple[str, tuple[Any, ...]]:
        t = f"{DATABASE}.t{plan.table}"
        if plan.kind == "upsert":
            return (
                f"INSERT INTO {t} (k, v) VALUES (%s, %s) AS new"
                " ON DUPLICATE KEY UPDATE v = new.v",
                (plan.key, plan.value),
            )
        if plan.kind == "update":
            return f"UPDATE {t} SET v = %s WHERE k = %s", (plan.value, plan.key)
        if plan.kind == "delete":
            return f"DELETE FROM {t} WHERE k = %s", (plan.key,)
        assert plan.kind == "delete_range"
        return f"DELETE FROM {t} WHERE k BETWEEN %s AND %s", (plan.key, plan.high)

    def commit(self, cur: Any) -> None:
        cur.execute("COMMIT")

    def position_after(self, cur: Any) -> int | None:
        return self._executed(cur)

    def positions(self, cur: Any, txns: list[int]) -> dict[int, int]:
        return {}

    def add_column(self, table: int, n: int) -> str:
        return f"ALTER TABLE {DATABASE}.t{table} ADD COLUMN c{n} varchar(8) DEFAULT 'c'"

    def progress(self) -> str:
        return (
            "SELECT 'p', source_id_lower::text || ' ' || source_id_upper::text,"
            f" transaction_id::text FROM {SCHEMA}.src"
        )

    def parse_progress(self, rows: list[tuple[str | None, str | None]]) -> int | None:
        if not rows:
            return None
        for bounds, next_id in rows:
            lower, _, upper = (bounds or "").partition(" ")
            if lower == upper == self.server_uuid.lower():
                return int(next_id) if next_id is not None else 0
        # No transaction of the server ingested yet.
        return 0

    def report(self, r: cdc.Review) -> None:
        v, base = r.verdict, r.details
        always(
            not v.duplicate_ledger,
            "mysql source: the ledger export holds at most one row per primary key",
            {**base, "duplicates": v.duplicate_ledger[:10]},
        )
        always(
            not v.unattempted,
            "mysql source: every ledger row visible in Materialize was written by the workload",
            {**base, "unattempted": v.unattempted[:10]},
        )
        always(
            not v.metadata,
            "mysql source: visible ledger rows preserve recorded transaction metadata and exclude aborted transactions",
            {**base, "transactions": v.metadata[:10]},
        )
        always(
            not v.torn,
            "mysql source: a transaction's ledger rows are visible together or not at all",
            {**base, "torn": v.torn[:10]},
        )
        always(
            not v.gaps,
            "mysql source: visible ledger rows form a gap-free prefix of the commit order",
            {**base, "beyond_the_prefix": v.gaps[:10]},
        )
        always(
            not v.committed_below_missing,
            "mysql source: transactions below the mapped GTID are visible in the ledger",
            {**base, "missing": v.committed_below_missing[:10]},
        )
        always(
            not v.begun_above_visible,
            "mysql source: transactions above the mapped GTID are not visible in the ledger",
            {**base, "early": v.begun_above_visible[:10]},
        )
        always(
            not r.regressions,
            "mysql source: the mapped GTID is monotone in mz time",
            {**base, "conflicting": r.regressions[:5]},
        )
        if v.prefix is not None:
            always(
                not v.duplicate_keys,
                "mysql source: an export holds at most one row per upstream primary key",
                {**base, "duplicates": v.duplicate_keys},
            )
            always(
                not v.mismatches,
                "mysql source: an export equals its upstream table replayed to the visible prefix",
                {**base, "prefix": v.prefix, "mismatches": v.mismatches},
            )
        if r.rtr and v.prefix is not None:
            always(
                not r.rtr_missing,
                "mysql source: a real-time recency read observes every upstream transaction committed before it began",
                {**base, "missing": r.rtr_missing[:10]},
            )
        sometimes(
            v.committed_below > len(v.committed_below_missing),
            "mysql source: a ledger check found an acknowledged transaction below the mapped GTID",
            {**base, "committed_below": v.committed_below},
        )
        sometimes(
            v.compared_rows > 0 and not v.mismatches,
            "mysql source: an export check matched nonempty exports",
            {**base, "rows": v.compared_rows},
        )
        sometimes(
            r.added_columns and v.prefix is not None and not v.mismatches,
            "mysql source: exports still match after an upstream ADD COLUMN",
            base,
        )

    def report_resolved(self, committed: bool, txn: int) -> None:
        sometimes(
            committed,
            "mysql source: a transaction whose commit response was lost turned out committed",
            {"txn": txn},
        )

    def report_caught_up(self, caught_up: bool, details: dict[str, Any]) -> None:
        always(
            caught_up,
            "mysql source: after faults stop the source reflects every committed upstream transaction",
            details,
        )


def create(ctx: Context) -> cdc.CdcSource | None:
    url = ctx.config.upstream_mysql_url
    if url is None:
        return None
    return cdc.CdcSource(ctx, MySql(url, ctx.config.upstream_mysql_mz_host))
