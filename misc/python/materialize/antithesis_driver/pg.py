# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Postgres source, a `cdc.Dialect`. Positions are LSNs.

The safety assertions keep the messages of the legacy Postgres source
driver. The real-time recency, coverage and post-fault ones are new. That
driver bucketed its export checks by whether a TRUNCATE or DROP preceded the
last ingestion restart, known bug classes. This one issues neither, so every
check falls in the bucket without one.
"""

from __future__ import annotations

from typing import Any
from urllib.parse import unquote, urlparse

import psycopg
from antithesis.assertions import (  # pyright: ignore[reportMissingModuleSource]
    always,
    always_or_unreachable,
    sometimes,
)

from materialize.antithesis_driver import cdc, cdc_types
from materialize.antithesis_driver.client import RolledBack, Upstream, connect
from materialize.antithesis_driver.context import Context

UPSTREAM_SCHEMA = "drv"
PUBLICATION = "drv_pub"
SCHEMA = "pgsrc"


def parse_lsn(text: str) -> int:
    """An LSN as `X/Y`, as Postgres prints it, or as a decimal `uint8`."""
    if "/" in text:
        hi, lo = text.split("/")
        return (int(hi, 16) << 32) | int(lo, 16)
    return int(text)


class Postgres:
    schema = SCHEMA
    ledger = f"{UPSTREAM_SCHEMA}.ledger"
    errors = (psycopg.Error, OSError, RolledBack)
    rtr_needs_capture = False
    types = cdc_types.POSTGRES
    types_table = f"{UPSTREAM_SCHEMA}.types"

    def __init__(self, url: str, mz_host: str | None) -> None:
        self.url = url
        self.mz_host = mz_host

    def connect(self) -> Any:
        conn = connect(Upstream(self.url), "driver-pg")
        conn.execute(b"SET statement_timeout = '30s'")
        conn.execute(b"SET lock_timeout = '10s'")
        return conn

    def setup_upstream(self) -> None:
        with connect(Upstream(self.url), "driver-setup") as conn:
            conn.execute(f"DROP PUBLICATION IF EXISTS {PUBLICATION}".encode())
            conn.execute(f"DROP SCHEMA IF EXISTS {UPSTREAM_SCHEMA} CASCADE".encode())
            # A local loop rerun leaves the last run's slots behind.
            conn.execute(
                b"SELECT pg_drop_replication_slot(slot_name)"
                b" FROM pg_replication_slots WHERE NOT active"
            )
            conn.execute(f"CREATE SCHEMA {UPSTREAM_SCHEMA}".encode())
            for j in range(cdc.TABLES):
                t = f"{UPSTREAM_SCHEMA}.t{j}"
                conn.execute(f"CREATE TABLE {t} (k int PRIMARY KEY, v text)".encode())
                conn.execute(f"ALTER TABLE {t} REPLICA IDENTITY FULL".encode())
            conn.execute(
                f"CREATE TABLE {self.ledger}"
                " (txn bigint, tbl int, PRIMARY KEY (txn, tbl))".encode()
            )
            conn.execute(f"ALTER TABLE {self.ledger} REPLICA IDENTITY FULL".encode())
            values = ", ".join(f"'{v}'" for v in cdc_types.ENUM_VALUES)
            conn.execute(
                f"CREATE TYPE {cdc_types.POSTGRES_ENUM} AS ENUM ({values})".encode()
            )
            t = self.types_table
            conn.execute(cdc_types.create_table(t, self.types).encode())
            conn.execute(f"ALTER TABLE {t} REPLICA IDENTITY FULL".encode())
            for c in self.types:
                if c.big is not None:
                    # Out of line whatever the value compresses to, so an
                    # update that leaves it untouched sends no value for it.
                    conn.execute(
                        f"ALTER TABLE {t} ALTER COLUMN {c.name} SET STORAGE EXTERNAL".encode()
                    )
            for statement, params in cdc_types.sweep(t, self.types):
                conn.execute(statement.encode(), params)
            conn.execute(
                f"CREATE PUBLICATION {PUBLICATION} FOR TABLES IN SCHEMA {UPSTREAM_SCHEMA}".encode()
            )

    def setup_statements(self) -> list[str]:
        url = urlparse(self.url)
        # Materialize may reach the server at another address than the
        # driver, such as inside a Docker network: `host` or `host:port`.
        mz = urlparse(f"//{self.mz_host or url.netloc.split('@')[-1]}")
        host, port = mz.hostname, mz.port or 5432
        password = unquote(url.password or "").replace("'", "''")
        database = url.path.lstrip("/") or "postgres"
        return [
            f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}",
            f"CREATE SECRET IF NOT EXISTS {SCHEMA}.password AS '{password}'",
            f"CREATE CONNECTION IF NOT EXISTS {SCHEMA}.conn TO POSTGRES"
            f" (HOST '{host}', PORT {port}, USER '{unquote(url.username or 'postgres')}',"
            f" PASSWORD SECRET {SCHEMA}.password, DATABASE '{database}')",
            f"CREATE SOURCE IF NOT EXISTS {SCHEMA}.src IN CLUSTER {cdc.CLUSTER}"
            f" FROM POSTGRES CONNECTION {SCHEMA}.conn (PUBLICATION '{PUBLICATION}')",
            f"CREATE TABLE IF NOT EXISTS {SCHEMA}.ledger FROM SOURCE {SCHEMA}.src"
            f" (REFERENCE {self.ledger})",
            *(
                f"CREATE TABLE IF NOT EXISTS {SCHEMA}.t{j} FROM SOURCE {SCHEMA}.src"
                f" (REFERENCE {UPSTREAM_SCHEMA}.t{j})"
                for j in range(cdc.TABLES)
            ),
            f"CREATE TABLE IF NOT EXISTS {SCHEMA}.types FROM SOURCE {SCHEMA}.src"
            f" (REFERENCE {self.types_table}){cdc_types.text_columns(self.types)}",
        ]

    def session(self, cur: Any) -> Any:
        cur.execute(
            "SELECT pid, backend_start::text FROM pg_stat_activity"
            " WHERE pid = pg_backend_pid()"
        )
        return tuple(cur.fetchone())

    def end_session(self, cur: Any, session: Any) -> bool:
        where = "FROM pg_stat_activity WHERE pid = %s AND backend_start::text = %s"
        cur.execute(f"SELECT pg_terminate_backend(pid) {where}", session)
        cur.execute(f"SELECT count(*) {where}", session)
        return cur.fetchone()[0] == 0

    def begin(self, cur: Any) -> int:
        cur.execute("BEGIN")
        cur.execute("SELECT pg_current_wal_insert_lsn()::text")
        return parse_lsn(cur.fetchone()[0])

    def statement(self, plan: cdc.Plan) -> tuple[str, tuple[Any, ...]]:
        t = f"{UPSTREAM_SCHEMA}.t{plan.table}"
        if plan.kind == "upsert":
            return (
                f"INSERT INTO {t} VALUES (%s, %s) ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v",
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
        if cur.statusmessage != "COMMIT":
            raise RolledBack(cur.statusmessage)

    def position_after(self, cur: Any) -> int | None:
        cur.execute("SELECT pg_current_wal_lsn()::text")
        return parse_lsn(cur.fetchone()[0])

    def positions(self, cur: Any, txns: list[int]) -> dict[int, int]:
        return {}

    def add_column(self, table: int, n: int) -> str:
        return (
            f"ALTER TABLE {UPSTREAM_SCHEMA}.t{table} ADD COLUMN c{n} text DEFAULT 'c'"
        )

    def reference(self, table: int) -> str:
        return f"{UPSTREAM_SCHEMA}.t{table}"

    def progress(self) -> str:
        return f"SELECT 'p', lsn::text, NULL::text FROM {SCHEMA}.src"

    def parse_progress(self, rows: list[tuple[str | None, str | None]]) -> int | None:
        lsns = [parse_lsn(a) for a, _ in rows if a is not None]
        return max(lsns) if lsns else None

    def report(self, r: cdc.Review) -> None:
        v, base = r.verdict, r.details
        always(
            not v.duplicate_ledger,
            "postgres source: the ledger export holds at most one row per primary key",
            {**base, "duplicates": v.duplicate_ledger[:10]},
        )
        always(
            not v.unattempted,
            "postgres source: every ledger row visible in Materialize was written by the workload",
            {**base, "unattempted": v.unattempted[:10]},
        )
        always(
            not v.metadata,
            "postgres source: visible ledger rows preserve recorded transaction metadata and exclude aborted transactions",
            {**base, "transactions": v.metadata[:10]},
        )
        always(
            not v.torn,
            "postgres source: a transaction's ledger rows are visible together or not at all",
            {**base, "torn": v.torn[:10]},
        )
        always(
            not v.gaps,
            "postgres source: visible ledger rows form a gap-free prefix of each table's commit order",
            {**base, "beyond_the_prefix": v.gaps[:10]},
        )
        always(
            not v.committed_below_missing,
            "postgres source: transactions committed below the mapped LSN are visible in the ledger",
            {**base, "missing": v.committed_below_missing[:10]},
        )
        always(
            not v.begun_above_visible,
            "postgres source: transactions begun above the mapped LSN are not visible in the ledger",
            {**base, "early": v.begun_above_visible[:10]},
        )
        always(
            not r.regressions,
            "postgres source: the mapped LSN is monotone in mz time",
            {**base, "conflicting": r.regressions[:5]},
        )
        if v.prefix is not None:
            always(
                not v.duplicate_keys,
                "postgres source: an export holds at most one row per upstream primary key, with no terminal upstream op before the last ingestion restart",
                {**base, "duplicates": v.duplicate_keys},
            )
            always(
                not v.mismatches,
                "postgres source: an export equals its upstream table replayed to the mapped LSN, with no terminal upstream op before the last ingestion restart",
                {**base, "prefix": v.prefix, "mismatches": v.mismatches},
            )
        if r.rtr and v.prefix is not None:
            always(
                not r.rtr_missing,
                "postgres source: a real-time recency read observes every upstream transaction committed before it began",
                {**base, "missing": r.rtr_missing[:10]},
            )
        sometimes(
            v.committed_below > len(v.committed_below_missing),
            "postgres source: a ledger check found an acknowledged transaction committed below the mapped LSN",
            {**base, "committed_below": v.committed_below},
        )
        sometimes(
            v.compared_rows > 0 and not v.mismatches,
            "postgres source: an export check matched nonempty exports",
            {**base, "rows": v.compared_rows},
        )
        sometimes(
            r.added_columns and v.prefix is not None and not v.mismatches,
            "postgres source: exports still match after an upstream ADD COLUMN",
            base,
        )

    def report_resolved(self, committed: bool, txn: int) -> None:
        sometimes(
            committed,
            "postgres source: a transaction whose commit response was lost turned out committed",
            {"txn": txn},
        )

    def report_caught_up(self, caught_up: bool, details: dict[str, Any]) -> None:
        always(
            caught_up,
            "postgres source: after faults stop the source reflects every committed upstream transaction",
            details,
        )

    def report_added(self, v: cdc.Verdict, details: dict[str, Any]) -> None:
        if v.prefix is not None:
            # Reported apart from the check below, so that an export that is
            # only ahead does not hide one that is wrong.
            always_or_unreachable(
                v.added_ahead is None,
                "postgres source: an export added while the source ingests is never read ahead of the other exports",
                {**details, "ahead": v.added_ahead, "mismatch": v.added_mismatch},
            )
            always_or_unreachable(
                v.added_mismatch is None or v.added_ahead is not None,
                "postgres source: an export added while the source ingests equals its upstream table at the same prefix as the other exports",
                {**details, "mismatch": v.added_mismatch},
            )
        sometimes(
            v.prefix is not None and bool(v.added_rows),
            "postgres source: a check compared a nonempty export added while the source ingests",
            details,
        )

    def report_types(self, v: cdc_types.Verdict, details: dict[str, Any]) -> None:
        always(
            not v.mismatches,
            "postgres source: every column of a visible types row holds the value its seed determines",
            {**details, "mismatches": v.mismatches},
        )
        always(
            not v.duplicates,
            "postgres source: the types export holds at most one row per upstream primary key",
            {**details, "duplicates": v.duplicates},
        )
        matched = v.rows > 0 and not v.mismatches
        sometimes(
            matched and v.snapshot_rows == cdc_types.SWEEP,
            "postgres source: a types check matched a row for every sample value of every column",
            {**details, "nulls": v.nulls},
        )
        sometimes(
            matched and v.snapshot_rows > 0 and v.workload_rows > 0,
            "postgres source: a types check matched rows that arrived by snapshot and by replication",
            {**details, "snapshot": v.snapshot_rows, "replication": v.workload_rows},
        )
        sometimes(
            matched and v.untouched_rows > 0,
            "postgres source: a types check matched a row whose large columns an update left untouched",
            {**details, "untouched": v.untouched_rows},
        )

    def report_types_caught_up(self, equal: bool, details: dict[str, Any]) -> None:
        always(
            equal,
            "postgres source: after faults stop the types export equals the upstream types table",
            details,
        )


def create(ctx: Context) -> cdc.CdcSource | None:
    url = ctx.config.upstream_postgres_url
    if url is None:
        return None
    return cdc.CdcSource(ctx, Postgres(url, ctx.config.upstream_postgres_mz_host))
