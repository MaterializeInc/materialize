# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""SQL Server source, a `cdc.Dialect`.

Positions are LSNs, ten big-endian bytes read as one integer. A commit's own
LSN is known only once the capture job has copied it into the ledger's
change table, so `positions` looks it up later. Real-time recency asks for
`sys.fn_cdc_get_max_lsn()`, the last captured commit, so it covers only
captured commits.
"""

from __future__ import annotations

from typing import Any
from urllib.parse import unquote, urlparse

from antithesis.assertions import (  # pyright: ignore[reportMissingModuleSource]
    always,
    always_or_unreachable,
    sometimes,
)

from materialize.antithesis_driver import cdc, cdc_types
from materialize.antithesis_driver.context import Context

DATABASE = "upstream"
SCHEMA = "sqlsrc"
TICK_JOB = "driver_tick"
APP = "driver-sql-server"


def lsn(raw: bytes | None) -> int:
    return int.from_bytes(raw, "big") if raw else 0


class SqlServer:
    schema = SCHEMA
    ledger = "dbo.ledger"
    rtr_needs_capture = True
    types = cdc_types.SQL_SERVER
    types_table = "dbo.types"

    def __init__(self, url: str, mz_host: str | None) -> None:
        import pymssql  # pyright: ignore[reportMissingImports]

        self.errors: tuple[type[BaseException], ...] = (pymssql.Error, OSError)
        u = urlparse(url)
        self.host = u.hostname or "localhost"
        self.port = u.port or 1433
        self.user = unquote(u.username or "SA")
        self.password = unquote(u.password or "")
        mz = urlparse(f"//{mz_host or u.netloc.split('@')[-1]}")
        self.mz_host, self.mz_port = mz.hostname, mz.port or 1433

    def connect(self, database: str = DATABASE) -> Any:
        import pymssql  # pyright: ignore[reportMissingImports]

        return pymssql.connect(
            server=self.host,
            port=str(self.port),
            user=self.user,
            password=self.password,
            database=database,
            autocommit=True,
            login_timeout=10,
            timeout=30,
            appname=APP,
        )

    def setup_upstream(self) -> None:
        with self.connect("master") as conn:
            cur = conn.cursor()
            cur.execute(
                f"IF DB_ID('{DATABASE}') IS NOT NULL BEGIN"
                f" ALTER DATABASE {DATABASE} SET SINGLE_USER WITH ROLLBACK IMMEDIATE;"
                f" DROP DATABASE {DATABASE}; END"
            )
            # A UTF-8 collation, so `varchar` holds any value the driver draws.
            cur.execute(
                f"CREATE DATABASE {DATABASE} COLLATE Latin1_General_100_CI_AI_SC_UTF8"
            )
            cur.execute(f"ALTER DATABASE {DATABASE} SET ALLOW_SNAPSHOT_ISOLATION ON")
            cur.execute(f"USE {DATABASE}")
            cur.execute("EXEC sys.sp_cdc_enable_db")
            tables = [f"t{j}" for j in range(cdc.TABLES)]
            for t in tables:
                cur.execute(f"CREATE TABLE dbo.{t} (k int PRIMARY KEY, v varchar(max))")
            cur.execute(
                "CREATE TABLE dbo.ledger"
                " (txn bigint NOT NULL, tbl int NOT NULL, PRIMARY KEY (txn, tbl))"
            )
            cur.execute("CREATE TABLE dbo.tick (t datetime2)")
            cur.execute("INSERT INTO dbo.tick VALUES (SYSUTCDATETIME())")
            cur.execute(cdc_types.create_table(self.types_table, self.types))
            for statement, params in cdc_types.sweep(self.types_table, self.types):
                cur.execute(statement, params)
            for t in [*tables, "ledger", "tick", "types"]:
                cur.execute(
                    "EXEC sys.sp_cdc_enable_table @source_schema = 'dbo',"
                    f" @source_name = '{t}', @role_name = NULL,"
                    " @supports_net_changes = 0"
                )
            # The max LSN advances only with captured changes. A job changing
            # a captured table every second keeps it, and so the source's
            # progress, moving while the driver writes nothing.
            cur.execute(
                "CREATE OR ALTER PROCEDURE dbo.tick_forever AS BEGIN SET NOCOUNT ON;"
                " WHILE 1 = 1 BEGIN UPDATE dbo.tick SET t = SYSUTCDATETIME();"
                " WAITFOR DELAY '00:00:01'; END END"
            )
            cur.execute("USE msdb")
            cur.execute(
                f"IF EXISTS (SELECT 1 FROM msdb.dbo.sysjobs WHERE name = N'{TICK_JOB}')"
                f" EXEC sp_delete_job @job_name = N'{TICK_JOB}'"
            )
            cur.execute(f"EXEC sp_add_job @job_name = N'{TICK_JOB}', @enabled = 1")
            cur.execute(
                f"EXEC sp_add_jobstep @job_name = N'{TICK_JOB}', @step_name = N'run',"
                f" @subsystem = N'TSQL', @database_name = N'{DATABASE}',"
                " @command = N'EXEC dbo.tick_forever;'"
            )
            # Also when the Agent starts, so the job outlives a server restart.
            cur.execute(
                f"EXEC sp_add_jobschedule @job_name = N'{TICK_JOB}',"
                " @name = N'agent start', @freq_type = 64"
            )
            cur.execute(f"EXEC sp_add_jobserver @job_name = N'{TICK_JOB}'")
            cur.execute(f"EXEC sp_start_job N'{TICK_JOB}'")

    def setup_statements(self) -> list[str]:
        password = self.password.replace("'", "''")
        return [
            f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}",
            f"CREATE SECRET IF NOT EXISTS {SCHEMA}.password AS '{password}'",
            f"CREATE CONNECTION IF NOT EXISTS {SCHEMA}.conn TO SQL SERVER"
            f" (HOST '{self.mz_host}', PORT {self.mz_port}, DATABASE {DATABASE},"
            f" USER '{self.user}', PASSWORD SECRET {SCHEMA}.password)",
            f"CREATE SOURCE IF NOT EXISTS {SCHEMA}.src IN CLUSTER {cdc.CLUSTER}"
            f" FROM SQL SERVER CONNECTION {SCHEMA}.conn",
            f"CREATE TABLE IF NOT EXISTS {SCHEMA}.ledger FROM SOURCE {SCHEMA}.src"
            f" (REFERENCE {self.ledger})",
            *(
                f"CREATE TABLE IF NOT EXISTS {SCHEMA}.t{j} FROM SOURCE {SCHEMA}.src"
                f" (REFERENCE dbo.t{j})"
                for j in range(cdc.TABLES)
            ),
            f"CREATE TABLE IF NOT EXISTS {SCHEMA}.types FROM SOURCE {SCHEMA}.src"
            f" (REFERENCE {self.types_table}){cdc_types.text_columns(self.types)}",
        ]

    def session(self, cur: Any) -> Any:
        cur.execute(
            "SELECT @@SPID, CONVERT(varchar(30), login_time, 126)"
            " FROM sys.dm_exec_sessions WHERE session_id = @@SPID"
        )
        spid, login = cur.fetchone()
        return int(spid), login

    def end_session(self, cur: Any, session: Any) -> bool:
        # Session ids are reused at once, by Materialize's own connections
        # too. Only a session with this login time and the driver's app name
        # is killed, and the check and the KILL run in one batch.
        cur.execute(
            "DECLARE @k nvarchar(32) = (SELECT 'KILL ' + CAST(session_id AS nvarchar(10))"
            " FROM sys.dm_exec_sessions WHERE session_id = %s"
            " AND CONVERT(varchar(30), login_time, 126) = %s AND program_name = %s);"
            " IF @k IS NOT NULL EXEC (@k);"
            " SELECT CASE WHEN @k IS NULL THEN 1 ELSE 0 END",
            (*session, APP),
        )
        return cur.fetchone()[0] == 1

    def begin(self, cur: Any) -> int:
        # The last captured commit precedes this transaction's.
        cur.execute("SELECT sys.fn_cdc_get_max_lsn()")
        before = lsn(cur.fetchone()[0])
        cur.execute("BEGIN TRANSACTION")
        return before

    def statement(self, plan: cdc.Plan) -> tuple[str, tuple[Any, ...]]:
        t = f"dbo.t{plan.table}"
        if plan.kind == "upsert":
            return (
                f"MERGE {t} WITH (HOLDLOCK) AS d"
                " USING (SELECT %s AS k, CAST(%s AS varchar(max)) AS v) AS s"
                " ON d.k = s.k WHEN MATCHED THEN UPDATE SET v = s.v"
                " WHEN NOT MATCHED THEN INSERT (k, v) VALUES (s.k, s.v);",
                (plan.key, plan.value),
            )
        if plan.kind == "update":
            return f"UPDATE {t} SET v = %s WHERE k = %s", (plan.value, plan.key)
        if plan.kind == "delete":
            return f"DELETE FROM {t} WHERE k = %s", (plan.key,)
        assert plan.kind == "delete_range"
        return f"DELETE FROM {t} WHERE k BETWEEN %s AND %s", (plan.key, plan.high)

    def commit(self, cur: Any) -> None:
        cur.execute("COMMIT TRANSACTION")

    def position_after(self, cur: Any) -> int | None:
        return None

    def positions(self, cur: Any, txns: list[int]) -> dict[int, int]:
        cur.execute(
            "SELECT txn, __$start_lsn FROM cdc.dbo_ledger_CT"
            f" WHERE __$operation = 2 AND txn IN ({', '.join(['%s'] * len(txns))})",
            tuple(txns),
        )
        return {int(txn): lsn(raw) for txn, raw in cur.fetchall()}

    def add_column(self, table: int, n: int) -> str:
        return f"ALTER TABLE dbo.t{table} ADD c{n} varchar(8) DEFAULT 'c'"

    def reference(self, table: int) -> str:
        return f"dbo.t{table}"

    def progress(self) -> str:
        return f"SELECT 'p', encode(lsn, 'hex'), NULL::text FROM {SCHEMA}.src"

    def parse_progress(self, rows: list[tuple[str | None, str | None]]) -> int | None:
        lsns = [int(a, 16) for a, _ in rows if a]
        return max(lsns) if lsns else None

    def report(self, r: cdc.Review) -> None:
        v, base = r.verdict, r.details
        always(
            not v.duplicate_ledger,
            "sql server source: the ledger export holds at most one row per primary key",
            {**base, "duplicates": v.duplicate_ledger[:10]},
        )
        always(
            not v.unattempted,
            "sql server source: every ledger row visible in Materialize was written by the workload",
            {**base, "unattempted": v.unattempted[:10]},
        )
        always(
            not v.metadata,
            "sql server source: visible ledger rows preserve recorded transaction metadata and exclude aborted transactions",
            {**base, "transactions": v.metadata[:10]},
        )
        always(
            not v.torn,
            "sql server source: a transaction's ledger rows are visible together or not at all",
            {**base, "torn": v.torn[:10]},
        )
        always(
            not v.gaps,
            "sql server source: visible ledger rows form a gap-free prefix of the commit order",
            {**base, "beyond_the_prefix": v.gaps[:10]},
        )
        always(
            not v.committed_below_missing,
            "sql server source: transactions committed below the mapped LSN are visible in the ledger",
            {**base, "missing": v.committed_below_missing[:10]},
        )
        always(
            not v.begun_above_visible,
            "sql server source: transactions begun above the mapped LSN are not visible in the ledger",
            {**base, "early": v.begun_above_visible[:10]},
        )
        always(
            not r.regressions,
            "sql server source: the mapped LSN is monotone in mz time",
            {**base, "conflicting": r.regressions[:5]},
        )
        if v.prefix is not None:
            always(
                not v.duplicate_keys,
                "sql server source: an export holds at most one row per upstream primary key",
                {**base, "duplicates": v.duplicate_keys},
            )
            always(
                not v.mismatches,
                "sql server source: an export equals its upstream table replayed to the visible prefix",
                {**base, "prefix": v.prefix, "mismatches": v.mismatches},
            )
        if r.rtr and v.prefix is not None:
            always(
                not r.rtr_missing,
                "sql server source: a real-time recency read observes every transaction captured before it began",
                {**base, "missing": r.rtr_missing[:10]},
            )
        sometimes(
            v.committed_below > len(v.committed_below_missing),
            "sql server source: a ledger check found a captured transaction committed below the mapped LSN",
            {**base, "committed_below": v.committed_below},
        )
        sometimes(
            v.compared_rows > 0 and not v.mismatches,
            "sql server source: an export check matched nonempty exports",
            {**base, "rows": v.compared_rows},
        )
        sometimes(
            r.added_columns and v.prefix is not None and not v.mismatches,
            "sql server source: exports still match after an upstream ADD COLUMN",
            base,
        )

    def report_resolved(self, committed: bool, txn: int) -> None:
        sometimes(
            committed,
            "sql server source: a transaction whose commit response was lost turned out committed",
            {"txn": txn},
        )

    def report_caught_up(self, caught_up: bool, details: dict[str, Any]) -> None:
        always(
            caught_up,
            "sql server source: after faults stop the source reflects every committed upstream transaction",
            details,
        )

    def report_added(self, v: cdc.Verdict, details: dict[str, Any]) -> None:
        if v.prefix is not None:
            # Reported apart from the check below, so that an export that is
            # only ahead does not hide one that is wrong.
            always_or_unreachable(
                v.added_ahead is None,
                "sql server source: an export added while the source ingests is never read ahead of the other exports",
                {**details, "ahead": v.added_ahead, "mismatch": v.added_mismatch},
            )
            always_or_unreachable(
                v.added_mismatch is None or v.added_ahead is not None,
                "sql server source: an export added while the source ingests equals its upstream table at the same prefix as the other exports",
                {**details, "mismatch": v.added_mismatch},
            )
        sometimes(
            v.prefix is not None and bool(v.added_rows),
            "sql server source: a check compared a nonempty export added while the source ingests",
            details,
        )

    def report_types(self, v: cdc_types.Verdict, details: dict[str, Any]) -> None:
        always(
            not v.mismatches,
            "sql server source: every column of a visible types row holds the value its seed determines",
            {**details, "mismatches": v.mismatches},
        )
        always(
            not v.duplicates,
            "sql server source: the types export holds at most one row per upstream primary key",
            {**details, "duplicates": v.duplicates},
        )
        matched = v.rows > 0 and not v.mismatches
        sometimes(
            matched and v.snapshot_rows == cdc_types.SWEEP,
            "sql server source: a types check matched a row for every sample value of every column",
            {**details, "nulls": v.nulls},
        )
        sometimes(
            matched and v.snapshot_rows > 0 and v.workload_rows > 0,
            "sql server source: a types check matched rows that arrived by snapshot and by replication",
            {**details, "snapshot": v.snapshot_rows, "replication": v.workload_rows},
        )
        sometimes(
            matched and v.untouched_rows > 0,
            "sql server source: a types check matched a row whose large columns an update left untouched",
            {**details, "untouched": v.untouched_rows},
        )

    def report_types_caught_up(self, equal: bool, details: dict[str, Any]) -> None:
        always(
            equal,
            "sql server source: after faults stop the types export equals the upstream types table",
            details,
        )


def create(ctx: Context) -> cdc.CdcSource | None:
    url = ctx.config.upstream_sql_server_url
    if url is None:
        return None
    return cdc.CdcSource(ctx, SqlServer(url, ctx.config.upstream_sql_server_mz_host))
