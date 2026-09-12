---
headless: true
---

The SQL Server source resumes replication from a [log sequence number
(LSN)](https://learn.microsoft.com/en-us/sql/relational-databases/sql-server-transaction-log-architecture-and-management-guide)
that Materialize tracks as it consumes the upstream change data capture (CDC)
change tables. Because LSNs live in the SQL Server transaction log, they survive
routine operational events: after a transient interruption the source stalls,
then resumes from its last committed LSN and catches up automatically. **No
action is required** for the operations in the first section below.

A smaller set of events breaks LSN or CDC-change-table continuity. When this
happens, Materialize cannot guarantee a correct, gap-free view of your data, so
it puts the source (or an individual table) into an error state that requires
**re-creating** the source or table. Re-creating triggers a fresh
[snapshot](/ingest-data/#snapshotting) and rehydration of dependent objects.

### Operations that do not require re-creating the source

The source recovers on its own — it briefly reports a `stalled` status while the
condition persists, then returns to `running` and catches up — for all of the
following:

- Restarting or patching SQL Server (including OS-level restarts).
- Restarting Materialize. The source resumes from its tracked LSN and does
  **not** re-snapshot already-ingested data.
- Transient network interruptions between Materialize and SQL Server.
- Taking the database `OFFLINE` and back `ONLINE`.
- Toggling the database between `SINGLE_USER`/`MULTI_USER` or
  `READ_ONLY`/`READ_WRITE` (for example, during patching).
- Data-file, filegroup, or index maintenance that rewrites data in place.
- [Availability group failover](#always-on-availability-groups), with the
  configuration change described below.

{{< note >}}
Recovery after an interruption depends on the required LSNs still being present
in the SQL Server CDC change tables. If the interruption lasts longer than the
CDC **retention period** (3 days by default) and SQL Server's cleanup job
removes change-table rows past the source's resume point, the source can no
longer recover on its own. See [Change-table retention](#change-table-retention).
{{< /note >}}

{{< warning >}}
If a maintenance script places the database into `SINGLE_USER` mode, note that an
active Materialize source's reconnection attempts can occupy the single available
connection and cause `ALTER DATABASE ... SET MULTI_USER` to fail with error 5064.
Terminate the Materialize session (or use `SET MULTI_USER WITH ROLLBACK
IMMEDIATE` after terminating it) before returning the database to multi-user
mode.
{{< /warning >}}

### Operations that require re-creating the source

The following events put the **entire source** into an error state. In each
case, the remediation is to drop and re-create the source:

```mzsql
DROP SOURCE mz_source CASCADE;

CREATE SOURCE mz_source
  FROM SQL SERVER CONNECTION sql_server_connection;

-- Re-create the tables you were ingesting.
CREATE TABLE table_1 FROM SOURCE mz_source (REFERENCE dbo.table_1);
```

#### Point-in-time restore

Restoring the source database from a backup — including restoring to a different
server for disaster recovery — is detected as a discontinuity. The source fails
with an error of the form:

```
source must be dropped and recreated due to failure: Restore history id changed
from None to Some(<n>)
```

Materialize detects the restore by reading `msdb.dbo.restorehistory`. (This check
does not apply to Azure SQL Database, which does not expose `msdb`.)

#### CDC disabled at the database level

Running `sys.sp_cdc_disable_db` drops all change tables. The source stalls with:

```
invalid SQL Server system setting 'database CDC'. Expected 'true'. Got 'Some(false)'.
```

Re-enable CDC on the database and on each table (`sys.sp_cdc_enable_db`,
`sys.sp_cdc_enable_table`), then re-create the source.

#### Change-table retention

SQL Server's CDC cleanup job removes change-table rows older than the retention
period (3 days by default). If Materialize is disconnected long enough that
cleanup removes rows past the source's resume LSN, the source stalls with:

```
the requested LSN '...' is less than the minimum '...' for `dbo_<table>`
```

To avoid this during a planned outage, keep the outage shorter than the retention
period, or increase retention beforehand with
[`sys.sp_cdc_change_job`](https://learn.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sys-sp-cdc-change-job-transact-sql)
(`@job_type = 'cleanup'`, `@retention`).

### Operations that require re-creating only the affected table

Some events fail a single table while the rest of the source keeps replicating.

- **Incompatible schema change** (for example, dropping an ingested column, or
  dropping a `NOT NULL` constraint) errors that table with `Incompatible schema
  change for table dbo_<table> ...`. See [Handling upstream
  operations](#handling-upstream-operations) for the per-table recovery steps.

- **Disabling CDC on one table** (`sys.sp_cdc_disable_table`) removes that
  table's change-table functions, which stalls the source. You can recover
  without re-creating the whole source by dropping just the affected table in
  Materialize — the remaining tables resume replicating:

  ```mzsql
  DROP TABLE table_1;
  ```

### Always On availability groups

Materialize supports SQL Server configured with Always On availability groups,
including failover between replicas, with one configuration change.

By default, an availability group failover is misdetected as a point-in-time
restore and fails the source with the `Restore history id changed` error
described above. This is a false positive: the LSN stream is continuous across an
availability group failover, but seeding a secondary replica writes rows to
`msdb.dbo.restorehistory`, which the restore-detection check reads as a restore.

To allow the source to survive failover, disable restore-history validation with
the [`sql_server_source_validate_restore_history`](/sql/alter-system-set/) system
parameter:

```mzsql
ALTER SYSTEM SET sql_server_source_validate_restore_history = false;
```

{{< warning >}}
Disabling this check is a trade-off: with it off, Materialize will also **not**
detect a genuine [point-in-time restore](#point-in-time-restore) of the source
database. Only disable it when the source connects to a database that fails over
between availability group replicas.
{{< /warning >}}

With the check disabled, the source no longer fails on failover. Because `msdb`
is per-instance, the CDC capture and cleanup jobs do not move with the
availability group database — after a failover, confirm that CDC is healthy on
the new primary (the capture and cleanup jobs exist, SQL Server Agent is running,
and the change tables are advancing) so that replication continues. Adding the
jobs on a replica that lacks them is done with
[`sys.sp_cdc_add_job`](https://learn.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sys-sp-cdc-add-job-transact-sql).
