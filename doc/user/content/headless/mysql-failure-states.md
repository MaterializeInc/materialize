---
headless: true
---

### Operations that do not require re-creating the source

Materialize tracks its position in the upstream binary log as a set of [global
transaction identifiers
(GTIDs)](https://dev.mysql.com/doc/refman/8.0/en/replication-gtids.html), which
it persists alongside the ingested data. A GTID identifies a transaction across
the whole replication topology, not a byte offset in a particular binlog file,
so it survives routine operational events: after an interruption the source
reconnects, asks the server for the transactions after its last committed GTID,
and catches up.

The source recovers on its own. It briefly reports a `stalled` status while the
condition persists, then returns to `running` and catches up for all of the
following:

- Restarting or patching MySQL (including OS-level restarts).
- Restarting Materialize. The source resumes from its tracked GTID set and does
  **not** re-snapshot already-ingested data.
- Transient network interruptions between Materialize and MySQL.
- The upstream server running out of disk space, until space is reclaimed.
- A long-running upstream transaction blocking the initial snapshot. The source
  stalls until that transaction commits or rolls back.
- [Failing over to a replica](#failovers), with the checks described below.

{{< note >}}
Recovery after an interruption depends on the binlog files that contain the
source's resume point still existing on the upstream server. If the
interruption outlasts the binlog retention window, the source can no longer
recover on its own. See [Binlog files removed before the resume
point](#binlog-files-removed-before-the-resume-point).
{{< /note >}}

### Operations that require re-creating the source

A smaller set of events breaks GTID continuity or makes the binlog stream
unreadable. Materialize cannot then guarantee a correct, gap-free view of your
data, so it puts the **entire source** into an error state that requires
**re-creating** the source. Upstream changes to an individual table's schema
are handled separately, and do not error the entire source.

In each case, the remediation is to drop the source and create it again with
the statements you originally used, which triggers a fresh
[snapshot](/ingest-data/#snapshotting):

```mzsql
DROP SOURCE mz_source CASCADE;
```

{{< warning >}}
`CASCADE` drops **every object that depends on the source**, including views,
materialized views, indexes, and sinks. Take stock of them before you drop the
source, because you have to re-create them yourself.
{{< /warning >}}

Once the source is back, it is healthy when `status` is `running` and `error`
is `NULL`:

```mzsql
SELECT status, error FROM mz_internal.mz_source_statuses WHERE name = 'mz_source';
```

#### Binlog files removed before the resume point

MySQL expires binlog files on a retention schedule, and `PURGE BINARY LOGS`
removes them on demand. If Materialize is disconnected or lagging long enough
that the files holding its resume point are removed, the source fails with one
of:

```nofmt
mysql server does not have the binlog available at the requested gtid set
```

```nofmt
mysql server binlog frontier at <frontier> is beyond required frontier <frontier>
```

To avoid this, keep planned outages shorter than the retention window and
monitor source lag against it. For how retention is configured, including the
service-specific parameters that override it, see [Binlog
retention](/sql/create-source/mysql-v2/#binlog-retention).

#### Resetting the binary log

`RESET BINARY LOGS AND GTIDS` (`RESET MASTER` before MySQL 8.4) discards the
binlog files and the server's GTID history, so the source's resume point no
longer exists. The source fails with:

```nofmt
mysql server does not have the binlog available at the requested gtid set
```

or, if the server then reissues GTIDs the source has already seen, with an
[out-of-order GTID error](#out-of-order-gtids).

#### Changing a required replication setting

Materialize re-validates the upstream replication settings each time it
(re-)establishes the replication stream. If one no longer holds its required
value, the source fails with:

```nofmt
mysql server configuration: invalid mysql system setting '<setting>'. Expected '<expected>'. Got '<actual>'.
```

The validated settings are `log_bin`, `binlog_format`, `binlog_row_image`,
`gtid_mode`, `enforce_gtid_consistency`, `gtid_next`, and, when
`replica_parallel_workers` is greater than `1`,
`replica_preserve_commit_order` (`slave_preserve_commit_order` on servers
older than MySQL 8.0). For the required values, see [Change data
capture](/sql/create-source/mysql-v2/#change-data-capture). Restore the setting
upstream, then re-create the source.

#### Restoring the upstream database

Materialize has no dedicated check for a restore of the upstream database.
Depending on how the restore handles GTIDs, the source either fails with one of
the errors above or with:

```nofmt
received a gtid set from the server that violates our requirements: <detail>
```

or, if the restore reuses GTIDs the source has already ingested, it keeps
replicating against a history that no longer matches its state. Materialize
cannot detect that case, so re-create the source after any restore of the
upstream database, including a disaster-recovery restore onto a new server,
rather than relying on an error.

#### Out-of-order GTIDs

If Materialize observes GTIDs in an order it cannot reconcile, the source fails
with:

```nofmt
received out of order gtids for source <source_id> at transaction-id <transaction_id>
```

This is most common when Materialize replicates from a MySQL replica that
applies transactions with multiple threads. See [Troubleshooting: Received out
of order GTIDs](/ingest-data/mysql/received-out-of-order-gtids/) for the
diagnosis steps and the upstream settings that make it less likely.

### Operations that require re-creating only the affected tables

#### Lowering `binlog_row_metadata`

A table that Materialize began ingesting while `binlog_row_metadata` was `FULL`
can no longer be decoded once the setting is lowered, and enters an error state
with:

```nofmt
unable to decode: Table <table> was created with binlog_row_metadata=FULL but
binlog_row_metadata has since been set to a different value, meaning we cannot
reliably decode the columns
```

Restoring `binlog_row_metadata=FULL` does not clear the error. Set it back to
`FULL` and re-create the affected tables. Tables that were created while the
setting was lower keep replicating.

{{< note >}}
`binlog_row_metadata=FULL` is required to use the [current `CREATE SOURCE`
syntax](/sql/create-source/mysql-v2/#change-data-capture), so for a source
created with that syntax this affects every table in the source. A common cause
is a MySQL restart discarding a `SET GLOBAL` that was never persisted.
{{< /note >}}

### Failovers

Because Materialize replicates by GTID rather than by binlog file and position,
it can follow a failover to a replica that was replicating from the failed
server with GTIDs enabled. Point the connection at the new primary with [`ALTER
CONNECTION`](/sql/alter-connection/), or at an endpoint that always resolves to
the current primary.

On the new primary, confirm that:

- The binlog files covering the source's resume point are present. A replica
  configured with a shorter retention than the old primary can leave the source
  with no way to resume. See [Binlog files removed before the resume
  point](#binlog-files-removed-before-the-resume-point).
- The [required replication settings](#changing-a-required-replication-setting)
  hold, including `replica_preserve_commit_order` while the server is still
  applying replication from another server. Multi-threaded apply is the main
  source of [out-of-order GTIDs](#out-of-order-gtids); preserving commit order
  reduces but does not eliminate the risk, so keep
  `replica_parallel_workers` at `0` or `1` where you can.
