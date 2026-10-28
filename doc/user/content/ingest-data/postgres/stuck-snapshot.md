---
title: "Troubleshooting: Stuck snapshot"
description: "How to diagnose a PostgreSQL source whose snapshot stops making progress in Materialize"
menu:
  main:
    parent: "pg-troubleshooting"
    name: "Stuck snapshot"
    weight: 25
---

This guide helps you diagnose a PostgreSQL source whose
[snapshot](/fundamentals/concepts/snapshotting/) stops making progress.

## What this means

A snapshot is stuck when `snapshot_records_staged` stops increasing and
`snapshot_committed` stays `false`. See [Monitoring the snapshotting
progress](/ingest-data/monitoring-data-ingestion/#monitoring-the-snapshotting-progress)
for the query. The source status stays `running` the whole time, so the status
alone won't tell you anything is wrong.

A snapshot that is slow but still moving is a different problem. See [Is the
upstream database overloaded?](/ingest-data/troubleshooting/#is-the-upstream-database-overloaded)
for that case.

## Common causes

A stuck snapshot is usually waiting on the upstream database. Before it copies
any data, Materialize creates two replication slots.

- The source's replication slot, named `materialize_<id>`. This happens when
  you create the source.
- A temporary snapshot slot, named `mzsnapshot_<id>`. This happens every time
  the source takes a snapshot, including when you add a table to an existing
  source.

Common reasons the snapshot waits are listed below.

- **Long-running write transactions**: PostgreSQL can't finish creating a
  logical replication slot until in-progress transactions that have written
  data finish. A single idle session with an open transaction can block slot
  creation indefinitely.
- **Orphaned prepared transactions**: A prepared transaction (created with
  `PREPARE TRANSACTION`) that was never committed or rolled back blocks slot
  creation the same way.
- **Conflicting table locks**: An `ACCESS EXCLUSIVE` lock on a published table
  blocks Materialize from reading the publication's metadata and the table's
  data. `ALTER TABLE`, `TRUNCATE`, `VACUUM FULL`, and `LOCK TABLE` all take
  this lock. While the lock is held, `CREATE SOURCE` and `CREATE TABLE ... FROM
  SOURCE` can hang before the snapshot even starts.

## Diagnosing the issue

Run the queries below against the upstream PostgreSQL database.

### Find the Materialize sessions

Filter `pg_stat_activity` by the `USER` in your PostgreSQL connection. You can
find it with [`SHOW CREATE CONNECTION`](/sql/show-create-connection/).
Replication sessions have `backend_type = 'walsender'`.

```sql
-- Replace <materialize_user> with the user in your PostgreSQL connection
SELECT
  pid,
  backend_type,
  state,
  wait_event_type,
  wait_event,
  pg_blocking_pids(pid) AS blocked_by,
  xact_start,
  query_start,
  left(query, 120) AS query
FROM pg_stat_activity
WHERE usename = '<materialize_user>';
```

Match each session's `query` and wait event against the table below.

| Query | Wait event | Meaning |
|-------|------------|---------|
| `CREATE_REPLICATION_SLOT "materialize_...` or `CREATE_REPLICATION_SLOT "mzsnapshot_...` | `Lock` / `transactionid` | Slot creation is waiting for a transaction to finish. The `blocked_by` column lists the blocking PID. |
| A query on `pg_publication_tables`, `SELECT count(*) ...`, or `COPY (SELECT ...) TO STDOUT` | `Lock` / `relation` | Materialize is waiting on a conflicting table lock. The `blocked_by` column lists the PID holding the lock. |
| `START_REPLICATION SLOT ...` | `WalSenderMain` or `WalSenderWaitForWal` | Normal streaming of changes. This session isn't blocking the snapshot. |

### Find the blocking transactions

The `blocked_by` column from the previous query points at the session holding
things up. To see every transaction that could block slot creation, list the
sessions that have been assigned a transaction ID, oldest first.

```sql
SELECT
  pid,
  usename,
  application_name,
  state,
  backend_xid,
  xact_start,
  now() - xact_start AS xact_age,
  left(query, 120) AS query
FROM pg_stat_activity
WHERE backend_xid IS NOT NULL
ORDER BY xact_start
LIMIT 20;
```

Prepared transactions don't show up in `pg_stat_activity`, so check for them
separately.

```sql
SELECT gid, prepared, owner, database
FROM pg_prepared_xacts;
```

{{< note >}}
`pg_blocking_pids` reports a prepared transaction as PID `0`. If `blocked_by`
contains `0`, look for the culprit in `pg_prepared_xacts`.
{{</ note >}}

Slot creation can wait on several transactions in turn. After a blocking
transaction finishes, run the session query again to see whether
`blocked_by` has moved on to another PID.

## Resolution

Once the blocking transaction or lock goes away, the snapshot continues on its
own. You don't need to change anything in Materialize.

If the blocker is legitimate work, such as a batch job or a schema migration,
the simplest fix is to let it finish.

If the blocker is stale, end it in PostgreSQL.

{{< warning >}}
Canceling or terminating a session rolls back its open transaction. Check with
the owner of the session before you end it.
{{</ warning >}}

```sql
-- Cancel the current query in the blocking session (replace ### with the PID)
SELECT pg_cancel_backend(###);

-- If canceling isn't enough, terminate the session
SELECT pg_terminate_backend(###);
```

An idle session with an open transaction has no query to cancel, so
`pg_terminate_backend` is the one to use there.

To get rid of an orphaned prepared transaction, roll it back using the `gid`
from `pg_prepared_xacts`.

```sql
ROLLBACK PREPARED '<gid>';
```

## Prevention

- **Check for long-running transactions first**: Before you create a source
  or add a table, run the blocking transactions query above. Close out any
  stale transactions so slot creation doesn't have to wait on them.
- **Watch for orphaned prepared transactions**: If your applications use
  `PREPARE TRANSACTION`, make sure every prepared transaction is committed or
  rolled back.
- **Schedule snapshots around maintenance**: Create sources and add tables
  outside of schema migrations and other work that holds long locks. See the
  [ingestion best practices](/ingest-data/#scheduling).
