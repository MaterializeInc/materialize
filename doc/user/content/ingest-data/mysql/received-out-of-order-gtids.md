---
title: "Troubleshooting: Received out of order GTIDs"
description: "How to troubleshoot and resolve the received out of order GTIDs error with MySQL sources in Materialize"
menu:
  main:
    parent: "mysql-troubleshooting"
    name: "Received out of order GTIDs"
    weight: 10
---

This guide helps you troubleshoot and resolve the "received out of order GTIDs"
error that can occur with MySQL sources in Materialize.

## What this error means

When you see an error like:

```nofmt
mysql: Source error: source must be dropped and recreated due to failure: received out of order gtids for source 16b115c3-7f51-11ec-83f8-0274e24fd16b at transaction-id 5747289
```

Materialize is telling you that it observed GTID events from MySQL in an order
that it cannot safely reconcile. At that point, Materialize treats the source as
potentially corrupted: continuing ingestion could produce incorrect results, so
the only safe action is to stop and rebuild from a clean snapshot.

## Common causes

- **Multi-threaded replication without commit order preservation**: If the MySQL
  instance Materialize is replicating from applies transactions in parallel,
  transactions can be committed out of order unless commit ordering is preserved.
  This can produce GTID sequences that Materialize cannot safely ingest.
- **Incomplete GTID enablement across a replication chain**: If GTID mode was
  enabled part-way through a system's lifetime, or enabled on some nodes but not
  others, replicas can end up with a GTID/binlog history that violates the
  assumptions required for GTID-based CDC.
- **Complex replication topologies (chained replication, filtering)**: Chained
  replication and replication filtering (for example, selective database
  replication) can change what gets written into replica binlogs. In combination
  with parallel replication, this can increase the risk of GTID ordering becoming
  incompatible with GTID-based CDC consumers.
- **Topology changes and failovers**: Failovers, topology changes, or
  configuration changes can alter replication behavior and surface issues that
  were latent before.

## Diagnosing the issue

### Confirm which MySQL server Materialize is connected to

Determine whether Materialize is connected to:

- The primary (writer)
- A read replica
- A replica-of-a-replica (chained replication)

Replica settings can differ from the primary, and those differences often matter
for GTID ordering.

### Check replication apply settings on the connected server

On the MySQL server Materialize connects to, confirm the replication settings
match what Materialize expects:

```sql
SHOW VARIABLES LIKE 'replica_preserve_commit_order';
SHOW VARIABLES LIKE 'replica_parallel_workers';
```

- `replica_preserve_commit_order` should be `ON`
- `replica_parallel_workers` should be `<= 1` (or `0` to disable parallel apply)

If `replica_parallel_workers > 1`, MySQL can still externalize transactions out
of order ("gaps") even when commit-order preservation is enabled.

### Verify GTID configuration is consistent end-to-end

If you recently enabled GTID, confirm that GTID mode and GTID consistency
settings are correctly configured across the full replication chain (primary and
any intermediate replicas), and that you followed the complete procedure for
enabling GTIDs in an existing topology.

```sql
SHOW VARIABLES LIKE 'gtid_mode';
SHOW VARIABLES LIKE 'enforce_gtid_consistency';
```

Both should return `ON` on every node in the replication chain.

### Determine whether the error is recurring

- If it happens repeatedly, focus on ongoing replication behavior/configuration.
- If it started right after a change window, focus on what changed upstream in
  that period (failover, configuration change, new replica, GTID enablement
  work).

## Resolution

### Immediate fix: Drop and recreate the source

{{< warning >}}
This will cause Materialize to take a new snapshot of your MySQL tables, which
may take time and temporarily increase load on your MySQL server.
{{</ warning >}}

Once Materialize reports this error, the data is considered potentially
corrupted. You must **drop and recreate the source**. Dropping the source will
also drop any dependent objects; be prepared to recreate them as part of the
recovery process.

### Long-term fixes

**1. Configure replica settings for GTID-based CDC**

If using MySQL replicas with parallel apply, ensure commit order is preserved:

```sql
SET GLOBAL replica_parallel_workers = 1;
SET GLOBAL replica_preserve_commit_order = ON;
```

`SET GLOBAL` changes the runtime value but does not persist across MySQL
restarts. To make the change permanent, also update your MySQL configuration
file (`my.cnf` or `my.ini`):

```ini
[mysqld]
replica_parallel_workers = 1
replica_preserve_commit_order = ON
```

If the error persists, consider disabling parallel apply entirely by setting
`replica_parallel_workers = 0`.

**2. Ensure consistent GTID enablement**

If enabling GTIDs on an existing replication chain, follow the full end-to-end
procedure and avoid partially-enabled states.

**3. Validate complex replication topologies**

If using chained replication and/or filtering, validate your replication setup
with GTID-based consumers (including Materialize) before relying on it in
production.

## Prevention

**Best practices to avoid this error:**

- If using MySQL replicas with parallel apply, ensure commit order is preserved
  and set `replica_parallel_workers <= 1` (or disable parallel apply).
- If enabling GTIDs on an existing replication chain, follow the full end-to-end
  procedure and avoid partially-enabled states.
- If using chained replication and/or filtering, validate your replication setup
  with GTID-based consumers (including Materialize) before relying on it in
  production.
- After upstream failovers or topology changes, monitor closely for source
  errors and address them immediately.

## Additional technical details

### Multi-threaded replicas

This error is most commonly seen with multithreaded MySQL replicas upstream from
Materialize. A multithreaded replica is a MySQL instance with parallel
replication apply enabled (`replica_parallel_workers > 0`). When
`replica_parallel_workers = N`, MySQL may dedicate `N` threads per replication
channel in multi-source replication.

### MySQL "gaps"

Even with `replica_preserve_commit_order=ON` and
`replica_parallel_type=LOGICAL_CLOCK`, MySQL can still present "gaps" in the
externalized transaction set. MySQL defines gaps as:

> A gap in the externalized transaction set appears when, given an ordered
> sequence of transactions, a transaction that is later in the sequence is
> applied before some other transaction that is prior in the sequence.

### Recommended settings

If this error is happening and Materialize is reading from a MySQL replica,
consider (at minimum):

- `replica_parallel_workers = 1`
- `replica_preserve_commit_order = ON`

If the error persists, consider disabling parallel apply entirely (for example,
setting `replica_parallel_workers = 0`), and validate that the replication
behavior matches the expectations of GTID-based CDC consumers.
