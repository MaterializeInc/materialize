---
title: "Troubleshooting: Replication slot is active"
description: "How to troubleshoot and resolve the replication slot is active error with PostgreSQL sources in Materialize"
menu:
  main:
    parent: "pg-troubleshooting"
    name: "Replication slot is active"
    weight: 15
---

This guide helps you troubleshoot and resolve the "replication slot... is active"
error that can occur with PostgreSQL sources in Materialize.

## What this error means

When you see an error like:

```nofmt
postgres: ERROR replication slot "materialize_1002f37c6eeb4c28b052fa3805d46baa" is active for PID 610
```

This means that PostgreSQL has detected an attempt to use a replication slot that
is already in use by another active connection. Each replication slot can only
have one active consumer at a time, and this error occurs when Materialize tries
to connect to a slot that PostgreSQL considers already active. This error is
generally transient and indicates a race condition during connection management
between Materialize and PostgreSQL.

## Common causes

- **PostgreSQL or Materialize maintenance**: During maintenance windows,
  Materialize clusters or PostgreSQL instances may restart, causing active
  replication connections to be temporarily interrupted. When the cluster comes
  back online, it attempts to reconnect to the replication slot before PostgreSQL
  has fully released the previous connection.
- **Cluster replica changes**: Adding replicas to your Materialize source cluster
  can cause the ingestion process to restart from a different replica, triggering
  this error as multiple replicas attempt to use the same replication slot.
- **Source restarts or failover**: When a source cluster restarts or fails over
  to another replica, the new replica may attempt to connect to the replication
  slot before PostgreSQL has released the previous connection.
- **Network interruptions**: Brief network disconnections can leave PostgreSQL
  thinking a connection is still active when Materialize has already moved to a
  new connection attempt.
- **PostgreSQL connection cleanup delays**: PostgreSQL may not immediately
  release replication slots when connections are terminated, especially if the
  termination was not graceful.
- **Multiple environments or sources**: Accidentally configuring multiple
  Materialize sources to use the same replication slot.

## Diagnosing the issue

### Check replication slot status in PostgreSQL

Connect to your PostgreSQL database and run:

```sql
-- Replace <slot_name> with the replication slot name shown in the error message
SELECT
  slot_name,
  active,
  active_pid,
  restart_lsn,
  confirmed_flush_lsn
FROM pg_replication_slots
WHERE slot_name LIKE '<slot_name>';
```

Look for:

- **active = true**: The slot is currently in use
- **active_pid**: The PostgreSQL backend process ID that is using the slot
- Multiple slots with similar names that might indicate configuration issues

### Check for active connections

Check which connections are using the replication slot:

```sql
-- Replace <slot_name> with the replication slot name shown in the error message
SELECT
  pid,
  usename,
  application_name,
  client_addr,
  state,
  backend_start
FROM pg_stat_activity
WHERE pid IN (
  SELECT active_pid
  FROM pg_replication_slots
  WHERE slot_name LIKE '<slot_name>'
);
```

## Resolution

### Immediate fix: Wait for automatic recovery

In most cases, Materialize will automatically reconnect and recover from this
error. The error is typically transient and resolves itself within a few minutes
as PostgreSQL releases the previous connection.

### If automatic recovery fails: Terminate stale connections

If the error persists and you've verified that there are no legitimate active
connections, you can terminate the stale connection in PostgreSQL:

{{< warning >}}
Only terminate connections if you're certain they are stale. Terminating an
active replication connection will interrupt data ingestion.
{{</ warning >}}

```sql
-- First, identify the PID from the error message or from pg_replication_slots
-- Replace <slot_name> with the replication slot name shown in the error message
SELECT
  slot_name,
  active_pid
FROM pg_replication_slots
WHERE slot_name LIKE '<slot_name>' AND active = true;

-- Terminate the connection (replace ### with the actual PID)
SELECT pg_terminate_backend(###);
```

After terminating the connection, Materialize should be able to reconnect to
the replication slot.

## Prevention

**Best practices to avoid this error:**

- **Avoid frequent replica changes**: Minimize adding or removing replicas from
  source clusters during active replication. Plan cluster sizing changes during
  maintenance windows when possible.
- **Ensure stable network connectivity**: Maintain reliable network connections
  between Materialize and PostgreSQL to prevent connection interruptions.
- **Monitor source health**: Regularly check source status to detect and address
  issues early:

```mzsql
SELECT *
FROM mz_internal.mz_source_statuses;
```

- **Configure connection timeouts appropriately**: Ensure PostgreSQL connection
  timeout settings allow for proper cleanup of disconnected sessions.
- **Use unique replication slots**: Verify that each Materialize source uses a
  unique replication slot and avoid reusing slot names across different sources
  or environments.
- **Plan for graceful restarts**: When performing maintenance that requires
  source restarts, allow sufficient time for PostgreSQL to release connections
  before restarting.
