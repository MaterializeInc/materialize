# Troubleshooting: Slot overcompacted

How to troubleshoot and resolve the slot overcompacted error with PostgreSQL sources in Materialize



This guide helps you troubleshoot and resolve the "slot overcompacted" error that
can occur with PostgreSQL sources in Materialize.

## What this error means

When you see an error like:

```nofmt
postgres: slot overcompacted. Requested LSN 181146050392 but only LSNs >= 332129862840 are available
```

This means Materialize tried to read from a PostgreSQL replication slot at a
specific Log Sequence Number (LSN), but that data has already been removed from
PostgreSQL's Write-Ahead Log (WAL). The WAL was "compacted" or cleaned up before
Materialize could read the data it needed.

## Common causes

- **WAL retention limits**: PostgreSQL has a setting called
  `max_slot_wal_keep_size` that limits how much WAL data is kept for replication
  slots. If this value is too small, PostgreSQL may delete WAL data that
  Materialize still needs.
- **Long-running snapshot operations**: If your source is taking a long time to
  complete its initial snapshot (e.g., for very large tables), the upstream
  PostgreSQL database may clean up WAL data before Materialize finishes.
- **Paused or slow replication**: If your Materialize cluster is paused,
  undersized, or experiencing performance issues, the replication slot may not
  advance quickly enough, causing PostgreSQL to reclaim WAL space.
- **Provider-specific WAL policies**: Some managed PostgreSQL providers (such
  as Neon) may have aggressive WAL cleanup policies that can trigger this error
  more frequently.

## Diagnosing the issue

### Check replication slot status in PostgreSQL

Connect to your PostgreSQL database and run:

```sql
SELECT
  slot_name,
  active,
  restart_lsn,
  confirmed_flush_lsn,
  pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS replication_lag
FROM pg_replication_slots
WHERE slot_name LIKE 'materialize%';
```

Look for:

- **Large replication lag** - Indicates Materialize is falling behind
- **Inactive slots** - May indicate connection issues

### Check PostgreSQL WAL settings

Check your `max_slot_wal_keep_size` setting:

```sql
SHOW max_slot_wal_keep_size;
```

If this is set too low (or to `-1` which means unlimited but may be overridden
by provider policies), you may experience this error.

### Check for long-running transactions

Long-running transactions can prevent WAL cleanup:

```sql
SELECT
  pid,
  age(clock_timestamp(), xact_start) AS transaction_age,
  state,
  query
FROM pg_stat_activity
WHERE xact_start IS NOT NULL
ORDER BY age DESC;
```

## Resolution

### Immediate fix: Recreate the source

> **Warning:** This will cause Materialize to take a new snapshot, which may take
> time and temporarily increase load on your PostgreSQL database.
>


Once a slot has been overcompacted, the data is permanently lost from the WAL.
You must **drop and recreate the source**. Dropping the source will also drop
any dependent objects; be prepared to recreate them as part of the recovery process.

### Long-term fixes

**1. Increase WAL retention**

Increase `max_slot_wal_keep_size` in your PostgreSQL configuration:

```sql
ALTER SYSTEM SET max_slot_wal_keep_size = '10GB';
SELECT pg_reload_conf();
```

The appropriate value depends on:

- Your data change rate
- How long snapshots take
- How often you pause/unpause clusters

**2. Ensure adequate cluster sizing**

Make sure your Materialize source cluster has enough resources to keep up with
replication:

```mzsql
ALTER CLUSTER your_source_cluster SET (SIZE = 'M.1-large');
```

**3. Monitor replication lag**

Regularly check that your sources are keeping up:

```mzsql
-- Check source statistics
SELECT *
FROM mz_internal.mz_source_statistics
WHERE id = 'your_source_id';
```

## Prevention

**Best practices to avoid this error:**

- Set `max_slot_wal_keep_size` to a value appropriate for your workload
  (typically 5-10GB or more).
- Size your source clusters appropriately for your data ingestion rate.
- Avoid pausing clusters for extended periods when sources are active.
- Monitor replication lag regularly.
- Consider limiting initial snapshot size by using `FOR TABLES` instead of
  `FOR ALL TABLES` if you have very large databases.
- If using a managed PostgreSQL provider, verify their replication slot and WAL
  retention policies.

## Provider-specific considerations

### Neon

Neon has been observed to have more aggressive WAL cleanup policies. If you're
using Neon:

- Monitor replication lag more frequently.
- Consider using a dedicated Neon branch for replication.
- Contact Neon support about their replication slot retention policies.

### Amazon RDS

RDS respects `max_slot_wal_keep_size` but also has instance storage limits.
Ensure your RDS instance has adequate storage for WAL retention.

### Self-managed PostgreSQL

You have full control over WAL retention settings, but ensure you also monitor
disk space to prevent storage issues.
