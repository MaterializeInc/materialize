---
headless: true
---

### Operations that do not require re-creating the source

Materialize tracks a [log sequence number
(LSN)](https://www.postgresql.org/docs/current/wal-internals.html) as it
consumes the upstream write-ahead log (WAL), and the source's replication slot
retains the WAL that Materialize has not yet consumed. Because the slot outlives
the connection, routine operational events do not lose data: after a transient
interruption the source stalls, then resumes from its committed LSN and catches
up automatically. **No action is required** for the following operations:

- Restarting or patching PostgreSQL (including OS-level restarts).
- Restarting Materialize. The source resumes from its committed LSN and does
  **not** re-snapshot already-ingested data.
- Transient network interruptions between Materialize and PostgreSQL. These
  surface as [`connection closed`](/ingest-data/postgres/connection-closed/).
- Resizing the cluster that hosts the source, or changing its replication
  factor. Briefly, the source may report [`replication slot ... is
  active`](/ingest-data/postgres/replication-slot-active/) while the upstream
  releases the slot from the previous connection.
- The upstream database running out of disk space, once space is freed.

{{< note >}}
Recovery after an interruption depends on the WAL that the replication slot is
holding still being available upstream. An interruption long enough for the slot
to be invalidated, or for the slot to be dropped, is not recoverable. See
[Replication slot invalidated](#replication-slot-invalidated) and [Replication
slot dropped or rewound](#replication-slot-dropped-or-rewound).
{{< /note >}}

{{< warning >}}
While a source is disconnected, the upstream WAL accumulates behind its
replication slot and cannot be reclaimed. A long outage, an undersized source
cluster, or a source cluster stuck in a restart loop can therefore consume
significant upstream disk. Monitor `restart_lsn` in
[`pg_replication_slots`](https://www.postgresql.org/docs/current/view-pg-replication-slots.html)
during planned maintenance.
{{< /warning >}}

### Operations that require re-creating the source

A smaller set of events breaks LSN continuity or destroys the replication slot.
When this happens, Materialize cannot guarantee a correct, gap-free view of your
data, so it puts the **entire source** into an error or permanently stalled
state that requires **re-creating** the source. Upstream changes to an
individual table's schema are handled separately, and do not error the entire
source.

In each case below, the remediation is to drop and re-create the source:

```mzsql
DROP SOURCE mz_source CASCADE;

CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg_connection (PUBLICATION 'mz_source');

-- Re-create the tables you were ingesting.
CREATE TABLE table_1 FROM SOURCE mz_source (REFERENCE public.table_1);
```

If you are using the legacy `CREATE SOURCE ... FOR TABLES` syntax, re-create the
source with `FOR TABLES` or `FOR ALL TABLES` instead of adding tables
separately.

Because a re-created source snapshots from the current state of the upstream
database, any changes it missed while it was in an error state are reflected in
the snapshot rather than replayed as individual updates.

{{< warning >}}
`CASCADE` drops every object that depends on the source, including its tables,
views, materialized views, indexes, and sinks. Capture their definitions before
you run it, and re-create them once the new source has finished snapshotting.
{{< /warning >}}

#### Point-in-time restore

Restoring the source database from a backup, including restoring to a different
server for disaster recovery, increments the PostgreSQL timeline and is detected
as a discontinuity. The source fails with an error of the form:

```
unsupported action: database restored from point-in-time backup. Expected
timeline ID 8 but got 9
```

The same error covers other events that change the timeline, such as a managed
failover between replicas. To see the timeline a source is pinned to, query
[`mz_internal.mz_postgres_sources`](/sql/system-catalog/mz_internal/#mz_postgres_sources):

```mzsql
SELECT s.name, p.replication_slot, p.timeline_id
FROM mz_internal.mz_postgres_sources p
JOIN mz_catalog.mz_sources s ON s.id = p.id;
```

If your upstream fails over between replicas as part of routine maintenance, see
[High-availability failovers](#high-availability-failovers).

#### Promotion of a physical replica

When a source reads from a physical standby (read replica) rather than the
primary, promoting that standby to a primary fails the source with:

```
unsupported action: upstream physical replica status changed (e.g. a physical
replica was promoted to a primary). Expected pg_is_in_recovery()=true but got
false
```

Materialize detects the promotion while the replication stream is live, without
waiting for a restart. Re-create the source against the promoted node.

#### Replication slot invalidated

PostgreSQL invalidates a replication slot once the WAL it holds exceeds
[`max_slot_wal_keep_size`](https://www.postgresql.org/docs/current/runtime-config-replication.html#GUC-MAX-SLOT-WAL-KEEP-SIZE).
This protects the upstream from running out of disk, at the cost of ending
replication. The source fails with:

```
replication slot has been invalidated because it exceeded the maximum reserved
size
```

To avoid this, size the source cluster so that it keeps up with the upstream
write rate, and set `max_slot_wal_keep_size` high enough to cover your longest
expected outage. Some hosted PostgreSQL services set this value for you and do
not allow it to be raised.

#### Replication slot dropped or rewound

If the slot Materialize is using is dropped upstream, or the upstream is rebuilt
from a base backup (which does not carry replication slots), a new slot starts
at the current LSN, past the point the source needs to resume from. The source
stalls with:

```
slot overcompacted. Requested LSN ... but only LSNs >= ... are available
```

For diagnosis steps, see [Slot
overcompacted](/ingest-data/postgres/slot-overcompacted/). PostgreSQL refuses to
drop a slot that is in use, so this generally happens only while the source is
paused or disconnected.

#### Dropping the publication

Running `DROP PUBLICATION` upstream stalls the source, and all of its tables,
with:

```
publication "mz_source" does not exist
```

Re-create the publication upstream, then re-create the source.

#### Major version upgrades

A PostgreSQL major version upgrade rewrites the on-disk format and does not
preserve the replication slot, so there is no in-place recovery. To upgrade without a gap in your downstream views, run a second source
against the upgraded instance in parallel and cut over once it has hydrated. See
[Upgrade the major version of your PostgreSQL
source](/ingest-data/postgres/major-version-upgrade/).

### High-availability failovers

Some managed PostgreSQL services increment the timeline during routine
high-availability operations, such as maintenance, a machine-tier change, or an
automatic failover between replicas. Materialize cannot distinguish these from a
genuine restore, so by default they fail the source with the [`Expected timeline
ID`](#point-in-time-restore) error.

On self-managed Materialize, where the upstream service guarantees that a
failover is a contiguous fork of the WAL with no data loss, you can disable
timeline validation with the
[`pg_source_validate_timeline`](/sql/alter-system-set/) system parameter:

```mzsql
ALTER SYSTEM SET pg_source_validate_timeline = false;
```

This parameter is not available on Materialize Cloud. There, a
high-availability failover that changes the timeline requires re-creating the
source.

{{< warning >}}
Disabling this check is a trade-off. With it off, Materialize also does **not**
detect a genuine [point-in-time restore](#point-in-time-restore) or any other
discontinuous timeline change, and silently ingesting across one can corrupt the
contents of the source. Only disable it when your provider documents that its
failovers preserve WAL continuity for logical replication subscribers, and
re-create the source manually after any operation that does not.
{{< /warning >}}
