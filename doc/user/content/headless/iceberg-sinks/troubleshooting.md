---
headless: true
---
### Sink creation fails with "input compacted past resume upper"

This error occurs when the source data has been compacted beyond the point where
the sink last committed. This can happen after a Materialize backup/restore
operation. You may need to drop and recreate the sink, which will re-snapshot
the entire source relation.

### Commit conflicts

If another process modifies the Iceberg table while Materialize is committing,
you may see commit conflict errors. Materialize will automatically retry, but
if conflicts persist, ensure no other writers are modifying the same table.

### Sink stalls on a table schema mismatch

If the Iceberg table's schema stops matching the sink's input, for example
because another writer adds, renames, or retypes a column, the sink reports
`stalled` with an error containing `schema does not match expected schema`.
Materialize does not retry this error. The sink stops and stays `stalled`
until it is restarted.

To recover:

1. Change the table's schema back to what the sink expects. The error names
   the first field that differs. Do not drop the table. Materialize would
   create a new one, but would not write the data it has already committed
   again.

1. Restart the sink by changing its commit interval. Setting the interval to
   its current value does not restart the sink.

   ```mzsql
   ALTER SINK <sink_name> SET (COMMIT INTERVAL = '<new_interval>');
   ```

   Alternatively, restart the replicas of the cluster that hosts the sink, as
   described in [Sink falls behind and does not catch
   up](#sink-falls-behind-and-does-not-catch-up).

### Sink falls behind and does not catch up

An Iceberg sink can fall behind its input when each commit to the catalog takes
longer than the sink's `COMMIT INTERVAL`. In steady state, the sink commits one
snapshot per interval of data. If a commit takes longer than the interval, the
sink falls further behind with every commit, and its lag keeps growing even
though the input relation is up to date. How long a commit takes depends on
your catalog and object store, and can be several seconds.

To check whether the sink is behind its input, compare their lag:

```mzsql
SELECT o.name, l.lag
FROM mz_internal.mz_wallclock_global_lag AS l
JOIN mz_catalog.mz_objects AS o ON o.id = l.object_id
WHERE o.name IN ('<sink_name>', '<input_relation_name>');
```

If the input's lag is low but the sink's lag is high and growing, recover the
sink in one of the following ways:

- Increase the commit interval to a value that comfortably exceeds the time a
   commit takes. Outside of development environments, use at least `1m`. See
   [Commit interval tradeoffs](/sql/create-sink/iceberg/#commit-interval-tradeoffs).

   ```mzsql
   ALTER SINK <sink_name> SET (COMMIT INTERVAL = '1m');
   ```

   Changing the commit interval restarts the sink.

- If the commit interval is already appropriate, restart the sink by
   restarting the replicas of the cluster that hosts it. Restarting the
   replicas also restarts every other object on that cluster, which must then
   rehydrate.

   For a managed cluster:

   ```mzsql
   ALTER CLUSTER <cluster_name> SET (REPLICATION FACTOR = 0);
   ALTER CLUSTER <cluster_name> SET (REPLICATION FACTOR = <original replication factor>);
   ```

   For an unmanaged cluster, drop and recreate each replica:

   ```mzsql
   DROP CLUSTER REPLICA <cluster_name>.<replica_name>;
   CREATE CLUSTER REPLICA <cluster_name>.<replica_name> (SIZE = '<original size>');
   ```

When the sink restarts, it writes all data between its last committed point and
the current state of its input in a single catch-up commit, then resumes
committing at the configured interval. Restarting the sink without increasing a
commit interval that is too short only recovers temporarily, and the sink falls
behind again.
