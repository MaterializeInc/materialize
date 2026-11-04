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
