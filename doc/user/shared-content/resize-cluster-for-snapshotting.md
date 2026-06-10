```sql
ALTER CLUSTER <cluster_name> SET ( SIZE = <new_size> );
```

{{% note %}}

Resizing a cluster with sources requires the cluster to restart. This operation
incurs downtime for the duration it takes for all objects in the cluster to
[hydrate](/ingest-data/#hydration).

You might want to let the new-sized replica hydrate before shutting down the
current replica. See [zero-downtime cluster
resizing](/sql/alter-cluster/#zero-downtime-cluster-resizing) about automating
this process.

{{% /note %}}

Once the initial snapshot has completed, you can resize the cluster for steady
state.
