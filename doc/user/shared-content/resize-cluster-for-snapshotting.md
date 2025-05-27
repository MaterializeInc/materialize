```sql
ALTER CLUSTER <cluster_name> SET ( SIZE = <new_size> );
```

{{% note %}}

Resizing a cluster that hosts sources requires the cluster to restart. This
operation incurs downtime for the duration it takes for all objects in the
cluster to [hydrate](/ingest-data/#hydration).

{{% /note %}}

Once the initial snapshot has completed, you can resize the cluster for steady
state.
