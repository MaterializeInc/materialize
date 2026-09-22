---
headless: true
---
### Snapshot parallelism

When a source is created, Materialize parallelizes the initial snapshot across
the cluster's workers, and can split the read of a large table across workers
when the table meets certain requirements. See [Snapshot
parallelism](/ingest-data/mysql/snapshot-parallelism/).

### Adding a table to an existing source

{{% include-headless "/headless/alter-source-snapshot-blocking-behavior" %}}
