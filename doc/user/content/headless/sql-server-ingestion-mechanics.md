---
headless: true
---
### Snapshot latency for inactive databases

When a new Source is created, Materialize performs a snapshotting operation to sync
the data. However, for a new SQL Server source, if none of the replicating tables
are receiving write queries, snapshotting may take up to an additional 5 minutes
to complete. The 5 minute interval is due to a hardcoded interval in the SQL Server
Change Data Capture (CDC) implementation which only notifies CDC consumers every
5 minutes when no changes are made to replicating tables.

See [Monitoring freshness status](/ingest-data/monitoring-data-ingestion/#monitoring-hydrationdata-freshness-status)

### Capture Instance Selection

When a new source is created, Materialize selects a capture instance for each
table. SQL Server permits at most two capture instances per table, which are
listed in the
[`sys.cdc_change_tables`](https://learn.microsoft.com/en-us/sql/relational-databases/system-tables/cdc-change-tables-transact-sql)
system table. For each table, Materialize picks the capture instance with the
most recent `create_date`.

If two capture instances for a table share the same timestamp (unlikely given the millisecond resolution), Materialize selects the `capture_instance` with the lexicographically larger name.

### Unique keys

Materialize records at most one unique key for each table: the `PRIMARY KEY` or
`UNIQUE` constraint backing the index that the table's capture instance uses,
listed as `index_name` in
[`sys.cdc_change_tables`](https://learn.microsoft.com/en-us/sql/relational-databases/system-tables/cdc-change-tables-transact-sql).
This is the table's primary key, unless the capture instance was created with
`@index_name` pointing at a `UNIQUE` constraint. Materialize does not record
any other `PRIMARY KEY` or `UNIQUE` constraint on the table. A table whose
capture instance has no index, because the table had no primary key when CDC
was enabled and no `@index_name` was given, has no key in Materialize.

SQL Server does not allow dropping the index that a table's newest capture
instance uses. As long as Materialize ingests from that capture instance, the
recorded key cannot be dropped upstream, so the key stays valid for the queries
Materialize plans against it.

### Adding a table to an existing source

{{% include-headless "/headless/alter-source-snapshot-blocking-behavior" %}}
