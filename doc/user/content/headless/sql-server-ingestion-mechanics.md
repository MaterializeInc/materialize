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

### Keys

Materialize records at most one key for each table it ingests from SQL Server:
the `PRIMARY KEY` or `UNIQUE` constraint that backs the index of the capture
instance it ingests from. The capture instance's index is listed in the
`index_name` column of
[`cdc.change_tables`](https://learn.microsoft.com/en-us/sql/relational-databases/system-tables/cdc-change-tables-transact-sql).

- By default, the capture instance uses the table's primary key, so the
  `PRIMARY KEY` constraint becomes the key.
- If the capture instance was created with
  [`@index_name`](https://learn.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sys-sp-cdc-enable-table-transact-sql#----index_name),
  the `UNIQUE` constraint of that name becomes the key instead. The primary key
  is not recorded.
- A table whose capture instance has no index, for example a table without a
  primary key, gets no key.

Other `PRIMARY KEY` and `UNIQUE` constraints on the table are not recorded as
keys, because SQL Server lets them be dropped at any time without notifying
Materialize. A `UNIQUE` constraint over nullable columns is never recorded,
because SQL Server does not accept it as a capture instance index.

Materialize uses the recorded key when optimizing queries, for example to
remove a redundant `DISTINCT`. SQL Server rejects dropping the capture
instance's index while that capture instance is the newest one for the table,
which keeps the recorded key valid. See [Changing
constraints](/ingest-data/sql-server/#changing-constraints) for what happens
when a newer capture instance exists.

### Adding a table to an existing source

{{% include-headless "/headless/alter-source-snapshot-blocking-behavior" %}}
