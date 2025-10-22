### Supported types

{{< include-md file="shared-content/sql-server-supported-types.md" >}}

{{< include-md file="shared-content/sql-server-unsupported-type-handling.md" >}}

### Timestamp Rounding

{{< include-md file="shared-content/sql-server-timestamp-rounding.md" >}}

### Snapshot latency for inactive databases

{{< include-md file="shared-content/create-table-from-source-snapshotting.md"
>}} {{< include-md
file="shared-content/sql-server-snapshot-latency.md">}}

See [Monitoring freshness
status](/ingest-data/monitoring-data-ingestion/#monitoring-hydrationdata-freshness-status).

### Capture Instance Selection

When a new source is created, Materialize selects a capture instance for each
table. SQL Server permits at most two capture instances per table, which are
listed in the
[`sys.cdc_change_tables`](https://learn.microsoft.com/en-us/sql/relational-databases/system-tables/cdc-change-tables-transact-sql)
system table. For each table, Materialize picks the capture instance with the
most recent `create_date`.

If two capture instances for a table share the same timestamp (unlikely given the millisecond resolution), Materialize selects the `capture_instance` with the lexicographically larger name.
