---
headless: true
---
### Schema changes
Materialize supports schema changes in the upstream database as follows:

#### Compatible schema changes (Legacy syntax)

{{< note >}}

This section refer to the legacy [`CREATE SOURCE ... FOR
...`](/sql/create-source/sql-server/) that creates subsources as part of the
`CREATE SOURCE` operation.  To be able to handle the upstream column additions
and drops, use [`CREATE SOURCE (New Syntax)`](/sql/create-source/sql-server-v2/)
and [`CREATE TABLE FROM SOURCE`](/sql/create-table) instead.  For details, see
[SQL Server: Source versioning
guide](/ingest-data/sql-server/source-versioning/).

{{< /note >}}

- Adding columns to tables. Materialize will **not ingest** new columns added
  upstream unless you use [`DROP SOURCE`](/sql/alter-source/#context) to first
  drop the affected subsource, and then add the table back to the source using
  [`ALTER SOURCE...ADD SUBSOURCE`](/sql/alter-source/).

- Dropping columns that were added after the source was created. These columns
  are never ingested, so you can drop them without issue.

- Adding or removing `NOT NULL` constraints to tables that were nullable when
  the source was created.


#### Incompatible schema changes

All other schema changes to upstream tables will set the corresponding subsource
into an error state, which prevents you from reading from the source.

To handle incompatible [schema changes](#schema-changes), use [`DROP SOURCE`](/sql/alter-source/#context)
and [`ALTER SOURCE...ADD SUBSOURCE`](/sql/alter-source/) to first drop the
affected subsource, and then add the table back to the source. When you add the
subsource, it will have the updated schema from the corresponding upstream
table.


### Supported types

{{< include-md file="shared-content/sql-server-supported-types.md" >}}

{{< include-md file="shared-content/sql-server-unsupported-type-handling.md" >}}

### Timestamp Rounding

The `time`, `datetime2`, and `datetimeoffset` types in SQL Server have a default
scale of 7 decimal places, or in other words a accuracy of 100 nanoseconds. But
the corresponding types in Materialize only support a scale of 6 decimal places.
If a column in SQL Server has a higher scale than what Materialize can support, it
will be rounded up to the largest scale possible.

```
-- In SQL Server
CREATE TABLE my_timestamps (a datetime2(7));
INSERT INTO my_timestamps VALUES
  ('2000-12-31 23:59:59.99999'),
  ('2000-12-31 23:59:59.999999'),
  ('2000-12-31 23:59:59.9999999');

-- Replicated into Materialize
SELECT * FROM my_timestamps;
'2000-12-31 23:59:59.999990'
'2000-12-31 23:59:59.999999'
'2001-01-01 00:00:00'
```

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

### Modifying an existing source

{{% include-headless "/headless/alter-source-snapshot-blocking-behavior" %}}
