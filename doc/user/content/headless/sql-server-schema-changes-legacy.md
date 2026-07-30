---
headless: true
---
### Schema changes (legacy source syntax)
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

#### Incompatible schema changes

All other schema changes to upstream tables will set the corresponding subsource
into an error state, which prevents you from reading from the source.

To handle incompatible [schema changes](#schema-changes-legacy-source-syntax), use [`DROP SOURCE`](/sql/alter-source/#context)
and [`ALTER SOURCE...ADD SUBSOURCE`](/sql/alter-source/) to first drop the
affected subsource, and then add the table back to the source. When you add the
subsource, it will have the updated schema from the corresponding upstream
table.
