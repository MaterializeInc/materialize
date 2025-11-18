### Schema changes

Materialize supports schema changes in the upstream database as follows:

#### Compatible schema changes (Legacy syntax)

{{< note >}}

This section refer to the legacy [`CREATE SOURCE ... FOR
...`](/sql/create-source/postgres/) that creates subsources as part of the
`CREATE SOURCE` operation.  To be able to handle the upstream column additions
and drops, see [`CREATE SOURCE (New Syntax)`](/sql/create-source/postgres-v2/)
and [`CREATE TABLE FROM SOURCE`](/sql/create-table).

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


### Publication membership

{{< include-md file="shared-content/postgres-publication-membership.md" >}}

### Supported types

{{< include-md file="shared-content/postgres-supported-types.md" >}}

{{% include-md file="shared-content/postgres-unsupported-types.md" %}}

### Truncation

{{< include-md file="shared-content/postgres-truncation-restriction.md" >}}

### Inherited tables

{{< include-md file="shared-content/postgres-inherited-tables.md" >}}
