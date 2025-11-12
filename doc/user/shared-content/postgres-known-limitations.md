### Schema changes (Legacy syntax) {#schema-changes}

{{< note >}}

This schema changes section refer to the legacy [`CREATE SOURCE ...
FOR ...`](/sql/create-source/postgres/) that creates
subsources as part of the `CREATE SOURCE` operation.  To be able to handle the
upstream table schema changes, see [`CREATE
SOURCE (New Syntax)`](/sql/create-source/postgres-v2/) and [`CREATE TABLE FROM SOURCE`](/sql/create-table).

{{< /note >}}

{{% schema-changes %}}

### Publication membership

{{< include-md file="shared-content/postgres-publication-membership.md" >}}

### Supported types

{{< include-md file="shared-content/postgres-supported-types.md" >}}

{{% include-md file="shared-content/postgres-unsupported-types.md" %}}

### Truncation

{{< include-md file="shared-content/postgres-truncation-restriction.md" >}}

### Inherited tables

{{< include-md file="shared-content/postgres-inherited-tables.md" >}}
