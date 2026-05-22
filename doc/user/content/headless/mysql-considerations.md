---
headless: true
---
### Schema changes

Materialize supports schema changes in the upstream database as follows:

#### Compatible schema changes (Legacy syntax)

{{< note >}}

This section refer to the legacy [`CREATE SOURCE ... FOR
...`](/sql/create-source/mysql/) that creates subsources as part of the `CREATE
SOURCE` operation.  To be able to handle the upstream column additions and
drops, use [`CREATE SOURCE (New Syntax)`](/sql/create-source/mysql-v2/) and
[`CREATE TABLE FROM SOURCE`](/sql/create-table) instead.  For details, see
[MySQL: Source versioning guide](/ingest-data/mysql/source-versioning/).

{{< /note >}}

{{% include-from-yaml data="mysql_source_details"
name="mysql-compatible-schema-changes-legacy" %}}

#### Incompatible schema changes

{{% include-from-yaml data="mysql_source_details"
name="mysql-incompatible-schema-changes-legacy" %}}

### Supported types

{{% include-from-yaml data="mysql_source_details"
name="mysql-supported-types" %}}

{{% include-from-yaml data="mysql_source_details"
name="mysql-unsupported-types" %}}

### Truncation

{{% include-from-yaml data="mysql_source_details"
name="mysql-truncation-restriction" %}}

### Modifying an existing source

{{% include-headless "/headless/alter-source-snapshot-blocking-behavior" %}}
