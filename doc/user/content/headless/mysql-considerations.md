---
headless: true
---
### Schema changes

{{% include-headless "/headless/schema-changes-in-progress" %}}

Materialize supports schema changes in the upstream database as follows:

#### Compatible schema changes

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
