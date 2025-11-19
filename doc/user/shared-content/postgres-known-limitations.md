### Schema changes

Materialize supports schema changes in the upstream database as follows:

#### Compatible schema changes (Legacy syntax)

{{% include-from-yaml data="postgres_source_details"
name="postgres-compatible-schema-changes-legacy" %}}

#### Incompatible schema changes

{{% include-from-yaml data="postgres_source_details"
name="postgres-incompatible-schema-changes" %}}

{{% include-from-yaml data="postgres_source_details"
name="postgres-incompatible-schema-changes-handling-legacy" %}}

### Publication membership

{{% include-from-yaml data="postgres_source_details"
name="postgres-publication-membership" %}}

{{% include-from-yaml data="postgres_source_details"
name="postgres-publication-membership-mitigation-legacy" %}}

### Supported types

{{% include-from-yaml data="postgres_source_details"
name="postgres-supported-types" %}}

{{% include-from-yaml data="postgres_source_details"
name="postgres-unsupported-types" %}}

### Truncation

{{% include-from-yaml data="postgres_source_details"
name="postgres-truncation-restriction" %}}

### Inherited tables

{{% include-from-yaml data="postgres_source_details"
name="postgres-inherited-tables" %}}
