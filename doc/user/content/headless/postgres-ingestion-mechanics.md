---
headless: true
---
### Replication slots

{{% include-from-yaml data="postgres_source_details"
name="postgres-replication-slots" %}}

{{% include-from-yaml data="postgres_source_details"
name="postgres-replication-slots-tip-list" %}}

### Snapshotting

{{% include-from-yaml data="postgres_source_details"
name="postgres-snapshot-behavior" %}}

### Publication membership

{{% include-from-yaml data="postgres_source_details"
name="postgres-publication-membership" %}}

{{% include-from-yaml data="postgres_source_details"
name="postgres-publication-membership-mitigation-legacy" %}}

### Inherited tables

{{% include-from-yaml data="postgres_source_details"
name="postgres-inherited-tables" %}}

- If using legacy syntax [`CREATE SOURCE ... FOR
  ...`](/sql/create-source/postgres/):

  {{% include-from-yaml data="postgres_source_details"
name="postgres-inherited-tables-action-legacy" %}}

- If using new [`CREATE TABLE FROM SOURCE`](/sql/create-table/) syntax:

  {{% include-from-yaml data="postgres_source_details"
name="postgres-inherited-tables-action" %}}

### Partitioned tables

{{% include-from-yaml data="postgres_source_details"
name="postgres-partitioned-tables" %}}

{{% include-from-yaml data="postgres_source_details"
name="postgres-partitioned-tables-action" %}}

### Modifying an existing source

{{% include-headless "/headless/alter-source-snapshot-blocking-behavior" %}}
