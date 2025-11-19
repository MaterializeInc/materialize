{{< include-md file="shared-content/postgres-known-limitations.md" >}}

### Replication slots

{{% include-from-yaml data="postgres_source_details"
name="postgres-replication-slots" %}}

{{% include-from-yaml data="postgres_source_details" name="postgres-replication-slots-tip-list" %}}

### Modifying an existing source

{{< include-md file="shared-content/alter-source-snapshot-blocking-behavior.md"
>}}
