### Schema changes

{{< include-md file="shared-content/schema-changes-in-progress.md" >}}

{{% schema-changes %}}

### Supported types

{{< include-md file="shared-content/mysql-supported-types.md" >}}

{{< include-md file="shared-content/mysql-unsupported-type-handling.md" >}}

### Truncation

Upstream tables replicated into Materialize should not be truncated. If an
upstream table is truncated while replicated, the whole source becomes
inaccessible and will not produce any data until it is recreated. Instead of
truncating, you can use an unqualified `DELETE` to remove all rows from the table:

```mzsql
DELETE FROM t;
```

### Modifying an existing source

{{< include-md file="shared-content/alter-source-snapshot-blocking-behavior.md"
>}}
