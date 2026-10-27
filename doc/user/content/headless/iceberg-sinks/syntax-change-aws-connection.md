---
headless: true
---

{{< note >}}
`CREATE SINK` no longer includes a `USING AWS CONNECTION` clause.
Instead, the sink inherits credentials from the [Iceberg catalog connection](/sql/create-connection/#iceberg-catalog).
Existing Iceberg sinks are not affected and will continue to function as before.
{{< /note >}}
