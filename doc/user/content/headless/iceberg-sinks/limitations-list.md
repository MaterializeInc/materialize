---
headless: true
---
- **Same region required**: {{% include-headless
  "/headless/iceberg-sinks/region-requirements" %}}
- **Schema evolution**: Materialize does not support changing the schema of an
  existing Iceberg table. If the source schema changes, you must drop and
  recreate the sink.
- **Partition evolution**: Partition spec changes are not supported.
- **Partitioning**: Materialize creates unpartitioned tables. Partitioned tables
  are not supported.
- **Record types**: Composite/record types are not supported. Use scalar types
  or flatten your data structure.
