---
source: src/sql/src/iceberg.rs
revision: 48175dc930
---

# mz-sql::iceberg

Thin file that uses the `generate_extracted_config!` macro to produce an `IcebergSinkConfigOptionExtracted` type, which deserializes the `TABLE` and `NAMESPACE` options from Iceberg sink `WITH` clauses.
