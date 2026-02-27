---
headless: true
---

Materialize converts SQL types to Iceberg/Parquet types:

| SQL type | Iceberg type |
|----------|--------------|
| `boolean` | `boolean` |
| `smallint`, `integer` | `int` |
| `bigint` | `long` |
| `real` | `float` |
| `double precision` | `double` |
| `numeric` | `decimal(38, scale)` |
| `date` | `date` |
| `time` | `time` (microsecond) |
| `timestamp` | `timestamp` (microsecond) |
| `timestamptz` | `timestamptz` (microsecond) |
| `text`, `varchar` | `string` |
| `bytea` | `binary` |
| `uuid` | `fixed(16)` |
| `jsonb` | `string` |
| `record` | `struct` |
| `list` | `list` |
| `map` | `map` |
