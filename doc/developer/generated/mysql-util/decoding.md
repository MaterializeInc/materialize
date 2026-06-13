---
source: src/mysql-util/src/decoding.rs
revision: 4b6c7d41d5
---

# mysql-util::decoding

Implements `pack_mysql_row`, which converts a `mysql_async::Row` into a Materialize `Row` by iterating over `table_desc.columns` and packing each `mysql_common::Value` into the appropriate `Datum` according to the column's `MySqlColumnDesc`.
Handles all supported MySQL scalar types (integers, floats, strings, dates, timestamps, time, numerics, JSON, binary, BIT, ENUM) and correctly maps the divergent wire representations between query responses and binlog events.
Columns are matched either by name (when `binlog_full_metadata=true`, which allows correct decoding even if the upstream reordered columns) or by ordinal position (when `binlog_full_metadata=false`).
Columns whose `column_type` is `None` are ignored and not decoded; for non-ignored columns, a missing wire counterpart returns an error.
For `MySqlColumnMeta::Year` columns, `mysql_common` returns `1900` to represent the zero YEAR value (`"0000"`); decoding remaps `1900` to `0` and formats the result as a 4-digit string so the output matches MySQL's `"0000"` display.
