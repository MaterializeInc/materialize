---
source: src/mysql-util/src/decoding.rs
revision: fc8d9dc1e4
---

# mysql-util::decoding

Implements `pack_mysql_row`, which converts a `mysql_async::Row` into a Materialize `Row` by iterating columns in ordinal order and packing each `mysql_common::Value` into the appropriate `Datum` according to the column's `MySqlColumnDesc`.
Handles all supported MySQL scalar types (integers, floats, strings, dates, timestamps, time, numerics, JSON, binary, BIT, ENUM) and correctly maps the divergent wire representations between query responses and binlog events.
Columns are always matched by ordinal position using `zip_longest` over `table_desc.columns` and the unwrapped row values; extra upstream columns are silently ignored and missing column descriptors return an error.
