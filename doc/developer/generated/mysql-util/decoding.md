---
source: src/mysql-util/src/decoding.rs
revision: 011b3b5573
---

# mysql-util::decoding

Implements `pack_mysql_row`, which converts a `mysql_async::Row` into a Materialize `Row` by iterating columns in ordinal order and packing each `mysql_common::Value` into the appropriate `Datum` according to the column's `MySqlColumnDesc`.
Handles all supported MySQL scalar types (integers, floats, strings, dates, timestamps, time, numerics, JSON, binary, BIT, ENUM) and correctly maps the divergent wire representations between query responses and binlog events.
When column names begin with `@` (indicating a binlog without `binlog_row_metadata=FULL`), columns are matched by ordinal position; otherwise they are matched by name, filtering to only those columns present in the `MySqlTableDesc`.
