---
source: src/mysql-util/src/decoding.rs
revision: db271c31b1
---

# mysql-util::decoding

Implements `pack_mysql_row`, which converts a `mysql_async::Row` into a Materialize `Row` by iterating columns in ordinal order and packing each `mysql_common::Value` into the appropriate `Datum` according to the column's `MySqlColumnDesc`.
Handles all supported MySQL scalar types (integers, floats, strings, dates, timestamps, numerics, JSON, binary, BIT, ENUM) and correctly maps the divergent wire representations between query responses and binlog events.
