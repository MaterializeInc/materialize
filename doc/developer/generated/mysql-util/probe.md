---
source: src/mysql-util/src/probe.rs
revision: 2c7b27fb08
---

# mysql-util::probe

`KeyProber` for navigating a MySQL table's string primary key column via prefix-based range queries. Used by the MySQL source to split a table's key space into balanced sub-ranges for parallel snapshot ingestion.

## `KeyProber`

Holds a live `mysql_async::Conn`, the quoted `schema.table` and key column identifiers for SQL interpolation, and their unquoted forms for error messages. The connection is assumed to use the `utf8mb4` character set so key values arrive as valid UTF-8.

### Methods

**`estimate_range_rows(lower_exclusive, upper_exclusive)`** — runs `EXPLAIN FORMAT=TRADITIONAL` on a range-filtered `SELECT` and returns the optimizer's row estimate. Estimates can be inaccurate (observed overcount of ~2x on large static tables) because MySQL samples only a small number of index pages.

**`prefix_of_first_key_in_range(lower_exclusive, upper_exclusive, max_prefix_length)`** — returns a prefix of up to `max_prefix_length` characters of the first key in the given range via `SELECT LEFT(col, n) ... ORDER BY col LIMIT 1`.

**`prefix_of_first_row_not_matching_prefix(prefix, upper_exclusive, max_prefix_length)`** — finds the prefix of the first key that does not share the given prefix, using two sequential queries (must run inside a `REPEATABLE READ` transaction): first finds the maximum key with the prefix via `max_key_with_prefix`, then calls `prefix_of_first_key_in_range` starting from that key.

### LIKE pattern construction

`like_prefix_pattern` builds a `LIKE 'prefix%'` pattern using `|` as the explicit escape character (set via `ESCAPE '|'`). This avoids dependence on MySQL's configurable backslash-escape mode (`NO_BACKSLASH_ESCAPES`). The `%`, `_`, and `|` characters in the prefix are escaped; backslash is left unescaped because it has no special meaning under an explicit `ESCAPE '|'`.

### Row estimation

`explain_row_estimate` runs `EXPLAIN FORMAT=TRADITIONAL` and reads the `rows` column of the query plan row. Returns `None` if the plan produces no row.
