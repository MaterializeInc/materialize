# MODE APPEND for Iceberg Sinks (SS-78)

## What this feature does

In append mode, every change in the Materialize update stream is written as a **data row** (never as an Iceberg delete file). Two extra columns are appended to every row:

| Column        | Type   | Meaning                                      |
|---------------|--------|----------------------------------------------|
| `_mz_diff`        | int    | `+1` for insertions, `-1` for deletions      |
| `_mz_timestamp`| bigint | Materialize logical timestamp of the change  |

Deletes and the "before" half of updates are written as ordinary data rows with `_mz_diff = -1`. Updates produce two rows at the same `_mz_timestamp`: one with `_mz_diff = -1` (old value) and one with `_mz_diff = +1` (new value).

This is the internal Materialize update-stream representation made directly visible in Iceberg, allowing consumers to reconstruct point-in-time state by replaying the log.

**Contrast with MODE UPSERT:** Upsert mode maintains a current-value table using Iceberg delete files. Append mode is simpler and produces a pure log.

## SQL syntax

```sql
CREATE SINK s
  FROM t
  INTO ICEBERG CATALOG CONNECTION c (NAMESPACE 'ns', TABLE 'tbl')
  USING AWS CONNECTION aws
  MODE APPEND               -- KEY is not permitted
  WITH (COMMIT INTERVAL '1s');
```

## Status

### Done — fully implemented and passing

**Parsing layer:**
- `src/sql-lexer/src/keywords.txt` — Added `Append` keyword
- `src/sql-parser/src/ast/defs/ddl.rs` — Added `IcebergSinkMode::Append` variant
- `src/sql-parser/src/parser.rs` — `parse_iceberg_sink_mode` handles `APPEND`
- `src/sql/src/plan/statement/ddl.rs` — Maps `MODE APPEND` → `SinkEnvelope::Append`; rejects KEY
- `src/storage-types/src/sinks.rs` — Added `SinkEnvelope::Append` variant
- `src/catalog/src/memory/objects.rs` — Display name "append" for `SinkEnvelope::Append`

**Write path (`src/storage/src/sink/iceberg.rs`):**
- `build_schema_with_append_columns` — extends Arrow schema with `_mz_diff` (Int32) and `_mz_timestamp` (Int64), assigning sequential Parquet field IDs
- `row_to_recordbatch_append` — converts `DiffPair<Row>` + `ts` into a `RecordBatch` with before→diff=-1 and after→diff=+1, no delete semantics
- `AppendWriterType` type alias and `BatchWriter` enum — unifies `DeltaWriter` (upsert) and `DataFileWriter` (append) behind a common `.write()` / `.close()` interface
- `write_data_files` — gained `envelope: SinkEnvelope` parameter; branches on upsert vs. append for key check, writer factory, schema setup, and row conversion
- `render_sink` — extends Arrow + Iceberg schemas with append columns before passing to the minter (table creation) and writer; passes `sink.envelope` to `write_data_files`
- `commit_to_iceberg` — no change needed; empty delete-file list is already handled correctly

**Test:**
- `test/iceberg/mode-append.td` — passing; verified inserts (_mz_diff=1), deletes (_mz_diff=-1), updates (_mz_diff=-1 + _mz_diff=+1), and Polaris REST catalog reads

All iceberg tests pass (`catalog.td`, `mode-append.td`, `nested-records.td`).

## How to run the tests

```bash
# Single test
bin/mzcompose --find iceberg run default -- mode-append.td

# All iceberg tests
bin/mzcompose --find iceberg run default
```

## Key file locations

| File | Notes |
|------|-------|
| `src/storage/src/sink/iceberg.rs` | All write-path changes; `build_schema_with_append_columns` near `build_schema_with_op_column`; `BatchWriter` enum after type aliases |
| `src/storage-types/src/sinks.rs` | `SinkEnvelope::Append` ~125; `StorageSinkDesc.envelope` ~44 |
| `src/sql/src/plan/statement/ddl.rs` | Mode mapping ~3503; KEY rejection ~3610 |
| `test/iceberg/mode-append.td` | End-to-end test |

## Next task: Add MODE APPEND test coverage wherever MODE UPSERT is tested

**Instruction:** Wherever there is a MODE UPSERT test for the Iceberg sink, add a corresponding MODE APPEND test. The APPEND variant does not take a KEY clause.

### Locations that need MODE APPEND tests

| File | What exists | What to add |
|------|-------------|-------------|
| `src/sql-parser/tests/testdata/ddl` (lines 917–936) | Two `parse-statement` cases for MODE UPSERT (with and without `NOT ENFORCED`); one error case for unknown option | A `parse-statement` case for MODE APPEND (no KEY); verify `mode: Some(Append)` in the AST |
| `test/iceberg/nested-records.td` | MODE UPSERT sink over a table with nested record columns; tests insert + update via DuckDB | A MODE APPEND sink over the same or similar nested-column table; verify `_mz_diff` and `_mz_timestamp` columns appear alongside nested fields |
| `test/race-condition/mzcompose.py` (`IcebergSink` class, lines 616–681) | `IcebergSink.create()` always emits `MODE UPSERT` with `KEY (a) NOT ENFORCED` | Add an `IcebergAppendSink` class (or randomise mode) that creates a MODE APPEND sink without a KEY clause |
| `test/bounded-memory/mzcompose.py` (`iceberg-sink` scenario, lines 1378–1431) | `KEY (key) NOT ENFORCED MODE UPSERT` under memory pressure | A second scenario (`iceberg-sink-append`) using `MODE APPEND` (no KEY), same data volume, same memory limits |
| `test/aws/aws-iceberg-e2e.td` | MODE UPSERT against S3 Tables REST catalog | A MODE APPEND sink after the upsert sink is dropped; verify `_mz_diff` and `_mz_timestamp` in the DuckDB query |

### Notes
- The `mode-append.td` testdrive file already covers the core append semantics (inserts, deletes, updates, Polaris REST catalog). The remaining gaps are parser round-trip, nested-column types, stress/race, bounded memory, and the AWS e2e test.
- For the parser testdata, run `cargo test -p mz-sql-parser` to regenerate and verify. The test format is: input line, `----`, pretty-printed SQL, `=>`, AST `Debug` output (or `error:` line for parse errors).
- For the race-condition and bounded-memory tests, MODE APPEND sinks should not include a KEY clause; KEY is rejected by the planner for APPEND mode.

---

## Design notes

- The upsert code path is unchanged; append is a fully separate branch.
- `render_sink` extends the schema with append columns **before** the minter, so the Iceberg table is created with `_mz_diff` and `_mz_timestamp` as real columns (not the internal `__op` signal used by DeltaWriter).
- In `write_data_files`, `arrow_schema` (post-merge) includes all columns including `_mz_diff`/`_mz_timestamp`. A `user_schema` sub-slice (all but the last 2 fields) is passed to `ArrowBuilder` for user-column serialization; `_mz_diff` and `_mz_timestamp` are appended as plain arrays afterward.
- `commit_to_iceberg` separates files by `content_type()`; append mode produces only `Data` files, so the delete list is always empty — no code change needed there.
