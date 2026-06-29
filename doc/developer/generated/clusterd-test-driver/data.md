---
source: src/clusterd-test-driver/src/data.rs
revision: 6d4c0fbb2b
---

# mz-clusterd-test-driver::data

Synthetic data generation and direct persist writes.

`Cell` is an owned scalar value enum covering `Null`, `Int16`, `Int32`, `Int64`, `UInt64`, `Float32`, `Float64`, `Bool`, `String`, `Bytes`. It bridges synthetic generation and explicit script-provided values, both of which need owned storage because `Datum` borrows its payloads.

`pack_cells(desc, cells)` packs a slice of `Cell`s into a `Row` according to a `RelationDesc`.

`sample_desc(width)` generates a `RelationDesc` with `width` nullable `text` columns.

`synth_rows(desc, count)` generates `count` deterministic synthetic rows for a given `RelationDesc`.

`write_rows_single_ts(client, shard_id, desc, rows, ts)` writes all rows at a single timestamp. `write_rows_spread(client, shard_id, desc, rows, ts_start)` spreads rows across consecutive timestamps starting at `ts_start`, one row per timestamp.
