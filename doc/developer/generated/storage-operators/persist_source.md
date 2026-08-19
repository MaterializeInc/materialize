---
source: src/storage-operators/src/persist_source.rs
revision: 95baa04a85
---

# storage-operators::persist_source

Provides the `persist_source` Timely operator that reads from a persist shard and emits `(Row, Timestamp, Diff)` and error streams.
It wraps the lower-level `shard_source` from `mz_persist_client`, adding storage-specific logic for MFP pushdown, stats-based part filtering, and txn-wal integration.
`persist_source`, `persist_source_core`, and `decode_and_mfp` are generic over an error type `E` that must implement `From<DataflowError>` and `From<EvalError>` (in addition to `timely::ExchangeData + Ord + Clone + Debug`), allowing callers to choose how errors are represented on the output stream.
Defines `Subtime`, an opaque sub-timestamp token that enables finer-grained frontier progress within a single timestamp, used for granular backpressure flow control.
`Subtime` implements `differential_dataflow::lattice::Lattice` (join as max, meet as min), `Maximum` (returning `u64::MAX`), and `columnation::Columnation` (using `CopyRegion<Subtime>`), allowing it to participate in differential dataflow lattice operations and columnar arrangements.
The `persist_source_core` function handles the inner scoped dataflow, wiring up `shard_source`, optional `txns_progress`, and the `decode_and_mfp` operator that deserializes `SourceData` and applies remaining MFP logic.
The `decode_and_mfp` operator's inner processing loop audits parts that were selected for `filter_pushdown_audit`: for `SourceData(Ok(row))` rows, if the MFP produces output the part should not have been discarded; for `SourceData(Err(_))` rows, the mere presence of an error row is a pushdown violation since errors must surface regardless of any filter. Both cases log a Sentry-tagged error (redacting the MFP output and the error value to avoid leaking user data) and optionally panic when `STATS_AUDIT_PANIC` is set.
A `filter_pushdown_audit` proptest module in the `#[cfg(test)]` block provides end-to-end soundness checks for persist filter pushdown: it builds a part from actual rows, computes the real production column statistics, then asserts that `filter_result` never returns `FilterResult::Discard` for a part whose MFP produces output on some actual row. This exercises the full path from stats derivation through `col_stats` to the interpreter, catching both interpreter bugs and stats-derivation bugs. The module also includes targeted regression tests via `assert_part_kept`, which verifies that a specific predicate keeps a part when real rows match it; one such test covers the case where a float column's `PrimitiveStats` bounds are unordered (e.g., `-NaN` as the lower bound in arrow total order, which sorts above finite values in `OrderedFloat` order), ensuring those bounds are not misread as an empty range and incorrectly discarding the part.
