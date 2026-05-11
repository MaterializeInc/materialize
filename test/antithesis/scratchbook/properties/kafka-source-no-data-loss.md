# kafka-source-no-data-loss

## Summary

Every Kafka message produced by the workload is eventually visible in the source — either as a row (NONE envelope) or as the latest value for its key (UPSERT envelope).

## Why this property

This is the headline guarantee of a streaming database. The previous catalog entry `source-ingestion-progress` covered the generic "frontier advances" liveness signal; this property is the Kafka-specific, workload-checkable version that compares produced records against `SELECT` output.

## Code paths

- `src/storage/src/source/kafka.rs` — `render_reader`: the reader loop that drains `PartitionQueue`s, deduplicates against `last_offsets`, and emits `(SourceMessage, KafkaTimestamp, +1)` triples.
- `src/storage/src/source/source_reader_pipeline.rs` — `create_raw_source`: assembles reader, remap, reclock.
- `src/storage/src/source/reclock.rs` — `ReclockOperator::mint`: binds source timestamps to Materialize timestamps and persists the binding via `compare_and_append` on the remap shard.
- `src/storage/src/render/persist_sink.rs` — `mint_batch_descriptions` → `write_batches` → `append_batches`: the path that actually puts rows into the source's data persist shard.
- For UPSERT: `src/storage/src/upsert.rs` (`upsert_classic`) and the continual-feedback variants in `upsert_continual_feedback*.rs`.

## How to check it

Workload-level:
1. The workload tracks every `(topic, partition, offset, key, value)` it produces.
2. After produce settles, the workload calls `ANTITHESIS_STOP_FAULTS` and waits for `mz_internal.mz_source_statistics_per_worker` to report `offset_committed >= max_produced_offset`.
3. The workload asserts via `assert_sometimes!("kafka source caught up to produced offsets", expected_rowcount_visible)` that `COUNT(*) FROM source >= produced_count` (NONE) or that the per-key latest-value model matches the source (UPSERT).

SUT-side anchor: `assert_sometimes!(persist_sink_appended_batch)` inside `append_batches` after the first successful `compare_and_append` for this source.

## What goes wrong on violation

Silent data loss: the source ingests fewer rows than were produced; the workload sees a stall that doesn't resolve even with faults paused. Downstream MVs see incomplete data.

## Antithesis angle

The interesting window is mid-batch crash: a clusterd kill between the persist sink's `write_batches` (which uploads parts) and `append_batches` (which compare-and-appends). The resume frontier on restart determines what gets re-read. Bugs here look like: wrong resume offset (commit history: kafka.rs:1158 dedup is per-incarnation only — across restart, idempotency depends on persist-sink correctness).

## Existing instrumentation

None. No `assert_sometimes!` in the source path today (verified against `existing-assertions.md`). To implement: add an `assert_sometimes!` in the persist sink's `append_batches` after a successful append, plus a workload-side `assert_sometimes!` after the quiet-period catch-up check.

## Provenance

Surfaced by: Data Integrity, Failure Recovery, Product Context.
