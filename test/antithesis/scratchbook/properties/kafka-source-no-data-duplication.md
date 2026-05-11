# kafka-source-no-data-duplication

## Summary

After settling, the source contains no duplicates — at most one row per `(partition, offset)` for NONE-envelope and at most one row per key for UPSERT-envelope.

## Why this property

Duplication is the symmetric failure mode to `kafka-source-no-data-loss`. It is silent, propagates into every downstream aggregate, and historically arose in the upsert operator under multi-replica drain (commit `1accbe28b3`, database-issues#9160). It is the more dangerous of the two failure modes because it is harder to detect operationally — the workload sees "extra" rows that look plausible.

## Code paths

- `src/storage/src/source/kafka.rs:1158` — per-incarnation dedup against `last_offsets` (drops messages with offset `<= last_offset`). Per-incarnation only; does not survive restart.
- `src/storage/src/render/persist_sink.rs` — the persist sink is responsible for ensuring writes are idempotent across restarts. Compare-and-append with idempotency tokens on retry handles the indeterminate-error case (compare with `idempotent-write-under-indeterminate`).
- `src/storage/src/upsert_continual_feedback.rs` — `drain_staged_input`: the regression target for commit `1accbe28b3`. Single-replica clusters masked the bug because capabilities were always singletons; multi-replica drained the same staged input twice.
- `src/storage/src/upsert.rs:541`, `upsert_continual_feedback*.rs` — `assert!(diff.is_positive(), "invalid upsert input")`. Retractions on the input would be the canonical "duplicate retraction" symptom.

## How to check it

Workload-level:
- NONE envelope: `SELECT partition, "offset", COUNT(*) FROM source GROUP BY 1, 2 HAVING COUNT(*) > 1` returns 0 rows. Assert with `assert_always!(no_dupes, "kafka source: no duplicate (partition, offset)")`.
- UPSERT envelope: `SELECT key, COUNT(*) FROM source GROUP BY 1 HAVING COUNT(*) > 1` returns 0 rows. Same assertion shape with a unique message.

These run on every check fire, ideally on a polling cadence, not just at end-of-test.

SUT-side: convert the existing `assert!(diff.is_positive(), "invalid upsert input")` into `assert_always!(diff.is_positive(), "upsert: input diff positive")` so a duplicate retraction surfaces as a property failure rather than a process abort. Distinct messages at each of the three callsites.

## What goes wrong on violation

Aggregates over the source double-count. Joins fan out. Downstream MVs become wrong in ways that are hard to attribute to ingestion.

## Antithesis angle

- Crash storage worker between `write_batches` and `append_batches`. Restart and verify that no `(partition, offset)` appears twice in the resulting persist shard.
- For UPSERT: multi-replica cluster topology (the historical bug requires it). Run two replicas on the same source and observe the persisted output for duplicate retractions.
- Race the upsert feedback-driven snapshot replay against new input.

## Existing instrumentation

The runtime `assert!` in upsert.rs already aborts on negative input diffs — it just doesn't surface as an Antithesis property. Wrapping each callsite with `assert_always!` (per-site unique message) gives Antithesis the signal it needs without changing semantics outside Antithesis (the underlying `assert!` already aborts on violation).

## Provenance

Surfaced by: Data Integrity, Concurrency, Failure Recovery. Direct regression target for database-issues#9160.
