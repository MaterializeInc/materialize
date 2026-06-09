# upsert-key-reflects-latest-value

## Summary

At a settled timestamp, every key in an UPSERT-envelope source maps to the value from the last `(key, value)` message produced — or to no row if the last message for that key was a tombstone.

## Code paths

- `src/storage/src/render/sources.rs` — `upsert_commands` converts `DecodeResult` into `(UpsertKey, Option<UpsertValue>, FromTime)`. `UpsertKey` is a SHA-256 of the key bytes (collision probability `2^-128`).
- `src/storage/src/upsert.rs` — `upsert_classic`: the main operator. For each input update at `from_time`:
  1. `multi_get(key)` → returns prior value + prior order key.
  2. Skip if `from_time <= prior_order` (stale update).
  3. Emit retraction of prior value at the new timestamp.
  4. Emit insertion of new value at the new timestamp.
  5. `multi_put(key, new_value)` updates the state store.
- `src/storage/src/upsert_continual_feedback.rs` and `_v2.rs` — alternative implementations driven by persist feedback. Same contract, different consolidation strategy.
- `src/storage/src/upsert/types.rs` — `StateValue::ensure_decoded` (~line 589) finalizes the XOR-checksum consolidating state into either a `Value` or a `tombstone`. Critical for snapshot replay correctness.

## How to check it

Workload-level:
1. Workload tracks `expected_state: Map<Key, Option<Value>>` of what was last produced per key.
2. After fault quiet period, for a sampled set of keys: `SELECT value FROM source WHERE key = ?` and compare to `expected_state[key]`.
3. `assert_always!(upsert_value_matches_latest_produced, "upsert: key value matches latest produced")` — checked on every sample. If the workload notices a divergence, it logs the diff (expected vs. observed) for replay.

## What goes wrong on violation

The source returns a stale value for a key. The user's downstream MV uses it. The bug is invisible until someone manually compares the source to the upstream system.

## Antithesis angle

- Crash clusterd between `multi_get` and `multi_put`. The next incarnation must reconstruct state correctly from feedback.
- Race produce ordering: if Kafka delivers `(k, v1)` then `(k, v2)`, the source's order-key tracking must serialize them. Order-key regression caused a historical panic (commit `f177db8286`, materialize#26655).
- For RocksDB backend: race `multi_put` against the merge operator running async.
- For multi-replica: both replicas process the same key concurrently (commit `1accbe28b3`).

## Open question (resolved)

Q: Does the workload need to know about the per-source `order_key` to validate, or is `from_time` ordering sufficient?

A: For correctness asserting at quiet periods, the workload only needs the *Kafka* produce order — the operator's job is to translate that into the correct visible value. Since Antithesis injects faults but doesn't reorder Kafka's per-partition delivery, the workload can rely on per-partition produce order to determine `expected_state`. Cross-partition reordering is not a concern because the workload assigns each key to a fixed partition.

## Existing instrumentation

None. Pure workload-side check. Optional SUT anchor: an `assert_sometimes!(upsert_emit_correct_retraction, …)` inside `upsert.rs` after a retraction is emitted whose prior value matched what was stored — this gives Antithesis a positive signal that the prior-value-lookup path is being exercised.

## Implementation status

Implemented 2026-05-11 as `test/antithesis/workload/test/parallel_driver_upsert_latest_value.py`. Three assertion messages, each unique:

| Message | Type | When |
|---------|------|------|
| `"upsert: SELECT for key matches latest produced value"` | `always` | Per sampled live key after quiet-period catchup |
| `"upsert: tombstoned key has no row in source"` | `always` | Per sampled key whose last produced message was a tombstone |
| `"upsert: source caught up to produced offsets after quiet period"` | `sometimes` | Once per invocation; liveness anchor proving the safety assertions ran against settled data |

Shared helpers introduced for this property and reusable by every subsequent Kafka source property: `helper_pg.py` (resilient pgwire), `helper_kafka.py` (producer + delivery tracker), `helper_quiet.py` (`ANTITHESIS_STOP_FAULTS` wrapper), `helper_random.py` (deterministic randomness with Antithesis SDK), `helper_source_stats.py` (catchup polling on `mz_internal.mz_source_statistics`), `helper_upsert_source.py` (idempotent `CREATE CONNECTION` + `CREATE SOURCE`).

No SUT-side instrumentation added in this pass — that is the candidate work in `properties/upsert-no-internal-panic.md`, `properties/upsert-state-consolidation-wellformed.md`, and `properties/upsert-ensure-decoded-called-before-access.md`.

## Provenance

Surfaced by: Data Integrity, Concurrency. Direct regression target for materialize#26655.
