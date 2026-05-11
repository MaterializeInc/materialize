# upsert-tombstone-removes-key

## Summary

A `(key, null)` tombstone message eventually removes the key from the UPSERT source, and the key stays absent until a non-null value is produced for it.

## Code paths

- `src/storage/src/render/sources.rs` — `upsert_commands` maps `None` value → tombstone signal: `(UpsertKey, None, from_time)`.
- `src/storage/src/upsert.rs` — `upsert_classic`: on `None` value with existing prior value, emit retraction at new timestamp and `multi_put(key, tombstone)`.
- `src/storage/src/upsert/types.rs` — `StateValue::tombstone()` constructor; `ensure_decoded` with `diff_sum == 0` produces this state.

## How to check it

Workload procedure:
1. Produce `(key, v)` to topic.
2. Wait for source to ingest it; verify row visible.
3. Produce `(key, null)`.
4. After quiet period, `assert_always!(tombstoned_key_absent, "upsert: tombstoned key has no row")` checking `SELECT count(*) FROM source WHERE key = ? = 0`.
5. Bonus: kill clusterd, restart, assert the row is still absent (no resurrection).

## What goes wrong on violation

A deleted row reappears after restart. Compliance and correctness hazard. The likely cause is the snapshot replay misinterpreting a tombstone consolidating state — the `diff_sum == 0` branch of `ensure_decoded` is what guards this.

## Antithesis angle

- Crash between tombstone retraction emit and `multi_put(tombstone)`. The state store is now ahead/behind the persisted output; the snapshot replay on restart is what reconciles.
- Race `(k, v)`, `(k, null)`, `(k, v')` deliveries: every interleaving must end with `v'` visible.
- For the no-resurrection half: produce tombstone, wait for `offset_committed` to advance past its offset, then kill clusterd. On restart, the key must not reappear.

## Existing instrumentation

None. Workload-side check. The `StateValue::tombstone` construction path and the `ensure_decoded` tombstone branch are the relevant code; adding `assert_sometimes!(tombstone_emitted, ...)` inside the tombstone-emit path gives a coverage signal.

## Provenance

Surfaced by: Data Integrity, Lifecycle Transitions (delete operations).
