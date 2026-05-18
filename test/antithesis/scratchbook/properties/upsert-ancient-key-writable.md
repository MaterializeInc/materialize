# upsert-ancient-key-writable

## Summary

After a key has been resident in an UPSERT-envelope source for a long time (many invocations, many fault windows, many materialized restarts), writing a fresh `(key, value)` to that key must still update the source's view of it.

The bug class this catches: an upsert state-store rehydration regression that "remembers" a key's old value with enough fidelity to serve reads but enough wrongness that fresh writes are silently dropped, leaving the source stuck at a value it shouldn't be at.

## Why this isn't just `upsert-key-reflects-latest-value`

`upsert-key-reflects-latest-value` is verified per invocation: each invocation writes its own short-lived keys, settles, asserts. The keys it touches were freshly created at the start of that invocation. The rehydration / long-resident-state code paths are exercised only incidentally, when an Antithesis fault lands between a produce and a check inside one invocation.

`upsert-state-rehydrates-correctly` covers explicit clusterd-restart rehydration but only over a single invocation's worth of state, and only for read-after-rehydrate, not write-after-rehydrate.

This property covers the gap: long-resident keys plus fresh writes against them.

## Code paths

- `src/storage/src/upsert.rs` — `upsert_classic` operator. The `multi_get` → check `from_time > prior_order` → `multi_put` sequence must work the same whether `multi_get` returns a value the worker freshly observed in this incarnation or one rehydrated from persist state at startup.
- `src/storage/src/upsert/types.rs` — `StateValue::ensure_decoded` finalizes consolidating state into either a `Value` or a tombstone. If `ensure_decoded` ever yields a stale value mismatching the persist state, this property will surface it after the next write.
- `src/storage/src/upsert_continual_feedback.rs` — same contract, persist-feedback flavor.

## How to check it

Workload procedure (per invocation):
1. Pick K=5 keys at random from a fixed ring of N=32 keys owned by this driver: `ancient-k{0..31}`.
2. For each picked key, `SELECT text FROM source WHERE key = ?` at real-time recency. Record `old_value` (which may be `NULL` if no prior invocation wrote that ring slot yet).
3. Produce a fresh value `cross-<my_prefix>-<nonce>` to each picked key on Kafka.
4. Request a quiet period and wait for `offset_committed` to reach the produced max offset.
5. Re-query each key. Assert:
   - If `old_value` was present: post-catchup the source's view must NOT equal `old_value`. Race-tolerant against concurrent peers writing or tombstoning the same key — those outcomes also change the value, and only "row still has the exact old value while no one else touched it" indicates our write was silently dropped.
   - If `old_value` was absent (first-time write to that ring slot): post-catchup a row must exist for the key.

## What goes wrong on violation

A write to a long-resident upsert key is silently dropped: Kafka acked the produce, materialize ingested the message (`offset_committed` advanced), but the upsert state did not update. Read-only paths still return the old value; the user sees their pipeline "stuck" with no error.

## Antithesis angle

The interesting time window is the time between the ring slot's most recent write and the next one. In a long Antithesis run, that window spans many fault injections, clusterd restarts, and materialize-driven rehydrations of the upsert source's persist state. The longer the run, the more genuinely "ancient" the prior value is when we revisit.

Combine with:
- Node-termination faults — exercises the rehydration path between writes to the same ring slot.
- Network-partition faults between materialized and clusterd-pool members — exercises feedback-channel recovery.
- Long-lived runs (multi-hour) — gives time for many ring-slot revisits with intervening faults.

## Dependencies

- The fixed ring `ancient-k{0..31}` is namespaced away from any sibling driver's keys, so this driver doesn't interfere with `upsert-key-reflects-latest-value`'s assertions.
- Two concurrent invocations of THIS driver picking the same ring slot is the race the `always` assertion is designed to tolerate. The `sometimes` clause "our specific new value reached the source" still fires when one invocation wins.

## Existing instrumentation

None. Candidate SUT anchors: `assert_sometimes!(upsert_long_resident_key_written, ...)` at the `multi_put` site, conditioned on the key's `from_time` being at least N minutes behind wall clock, would confirm the property's specific path is exercised. Deferred.

## Implementation status

Implemented as `test/antithesis/workload/test/parallel_driver_upsert_ancient_key_writable.py`.

| Message | Type | Fires when |
|---------|------|------------|
| `"upsert: write to ancient key changes its reflected value"` | `always` | Per ancient ring slot that had a prior value, post-catchup. False ⟺ row still present with the exact pre-write value. |
| `"upsert: write to previously-empty ancient key creates a row"` | `always` | Per ancient ring slot that was empty before our write, post-catchup. False ⟺ no row exists despite our non-null produce + catchup. |
| `"upsert: at least one ancient-ring key has a prior value to overwrite"` | `sometimes` | Per invocation. Confirms the property's interesting path (overwrite, not first-touch) is exercised. |
| `"upsert: source caught up after cross-invocation produces"` | `sometimes` | Per invocation. Liveness for the catchup gate. |
| `"upsert: cross-invocation driver's own write reached the source"` | `sometimes` | Per invocation. Confirms the full write→catchup→read pipeline works end-to-end at least sometimes (most of the time, under low concurrency). |

Knobs: `ANCIENT_KEY_RING_SIZE=32`, `ANCIENT_KEYS_PER_INVOCATION=5`, `QUIET_PERIOD_S=20`, `CATCHUP_TIMEOUT_S=60.0`.

## Provenance

Surfaced by: Data Integrity (long-lived upsert state correctness).
