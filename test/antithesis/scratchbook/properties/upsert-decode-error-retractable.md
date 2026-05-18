# upsert-decode-error-retractable

## Summary

An `UpsertError` (key decode failure, null key, or value decode failure) for a key is retracted once a subsequent valid `(key, value)` message for the same key is ingested. After settling, the source reflects the corrected value and contains no remaining error row for that key.

This is the upsert envelope's recovery contract for upstream schema mistakes — "fix the bad message and continue" without dropping the source.

## Code paths

- `src/storage/src/render/sources.rs` — `upsert_commands` (line ~509-560 and following): maps decode failures to `UpsertError::NullKey` / `KeyDecode` / `Value`. The result still flows through the upsert pipeline keyed by `UpsertKey::from_key(Err(&err))` so a future good value can retract it.
- `src/storage-types/src/errors.rs:161-199` — `EnvelopeError::Upsert(UpsertError)` is the *retractable* error variant. `EnvelopeError::Flat(text)` is explicitly *not retractable*.
- `src/storage/src/upsert.rs:748-750` — error emission paths.

## How to check it

Workload procedure:
1. Produce a malformed message for key `K` (e.g., invalid Avro under a schema-registry-backed source, or null key on a non-null-key source).
2. Verify the source contains an error row keyed by `K`.
3. Produce a valid `(K, value)` message.
4. After quiet period, `assert_always!(upsert_error_retracted, "upsert: bad value retracted by subsequent good value")` checking that `SELECT * FROM source WHERE key = K` returns exactly one row with `value`, no error row.

## What goes wrong on violation

If the error is not retractable, the source carries a stuck error row that nothing can clear — the only recovery is to drop and re-create the source.

## Distinguishing retractable from non-retractable

This property targets `EnvelopeError::Upsert(_)` only. `EnvelopeError::Flat(_)` is explicitly non-retractable and should not be tested with this property. Workloads must take care to produce errors that map to the Upsert variant — null key, malformed key/value under upsert mode — rather than envelope-fatal errors.

## Antithesis angle

- Race the bad and good messages closely. Verify ordering is preserved.
- Crash clusterd between the bad message ingesting and the good message ingesting. The error row must persist across the restart and the good message must retract it on resume.

## Existing instrumentation

None. Workload-side check.

## Provenance

Surfaced by: Protocol Contracts, Failure Recovery.
