# upsert-state-consolidation-wellformed

## Summary

`StateValue::ensure_decoded` always finalizes a `Consolidating` cell into either a `Value(value)` (when `diff_sum == 1` and the recovered bytes match the stored `len_sum` and seahash `checksum_sum`) or a `tombstone()` (when `diff_sum == 0` and the entire accumulator is zero). Any other state — non-{0,1} `diff_sum`, mismatched checksum, non-zero residue on a tombstone — is an XOR/accounting corruption and must never be observed.

## Code

`src/storage/src/upsert/types.rs:584-682`:

```rust
pub fn ensure_decoded(&mut self, bincode_opts, source_id, key) {
    match self {
        StateValue::Consolidating(consolidating) => {
            match consolidating.diff_sum.0 {
                1 => {
                    let len = usize::try_from(consolidating.len_sum.0)...expect(...);
                    let value = &consolidating.value_xor.get(..len)...expect(...);
                    assert_eq!(consolidating.checksum_sum.0, seahash::hash(value) as i64, ...);
                    *self = Self::finalized_value(bincode_opts.deserialize(value).unwrap());
                }
                0 => {
                    assert_eq!(consolidating.len_sum.0, 0, ...);
                    assert_eq!(consolidating.checksum_sum.0, 0, ...);
                    assert!(consolidating.value_xor.iter().all(|&x| x == 0), ...);
                    *self = Self::tombstone();
                }
                other => panic!("invalid upsert state: non 0/1 diff_sum: {other}, ..."),
            }
        }
        StateValue::Value(_) => {}
    }
}
```

## Antithesis form

Each of the four assertions in this function becomes a uniquely-messaged `assert_always!`:

| Existing | Antithesis form | Message |
|---|---|---|
| `assert_eq!(checksum_sum, seahash::hash(value))` (621) | `assert_always!(checksum_sum == seahash::hash(value), …)` | `"upsert: consolidating checksum_sum mismatch (diff_sum=1)"` |
| `assert_eq!(len_sum, 0)` (632) | `assert_always!(len_sum == 0, …)` | `"upsert: consolidating len_sum nonzero (diff_sum=0)"` |
| `assert_eq!(checksum_sum, 0)` (637) | `assert_always!(checksum_sum == 0, …)` | `"upsert: consolidating checksum_sum nonzero (diff_sum=0)"` |
| `assert!(value_xor.iter().all(==0))` (642) | `assert_always!(value_xor.iter().all(==0), …)` | `"upsert: consolidating value_xor nonzero (diff_sum=0)"` |
| `panic!("invalid upsert state: non 0/1 diff_sum: {other}, …")` (672) | `assert_always!(false, …)` | `"upsert: consolidating diff_sum not in {0,1}"` |

Plus the two `expect("invalid upsert state")` calls at 606 and 619 (slice-into-bytes failures); these should become `assert_always!(value_xor.len() >= len, …)` with a distinct message.

## What goes wrong on violation

The XOR-based consolidation collapses many `(diff, bytes)` updates per key into a single accumulator. The math only works if every retraction is exactly paired with its insertion. A trip into the non-{0,1} branch indicates one of:

- A duplicate retraction (commit `1accbe28b3` style multi-replica double-drain).
- A retraction without a matching insertion in the replay stream (incomplete feedback delivery across crash).
- A `seahash` collision (negligible probability — if seen, it's a bug elsewhere, not the hash).
- A bug in the `merge_update_state` math (`upsert/types.rs:533+`).

## Antithesis angle

- Kill clusterd mid-feedback-replay; restart and assert that `ensure_decoded` always completes cleanly.
- Multi-replica with concurrent drains feeding the same RocksDB backend.
- Race RocksDB's async merge operator against `multi_put`.

## Why this is the deepest signal

The XOR/checksum consolidation is the *math*: if this assertion ever trips, something upstream — feedback delivery, retraction emission, or order-key tracking — produced an inconsistent update sequence. The signal is high because the assertion is at the *bottom* of the pipeline; everything else has had a chance to introduce the bug, but only this site can detect it.

## Existing instrumentation

The runtime `panic!` and `assert!`s already exist and would abort clusterd on violation. Today, an abort in test looks like "the storage worker crashed" — possibly retried, possibly noticed only via a log scrape. Wrapping them with Antithesis assertions turns each into a reportable, replay-anchored property failure with a unique signature.

## Provenance

Surfaced by: Data Integrity, Concurrency (via the multi-replica drain bug history).
