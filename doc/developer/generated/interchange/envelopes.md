---
source: src/interchange/src/envelopes.rs
revision: 2d842fe6d1
---

# interchange::envelopes

Provides two functions for walking arrangement batches and emitting `DiffPair`s:

* `for_each_diff_pair` — walks a batch and invokes a callback for each `DiffPair` at each `(key, timestamp)`. Thin wrapper around `iter_diff_pairs`.
* `iter_diff_pairs` — walks a batch and emits, for each key, the `DiffPair`s at each timestamp. Accepts optional inclusive lower and exclusive upper time bounds; updates outside the range are ignored. Keys with no updates in range are omitted entirely. The per-key iterator owns its data and can be held or consumed after the outer iterator has advanced. An update with multiplicity `n` fans out into `n` pairs lazily, cloning the value as pairs are consumed; memory held per key is proportional to the number of distinct updates, not the fan-out. No ordering is guaranteed across keys.

Both functions are generic over a batch type `B` (bounded by `BatchReader<Time = C::Time> + Navigable<Cursor = C>`) and an explicit cursor type `C` (bounded by `Cursor<Storage = B, Diff = Diff>`). Within a key, updates are partitioned by sign into retractions (befores) and insertions (afters), sorted by timestamp, and zipped into `DiffPair`s via a merge-join; pairs are emitted in ascending timestamp order per key.
Also defines `dbz_envelope` (wraps column types in a `before`/`after` record) and `dbz_format` (packs a `DiffPair<Row>` as Debezium before/after nullables), plus the static `ENVELOPE_CUSTOM_NAMES` map used to give stable Avro type names to transaction and row record types.
