---
source: src/interchange/src/envelopes.rs
revision: 85fe1c5a51
---

# interchange::envelopes

Provides `for_each_diff_pair`, a function that walks a single arrangement batch and invokes a callback for each `DiffPair` at each `(key, timestamp)`.
Within a key, updates are partitioned by sign into retractions (befores) and insertions (afters), sorted by timestamp, and zipped into `DiffPair`s via a merge-join; pairs are emitted in ascending timestamp order per key.
Also defines `dbz_envelope` (wraps column types in a `before`/`after` record) and `dbz_format` (packs a `DiffPair<Row>` as Debezium before/after nullables), plus the static `ENVELOPE_CUSTOM_NAMES` map used to give stable Avro type names to transaction and row record types.
