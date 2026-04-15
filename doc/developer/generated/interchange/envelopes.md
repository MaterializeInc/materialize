---
source: src/interchange/src/envelopes.rs
revision: b0fa98e931
---

# interchange::envelopes

Provides the `combine_at_timestamp` Timely operator, which groups a differential dataflow arranged collection's updates by key and timestamp into `Vec<DiffPair>` suitable for Debezium/upsert sinks.
Also defines `dbz_envelope` (wraps column types in a `before`/`after` record) and `dbz_format` (packs a `DiffPair<Row>` as Debezium before/after nullables), plus the static `ENVELOPE_CUSTOM_NAMES` map used to give stable Avro type names to transaction and row record types.
