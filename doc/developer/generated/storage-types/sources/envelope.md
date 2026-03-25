---
source: src/storage-types/src/sources/envelope.rs
revision: db271c31b1
---

# storage-types::sources::envelope

Defines `SourceEnvelope` (None, Upsert, CdcV2) and supporting types `NoneEnvelope`, `UpsertEnvelope`, `KeyEnvelope`, and `UpsertStyle`.
The envelope determines how a raw message stream is converted into a differential (data, time, diff) stream.
`UnplannedSourceEnvelope::into_source_envelope` resolves the envelope together with a `RelationDesc` once key arity is known.
