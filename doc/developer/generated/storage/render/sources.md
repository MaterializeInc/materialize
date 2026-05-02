---
source: src/storage/src/render/sources.rs
revision: 3da45d073d
---

# mz-storage::render::sources

Implements `render_source`, the generic function that composes source reading, reclocking, decoding, envelope processing, and persist-sink writing into a complete ingestion dataflow fragment for a single `IngestionDescription`.
It dispatches on the `SourceEnvelope` (None, Upsert, CDCv2) to apply the appropriate post-decode transformation, then splits the multiplexed output by export ID and passes each stream to `persist_sink::render`.
For the Upsert envelope, the operator is selected at runtime based on the `ENABLE_UPSERT_V2` dyncfg: when enabled, `upsert::upsert_v2` is used; otherwise the original `upsert::upsert` is called.
Returns a map from export ID to ok/error collections, health streams, and lifecycle tokens.
