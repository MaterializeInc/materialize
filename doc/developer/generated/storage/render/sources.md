---
source: src/storage/src/render/sources.rs
revision: e79a6d96d9
---

# mz-storage::render::sources

Implements `render_source`, the generic function that composes source reading, reclocking, decoding, envelope processing, and persist-sink writing into a complete ingestion dataflow fragment for a single `IngestionDescription`.
It dispatches on the `SourceEnvelope` (None, Upsert, CDCv2) to apply the appropriate post-decode transformation, then splits the multiplexed output by export ID and passes each stream to `persist_sink::render`.
Returns a map from export ID to ok/error collections, health streams, and lifecycle tokens.
