---
source: src/storage/src/render/sources.rs
revision: 844b947128
---

# mz-storage::render::sources

Implements `render_source`, the generic function that composes source reading, reclocking, decoding, envelope processing, and persist-sink writing into a complete ingestion dataflow fragment for a single `IngestionDescription`.
It dispatches on the `SourceEnvelope` (None, Upsert, CDCv2) to apply the appropriate post-decode transformation, then splits the multiplexed output by export ID and passes each stream to `persist_sink::render`.
For the Upsert envelope, the operator is selected at runtime based on the `ENABLE_UPSERT_V2` dyncfg: when enabled, `upsert::upsert_v2` is used; otherwise the original `upsert::upsert` is called. When upsert-v2 is selected, the `UpsertStashFlavor` is resolved from `ENABLE_UPSERT_CHUNKED_STASH` at operator construction time so the dataflow keeps one stash flavor for its whole life even if the flag flips later.
Returns a map from export ID to ok/error collections, health streams, and lifecycle tokens.
