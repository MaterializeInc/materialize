//! The columnar storage subsystem for differential dataflow.
//!
//! **Experimental.** API and internals are still settling. Expect breaking
//! changes; do not rely on stability across releases.
//!
//! This is a self-contained storage variant: a columnar **data** core plus the
//! two integration faces DD consumes, all derived from the same data. It borrows
//! DD's abstractions (the [`Chunk`](crate::trace::chunk::Chunk) trait, the
//! collection container traits) but could be sheared off without changing DD's
//! own structure — a template for further storage variants.
//!
//! - **DATA** (this module's files) — the shared core:
//!   - [`layout`] — `ColumnarUpdate` / `ColumnarLayout` / `OrdContainer` / `Coltainer`
//!     (the columnar container as a DD `BatchContainer`).
//!   - [`updates`] — `UpdatesTyped<U>` trie, `Consolidating`, `UpdatesBuilder`, byte codec.
//!   - [`trie_merger`] — the trie-native survey/merge core the trace face delegates to.
//! - [`collection`] — the COLLECTION face: `RecordedUpdates` + builder / Pact / operators.
//! - [`trace`] — the TRACE face: `ColChunk` (a `Chunk` impl) + `Spine` / `Batcher` /
//!   `Builder` / `Chunker`.
//!
//! Known rough edge: `ContainerBytes` for `UpdatesTyped` is `unimplemented!()`.
//! The wire-side container is `RecordedUpdates` (in [`collection`]), whose
//! `ContainerBytes` is implemented; `UpdatesTyped` is the input-builder type and
//! isn't shipped over channels.
//!
//! Files that touch both the local module path and the
//! [`columnar`](https://docs.rs/columnar) crate should `use columnar as col;`
//! to disambiguate.

pub mod layout;
pub mod updates;
pub mod trie_merger;

pub mod collection;
pub mod trace;

pub use updates::UpdatesTyped;

/// Target size for update batches, in number of updates. The collection-side
/// input builder and the trace-side `Chunker` aim to ship chunks within 1-2x this.
pub const LINK_TARGET: usize = 64 * 1024;
