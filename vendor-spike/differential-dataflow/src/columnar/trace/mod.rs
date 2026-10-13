//! The TRACE face of the columnar subsystem: the columnar data arranged as a
//! differential-dataflow trace.
//!
//! Under the hood this is a [`Chunk`](crate::trace::chunk::Chunk) implementation
//! ([`ColChunk`], an `UpdatesTyped` trie, resident or paged), plugged into the
//! generic chunk harness. The exported names are the BATCH/TRACE vocabulary DD
//! arranges with — [`Spine`], [`Batcher`], [`Builder`], [`Chunker`] — so callers
//! work in trace terms, not chunk-implementation terms.
//!
//! - [`chunk`] — `ColChunk`: the `Chunk` impl (cursor + merge/extract/advance/settle).
//! - [`chunker`] — melds [`RecordedUpdates`](super::collection::RecordedUpdates)
//!   streams into `ColChunk` batches.
//! - [`spill`] — per-worker page-out control (`Chunk::settle`'s spill hook).

pub mod chunk;
pub mod chunker;
pub mod spill;

pub use chunk::ColChunk;

/// The columnar trace: a spine of `Rc`-shared [`ColChunk`] batches.
pub type Spine<K, V, T, R> = crate::trace::chunk::ChunkSpine<ColChunk<(K, V, T, R)>>;
/// The columnar merge batcher (the chunk harness's `MergeBatcher` over `ColChunk`).
pub type Batcher<K, V, T, R> = crate::trace::chunk::ChunkBatcher<ColChunk<(K, V, T, R)>>;
/// The columnar batch builder.
pub type Builder<K, V, T, R> = crate::trace::chunk::ChunkBuilder<ColChunk<(K, V, T, R)>>;
/// The input chunker: melds `RecordedUpdates<U>` streams into `ColChunk<U>` batches.
pub type Chunker<U> = chunker::TrieChunker<U>;
