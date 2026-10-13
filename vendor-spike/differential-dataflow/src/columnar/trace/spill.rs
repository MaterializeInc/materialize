//! Per-worker spill control for the columnar `Chunk` trace.
//!
//! [`ColChunk`](super::ColChunk) is either resident (a trie in
//! memory) or paged (resident bounds + a byte handle to fetch the trie back).
//! `Chunk::settle` is where committed chunks may be **paged out**: it calls
//! [`try_page`], which consults a per-worker [`SpillState`] and, when over budget,
//! serializes the trie and hands the bytes to a pluggable [`BytesStore`]. Reads
//! ([`Chunk::merge`](crate::trace::chunk::Chunk::merge) etc., and the cursor) fetch paged chunks back on demand.
//!
//! The backend is pluggable: a worker calls [`install`] with its own
//! [`BytesStore`] (e.g. file-backed). With nothing installed, no chunk is ever
//! paged and `ColChunk` stays resident — zero overhead beyond a thread-local peek.
//!
//! # Known limitation
//!
//! `settle` sees only its **local** committed output, not the whole batcher
//! queue (timely's `MergeBatcher` does not expose it, and the shared `settle` is
//! generic so it can't notify a columnar-specific accountant per consumed chunk).
//! So the eviction policy is a simple per-worker **high-water mark**: keep the
//! first `budget_records` worth of committed chunks resident, page the rest. It
//! bounds the resident set and round-trips through the store correctly, but it
//! favours keeping older data and can't reclaim budget as data leaves — coarser
//! than the exact head-reserve-over-the-queue policy the old bespoke batcher ran.

use std::cell::RefCell;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};

use crate::columnar::layout::ColumnarUpdate as Update;
use crate::columnar::updates::{Updates, UpdatesTyped};

/// Append a chunk's bytes to backing storage, returning a handle to read them back.
pub trait BytesStore {
    /// Persist `bytes`, returning a source that can reproduce them.
    fn store(&mut self, bytes: &[u8]) -> Box<dyn BytesSource>;
}

/// A handle to bytes previously written via a [`BytesStore`].
pub trait BytesSource {
    /// Read the bytes back. Panics on failure (the trace cannot make progress without them).
    fn load(&self) -> Vec<u8>;
}

/// Cumulative spill counters, shared (via `Arc`) so a run can sum across workers.
#[derive(Default)]
pub struct SpillStats {
    /// Chunks paged out.
    pub spilled_chunks: AtomicUsize,
    /// Records (updates) in paged-out chunks.
    pub spilled_records: AtomicUsize,
    /// Chunks fetched back in.
    pub fetched_chunks: AtomicUsize,
    /// Serialized bytes written.
    pub bytes_written: AtomicUsize,
}

/// Per-worker spill state: the budget, the running resident estimate, the store,
/// and counters.
struct SpillState {
    /// Keep up to this many records resident before paging committed chunks.
    budget_records: usize,
    /// Records kept resident so far — a monotonic high-water mark. Once it reaches
    /// `budget_records`, every further committed chunk is paged (see the module's
    /// "Known limitation").
    resident_records: usize,
    /// Pluggable backing store.
    store: Box<dyn BytesStore>,
    /// Counters, shared with the installer.
    stats: Arc<SpillStats>,
}

thread_local! {
    static STATE: RefCell<Option<SpillState>> = const { RefCell::new(None) };
}

/// Install spill on this worker: page committed chunks once resident records
/// exceed `budget_records`, writing to `store`. `stats` receives the counters.
pub fn install(budget_records: usize, store: Box<dyn BytesStore>, stats: Arc<SpillStats>) {
    STATE.with(|s| *s.borrow_mut() = Some(SpillState { budget_records, resident_records: 0, store, stats }));
}

/// Remove this worker's spill state (so later traces stay resident).
pub fn uninstall() {
    STATE.with(|s| *s.borrow_mut() = None);
}

/// Whether a spiller is installed on this worker (a cheap peek `settle` makes
/// before computing any per-chunk paging metadata).
pub(crate) fn active() -> bool {
    STATE.with(|s| s.borrow().is_some())
}

/// Try to page `updates` out. On success returns a [`BytesSource`] handle to fetch
/// it back; otherwise returns the trie to keep resident.
///
/// Policy: a **high-water mark** — keep the first `budget_records` worth of
/// committed chunks resident, page everything after. Simple and robust (the
/// counter only rises, so paging is deterministic once the budget is reached);
/// it does not reclaim budget when data later leaves the batcher, since
/// `settle` only sees its local output (see the module-level limitation).
pub(crate) fn try_page<U: Update>(updates: UpdatesTyped<U>) -> Result<Box<dyn BytesSource>, UpdatesTyped<U>> {
    STATE.with(|s| {
        let mut guard = s.borrow_mut();
        let Some(state) = guard.as_mut() else { return Err(updates) };
        let records = updates.len();
        if state.resident_records + records <= state.budget_records {
            state.resident_records += records;
            return Err(updates);
        }
        // Over the high-water mark: serialize and page out.
        let mut buf = Vec::new();
        Updates::<U>::from(updates).write_to(&mut buf);
        state.stats.spilled_chunks.fetch_add(1, Relaxed);
        state.stats.spilled_records.fetch_add(records, Relaxed);
        state.stats.bytes_written.fetch_add(buf.len(), Relaxed);
        Ok(state.store.store(&buf))
    })
}

/// A paged chunk was fetched back in; bump the counter (for stats).
pub(crate) fn note_fetched() {
    STATE.with(|s| {
        if let Some(state) = s.borrow_mut().as_mut() {
            state.stats.fetched_chunks.fetch_add(1, Relaxed);
        }
    });
}

/// Reconstruct a trie from bytes a [`BytesSource`] produced (the inverse of
/// `try_page`'s serialization).
pub(crate) fn decode<U: Update>(source: &dyn BytesSource) -> UpdatesTyped<U> {
    let bytes = timely::bytes::arc::BytesMut::from(source.load()).freeze();
    Updates::<U>::read_from(bytes).into_typed()
}
