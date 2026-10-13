//! The container→batch chunker for the columnar trace.
//!
//! [`TrieChunker`] is the `ContainerBuilder` that turns a stream of
//! [`RecordedUpdates`](crate::columnar::collection::RecordedUpdates) into
//! [`ColChunk`](crate::columnar::trace::ColChunk) batches for the `Chunk`
//! harness's batcher. It accumulates small inputs, consolidates, and ships
//! chunks sized within 1-2x [`LINK_TARGET`](crate::columnar::LINK_TARGET).

use super::ColChunk;

use crate::columnar::updates::UpdatesTyped;
use crate::columnar::collection::RecordedUpdates;

/// A chunker that unwraps `RecordedUpdates` into bare `UpdatesTyped` for the merge batcher.
///
/// The intended behavior is to produce chunks whose size is within 1-2x `LINK_TARGET`.
/// It ships large batches immediately, accumulates small batches, consolidates as they
/// exceed 2xLINK_TARGET, and ships them unless they drop below 1xLINK_TARGET.
///
/// The flow is into (or around) `self.stage`, then consolidated blocks into `self.ready`,
/// each of which is put in `self.stage`
pub struct TrieChunker<U: crate::columnar::layout::ColumnarUpdate> {
    /// Insufficiently large updates we haven't figured out how to ship yet.
    blobs: Vec<(UpdatesTyped<U>, bool)>,
    /// Sum of `len()` across `blobs`.
    blob_records: usize,
    /// Ready-to-emit chunks. Each is sorted and consolidated; size ≥ `LINK_TARGET`
    /// (or smaller, only for the final chunk produced by `finish`).
    ready: std::collections::VecDeque<UpdatesTyped<U>>,
    /// Staging area for the next pull call: the ready trie wrapped as a `ColChunk`.
    stage: Option<ColChunk<U>>,
}

impl<U: crate::columnar::layout::ColumnarUpdate> Default for TrieChunker<U> {
    fn default() -> Self {
        Self {
            blobs: Default::default(),
            blob_records: 0,
            ready: Default::default(),
            stage: None,
        }
    }
}

impl<U: crate::columnar::layout::ColumnarUpdate> TrieChunker<U> {
    /// Consolidate and empty `self.blobs`, into `self.ready` if large enough or else return.
    fn consolidate_blobs(&mut self) -> UpdatesTyped<U> {
        // Single consolidated entry: pass through, no work.
        if self.blobs.len() == 1 && self.blobs[0].1 {
            let (result, _) = self.blobs.pop().unwrap();
            self.blob_records = 0;
            return result;
        }

        // TODO: Improve consolidation through column-oriented sorts.
        let result = UpdatesTyped::<U>::form_unsorted(self.blobs.iter().flat_map(|(u, _)| u.iter()));
        self.blobs.clear();
        self.blob_records = 0;
        result
    }

    /// Push a non-empty `UpdatesTyped` into blobs and update accounting.
    fn absorb(&mut self, updates: UpdatesTyped<U>, consolidated: bool) {
        self.blob_records += updates.len();
        self.blobs.push((updates, consolidated));
    }
}

impl<'a, U: crate::columnar::layout::ColumnarUpdate> timely::container::PushInto<&'a mut RecordedUpdates<U>> for TrieChunker<U> {
    fn push_into(&mut self, container: &'a mut RecordedUpdates<U>) {
        // Early return if an empty container (legit, for accountable progress tracking).
        if container.updates.len() == 0 { return; }

        // Our main goal is to only ship links that are 1-2 x LINK_TARGET, using blobs
        // to accumulate updates until they are ready to go or we are asked to finish.
        //
        // Informally, we are aiming to move `container` into or around `self.blobs`.
        // Into if small enough, as we can further consolidate, but if not we need to
        // consolidate and then either ship (if large) or hold (if small) the results.

        let updates = std::mem::take(&mut container.updates).into_typed();
        let consolidated = container.consolidated;
        let len = updates.len();

        // The input may be ready to ship on its own.
        // This is ideal, if we've used an accumulating container builder elsewhere.
        if consolidated && len >= crate::columnar::LINK_TARGET { self.ready.push_back(updates); }
        // Can move into blobs if the combined length is not too large.
        else if self.blob_records + len < 2 * crate::columnar::LINK_TARGET { self.absorb(updates, consolidated); }
        // Otherwise, we'll need to manage `self.blobs`.
        else {
            // Together `updates` and `self.blobs` exceed 2 * LINK_TARGET.
            // At least one, perhaps both of them, are LINK_TARGET in size.
            // We'll consolidate any that are, and ship or merge the results.
            // We'll end up with at most LINK_TARGET in `self.blobs`, retiring
            // a constant factor of the pending work we started with.

            // Consolidate and move to ready if large; stash otherwise.
            let input_residual = if len >= crate::columnar::LINK_TARGET {
                let cons = if consolidated { updates } else { updates.consolidate() };
                if cons.len() >= crate::columnar::LINK_TARGET { self.ready.push_back(cons); None }
                else if cons.len() > 0 { Some((cons, true)) }
                else { None }
            }
            else { Some((updates, consolidated)) };

            // Consolidate and move to ready if large; stash otherwise.
            let blobs_residual = if self.blob_records >= crate::columnar::LINK_TARGET {
                let cons = self.consolidate_blobs();
                if cons.len() >= crate::columnar::LINK_TARGET { self.ready.push_back(cons); None }
                else if cons.len() > 0 { Some((cons, true)) }
                else { None }
            }
            else { None };

            // Return un-shipped
            if let Some((r, c)) = input_residual { self.absorb(r, c); }
            if let Some((r, c)) = blobs_residual { self.absorb(r, c); }
        }
    }
}

impl<U: crate::columnar::layout::ColumnarUpdate> timely::container::ContainerBuilder for TrieChunker<U> {
    type Container = ColChunk<U>;
    fn extract(&mut self) -> Option<&mut Self::Container> {
        self.stage = self.ready.pop_front().map(ColChunk::from_trie);
        self.stage.as_mut()
    }
    fn finish(&mut self) -> Option<&mut Self::Container> {
        // Drain whatever's left in blobs as a single (possibly small) final chunk.
        if !self.blobs.is_empty() {
            let cons = self.consolidate_blobs();
            if cons.len() > 0 { self.ready.push_back(cons); }
        }
        self.extract()
    }
}
