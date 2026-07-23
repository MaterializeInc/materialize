// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! An `Arc`-backed batch newtype whose contents can be shared across timely runtimes.
//!
//! Differential's default spines reference-count their batches with `Rc`, which is worker-local.
//! Sharing an arrangement with another runtime (a reader on a different worker thread) needs the
//! batches behind an `Arc` so a batch whose contents are `Send + Sync` can be read from that other
//! thread.
//!
//! The blanket `impl Trait for Arc<B>` that would express this lives outside this crate: both `Arc`
//! and differential's `Batch`/`Builder`/`Merger`/`Cursor` traits are foreign, so the orphan rule
//! forbids it here. [`ArcBatch`] is a local newtype around `Arc<B>` that carries those impls
//! instead. The impls delegate straight through to the inner batch, so `ArcBatch<B>` behaves
//! exactly like `B` except that its handle is atomically reference counted.
//!
//! This mirrors differential's own `rc_blanket_impls` (for `Rc<B>`), swapping in `Arc`. It replaces
//! the fork-side `arc_blanket_impls`, keeping the sharing machinery in Materialize so the vendored
//! differential-dataflow fork is not needed for it.

use std::sync::Arc;

use differential_dataflow::trace::{
    Batch, BatchReader, Builder, Cursor, Description, Merger, Navigable,
};
use timely::progress::{Antichain, frontier::AntichainRef};

/// An `Arc`-backed batch, shareable across threads when `B`'s contents are `Send + Sync`.
///
/// A transparent newtype around `Arc<B>`. Cloning shares the underlying batch, exactly like the
/// `Rc`-backed default, but with atomic reference counting.
pub struct ArcBatch<B>(pub Arc<B>);

// Hand-written rather than derived: `#[derive(Clone)]` would bound `B: Clone`, but `Arc<B>` is
// `Clone` for any `B` (it clones the handle, not the batch). The derived bound would make
// `ArcBatch<B>: Clone` fail for a non-`Clone` batch such as `OrdValBatch`, which in turn breaks
// `Spine<ArcBatch<B>>: TraceReader`.
impl<B> Clone for ArcBatch<B> {
    fn clone(&self) -> Self {
        ArcBatch(Arc::clone(&self.0))
    }
}

impl<B> ArcBatch<B> {
    /// Wraps a batch in an `Arc`.
    pub fn new(batch: B) -> Self {
        ArcBatch(Arc::new(batch))
    }
}

impl<B> std::ops::Deref for ArcBatch<B> {
    type Target = B;
    fn deref(&self) -> &B {
        &self.0
    }
}

impl<B: BatchReader + Navigable> Navigable for ArcBatch<B> {
    type Cursor = ArcBatchCursor<B::Cursor>;
    fn cursor(&self) -> Self::Cursor {
        // Call the inner batch's cursor explicitly. While the differential fork is still
        // `[patch]`-active, `Arc<B>: Navigable` too, so `self.0.cursor()` would resolve to that
        // impl and yield the wrong cursor type. This disambiguates to `B`'s cursor.
        ArcBatchCursor::new(<B as Navigable>::cursor(&self.0))
    }
}

impl<B: BatchReader> BatchReader for ArcBatch<B> {
    type Time = B::Time;
    fn len(&self) -> usize {
        self.0.len()
    }
    fn description(&self) -> &Description<Self::Time> {
        self.0.description()
    }
}

/// Cursor over an [`ArcBatch`], delegating to the inner batch's cursor.
pub struct ArcBatchCursor<C> {
    cursor: C,
}

impl<C> ArcBatchCursor<C> {
    fn new(cursor: C) -> Self {
        ArcBatchCursor { cursor }
    }
}

impl<C: Cursor> Cursor for ArcBatchCursor<C> {
    type Storage = ArcBatch<C::Storage>;

    type Key<'a> = C::Key<'a>;
    type ValOwn = C::ValOwn;
    type Val<'a> = C::Val<'a>;
    type Time = C::Time;
    type TimeGat<'a> = C::TimeGat<'a>;
    type Diff = C::Diff;
    type DiffGat<'a> = C::DiffGat<'a>;
    type KeyContainer = C::KeyContainer;
    type ValContainer = C::ValContainer;
    type TimeContainer = C::TimeContainer;
    type DiffContainer = C::DiffContainer;

    #[inline]
    fn key_valid(&self, storage: &Self::Storage) -> bool {
        self.cursor.key_valid(&storage.0)
    }
    #[inline]
    fn val_valid(&self, storage: &Self::Storage) -> bool {
        self.cursor.val_valid(&storage.0)
    }

    #[inline]
    fn key<'a>(&self, storage: &'a Self::Storage) -> Self::Key<'a> {
        self.cursor.key(&storage.0)
    }
    #[inline]
    fn val<'a>(&self, storage: &'a Self::Storage) -> Self::Val<'a> {
        self.cursor.val(&storage.0)
    }

    #[inline]
    fn get_key<'a>(&self, storage: &'a Self::Storage) -> Option<Self::Key<'a>> {
        self.cursor.get_key(&storage.0)
    }
    #[inline]
    fn get_val<'a>(&self, storage: &'a Self::Storage) -> Option<Self::Val<'a>> {
        self.cursor.get_val(&storage.0)
    }

    #[inline]
    fn map_times<L: FnMut(Self::TimeGat<'_>, Self::DiffGat<'_>)>(
        &mut self,
        storage: &Self::Storage,
        logic: L,
    ) {
        self.cursor.map_times(&storage.0, logic)
    }

    #[inline]
    fn step_key(&mut self, storage: &Self::Storage) {
        self.cursor.step_key(&storage.0)
    }
    #[inline]
    fn seek_key(&mut self, storage: &Self::Storage, key: Self::Key<'_>) {
        self.cursor.seek_key(&storage.0, key)
    }

    #[inline]
    fn step_val(&mut self, storage: &Self::Storage) {
        self.cursor.step_val(&storage.0)
    }
    #[inline]
    fn seek_val(&mut self, storage: &Self::Storage, val: Self::Val<'_>) {
        self.cursor.seek_val(&storage.0, val)
    }

    #[inline]
    fn rewind_keys(&mut self, storage: &Self::Storage) {
        self.cursor.rewind_keys(&storage.0)
    }
    #[inline]
    fn rewind_vals(&mut self, storage: &Self::Storage) {
        self.cursor.rewind_vals(&storage.0)
    }
}

impl<B: Batch> Batch for ArcBatch<B> {
    type Merger = ArcMerger<B>;
    fn empty(lower: Antichain<Self::Time>, upper: Antichain<Self::Time>) -> Self {
        ArcBatch::new(B::empty(lower, upper))
    }
}

/// Builds [`ArcBatch`]es, delegating to the inner batch's builder.
pub struct ArcBuilder<B: Builder> {
    builder: B,
}

impl<B: Builder> Builder for ArcBuilder<B> {
    type Input = B::Input;
    type Time = B::Time;
    type Output = ArcBatch<B::Output>;
    fn with_capacity(keys: usize, vals: usize, upds: usize) -> Self {
        ArcBuilder {
            builder: B::with_capacity(keys, vals, upds),
        }
    }
    fn push(&mut self, input: &mut Self::Input) {
        self.builder.push(input)
    }
    fn done(self, description: Description<Self::Time>) -> ArcBatch<B::Output> {
        ArcBatch::new(self.builder.done(description))
    }
    fn seal(chain: &mut Vec<Self::Input>, description: Description<Self::Time>) -> Self::Output {
        ArcBatch::new(B::seal(chain, description))
    }
}

/// Merges [`ArcBatch`]es, delegating to the inner batch's merger.
pub struct ArcMerger<B: Batch> {
    merger: B::Merger,
}

impl<B: Batch> Merger<ArcBatch<B>> for ArcMerger<B> {
    fn new(
        source1: &ArcBatch<B>,
        source2: &ArcBatch<B>,
        compaction_frontier: AntichainRef<B::Time>,
    ) -> Self {
        ArcMerger {
            merger: B::begin_merge(&source1.0, &source2.0, compaction_frontier),
        }
    }
    fn work(&mut self, source1: &ArcBatch<B>, source2: &ArcBatch<B>, fuel: &mut isize) {
        self.merger.work(&source1.0, &source2.0, fuel)
    }
    fn done(self) -> ArcBatch<B> {
        ArcBatch::new(self.merger.done())
    }
}

#[cfg(test)]
mod tests {
    use differential_dataflow::trace::cursor::Cursor;
    use differential_dataflow::trace::implementations::ord_neu::OrdValBatcher;
    use differential_dataflow::trace::{Batcher, Builder, Navigable};
    use timely::container::PushInto;
    use timely::progress::Antichain;

    use crate::ArcOrdValBuilder;

    /// An `ArcBatch`'s cursor can be constructed and read from a thread other than the one that
    /// built it, proving the newtype's batches are usable across a thread boundary. This is the
    /// property that lets [`crate::ArcOrdValSpine`] (and the `RowRow`/`Err` spines built on
    /// [`ArcBatch`]) back a cross-runtime shared trace; the default `Rc`-backed spines are
    /// worker-local by design and do not have it.
    ///
    /// Ported from the differential-dataflow primitive's own `tests/trace.rs`
    /// `test_batches_read_from_other_thread`, adapted from the fork's `arc_blanket_impls` (which
    /// this newtype replaces) to `ArcBatch`.
    #[mz_ore::test]
    fn arc_batch_reads_from_other_thread() {
        fn assert_send_sync<T: Send + Sync>(_: &T) {}

        let mut batcher = OrdValBatcher::<u64, u64, usize, i64>::new(None, 0);
        batcher.push_into(vec![((1, 2), 0, 1), ((2, 3), 1, 1)]);
        let (mut chain, description) = batcher.seal(Antichain::from_elem(2));
        let batch = ArcOrdValBuilder::<u64, u64, usize, i64>::seal(&mut chain, description);

        assert_send_sync(&batch);

        let read = std::thread::spawn(move || {
            let mut cursor = batch.cursor();
            cursor.to_vec(&batch, |k| *k, |v| *v)
        })
        .join()
        .expect("reader thread panicked");

        assert_eq!(read, vec![((1, 2), vec![(0, 1)]), ((2, 3), vec![(1, 1)])]);
    }
}
