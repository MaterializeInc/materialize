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
//! and differential's `SpineBatch`/`Builder`/`Merger`/`Cursor` traits are foreign, so the orphan rule
//! forbids it here. [`ArcBatch`] is a local newtype around `Arc<B>` that carries those impls
//! instead. The impls delegate straight through to the inner batch, so `ArcBatch<B>` behaves
//! exactly like `B` except that its handle is atomically reference counted.
//!
//! This mirrors differential's own `rc_blanket_impls` (for `Rc<B>`), swapping in `Arc`. Keeping it
//! here as a newtype lets cross-thread arrangement sharing build against a released
//! differential-dataflow, with no differential-side `Arc` batch impls required.

use differential_dataflow::trace::implementations::merge_batcher::Sealer;
use differential_dataflow::trace::implementations::spine_fueled::{Merger, SpineBatch};
use differential_dataflow::trace::{Builder, Cursor, Navigable};
use std::sync::Arc;
use timely::progress::frontier::AntichainRef;

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

impl<B: Navigable> Navigable for ArcBatch<B> {
    type Cursor = ArcBatchCursor<B::Cursor>;
    fn cursor(&self) -> Self::Cursor {
        // Disambiguate to the inner batch's cursor, reached through the `Deref`, so the wrapper's
        // `Cursor` is `B`'s rather than any impl that might exist on `Arc<B>` itself.
        ArcBatchCursor::new(<B as Navigable>::cursor(&self.0))
    }
}

impl<B: SpineBatch> SpineBatch for ArcBatch<B> {
    type Time = B::Time;
    type Merger = ArcMerger<B>;
    fn len(&self) -> usize {
        self.0.len()
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

/// Builds [`ArcBatch`]es, delegating to the inner batch's builder.
pub struct ArcBuilder<B> {
    builder: B,
}

impl<B: Default> Default for ArcBuilder<B> {
    fn default() -> Self {
        ArcBuilder {
            builder: B::default(),
        }
    }
}

impl<B: Builder + Default> Builder for ArcBuilder<B> {
    type Input = B::Input;
    type Time = B::Time;
    type Output = ArcBatch<B::Output>;
    fn push(&mut self, input: &mut Self::Input) {
        self.builder.push(input)
    }
    fn done(self) -> Option<ArcBatch<B::Output>> {
        self.builder.done().map(ArcBatch::new)
    }
}

impl<C, B: Sealer<C>> Sealer<C> for ArcBuilder<B> {
    type Output = ArcBatch<B::Output>;
    fn seal(chain: &mut Vec<C>) -> Option<Self::Output> {
        B::seal(chain).map(ArcBatch::new)
    }
}

/// Merges [`ArcBatch`]es, delegating to the inner batch's merger.
pub struct ArcMerger<B: SpineBatch> {
    merger: B::Merger,
}

impl<B: SpineBatch> Merger<ArcBatch<B>> for ArcMerger<B> {
    fn new(
        source1: &ArcBatch<B>,
        source2: &ArcBatch<B>,
        compaction_frontier: AntichainRef<B::Time>,
    ) -> Self {
        ArcMerger {
            merger: B::Merger::new(&source1.0, &source2.0, compaction_frontier),
        }
    }
    fn work(&mut self, source1: &ArcBatch<B>, source2: &ArcBatch<B>, fuel: &mut isize) {
        self.merger.work(&source1.0, &source2.0, fuel)
    }
    fn done(self) -> Option<ArcBatch<B>> {
        self.merger.done().map(ArcBatch::new)
    }
}

#[cfg(test)]
mod tests {
    use differential_dataflow::batcher::Batcher;
    use differential_dataflow::trace::Navigable;
    use differential_dataflow::trace::cursor::Cursor;
    use timely::progress::Antichain;

    use crate::ArcOrdValBatcher;

    /// An `ArcBatch`'s cursor can be constructed and read from a thread other than the one that
    /// built it, proving the newtype's batches are usable across a thread boundary. This is the
    /// property that lets [`crate::ArcOrdValSpine`] (and the `RowRow`/`Err` spines built on
    /// [`ArcBatch`]) back a cross-runtime shared trace; the default `Rc`-backed spines are
    /// worker-local by design and do not have it.
    ///
    /// Mirrors differential-dataflow's own `tests/trace.rs` cross-thread batch read, over the local
    /// [`ArcBatch`] newtype.
    #[mz_ore::test]
    fn arc_batch_reads_from_other_thread() {
        fn assert_send_sync<T: Send + Sync>(_: &T) {}

        let mut batcher = ArcOrdValBatcher::<u64, u64, usize, i64>::new(None, 0);
        let mut updates: Vec<((u64, u64), usize, i64)> = vec![((1, 2), 0, 1), ((2, 3), 1, 1)];
        Batcher::<Vec<((u64, u64), usize, i64)>>::insert(&mut batcher, &mut updates);
        let (batch, _frontier) = Batcher::<Vec<((u64, u64), usize, i64)>>::extract(
            &mut batcher,
            Antichain::from_elem(2).borrow(),
        );
        let batch = batch.expect("updates were pushed, so the batch is non-empty");

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
