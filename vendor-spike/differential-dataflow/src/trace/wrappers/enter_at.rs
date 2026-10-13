//! Wrappers to provide trace access to nested scopes.

use timely::progress::timestamp::Refines;
use timely::progress::{Antichain, frontier::AntichainRef};

use crate::lattice::Lattice;
use crate::trace::{BatchKey, BatchReader, BatchTimeGat, BatchVal, Description, Navigable, TraceReader};
use crate::trace::cursor::Cursor;

/// Wrapper to provide trace to nested scope.
///
/// Each wrapped update is presented with a timestamp determined by `logic`.
///
/// At the same time, we require a method `prior` that can "invert" timestamps,
/// and which will be applied to compaction frontiers as they are communicated
/// back to the wrapped traces. A better explanation is pending, and until that
/// happens use this construct at your own peril!
pub struct TraceEnter<Tr: TraceReader, TInner, F, G> {
    trace: Tr,
    stash1: Antichain<Tr::Time>,
    stash2: Antichain<TInner>,
    logic: F,
    prior: G,
}

impl<Tr,TInner,F,G> Clone for TraceEnter<Tr, TInner, F, G>
where
    Tr: TraceReader+Clone,
    F: Clone,
    G: Clone,
{
    fn clone(&self) -> Self {
        TraceEnter {
            trace: self.trace.clone(),
            stash1: Antichain::new(),
            stash2: Antichain::new(),
            logic: self.logic.clone(),
            prior: self.prior.clone(),
        }
    }
}

impl<Tr, TInner, F, G> TraceReader for TraceEnter<Tr, TInner, F, G>
where
    Tr: TraceReader<Batch: Navigable>,
    TInner: Refines<Tr::Time>+Lattice,
    F: 'static,
    F: FnMut(BatchKey<'_, Tr>, BatchVal<'_, Tr>, BatchTimeGat<'_, Tr>)->TInner+Clone,
    G: FnMut(&TInner)->Tr::Time+Clone+'static,
{
    type Time = TInner;
    type Batch = BatchEnter<Tr::Batch, TInner,F>;

    fn map_batches<F2: FnMut(&Self::Batch)>(&self, mut f: F2) {
        let logic = self.logic.clone();
        self.trace.map_batches(|batch| {
            f(&Self::Batch::make_from(batch.clone(), logic.clone()));
        })
    }

    fn set_logical_compaction(&mut self, frontier: AntichainRef<'_, TInner>) {
        self.stash1.clear();
        for time in frontier.iter() {
            self.stash1.insert((self.prior)(time));
        }
        self.trace.set_logical_compaction(self.stash1.borrow());
    }
    fn get_logical_compaction(&mut self) -> AntichainRef<'_, TInner> {
        self.stash2.clear();
        for time in self.trace.get_logical_compaction().iter() {
            self.stash2.insert(TInner::to_inner(time.clone()));
        }
        self.stash2.borrow()
    }

    fn set_physical_compaction(&mut self, frontier: AntichainRef<'_, TInner>) {
        self.stash1.clear();
        for time in frontier.iter() {
            self.stash1.insert((self.prior)(time));
        }
        self.trace.set_physical_compaction(self.stash1.borrow());
    }
    fn get_physical_compaction(&mut self) -> AntichainRef<'_, TInner> {
        self.stash2.clear();
        for time in self.trace.get_physical_compaction().iter() {
            self.stash2.insert(TInner::to_inner(time.clone()));
        }
        self.stash2.borrow()
    }

    fn batches_through(&mut self, upper: AntichainRef<TInner>) -> Option<Vec<Self::Batch>> {
        self.stash1.clear();
        for time in upper.iter() {
            self.stash1.insert(time.clone().to_outer());
        }
        let logic = self.logic.clone();
        let storage = self.trace.batches_through(self.stash1.borrow())?;
        Some(storage.into_iter().map(|batch| BatchEnter::make_from(batch, logic.clone())).collect())
    }
}

impl<Tr, TInner, F, G> TraceEnter<Tr, TInner, F, G>
where
    Tr: TraceReader,
    TInner: Refines<Tr::Time>+Lattice,
{
    /// Makes a new trace wrapper
    pub fn make_from(trace: Tr, logic: F, prior: G) -> Self {
        TraceEnter {
            trace,
            stash1: Antichain::new(),
            stash2: Antichain::new(),
            logic,
            prior,
        }
    }
}


/// Wrapper to provide batch to nested scope.
#[derive(Clone)]
pub struct BatchEnter<B, TInner, F> {
    batch: B,
    description: Description<TInner>,
    logic: F,
}

impl<B, TInner, F> Navigable for BatchEnter<B, TInner, F>
where
    B: BatchReader + Navigable,
    TInner: Refines<B::Time>+Lattice,
    TInner: Refines<<B::Cursor as Cursor>::Time>,
    F: FnMut(<B::Cursor as Cursor>::Key<'_>, <B::Cursor as Cursor>::Val<'_>, <B::Cursor as Cursor>::TimeGat<'_>)->TInner+Clone,
{
    type Cursor = BatchCursorEnter<B::Cursor, TInner, F>;

    fn cursor(&self) -> Self::Cursor {
        BatchCursorEnter::new(self.batch.cursor(), self.logic.clone())
    }
}

impl<B, TInner, F> BatchReader for BatchEnter<B, TInner, F>
where
    B: BatchReader + Navigable,
    TInner: Refines<B::Time>+Lattice,
    F: FnMut(<B::Cursor as Cursor>::Key<'_>, <B::Cursor as Cursor>::Val<'_>, <B::Cursor as Cursor>::TimeGat<'_>)->TInner+Clone,
{
    type Time = TInner;
    fn len(&self) -> usize { self.batch.len() }
    fn description(&self) -> &Description<TInner> { &self.description }
}

impl<B, TInner, F> BatchEnter<B, TInner, F>
where
    B: BatchReader,
    TInner: Refines<B::Time>+Lattice,
{
    /// Makes a new batch wrapper
    pub fn make_from(batch: B, logic: F) -> Self {
        let lower: Vec<_> = batch.description().lower().elements().iter().map(|x| TInner::to_inner(x.clone())).collect();
        let upper: Vec<_> = batch.description().upper().elements().iter().map(|x| TInner::to_inner(x.clone())).collect();
        let since: Vec<_> = batch.description().since().elements().iter().map(|x| TInner::to_inner(x.clone())).collect();

        BatchEnter {
            batch,
            description: Description::new(Antichain::from(lower), Antichain::from(upper), Antichain::from(since)),
            logic,
        }
    }
}

use crate::trace::implementations::BatchContainer;

/// Wrapper to provide cursor to nested scope.
pub struct BatchCursorEnter<C, TInner, F> {
    phantom: ::std::marker::PhantomData<TInner>,
    cursor: C,
    logic: F,
}

impl<C, TInner, F> BatchCursorEnter<C, TInner, F> {
    fn new(cursor: C, logic: F) -> Self {
        BatchCursorEnter {
            phantom: ::std::marker::PhantomData,
            cursor,
            logic,
        }
    }
}

impl<TInner, C: Cursor, F> Cursor for BatchCursorEnter<C, TInner, F>
where
    TInner: Refines<C::Time>+Lattice,
    F: FnMut(C::Key<'_>, C::Val<'_>, C::TimeGat<'_>)->TInner,
{
    type Storage = BatchEnter<C::Storage, TInner, F>;

    type Key<'a> = C::Key<'a>;
    type ValOwn = C::ValOwn;
    type Val<'a> = C::Val<'a>;
    type KeyContainer = C::KeyContainer;
    type ValContainer = C::ValContainer;
    type DiffContainer = C::DiffContainer;
    type Diff = C::Diff;
    type DiffGat<'a> = C::DiffGat<'a>;
    type TimeContainer = Vec<TInner>;
    type Time = <Vec<TInner> as BatchContainer>::Owned;
    type TimeGat<'a> = <Vec<TInner> as BatchContainer>::ReadItem<'a>;

    #[inline] fn key_valid(&self, storage: &Self::Storage) -> bool { self.cursor.key_valid(&storage.batch) }
    #[inline] fn val_valid(&self, storage: &Self::Storage) -> bool { self.cursor.val_valid(&storage.batch) }

    #[inline] fn key<'a>(&self, storage: &'a Self::Storage) -> Self::Key<'a> { self.cursor.key(&storage.batch) }
    #[inline] fn val<'a>(&self, storage: &'a Self::Storage) -> Self::Val<'a> { self.cursor.val(&storage.batch) }

    #[inline] fn get_key<'a>(&self, storage: &'a Self::Storage) -> Option<Self::Key<'a>> { self.cursor.get_key(&storage.batch) }
    #[inline] fn get_val<'a>(&self, storage: &'a Self::Storage) -> Option<Self::Val<'a>> { self.cursor.get_val(&storage.batch) }

    #[inline]
    fn map_times<L: FnMut(&TInner, Self::DiffGat<'_>)>(&mut self, storage: &Self::Storage, mut logic: L) {
        let key = self.key(storage);
        let val = self.val(storage);
        let logic2 = &mut self.logic;
        self.cursor.map_times(&storage.batch, |time, diff| {
            logic(&logic2(key, val, time), diff)
        })
    }

    #[inline] fn step_key(&mut self, storage: &Self::Storage) { self.cursor.step_key(&storage.batch) }
    #[inline] fn seek_key(&mut self, storage: &Self::Storage, key: Self::Key<'_>) { self.cursor.seek_key(&storage.batch, key) }

    #[inline] fn step_val(&mut self, storage: &Self::Storage) { self.cursor.step_val(&storage.batch) }
    #[inline] fn seek_val(&mut self, storage: &Self::Storage, val: Self::Val<'_>) { self.cursor.seek_val(&storage.batch, val) }

    #[inline] fn rewind_keys(&mut self, storage: &Self::Storage) { self.cursor.rewind_keys(&storage.batch) }
    #[inline] fn rewind_vals(&mut self, storage: &Self::Storage) { self.cursor.rewind_vals(&storage.batch) }
}
