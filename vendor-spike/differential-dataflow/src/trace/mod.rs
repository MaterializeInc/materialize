//! Traits and datastructures representing a collection trace.
//!
//! A collection trace is a set of updates of the form `(key, val, time, diff)`, which determine the contents
//! of a collection at given times by accumulating updates whose time field is less or equal to the target field.
//!
//! The `Trace` trait describes those types and methods that a data structure must implement to be viewed as a
//! collection trace. This trait allows operator implementations to be generic with respect to the type of trace,
//! and allows various data structures to be interpretable as multiple different types of trace.

pub mod chunk;
pub mod cursor;
pub mod description;
pub mod implementations;
pub mod wrappers;

use timely::container::PushInto;
use timely::progress::{Antichain, frontier::AntichainRef};
use timely::progress::Timestamp;
use crate::lattice::Lattice;

use crate::logging::Logger;
pub use self::cursor::Cursor;
pub use self::cursor::Navigable;
pub use self::cursor::{BatchCursor, BatchKey, BatchVal, BatchValOwn, BatchDiff, BatchDiffGat, BatchTimeGat};
use self::cursor::CursorList;
pub use self::description::Description;

/// A type used to express how much effort a trace should exert even in the absence of updates.
pub type ExertionLogic = std::sync::Arc<dyn for<'a> Fn(&'a [(usize, usize, usize)])->Option<usize>+Send+Sync>;

/// A trace whose contents may be read.
///
/// This is a restricted interface to the more general `Trace` trait, which extends this trait with further methods
/// to update the contents of the trace. These methods are used to examine the contents, and to update the reader's
/// capabilities (which may release restrictions on the mutations to the underlying trace and cause work to happen).
pub trait TraceReader {

    /// The timestamp type of the trace's updates.
    ///
    /// Key/val/diff opinions live on the batches' cursors; the trace itself only needs time, to
    /// bound its contents and to drive compaction.
    type Time: Timestamp + Lattice;

    /// The type of an immutable collection of updates.
    type Batch:
        'static +
        Clone +
        BatchReader<Time = Self::Time>;

    /// Acquires the non-empty sequence of batches covering updates at times not greater or equal to an
    /// element of `upper`.
    ///
    /// This is the sole primitive each `TraceReader` must implement to expose its contents: the
    /// `cursor` and `cursor_through` methods assemble a [`CursorList`] over these batches' cursors,
    /// for which the returned `Vec` serves as storage (the cursor borrows from it).
    ///
    /// This method is expected to work if called with an `upper` that (i) was an observed bound in batches from
    /// the trace, and (ii) the trace has not been advanced beyond `upper`. Practically, the implementation should
    /// be expected to look for a "clean cut" using `upper`, and if it finds such a cut can return the batches. This
    /// should allow `upper` such as `&[]` as used by `self.cursor()`, though it is difficult to imagine other uses.
    fn batches_through(&mut self, upper: AntichainRef<Self::Time>) -> Option<Vec<Self::Batch>>;

    /// Provides a cursor over updates contained in the trace.
    fn cursor(&mut self) -> (CursorList<<Self::Batch as Navigable>::Cursor>, Vec<Self::Batch>) where Self::Batch: Navigable {
        if let Some(cursor) = self.cursor_through(Antichain::new().borrow()) {
            cursor
        }
        else {
            panic!("unable to acquire complete cursor for trace; is it closed?");
        }
    }

    /// Acquires a cursor to the restriction of the collection's contents to updates at times not greater or
    /// equal to an element of `upper`.
    ///
    /// The cursor is a [`CursorList`] that merges the cursors of the batches returned by
    /// [`batches_through`](TraceReader::batches_through); see that method for the contract on `upper`.
    fn cursor_through(&mut self, upper: AntichainRef<Self::Time>) -> Option<(CursorList<<Self::Batch as Navigable>::Cursor>, Vec<Self::Batch>)> where Self::Batch: Navigable {
        Some(self::cursor::cursor_list(self.batches_through(upper)?))
    }

    /// Advances the frontier that constrains logical compaction.
    ///
    /// Logical compaction is the ability of the trace to change the times of the updates it contains.
    /// Update times may be changed as long as their comparison to all query times beyond the logical compaction
    /// frontier remains unchanged. Practically, this means that groups of timestamps not beyond the frontier can
    /// be coalesced into fewer representative times.
    ///
    /// Logical compaction is important, as it allows the trace to forget historical distinctions between update
    /// times, and maintain a compact memory footprint over an unbounded update history.
    ///
    /// By advancing the logical compaction frontier, the caller unblocks merging of otherwise equivalent updates,
    /// but loses the ability to observe historical detail that is not beyond `frontier`.
    ///
    /// It is an error to call this method with a frontier not equal to or beyond the most recent arguments to
    /// this method, or the initial value of `get_logical_compaction()` if this method has not yet been called.
    fn set_logical_compaction(&mut self, frontier: AntichainRef<Self::Time>);

    /// Reports the logical compaction frontier.
    ///
    /// All update times beyond this frontier will be presented with their original times, and all update times
    /// not beyond this frontier will present as a time that compares identically with all query times beyond
    /// this frontier. Practically, update times not beyond this frontier should not be taken to be accurate as
    /// presented, and should be used carefully, only in accumulation to times that are beyond the frontier.
    fn get_logical_compaction(&mut self) -> AntichainRef<'_, Self::Time>;

    /// Advances the frontier that constrains physical compaction.
    ///
    /// Physical compaction is the ability of the trace to merge the batches of updates it maintains. Physical
    /// compaction does not change the updates or their timestamps, although it is also the moment at which
    /// logical compaction is most likely to happen.
    ///
    /// Physical compaction allows the trace to maintain a logarithmic number of batches of updates, which is
    /// what allows the trace to provide efficient random access by keys and values.
    ///
    /// By advancing the physical compaction frontier, the caller unblocks the merging of batches of updates,
    /// but loses the ability to create a cursor through any frontier not beyond `frontier`.
    ///
    /// It is an error to call this method with a frontier not equal to or beyond the most recent arguments to
    /// this method, or the initial value of `get_physical_compaction()` if this method has not yet been called.
    fn set_physical_compaction(&mut self, frontier: AntichainRef<'_, Self::Time>);

    /// Reports the physical compaction frontier.
    ///
    /// All batches containing updates beyond this frontier will not be merged with other batches. This allows
    /// the caller to create a cursor through any frontier beyond the physical compaction frontier, with the
    /// `cursor_through()` method. This functionality is primarily of interest to the `join` operator, and any
    /// other operators who need to take notice of the physical structure of update batches.
    fn get_physical_compaction(&mut self) -> AntichainRef<'_, Self::Time>;

    /// Maps logic across the non-empty sequence of batches in the trace.
    ///
    /// This is currently used only to extract historical data to prime late-starting operators who want to reproduce
    /// the stream of batches moving past the trace. It could also be a fine basis for a default implementation of the
    /// cursor methods, as they (by default) just move through batches accumulating cursors into a cursor list.
    fn map_batches<F: FnMut(&Self::Batch)>(&self, f: F);

    /// Reads the upper frontier of committed times.
    ///
    ///
    #[inline]
    fn read_upper(&mut self, target: &mut Antichain<Self::Time>) {
        target.clear();
        target.insert(<Self::Time as timely::progress::Timestamp>::minimum());
        self.map_batches(|batch| {
            target.clone_from(batch.upper());
        });
    }

    /// Advances `upper` by any empty batches.
    ///
    /// An empty batch whose `batch.lower` bound equals the current
    /// contents of `upper` will advance `upper` to `batch.upper`.
    /// Taken across all batches, this should advance `upper` across
    /// empty batch regions.
    fn advance_upper(&mut self, upper: &mut Antichain<Self::Time>) {
        self.map_batches(|batch| {
            if batch.is_empty() && batch.lower() == upper {
                upper.clone_from(batch.upper());
            }
        });
    }

}

/// An append-only collection of `(key, val, time, diff)` tuples.
///
/// The trace must pretend to look like a collection of `(Key, Val, Time, isize)` tuples, but is permitted
/// to introduce new types `KeyRef`, `ValRef`, and `TimeRef` which can be dereference to the types above.
///
/// The trace must be constructable from, and navigable by the `Key`, `Val`, `Time` types, but does not need
/// to return them.
pub trait Trace : TraceReader<Batch: Batch> {

    /// Allocates a new empty trace.
    fn new(
        info: ::timely::dataflow::operators::generic::OperatorInfo,
        logging: Option<crate::logging::Logger>,
        activator: Option<timely::scheduling::activate::Activator>,
    ) -> Self;

    /// Exert merge effort, even without updates.
    fn exert(&mut self);

    /// Sets the logic for exertion in the absence of updates.
    ///
    /// The function receives an iterator over batch levels, from large to small, as triples `(level, count, length)`,
    /// indicating the level, the number of batches, and their total length in updates. It should return a number of
    /// updates to perform, or `None` if no work is required.
    fn set_exert_logic(&mut self, logic: ExertionLogic);

    /// Introduces a batch of updates to the trace.
    ///
    /// Batches describe the time intervals they contain, and they should be added to the trace in contiguous
    /// intervals. If a batch arrives with a lower bound that does not equal the upper bound of the most recent
    /// addition, the trace will add an empty batch. It is an error to then try to populate that region of time.
    ///
    /// This restriction could be relaxed, especially if we discover ways in which batch interval order could
    /// commute. For now, the trace should complain, to the extent that it cares about contiguous intervals.
    fn insert(&mut self, batch: Self::Batch);

    /// Introduces an empty batch concluding the trace.
    ///
    /// This method should be logically equivalent to introducing an empty batch whose lower frontier equals
    /// the upper frontier of the most recently introduced batch, and whose upper frontier is empty.
    fn close(&mut self);
}

/// A batch of updates whose contents may be read.
///
/// This is a restricted interface to batches of updates, which support the reading of the batch's contents,
/// but do not expose ways to construct the batches. This trait is appropriate for views of the batch, and is
/// especially useful for views derived from other sources in ways that prevent the construction of batches
/// from the type of data in the view (for example, filtered views, or views with extended time coordinates).
pub trait BatchReader : Sized {

    /// The timestamp type of the batch's updates.
    ///
    /// A batch carries only time; navigating its contents is the separate, optional [`Navigable`]
    /// capability, which an operator requests with a `Self: Navigable` bound when it needs a cursor.
    type Time: Timestamp + Lattice;

    /// The number of updates in the batch.
    fn len(&self) -> usize;
    /// True if the batch is empty.
    fn is_empty(&self) -> bool { self.len() == 0 }
    /// Describes the times of the updates in the batch.
    fn description(&self) -> &Description<Self::Time>;

    /// All times in the batch are greater or equal to an element of `lower`.
    fn lower(&self) -> &Antichain<Self::Time> { self.description().lower() }
    /// All times in the batch are not greater or equal to any element of `upper`.
    fn upper(&self) -> &Antichain<Self::Time> { self.description().upper() }
}

/// An immutable collection of updates.
pub trait Batch : BatchReader + Sized {
    /// A type used to progressively merge batches.
    type Merger: Merger<Self>;

    /// Initiates the merging of consecutive batches.
    ///
    /// The result of this method can be exercised to eventually produce the same result
    /// that a call to `self.merge(other)` would produce, but it can be done in a measured
    /// fashion. This can help to avoid latency spikes where a large merge needs to happen.
    fn begin_merge(&self, other: &Self, compaction_frontier: AntichainRef<Self::Time>) -> Self::Merger {
        Self::Merger::new(self, other, compaction_frontier)
    }

    /// Produce an empty batch over the indicated interval.
    fn empty(lower: Antichain<Self::Time>, upper: Antichain<Self::Time>) -> Self;
}

/// Functionality for collecting and batching updates.
///
/// Accepts containers of type `Output` via [`PushInto`] and produces output batches of the same
/// type. Callers are responsible for converting raw input data into `Output` containers (e.g.
/// using a chunker) before pushing into the batcher.
pub trait Batcher: PushInto<Self::Output> {
    /// Type produced by the batcher, and also the type it consumes.
    type Output: Default;
    /// Times at which batches are formed.
    type Time: Timestamp;
    /// Allocates a new empty batcher.
    fn new(logger: Option<Logger>, operator_id: usize) -> Self;
    /// Returns all updates not greater or equal to an element of `upper`, as a sorted and
    /// consolidated chain together with the description that bounds them.
    ///
    /// The returned chain is suitable to hand directly to [`Builder::seal`].
    fn seal(&mut self, upper: Antichain<Self::Time>) -> (Vec<Self::Output>, Description<Self::Time>);
    /// Returns the lower envelope of contained update times.
    fn frontier(&mut self) -> AntichainRef<'_, Self::Time>;
}

/// Functionality for building batches from ordered update sequences.
pub trait Builder: Sized {
    /// Input item type.
    type Input;
    /// Timestamp type.
    type Time: Timestamp;
    /// Output batch type.
    type Output;

    /// Allocates an empty builder.
    ///
    /// Ideally we deprecate this and insist all non-trivial building happens via `with_capacity()`.
    // #[deprecated]
    fn new() -> Self { Self::with_capacity(0, 0, 0) }
    /// Allocates an empty builder with capacity for the specified keys, values, and updates.
    ///
    /// They represent respectively the number of distinct `key`, `(key, val)`, and total updates.
    fn with_capacity(keys: usize, vals: usize, upds: usize) -> Self;
    /// Adds a chunk of elements to the batch.
    ///
    /// Adds all elements from `chunk` to the builder and leaves `chunk` in an undefined state.
    fn push(&mut self, chunk: &mut Self::Input);
    /// Completes building and returns the batch.
    fn done(self, description: Description<Self::Time>) -> Self::Output;

    /// Builds a batch from a chain of updates corresponding to the indicated lower and upper bounds.
    ///
    /// This method relies on the chain only containing updates greater or equal to the lower frontier,
    /// and not greater or equal to the upper frontier, as encoded in the description. Chains must also
    /// be sorted and consolidated.
    fn seal(chain: &mut Vec<Self::Input>, description: Description<Self::Time>) -> Self::Output;
}

/// Represents a merge in progress.
pub trait Merger<Output: Batch> {
    /// Creates a new merger to merge the supplied batches, optionally compacting
    /// up to the supplied frontier.
    fn new(source1: &Output, source2: &Output, compaction_frontier: AntichainRef<Output::Time>) -> Self;
    /// Perform some amount of work, decrementing `fuel`.
    ///
    /// If `fuel` is non-zero after the call, the merging is complete and
    /// one should call `done` to extract the merged results.
    fn work(&mut self, source1: &Output, source2: &Output, fuel: &mut isize);
    /// Extracts merged results.
    ///
    /// This method should only be called after `work` has been called and
    /// has not brought `fuel` to zero. Otherwise, the merge is still in
    /// progress.
    fn done(self) -> Output;
}


/// Blanket implementations for reference counted batches.
pub mod rc_blanket_impls {

    use std::rc::Rc;

    use timely::progress::{Antichain, frontier::AntichainRef};
    use super::{Batch, BatchReader, Builder, Merger, Navigable, Cursor, Description};

    impl<B: BatchReader + Navigable> Navigable for Rc<B> {
        /// The type used to enumerate the batch's contents.
        type Cursor = RcBatchCursor<B::Cursor>;
        /// Acquires a cursor to the batch's contents.
        fn cursor(&self) -> Self::Cursor {
            RcBatchCursor::new((**self).cursor())
        }
    }

    impl<B: BatchReader> BatchReader for Rc<B> {

        type Time = B::Time;
        /// The number of updates in the batch.
        fn len(&self) -> usize { (**self).len() }
        /// Describes the times of the updates in the batch.
        fn description(&self) -> &Description<Self::Time> { (**self).description() }
    }

    /// Wrapper to provide cursor to nested scope.
    pub struct RcBatchCursor<C> {
        cursor: C,
    }

    impl<C> RcBatchCursor<C> {
        fn new(cursor: C) -> Self {
            RcBatchCursor {
                cursor,
            }
        }
    }

    impl<C: Cursor> Cursor for RcBatchCursor<C> {

        type Storage = Rc<C::Storage>;

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

        #[inline] fn key_valid(&self, storage: &Self::Storage) -> bool { self.cursor.key_valid(storage) }
        #[inline] fn val_valid(&self, storage: &Self::Storage) -> bool { self.cursor.val_valid(storage) }

        #[inline] fn key<'a>(&self, storage: &'a Self::Storage) -> Self::Key<'a> { self.cursor.key(storage) }
        #[inline] fn val<'a>(&self, storage: &'a Self::Storage) -> Self::Val<'a> { self.cursor.val(storage) }

        #[inline] fn get_key<'a>(&self, storage: &'a Self::Storage) -> Option<Self::Key<'a>> { self.cursor.get_key(storage) }
        #[inline] fn get_val<'a>(&self, storage: &'a Self::Storage) -> Option<Self::Val<'a>> { self.cursor.get_val(storage) }

        #[inline]
        fn map_times<L: FnMut(Self::TimeGat<'_>, Self::DiffGat<'_>)>(&mut self, storage: &Self::Storage, logic: L) {
            self.cursor.map_times(storage, logic)
        }

        #[inline] fn step_key(&mut self, storage: &Self::Storage) { self.cursor.step_key(storage) }
        #[inline] fn seek_key(&mut self, storage: &Self::Storage, key: Self::Key<'_>) { self.cursor.seek_key(storage, key) }

        #[inline] fn step_val(&mut self, storage: &Self::Storage) { self.cursor.step_val(storage) }
        #[inline] fn seek_val(&mut self, storage: &Self::Storage, val: Self::Val<'_>) { self.cursor.seek_val(storage, val) }

        #[inline] fn rewind_keys(&mut self, storage: &Self::Storage) { self.cursor.rewind_keys(storage) }
        #[inline] fn rewind_vals(&mut self, storage: &Self::Storage) { self.cursor.rewind_vals(storage) }
    }

    /// An immutable collection of updates.
    impl<B: Batch> Batch for Rc<B> {
        type Merger = RcMerger<B>;
        fn empty(lower: Antichain<Self::Time>, upper: Antichain<Self::Time>) -> Self {
            Rc::new(B::empty(lower, upper))
        }
    }

    /// Wrapper type for building reference counted batches.
    pub struct RcBuilder<B: Builder> { builder: B }

    /// Functionality for building batches from ordered update sequences.
    impl<B: Builder> Builder for RcBuilder<B> {
        type Input = B::Input;
        type Time = B::Time;
        type Output = Rc<B::Output>;
        fn with_capacity(keys: usize, vals: usize, upds: usize) -> Self { RcBuilder { builder: B::with_capacity(keys, vals, upds) } }
        fn push(&mut self, input: &mut Self::Input) { self.builder.push(input) }
        fn done(self, description: Description<Self::Time>) -> Rc<B::Output> { Rc::new(self.builder.done(description)) }
        fn seal(chain: &mut Vec<Self::Input>, description: Description<Self::Time>) -> Self::Output {
            Rc::new(B::seal(chain, description))
        }
    }

    /// Wrapper type for merging reference counted batches.
    pub struct RcMerger<B:Batch> { merger: B::Merger }

    /// Represents a merge in progress.
    impl<B:Batch> Merger<Rc<B>> for RcMerger<B> {
        fn new(source1: &Rc<B>, source2: &Rc<B>, compaction_frontier: AntichainRef<B::Time>) -> Self { RcMerger { merger: B::begin_merge(source1, source2, compaction_frontier) } }
        fn work(&mut self, source1: &Rc<B>, source2: &Rc<B>, fuel: &mut isize) { self.merger.work(source1, source2, fuel) }
        fn done(self) -> Rc<B> { Rc::new(self.merger.done()) }
    }
}
