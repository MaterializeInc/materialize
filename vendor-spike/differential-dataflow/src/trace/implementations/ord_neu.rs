//! Trace and batch implementations based on sorted ranges.
//!
//! The types and type aliases in this module start with either
//!
//! * `OrdVal`: Collections whose data have the form `(key, val)` where `key` is ordered.
//! * `OrdKey`: Collections whose data have the form `key` where `key` is ordered.
//!
//! Although `OrdVal` is more general than `OrdKey`, the latter has a simpler representation
//! and should consume fewer resources (computation and memory) when it applies.

use std::rc::Rc;

use crate::trace::implementations::spine_fueled::Spine;
use crate::trace::implementations::merge_batcher::MergeBatcher;
use crate::trace::implementations::merge_batcher::vec::VecMerger;
use crate::trace::rc_blanket_impls::RcBuilder;

use super::{Layout, Vector};

pub use self::val_batch::{OrdValBatch, OrdValBuilder};
pub use self::key_batch::{OrdKeyBatch, OrdKeyBuilder};

/// A trace implementation using a spine of ordered lists.
pub type OrdValSpine<K, V, T, R> = Spine<Rc<OrdValBatch<Vector<((K,V),T,R)>>>>;
/// A batcher using ordered lists.
pub type OrdValBatcher<K, V, T, R> = MergeBatcher<VecMerger<(K, V), T, R>>;
/// A builder using ordered lists.
pub type RcOrdValBuilder<K, V, T, R> = RcBuilder<OrdValBuilder<Vector<((K,V),T,R)>, Vec<((K,V),T,R)>>>;

/// A trace implementation using a spine of ordered lists.
pub type OrdKeySpine<K, T, R> = Spine<Rc<OrdKeyBatch<Vector<((K,()),T,R)>>>>;
/// A batcher for ordered lists.
pub type OrdKeyBatcher<K, T, R> = MergeBatcher<VecMerger<(K, ()), T, R>>;
/// A builder for ordered lists.
pub type RcOrdKeyBuilder<K, T, R> = RcBuilder<OrdKeyBuilder<Vector<((K,()),T,R)>, Vec<((K,()),T,R)>>>;

pub use layers::{Vals, Upds};
/// Layers are containers of lists of some type.
///
/// The intent is that they "attach" to an outer layer which has as many values
/// as the layer has lists, thereby associating a list with each outer value.
/// A sequence of layers, each matching the number of values in its predecessor,
/// forms a layered trie: a tree with values of some type on nodes at each depth.
///
/// We will form tries here by layering `[Keys, Vals, Upds]` or `[Keys, Upds]`.
pub mod layers {

    use serde::{Deserialize, Serialize};
    use crate::trace::implementations::BatchContainer;

    /// A container for non-empty lists of values.
    #[derive(Debug, Serialize, Deserialize)]
    pub struct Vals<O, V> {
        /// Offsets used to provide indexes from keys to values.
        ///
        /// The length of this list is one longer than `keys`, so that we can avoid bounds logic.
        pub offs: O,
        /// Concatenated ordered lists of values, bracketed by offsets in `offs`.
        pub vals: V,
    }

    impl<O: for<'a> BatchContainer<ReadItem<'a> = usize>, V: BatchContainer> Default for Vals<O, V> {
        fn default() -> Self { Self::with_capacity(0, 0) }
    }

    impl<O: for<'a> BatchContainer<ReadItem<'a> = usize>, V: BatchContainer> Vals<O, V> {
        /// Lower and upper bounds in `self.vals` of the indexed list.
        #[inline(always)] pub fn bounds(&self, index: usize) -> (usize, usize) {
            (self.offs.index(index), self.offs.index(index+1))
        }
        /// Retrieves a value using relative indexes.
        ///
        /// The first index identifies a list, and the second an item within the list.
        /// The method adds the list's lower bound to the item index, and then calls
        /// `get_abs`. Using absolute indexes within the list's bounds can be more
        /// efficient than using relative indexing.
        pub fn get_rel(&self, list_idx: usize, item_idx: usize) -> V::ReadItem<'_> {
            self.get_abs(self.bounds(list_idx).0 + item_idx)
        }

        /// Number of lists in the container.
        pub fn len(&self) -> usize { self.offs.len() - 1 }
        /// Retrieves a value using an absolute rather than relative index.
        pub fn get_abs(&self, index: usize) -> V::ReadItem<'_> {
            self.vals.index(index)
        }
        /// Allocates with capacities for a number of lists and values.
        pub fn with_capacity(o_size: usize, v_size: usize) -> Self {
            let mut offs = <O as BatchContainer>::with_capacity(o_size);
            offs.push_ref(0);
            Self {
                offs,
                vals: <V as BatchContainer>::with_capacity(v_size),
            }
        }
        /// Allocates with enough capacity to contain two inputs.
        pub fn merge_capacity(this: &Self, that: &Self) -> Self {
            let mut offs = <O as BatchContainer>::with_capacity(this.offs.len() + that.offs.len());
            offs.push_ref(0);
            Self {
                offs,
                vals: <V as BatchContainer>::merge_capacity(&this.vals, &that.vals),
            }
        }
    }

    /// A container for non-empty lists of updates.
    ///
    /// This container uses the special representiation of an empty slice to stand in for
    /// "the previous single element". An empty slice is an otherwise invalid representation.
    #[derive(Debug, Serialize, Deserialize)]
    pub struct Upds<O, T, D> {
        /// Offsets used to provide indexes from values to updates.
        pub offs: O,
        /// Concatenated ordered lists of update times, bracketed by offsets in `offs`.
        pub times: T,
        /// Concatenated ordered lists of update diffs, bracketed by offsets in `offs`.
        pub diffs: D,
    }

    impl<O: for<'a> BatchContainer<ReadItem<'a> = usize>, T: BatchContainer, D: BatchContainer> Default for Upds<O, T, D> {
        fn default() -> Self { Self::with_capacity(0, 0) }
    }
    impl<O: for<'a> BatchContainer<ReadItem<'a> = usize>, T: BatchContainer, D: BatchContainer> Upds<O, T, D> {
        /// Lower and upper bounds in `self.times` and `self.diffs` of the indexed list.
        pub fn bounds(&self, index: usize) -> (usize, usize) {
            let mut lower = self.offs.index(index);
            let upper = self.offs.index(index+1);
            // We use equal lower and upper to encode "singleton update; just before here".
            // It should only apply when there is a prior element, so `lower` should be greater than zero.
            if lower == upper {
                assert!(lower > 0);
                lower -= 1;
            }
            (lower, upper)
        }
        /// Retrieves a value using relative indexes.
        ///
        /// The first index identifies a list, and the second an item within the list.
        /// The method adds the list's lower bound to the item index, and then calls
        /// `get_abs`. Using absolute indexes within the list's bounds can be more
        /// efficient than using relative indexing.
        pub fn get_rel(&self, list_idx: usize, item_idx: usize) -> (T::ReadItem<'_>, D::ReadItem<'_>) {
            self.get_abs(self.bounds(list_idx).0 + item_idx)
        }

        /// Number of lists in the container.
        pub fn len(&self) -> usize { self.offs.len() - 1 }
        /// Retrieves a value using an absolute rather than relative index.
        pub fn get_abs(&self, index: usize) -> (T::ReadItem<'_>, D::ReadItem<'_>) {
            (self.times.index(index), self.diffs.index(index))
        }
        /// Allocates with capacities for a number of lists and values.
        pub fn with_capacity(o_size: usize, u_size: usize) -> Self {
            let mut offs = <O as BatchContainer>::with_capacity(o_size);
            offs.push_ref(0);
            Self {
                offs,
                times: <T as BatchContainer>::with_capacity(u_size),
                diffs: <D as BatchContainer>::with_capacity(u_size),
            }
        }
        /// Allocates with enough capacity to contain two inputs.
        pub fn merge_capacity(this: &Self, that: &Self) -> Self {
            let mut offs = <O as BatchContainer>::with_capacity(this.offs.len() + that.offs.len());
            offs.push_ref(0);
            Self {
                offs,
                times: <T as BatchContainer>::merge_capacity(&this.times, &that.times),
                diffs: <D as BatchContainer>::merge_capacity(&this.diffs, &that.diffs),
            }
        }
    }

    /// Helper type for constructing `Upds` containers.
    pub struct UpdsBuilder<T: BatchContainer, D: BatchContainer> {
        /// Local stash of updates, to use for consolidation.
        ///
        /// We could emulate a `ChangeBatch` here, with related compaction smarts.
        /// A `ChangeBatch` itself needs an `i64` diff type, which we have not.
        stash: Vec<(T::Owned, D::Owned)>,
        /// Total number of consolidated updates.
        ///
        /// Tracked independently to account for duplicate compression.
        total: usize,

        /// Time container to stage singleton times for evaluation.
        time_con: T,
        /// Diff container to stage singleton times for evaluation.
        diff_con: D,
    }

    impl<T: BatchContainer, D: BatchContainer> Default for UpdsBuilder<T, D> {
        fn default() -> Self { Self { stash: Vec::default(), total: 0, time_con: BatchContainer::with_capacity(1), diff_con: BatchContainer::with_capacity(1) } }
    }


    impl<T, D> UpdsBuilder<T, D>
    where
        T: BatchContainer<Owned: Ord>,
        D: BatchContainer<Owned: crate::difference::Semigroup>,
    {
        /// Stages one update, but does not seal the set of updates.
        pub fn push(&mut self, time: T::Owned, diff: D::Owned) {
            self.stash.push((time, diff));
        }

        /// Consolidate and insert (if non-empty) the stashed updates.
        ///
        /// The return indicates whether the results were indeed non-empty.
        pub fn seal<O: for<'a> BatchContainer<ReadItem<'a> = usize>>(&mut self, upds: &mut Upds<O, T, D>) -> bool {
            use crate::consolidation;
            consolidation::consolidate(&mut self.stash);
            // If everything consolidates away, return false.
            if self.stash.is_empty() { return false; }
            // If there is a singleton, we may be able to optimize.
            if self.stash.len() == 1 {
                let (time, diff) = self.stash.last().unwrap();
                self.time_con.clear(); self.time_con.push_own(time);
                self.diff_con.clear(); self.diff_con.push_own(diff);
                if upds.times.last() == self.time_con.get(0) && upds.diffs.last() == self.diff_con.get(0) {
                    self.total += 1;
                    self.stash.clear();
                    upds.offs.push_ref(upds.times.len());
                    return true;
                }
            }
            // Conventional; move `stash` into `updates`.
            self.total += self.stash.len();
            for (time, diff) in self.stash.drain(..) {
                upds.times.push_own(&time);
                upds.diffs.push_own(&diff);
            }
            upds.offs.push_ref(upds.times.len());
            true
        }

        /// Completes the building and returns the total updates sealed.
        pub fn total(&self) -> usize { self.total }
    }
}

/// Types related to forming batches with values.
pub mod val_batch {

    use std::marker::PhantomData;
    use serde::{Deserialize, Serialize};
    use timely::container::PushInto;
    use timely::progress::{Antichain, frontier::AntichainRef};

    use crate::trace::{Batch, BatchReader, Builder, Cursor, Description, Merger};
    use crate::trace::implementations::{BatchContainer, BuilderInput};
    use crate::trace::implementations::layout;

    use super::{Layout, Vals, Upds, layers::UpdsBuilder};

    /// An immutable collection of update tuples, from a contiguous interval of logical times.
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(bound = "
        L::KeyContainer: Serialize + for<'a> Deserialize<'a>,
        L::ValContainer: Serialize + for<'a> Deserialize<'a>,
        L::OffsetContainer: Serialize + for<'a> Deserialize<'a>,
        L::TimeContainer: Serialize + for<'a> Deserialize<'a>,
        L::DiffContainer: Serialize + for<'a> Deserialize<'a>,
    ")]
    pub struct OrdValStorage<L: Layout> {
        /// An ordered list of keys.
        pub keys: L::KeyContainer,
        /// For each key in `keys`, a list of values.
        pub vals: Vals<L::OffsetContainer, L::ValContainer>,
        /// For each val in `vals`, a list of (time, diff) updates.
        pub upds: Upds<L::OffsetContainer, L::TimeContainer, L::DiffContainer>,
    }

    /// An immutable collection of update tuples, from a contiguous interval of logical times.
    ///
    /// The `L` parameter captures how the updates should be laid out, and `C` determines which
    /// merge batcher to select.
    #[derive(Serialize, Deserialize)]
    #[serde(bound = "
        L::KeyContainer: Serialize + for<'a> Deserialize<'a>,
        L::ValContainer: Serialize + for<'a> Deserialize<'a>,
        L::OffsetContainer: Serialize + for<'a> Deserialize<'a>,
        L::TimeContainer: Serialize + for<'a> Deserialize<'a>,
        L::DiffContainer: Serialize + for<'a> Deserialize<'a>,
    ")]
    pub struct OrdValBatch<L: Layout> {
        /// The updates themselves.
        pub storage: OrdValStorage<L>,
        /// Description of the update times this layer represents.
        pub description: Description<layout::Time<L>>,
        /// The number of updates reflected in the batch.
        ///
        /// We track this separately from `storage` because due to the singleton optimization,
        /// we may have many more updates than `storage.updates.len()`. It should equal that
        /// length, plus the number of singleton optimizations employed.
        pub updates: usize,
    }

    impl<L: Layout> WithLayout for OrdValBatch<L> {
        type Layout = L;
    }

    impl<L: Layout> crate::trace::Navigable for OrdValBatch<L> {
        type Cursor = OrdValCursor<L>;
        fn cursor(&self) -> Self::Cursor {
            OrdValCursor {
                key_cursor: 0,
                val_cursor: 0,
                phantom: PhantomData,
            }
        }
    }

    impl<L: Layout> BatchReader for OrdValBatch<L> {

        type Time = layout::Time<L>;
        fn len(&self) -> usize {
            // Normally this would be `self.updates.len()`, but we have a clever compact encoding.
            // Perhaps we should count such exceptions to the side, to provide a correct accounting.
            self.updates
        }
        fn description(&self) -> &Description<layout::Time<L>> { &self.description }
    }

    impl<L: Layout> Batch for OrdValBatch<L> {
        type Merger = OrdValMerger<L>;

        fn begin_merge(&self, other: &Self, compaction_frontier: AntichainRef<layout::Time<L>>) -> Self::Merger {
            OrdValMerger::new(self, other, compaction_frontier)
        }

        fn empty(lower: Antichain<Self::Time>, upper: Antichain<Self::Time>) -> Self {
            use timely::progress::Timestamp;
            Self {
                storage: OrdValStorage {
                    keys: L::KeyContainer::with_capacity(0),
                    vals: Default::default(),
                    upds: Default::default(),
                },
                description: Description::new(lower, upper, Antichain::from_elem(Self::Time::minimum())),
                updates: 0,
            }
        }
    }

    /// State for an in-progress merge.
    pub struct OrdValMerger<L: Layout> {
        /// Key position to merge next in the first batch.
        key_cursor1: usize,
        /// Key position to merge next in the second batch.
        key_cursor2: usize,
        /// result that we are currently assembling.
        result: OrdValStorage<L>,
        /// description
        description: Description<layout::Time<L>>,
        /// Staging area to consolidate owned times and diffs, before sealing.
        staging: UpdsBuilder<L::TimeContainer, L::DiffContainer>,
    }

    impl<L: Layout> Merger<OrdValBatch<L>> for OrdValMerger<L>
    where
        OrdValBatch<L>: Batch<Time=layout::Time<L>>,
    {
        fn new(batch1: &OrdValBatch<L>, batch2: &OrdValBatch<L>, compaction_frontier: AntichainRef<layout::Time<L>>) -> Self {

            assert!(batch1.upper() == batch2.lower());
            use crate::lattice::Lattice;
            let mut since = batch1.description().since().join(batch2.description().since());
            since = since.join(&compaction_frontier.to_owned());

            let description = Description::new(batch1.lower().clone(), batch2.upper().clone(), since);

            let batch1 = &batch1.storage;
            let batch2 = &batch2.storage;

            OrdValMerger {
                key_cursor1: 0,
                key_cursor2: 0,
                result: OrdValStorage {
                    keys: L::KeyContainer::merge_capacity(&batch1.keys, &batch2.keys),
                    vals: Vals::merge_capacity(&batch1.vals, &batch2.vals),
                    upds: Upds::merge_capacity(&batch1.upds, &batch2.upds),
                },
                description,
                staging: UpdsBuilder::default(),
            }
        }
        fn done(self) -> OrdValBatch<L> {
            OrdValBatch {
                updates: self.staging.total(),
                storage: self.result,
                description: self.description,
            }
        }
        fn work(&mut self, source1: &OrdValBatch<L>, source2: &OrdValBatch<L>, fuel: &mut isize) {

            // An (incomplete) indication of the amount of work we've done so far.
            let starting_updates = self.staging.total();
            let mut effort = 0isize;

            // While both mergees are still active, perform single-key merges.
            while self.key_cursor1 < source1.storage.keys.len() && self.key_cursor2 < source2.storage.keys.len() && effort < *fuel {
                self.merge_key(&source1.storage, &source2.storage);
                // An (incomplete) accounting of the work we've done.
                effort = (self.staging.total() - starting_updates) as isize;
            }

            // Merging is complete, and only copying remains.
            // Key-by-key copying allows effort interruption, and compaction.
            while self.key_cursor1 < source1.storage.keys.len() && effort < *fuel {
                self.copy_key(&source1.storage, self.key_cursor1);
                self.key_cursor1 += 1;
                effort = (self.staging.total() - starting_updates) as isize;
            }
            while self.key_cursor2 < source2.storage.keys.len() && effort < *fuel {
                self.copy_key(&source2.storage, self.key_cursor2);
                self.key_cursor2 += 1;
                effort = (self.staging.total() - starting_updates) as isize;
            }

            *fuel -= effort;
        }
    }

    // Helper methods in support of merging batches.
    impl<L: Layout> OrdValMerger<L> {
        /// Copy the next key in `source`.
        ///
        /// The method extracts the key in `source` at `cursor`, and merges it in to `self`.
        /// If the result does not wholly cancel, they key will be present in `self` with the
        /// compacted values and updates.
        ///
        /// The caller should be certain to update the cursor, as this method does not do this.
        fn copy_key(&mut self, source: &OrdValStorage<L>, cursor: usize) {
            // Capture the initial number of values to determine if the merge was ultimately non-empty.
            let init_vals = self.result.vals.vals.len();
            let (mut lower, upper) = source.vals.bounds(cursor);
            while lower < upper {
                self.stash_updates_for_val(source, lower);
                if self.staging.seal(&mut self.result.upds) {
                    self.result.vals.vals.push_ref(source.vals.get_abs(lower));
                }
                lower += 1;
            }

            // If we have pushed any values, copy the key as well.
            if self.result.vals.vals.len() > init_vals {
                self.result.keys.push_ref(source.keys.index(cursor));
                self.result.vals.offs.push_ref(self.result.vals.vals.len());
            }
        }
        /// Merge the next key in each of `source1` and `source2` into `self`, updating the appropriate cursors.
        ///
        /// This method only merges a single key. It applies all compaction necessary, and may result in no output
        /// if the updates cancel either directly or after compaction.
        fn merge_key(&mut self, source1: &OrdValStorage<L>, source2: &OrdValStorage<L>) {
            use ::std::cmp::Ordering;
            match source1.keys.index(self.key_cursor1).cmp(&source2.keys.index(self.key_cursor2)) {
                Ordering::Less => {
                    self.copy_key(source1, self.key_cursor1);
                    self.key_cursor1 += 1;
                },
                Ordering::Equal => {
                    // Keys are equal; must merge all values from both sources for this one key.
                    let (lower1, upper1) = source1.vals.bounds(self.key_cursor1);
                    let (lower2, upper2) = source2.vals.bounds(self.key_cursor2);
                    if let Some(off) = self.merge_vals((source1, lower1, upper1), (source2, lower2, upper2)) {
                        self.result.keys.push_ref(source1.keys.index(self.key_cursor1));
                        self.result.vals.offs.push_ref(off);
                    }
                    // Increment cursors in either case; the keys are merged.
                    self.key_cursor1 += 1;
                    self.key_cursor2 += 1;
                },
                Ordering::Greater => {
                    self.copy_key(source2, self.key_cursor2);
                    self.key_cursor2 += 1;
                },
            }
        }
        /// Merge two ranges of values into `self`.
        ///
        /// If the compacted result contains values with non-empty updates, the function returns
        /// an offset that should be recorded to indicate the upper extent of the result values.
        fn merge_vals(
            &mut self,
            (source1, mut lower1, upper1): (&OrdValStorage<L>, usize, usize),
            (source2, mut lower2, upper2): (&OrdValStorage<L>, usize, usize),
        ) -> Option<usize> {
            // Capture the initial number of values to determine if the merge was ultimately non-empty.
            let init_vals = self.result.vals.vals.len();
            while lower1 < upper1 && lower2 < upper2 {
                // We compare values, and fold in updates for the lowest values;
                // if they are non-empty post-consolidation, we write the value.
                // We could multi-way merge and it wouldn't be very complicated.
                use ::std::cmp::Ordering;
                match source1.vals.get_abs(lower1).cmp(&source2.vals.get_abs(lower2)) {
                    Ordering::Less => {
                        // Extend stash by updates, with logical compaction applied.
                        self.stash_updates_for_val(source1, lower1);
                        if self.staging.seal(&mut self.result.upds) {
                            self.result.vals.vals.push_ref(source1.vals.get_abs(lower1));
                        }
                        lower1 += 1;
                    },
                    Ordering::Equal => {
                        self.stash_updates_for_val(source1, lower1);
                        self.stash_updates_for_val(source2, lower2);
                        if self.staging.seal(&mut self.result.upds) {
                            self.result.vals.vals.push_ref(source1.vals.get_abs(lower1));
                        }
                        lower1 += 1;
                        lower2 += 1;
                    },
                    Ordering::Greater => {
                        // Extend stash by updates, with logical compaction applied.
                        self.stash_updates_for_val(source2, lower2);
                        if self.staging.seal(&mut self.result.upds) {
                            self.result.vals.vals.push_ref(source2.vals.get_abs(lower2));
                        }
                        lower2 += 1;
                    },
                }
            }
            // Merging is complete, but we may have remaining elements to push.
            while lower1 < upper1 {
                self.stash_updates_for_val(source1, lower1);
                if self.staging.seal(&mut self.result.upds) {
                    self.result.vals.vals.push_ref(source1.vals.get_abs(lower1));
                }
                lower1 += 1;
            }
            while lower2 < upper2 {
                self.stash_updates_for_val(source2, lower2);
                if self.staging.seal(&mut self.result.upds) {
                    self.result.vals.vals.push_ref(source2.vals.get_abs(lower2));
                }
                lower2 += 1;
            }

            // Values being pushed indicate non-emptiness.
            if self.result.vals.vals.len() > init_vals {
                Some(self.result.vals.vals.len())
            } else {
                None
            }
        }

        /// Transfer updates for an indexed value in `source` into `self`, with compaction applied.
        fn stash_updates_for_val(&mut self, source: &OrdValStorage<L>, index: usize) {
            let (lower, upper) = source.upds.bounds(index);
            for i in lower .. upper {
                // NB: Here is where we would need to look back if `lower == upper`.
                let (time, diff) = source.upds.get_abs(i);
                use crate::lattice::Lattice;
                let mut new_time: layout::Time<L> = L::TimeContainer::into_owned(time);
                new_time.advance_by(self.description.since().borrow());
                self.staging.push(new_time, L::DiffContainer::into_owned(diff));
            }
        }
    }

    /// A cursor for navigating a single layer.
    pub struct OrdValCursor<L: Layout> {
        /// Absolute position of the current key.
        key_cursor: usize,
        /// Absolute position of the current value.
        val_cursor: usize,
        /// Phantom marker for Rust happiness.
        phantom: PhantomData<L>,
    }

    use crate::trace::implementations::WithLayout;
    impl<L: Layout> WithLayout for OrdValCursor<L> {
        type Layout = L;
    }

    impl<L: Layout> Cursor for OrdValCursor<L> {

        type Storage = OrdValBatch<L>;

        type KeyContainer = <L as Layout>::KeyContainer;
        type Key<'a> = <<L as Layout>::KeyContainer as BatchContainer>::ReadItem<'a>;
        type ValContainer = <L as Layout>::ValContainer;
        type Val<'a> = <<L as Layout>::ValContainer as BatchContainer>::ReadItem<'a>;
        type ValOwn = <<L as Layout>::ValContainer as BatchContainer>::Owned;
        type TimeContainer = <L as Layout>::TimeContainer;
        type TimeGat<'a> = <<L as Layout>::TimeContainer as BatchContainer>::ReadItem<'a>;
        type Time = <<L as Layout>::TimeContainer as BatchContainer>::Owned;
        type DiffContainer = <L as Layout>::DiffContainer;
        type DiffGat<'a> = <<L as Layout>::DiffContainer as BatchContainer>::ReadItem<'a>;
        type Diff = <<L as Layout>::DiffContainer as BatchContainer>::Owned;

        fn get_key<'a>(&self, storage: &'a Self::Storage) -> Option<Self::Key<'a>> { storage.storage.keys.get(self.key_cursor) }
        fn get_val<'a>(&self, storage: &'a Self::Storage) -> Option<Self::Val<'a>> { if self.val_valid(storage) { Some(self.val(storage)) } else { None } }

        fn key<'a>(&self, storage: &'a OrdValBatch<L>) -> Self::Key<'a> { storage.storage.keys.index(self.key_cursor) }
        fn val<'a>(&self, storage: &'a OrdValBatch<L>) -> Self::Val<'a> { storage.storage.vals.get_abs(self.val_cursor) }
        fn map_times<L2: FnMut(Self::TimeGat<'_>, Self::DiffGat<'_>)>(&mut self, storage: &OrdValBatch<L>, mut logic: L2) {
            let (lower, upper) = storage.storage.upds.bounds(self.val_cursor);
            for index in lower .. upper {
                let (time, diff) = storage.storage.upds.get_abs(index);
                logic(time, diff);
            }
        }
        fn key_valid(&self, storage: &OrdValBatch<L>) -> bool { self.key_cursor < storage.storage.keys.len() }
        fn val_valid(&self, storage: &OrdValBatch<L>) -> bool { self.val_cursor < storage.storage.vals.bounds(self.key_cursor).1 }
        fn step_key(&mut self, storage: &OrdValBatch<L>){
            self.key_cursor += 1;
            if self.key_valid(storage) {
                self.rewind_vals(storage);
            }
            else {
                self.key_cursor = storage.storage.keys.len();
            }
        }
        fn seek_key(&mut self, storage: &OrdValBatch<L>, key: Self::Key<'_>) {
            self.key_cursor += storage.storage.keys.advance(self.key_cursor, storage.storage.keys.len(), |x| <L::KeyContainer as BatchContainer>::reborrow(x).lt(&<L::KeyContainer as BatchContainer>::reborrow(key)));
            if self.key_valid(storage) {
                self.rewind_vals(storage);
            }
        }
        fn step_val(&mut self, storage: &OrdValBatch<L>) {
            self.val_cursor += 1;
            if !self.val_valid(storage) {
                self.val_cursor = storage.storage.vals.bounds(self.key_cursor).1;
            }
        }
        fn seek_val(&mut self, storage: &OrdValBatch<L>, val: Self::Val<'_>) {
            self.val_cursor += storage.storage.vals.vals.advance(self.val_cursor, storage.storage.vals.bounds(self.key_cursor).1, |x| <L::ValContainer as BatchContainer>::reborrow(x).lt(&<L::ValContainer as BatchContainer>::reborrow(val)));
        }
        fn rewind_keys(&mut self, storage: &OrdValBatch<L>) {
            self.key_cursor = 0;
            if self.key_valid(storage) {
                self.rewind_vals(storage)
            }
        }
        fn rewind_vals(&mut self, storage: &OrdValBatch<L>) {
            self.val_cursor = storage.storage.vals.bounds(self.key_cursor).0;
        }
    }

    /// A builder for creating layers from unsorted update tuples.
    pub struct OrdValBuilder<L: Layout, CI> {
        /// The in-progress result.
        ///
        /// This is public to allow container implementors to set and inspect their container.
        pub result: OrdValStorage<L>,
        staging: UpdsBuilder<L::TimeContainer, L::DiffContainer>,
        _marker: PhantomData<CI>,
    }

    impl<L, CI> Builder for OrdValBuilder<L, CI>
    where
        L: for<'a> Layout<
            KeyContainer: PushInto<CI::Key<'a>>,
            ValContainer: PushInto<CI::Val<'a>>,
        >,
        CI: for<'a> BuilderInput<L::KeyContainer, L::ValContainer, Time=layout::Time<L>, Diff=layout::Diff<L>>,
    {

        type Input = CI;
        type Time = layout::Time<L>;
        type Output = OrdValBatch<L>;

        fn with_capacity(keys: usize, vals: usize, upds: usize) -> Self {
            Self {
                result: OrdValStorage {
                    keys: L::KeyContainer::with_capacity(keys),
                    vals: Vals::with_capacity(keys + 1, vals),
                    upds: Upds::with_capacity(vals + 1, upds),
                },
                staging: UpdsBuilder::default(),
                _marker: PhantomData,
            }
        }

        #[inline]
        fn push(&mut self, chunk: &mut Self::Input) {
            for item in chunk.drain() {
                let (key, val, time, diff) = CI::into_parts(item);

                // Pre-load the first update.
                if self.result.keys.is_empty() {
                    self.result.vals.vals.push_into(val);
                    self.result.keys.push_into(key);
                    self.staging.push(time, diff);
                }
                // Perhaps this is a continuation of an already received key.
                else if self.result.keys.last().map(|k| CI::key_eq(&key, k)).unwrap_or(false) {
                    // Perhaps this is a continuation of an already received value.
                    if self.result.vals.vals.last().map(|v| CI::val_eq(&val, v)).unwrap_or(false) {
                        self.staging.push(time, diff);
                    } else {
                        // New value; complete representation of prior value.
                        self.staging.seal(&mut self.result.upds);
                        self.staging.push(time, diff);
                        self.result.vals.vals.push_into(val);
                    }
                } else {
                    // New key; complete representation of prior key.
                    self.staging.seal(&mut self.result.upds);
                    self.staging.push(time, diff);
                    self.result.vals.offs.push_ref(self.result.vals.vals.len());
                    self.result.vals.vals.push_into(val);
                    self.result.keys.push_into(key);
                }
            }
        }

        #[inline(never)]
        fn done(mut self, description: Description<Self::Time>) -> OrdValBatch<L> {
            self.staging.seal(&mut self.result.upds);
            self.result.vals.offs.push_ref(self.result.vals.vals.len());
            OrdValBatch {
                updates: self.staging.total(),
                storage: self.result,
                description,
            }
        }

        fn seal(chain: &mut Vec<Self::Input>, description: Description<Self::Time>) -> Self::Output {
            let (keys, vals, upds) = Self::Input::key_val_upd_counts(&chain[..]);
            let mut builder = Self::with_capacity(keys, vals, upds);
            for mut chunk in chain.drain(..) {
                builder.push(&mut chunk);
            }

            builder.done(description)
        }
    }
}

/// Types related to forming batches of keys.
pub mod key_batch {

    use std::marker::PhantomData;
    use serde::{Deserialize, Serialize};
    use timely::container::PushInto;
    use timely::progress::{Antichain, frontier::AntichainRef};

    use crate::trace::{Batch, BatchReader, Builder, Cursor, Description, Merger};
    use crate::trace::implementations::{BatchContainer, BuilderInput};
    use crate::trace::implementations::layout;

    use super::{Layout, Upds, layers::UpdsBuilder};

    /// An immutable collection of update tuples, from a contiguous interval of logical times.
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(bound = "
        L::KeyContainer: Serialize + for<'a> Deserialize<'a>,
        L::OffsetContainer: Serialize + for<'a> Deserialize<'a>,
        L::TimeContainer: Serialize + for<'a> Deserialize<'a>,
        L::DiffContainer: Serialize + for<'a> Deserialize<'a>,
    ")]
    pub struct OrdKeyStorage<L: Layout> {
        /// An ordered list of keys, corresponding to entries in `keys_offs`.
        pub keys: L::KeyContainer,
        /// For each key in `keys`, a list of (time, diff) updates.
        pub upds: Upds<L::OffsetContainer, L::TimeContainer, L::DiffContainer>,
    }

    /// An immutable collection of update tuples, from a contiguous interval of logical times.
    ///
    /// The `L` parameter captures how the updates should be laid out, and `C` determines which
    /// merge batcher to select.
    #[derive(Serialize, Deserialize)]
    #[serde(bound = "
        L::KeyContainer: Serialize + for<'a> Deserialize<'a>,
        L::ValContainer: Serialize + for<'a> Deserialize<'a>,
        L::OffsetContainer: Serialize + for<'a> Deserialize<'a>,
        L::TimeContainer: Serialize + for<'a> Deserialize<'a>,
        L::DiffContainer: Serialize + for<'a> Deserialize<'a>,
    ")]
    pub struct OrdKeyBatch<L: Layout> {
        /// The updates themselves.
        pub storage: OrdKeyStorage<L>,
        /// Description of the update times this layer represents.
        pub description: Description<layout::Time<L>>,
        /// The number of updates reflected in the batch.
        ///
        /// We track this separately from `storage` because due to the singleton optimization,
        /// we may have many more updates than `storage.updates.len()`. It should equal that
        /// length, plus the number of singleton optimizations employed.
        pub updates: usize,

        /// Single value to return if asked.
        pub value: L::ValContainer,
    }

    impl<L: Layout<ValContainer: BatchContainer<Owned: Default>>> OrdKeyBatch<L> {
        /// Creates a container with one value, to slot in to `self.value`.
        pub fn create_value() -> L::ValContainer {
            let mut value = L::ValContainer::with_capacity(1);
            value.push_own(&Default::default());
            value
        }
    }

    impl<L: Layout<ValContainer: BatchContainer<Owned: Default>>> WithLayout for OrdKeyBatch<L> {
        type Layout = L;
    }

    impl<L: Layout<ValContainer: BatchContainer<Owned: Default>>> crate::trace::Navigable for OrdKeyBatch<L> {
        type Cursor = OrdKeyCursor<L>;
        fn cursor(&self) -> Self::Cursor {
            OrdKeyCursor {
                key_cursor: 0,
                val_stepped: false,
                phantom: std::marker::PhantomData,
            }
        }
    }

    impl<L: Layout<ValContainer: BatchContainer<Owned: Default>>> BatchReader for OrdKeyBatch<L> {

        type Time = layout::Time<L>;
        fn len(&self) -> usize {
            // Normally this would be `self.updates.len()`, but we have a clever compact encoding.
            // Perhaps we should count such exceptions to the side, to provide a correct accounting.
            self.updates
        }
        fn description(&self) -> &Description<layout::Time<L>> { &self.description }
    }

    impl<L: Layout<ValContainer: BatchContainer<Owned: Default>>> Batch for OrdKeyBatch<L> {
        type Merger = OrdKeyMerger<L>;

        fn begin_merge(&self, other: &Self, compaction_frontier: AntichainRef<layout::Time<L>>) -> Self::Merger {
            OrdKeyMerger::new(self, other, compaction_frontier)
        }

        fn empty(lower: Antichain<Self::Time>, upper: Antichain<Self::Time>) -> Self {
            use timely::progress::Timestamp;
            Self {
                storage: OrdKeyStorage {
                    keys: L::KeyContainer::with_capacity(0),
                    upds: Upds::default(),
                },
                description: Description::new(lower, upper, Antichain::from_elem(Self::Time::minimum())),
                updates: 0,
                value: Self::create_value(),
            }
        }
    }

    /// State for an in-progress merge.
    pub struct OrdKeyMerger<L: Layout> {
        /// Key position to merge next in the first batch.
        key_cursor1: usize,
        /// Key position to merge next in the second batch.
        key_cursor2: usize,
        /// result that we are currently assembling.
        result: OrdKeyStorage<L>,
        /// description
        description: Description<layout::Time<L>>,

        /// Local stash of updates, to use for consolidation.
        staging: UpdsBuilder<L::TimeContainer, L::DiffContainer>,
    }

    impl<L: Layout<ValContainer: BatchContainer<Owned: Default>>> Merger<OrdKeyBatch<L>> for OrdKeyMerger<L>
    where
        OrdKeyBatch<L>: Batch<Time=layout::Time<L>>,
    {
        fn new(batch1: &OrdKeyBatch<L>, batch2: &OrdKeyBatch<L>, compaction_frontier: AntichainRef<layout::Time<L>>) -> Self {

            assert!(batch1.upper() == batch2.lower());
            use crate::lattice::Lattice;
            let mut since = batch1.description().since().join(batch2.description().since());
            since = since.join(&compaction_frontier.to_owned());

            let description = Description::new(batch1.lower().clone(), batch2.upper().clone(), since);

            let batch1 = &batch1.storage;
            let batch2 = &batch2.storage;

            OrdKeyMerger {
                key_cursor1: 0,
                key_cursor2: 0,
                result: OrdKeyStorage {
                    keys: L::KeyContainer::merge_capacity(&batch1.keys, &batch2.keys),
                    upds: Upds::merge_capacity(&batch1.upds, &batch2.upds),
                },
                description,
                staging: UpdsBuilder::default(),
            }
        }
        fn done(self) -> OrdKeyBatch<L> {
            OrdKeyBatch {
                updates: self.staging.total(),
                storage: self.result,
                description: self.description,
                value: OrdKeyBatch::<L>::create_value(),
            }
        }
        fn work(&mut self, source1: &OrdKeyBatch<L>, source2: &OrdKeyBatch<L>, fuel: &mut isize) {

            // An (incomplete) indication of the amount of work we've done so far.
            let starting_updates = self.staging.total();
            let mut effort = 0isize;

            // While both mergees are still active, perform single-key merges.
            while self.key_cursor1 < source1.storage.keys.len() && self.key_cursor2 < source2.storage.keys.len() && effort < *fuel {
                self.merge_key(&source1.storage, &source2.storage);
                // An (incomplete) accounting of the work we've done.
                effort = (self.staging.total() - starting_updates) as isize;
            }

            // Merging is complete, and only copying remains.
            // Key-by-key copying allows effort interruption, and compaction.
            while self.key_cursor1 < source1.storage.keys.len() && effort < *fuel {
                self.copy_key(&source1.storage, self.key_cursor1);
                self.key_cursor1 += 1;
                effort = (self.staging.total() - starting_updates) as isize;
            }
            while self.key_cursor2 < source2.storage.keys.len() && effort < *fuel {
                self.copy_key(&source2.storage, self.key_cursor2);
                self.key_cursor2 += 1;
                effort = (self.staging.total() - starting_updates) as isize;
            }

            *fuel -= effort;
        }
    }

    // Helper methods in support of merging batches.
    impl<L: Layout> OrdKeyMerger<L> {
        /// Copy the next key in `source`.
        ///
        /// The method extracts the key in `source` at `cursor`, and merges it in to `self`.
        /// If the result does not wholly cancel, they key will be present in `self` with the
        /// compacted values and updates.
        ///
        /// The caller should be certain to update the cursor, as this method does not do this.
        fn copy_key(&mut self, source: &OrdKeyStorage<L>, cursor: usize) {
            self.stash_updates_for_key(source, cursor);
            if self.staging.seal(&mut self.result.upds) {
                self.result.keys.push_ref(source.keys.index(cursor));
            }
        }
        /// Merge the next key in each of `source1` and `source2` into `self`, updating the appropriate cursors.
        ///
        /// This method only merges a single key. It applies all compaction necessary, and may result in no output
        /// if the updates cancel either directly or after compaction.
        fn merge_key(&mut self, source1: &OrdKeyStorage<L>, source2: &OrdKeyStorage<L>) {
            use ::std::cmp::Ordering;
            match source1.keys.index(self.key_cursor1).cmp(&source2.keys.index(self.key_cursor2)) {
                Ordering::Less => {
                    self.copy_key(source1, self.key_cursor1);
                    self.key_cursor1 += 1;
                },
                Ordering::Equal => {
                    // Keys are equal; must merge all updates from both sources for this one key.
                    self.stash_updates_for_key(source1, self.key_cursor1);
                    self.stash_updates_for_key(source2, self.key_cursor2);
                    if self.staging.seal(&mut self.result.upds) {
                        self.result.keys.push_ref(source1.keys.index(self.key_cursor1));
                    }
                    // Increment cursors in either case; the keys are merged.
                    self.key_cursor1 += 1;
                    self.key_cursor2 += 1;
                },
                Ordering::Greater => {
                    self.copy_key(source2, self.key_cursor2);
                    self.key_cursor2 += 1;
                },
            }
        }

        /// Transfer updates for an indexed value in `source` into `self`, with compaction applied.
        fn stash_updates_for_key(&mut self, source: &OrdKeyStorage<L>, index: usize) {
            let (lower, upper) = source.upds.bounds(index);
            for i in lower .. upper {
                // NB: Here is where we would need to look back if `lower == upper`.
                let (time, diff) = source.upds.get_abs(i);
                use crate::lattice::Lattice;
                let mut new_time = L::TimeContainer::into_owned(time);
                new_time.advance_by(self.description.since().borrow());
                self.staging.push(new_time, L::DiffContainer::into_owned(diff));
            }
        }
    }

    /// A cursor for navigating a single layer.
    pub struct OrdKeyCursor<L: Layout> {
        /// Absolute position of the current key.
        key_cursor: usize,
        /// If the value has been stepped for the key, there are no more values.
        val_stepped: bool,
        /// Phantom marker for Rust happiness.
        phantom: PhantomData<L>,
    }

    use crate::trace::implementations::WithLayout;
    impl<L: Layout<ValContainer: BatchContainer>> WithLayout for OrdKeyCursor<L> {
        type Layout = L;
    }

    impl<L: for<'a> Layout<ValContainer: BatchContainer<Owned: Default>>> Cursor for OrdKeyCursor<L> {

        type Storage = OrdKeyBatch<L>;

        type KeyContainer = <L as Layout>::KeyContainer;
        type Key<'a> = <<L as Layout>::KeyContainer as BatchContainer>::ReadItem<'a>;
        type ValContainer = <L as Layout>::ValContainer;
        type Val<'a> = <<L as Layout>::ValContainer as BatchContainer>::ReadItem<'a>;
        type ValOwn = <<L as Layout>::ValContainer as BatchContainer>::Owned;
        type TimeContainer = <L as Layout>::TimeContainer;
        type TimeGat<'a> = <<L as Layout>::TimeContainer as BatchContainer>::ReadItem<'a>;
        type Time = <<L as Layout>::TimeContainer as BatchContainer>::Owned;
        type DiffContainer = <L as Layout>::DiffContainer;
        type DiffGat<'a> = <<L as Layout>::DiffContainer as BatchContainer>::ReadItem<'a>;
        type Diff = <<L as Layout>::DiffContainer as BatchContainer>::Owned;

        fn get_key<'a>(&self, storage: &'a Self::Storage) -> Option<Self::Key<'a>> { storage.storage.keys.get(self.key_cursor) }
        fn get_val<'a>(&self, storage: &'a Self::Storage) -> Option<Self::Val<'a>> { if self.val_valid(storage) { Some(self.val(storage)) } else { None } }

        fn key<'a>(&self, storage: &'a Self::Storage) -> Self::Key<'a> { storage.storage.keys.index(self.key_cursor) }
        fn val<'a>(&self, storage: &'a Self::Storage) -> Self::Val<'a> { storage.value.index(0) }
        fn map_times<L2: FnMut(Self::TimeGat<'_>, Self::DiffGat<'_>)>(&mut self, storage: &Self::Storage, mut logic: L2) {
            let (lower, upper) = storage.storage.upds.bounds(self.key_cursor);
            for index in lower .. upper {
                let (time, diff) = storage.storage.upds.get_abs(index);
                logic(time, diff);
            }
        }
        fn key_valid(&self, storage: &Self::Storage) -> bool { self.key_cursor < storage.storage.keys.len() }
        fn val_valid(&self, _storage: &Self::Storage) -> bool { !self.val_stepped }
        fn step_key(&mut self, storage: &Self::Storage){
            self.key_cursor += 1;
            if self.key_valid(storage) {
                self.rewind_vals(storage);
            }
            else {
                self.key_cursor = storage.storage.keys.len();
            }
        }
        fn seek_key(&mut self, storage: &Self::Storage, key: Self::Key<'_>) {
            self.key_cursor += storage.storage.keys.advance(self.key_cursor, storage.storage.keys.len(), |x| <L::KeyContainer as BatchContainer>::reborrow(x).lt(&<L::KeyContainer as BatchContainer>::reborrow(key)));
            if self.key_valid(storage) {
                self.rewind_vals(storage);
            }
        }
        fn step_val(&mut self, _storage: &Self::Storage) {
            self.val_stepped = true;
        }
        fn seek_val(&mut self, _storage: &Self::Storage, _val: Self::Val<'_>) { }
        fn rewind_keys(&mut self, storage: &Self::Storage) {
            self.key_cursor = 0;
            if self.key_valid(storage) {
                self.rewind_vals(storage)
            }
        }
        fn rewind_vals(&mut self, _storage: &Self::Storage) {
            self.val_stepped = false;
        }
    }

    /// A builder for creating layers from unsorted update tuples.
    pub struct OrdKeyBuilder<L: Layout, CI> {
        /// The in-progress result.
        ///
        /// This is public to allow container implementors to set and inspect their container.
        pub result: OrdKeyStorage<L>,
        staging: UpdsBuilder<L::TimeContainer, L::DiffContainer>,
        _marker: PhantomData<CI>,
    }

    impl<L: Layout, CI> Builder for OrdKeyBuilder<L, CI>
    where
        L: for<'a> Layout<KeyContainer: PushInto<CI::Key<'a>>>,
        L: Layout<ValContainer: BatchContainer<Owned: Default>>,
        CI: BuilderInput<L::KeyContainer, L::ValContainer, Time=layout::Time<L>, Diff=layout::Diff<L>>,
    {

        type Input = CI;
        type Time = layout::Time<L>;
        type Output = OrdKeyBatch<L>;

        fn with_capacity(keys: usize, _vals: usize, upds: usize) -> Self {
            Self {
                result: OrdKeyStorage {
                    keys: L::KeyContainer::with_capacity(keys),
                    upds: Upds::with_capacity(keys+1, upds),
                },
                staging: UpdsBuilder::default(),
                _marker: PhantomData,
            }
        }

        #[inline]
        fn push(&mut self, chunk: &mut Self::Input) {
            for item in chunk.drain() {
                let (key, _val, time, diff) = CI::into_parts(item);
                if self.result.keys.is_empty() {
                    self.result.keys.push_into(key);
                    self.staging.push(time, diff);
                }
                // Perhaps this is a continuation of an already received key.
                else if self.result.keys.last().map(|k| CI::key_eq(&key, k)).unwrap_or(false) {
                    self.staging.push(time, diff);
                } else {
                    self.staging.seal(&mut self.result.upds);
                    self.staging.push(time, diff);
                    self.result.keys.push_into(key);
                }
            }
        }

        #[inline(never)]
        fn done(mut self, description: Description<Self::Time>) -> OrdKeyBatch<L> {
            self.staging.seal(&mut self.result.upds);
            OrdKeyBatch {
                updates: self.staging.total(),
                storage: self.result,
                description,
                value: OrdKeyBatch::<L>::create_value(),
            }
        }

        fn seal(chain: &mut Vec<Self::Input>, description: Description<Self::Time>) -> Self::Output {
            let (keys, vals, upds) = Self::Input::key_val_upd_counts(&chain[..]);
            let mut builder = Self::with_capacity(keys, vals, upds);
            for mut chunk in chain.drain(..) {
                builder.push(&mut chunk);
            }

            builder.done(description)
        }
    }

}
