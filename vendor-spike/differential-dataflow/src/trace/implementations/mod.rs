//! Implementations of `Trace` and associated traits.
//!
//! The `Trace` trait provides access to an ordered collection of `(key, val, time, diff)` tuples, but
//! there is substantial flexibility in implementations of this trait. Depending on characteristics of
//! the data, we may wish to represent the data in different ways. This module contains several of these
//! implementations, and combiners for merging the results of different traces.
//!
//! As examples of implementations,
//!
//! *  The `trie` module is meant to represent general update tuples, with no particular assumptions made
//!    about their contents. It organizes the data first by key, then by val, and then leaves the rest
//!    in an unordered pile.
//!
//! *  The `keys` module is meant for collections whose value type is `()`, which is to say there is no
//!    (key, val) structure on the records; all of them are just viewed as "keys".
//!
//! *  The `time` module is meant for collections with a single time value. This can remove repetition
//!    from the representation, at the cost of requiring more instances and run-time merging.
//!
//! *  The `base` module is meant for collections with a single time value equivalent to the least time.
//!    These collections must always accumulate to non-negative collections, and as such we can indicate
//!    the frequency of an element by its multiplicity. This removes both the time and weight from the
//!    representation, but is only appropriate for a subset (often substantial) of the data.
//!
//! Each of these representations is best suited for different data, but they can be combined to get the
//! benefits of each, as appropriate. There are several `Cursor` combiners, `CursorList` and `CursorPair`,
//! for homogeneous and inhomogeneous cursors, respectively.
//!
//! #Musings
//!
//! What is less clear is how to transfer updates between the representations at merge time in a tasteful
//! way. Perhaps we could put an ordering on the representations, each pair with a dominant representation,
//! and part of merging the latter filters updates into the former. Although back and forth might be
//! appealing, more thinking is required to negotiate all of these policies.
//!
//! One option would be to require the layer builder to handle these smarts. Merging is currently done by
//! the layer as part of custom code, but we could make it simply be "iterate through cursor, push results
//! into 'ordered builder'". Then the builder would be bright enough to emit a "batch" for the composite
//! trace, rather than just a batch of the type merged.

pub mod spine_fueled;

pub mod merge_batcher;
pub mod ord_neu;
pub mod chunker;

// Opinionated takes on default spines.
pub use self::chunker::ContainerChunker;
pub use self::ord_neu::OrdValSpine as ValSpine;
pub use self::ord_neu::OrdValBatcher as ValBatcher;
pub use self::ord_neu::RcOrdValBuilder as ValBuilder;
pub use self::ord_neu::OrdKeySpine as KeySpine;
pub use self::ord_neu::OrdKeyBatcher as KeyBatcher;
pub use self::ord_neu::RcOrdKeyBuilder as KeyBuilder;

use std::convert::TryInto;

use serde::{Deserialize, Serialize};
use timely::container::{DrainContainer, PushInto};
use timely::progress::Timestamp;

use crate::lattice::Lattice;
use crate::difference::Semigroup;

/// A type that names constituent update types.
pub trait Update {
    /// Key by which data are grouped.
    type Key: Ord + Clone + 'static;
    /// Values associated with the key.
    type Val: Ord + Clone + 'static;
    /// Time at which updates occur.
    type Time: Lattice + timely::progress::Timestamp;
    /// Way in which updates occur.
    type Diff: Ord + Semigroup + 'static;
}

impl<K,V,T,R> Update for ((K, V), T, R)
where
    K: Ord+Clone+'static,
    V: Ord+Clone+'static,
    T: Lattice+timely::progress::Timestamp,
    R: Ord+Semigroup+'static,
{
    type Key = K;
    type Val = V;
    type Time = T;
    type Diff = R;
}

/// A type with opinions on how updates should be laid out.
pub trait Layout {
    /// Container for update keys.
    type KeyContainer: BatchContainer;
    /// Container for update vals.
    type ValContainer: BatchContainer;
    /// Container for times.
    type TimeContainer: BatchContainer<Owned: Lattice + timely::progress::Timestamp>;
    /// Container for diffs.
    type DiffContainer: BatchContainer<Owned: Semigroup + 'static>;
    /// Container for offsets.
    type OffsetContainer: for<'a> BatchContainer<ReadItem<'a> = usize>;
}

/// A type bearing a layout.
pub trait WithLayout {
    /// The layout.
    type Layout: Layout;
}


// An easy way to provide an explicit layout: as a 5-tuple.
// Valuable when one wants to perform layout surgery.
impl<KC, VC, TC, DC, OC> Layout for (KC, VC, TC, DC, OC)
where
    KC: BatchContainer,
    VC: BatchContainer,
    TC: BatchContainer<Owned: Lattice + timely::progress::Timestamp>,
    DC: BatchContainer<Owned: Semigroup>,
    OC: for<'a> BatchContainer<ReadItem<'a> = usize>,
{
    type KeyContainer = KC;
    type ValContainer = VC;
    type TimeContainer = TC;
    type DiffContainer = DC;
    type OffsetContainer = OC;
}

/// Aliases for types provided by the containers within a `Layout`.
///
/// For clarity, prefer `use layout;` and then `layout::Key<L>` to retain the layout context.
pub mod layout {
    use crate::trace::implementations::{BatchContainer, Layout};

    /// Alias for an owned key of a layout.
    pub type Key<L> = <<L as Layout>::KeyContainer as BatchContainer>::Owned;
    /// Alias for an borrowed key of a layout.
    pub type KeyRef<'a, L> = <<L as Layout>::KeyContainer as BatchContainer>::ReadItem<'a>;
    /// Alias for an owned val of a layout.
    pub type Val<L> = <<L as Layout>::ValContainer as BatchContainer>::Owned;
    /// Alias for an borrowed val of a layout.
    pub type ValRef<'a, L> = <<L as Layout>::ValContainer as BatchContainer>::ReadItem<'a>;
    /// Alias for an owned time of a layout.
    pub type Time<L> = <<L as Layout>::TimeContainer as BatchContainer>::Owned;
    /// Alias for an borrowed time of a layout.
    pub type TimeRef<'a, L> = <<L as Layout>::TimeContainer as BatchContainer>::ReadItem<'a>;
    /// Alias for an owned diff of a layout.
    pub type Diff<L> = <<L as Layout>::DiffContainer as BatchContainer>::Owned;
    /// Alias for an borrowed diff of a layout.
    pub type DiffRef<'a, L> = <<L as Layout>::DiffContainer as BatchContainer>::ReadItem<'a>;
}

/// A layout that uses vectors
pub struct Vector<U: Update> {
    phantom: std::marker::PhantomData<U>,
}

impl<U: Update<Diff: Ord>> Layout for Vector<U> {
    type KeyContainer = Vec<U::Key>;
    type ValContainer = Vec<U::Val>;
    type TimeContainer = Vec<U::Time>;
    type DiffContainer = Vec<U::Diff>;
    type OffsetContainer = OffsetList;
}

/// A list of unsigned integers that uses `u32` elements as long as they are small enough, and switches to `u64` once they are not.
#[derive(Eq, PartialEq, Ord, PartialOrd, Clone, Serialize, Deserialize)]
pub struct OffsetList {
    /// Length of a prefix of zero elements.
    pub zero_prefix: usize,
    /// Offsets that fit within a `u32`.
    pub smol: Vec<u32>,
    /// Offsets that either do not fit in a `u32`, or are inserted after some offset that did not fit.
    pub chonk: Vec<u64>,
}

impl std::fmt::Debug for OffsetList {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.into_iter()).finish()
    }
}

impl OffsetList {
    /// Allocate a new list with a specified capacity.
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            zero_prefix: 0,
            smol: Vec::with_capacity(cap),
            chonk: Vec::new(),
        }
    }
    /// Inserts the offset, as a `u32` if that is still on the table.
    pub fn push(&mut self, offset: usize) {
        if self.smol.is_empty() && self.chonk.is_empty() && offset == 0 {
            self.zero_prefix += 1;
        }
        else if self.chonk.is_empty() {
            if let Ok(smol) = offset.try_into() {
                self.smol.push(smol);
            }
            else {
                self.chonk.push(offset.try_into().unwrap())
            }
        }
        else {
            self.chonk.push(offset.try_into().unwrap())
        }
    }
    /// Like `std::ops::Index`, which we cannot implement as it must return a `&usize`.
    pub fn index(&self, index: usize) -> usize {
        if index < self.zero_prefix {
            0
        }
        else if index - self.zero_prefix < self.smol.len() {
            self.smol[index - self.zero_prefix].try_into().unwrap()
        }
        else {
            self.chonk[index - self.zero_prefix - self.smol.len()].try_into().unwrap()
        }
    }
    /// The number of offsets in the list.
    pub fn len(&self) -> usize {
        self.zero_prefix + self.smol.len() + self.chonk.len()
    }
}

impl<'a> IntoIterator for &'a OffsetList {
    type Item = usize;
    type IntoIter = OffsetListIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        OffsetListIter {list: self, index: 0 }
    }
}

/// An iterator for [`OffsetList`].
pub struct OffsetListIter<'a> {
    list: &'a OffsetList,
    index: usize,
}

impl<'a> Iterator for OffsetListIter<'a> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.list.len() {
            let res = Some(self.list.index(self.index));
            self.index += 1;
            res
        } else {
            None
        }
    }
}

impl PushInto<usize> for OffsetList {
    fn push_into(&mut self, item: usize) {
        self.push(item);
    }
}

impl BatchContainer for OffsetList {
    type Owned = usize;
    type ReadItem<'a> = usize;

    #[inline(always)]
    fn into_owned<'a>(item: Self::ReadItem<'a>) -> Self::Owned { item }
    #[inline(always)]
    fn reborrow<'b, 'a: 'b>(item: Self::ReadItem<'a>) -> Self::ReadItem<'b> { item }

    fn push_ref(&mut self, item: Self::ReadItem<'_>) { self.push_into(item) }
    fn push_own(&mut self, item: &Self::Owned) { self.push_into(*item) }

    fn clear(&mut self) { self.zero_prefix = 0; self.smol.clear(); self.chonk.clear(); }

    fn with_capacity(size: usize) -> Self {
        Self::with_capacity(size)
    }

    fn merge_capacity(cont1: &Self, cont2: &Self) -> Self {
        Self::with_capacity(cont1.len() + cont2.len())
    }

    fn index(&self, index: usize) -> Self::ReadItem<'_> {
        self.index(index)
    }

    fn len(&self) -> usize {
        self.len()
    }
}

/// Behavior to split an update into principal components.
pub trait BuilderInput<K: BatchContainer, V: BatchContainer>: DrainContainer + Sized {
    /// Key portion
    type Key<'a>: Ord;
    /// Value portion
    type Val<'a>: Ord;
    /// Time
    type Time;
    /// Diff
    type Diff;

    /// Split an item into separate parts.
    fn into_parts<'a>(item: Self::Item<'a>) -> (Self::Key<'a>, Self::Val<'a>, Self::Time, Self::Diff);

    /// Test that the key equals a key in the layout's key container.
    fn key_eq(this: &Self::Key<'_>, other: K::ReadItem<'_>) -> bool;

    /// Test that the value equals a key in the layout's value container.
    fn val_eq(this: &Self::Val<'_>, other: V::ReadItem<'_>) -> bool;

    /// Count the number of distinct keys, (key, val) pairs, and total updates.
    fn key_val_upd_counts(chain: &[Self]) -> (usize, usize, usize);
}

impl<K,KBC,V,VBC,T,R> BuilderInput<KBC, VBC> for Vec<((K, V), T, R)>
where
    K: Ord + Clone + 'static,
    KBC: for<'a> BatchContainer<ReadItem<'a>: PartialEq<&'a K>>,
    V: Ord + Clone + 'static,
    VBC: for<'a> BatchContainer<ReadItem<'a>: PartialEq<&'a V>>,
    T: Timestamp + Lattice + 'static,
    R: Ord + Semigroup + 'static,
{
    type Key<'a> = K;
    type Val<'a> = V;
    type Time = T;
    type Diff = R;

    fn into_parts<'a>(((key, val), time, diff): Self::Item<'a>) -> (Self::Key<'a>, Self::Val<'a>, Self::Time, Self::Diff) {
        (key, val, time, diff)
    }

    fn key_eq(this: &K, other: KBC::ReadItem<'_>) -> bool {
        KBC::reborrow(other) == this
    }

    fn val_eq(this: &V, other: VBC::ReadItem<'_>) -> bool {
        VBC::reborrow(other) == this
    }

    fn key_val_upd_counts(chain: &[Self]) -> (usize, usize, usize) {
        let mut keys = 0;
        let mut vals = 0;
        let mut upds = 0;
        let mut prev_keyval = None;
        for link in chain.iter() {
            for ((key, val), _, _) in link.iter() {
                if let Some((p_key, p_val)) = prev_keyval {
                    if p_key != key {
                        keys += 1;
                        vals += 1;
                    } else if p_val != val {
                        vals += 1;
                    }
                } else {
                    keys += 1;
                    vals += 1;
                }
                upds += 1;
                prev_keyval = Some((key, val));
            }
        }
        (keys, vals, upds)
    }
}

pub use self::containers::{BatchContainer, SliceContainer};

/// Containers for data that resemble `Vec<T>`, with leaner implementations.
pub mod containers {

    use timely::container::PushInto;

    /// A general-purpose container resembling `Vec<T>`.
    pub trait BatchContainer: 'static {
        /// An owned instance of `Self::ReadItem<'_>`.
        type Owned: Clone + Ord;

        /// The type that can be read back out of the container.
        type ReadItem<'a>: Copy + Ord;


        /// Conversion from an instance of this type to the owned type.
        #[must_use]
        fn into_owned<'a>(item: Self::ReadItem<'a>) -> Self::Owned;
        /// Clones `self` onto an existing instance of the owned type.
        #[inline(always)]
        fn clone_onto<'a>(item: Self::ReadItem<'a>, other: &mut Self::Owned) {
            *other = Self::into_owned(item);
        }

        /// Push an item into this container
        fn push_ref(&mut self, item: Self::ReadItem<'_>);
        /// Push an item into this container
        fn push_own(&mut self, item: &Self::Owned);

        /// Clears the container. May not release resources.
        fn clear(&mut self);

        /// Creates a new container with sufficient capacity.
        fn with_capacity(size: usize) -> Self;
        /// Creates a new container with sufficient capacity.
        fn merge_capacity(cont1: &Self, cont2: &Self) -> Self;

        /// Converts a read item into one with a narrower lifetime.
        fn reborrow<'b, 'a: 'b>(item: Self::ReadItem<'a>) -> Self::ReadItem<'b>;

        /// Reference to the element at this position.
        fn index(&self, index: usize) -> Self::ReadItem<'_>;

        /// Reference to the element at this position, if it exists.
        fn get(&self, index: usize) -> Option<Self::ReadItem<'_>> {
            if index < self.len() {
                Some(self.index(index))
            }
            else { None }
        }

        /// Number of contained elements
        fn len(&self) -> usize;
        /// Returns the last item if the container is non-empty.
        fn last(&self) -> Option<Self::ReadItem<'_>> {
            if self.len() > 0 {
                Some(self.index(self.len()-1))
            }
            else {
                None
            }
        }
        /// Indicates if the length is zero.
        fn is_empty(&self) -> bool { self.len() == 0 }

        /// Reports the number of elements satisfying the predicate.
        ///
        /// This methods *relies strongly* on the assumption that the predicate
        /// stays false once it becomes false, a joint property of the predicate
        /// and the layout of `Self. This allows `advance` to use exponential search to
        /// count the number of elements in time logarithmic in the result.
        fn advance<F: for<'a> Fn(Self::ReadItem<'a>)->bool>(&self, start: usize, end: usize, function: F) -> usize {

            let small_limit = 8;

            // Exponential search if the answer isn't within `small_limit`.
            if end > start + small_limit && function(self.index(start + small_limit)) {

                // start with no advance
                let mut index = small_limit + 1;
                if start + index < end && function(self.index(start + index)) {

                    // advance in exponentially growing steps.
                    let mut step = 1;
                    while start + index + step < end && function(self.index(start + index + step)) {
                        index += step;
                        step <<= 1;
                    }

                    // advance in exponentially shrinking steps.
                    step >>= 1;
                    while step > 0 {
                        if start + index + step < end && function(self.index(start + index + step)) {
                            index += step;
                        }
                        step >>= 1;
                    }

                    index += 1;
                }

                index
            }
            else {
                let limit = std::cmp::min(end, start + small_limit);
                (start .. limit).filter(|x| function(self.index(*x))).count()
            }
        }
    }

    // All `T: Clone` also implement `ToOwned<Owned = T>`, but without the constraint Rust
    // struggles to understand why the owned type must be `T` (i.e. the one blanket impl).
    impl<T: Ord + Clone + 'static> BatchContainer for Vec<T> {
        type Owned = T;
        type ReadItem<'a> = &'a T;

        #[inline(always)] fn into_owned<'a>(item: Self::ReadItem<'a>) -> Self::Owned { item.clone() }
        #[inline(always)] fn clone_onto<'a>(item: Self::ReadItem<'a>, other: &mut Self::Owned) { other.clone_from(item); }

        fn reborrow<'b, 'a: 'b>(item: Self::ReadItem<'a>) -> Self::ReadItem<'b> { item }

        fn push_ref(&mut self, item: Self::ReadItem<'_>) { self.push_into(item) }
        fn push_own(&mut self, item: &Self::Owned) { self.push_into(item.clone()) }

        fn clear(&mut self) { self.clear() }

        fn with_capacity(size: usize) -> Self {
            Vec::with_capacity(size)
        }
        fn merge_capacity(cont1: &Self, cont2: &Self) -> Self {
            Vec::with_capacity(cont1.len() + cont2.len())
        }
        fn index(&self, index: usize) -> Self::ReadItem<'_> {
            &self[index]
        }
        fn get(&self, index: usize) -> Option<Self::ReadItem<'_>> {
            <[T]>::get(self, index)
        }
        fn len(&self) -> usize {
            self[..].len()
        }
    }

    /// A container that accepts slices `[B::Item]`.
    pub struct SliceContainer<B> {
        /// Offsets that bound each contained slice.
        ///
        /// The length will be one greater than the number of contained slices,
        /// starting with zero and ending with `self.inner.len()`.
        offsets: Vec<usize>,
        /// An inner container for sequences of `B` that dereferences to a slice.
        inner: Vec<B>,
    }

    impl<B: Ord + Clone + 'static> PushInto<&[B]> for SliceContainer<B> {
        fn push_into(&mut self, item: &[B]) {
            for x in item.iter() {
                self.inner.push_into(x);
            }
            self.offsets.push(self.inner.len());
        }
    }

    impl<B: Ord + Clone + 'static> PushInto<&Vec<B>> for SliceContainer<B> {
        fn push_into(&mut self, item: &Vec<B>) {
            self.push_into(&item[..]);
        }
    }

    impl<B> BatchContainer for SliceContainer<B>
    where
        B: Ord + Clone + Sized + 'static,
    {
        type Owned = Vec<B>;
        type ReadItem<'a> = &'a [B];

        #[inline(always)] fn into_owned<'a>(item: Self::ReadItem<'a>) -> Self::Owned { item.to_vec() }
        #[inline(always)] fn clone_onto<'a>(item: Self::ReadItem<'a>, other: &mut Self::Owned) { other.clone_from_slice(item); }

        fn reborrow<'b, 'a: 'b>(item: Self::ReadItem<'a>) -> Self::ReadItem<'b> { item }

        fn push_ref(&mut self, item: Self::ReadItem<'_>) { self.push_into(item) }
        fn push_own(&mut self, item: &Self::Owned) { self.push_into(item) }

        fn clear(&mut self) {
            self.offsets.clear();
            self.offsets.push(0);
            self.inner.clear();
        }

        fn with_capacity(size: usize) -> Self {
            let mut offsets = Vec::with_capacity(size + 1);
            offsets.push(0);
            Self {
                offsets,
                inner: Vec::with_capacity(size),
            }
        }
        fn merge_capacity(cont1: &Self, cont2: &Self) -> Self {
            let mut offsets = Vec::with_capacity(cont1.inner.len() + cont2.inner.len() + 1);
            offsets.push(0);
            Self {
                offsets,
                inner: Vec::with_capacity(cont1.inner.len() + cont2.inner.len()),
            }
        }
        fn index(&self, index: usize) -> Self::ReadItem<'_> {
            let lower = self.offsets[index];
            let upper = self.offsets[index+1];
            &self.inner[lower .. upper]
        }
        fn len(&self) -> usize {
            self.offsets.len() - 1
        }
    }

    /// Default implementation introduces a first offset.
    impl<B> Default for SliceContainer<B> {
        fn default() -> Self {
            Self {
                offsets: vec![0],
                inner: Default::default(),
            }
        }
    }
}
