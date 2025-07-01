// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Reusable containers.

use std::hash::Hash;

use columnar::Columnar;
use differential_dataflow::Hashable;

use crate::containers::merger::MergeBatcher;

pub(crate) use alloc::alloc_aligned_zeroed;
pub use alloc::{enable_columnar_lgalloc, set_enable_columnar_lgalloc};
pub use builder::ColumnBuilder;
pub use container::Column;
pub use dd_builder::{ColKeyBuilder as ColumnKeyBuilder, OrdValBuilder as ColumnValBuilder};
pub use provided_builder::ProvidedBuilder;

mod compressed;
mod merger;
pub mod stack;

pub static ENABLE_COLUMNAR_COMPRESSION: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

mod alloc {
    use mz_ore::region::Region;

    /// Allocate a region of memory with a capacity of at least `len` that is properly aligned
    /// and zeroed. The memory in Regions is always aligned to its content type.
    #[inline]
    pub(crate) fn alloc_aligned_zeroed<T: bytemuck::AnyBitPattern>(len: usize) -> Region<T> {
        if enable_columnar_lgalloc() {
            Region::new_auto_zeroed(len)
        } else {
            Region::new_heap_zeroed(len)
        }
    }

    thread_local! {
        static ENABLE_COLUMNAR_LGALLOC: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    }

    /// Returns `true` if columnar allocations should come from lgalloc.
    #[inline]
    pub fn enable_columnar_lgalloc() -> bool {
        ENABLE_COLUMNAR_LGALLOC.get()
    }

    /// Set whether columnar allocations should come from lgalloc. Applies to future allocations.
    pub fn set_enable_columnar_lgalloc(enabled: bool) {
        ENABLE_COLUMNAR_LGALLOC.set(enabled);
    }
}

pub trait PackedContainer: timely::Container {
    type State: Default;

    #[inline(always)]
    fn pack(&mut self, _state: &mut Self::State) {
        // Default implementation does nothing.
    }

    #[inline(always)]
    fn unpack(&mut self, _state: &mut Self::State) {
        // Default implementation does nothing.
    }
}

mod packed_container {
    use columnation::Columnation;
    use differential_dataflow::containers::TimelyStack;

    use crate::containers::PackedContainer;

    impl<T: Columnation> PackedContainer for TimelyStack<T> {
        type State = ();
    }
}

mod container {
    use columnar::Columnar;
    use columnar::Container as _;
    use columnar::bytes::{EncodeDecode, Indexed};
    use columnar::common::IterOwn;
    use columnar::{Clear, FromBytes, Index, Len};
    use mz_ore::region::Region;
    use timely::Container;
    use timely::bytes::arc::Bytes;
    use timely::container::PushInto;
    use timely::dataflow::channels::ContainerBytes;

    use crate::containers::compressed::CompressedColumn;
    use crate::containers::{ENABLE_COLUMNAR_COMPRESSION, PackedContainer};

    /// A container based on a columnar store, encoded in aligned bytes.
    ///
    /// The type can represent typed data, bytes from Timely, or an aligned allocation. The name
    /// is singular to express that the preferred format is [`Column::Align`]. The [`Column::Typed`]
    /// variant is used to construct the container, and it owns potentially multiple columns of data.
    pub enum Column<C: Columnar> {
        /// The typed variant of the container.
        Typed(C::Container),
        /// The binary variant of the container.
        Bytes(Bytes),
        /// Relocated, aligned binary data, if `Bytes` doesn't work for some reason.
        ///
        /// Reasons could include misalignment, cloning of data, or wanting
        /// to release the `Bytes` as a scarce resource.
        Align(Region<u64>),
        Compressed(CompressedColumn),
    }

    #[derive(Default)]
    pub struct PackState {
        /// Buffer to write into.
        buffer: Vec<u8>,
        /// Empty aligned allocation, used to store the aligned data.
        empty_aligned: Region<u64>,
        /// Empty compressed column, used to store the compressed data.
        empty_compressed: CompressedColumn,
    }

    impl<C: Columnar> Column<C> {
        /// Borrows the container as a reference.
        #[inline(always)]
        pub(crate) fn borrow(&self) -> <C::Container as columnar::Container<C>>::Borrowed<'_> {
            type Borrowed<'a, C> =
                <<C as Columnar>::Container as columnar::Container<C>>::Borrowed<'a>;
            match self {
                Column::Typed(t) => t.borrow(),
                Column::Bytes(b) => {
                    Borrowed::<C>::from_bytes(&mut Indexed::decode(bytemuck::cast_slice(b)))
                }
                Column::Align(a) => Borrowed::<C>::from_bytes(&mut Indexed::decode(a)),
                Column::Compressed(_) => panic!("Cannot borrow compressed column"),
            }
        }
        #[inline(always)]
        pub fn get(&self, index: usize) -> C::Ref<'_> {
            self.borrow().get(index)
        }
    }
    impl<C: Columnar> PackedContainer for Column<C> {
        type State = PackState;

        fn pack(&mut self, state: &mut PackState) {
            // container.compress(&mut state.0, &mut state.1);
            let len = if let Column::Align(_) = &self {
                self.len()
            } else {
                0
            };
            match self {
                Column::Typed(_) => {}
                Column::Bytes(_) => {}
                Column::Align(aligned) => {
                    if !ENABLE_COLUMNAR_COMPRESSION.load(std::sync::atomic::Ordering::Relaxed) {
                        return;
                    }
                    let aligned = std::mem::take(aligned);
                    *self = Column::Compressed(CompressedColumn::compress_aligned(
                        len,
                        &aligned,
                        &mut state.buffer,
                        &mut state.empty_compressed,
                    ));
                    state.empty_aligned = aligned;
                }
                Column::Compressed(_) => {}
            }
        }
        fn unpack(&mut self, state: &mut PackState) {
            // container.decompress(&mut state.0, &mut state.1);
            match self {
                Column::Typed(_) => {}
                Column::Bytes(_) => {}
                Column::Align(_) => {}
                Column::Compressed(compressed) => {
                    let compressed = std::mem::take(compressed);
                    let elements = compressed.elements();
                    let uncompressed_size = compressed.uncompressed_size() / 8;
                    let mut aligned = std::mem::take(&mut state.empty_aligned);
                    if aligned.capacity() < compressed.uncompressed_size() {
                        aligned = super::alloc_aligned_zeroed(uncompressed_size);
                    } else {
                        unsafe { aligned.clear() };
                        aligned.extend(std::iter::repeat(0).take(uncompressed_size));
                    }
                    compressed.decompress(&mut aligned[..uncompressed_size]);
                    *self = Column::Align(aligned);
                    state.empty_compressed = compressed;
                    assert_eq!(
                        self.len(),
                        elements,
                        "Decompressed length does not match expected length"
                    );
                }
            }
        }
    }

    impl<C: Columnar> Default for Column<C> {
        #[inline(always)]
        fn default() -> Self {
            Self::Typed(Default::default())
        }
    }

    impl<C: Columnar> Clone for Column<C> {
        fn clone(&self) -> Self {
            match self {
                // Typed stays typed, although we would have the option to move to aligned data.
                // If we did it might be confusing why we couldn't push into a cloned column.
                Column::Typed(t) => Column::Typed(t.clone()),
                Column::Bytes(b) => {
                    assert_eq!(b.len() % 8, 0);
                    let mut alloc: Region<u64> = super::alloc_aligned_zeroed(b.len() / 8);
                    let alloc_bytes = bytemuck::cast_slice_mut(&mut alloc);
                    alloc_bytes[..b.len()].copy_from_slice(b);
                    Self::Align(alloc)
                }
                Column::Align(a) => {
                    let mut alloc = super::alloc_aligned_zeroed(a.len());
                    alloc[..a.len()].copy_from_slice(a);
                    Column::Align(alloc)
                }
                Column::Compressed(compressed) => Column::Compressed(compressed.clone()),
            }
        }
    }

    impl<C: Columnar> Container for Column<C> {
        type ItemRef<'a> = C::Ref<'a>;
        type Item<'a> = C::Ref<'a>;

        #[inline(always)]
        fn len(&self) -> usize {
            use Column::*;
            match self {
                Typed(_) | Bytes(_) | Align(_) => self.borrow().len(),
                Compressed(c) => c.elements(),
            }
        }

        // This sets the `Bytes` variant to be an empty `Typed` variant, appropriate for pushing into.
        #[inline(always)]
        fn clear(&mut self) {
            use Column::*;
            match self {
                Typed(t) => t.clear(),
                Bytes(_) | Align(_) | Compressed(_) => *self = Typed(Default::default()),
            }
        }

        type Iter<'a> = IterOwn<<C::Container as columnar::Container<C>>::Borrowed<'a>>;

        #[inline(always)]
        fn iter(&self) -> Self::Iter<'_> {
            self.borrow().into_index_iter()
        }

        type DrainIter<'a> = IterOwn<<C::Container as columnar::Container<C>>::Borrowed<'a>>;

        #[inline(always)]
        fn drain(&mut self) -> Self::DrainIter<'_> {
            self.borrow().into_index_iter()
        }
    }

    impl<C, T> PushInto<T> for Column<C>
    where
        C: Columnar<Container: columnar::Push<T>>,
    {
        #[inline]
        fn push_into(&mut self, item: T) {
            use Column::*;
            use columnar::Push;
            match self {
                Typed(t) => {
                    t.push(item);
                    let length_in_bytes = Indexed::length_in_bytes(&t.borrow());

                    if length_in_bytes >= (1 << 20) {
                        let mut alloc = super::alloc_aligned_zeroed(length_in_bytes);
                        let writer = std::io::Cursor::new(bytemuck::cast_slice_mut(&mut alloc[..]));
                        Indexed::write(writer, &t.borrow()).unwrap();
                        *self = Column::Align(alloc);
                    }
                }
                Align(_) | Bytes(_) | Compressed(_) => {
                    // We really oughtn't be calling this in this case.
                    // We could convert to owned, but need more constraints on `C`.
                    unimplemented!("Pushing into Column::Bytes without first clearing");
                }
            }
        }
    }

    impl<C: Columnar> ContainerBytes for Column<C> {
        fn from_bytes(bytes: Bytes) -> Self {
            // Our expectation / hope is that `bytes` is `u64` aligned and sized.
            // If the alignment is borked, we can relocate. If the size is borked,
            // not sure what we do in that case. An incorrect size indicates a problem
            // of `into_bytes`, or a failure of the communication layer, both of which
            // are unrecoverable.
            assert_eq!(bytes.len() % 8, 0);
            if let Ok(_) = bytemuck::try_cast_slice::<_, u64>(&bytes) {
                Self::Bytes(bytes)
            } else {
                // We failed to cast the slice, so we'll reallocate.
                let mut alloc: Region<u64> = super::alloc_aligned_zeroed(bytes.len() / 8);
                let alloc_bytes = bytemuck::cast_slice_mut(&mut alloc);
                alloc_bytes[..bytes.len()].copy_from_slice(&bytes);
                Self::Align(alloc)
            }
        }

        fn length_in_bytes(&self) -> usize {
            match self {
                Column::Typed(t) => Indexed::length_in_bytes(&t.borrow()),
                Column::Bytes(b) => b.len(),
                Column::Align(a) => 8 * a.len(),
                Column::Compressed(c) => c.uncompressed_size(),
            }
        }

        fn into_bytes<W: std::io::Write>(&self, writer: &mut W) {
            match self {
                Column::Typed(t) => Indexed::write(writer, &t.borrow()).unwrap(),
                Column::Bytes(b) => writer.write_all(b).unwrap(),
                Column::Align(a) => writer.write_all(bytemuck::cast_slice(a)).unwrap(),
                Column::Compressed(_) => panic!("Cannot write compressed column directly"),
            }
        }
    }
}

mod builder {
    use std::collections::VecDeque;

    use columnar::bytes::{EncodeDecode, Indexed};
    use columnar::{Clear, Columnar, Len, Push};
    use timely::container::PushInto;
    use timely::container::{ContainerBuilder, LengthPreservingContainerBuilder};

    use crate::containers::Column;

    /// A container builder for `Column<C>`.
    pub struct ColumnBuilder<C: Columnar> {
        /// Container that we're writing to.
        current: C::Container,
        /// Finished container that we presented to callers of extract/finish.
        ///
        /// We don't recycle the column because for extract, it's not typed, and after calls
        /// to finish it'll be `None`.
        finished: Option<Column<C>>,
        /// Completed containers pending to be sent.
        pending: VecDeque<Column<C>>,
    }

    impl<C: Columnar<Container: Push<T>>, T> PushInto<T> for ColumnBuilder<C> {
        #[inline(always)]
        fn push_into(&mut self, item: T) {
            self.current.push(item);
            // If there is less than 10% slop with 2MB backing allocations, mint a container.
            use columnar::Container;
            let words = Indexed::length_in_words(&self.current.borrow());
            let round = (words + ((1 << 18) - 1)) & !((1 << 18) - 1);
            if round - words < round / 10 {
                /// Move the contents from `current` to an aligned allocation, and push it to `pending`.
                /// The contents must fit in `round` words (u64).
                #[cold]
                fn outlined_align<C>(
                    current: &mut C::Container,
                    round: usize,
                    pending: &mut VecDeque<Column<C>>,
                    empty: Option<Column<C>>,
                ) where
                    C: Columnar,
                {
                    let mut alloc = if let Some(Column::Align(mut alloc)) = empty {
                        if alloc.capacity() >= round {
                            unsafe { alloc.clear() };
                            alloc.extend(std::iter::repeat(0).take(round));
                            alloc
                        } else {
                            super::alloc_aligned_zeroed(round)
                        }
                    } else {
                        super::alloc_aligned_zeroed(round)
                    };
                    let writer = std::io::Cursor::new(bytemuck::cast_slice_mut(&mut alloc[..]));
                    Indexed::write(writer, &current.borrow()).unwrap();
                    pending.push_back(Column::Align(alloc));
                    current.clear();
                }

                outlined_align(
                    &mut self.current,
                    round,
                    &mut self.pending,
                    self.finished.take(),
                );
            }
        }
    }

    impl<C: Columnar> Default for ColumnBuilder<C> {
        #[inline(always)]
        fn default() -> Self {
            ColumnBuilder {
                current: Default::default(),
                finished: None,
                pending: Default::default(),
            }
        }
    }

    impl<C: Columnar> ContainerBuilder for ColumnBuilder<C> {
        type Container = Column<C>;

        #[inline]
        fn extract(&mut self) -> Option<&mut Self::Container> {
            if let Some(container) = self.pending.pop_front() {
                self.finished = Some(container);
                self.finished.as_mut()
            } else {
                None
            }
        }

        #[inline]
        fn finish(&mut self) -> Option<&mut Self::Container> {
            if !self.current.is_empty() {
                use columnar::Container;
                let words = Indexed::length_in_words(&self.current.borrow());
                let mut alloc = if let Some(Column::Align(mut alloc)) = self.finished.take() {
                    if alloc.capacity() >= words {
                        unsafe { alloc.clear() };
                        alloc.extend(std::iter::repeat(0).take(words));
                        alloc
                    } else {
                        super::alloc_aligned_zeroed(words)
                    }
                } else {
                    super::alloc_aligned_zeroed(words)
                };
                let writer = std::io::Cursor::new(bytemuck::cast_slice_mut(&mut alloc[..]));
                Indexed::write(writer, &self.current.borrow()).unwrap();
                self.pending.push_back(Column::Align(alloc));
                self.current.clear();
            }
            self.finished = self.pending.pop_front();
            self.finished.as_mut()
        }

        #[inline]
        fn relax(&mut self) {
            *self = Self::default();
        }
    }

    impl<C: Columnar> LengthPreservingContainerBuilder for ColumnBuilder<C> where C::Container: Clone {}
}

/// A batcher for columnar storage.
pub type Col2ValBatcher<K, V, T, R> = MergeBatcher<Column<((K, V), T, R)>, (K, V), T, R>;
pub type Col2KeyBatcher<K, T, R> = Col2ValBatcher<K, (), T, R>;
pub type Vec2Col2ValBatcher<K, V, T, R> = MergeBatcher<Vec<((K, V), T, R)>, (K, V), T, R>;
pub type Vec2Col2KeyBatcher<K, T, R> = Vec2Col2ValBatcher<K, (), T, R>;

/// An exchange function for columnar tuples of the form `((K, V), T, D)`. Rust has a hard
/// time to figure out the lifetimes of the elements when specified as a closure, so we rather
/// specify it as a function.
#[inline(always)]
pub fn columnar_exchange<K, V, T, D>(((k, _), _, _): &<((K, V), T, D) as Columnar>::Ref<'_>) -> u64
where
    K: for<'b> Columnar<Ref<'b>: Hash>,
    V: Columnar,
    D: Columnar,
    T: Columnar,
{
    k.hashed()
}

/// Types for consolidating, merging, and extracting columnar update collections.
pub mod batcher {
    use columnar::Columnar;
    use differential_dataflow::difference::Semigroup;
    use timely::Container;
    use timely::container::{ContainerBuilder, PushInto};

    use crate::containers::Column;

    #[derive(Default)]
    pub struct Chunker<CB> {
        /// Builder to absorb sorted data.
        builder: CB,
    }

    impl<CB: ContainerBuilder> ContainerBuilder for Chunker<CB> {
        type Container = CB::Container;

        #[inline(always)]
        fn extract(&mut self) -> Option<&mut Self::Container> {
            self.builder.extract()
        }

        #[inline(always)]
        fn finish(&mut self) -> Option<&mut Self::Container> {
            self.builder.finish()
        }
    }

    impl<'a, D, T, R, CB> PushInto<&'a mut Vec<(D, T, R)>> for Chunker<CB>
    where
        D: Columnar + Ord,
        T: Columnar + Ord,
        R: Columnar + Semigroup,
        CB: ContainerBuilder + for<'b> PushInto<(&'b D, &'b T, &'b R)>,
    {
        fn push_into(&mut self, container: &'a mut Vec<(D, T, R)>) {
            // Sort input data
            differential_dataflow::consolidation::consolidate_updates(container);

            for (data, time, diff) in container.drain(..) {
                self.builder.push_into((&data, &time, &diff));
            }
        }
    }
    impl<'a, D, T, R, CB> PushInto<&'a mut Column<(D, T, R)>> for Chunker<CB>
    where
        D: for<'b> Columnar<Ref<'b>: Ord + Copy>,
        T: for<'b> Columnar<Ref<'b>: Ord + Copy>,
        R: for<'b> Columnar<Ref<'b>: Ord> + Semigroup + for<'b> Semigroup<R::Ref<'b>>,
        CB: ContainerBuilder + for<'b, 'c> PushInto<(D::Ref<'b>, T::Ref<'b>, &'c R)>,
    {
        fn push_into(&mut self, container: &'a mut Column<(D, T, R)>) {
            // Sort input data
            // TODO: consider `Vec<usize>` that we retain, containing indexes.
            let mut permutation = Vec::with_capacity(container.len());
            permutation.extend(container.drain());
            permutation.sort();

            // Iterate over the data, accumulating diffs for like keys.
            let mut iter = permutation.drain(..);
            if let Some((data, time, diff)) = iter.next() {
                let mut prev_data = data;
                let mut prev_time = time;
                let mut prev_diff = <R as Columnar>::into_owned(diff);

                for (data, time, diff) in iter {
                    if (&prev_data, &prev_time) == (&data, &time) {
                        prev_diff.plus_equals(&diff);
                    } else {
                        if !prev_diff.is_zero() {
                            let tuple = (prev_data, prev_time, &prev_diff);
                            self.builder.push_into(tuple);
                        }
                        prev_data = data;
                        prev_time = time;
                        R::copy_from(&mut prev_diff, diff);
                    }
                }

                if !prev_diff.is_zero() {
                    let tuple = (prev_data, prev_time, &prev_diff);
                    self.builder.push_into(tuple);
                }
            }
        }
    }
}

pub mod dd_builder {
    use columnar::Columnar;
    use differential_dataflow::IntoOwned;
    use differential_dataflow::trace::Builder;
    use differential_dataflow::trace::Description;
    use differential_dataflow::trace::implementations::BatchContainer;
    use differential_dataflow::trace::implementations::Layout;
    use differential_dataflow::trace::implementations::TStack;
    use differential_dataflow::trace::implementations::Update;
    use differential_dataflow::trace::implementations::ord_neu::{
        OrdValBatch, val_batch::OrdValStorage,
    };
    use differential_dataflow::trace::rc_blanket_impls::RcBuilder;
    use timely::container::PushInto;

    use crate::containers::{Column, PackedContainer};

    pub type ColValBuilder<K, V, T, R> = RcBuilder<OrdValBuilder<TStack<((K, V), T, R)>>>;
    pub type ColKeyBuilder<K, T, R> = RcBuilder<OrdValBuilder<TStack<((K, ()), T, R)>>>;

    type OwnedKey<L> = <<L as Layout>::KeyContainer as BatchContainer>::Owned;
    type ReadItemKey<'a, L> = <<L as Layout>::KeyContainer as BatchContainer>::ReadItem<'a>;
    type OwnedVal<L> = <<L as Layout>::ValContainer as BatchContainer>::Owned;
    type ReadItemVal<'a, L> = <<L as Layout>::ValContainer as BatchContainer>::ReadItem<'a>;
    type OwnedTime<L> = <<L as Layout>::TimeContainer as BatchContainer>::Owned;
    type ReadItemTime<'a, L> = <<L as Layout>::TimeContainer as BatchContainer>::ReadItem<'a>;
    type OwnedDiff<L> = <<L as Layout>::DiffContainer as BatchContainer>::Owned;
    type ReadItemDiff<'a, L> = <<L as Layout>::DiffContainer as BatchContainer>::ReadItem<'a>;

    /// A builder for creating layers from unsorted update tuples.
    pub struct OrdValBuilder<L: Layout> {
        /// The in-progress result.
        ///
        /// This is public to allow container implementors to set and inspect their container.
        pub result: OrdValStorage<L>,
        singleton: Option<(<L::Target as Update>::Time, <L::Target as Update>::Diff)>,
        /// Counts the number of singleton optimizations we performed.
        ///
        /// This number allows us to correctly gauge the total number of updates reflected in a batch,
        /// even though `updates.len()` may be much shorter than this amount.
        singletons: usize,
    }

    impl<L: Layout> OrdValBuilder<L> {
        /// Pushes a single update, which may set `self.singleton` rather than push.
        ///
        /// This operation is meant to be equivalent to `self.results.updates.push((time, diff))`.
        /// However, for "clever" reasons it does not do this. Instead, it looks for opportunities
        /// to encode a singleton update with an "absert" update: repeating the most recent offset.
        /// This otherwise invalid state encodes "look back one element".
        ///
        /// When `self.singleton` is `Some`, it means that we have seen one update and it matched the
        /// previously pushed update exactly. In that case, we do not push the update into `updates`.
        /// The update tuple is retained in `self.singleton` in case we see another update and need
        /// to recover the singleton to push it into `updates` to join the second update.
        fn push_update(
            &mut self,
            time: <L::Target as Update>::Time,
            diff: <L::Target as Update>::Diff,
        ) {
            // If a just-pushed update exactly equals `(time, diff)` we can avoid pushing it.
            let last_time = self.result.times.last();
            let last_diff = self.result.diffs.last();
            if last_time.map_or(false, |t| t == ReadItemTime::<L>::borrow_as(&time))
                && last_diff.map_or(false, |d| d == ReadItemDiff::<L>::borrow_as(&diff))
            {
                assert!(self.singleton.is_none());
                self.singleton = Some((time, diff));
            } else {
                // If we have pushed a single element, we need to copy it out to meet this one.
                if let Some((time, diff)) = self.singleton.take() {
                    self.result.times.push(time);
                    self.result.diffs.push(diff);
                }
                self.result.times.push(time);
                self.result.diffs.push(diff);
            }
        }
    }

    // The layout `L` determines the key, val, time, and diff types.
    impl<L> Builder for OrdValBuilder<L>
    where
        L: Layout,
        OwnedKey<L>: Columnar,
        OwnedVal<L>: Columnar,
        OwnedTime<L>: Columnar,
        OwnedDiff<L>: Columnar,
        // These two constraints seem .. like we could potentially replace by `Columnar::Ref<'a>`.
        for<'a> L::KeyContainer: PushInto<&'a OwnedKey<L>>,
        for<'a> L::ValContainer: PushInto<&'a OwnedVal<L>>,
        for<'a> <L::TimeContainer as BatchContainer>::ReadItem<'a>:
            IntoOwned<'a, Owned = <L::Target as Update>::Time>,
        for<'a> <L::DiffContainer as BatchContainer>::ReadItem<'a>:
            IntoOwned<'a, Owned = <L::Target as Update>::Diff>,
    {
        type Input = Column<((OwnedKey<L>, OwnedVal<L>), OwnedTime<L>, OwnedDiff<L>)>;
        type Time = <L::Target as Update>::Time;
        type Output = OrdValBatch<L>;

        fn with_capacity(keys: usize, vals: usize, upds: usize) -> Self {
            // We don't introduce zero offsets as they will be introduced by the first `push` call.
            Self {
                result: OrdValStorage {
                    keys: L::KeyContainer::with_capacity(keys),
                    keys_offs: L::OffsetContainer::with_capacity(keys + 1),
                    vals: L::ValContainer::with_capacity(vals),
                    vals_offs: L::OffsetContainer::with_capacity(vals + 1),
                    times: L::TimeContainer::with_capacity(upds),
                    diffs: L::DiffContainer::with_capacity(upds),
                },
                singleton: None,
                singletons: 0,
            }
        }

        #[inline]
        fn push(&mut self, chunk: &mut Self::Input) {
            use timely::Container;

            // NB: Maintaining owned key and val across iterations to track the "last", which we clone into,
            // is somewhat appealing from an ease point of view. Might still allocate, do work we don't need,
            // but avoids e.g. calls into `last()` and breaks horrid trait requirements.
            // Owned key and val would need to be members of `self`, as this method can be called multiple times,
            // and we need to correctly cache last for reasons of correctness, not just performance.

            let mut owned_key = None;
            let mut owned_val = None;

            for ((key, val), time, diff) in chunk.drain() {
                let key = if let Some(owned_key) = owned_key.as_mut() {
                    OwnedKey::<L>::copy_from(owned_key, key);
                    owned_key
                } else {
                    owned_key.insert(OwnedKey::<L>::into_owned(key))
                };
                let val = if let Some(owned_val) = owned_val.as_mut() {
                    OwnedVal::<L>::copy_from(owned_val, val);
                    owned_val
                } else {
                    owned_val.insert(OwnedVal::<L>::into_owned(val))
                };

                let time = OwnedTime::<L>::into_owned(time);
                let diff = OwnedDiff::<L>::into_owned(diff);

                // Perhaps this is a continuation of an already received key.
                let last_key = self.result.keys.last();
                if last_key.map_or(false, |k| ReadItemKey::<L>::borrow_as(key).eq(&k)) {
                    // Perhaps this is a continuation of an already received value.
                    let last_val = self.result.vals.last();
                    if last_val.map_or(false, |v| ReadItemVal::<L>::borrow_as(val).eq(&v)) {
                        self.push_update(time, diff);
                    } else {
                        // New value; complete representation of prior value.
                        self.result.vals_offs.push(self.result.times.len());
                        if self.singleton.take().is_some() {
                            self.singletons += 1;
                        }
                        self.push_update(time, diff);
                        self.result.vals.push(val);
                    }
                } else {
                    // New key; complete representation of prior key.
                    self.result.vals_offs.push(self.result.times.len());
                    if self.singleton.take().is_some() {
                        self.singletons += 1;
                    }
                    self.result.keys_offs.push(self.result.vals.len());
                    self.push_update(time, diff);
                    self.result.vals.push(val);
                    self.result.keys.push(key);
                }
            }
        }

        #[inline(never)]
        fn done(mut self, description: Description<Self::Time>) -> OrdValBatch<L> {
            // Record the final offsets
            self.result.vals_offs.push(self.result.times.len());
            // Remove any pending singleton, and if it was set increment our count.
            if self.singleton.take().is_some() {
                self.singletons += 1;
            }
            self.result.keys_offs.push(self.result.vals.len());
            OrdValBatch {
                updates: self.result.times.len() + self.singletons,
                storage: self.result,
                description,
            }
        }

        fn seal(
            chain: &mut Vec<Self::Input>,
            description: Description<Self::Time>,
        ) -> Self::Output {
            // let (keys, vals, upds) = Self::Input::key_val_upd_counts(&chain[..]);
            // let mut builder = Self::with_capacity(keys, vals, upds);
            let mut builder = Self::with_capacity(0, 0, 0);
            let mut state = Default::default();
            for mut chunk in chain.drain(..) {
                chunk.unpack(&mut state);
                builder.push(&mut chunk);
            }

            builder.done(description)
        }
    }
}

mod provided_builder {
    use timely::Container;
    use timely::container::ContainerBuilder;

    /// A container builder that doesn't support pushing elements, and is only suitable for pushing
    /// whole containers at Timely sessions. See [`give_container`] for more information.
    ///
    ///  [`give_container`]: timely::dataflow::channels::pushers::buffer::Session::give_container
    pub struct ProvidedBuilder<C> {
        _marker: std::marker::PhantomData<C>,
    }

    impl<C> Default for ProvidedBuilder<C> {
        fn default() -> Self {
            Self {
                _marker: std::marker::PhantomData,
            }
        }
    }

    impl<C: Container + Clone + 'static> ContainerBuilder for ProvidedBuilder<C> {
        type Container = C;

        #[inline(always)]
        fn extract(&mut self) -> Option<&mut Self::Container> {
            None
        }

        #[inline(always)]
        fn finish(&mut self) -> Option<&mut Self::Container> {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use mz_ore::region::Region;
    use timely::Container;
    use timely::bytes::arc::BytesMut;
    use timely::dataflow::channels::ContainerBytes;

    use super::*;

    /// Produce some bytes that are in columnar format.
    fn raw_columnar_bytes() -> Vec<u8> {
        let mut raw = Vec::new();
        raw.extend(16_u64.to_le_bytes()); // length
        raw.extend(28_u64.to_le_bytes()); // length
        raw.extend(1_i32.to_le_bytes());
        raw.extend(2_i32.to_le_bytes());
        raw.extend(3_i32.to_le_bytes());
        raw.extend([0, 0, 0, 0]); // padding
        raw
    }

    #[mz_ore::test]
    fn test_column_clone() {
        let columns = Columnar::as_columns([1, 2, 3].iter());
        let column_typed: Column<i32> = Column::Typed(columns);
        let column_typed2 = column_typed.clone();

        assert_eq!(column_typed2.iter().collect::<Vec<_>>(), vec![&1, &2, &3]);

        let bytes = BytesMut::from(raw_columnar_bytes()).freeze();
        let column_bytes: Column<i32> = Column::Bytes(bytes);
        let column_bytes2 = column_bytes.clone();

        assert_eq!(column_bytes2.iter().collect::<Vec<_>>(), vec![&1, &2, &3]);

        let raw = raw_columnar_bytes();
        let mut region: Region<u64> = alloc_aligned_zeroed(raw.len() / 8);
        let region_bytes = bytemuck::cast_slice_mut(&mut region);
        region_bytes[..raw.len()].copy_from_slice(&raw);
        let column_align: Column<i32> = Column::Align(region);
        let column_align2 = column_align.clone();

        assert_eq!(column_align2.iter().collect::<Vec<_>>(), vec![&1, &2, &3]);
    }

    #[mz_ore::test]
    fn test_column_from_bytes() {
        let raw = raw_columnar_bytes();

        let buf = vec![0; raw.len() + 8];
        let align = buf.as_ptr().align_offset(std::mem::size_of::<u64>());
        let mut bytes_mut = BytesMut::from(buf);
        let _ = bytes_mut.extract_to(align);
        bytes_mut[..raw.len()].copy_from_slice(&raw);
        let aligned_bytes = bytes_mut.extract_to(raw.len());

        let column: Column<i32> = Column::from_bytes(aligned_bytes);
        assert!(matches!(column, Column::Bytes(_)));
        assert_eq!(column.iter().collect::<Vec<_>>(), vec![&1, &2, &3]);

        let buf = vec![0; raw.len() + 8];
        let align = buf.as_ptr().align_offset(std::mem::size_of::<u64>());
        let mut bytes_mut = BytesMut::from(buf);
        let _ = bytes_mut.extract_to(align + 1);
        bytes_mut[..raw.len()].copy_from_slice(&raw);
        let unaligned_bytes = bytes_mut.extract_to(raw.len());

        let column: Column<i32> = Column::from_bytes(unaligned_bytes);
        assert!(matches!(column, Column::Align(_)));
        assert_eq!(column.iter().collect::<Vec<_>>(), vec![&1, &2, &3]);
    }
}
