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

use columnar::{Columnar, Ref};
use differential_dataflow::Hashable;
use differential_dataflow::containers::TimelyStack;
use differential_dataflow::trace::implementations::merge_batcher::{ColMerger, MergeBatcher};

pub mod stack;

pub(crate) use alloc::alloc_aligned_zeroed;
pub use alloc::{enable_columnar_lgalloc, set_enable_columnar_lgalloc};
pub use builder::ColumnBuilder;
pub use container::Column;
pub use provided_builder::ProvidedBuilder;

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
    }

    impl<C: Columnar> Column<C> {
        /// Borrows the container as a reference.
        #[inline]
        fn borrow(&self) -> <C::Container as columnar::Container>::Borrowed<'_> {
            match self {
                Column::Typed(t) => t.borrow(),
                Column::Bytes(b) => {
                    <<C::Container as columnar::Container>::Borrowed<'_>>::from_bytes(
                        &mut Indexed::decode(bytemuck::cast_slice(b)),
                    )
                }
                Column::Align(a) => {
                    <<C::Container as columnar::Container>::Borrowed<'_>>::from_bytes(
                        &mut Indexed::decode(a),
                    )
                }
            }
        }
    }

    impl<C: Columnar> Default for Column<C> {
        fn default() -> Self {
            Self::Typed(Default::default())
        }
    }

    impl<C: Columnar> Clone for Column<C>
    where
        C::Container: Clone,
    {
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
            }
        }
    }

    impl<C: Columnar> Container for Column<C> {
        type ItemRef<'a> = columnar::Ref<'a, C>;
        type Item<'a> = columnar::Ref<'a, C>;

        #[inline]
        fn len(&self) -> usize {
            self.borrow().len()
        }

        // This sets the `Bytes` variant to be an empty `Typed` variant, appropriate for pushing into.
        #[inline]
        fn clear(&mut self) {
            match self {
                Column::Typed(t) => t.clear(),
                Column::Bytes(_) | Column::Align(_) => *self = Column::Typed(Default::default()),
            }
        }

        type Iter<'a> = IterOwn<<C::Container as columnar::Container>::Borrowed<'a>>;

        #[inline]
        fn iter(&self) -> Self::Iter<'_> {
            self.borrow().into_index_iter()
        }

        type DrainIter<'a> = IterOwn<<C::Container as columnar::Container>::Borrowed<'a>>;

        #[inline]
        fn drain(&mut self) -> Self::DrainIter<'_> {
            self.borrow().into_index_iter()
        }
    }

    impl<C: Columnar, T> PushInto<T> for Column<C>
    where
        C::Container: columnar::Push<T>,
    {
        #[inline]
        fn push_into(&mut self, item: T) {
            use columnar::Push;
            match self {
                Column::Typed(t) => t.push(item),
                Column::Align(_) | Column::Bytes(_) => {
                    // We really oughtn't be calling this in this case.
                    // We could convert to owned, but need more constraints on `C`.
                    unimplemented!("Pushing into Column::Bytes without first clearing");
                }
            }
        }
    }

    impl<C: Columnar> ContainerBytes for Column<C> {
        #[inline]
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

        #[inline]
        fn length_in_bytes(&self) -> usize {
            match self {
                Column::Typed(t) => Indexed::length_in_bytes(&t.borrow()),
                Column::Bytes(b) => b.len(),
                Column::Align(a) => 8 * a.len(),
            }
        }

        #[inline]
        fn into_bytes<W: ::std::io::Write>(&self, writer: &mut W) {
            match self {
                Column::Typed(t) => Indexed::write(writer, &t.borrow()).unwrap(),
                Column::Bytes(b) => writer.write_all(b).unwrap(),
                Column::Align(a) => writer.write_all(bytemuck::cast_slice(a)).unwrap(),
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

    impl<C: Columnar, T> PushInto<T> for ColumnBuilder<C>
    where
        C::Container: Push<T>,
    {
        #[inline]
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
                ) where
                    C: Columnar,
                {
                    let mut alloc = super::alloc_aligned_zeroed(round);
                    let writer = std::io::Cursor::new(bytemuck::cast_slice_mut(&mut alloc[..]));
                    Indexed::write(writer, &current.borrow()).unwrap();
                    pending.push_back(Column::Align(alloc));
                    current.clear();
                }

                outlined_align(&mut self.current, round, &mut self.pending);
            }
        }
    }

    impl<C: Columnar> Default for ColumnBuilder<C> {
        #[inline]
        fn default() -> Self {
            ColumnBuilder {
                current: Default::default(),
                finished: None,
                pending: Default::default(),
            }
        }
    }

    impl<C: Columnar> ContainerBuilder for ColumnBuilder<C>
    where
        C::Container: Clone,
    {
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
                self.pending
                    .push_back(Column::Typed(std::mem::take(&mut self.current)));
            }
            self.finished = self.pending.pop_front();
            self.finished.as_mut()
        }
    }

    impl<C: Columnar> LengthPreservingContainerBuilder for ColumnBuilder<C> where C::Container: Clone {}
}

/// A batcher for columnar storage.
pub type Col2ValBatcher<K, V, T, R> = MergeBatcher<
    Column<((K, V), T, R)>,
    batcher::Chunker<TimelyStack<((K, V), T, R)>>,
    ColMerger<(K, V), T, R>,
>;
pub type Col2KeyBatcher<K, T, R> = Col2ValBatcher<K, (), T, R>;

/// An exchange function for columnar tuples of the form `((K, V), T, D)`. Rust has a hard
/// time to figure out the lifetimes of the elements when specified as a closure, so we rather
/// specify it as a function.
#[inline(always)]
pub fn columnar_exchange<K, V, T, D>(((k, _), _, _): &Ref<'_, ((K, V), T, D)>) -> u64
where
    K: Columnar,
    for<'a> Ref<'a, K>: Hash,
    V: Columnar,
    D: Columnar,
    T: Columnar,
{
    k.hashed()
}

/// Types for consolidating, merging, and extracting columnar update collections.
pub mod batcher {
    use std::collections::VecDeque;

    use columnar::Columnar;
    use differential_dataflow::difference::Semigroup;
    use timely::Container;
    use timely::container::{ContainerBuilder, PushInto};

    use crate::containers::Column;

    #[derive(Default)]
    pub struct Chunker<C> {
        /// Buffer into which we'll consolidate.
        ///
        /// Also the buffer where we'll stage responses to `extract` and `finish`.
        /// When these calls return, the buffer is available for reuse.
        target: C,
        /// Consolidated buffers ready to go.
        ready: VecDeque<C>,
    }

    impl<C: Container + Clone + 'static> ContainerBuilder for Chunker<C> {
        type Container = C;

        fn extract(&mut self) -> Option<&mut Self::Container> {
            if let Some(ready) = self.ready.pop_front() {
                self.target = ready;
                Some(&mut self.target)
            } else {
                None
            }
        }

        fn finish(&mut self) -> Option<&mut Self::Container> {
            self.extract()
        }
    }

    impl<'a, D, T, R, C2> PushInto<&'a mut Column<(D, T, R)>> for Chunker<C2>
    where
        D: Columnar,
        for<'b> columnar::Ref<'b, D>: Ord + Copy,
        T: Columnar,
        for<'b> columnar::Ref<'b, T>: Ord + Copy,
        R: Columnar + Semigroup + for<'b> Semigroup<columnar::Ref<'b, R>>,
        for<'b> columnar::Ref<'b, R>: Ord,
        C2: Container + for<'b> PushInto<&'b (D, T, R)>,
    {
        fn push_into(&mut self, container: &'a mut Column<(D, T, R)>) {
            // Sort input data
            // TODO: consider `Vec<usize>` that we retain, containing indexes.
            let mut permutation = Vec::with_capacity(container.len());
            permutation.extend(container.drain());
            permutation.sort();

            self.target.clear();
            // Iterate over the data, accumulating diffs for like keys.
            let mut iter = permutation.drain(..);
            if let Some((data, time, diff)) = iter.next() {
                let mut owned_data = D::into_owned(data);
                let mut owned_time = T::into_owned(time);

                let mut prev_data = data;
                let mut prev_time = time;
                let mut prev_diff = <R as Columnar>::into_owned(diff);

                for (data, time, diff) in iter {
                    if (&prev_data, &prev_time) == (&data, &time) {
                        prev_diff.plus_equals(&diff);
                    } else {
                        if !prev_diff.is_zero() {
                            D::copy_from(&mut owned_data, prev_data);
                            T::copy_from(&mut owned_time, prev_time);
                            let tuple = (owned_data, owned_time, prev_diff);
                            self.target.push_into(&tuple);
                            (owned_data, owned_time, prev_diff) = tuple;
                        }
                        prev_data = data;
                        prev_time = time;
                        R::copy_from(&mut prev_diff, diff);
                    }
                }

                if !prev_diff.is_zero() {
                    D::copy_from(&mut owned_data, prev_data);
                    T::copy_from(&mut owned_time, prev_time);
                    let tuple = (owned_data, owned_time, prev_diff);
                    self.target.push_into(&tuple);
                }
            }

            if !self.target.is_empty() {
                self.ready.push_back(std::mem::take(&mut self.target));
            }
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
    use timely::container::PushInto;
    use timely::dataflow::channels::ContainerBytes;

    use super::*;

    /// Produce some bytes that are in columnar format.
    fn raw_columnar_bytes() -> Vec<u8> {
        let mut raw = Vec::new();
        raw.extend(16_u64.to_le_bytes()); // offsets
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
        {
            let mut column: Column<i32> = Default::default();
            column.push_into(1);
            column.push_into(2);
            column.push_into(3);
            let mut data = Vec::new();
            column.into_bytes(&mut std::io::Cursor::new(&mut data));
            println!("data: {:?}", data);
        }

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
