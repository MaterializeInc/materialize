// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub use self::container::DatumContainer;
pub use self::spines::{RowRowSpine, RowSpine, RowValSpine};

/// Spines specialized to contain `Row` types in keys and values.
mod spines {

    use differential_dataflow::trace::implementations::merge_batcher_col::ColumnatedMergeBatcher;
    use differential_dataflow::trace::implementations::ord_neu::{OrdKeyBatch, OrdKeyBuilder};
    use differential_dataflow::trace::implementations::ord_neu::{OrdValBatch, OrdValBuilder};
    use differential_dataflow::trace::implementations::spine_fueled::Spine;
    use differential_dataflow::trace::implementations::Update;
    use differential_dataflow::trace::implementations::{Layout, OffsetList};
    use differential_dataflow::trace::rc_blanket_impls::RcBuilder;
    use std::rc::Rc;
    use timely::container::columnation::{Columnation, TimelyStack};

    use super::DatumContainer;
    use mz_repr::Row;

    pub type RowRowSpine<T, R> = Spine<
        Rc<OrdValBatch<RowRowLayout<((Row, Row), T, R)>>>,
        ColumnatedMergeBatcher<Row, Row, T, R>,
        RcBuilder<OrdValBuilder<RowRowLayout<((Row, Row), T, R)>>>,
    >;
    pub type RowValSpine<V, T, R> = Spine<
        Rc<OrdValBatch<RowValLayout<((Row, V), T, R)>>>,
        ColumnatedMergeBatcher<Row, V, T, R>,
        RcBuilder<OrdValBuilder<RowValLayout<((Row, V), T, R)>>>,
    >;
    pub type RowSpine<T, R> = Spine<
        Rc<OrdKeyBatch<RowLayout<((Row, ()), T, R)>>>,
        ColumnatedMergeBatcher<Row, (), T, R>,
        RcBuilder<OrdKeyBuilder<RowLayout<((Row, ()), T, R)>>>,
    >;

    /// A layout based on timely stacks
    pub struct RowRowLayout<U: Update<Key = Row, Val = Row>> {
        phantom: std::marker::PhantomData<U>,
    }
    pub struct RowValLayout<U: Update<Key = Row>> {
        phantom: std::marker::PhantomData<U>,
    }
    pub struct RowLayout<U: Update<Key = Row, Val = ()>> {
        phantom: std::marker::PhantomData<U>,
    }

    impl<U: Update<Key = Row, Val = Row>> Layout for RowRowLayout<U>
    where
        U::Time: Columnation,
        U::Diff: Columnation,
    {
        type Target = U;
        type KeyContainer = DatumContainer;
        type ValContainer = DatumContainer;
        type UpdContainer = TimelyStack<(U::Time, U::Diff)>;
        type OffsetContainer = OffsetList;
    }
    impl<U: Update<Key = Row>> Layout for RowValLayout<U>
    where
        U::Val: Columnation,
        U::Time: Columnation,
        U::Diff: Columnation,
    {
        type Target = U;
        type KeyContainer = DatumContainer;
        type ValContainer = TimelyStack<U::Val>;
        type UpdContainer = TimelyStack<(U::Time, U::Diff)>;
        type OffsetContainer = OffsetList;
    }
    impl<U: Update<Key = Row, Val = ()>> Layout for RowLayout<U>
    where
        U::Time: Columnation,
        U::Diff: Columnation,
    {
        type Target = U;
        type KeyContainer = DatumContainer;
        type ValContainer = TimelyStack<()>;
        type UpdContainer = TimelyStack<(U::Time, U::Diff)>;
        type OffsetContainer = OffsetList;
    }
}

/// A `Row`-specialized container using dictionary compression.
mod container {

    use differential_dataflow::trace::cursor::MyTrait;
    use differential_dataflow::trace::implementations::BatchContainer;
    use differential_dataflow::trace::implementations::OffsetList;

    use mz_repr::{read_datum, Datum, Row};

    /// A slice container with four bytes overhead per slice.
    pub struct DatumContainer {
        batches: Vec<DatumBatch>,
        /// Stored out of line from batches to allow more effective binary search.
        offsets: Vec<usize>,
    }

    impl DatumContainer {
        /// Visit contained allocations to determine their size and capacity.
        #[inline]
        pub fn heap_size(&self, mut callback: impl FnMut(usize, usize)) {
            // Calculate heap size for local, stash, and stash entries
            callback(
                self.batches.len() * std::mem::size_of::<DatumBatch>(),
                self.batches.capacity() * std::mem::size_of::<DatumBatch>(),
            );
            callback(
                self.offsets.len() * std::mem::size_of::<usize>(),
                self.offsets.capacity() * std::mem::size_of::<usize>(),
            );
            for batch in self.batches.iter() {
                use crate::extensions::arrange;
                arrange::offset_list_size(&batch.offsets, &mut callback);
                callback(batch.storage.len(), batch.storage.capacity());
            }
        }
    }

    impl BatchContainer for DatumContainer {
        type PushItem = Row;
        type ReadItem<'a> = DatumSeq<'a>;

        fn copy(&mut self, item: Self::ReadItem<'_>) {
            if let Some(batch) = self.batches.last_mut() {
                let success = batch.try_push(item.bytes);
                if !success {
                    // double the lengths from `batch`.
                    let item_cap = 2 * batch.offsets.len();
                    // New byte capacity should be in the range 2MB - 128MB, and at least the item length.
                    let mut byte_cap = 2 * batch.storage.capacity();
                    byte_cap = std::cmp::max(byte_cap, 2 << 20);
                    byte_cap = std::cmp::min(byte_cap, 128 << 20);
                    byte_cap = std::cmp::max(byte_cap, item.bytes.len());
                    let mut new_batch = DatumBatch::with_capacities(item_cap, byte_cap);
                    assert!(new_batch.try_push(item.bytes));
                    self.offsets.push(self.len());
                    self.batches.push(new_batch);
                }
            }
        }

        fn with_capacity(size: usize) -> Self {
            Self {
                batches: vec![DatumBatch::with_capacities(size, size)],
                offsets: vec![0],
            }
        }

        fn merge_capacity(cont1: &Self, cont2: &Self) -> Self {
            let mut item_cap = 1;
            let mut byte_cap = 0;
            for batch in cont1.batches.iter() {
                item_cap += batch.offsets.len() - 1;
                byte_cap += batch.storage.len();
            }
            for batch in cont2.batches.iter() {
                item_cap += batch.offsets.len() - 1;
                byte_cap += batch.storage.len();
            }

            // TODO: should we limit `item_cap` to avoid an inappropriately large allocation?
            // It would be "when we cross the `128 << 20` byte boundary" or something like that.
            byte_cap = std::cmp::max(byte_cap, 2 << 20);
            byte_cap = std::cmp::min(byte_cap, 128 << 20);

            Self {
                batches: vec![DatumBatch::with_capacities(item_cap, byte_cap)],
                offsets: vec![0],
            }
        }

        fn index(&self, index: usize) -> Self::ReadItem<'_> {
            // Determine which batch the index belongs to.
            // Binary search gives different answers based on whether it finds
            // the result or not, and we need to tidy up those results to point
            // at the first batch for which the offset is less or equal to `index`.
            let batch_idx = match self.offsets.binary_search(&index) {
                Ok(x) => x,
                Err(x) => x - 1,
            };

            DatumSeq {
                bytes: self.batches[batch_idx].index(index - self.offsets[batch_idx]),
            }
        }

        fn len(&self) -> usize {
            self.offsets.last().map(|x| *x).unwrap_or(0)
                + self.batches.last().map(|x| x.len()).unwrap_or(0)
        }
    }

    /// A batch of slice storage.
    ///
    /// The backing storage for this batch will not be resized.
    pub struct DatumBatch {
        offsets: OffsetList,
        storage: lgalloc::Region<u8>,
    }

    impl DatumBatch {
        /// Either accepts the slice and returns true,
        /// or does not and returns false.
        fn try_push(&mut self, slice: &[u8]) -> bool {
            if self.storage.len() + slice.len() <= self.storage.capacity() {
                self.storage.extend(slice.iter().cloned());
                self.offsets.push(self.storage.len());
                true
            } else {
                false
            }
        }
        fn index(&self, index: usize) -> &[u8] {
            let lower = self.offsets.index(index);
            let upper = self.offsets.index(index + 1);
            &self.storage[lower..upper]
        }
        fn len(&self) -> usize {
            self.offsets.len() - 1
        }

        fn with_capacities(item_cap: usize, byte_cap: usize) -> Self {
            // TODO: be wary of `byte_cap` greater than 2^32.
            let mut offsets = OffsetList::with_capacity(item_cap + 1);
            offsets.push(0);
            Self {
                offsets,
                storage: lgalloc::Region::new_auto(byte_cap.next_power_of_two()),
            }
        }
    }

    #[derive(Debug)]
    pub struct DatumSeq<'a> {
        bytes: &'a [u8],
    }

    impl<'a> Copy for DatumSeq<'a> {}
    impl<'a> Clone for DatumSeq<'a> {
        fn clone(&self) -> Self {
            *self
        }
    }

    use std::cmp::Ordering;
    impl<'a, 'b> PartialEq<DatumSeq<'a>> for DatumSeq<'b> {
        fn eq(&self, other: &DatumSeq<'a>) -> bool {
            self.bytes.eq(other.bytes)
        }
    }
    impl<'a> Eq for DatumSeq<'a> {}
    impl<'a, 'b> PartialOrd<DatumSeq<'a>> for DatumSeq<'b> {
        fn partial_cmp(&self, other: &DatumSeq<'a>) -> Option<Ordering> {
            Some(self.cmp(other))
        }
    }
    impl<'a> Ord for DatumSeq<'a> {
        fn cmp(&self, other: &Self) -> Ordering {
            match self.bytes.len().cmp(&other.bytes.len()) {
                std::cmp::Ordering::Less => std::cmp::Ordering::Less,
                std::cmp::Ordering::Greater => std::cmp::Ordering::Greater,
                std::cmp::Ordering::Equal => self.bytes.cmp(other.bytes),
            }
        }
    }
    impl<'a> MyTrait<'a> for DatumSeq<'a> {
        type Owned = Row;
        fn into_owned(self) -> Self::Owned {
            Row::pack(self)
        }
        fn clone_onto(&self, other: &mut Self::Owned) {
            let mut packer = other.packer();
            packer.extend(*self);
        }
        fn compare(&self, other: &Self::Owned) -> std::cmp::Ordering {
            self.cmp(&DatumSeq::borrow_as(other))
        }
        fn borrow_as(other: &'a Self::Owned) -> Self {
            Self {
                bytes: other.data(),
            }
        }
    }

    impl<'a> Iterator for DatumSeq<'a> {
        type Item = Datum<'a>;
        fn next(&mut self) -> Option<Self::Item> {
            if self.bytes.is_empty() {
                None
            } else {
                let mut offset = 0;
                let result = unsafe { read_datum(self.bytes, &mut offset) };
                self.bytes = &self.bytes[offset..];
                Some(result)
            }
        }
    }

    use mz_repr::fixed_length::IntoRowByTypes;
    use mz_repr::ColumnType;
    impl<'long> IntoRowByTypes for DatumSeq<'long> {
        type DatumIter<'short> = DatumSeq<'short> where Self: 'short;
        fn into_datum_iter<'short>(
            &'short self,
            _types: Option<&[ColumnType]>,
        ) -> Self::DatumIter<'short> {
            *self
        }
    }
}
