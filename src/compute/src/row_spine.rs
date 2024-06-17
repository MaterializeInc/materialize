// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub use self::container::DatumContainer;
pub use self::container::DatumSeq;
pub use self::offset_opt::OffsetOptimized;
pub use self::spines::{RowRowSpine, RowSpine, RowValSpine};
use differential_dataflow::trace::implementations::OffsetList;

/// Spines specialized to contain `Row` types in keys and values.
mod spines {
    use std::rc::Rc;

    use differential_dataflow::trace::implementations::ord_neu::{OrdKeyBatch, OrdKeyBuilder};
    use differential_dataflow::trace::implementations::ord_neu::{OrdValBatch, OrdValBuilder};
    use differential_dataflow::trace::implementations::spine_fueled::Spine;
    use differential_dataflow::trace::implementations::Layout;
    use differential_dataflow::trace::implementations::Update;
    use differential_dataflow::trace::rc_blanket_impls::RcBuilder;
    use mz_repr::Row;
    use timely::container::columnation::{Columnation, TimelyStack};

    use crate::containers::stack::StackWrapper;
    use crate::row_spine::{DatumContainer, OffsetOptimized};
    use crate::typedefs::{KeyBatcher, KeyValBatcher};

    pub type RowRowSpine<T, R> = Spine<
        Rc<OrdValBatch<RowRowLayout<((Row, Row), T, R)>>>,
        KeyValBatcher<Row, Row, T, R>,
        RcBuilder<OrdValBuilder<RowRowLayout<((Row, Row), T, R)>, TimelyStack<((Row, Row), T, R)>>>,
    >;
    pub type RowValSpine<V, T, R> = Spine<
        Rc<OrdValBatch<RowValLayout<((Row, V), T, R)>>>,
        KeyValBatcher<Row, V, T, R>,
        RcBuilder<OrdValBuilder<RowValLayout<((Row, V), T, R)>, TimelyStack<((Row, V), T, R)>>>,
    >;
    pub type RowSpine<T, R> = Spine<
        Rc<OrdKeyBatch<RowLayout<((Row, ()), T, R)>>>,
        KeyBatcher<Row, T, R>,
        RcBuilder<OrdKeyBuilder<RowLayout<((Row, ()), T, R)>, TimelyStack<((Row, ()), T, R)>>>,
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
        type TimeContainer = StackWrapper<U::Time>;
        type DiffContainer = StackWrapper<U::Diff>;
        type OffsetContainer = OffsetOptimized;
    }
    impl<U: Update<Key = Row>> Layout for RowValLayout<U>
    where
        U::Val: Columnation,
        U::Time: Columnation,
        U::Diff: Columnation,
    {
        type Target = U;
        type KeyContainer = DatumContainer;
        type ValContainer = StackWrapper<U::Val>;
        type TimeContainer = StackWrapper<U::Time>;
        type DiffContainer = StackWrapper<U::Diff>;
        type OffsetContainer = OffsetOptimized;
    }
    impl<U: Update<Key = Row, Val = ()>> Layout for RowLayout<U>
    where
        U::Time: Columnation,
        U::Diff: Columnation,
    {
        type Target = U;
        type KeyContainer = DatumContainer;
        type ValContainer = StackWrapper<()>;
        type TimeContainer = StackWrapper<U::Time>;
        type DiffContainer = StackWrapper<U::Diff>;
        type OffsetContainer = OffsetOptimized;
    }
}

/// A `Row`-specialized container using dictionary compression.
mod container {
    use differential_dataflow::trace::cursor::IntoOwned;
    use std::cmp::Ordering;

    use differential_dataflow::trace::implementations::BatchContainer;
    use mz_ore::region::Region;
    use mz_repr::{read_datum, Datum, Row, RowPacker};
    use timely::container::PushInto;

    /// A slice container with four bytes overhead per slice.
    pub struct DatumContainer {
        batches: Vec<DatumBatch>,
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
            for batch in self.batches.iter() {
                batch.offsets.heap_size(&mut callback);
                callback(batch.storage.len(), batch.storage.capacity());
            }
        }
    }

    impl BatchContainer for DatumContainer {
        type Owned = Row;
        type ReadItem<'a> = DatumSeq<'a>;

        fn with_capacity(size: usize) -> Self {
            Self {
                batches: vec![DatumBatch::with_capacities(size, size)],
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
            Self {
                batches: vec![DatumBatch::with_capacities(item_cap, byte_cap)],
            }
        }

        fn reborrow<'b, 'a: 'b>(item: Self::ReadItem<'a>) -> Self::ReadItem<'b> {
            item
        }

        fn index(&self, mut index: usize) -> Self::ReadItem<'_> {
            for batch in self.batches.iter() {
                if index < batch.len() {
                    return DatumSeq {
                        bytes: batch.index(index),
                    };
                }
                index -= batch.len();
            }
            panic!("Index out of bounds");
        }

        fn len(&self) -> usize {
            let mut result = 0;
            for batch in self.batches.iter() {
                result += batch.len();
            }
            result
        }
    }

    impl PushInto<Row> for DatumContainer {
        fn push_into(&mut self, item: Row) {
            self.push_into(&item);
        }
    }

    impl PushInto<&Row> for DatumContainer {
        fn push_into(&mut self, item: &Row) {
            let item: DatumSeq<'_> = IntoOwned::borrow_as(item);
            self.push_into(item);
        }
    }

    impl PushInto<DatumSeq<'_>> for DatumContainer {
        fn push_into(&mut self, item: DatumSeq<'_>) {
            if let Some(batch) = self.batches.last_mut() {
                let success = batch.try_push(item.bytes);
                if !success {
                    // double the lengths from `batch`.
                    let item_cap = 2 * batch.offsets.len();
                    let byte_cap = std::cmp::max(2 * batch.storage.capacity(), item.bytes.len());
                    let mut new_batch = DatumBatch::with_capacities(item_cap, byte_cap);
                    assert!(new_batch.try_push(item.bytes));
                    self.batches.push(new_batch);
                }
            }
        }
    }

    /// A batch of slice storage.
    ///
    /// The backing storage for this batch will not be resized.
    pub struct DatumBatch {
        offsets: crate::row_spine::OffsetOptimized,
        storage: Region<u8>,
    }

    impl DatumBatch {
        /// Either accepts the slice and returns true,
        /// or does not and returns false.
        fn try_push(&mut self, slice: &[u8]) -> bool {
            if self.storage.len() + slice.len() <= self.storage.capacity() {
                self.storage.extend_from_slice(slice);
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
            let mut offsets = crate::row_spine::OffsetOptimized::with_capacity(item_cap + 1);
            offsets.push(0);
            Self {
                offsets,
                storage: Region::new_auto(byte_cap.next_power_of_two()),
            }
        }
    }

    #[derive(Debug)]
    pub struct DatumSeq<'a> {
        bytes: &'a [u8],
    }

    impl DatumSeq<'_> {
        pub fn copy_into(&self, row: &mut RowPacker) {
            // SAFETY: `self.bytes` is a correctly formatted row.
            unsafe { row.extend_by_slice_unchecked(self.bytes) }
        }
    }

    impl<'a> Copy for DatumSeq<'a> {}
    impl<'a> Clone for DatumSeq<'a> {
        fn clone(&self) -> Self {
            *self
        }
    }

    impl<'a, 'b> PartialEq<DatumSeq<'a>> for DatumSeq<'b> {
        fn eq(&self, other: &DatumSeq<'a>) -> bool {
            self.bytes.eq(other.bytes)
        }
    }
    impl<'a> PartialEq<&Row> for DatumSeq<'a> {
        fn eq(&self, other: &&Row) -> bool {
            self.bytes.eq(other.data())
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
    impl<'a> IntoOwned<'a> for DatumSeq<'a> {
        type Owned = Row;
        fn into_owned(self) -> Self::Owned {
            // SAFETY: `bytes` contains a valid row.
            unsafe { Row::from_bytes_unchecked(self.bytes) }
        }
        fn clone_onto(self, other: &mut Self::Owned) {
            let mut packer = other.packer();
            self.copy_into(&mut packer);
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

    use mz_repr::fixed_length::ToDatumIter;
    impl<'long> ToDatumIter for DatumSeq<'long> {
        type DatumIter<'short> = DatumSeq<'short> where Self: 'short;
        fn to_datum_iter<'short>(&'short self) -> Self::DatumIter<'short> {
            *self
        }
    }

    #[cfg(test)]
    mod tests {
        use crate::row_spine::DatumContainer;
        use differential_dataflow::trace::implementations::BatchContainer;
        use mz_repr::adt::date::Date;
        use mz_repr::adt::interval::Interval;
        use mz_repr::{Datum, Row, ScalarType};

        #[mz_ore::test]
        #[cfg_attr(miri, ignore)] // unsupported operation: integer-to-pointer casts and `ptr::with_exposed_provenance` are not supported
        fn test_round_trip() {
            fn round_trip(datums: Vec<Datum>) {
                let row = Row::pack(datums.clone());

                let mut container = DatumContainer::with_capacity(row.byte_len());
                container.push(&row);

                // When run under miri this catches undefined bytes written to data
                // eg by calling push_copy! on a type which contains undefined padding values
                println!("{:?}", container.index(0).bytes);

                let datums2 = container.index(0).collect::<Vec<_>>();
                assert_eq!(datums, datums2);
            }

            round_trip(vec![]);
            round_trip(
                ScalarType::enumerate()
                    .iter()
                    .flat_map(|r#type| r#type.interesting_datums())
                    .collect(),
            );
            round_trip(vec![
                Datum::Null,
                Datum::Null,
                Datum::False,
                Datum::True,
                Datum::Int16(-21),
                Datum::Int32(-42),
                Datum::Int64(-2_147_483_648 - 42),
                Datum::UInt8(0),
                Datum::UInt8(1),
                Datum::UInt16(0),
                Datum::UInt16(1),
                Datum::UInt16(1 << 8),
                Datum::UInt32(0),
                Datum::UInt32(1),
                Datum::UInt32(1 << 8),
                Datum::UInt32(1 << 16),
                Datum::UInt32(1 << 24),
                Datum::UInt64(0),
                Datum::UInt64(1),
                Datum::UInt64(1 << 8),
                Datum::UInt64(1 << 16),
                Datum::UInt64(1 << 24),
                Datum::UInt64(1 << 32),
                Datum::UInt64(1 << 40),
                Datum::UInt64(1 << 48),
                Datum::UInt64(1 << 56),
                Datum::Date(Date::from_pg_epoch(365 * 45 + 21).unwrap()),
                Datum::Interval(Interval {
                    months: 312,
                    ..Default::default()
                }),
                Datum::Interval(Interval::new(0, 0, 1_012_312)),
                Datum::Bytes(&[]),
                Datum::Bytes(&[0, 2, 1, 255]),
                Datum::String(""),
                Datum::String("العَرَبِيَّة"),
            ]);
        }
    }
}

mod offset_opt {
    use differential_dataflow::trace::implementations::BatchContainer;
    use differential_dataflow::trace::implementations::OffsetList;
    use timely::container::PushInto;

    enum OffsetStride {
        Empty,
        Zero,
        Striding(usize, usize),
        Saturated(usize, usize, usize),
    }

    impl OffsetStride {
        /// Accepts or rejects a newly pushed element.
        fn push(&mut self, item: usize) -> bool {
            match self {
                OffsetStride::Empty => {
                    if item == 0 {
                        *self = OffsetStride::Zero;
                        true
                    } else {
                        false
                    }
                }
                OffsetStride::Zero => {
                    *self = OffsetStride::Striding(item, 2);
                    true
                }
                OffsetStride::Striding(stride, count) => {
                    if item == *stride * *count {
                        *count += 1;
                        true
                    } else if item == *stride * (*count - 1) {
                        *self = OffsetStride::Saturated(*stride, *count, 1);
                        true
                    } else {
                        false
                    }
                }
                OffsetStride::Saturated(stride, count, reps) => {
                    if item == *stride * (*count - 1) {
                        *reps += 1;
                        true
                    } else {
                        false
                    }
                }
            }
        }

        fn index(&self, index: usize) -> usize {
            match self {
                OffsetStride::Empty => {
                    panic!("Empty OffsetStride")
                }
                OffsetStride::Zero => 0,
                OffsetStride::Striding(stride, _steps) => *stride * index,
                OffsetStride::Saturated(stride, steps, _reps) => {
                    if index < *steps {
                        *stride * index
                    } else {
                        *stride * (*steps - 1)
                    }
                }
            }
        }

        fn len(&self) -> usize {
            match self {
                OffsetStride::Empty => 0,
                OffsetStride::Zero => 1,
                OffsetStride::Striding(_stride, steps) => *steps,
                OffsetStride::Saturated(_stride, steps, reps) => *steps + *reps,
            }
        }
    }

    pub struct OffsetOptimized {
        strided: OffsetStride,
        spilled: OffsetList,
    }

    impl BatchContainer for OffsetOptimized {
        type Owned = usize;
        type ReadItem<'a> = usize;

        fn with_capacity(_size: usize) -> Self {
            Self {
                strided: OffsetStride::Empty,
                spilled: OffsetList::with_capacity(0),
            }
        }

        fn merge_capacity(_cont1: &Self, _cont2: &Self) -> Self {
            Self {
                strided: OffsetStride::Empty,
                spilled: OffsetList::with_capacity(0),
            }
        }

        fn reborrow<'b, 'a: 'b>(item: Self::ReadItem<'a>) -> Self::ReadItem<'b> {
            item
        }

        fn index(&self, index: usize) -> Self::ReadItem<'_> {
            if index < self.strided.len() {
                self.strided.index(index)
            } else {
                self.spilled.index(index - self.strided.len())
            }
        }

        fn len(&self) -> usize {
            self.strided.len() + self.spilled.len()
        }
    }

    impl PushInto<usize> for OffsetOptimized {
        fn push_into(&mut self, item: usize) {
            if !self.spilled.is_empty() {
                self.spilled.push(item);
            } else {
                let inserted = self.strided.push(item);
                if !inserted {
                    self.spilled.push(item);
                }
            }
        }
    }

    impl OffsetOptimized {
        pub fn heap_size(&self, callback: impl FnMut(usize, usize)) {
            crate::row_spine::offset_list_size(&self.spilled, callback);
        }
    }
}

/// Helper to compute the size of an [`OffsetList`] in memory.
#[inline]
pub(crate) fn offset_list_size(data: &OffsetList, mut callback: impl FnMut(usize, usize)) {
    // Private `vec_size` because we should only use it where data isn't region-allocated.
    // `T: Copy` makes sure the implementation is correct even if types change!
    #[inline(always)]
    fn vec_size<T: Copy>(data: &Vec<T>, mut callback: impl FnMut(usize, usize)) {
        let size_of_t = std::mem::size_of::<T>();
        callback(data.len() * size_of_t, data.capacity() * size_of_t);
    }

    vec_size(&data.smol, &mut callback);
    vec_size(&data.chonk, callback);
}
