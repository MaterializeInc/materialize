// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub use self::container::DatumContainer;
pub use self::offset_opt::OffsetOptimized;
pub use self::spines::{
    RowBatcher, RowBuilder, RowSpine, RowValBatcher, RowValBuilder, RowValSpine,
};
use differential_dataflow::trace::implementations::OffsetList;
pub use mz_repr::DatumSeq;

/// Spines specialized to contain `Row` types in keys and values.
mod spines {
    use std::rc::Rc;

    use columnation::Columnation;
    use differential_dataflow::trace::implementations::Layout;
    use differential_dataflow::trace::implementations::Update;
    use differential_dataflow::trace::implementations::ord_neu::{OrdKeyBatch, OrdKeyBuilder};
    use differential_dataflow::trace::implementations::ord_neu::{OrdValBatch, OrdValBuilder};
    use differential_dataflow::trace::implementations::spine_fueled::Spine;
    use differential_dataflow::trace::rc_blanket_impls::RcBuilder;
    use mz_repr::Row;
    use mz_timely_util::columnation::ColumnationStack;

    use crate::row_spine::{DatumContainer, OffsetOptimized};
    use crate::typedefs::{KeyBatcher, KeyValBatcher};

    /// Spine for `Row` keys with arbitrary `Columnation` values.
    pub type RowValSpine<V, T, R> = Spine<Rc<OrdValBatch<RowValLayout<((Row, V), T, R)>>>>;
    /// Batcher for [`RowValSpine`].
    pub type RowValBatcher<V, T, R> = KeyValBatcher<Row, V, T, R>;
    /// Builder for [`RowValSpine`].
    pub type RowValBuilder<V, T, R> = RcBuilder<
        OrdValBuilder<RowValLayout<((Row, V), T, R)>, ColumnationStack<((Row, V), T, R)>>,
    >;

    /// Key-only spine specialized for `Row` keys.
    pub type RowSpine<T, R> = Spine<Rc<OrdKeyBatch<RowLayout<((Row, ()), T, R)>>>>;
    /// Batcher for [`RowSpine`].
    pub type RowBatcher<T, R> = KeyBatcher<Row, T, R>;
    /// Builder for [`RowSpine`].
    pub type RowBuilder<T, R> =
        RcBuilder<OrdKeyBuilder<RowLayout<((Row, ()), T, R)>, ColumnationStack<((Row, ()), T, R)>>>;

    /// Layout for [`RowValSpine`]: `DatumContainer` keys, `ColumnationStack` values.
    pub struct RowValLayout<U: Update<Key = Row>> {
        phantom: std::marker::PhantomData<U>,
    }
    /// Layout for [`RowSpine`]: `DatumContainer` keys, unit values.
    pub struct RowLayout<U: Update<Key = Row, Val = ()>> {
        phantom: std::marker::PhantomData<U>,
    }

    impl<U: Update<Key = Row>> Layout for RowValLayout<U>
    where
        U::Val: Columnation,
        U::Time: Columnation,
        U::Diff: Columnation,
    {
        type KeyContainer = DatumContainer;
        type ValContainer = ColumnationStack<U::Val>;
        type TimeContainer = ColumnationStack<U::Time>;
        type DiffContainer = ColumnationStack<U::Diff>;
        type OffsetContainer = OffsetOptimized;
    }
    impl<U: Update<Key = Row, Val = ()>> Layout for RowLayout<U>
    where
        U::Time: Columnation,
        U::Diff: Columnation,
    {
        type KeyContainer = DatumContainer;
        type ValContainer = ColumnationStack<()>;
        type TimeContainer = ColumnationStack<U::Time>;
        type DiffContainer = ColumnationStack<U::Diff>;
        type OffsetContainer = OffsetOptimized;
    }
}

/// A `Row`-specialized container using dictionary compression.
mod container {

    use differential_dataflow::trace::implementations::BatchContainer;
    use timely::container::PushInto;

    use mz_repr::{DatumSeq, Row};

    use super::bytes_container::BytesContainer;

    /// Container wrapping `BytesContainer` that traffics only in `Row`-formatted bytes.
    ///
    /// This type accepts only `Row`-formatted bytes in its `Push` implementation, and
    /// in return provides a `DatumSeq` view of the bytes which can be decoded as `Datum`s.
    pub struct DatumContainer {
        bytes: BytesContainer,
    }

    impl DatumContainer {
        /// Visit contained allocations to determine their size and capacity.
        #[inline]
        pub fn heap_size(&self, callback: impl FnMut(usize, usize)) {
            self.bytes.heap_size(callback)
        }
    }

    impl BatchContainer for DatumContainer {
        type Owned = Row;
        type ReadItem<'a> = DatumSeq<'a>;

        #[inline(always)]
        fn into_owned<'a>(item: Self::ReadItem<'a>) -> Self::Owned {
            item.to_row()
        }

        #[inline]
        fn clone_onto<'a>(item: Self::ReadItem<'a>, other: &mut Self::Owned) {
            let mut packer = other.packer();
            item.copy_into(&mut packer);
        }

        #[inline(always)]
        fn push_ref(&mut self, item: Self::ReadItem<'_>) {
            self.bytes.push_into(item.as_bytes());
        }

        #[inline(always)]
        fn push_own(&mut self, item: &Self::Owned) {
            self.bytes.push_into(item.data());
        }

        #[inline(always)]
        fn clear(&mut self) {
            self.bytes.clear();
        }

        #[inline(always)]
        fn with_capacity(size: usize) -> Self {
            Self {
                bytes: BytesContainer::with_capacity(size),
            }
        }

        #[inline(always)]
        fn merge_capacity(cont1: &Self, cont2: &Self) -> Self {
            Self {
                bytes: BytesContainer::merge_capacity(&cont1.bytes, &cont2.bytes),
            }
        }

        #[inline(always)]
        fn reborrow<'b, 'a: 'b>(item: Self::ReadItem<'a>) -> Self::ReadItem<'b> {
            item
        }

        #[inline(always)]
        fn index(&self, index: usize) -> Self::ReadItem<'_> {
            // SAFETY: `DatumContainer` only accepts validly-encoded row bytes.
            unsafe { DatumSeq::from_bytes(self.bytes.index(index)) }
        }

        #[inline(always)]
        fn len(&self) -> usize {
            self.bytes.len()
        }
    }

    impl PushInto<Row> for DatumContainer {
        fn push_into(&mut self, item: Row) {
            self.push_into(&item);
        }
    }

    impl PushInto<&Row> for DatumContainer {
        fn push_into(&mut self, item: &Row) {
            self.push_own(item);
        }
    }

    impl PushInto<DatumSeq<'_>> for DatumContainer {
        fn push_into(&mut self, item: DatumSeq<'_>) {
            self.bytes.push_into(item.as_bytes())
        }
    }

    #[cfg(test)]
    mod tests {
        use crate::row_spine::DatumContainer;
        use differential_dataflow::trace::implementations::BatchContainer;
        use mz_repr::adt::date::Date;
        use mz_repr::adt::interval::Interval;
        use mz_repr::{Datum, Row, SqlScalarType};

        #[mz_ore::test]
        #[cfg_attr(miri, ignore)] // unsupported operation: integer-to-pointer casts and `ptr::with_exposed_provenance` are not supported
        fn test_round_trip() {
            fn round_trip(datums: Vec<Datum>) {
                let row = Row::pack(datums.clone());

                let mut container = DatumContainer::with_capacity(row.byte_len());
                container.push_own(&row);

                // When run under miri this catches undefined bytes written to data
                // eg by calling push_copy! on a type which contains undefined padding values
                println!("{:?}", container.index(0).as_bytes());

                let datums2 = container.index(0).collect::<Vec<_>>();
                assert_eq!(datums, datums2);
            }

            round_trip(vec![]);
            round_trip(
                SqlScalarType::enumerate()
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

mod bytes_container {

    use differential_dataflow::trace::implementations::BatchContainer;
    use timely::container::PushInto;

    use mz_ore::region::Region;

    /// A slice container with four bytes overhead per slice.
    pub struct BytesContainer {
        /// Total length of `batches`, maintained because recomputation is expensive.
        length: usize,
        batches: Vec<BytesBatch>,
    }

    impl BytesContainer {
        /// Visit contained allocations to determine their size and capacity.
        #[inline]
        pub fn heap_size(&self, mut callback: impl FnMut(usize, usize)) {
            // Calculate heap size for local, stash, and stash entries
            callback(
                self.batches.len() * std::mem::size_of::<BytesBatch>(),
                self.batches.capacity() * std::mem::size_of::<BytesBatch>(),
            );
            for batch in self.batches.iter() {
                batch.offsets.heap_size(&mut callback);
                callback(batch.storage.len(), batch.storage.capacity());
            }
        }
    }

    impl BatchContainer for BytesContainer {
        type Owned = Vec<u8>;
        type ReadItem<'a> = &'a [u8];

        #[inline]
        fn into_owned<'a>(item: Self::ReadItem<'a>) -> Self::Owned {
            item.to_vec()
        }

        #[inline]
        fn clone_onto<'a>(item: Self::ReadItem<'a>, other: &mut Self::Owned) {
            other.clear();
            other.extend_from_slice(item);
        }

        #[inline(always)]
        fn push_ref(&mut self, item: Self::ReadItem<'_>) {
            self.push_into(item);
        }

        #[inline(always)]
        fn push_own(&mut self, item: &Self::Owned) {
            self.push_into(item.as_slice())
        }

        fn clear(&mut self) {
            self.batches.clear();
            self.batches.push(BytesBatch::with_capacities(0, 0));
            self.length = 0;
        }

        fn with_capacity(size: usize) -> Self {
            Self {
                length: 0,
                batches: vec![BytesBatch::with_capacities(size, size)],
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
                length: 0,
                batches: vec![BytesBatch::with_capacities(item_cap, byte_cap)],
            }
        }

        #[inline(always)]
        fn reborrow<'b, 'a: 'b>(item: Self::ReadItem<'a>) -> Self::ReadItem<'b> {
            item
        }

        #[inline]
        fn index(&self, mut index: usize) -> Self::ReadItem<'_> {
            for batch in self.batches.iter() {
                if index < batch.len() {
                    return batch.index(index);
                }
                index -= batch.len();
            }
            panic!("Index out of bounds");
        }

        #[inline(always)]
        fn len(&self) -> usize {
            self.length
        }
    }

    impl PushInto<&[u8]> for BytesContainer {
        #[inline]
        fn push_into(&mut self, item: &[u8]) {
            self.length += 1;
            if let Some(batch) = self.batches.last_mut() {
                let success = batch.try_push(item);
                if !success {
                    // double the lengths from `batch`.
                    let item_cap = 2 * batch.offsets.len();
                    let byte_cap = std::cmp::max(2 * batch.storage.capacity(), item.len());
                    let mut new_batch = BytesBatch::with_capacities(item_cap, byte_cap);
                    assert!(new_batch.try_push(item));
                    self.batches.push(new_batch);
                }
            }
        }
    }

    /// A batch of slice storage.
    ///
    /// The backing storage for this batch will not be resized.
    pub struct BytesBatch {
        offsets: crate::row_spine::OffsetOptimized,
        storage: Region<u8>,
        len: usize,
    }

    impl BytesBatch {
        /// Either accepts the slice and returns true,
        /// or does not and returns false.
        fn try_push(&mut self, slice: &[u8]) -> bool {
            if self.storage.len() + slice.len() <= self.storage.capacity() {
                self.storage.extend_from_slice(slice);
                self.offsets.push_into(self.storage.len());
                self.len += 1;
                true
            } else {
                false
            }
        }
        #[inline]
        fn index(&self, index: usize) -> &[u8] {
            let lower = self.offsets.index(index);
            let upper = self.offsets.index(index + 1);
            &self.storage[lower..upper]
        }
        #[inline(always)]
        fn len(&self) -> usize {
            debug_assert_eq!(self.len, self.offsets.len() - 1);
            self.len
        }

        fn with_capacities(item_cap: usize, byte_cap: usize) -> Self {
            // TODO: be wary of `byte_cap` greater than 2^32.
            let mut offsets = crate::row_spine::OffsetOptimized::with_capacity(item_cap + 1);
            offsets.push_into(0);
            Self {
                offsets,
                storage: Region::new_auto(byte_cap.next_power_of_two()),
                len: 0,
            }
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
        #[inline]
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

        #[inline]
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

        #[inline]
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

        #[inline]
        fn into_owned<'a>(item: Self::ReadItem<'a>) -> Self::Owned {
            item
        }

        #[inline]
        fn push_ref(&mut self, item: Self::ReadItem<'_>) {
            self.push_into(item)
        }

        #[inline]
        fn push_own(&mut self, item: &Self::Owned) {
            self.push_into(*item)
        }

        fn clear(&mut self) {
            self.strided = OffsetStride::Empty;
            self.spilled.clear();
        }

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

        #[inline]
        fn reborrow<'b, 'a: 'b>(item: Self::ReadItem<'a>) -> Self::ReadItem<'b> {
            item
        }

        #[inline]
        fn index(&self, index: usize) -> Self::ReadItem<'_> {
            if index < self.strided.len() {
                self.strided.index(index)
            } else {
                self.spilled.index(index - self.strided.len())
            }
        }

        #[inline]
        fn len(&self) -> usize {
            self.strided.len() + self.spilled.len()
        }
    }

    impl PushInto<usize> for OffsetOptimized {
        #[inline]
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
