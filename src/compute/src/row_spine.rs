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
pub use self::spines::{
    RowBatcher, RowBuilder, RowRowBatcher, RowRowBuilder, RowRowSpine, RowSpine, RowValBatcher,
    RowValBuilder, RowValSpine,
};
use differential_dataflow::trace::implementations::OffsetList;

/// Spines specialized to contain `Row` types in keys and values.
mod spines {
    use std::rc::Rc;

    use differential_dataflow::containers::{Columnation, TimelyStack};
    use differential_dataflow::trace::implementations::Layout;
    use differential_dataflow::trace::implementations::Update;
    use differential_dataflow::trace::implementations::ord_neu::{OrdKeyBatch, OrdKeyBuilder};
    use differential_dataflow::trace::implementations::ord_neu::{OrdValBatch, OrdValBuilder};
    use differential_dataflow::trace::implementations::spine_fueled::Spine;
    use differential_dataflow::trace::rc_blanket_impls::RcBuilder;
    use mz_repr::Row;

    use crate::row_spine::{DatumContainer, OffsetOptimized};
    use crate::typedefs::{KeyBatcher, KeyValBatcher};

    pub type RowRowSpine<T, R> = Spine<Rc<OrdValBatch<RowRowLayout<((Row, Row), T, R)>>>>;
    pub type RowRowBatcher<T, R> = KeyValBatcher<Row, Row, T, R>;
    pub type RowRowBuilder<T, R> =
        RcBuilder<OrdValBuilder<RowRowLayout<((Row, Row), T, R)>, TimelyStack<((Row, Row), T, R)>>>;

    pub type RowValSpine<V, T, R> = Spine<Rc<OrdValBatch<RowValLayout<((Row, V), T, R)>>>>;
    pub type RowValBatcher<V, T, R> = KeyValBatcher<Row, V, T, R>;
    pub type RowValBuilder<V, T, R> =
        RcBuilder<OrdValBuilder<RowValLayout<((Row, V), T, R)>, TimelyStack<((Row, V), T, R)>>>;

    pub type RowSpine<T, R> = Spine<Rc<OrdKeyBatch<RowLayout<((Row, ()), T, R)>>>>;
    pub type RowBatcher<T, R> = KeyBatcher<Row, T, R>;
    pub type RowBuilder<T, R> =
        RcBuilder<OrdKeyBuilder<RowLayout<((Row, ()), T, R)>, TimelyStack<((Row, ()), T, R)>>>;

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
        type KeyContainer = DatumContainer;
        type ValContainer = DatumContainer;
        type TimeContainer = TimelyStack<U::Time>;
        type DiffContainer = TimelyStack<U::Diff>;
        type OffsetContainer = OffsetOptimized;
    }
    impl<U: Update<Key = Row>> Layout for RowValLayout<U>
    where
        U::Val: Columnation,
        U::Time: Columnation,
        U::Diff: Columnation,
    {
        type KeyContainer = DatumContainer;
        type ValContainer = TimelyStack<U::Val>;
        type TimeContainer = TimelyStack<U::Time>;
        type DiffContainer = TimelyStack<U::Diff>;
        type OffsetContainer = OffsetOptimized;
    }
    impl<U: Update<Key = Row, Val = ()>> Layout for RowLayout<U>
    where
        U::Time: Columnation,
        U::Diff: Columnation,
    {
        type KeyContainer = DatumContainer;
        type ValContainer = TimelyStack<()>;
        type TimeContainer = TimelyStack<U::Time>;
        type DiffContainer = TimelyStack<U::Diff>;
        type OffsetContainer = OffsetOptimized;
    }
}

/// A `Row`-specialized container using dictionary compression.
mod container {

    use std::cmp::Ordering;

    use differential_dataflow::trace::implementations::BatchContainer;
    use timely::container::PushInto;

    use mz_repr::{Datum, Row, RowPacker, read_datum};

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
            self.bytes.push_into(item.bytes);
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
            DatumSeq {
                bytes: self.bytes.index(index),
            }
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

    #[derive(Debug)]
    pub struct DatumSeq<'a> {
        bytes: &'a [u8],
    }

    impl<'a> DatumSeq<'a> {
        #[inline]
        pub fn copy_into(&self, row: &mut RowPacker) {
            // SAFETY: `self.bytes` is a correctly formatted row.
            unsafe { row.extend_by_slice_unchecked(self.bytes) }
        }
        #[inline]
        fn as_bytes(&self) -> &'a [u8] {
            self.bytes
        }
        #[inline]
        pub fn to_row(&self) -> Row {
            // SAFETY: `self.bytes` is a correctly formatted row.
            unsafe { Row::from_bytes_unchecked(self.bytes) }
        }
    }

    impl<'a> Copy for DatumSeq<'a> {}
    impl<'a> Clone for DatumSeq<'a> {
        #[inline(always)]
        fn clone(&self) -> Self {
            *self
        }
    }

    impl<'a, 'b> PartialEq<DatumSeq<'a>> for DatumSeq<'b> {
        #[inline]
        fn eq(&self, other: &DatumSeq<'a>) -> bool {
            self.bytes.eq(other.bytes)
        }
    }
    impl<'a> PartialEq<&Row> for DatumSeq<'a> {
        #[inline]
        fn eq(&self, other: &&Row) -> bool {
            self.bytes.eq(other.data())
        }
    }
    impl<'a> Eq for DatumSeq<'a> {}
    impl<'a, 'b> PartialOrd<DatumSeq<'a>> for DatumSeq<'b> {
        #[inline]
        fn partial_cmp(&self, other: &DatumSeq<'a>) -> Option<Ordering> {
            Some(self.cmp(other))
        }
    }
    impl<'a> Ord for DatumSeq<'a> {
        #[inline]
        fn cmp(&self, other: &Self) -> Ordering {
            match self.bytes.len().cmp(&other.bytes.len()) {
                std::cmp::Ordering::Less => std::cmp::Ordering::Less,
                std::cmp::Ordering::Greater => std::cmp::Ordering::Greater,
                std::cmp::Ordering::Equal => self.bytes.cmp(other.bytes),
            }
        }
    }
    impl<'a> Iterator for DatumSeq<'a> {
        type Item = Datum<'a>;
        #[inline]
        fn next(&mut self) -> Option<Self::Item> {
            if self.bytes.is_empty() {
                None
            } else {
                let result = unsafe { read_datum(&mut self.bytes) };
                Some(result)
            }
        }
    }

    use mz_repr::fixed_length::ToDatumIter;
    impl<'long> ToDatumIter for DatumSeq<'long> {
        type DatumIter<'short>
            = DatumSeq<'short>
        where
            Self: 'short;
        #[inline]
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
                println!("{:?}", container.index(0).bytes);

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

    /// Offset container that compresses uniform-stride offsets into three integers.
    ///
    /// Replaces an enum-based design with flat fields to eliminate match dispatch
    /// on the hot index path. The common case (all rows same length) becomes a
    /// single branch + multiply + branchless min.
    pub struct OffsetOptimized {
        /// The stride value (byte length of each row when uniform).
        stride: usize,
        /// Number of strictly-increasing strided elements where offset[i] = stride * i.
        steps: usize,
        /// Number of additional saturated elements where offset = stride * (steps - 1).
        reps: usize,
        /// Fallback for elements that break the stride pattern.
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
            self.stride = 0;
            self.steps = 0;
            self.reps = 0;
            self.spilled.clear();
        }

        fn with_capacity(_size: usize) -> Self {
            Self {
                stride: 0,
                steps: 0,
                reps: 0,
                spilled: OffsetList::with_capacity(0),
            }
        }

        fn merge_capacity(_cont1: &Self, _cont2: &Self) -> Self {
            Self {
                stride: 0,
                steps: 0,
                reps: 0,
                spilled: OffsetList::with_capacity(0),
            }
        }

        #[inline]
        fn reborrow<'b, 'a: 'b>(item: Self::ReadItem<'a>) -> Self::ReadItem<'b> {
            item
        }

        #[inline(always)]
        fn index(&self, index: usize) -> Self::ReadItem<'_> {
            let strided_len = self.steps + self.reps;
            if index < strided_len {
                // Branchless: min(index, steps - 1) handles both Striding and
                // Saturated cases. For Striding (reps == 0), index < steps always
                // holds so min is a no-op. For Saturated, indices >= steps clamp
                // to steps - 1.
                let clamped = index.min(self.steps.wrapping_sub(1));
                self.stride * clamped
            } else {
                self.spilled.index(index - strided_len)
            }
        }

        #[inline(always)]
        fn len(&self) -> usize {
            self.steps + self.reps + self.spilled.len()
        }
    }

    impl PushInto<usize> for OffsetOptimized {
        #[inline]
        fn push_into(&mut self, item: usize) {
            if !self.spilled.is_empty() {
                self.spilled.push(item);
                return;
            }
            let total = self.steps + self.reps;
            let accepted = if total == 0 {
                // First element must be 0.
                if item == 0 {
                    self.steps = 1;
                    true
                } else {
                    false
                }
            } else if self.steps == 1 && self.reps == 0 {
                // Second element sets the stride.
                self.stride = item;
                self.steps = 2;
                true
            } else if self.reps == 0 {
                // Striding: accept next stride step, or transition to saturated.
                if item == self.stride * self.steps {
                    self.steps += 1;
                    true
                } else if item == self.stride * (self.steps - 1) {
                    self.reps = 1;
                    true
                } else {
                    false
                }
            } else {
                // Saturated: only accept repeated max value.
                if item == self.stride * (self.steps - 1) {
                    self.reps += 1;
                    true
                } else {
                    false
                }
            };
            if !accepted {
                self.spilled.push(item);
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
