// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub use self::dictionary::DatumContainer;
pub use self::dictionary::DatumSeq;
pub use self::offset_opt::OffsetOptimized;
pub use self::spines::{
    RowBatcher, RowBuilder, RowRowBatcher, RowRowBuilder, RowRowSpine, RowSpine, RowValBatcher,
    RowValBuilder, RowValSpine,
};
use differential_dataflow::trace::implementations::OffsetList;

/// Enable per-column dictionary compression in row containers.
pub static DICTIONARY_COMPRESSION: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// Spines specialized to contain `Row` types in keys and values.
mod spines {
    use std::rc::Rc;

    use differential_dataflow::containers::{Columnation, TimelyStack};
    use differential_dataflow::trace::implementations::Layout;
    use differential_dataflow::trace::implementations::Update;
    use differential_dataflow::trace::implementations::ord_neu::OrdKeyBatch;
    use differential_dataflow::trace::implementations::ord_neu::OrdValBatch;
    use differential_dataflow::trace::implementations::spine_fueled::Spine;
    use differential_dataflow::trace::rc_blanket_impls::RcBuilder;
    use mz_repr::Row;

    use crate::row_spine::{DatumContainer, OffsetOptimized};
    use crate::typedefs::{KeyBatcher, KeyValBatcher};

    pub type RowRowSpine<T, R> = Spine<Rc<OrdValBatch<RowRowLayout<((Row, Row), T, R)>>>>;
    pub type RowRowBatcher<T, R> = KeyValBatcher<Row, Row, T, R>;
    pub type RowRowBuilder<T, R> =
        RcBuilder<crate::row_spine::dictionary::builders::RowRowBuilder<T, R>>;

    pub type RowValSpine<V, T, R> = Spine<Rc<OrdValBatch<RowValLayout<((Row, V), T, R)>>>>;
    pub type RowValBatcher<V, T, R> = KeyValBatcher<Row, V, T, R>;
    pub type RowValBuilder<V, T, R> =
        RcBuilder<crate::row_spine::dictionary::builders::RowValBuilder<V, T, R>>;

    pub type RowSpine<T, R> = Spine<Rc<OrdKeyBatch<RowLayout<((Row, ()), T, R)>>>>;
    pub type RowBatcher<T, R> = KeyBatcher<Row, T, R>;
    pub type RowBuilder<T, R> = RcBuilder<crate::row_spine::dictionary::builders::RowBuilder<T, R>>;

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

/// A `Row`-specialized container.
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
        #[allow(unused)]
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
        pub fn as_bytes(&self) -> &'a [u8] {
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
                println!("{:?}", container.index(0).iter.data);

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

/// A `[u8]`-specialized container.
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

/// A `Row`-specialized container using dictionary compression.
mod dictionary {

    use differential_dataflow::trace::implementations::BatchContainer;

    use mz_repr::Row;

    use super::row_codec::{Codec, ColumnsCodec, ColumnsIter};

    pub mod builders {

        use differential_dataflow::containers::Columnation;
        use differential_dataflow::containers::TimelyStack;
        use differential_dataflow::difference::Semigroup;
        use differential_dataflow::lattice::Lattice;
        use differential_dataflow::trace::Builder;
        use differential_dataflow::trace::Description;
        use differential_dataflow::trace::implementations::ord_neu::{OrdKeyBatch, OrdKeyBuilder};
        use differential_dataflow::trace::implementations::ord_neu::{OrdValBatch, OrdValBuilder};
        use timely::progress::Timestamp;

        use mz_repr::Row;

        use super::super::row_codec::{Codec, ColumnsCodec};
        use super::{DatumContainer, DatumSeq};
        use crate::row_spine::DICTIONARY_COMPRESSION;
        use crate::row_spine::spines::{RowLayout, RowRowLayout, RowValLayout};

        pub struct RowRowBuilder<
            T: Lattice + Timestamp + Columnation,
            R: Ord + Semigroup + Columnation + 'static,
        > {
            inner: OrdValBuilder<RowRowLayout<((Row, Row), T, R)>, TimelyStack<((Row, Row), T, R)>>,
        }

        impl<T: Lattice + Timestamp + Columnation, R: Ord + Semigroup + Columnation + 'static>
            Builder for RowRowBuilder<T, R>
        {
            type Input = TimelyStack<((Row, Row), T, R)>;
            type Time = T;
            type Output = OrdValBatch<RowRowLayout<((Row, Row), T, R)>>;

            fn with_capacity(keys: usize, vals: usize, upds: usize) -> Self {
                Self {
                    inner: Builder::with_capacity(keys, vals, upds),
                }
            }
            fn push(&mut self, chunk: &mut Self::Input) {
                self.inner.push(chunk)
            }
            fn done(self, description: Description<Self::Time>) -> Self::Output {
                self.inner.done(description)
            }
            fn seal(
                chain: &mut Vec<Self::Input>,
                description: Description<Self::Time>,
            ) -> Self::Output {
                let (key_codec, val_codec) = if DICTIONARY_COMPRESSION
                    .load(std::sync::atomic::Ordering::Relaxed)
                {
                    let mut key_codec = ColumnsCodec::default();
                    let mut val_codec = ColumnsCodec::default();
                    let mut vec = Vec::default();
                    for link in chain.iter() {
                        for ((key, val), _, _) in link.iter() {
                            if !key.is_empty() {
                                key_codec.encode(DatumSeq::borrow_as(key).bytes_iter(), &mut vec);
                                vec.clear();
                            }
                            if !val.is_empty() {
                                val_codec.encode(DatumSeq::borrow_as(val).bytes_iter(), &mut vec);
                                vec.clear();
                            }
                        }
                    }
                    (
                        Some(ColumnsCodec::new_from([&key_codec])),
                        Some(ColumnsCodec::new_from([&val_codec])),
                    )
                } else {
                    (None, None)
                };

                use differential_dataflow::trace::implementations::BuilderInput;

                let (keys, vals, upds) = <Self::Input as BuilderInput<
                    DatumContainer,
                    DatumContainer,
                >>::key_val_upd_counts(&chain[..]);
                let mut builder = Self::with_capacity(keys, vals, upds);
                if let Some(key_codec) = &key_codec {
                    key_codec.report();
                }
                builder.inner.result.keys.codec = key_codec;
                if let Some(val_codec) = &val_codec {
                    val_codec.report();
                }
                builder.inner.result.vals.vals.codec = val_codec;

                for mut chunk in chain.drain(..) {
                    builder.push(&mut chunk);
                }

                builder.done(description)
            }
        }

        pub struct RowValBuilder<
            V: Ord + Clone + Columnation + 'static,
            T: Lattice + Timestamp + Columnation,
            R: Ord + Semigroup + Columnation + 'static,
        > {
            inner: OrdValBuilder<RowValLayout<((Row, V), T, R)>, TimelyStack<((Row, V), T, R)>>,
        }

        impl<
            V: Ord + Clone + Columnation,
            T: Lattice + Timestamp + Columnation,
            R: Ord + Semigroup + Columnation + 'static,
        > Builder for RowValBuilder<V, T, R>
        {
            type Input = TimelyStack<((Row, V), T, R)>;
            type Time = T;
            type Output = OrdValBatch<RowValLayout<((Row, V), T, R)>>;

            fn with_capacity(keys: usize, vals: usize, upds: usize) -> Self {
                Self {
                    inner: Builder::with_capacity(keys, vals, upds),
                }
            }
            fn push(&mut self, chunk: &mut Self::Input) {
                self.inner.push(chunk)
            }
            fn done(self, description: Description<Self::Time>) -> Self::Output {
                self.inner.done(description)
            }
            fn seal(
                chain: &mut Vec<Self::Input>,
                description: Description<Self::Time>,
            ) -> Self::Output {
                let key_codec = if DICTIONARY_COMPRESSION.load(std::sync::atomic::Ordering::Relaxed)
                {
                    let mut key_codec = ColumnsCodec::default();
                    let mut vec = Vec::default();
                    for link in chain.iter() {
                        for ((key, _), _, _) in link.iter() {
                            if !key.is_empty() {
                                key_codec.encode(DatumSeq::borrow_as(key).bytes_iter(), &mut vec);
                                vec.clear();
                            }
                        }
                    }
                    Some(ColumnsCodec::new_from([&key_codec]))
                } else {
                    None
                };

                use differential_dataflow::trace::implementations::BuilderInput;

                let (keys, vals, upds) = <Self::Input as BuilderInput<
                    DatumContainer,
                    TimelyStack<V>,
                >>::key_val_upd_counts(&chain[..]);
                let mut builder = Self::with_capacity(keys, vals, upds);

                if let Some(key_codec) = &key_codec {
                    key_codec.report();
                }
                builder.inner.result.keys.codec = key_codec;

                for mut chunk in chain.drain(..) {
                    builder.push(&mut chunk);
                }

                builder.done(description)
            }
        }

        pub struct RowBuilder<
            T: Lattice + Timestamp + Columnation,
            R: Ord + Semigroup + Columnation + 'static,
        > {
            inner: OrdKeyBuilder<RowLayout<((Row, ()), T, R)>, TimelyStack<((Row, ()), T, R)>>,
        }

        impl<T: Lattice + Timestamp + Columnation, R: Ord + Semigroup + Columnation + 'static>
            Builder for RowBuilder<T, R>
        {
            type Input = TimelyStack<((Row, ()), T, R)>;
            type Time = T;
            type Output = OrdKeyBatch<RowLayout<((Row, ()), T, R)>>;

            fn with_capacity(keys: usize, vals: usize, upds: usize) -> Self {
                Self {
                    inner: Builder::with_capacity(keys, vals, upds),
                }
            }
            fn push(&mut self, chunk: &mut Self::Input) {
                self.inner.push(chunk)
            }
            fn done(self, description: Description<Self::Time>) -> Self::Output {
                self.inner.done(description)
            }
            fn seal(
                chain: &mut Vec<Self::Input>,
                description: Description<Self::Time>,
            ) -> Self::Output {
                let key_codec = if DICTIONARY_COMPRESSION.load(std::sync::atomic::Ordering::Relaxed)
                {
                    let mut key_codec = ColumnsCodec::default();
                    let mut vec = Vec::default();
                    for link in chain.iter() {
                        for ((key, _), _, _) in link.iter() {
                            if !key.is_empty() {
                                key_codec.encode(DatumSeq::borrow_as(key).bytes_iter(), &mut vec);
                                vec.clear();
                            }
                        }
                    }
                    Some(ColumnsCodec::new_from([&key_codec]))
                } else {
                    None
                };

                use differential_dataflow::trace::implementations::BuilderInput;

                let (keys, vals, upds) = <Self::Input as BuilderInput<
                    DatumContainer,
                    TimelyStack<()>,
                >>::key_val_upd_counts(&chain[..]);
                let mut builder = Self::with_capacity(keys, vals, upds);
                if let Some(key_codec) = &key_codec {
                    key_codec.report();
                }
                builder.inner.result.keys.codec = key_codec;

                for mut chunk in chain.drain(..) {
                    builder.push(&mut chunk);
                }

                builder.done(description)
            }
        }
    }

    // #[derive(Default)]
    pub struct DatumContainer {
        /// DictionaryCodec with encoder, decoder, and stastistics.
        codec: Option<ColumnsCodec>,
        /// A list of rows
        inner: super::bytes_container::BytesContainer,
        /// Staging buffer for ingested `Row` types.
        staging: Vec<u8>,
    }

    impl BatchContainer for DatumContainer {
        type Owned = Row;
        type ReadItem<'a> = DatumSeq<'a>;

        fn with_capacity(size: usize) -> Self {
            let codec = if crate::row_spine::DICTIONARY_COMPRESSION
                .load(std::sync::atomic::Ordering::Relaxed)
            {
                Some(Default::default())
            } else {
                None
            };

            Self {
                codec,
                inner: BatchContainer::with_capacity(size),
                staging: Vec::new(),
            }
        }
        fn merge_capacity(cont1: &Self, cont2: &Self) -> Self {
            let codec = match (&cont1.codec, &cont2.codec) {
                (Some(c1), Some(c2)) => {
                    c1.report();
                    c2.report();
                    Some(ColumnsCodec::new_from([c1, c2]))
                }
                (None, None) => None,
                _ => {
                    panic!("Unclear codec configuration!")
                }
            };

            Self {
                codec,
                inner: BatchContainer::merge_capacity(&cont1.inner, &cont2.inner),
                staging: Vec::new(),
            }
        }
        #[inline]
        fn index(&self, index: usize) -> Self::ReadItem<'_> {
            let data = self.inner.index(index);
            let iter = if let Some(codec) = &self.codec {
                codec.decode(data)
            } else {
                // Safety: without a codec we only push rows or datumseqs into `self.inner`.
                // Each retrieved byte slice should be row-encoded data, as long as we have
                // not unset the codec in the interim.
                unsafe { ColumnsIter::without_codec(data) }
            };
            DatumSeq { iter }
        }
        #[inline(always)]
        fn len(&self) -> usize {
            self.inner.len()
        }

        #[inline(always)]
        fn reborrow<'b, 'a: 'b>(item: Self::ReadItem<'a>) -> Self::ReadItem<'b> {
            item
        }

        #[inline(always)]
        fn into_owned<'a>(item: Self::ReadItem<'a>) -> Self::Owned {
            Row::pack(item)
        }

        #[inline(always)]
        fn clone_onto<'a>(item: Self::ReadItem<'a>, other: &mut Self::Owned) {
            other.packer().extend(item);
        }

        #[inline(always)]
        fn push_ref(&mut self, item: Self::ReadItem<'_>) {
            self.push_into(item);
        }

        #[inline(always)]
        fn push_own(&mut self, item: &Self::Owned) {
            self.push_into(item);
        }

        #[inline(always)]
        fn clear(&mut self) {
            self.inner.clear();
            self.staging.clear();
            if let Some(codec) = &mut self.codec {
                codec.clear();
            }
        }
    }

    impl DatumContainer {
        /// Visit contained allocations to determine their size and capacity.
        #[inline]
        pub fn heap_size(&self, callback: impl FnMut(usize, usize)) {
            self.inner.heap_size(callback)
        }
    }

    use timely::container::PushInto;
    impl PushInto<Row> for DatumContainer {
        #[inline(always)]
        fn push_into(&mut self, item: Row) {
            self.push_into(&item);
        }
    }

    impl PushInto<&Row> for DatumContainer {
        #[inline(always)]
        fn push_into(&mut self, item: &Row) {
            self.push_into(DatumSeq::borrow_as(item));
        }
    }

    impl PushInto<DatumSeq<'_>> for DatumContainer {
        #[inline]
        fn push_into(&mut self, item: DatumSeq<'_>) {
            if let Some(codec) = &mut self.codec {
                codec.encode(item.bytes_iter(), &mut self.staging);
            } else {
                for slice in item.bytes_iter() {
                    self.staging.extend_from_slice(slice);
                }
            }
            // TODO: Copy rather than clone, into better storage.
            self.inner.push_ref(&self.staging[..]);
            self.staging.clear();
        }
    }

    use mz_repr::{Datum, read_datum};

    /// A reference that can be resolved to a sequence of `Datum`s.
    ///
    /// This type must "compare" as if decoded to a `Row`, which means it needs to track
    /// various nuances of `Row::cmp`, which at the moment is first by length, and then by
    /// the raw binary slice backing the row. Neither of those are explicit in this struct.
    /// We will need to produce them in order to perform comparisons.
    #[derive(Debug)]
    pub struct DatumSeq<'a> {
        pub iter: ColumnsIter<'a>,
    }

    impl<'a> DatumSeq<'a> {
        #[inline(always)]
        fn borrow_as(other: &'a Row) -> Self {
            Self {
                iter: ColumnsCodec::borrow_row(other),
            }
        }

        #[inline]
        pub fn to_row(&self) -> Row {
            Row::pack(*self)
        }
    }

    impl<'a> Copy for DatumSeq<'a> {}
    impl<'a> Clone for DatumSeq<'a> {
        #[inline(always)]
        fn clone(&self) -> Self {
            *self
        }
    }

    use std::cmp::Ordering;
    impl<'a, 'b> PartialEq<DatumSeq<'a>> for DatumSeq<'b> {
        #[inline(always)]
        fn eq(&self, other: &DatumSeq<'a>) -> bool {
            Iterator::eq(self.iter, other.iter)
        }
    }
    impl<'a> Eq for DatumSeq<'a> {}
    impl<'a, 'b> PartialOrd<DatumSeq<'a>> for DatumSeq<'b> {
        #[inline(always)]
        fn partial_cmp(&self, other: &DatumSeq<'a>) -> Option<Ordering> {
            let len1: usize = self.iter.map(|b| b.len()).sum();
            let len2: usize = other.iter.map(|b| b.len()).sum();
            if len1 == len2 {
                // Lexicographic, when lengths are equal.
                Some(self.iter.flatten().cmp(other.iter.flatten()))
            } else {
                Some(len1.cmp(&len2))
            }
        }
    }
    impl<'a> Ord for DatumSeq<'a> {
        #[inline(always)]
        fn cmp(&self, other: &Self) -> Ordering {
            self.partial_cmp(other).unwrap()
        }
    }

    impl<'a> PartialEq<&'a Row> for DatumSeq<'a> {
        #[inline(always)]
        fn eq(&self, other: &&'a Row) -> bool {
            self.eq(&Self::borrow_as(*other))
        }
    }

    impl<'a> DatumSeq<'a> {
        #[inline(always)]
        pub fn bytes_iter(self) -> ColumnsIter<'a> {
            self.iter
        }
    }

    impl<'a> Iterator for DatumSeq<'a> {
        type Item = Datum<'a>;
        #[inline(always)]
        fn next(&mut self) -> Option<Self::Item> {
            self.iter
                .next()
                .map(|mut bytes| unsafe { read_datum(&mut bytes) })
        }
    }

    use mz_repr::fixed_length::ToDatumIter;
    impl<'long> ToDatumIter for DatumSeq<'long> {
        type DatumIter<'short>
            = DatumSeq<'short>
        where
            Self: 'short;
        #[inline(always)]
        fn to_datum_iter<'short>(&'short self) -> Self::DatumIter<'short> {
            *self
        }
    }
}

/// Traits abstracting the processes of encoding and decoding row-encoded byte sequences.
///
/// It is unsafe to use these types to encode byte sequences that are not row-encoded,
/// as they are parsed out of contiguous `[u8]` slices using `mz_repr::read_datum`.
mod row_codec {

    use mz_repr::Row;

    pub use self::misra_gries::MisraGries;
    pub use columns::{ColumnsCodec, ColumnsIter};
    pub use dictionary::DictionaryCodec;

    pub trait Codec: Default + 'static {
        /// The iterator type returned by decoding.
        type DecodeIter<'a>: Iterator<Item = &'a [u8]> + Copy;
        /// Decodes an input byte slice into a sequence of byte slices.
        fn decode<'a>(&'a self, bytes: &'a [u8]) -> Self::DecodeIter<'a>;
        /// Encodes a sequence of byte slices into an output byte slice.
        fn encode<'a, I>(&mut self, iter: I, output: &mut Vec<u8>)
        where
            I: IntoIterator<Item = &'a [u8]>;
        /// Constructs a new instance of `Self` from accumulated statistics.
        /// These statistics should cover the data the output expects to see.
        fn new_from<'a>(stats: impl IntoIterator<Item = &'a Self>) -> Self;
        /// Diagnostic information about the state of the codec.
        fn report(&self) {}

        fn borrow_row<'a>(row: &'a Row) -> Self::DecodeIter<'a>;
    }

    mod columns {

        use mz_repr::{Row, read_datum};

        use super::{Codec, DictionaryCodec};

        /// Independently encodes each column.
        #[derive(Default, Debug)]
        pub struct ColumnsCodec {
            columns: Vec<DictionaryCodec>,
            bytes: usize,
            total: usize,
        }

        impl ColumnsCodec {
            #[inline(always)]
            pub fn clear(&mut self) {
                self.columns.clear();
                self.bytes = 0;
                self.total = 0;
            }
        }

        impl Codec for ColumnsCodec {
            type DecodeIter<'a> = ColumnsIter<'a>;
            fn decode<'a>(&'a self, bytes: &'a [u8]) -> Self::DecodeIter<'a> {
                ColumnsIter {
                    index: Some(self),
                    column: 0,
                    data: bytes,
                }
            }
            fn encode<'a, I>(&mut self, iter: I, output: &mut Vec<u8>)
            where
                I: IntoIterator<Item = &'a [u8]>,
            {
                let mut iter = iter.into_iter();
                let mut index = 0;
                while let Some(bytes) = iter.next() {
                    self.total += bytes.len();
                    if self.columns.len() <= index {
                        self.columns.push(Default::default());
                    }
                    self.columns[index].encode(std::iter::once(bytes), output);
                    index += 1;
                }
                self.bytes += output.len();
            }

            fn new_from<'a>(stats: impl IntoIterator<Item = &'a Self>) -> Self {
                // Is it possible that one of the inputs has no stats?
                let stats = stats.into_iter().collect::<Vec<_>>();
                let cols = stats.iter().map(|s| s.columns.len()).max().unwrap_or(0);
                let mut columns = Vec::with_capacity(cols);
                let default: DictionaryCodec = Default::default();
                for index in 0..cols {
                    columns.push(DictionaryCodec::new_from(
                        stats
                            .iter()
                            .map(|s| s.columns.get(index).unwrap_or(&default)),
                    ));
                }
                Self {
                    columns,
                    total: 0,
                    bytes: 0,
                }
            }
            fn report(&self) {
                if self.total > 500000 {
                    //} && self.columns.iter().all(|c| c.decode.len() > 0) {
                    println!(
                        "REPORT: {:?} -> {:?} (x{:?})",
                        self.total,
                        self.bytes,
                        self.total / self.bytes
                    );
                    println!("COLUMNS: {:?}", self.columns.len());
                    for column in self.columns.iter() {
                        column.report()
                    }
                }
            }

            #[inline(always)]
            fn borrow_row(row: &Row) -> Self::DecodeIter<'_> {
                ColumnsIter {
                    index: None,
                    column: 0,
                    data: row.data(),
                }
            }
        }

        #[derive(Debug, Copy, Clone)]
        pub struct ColumnsIter<'a> {
            // Optional only to support borrowing owned as this
            pub index: Option<&'a ColumnsCodec>,
            pub column: usize,
            pub data: &'a [u8],
        }

        impl<'a> Iterator for ColumnsIter<'a> {
            type Item = &'a [u8];
            fn next(&mut self) -> Option<Self::Item> {
                if self.data.is_empty() {
                    None
                } else if let Some(bytes) = self
                    .index
                    .as_ref()
                    .and_then(|i| i.columns.get(self.column))
                    .and_then(|i| i.decode.get(self.data[0].into()))
                {
                    self.data = &self.data[1..];
                    self.column += 1;
                    Some(bytes)
                } else {
                    let mut data = self.data;
                    let data_len = data.len();
                    unsafe {
                        read_datum(&mut data);
                    }
                    let (prev, next) = self.data.split_at(data_len - data.len());
                    self.data = next;
                    self.column += 1;
                    Some(prev)
                }
            }
        }

        impl<'a> ColumnsIter<'a> {
            /// Create a column iterator without a codec.
            ///
            /// This requires the data to be row-formatted, and it will be erroneous otherwise.
            #[inline(always)]
            pub unsafe fn without_codec(data: &'a [u8]) -> Self {
                Self {
                    index: None,
                    column: 0,
                    data,
                }
            }
        }
    }

    mod dictionary {

        use mz_repr::{Row, read_datum};
        use std::collections::BTreeMap;

        pub use super::{BytesMap, Codec, MisraGries};

        /// A type that can both encode and decode sequences of byte slices.
        #[derive(Default, Debug)]
        pub struct DictionaryCodec {
            encode: BTreeMap<Vec<u8>, u8>,
            pub decode: BytesMap,
            stats: (MisraGries<Vec<u8>>, [u64; 4]),
            bytes: usize,
            total: usize,
        }

        impl Codec for DictionaryCodec {
            type DecodeIter<'a> = DictionaryIter<'a>;

            /// Decode a sequence of byte slices.
            #[inline(always)]
            fn decode<'a>(&'a self, bytes: &'a [u8]) -> Self::DecodeIter<'a> {
                DictionaryIter {
                    index: Some(&self.decode),
                    data: bytes,
                }
            }

            /// Encode a sequence of byte slices.
            ///
            /// Encoding also records statistics about the structure of the input.
            fn encode<'a, I>(&mut self, iter: I, output: &mut Vec<u8>)
            where
                I: IntoIterator<Item = &'a [u8]>,
            {
                let pre_len = output.len();
                for bytes in iter.into_iter() {
                    self.total += bytes.len();
                    // If we have an index referencing `bytes`, use the index key.
                    if let Some(b) = self.encode.get(bytes) {
                        output.push(*b);
                    } else {
                        output.extend(bytes);
                    }
                    // Stats stuff.
                    self.stats.0.insert(bytes.to_owned());
                    let tag = bytes[0];
                    let tag_idx: usize = (tag % 4).into();
                    self.stats.1[tag_idx] |= 1 << (tag >> 2);
                }
                self.bytes += output.len() - pre_len;
            }

            /// Construct a new encoder from supplied statistics.
            fn new_from<'a>(stats: impl IntoIterator<Item = &'a Self>) -> Self {
                // Collect most popular bytes from combined containers.
                let mut mg = MisraGries::default();
                let mut tags: [u64; 4] = [0; 4];
                for stat in stats.into_iter() {
                    for (thing, count) in stat.stats.0.clone().done() {
                        mg.update(thing, count);
                    }
                    tags[0] |= stat.stats.1[0];
                    tags[1] |= stat.stats.1[1];
                    tags[2] |= stat.stats.1[2];
                    tags[3] |= stat.stats.1[3];
                }
                let mut mg = mg
                    .done()
                    .into_iter()
                    .filter(|(next_bytes, count)| next_bytes.len() > 1 && count > &1);
                // Establish encoding and decoding rules.
                let mut encode = BTreeMap::new();
                let mut decode = BytesMap::default();
                for tag in 0..=255 {
                    let tag_idx: usize = (tag % 4).into();
                    let shift = tag >> 2;
                    if (tags[tag_idx] >> shift) & 0x01 != 0 {
                        decode.push(None);
                    } else if let Some((next_bytes, _count)) = mg.next() {
                        decode.push(Some(&next_bytes[..]));
                        encode.insert(next_bytes, tag);
                    }
                }

                Self {
                    encode,
                    decode,
                    stats: (MisraGries::default(), [0u64; 4]),
                    bytes: 0,
                    total: 0,
                }
            }

            fn report(&self) {
                let mut tags_used = 0;
                tags_used += self.stats.1[0].count_ones();
                tags_used += self.stats.1[1].count_ones();
                tags_used += self.stats.1[2].count_ones();
                tags_used += self.stats.1[3].count_ones();
                let mg = self.stats.0.clone().done();
                let mut bytes = 0;
                for (vec, _count) in mg.iter() {
                    bytes += vec.len();
                }
                // if self.total > 10000 && !mg.is_empty() {
                println!(
                    "\t{:?}v{:?}: {:?} -> {:?} + {:?} = (x{:?})",
                    tags_used,
                    mg.len(),
                    self.total,
                    self.bytes,
                    bytes,
                    self.total / (self.bytes + bytes),
                )
                // }
            }

            #[inline(always)]
            fn borrow_row(row: &Row) -> Self::DecodeIter<'_> {
                DictionaryIter {
                    index: None,
                    data: row.data(),
                }
            }
        }

        #[derive(Debug, Copy, Clone)]
        pub struct DictionaryIter<'a> {
            // Optional only to support borrowing owned as this
            pub index: Option<&'a BytesMap>,
            pub data: &'a [u8],
        }

        impl<'a> Iterator for DictionaryIter<'a> {
            type Item = &'a [u8];
            fn next(&mut self) -> Option<Self::Item> {
                if self.data.is_empty() {
                    None
                } else if let Some(bytes) =
                    self.index.as_ref().and_then(|i| i.get(self.data[0].into()))
                {
                    self.data = &self.data[1..];
                    Some(bytes)
                } else {
                    let mut data = self.data;
                    let data_len = data.len();
                    unsafe {
                        read_datum(&mut data);
                    }
                    let (prev, next) = self.data.split_at(data_len - data.len());
                    self.data = next;
                    Some(prev)
                }
            }
        }
    }

    /// A map from `0 .. something` to `Option<&[u8]>`.
    ///
    /// Non-empty slices are pushed in order, and can be retrieved by index.
    /// Pushing an empty slice is equivalent to pushing `None`.
    #[derive(Debug)]
    pub struct BytesMap {
        offsets: Vec<usize>,
        bytes: Vec<u8>,
    }
    impl Default for BytesMap {
        #[inline(always)]
        fn default() -> Self {
            Self {
                offsets: vec![0],
                bytes: Vec::new(),
            }
        }
    }
    impl BytesMap {
        #[inline]
        fn push(&mut self, input: Option<&[u8]>) {
            if let Some(bytes) = input {
                self.bytes.extend(bytes);
            }
            self.offsets.push(self.bytes.len());
        }
        #[inline]
        fn get(&self, index: usize) -> Option<&[u8]> {
            if index < self.offsets.len() - 1 {
                let lower = self.offsets[index];
                let upper = self.offsets[index + 1];
                if lower < upper {
                    Some(&self.bytes[lower..upper])
                } else {
                    None
                }
            } else {
                None
            }
        }
        #[allow(dead_code)]
        fn len(&self) -> usize {
            self.offsets.len() - 1
        }
    }

    mod misra_gries {

        /// Maintains a summary of "heavy hitters" in a presented collection of items.
        #[derive(Clone, Debug)]
        pub struct MisraGries<T> {
            pub inner: Vec<(T, usize)>,
        }

        impl<T> Default for MisraGries<T> {
            #[inline(always)]
            fn default() -> Self {
                Self {
                    inner: Vec::with_capacity(1024),
                }
            }
        }

        impl<T: Ord> MisraGries<T> {
            /// Inserts an additional element to the summary.
            #[inline(always)]
            pub fn insert(&mut self, element: T) {
                self.update(element, 1);
            }
            /// Inserts multiple copies of an element to the summary.
            #[inline]
            pub fn update(&mut self, element: T, count: usize) {
                self.inner.push((element, count));
                if self.inner.len() == self.inner.capacity() {
                    self.tidy();
                }
            }
            // /// Allocates a Misra-Gries summary which intends to hold up to `k` examples.
            // ///
            // /// After `n` insertions it will contain only elements that were inserted at least `n/k` times.
            // /// The actual memory use is proportional to `2 * k`, so that we can amortize the consolidation.
            // pub fn with_capacity(k: usize) -> Self {
            //     Self {
            //         inner: Vec::with_capacity(2 * k),
            //     }
            // }

            /// Completes the summary, and extracts the items and their counts.
            pub fn done(mut self) -> Vec<(T, usize)> {
                use differential_dataflow::consolidation::consolidate;
                consolidate(&mut self.inner);
                self.inner.sort_by(|x, y| y.1.cmp(&x.1));
                self.inner
            }

            /// Internal method that reduces the summary down to at most `k-1` distinct items, by repeatedly
            /// removing sets of `k` distinct items. The removal is biased towards the lowest counts, so as
            /// to preserve fidelity around the larger counts, for whatever that is worth.
            fn tidy(&mut self) {
                use differential_dataflow::consolidation::consolidate;
                consolidate(&mut self.inner);
                self.inner.sort_by(|x, y| y.1.cmp(&x.1));
                let k = self.inner.capacity() / 2;
                if self.inner.len() > k {
                    let sub_weight = self.inner[k].1 - 1;
                    self.inner.truncate(k);
                    for (_, weight) in self.inner.iter_mut() {
                        *weight -= sub_weight;
                    }
                    while self.inner.last().map(|x| x.1) == Some(0) {
                        self.inner.pop();
                    }
                }
            }
        }

        impl<T: Ord> std::ops::AddAssign for MisraGries<T> {
            fn add_assign(&mut self, rhs: Self) {
                for (element, count) in rhs.done() {
                    self.update(element, count);
                }
            }
        }
    }
}
