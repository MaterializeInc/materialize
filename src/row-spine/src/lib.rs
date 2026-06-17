// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Types and traits in support of containers for row-encoded byte slices.
//!
//! This includes the vanilla `bytes_container` that holds byte slices in contiguous
//! allocations, as well as a `dictionary` encoding wrapper that is able to rewrite
//! the byte slices to use spare tags in each column to reference common values.

pub use self::dictionary::DatumContainer;
pub use self::dictionary::DatumSeq;
pub use self::offset_opt::OffsetOptimized;
pub use self::spines::{
    RowBatcher, RowBuilder, RowRowBatcher, RowRowBuilder, RowRowColPagedBuilder, RowRowSpine,
    RowSpine, RowValBatcher, RowValBuilder, RowValSpine, ValRowBatcher, ValRowBuilder,
    ValRowColPagedBuilder, ValRowSpine,
};
use differential_dataflow::trace::implementations::OffsetList;

// The entropy layer. Its API is exercised by tests today and wired into the
// containers in a follow-up; allow it to land unused in the meantime.
#[allow(dead_code)]
mod huffman;

/// Enable per-column dictionary compression in row containers.
pub static DICTIONARY_COMPRESSION: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// Spines specialized to contain `Row` types in keys and values.
mod spines {
    use std::rc::Rc;

    use columnation::Columnation;
    use differential_dataflow::trace::implementations::Layout;
    use differential_dataflow::trace::implementations::Update;
    use differential_dataflow::trace::implementations::merge_batcher::MergeBatcher;
    use differential_dataflow::trace::implementations::ord_neu::{
        OrdKeyBatch, OrdValBatch, OrdValBuilder,
    };
    use differential_dataflow::trace::implementations::spine_fueled::Spine;
    use differential_dataflow::trace::rc_blanket_impls::RcBuilder;
    use mz_repr::Row;
    use mz_timely_util::columnar::Column;
    use mz_timely_util::columnation::{ColInternalMerger, ColumnationStack};

    use crate::{DatumContainer, OffsetOptimized};

    /// Batcher matching `mz_compute::typedefs::KeyValBatcher`, redeclared
    /// locally so this crate does not need to depend on `mz_compute`.
    type KeyValBatcher<K, V, T, D> = MergeBatcher<ColInternalMerger<(K, V), T, D>>;
    type KeyBatcher<K, T, D> = KeyValBatcher<K, (), T, D>;

    pub type RowRowSpine<T, R> = Spine<Rc<OrdValBatch<RowRowLayout<((Row, Row), T, R)>>>>;
    pub type RowRowBatcher<T, R> = KeyValBatcher<Row, Row, T, R>;
    pub type RowRowBuilder<T, R> = RcBuilder<crate::dictionary::builders::RowRowBuilder<T, R>>;

    /// `RowRowBuilder` variant that consumes [`Column`] chunks. Pairs with
    /// [`Col2ValPagedBatcher`] for the spillable arrange path. This is the stock
    /// (non-dictionary) builder; dictionary-compressing the paged path is a follow-up.
    ///
    /// [`Col2ValPagedBatcher`]: mz_timely_util::columnar::Col2ValPagedBatcher
    pub type RowRowColPagedBuilder<T, R> =
        RcBuilder<OrdValBuilder<RowRowLayout<((Row, Row), T, R)>, Column<((Row, Row), T, R)>>>;

    pub type RowValSpine<V, T, R> = Spine<Rc<OrdValBatch<RowValLayout<((Row, V), T, R)>>>>;
    pub type RowValBatcher<V, T, R> = KeyValBatcher<Row, V, T, R>;
    pub type RowValBuilder<V, T, R> =
        RcBuilder<crate::dictionary::builders::RowValBuilder<V, T, R>>;

    pub type RowSpine<T, R> = Spine<Rc<OrdKeyBatch<RowLayout<((Row, ()), T, R)>>>>;
    pub type RowBatcher<T, R> = KeyBatcher<Row, T, R>;
    pub type RowBuilder<T, R> = RcBuilder<crate::dictionary::builders::RowBuilder<T, R>>;

    pub type ValRowSpine<K, T, R> = Spine<Rc<OrdValBatch<ValRowLayout<((K, Row), T, R)>>>>;
    pub type ValRowBatcher<K, T, R> = KeyValBatcher<K, Row, T, R>;
    pub type ValRowBuilder<K, T, R> =
        RcBuilder<crate::dictionary::builders::ValRowBuilder<K, T, R>>;

    /// `ValRowBuilder` variant that consumes [`Column`] chunks. Pairs with
    /// `Col2ValPagedBatcher<K, Row, T, R>` for the spillable arrange path where
    /// keys are arbitrary `Columnar` values (e.g. `UpsertKey`) and values are
    /// packed `Row` bytes.
    pub type ValRowColPagedBuilder<K, T, R> =
        RcBuilder<OrdValBuilder<ValRowLayout<((K, Row), T, R)>, Column<((K, Row), T, R)>>>;

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
    /// Mirror of [`RowValLayout`] with the roles swapped: arbitrary `Columnation`
    /// keys with `Row` values stored as packed bytes in a [`DatumContainer`].
    pub struct ValRowLayout<U: Update<Val = Row>> {
        phantom: std::marker::PhantomData<U>,
    }

    impl<U: Update<Key = Row, Val = Row>> Layout for RowRowLayout<U>
    where
        U::Time: Columnation,
        U::Diff: Columnation,
    {
        type KeyContainer = DatumContainer;
        type ValContainer = DatumContainer;
        type TimeContainer = ColumnationStack<U::Time>;
        type DiffContainer = ColumnationStack<U::Diff>;
        type OffsetContainer = OffsetOptimized;
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
    impl<U: Update<Val = Row>> Layout for ValRowLayout<U>
    where
        U::Key: Columnation,
        U::Time: Columnation,
        U::Diff: Columnation,
    {
        type KeyContainer = ColumnationStack<U::Key>;
        type ValContainer = DatumContainer;
        type TimeContainer = ColumnationStack<U::Time>;
        type DiffContainer = ColumnationStack<U::Diff>;
        type OffsetContainer = OffsetOptimized;
    }
}

#[cfg(test)]
mod tests {
    use crate::DatumContainer;
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

    /// Exercises the *compressed* encode→decode paths, which the dyncfg-gated
    /// `test_round_trip` never reaches (it installs no codec). We drive the codec
    /// directly: observe a sample, build a codec via both `new_from([c1, c2])`
    /// (the merge path) and `new_safe` (the safe-tag path), then round-trip every
    /// row through it. We additionally assert the dictionary actually engaged, so
    /// the test keeps covering the compressed branch rather than silently
    /// degrading to raw fall-through.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // integer-to-pointer casts in row decoding are unsupported under miri
    fn test_codec_round_trip() {
        use crate::row_codec::ColumnsCodec;

        // Rows with a small set of repeated, multi-byte string values, so the
        // dictionary installs entries (MisraGries keeps values with len > 1 and
        // count > 1). Mixing in an integer column exercises the raw fall-through
        // (and thus the new soundness `debug_assert`) alongside dictionary hits.
        let values = ["apple", "banana", "cherry"];
        let rows: Vec<Row> = (0..3_000)
            .map(|i| {
                Row::pack_slice(&[
                    Datum::String(values[i % values.len()]),
                    Datum::Int64(i64::try_from(i).unwrap()),
                    Datum::String(values[(i / 7) % values.len()]),
                ])
            })
            .collect();

        // Accumulate statistics in two independent observers, so the merge in
        // `new_from([&stats1, &stats2])` is actually exercised.
        let mut stats1 = ColumnsCodec::default();
        let mut stats2 = ColumnsCodec::default();
        let mut scratch = Vec::new();
        for (i, row) in rows.iter().enumerate() {
            scratch.clear();
            let stats = if i % 2 == 0 { &mut stats1 } else { &mut stats2 };
            stats.encode(ColumnsCodec::borrow_row(row), &mut scratch);
        }

        let merged = ColumnsCodec::new_from([&stats1, &stats2]);
        let safe = stats1.new_safe();
        for mut codec in [merged, safe] {
            let mut compressed_any = false;
            for row in &rows {
                let mut buf = Vec::new();
                codec.encode(ColumnsCodec::borrow_row(row), &mut buf);

                let decoded = codec.decode(&buf).collect::<Vec<_>>();
                let expected = ColumnsCodec::borrow_row(row).collect::<Vec<_>>();
                assert_eq!(decoded, expected, "round-trip mismatch for {row:?}");

                compressed_any |= buf.len() < row.data().len();
            }
            assert!(
                compressed_any,
                "dictionary never engaged; test no longer covers the compressed path",
            );
        }
    }

    /// Regression test for a dictionary-codec soundness bug in the safe-install
    /// path (`new_safe`), reachable with the paged batcher enabled.
    ///
    /// A from-scratch container stores its pre-install rows *raw* while gathering
    /// statistics, then installs a *safe* codec via `new_safe`. `new_safe` used to
    /// discard the first-byte bitmap gathered over those raw rows. That bitmap is
    /// soundness-critical: a later `new_from` merge consults it to decide which
    /// one-byte tags are free to hand out as dictionary keys. With the bitmap
    /// dropped, the merge could assign a dictionary tag equal to a raw datum's
    /// first byte, after which `decode` resolves that literal datum to the
    /// dictionary entry — returning the wrong value.
    ///
    /// We drive the lifecycle directly: observe short strings (first byte
    /// `StringTiny`) into the pre-install statistics, install a safe codec, then
    /// feed it many distinct *long* strings (first byte `StringShort`)
    /// post-install so the merge has heavy hitters to compress. Merging via
    /// `new_from` and re-encoding the short strings then exercises the raw
    /// fall-through whose first byte the merge must not have claimed as a tag.
    /// Before the fix the `StringTiny` tag was handed out and the round-trip
    /// produced a long string (and tripped `encode`'s soundness `debug_assert`).
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // integer-to-pointer casts in row decoding are unsupported under miri
    fn test_safe_codec_merge_bitmap_carryover() {
        use crate::row_codec::ColumnsCodec;

        // Short strings: length < 256, so they encode with the `StringTiny` tag.
        // Unique, so MisraGries never makes them dictionary entries; they always
        // fall through raw, exposing their first byte.
        let short_rows: Vec<Row> = (0..256)
            .map(|i| Row::pack_slice(&[Datum::String(&format!("s{i}"))]))
            .collect();
        // Long strings: length >= 256, so they encode with the `StringShort` tag —
        // a *different* first byte than the short strings. Distinct values, each
        // repeated, so the post-install codec accrues many heavy hitters and the
        // merge assigns dictionary tags across the low byte range, reaching the
        // short strings' `StringTiny` tag unless the bitmap reserves it.
        let long_values: Vec<String> = (0..64).map(|i| format!("{i:0>300}")).collect();

        // Pre-install statistics observe only the short strings' first bytes.
        let mut stats = ColumnsCodec::default();
        let mut scratch = Vec::new();
        for row in &short_rows {
            scratch.clear();
            stats.encode(ColumnsCodec::borrow_row(row), &mut scratch);
        }

        // Install a safe codec, then feed it the long strings post-install so it
        // accrues heavy hitters (and observes only the `StringShort` first byte).
        let mut safe = stats.new_safe();
        for _ in 0..8 {
            for v in &long_values {
                let row = Row::pack_slice(&[Datum::String(v)]);
                scratch.clear();
                safe.encode(ColumnsCodec::borrow_row(&row), &mut scratch);
            }
        }

        // Merge, then round-trip the short strings. With the bitmap carried over,
        // no dictionary tag collides with the short strings' first byte; without
        // it, one does.
        let mut merged = ColumnsCodec::new_from([&safe]);
        for row in &short_rows {
            let mut buf = Vec::new();
            merged.encode(ColumnsCodec::borrow_row(row), &mut buf);
            let decoded = merged.decode(&buf).collect::<Vec<_>>();
            let expected = ColumnsCodec::borrow_row(row).collect::<Vec<_>>();
            assert_eq!(decoded, expected, "round-trip mismatch for {row:?}");
        }
    }

    /// Confirms the structural assumption underpinning `SAFE_TAG_BASE`: every
    /// datum the row format produces encodes with a first byte strictly less
    /// than `SAFE_TAG_BASE`. If `mz_repr` ever introduces a tag that crosses
    /// the boundary, `DictionaryCodec::new_safe` would assign a dictionary tag
    /// that collides with a literal datum first-byte, breaking decoding.
    #[mz_ore::test]
    fn test_safe_tag_base() {
        use crate::row_codec::SAFE_TAG_BASE;
        let check = |datum: Datum| {
            let row = Row::pack_slice(&[datum]);
            let data = row.data();
            assert!(!data.is_empty(), "empty encoding for {datum:?}");
            assert!(
                data[0] < SAFE_TAG_BASE,
                "datum {datum:?} encodes with first byte {} >= SAFE_TAG_BASE ({}); \
                 a new row tag has crossed the safe boundary",
                data[0],
                SAFE_TAG_BASE,
            );
        };
        for ty in SqlScalarType::enumerate().iter() {
            for datum in ty.interesting_datums() {
                check(datum);
            }
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
        offsets: crate::OffsetOptimized,
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
            let mut offsets = crate::OffsetOptimized::with_capacity(item_cap + 1);
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
            crate::offset_list_size(&self.spilled, callback);
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
///
/// The approach is to establish for each column lists of common values, and to use "unoccupied"
/// tags in the row encoding (e.g. where we would indicate types) to replace these common values.
/// This substitution is opt-in, in that we don't need to do it, and in particular do not do it
/// while we are collecting preliminary information about common values, and then start to use it
/// once we believe we have enough information. Once we have started to use the substitutions we
/// cannot change the meaning of a reserved byte pattern, for the container we are populating.
///
/// Each from-scratch container observes `STATS_THRESHOLD` records before establishing a mapping
/// from spare tags to common values. Containers that are formed from merging other containers
/// use those input containers' common values to populate a codec and use it immediately.
///
/// The dictionary behavior is controlled by the `DICTIONARY_COMPRESSION` flag, which if disabled
/// prevents the construction of codecs, which when absent simply cause the wrapper to behave as
/// a no-op that fails to use any spare tags for common values. The flag is set once, when a
/// replica is created (from compute's `InstanceConfig::arrangement_dictionary_compression`, itself
/// captured from the `enable_arrangement_dictionary_compression_alpha` dyncfg at that moment), and is
/// not changed for the life of the process; flipping the dyncfg only affects replicas created
/// afterwards. Even with the flag fixed, a single replica can hold a mix of compressed and
/// uncompressed containers — e.g. containers that never observed enough records to install a
/// codec, or that were merged from uncompressed inputs.
mod dictionary {

    use differential_dataflow::trace::implementations::BatchContainer;

    use mz_repr::{Row, RowRef};

    use super::row_codec::{ColumnsCodec, ColumnsIter};

    /// Wrapper types that exist to support the creation of dictionary codecs.
    ///
    /// These types interpose at the seal() call, to traverse the data that is being sealed and
    /// then construct codecs that are used to encode the row-shaped keys and values. There are
    /// several variants, corresponding to the RowRow, RowVal, and Row-only spine types.
    pub mod builders {

        use columnation::Columnation;
        use differential_dataflow::difference::Semigroup;
        use differential_dataflow::lattice::Lattice;
        use differential_dataflow::trace::Builder;
        use differential_dataflow::trace::Description;
        use differential_dataflow::trace::implementations::ord_neu::{OrdKeyBatch, OrdKeyBuilder};
        use differential_dataflow::trace::implementations::ord_neu::{OrdValBatch, OrdValBuilder};
        use mz_timely_util::columnation::ColumnationStack as TimelyStack;
        use timely::progress::Timestamp;

        use mz_repr::Row;

        use super::super::row_codec::ColumnsCodec;
        use super::{DatumContainer, DatumSeq};
        use crate::DICTIONARY_COMPRESSION;
        use crate::spines::{RowLayout, RowRowLayout, RowValLayout, ValRowLayout};

        /// Gather encoding statistics across `rows` and produce a codec from them.
        ///
        /// Returns `None` when dictionary compression is disabled.
        fn build_codec<'a>(rows: impl IntoIterator<Item = &'a Row>) -> Option<ColumnsCodec> {
            if !DICTIONARY_COMPRESSION.load(std::sync::atomic::Ordering::Relaxed) {
                return None;
            }
            let mut stats = ColumnsCodec::default();
            let mut buf = Vec::default();
            for row in rows {
                if !row.is_empty() {
                    stats.encode(DatumSeq::borrow_as(row).bytes_iter(), &mut buf);
                    buf.clear();
                }
            }
            Some(ColumnsCodec::new_from([&stats]))
        }

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
                let key_codec = build_codec(
                    chain
                        .iter()
                        .flat_map(|link| link.iter().map(|((k, _), _, _)| k)),
                );
                let val_codec = build_codec(
                    chain
                        .iter()
                        .flat_map(|link| link.iter().map(|((_, v), _, _)| v)),
                );

                use differential_dataflow::trace::implementations::BuilderInput;

                let (keys, vals, upds) = <Self::Input as BuilderInput<
                    DatumContainer,
                    DatumContainer,
                >>::key_val_upd_counts(&chain[..]);
                let mut builder = Self::with_capacity(keys, vals, upds);
                // The seal path installs a codec directly, so the per-container stats
                // gatherer (which `with_capacity` may have allocated) is dead weight and
                // would contradict the `stats: None once codec installed` invariant.
                builder.inner.result.keys.codec = key_codec;
                builder.inner.result.keys.stats = None;
                builder.inner.result.vals.vals.codec = val_codec;
                builder.inner.result.vals.vals.stats = None;

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
                let key_codec = build_codec(
                    chain
                        .iter()
                        .flat_map(|link| link.iter().map(|((k, _), _, _)| k)),
                );

                use differential_dataflow::trace::implementations::BuilderInput;

                let (keys, vals, upds) = <Self::Input as BuilderInput<
                    DatumContainer,
                    TimelyStack<V>,
                >>::key_val_upd_counts(&chain[..]);
                let mut builder = Self::with_capacity(keys, vals, upds);
                // See `RowRowBuilder::seal`: drop the now-redundant stats gatherer.
                builder.inner.result.keys.codec = key_codec;
                builder.inner.result.keys.stats = None;

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
                let key_codec = build_codec(
                    chain
                        .iter()
                        .flat_map(|link| link.iter().map(|((k, _), _, _)| k)),
                );

                use differential_dataflow::trace::implementations::BuilderInput;

                let (keys, vals, upds) = <Self::Input as BuilderInput<
                    DatumContainer,
                    TimelyStack<()>,
                >>::key_val_upd_counts(&chain[..]);
                let mut builder = Self::with_capacity(keys, vals, upds);
                // See `RowRowBuilder::seal`: drop the now-redundant stats gatherer.
                builder.inner.result.keys.codec = key_codec;
                builder.inner.result.keys.stats = None;

                for mut chunk in chain.drain(..) {
                    builder.push(&mut chunk);
                }

                builder.done(description)
            }
        }

        /// Mirror of [`RowValBuilder`] with the roles swapped: arbitrary keys and
        /// `Row` *values*, so the dictionary codec is built for and installed on the
        /// value container.
        pub struct ValRowBuilder<
            K: Ord + Clone + Columnation + 'static,
            T: Lattice + Timestamp + Columnation,
            R: Ord + Semigroup + Columnation + 'static,
        > {
            inner: OrdValBuilder<ValRowLayout<((K, Row), T, R)>, TimelyStack<((K, Row), T, R)>>,
        }

        impl<
            K: Ord + Clone + Columnation,
            T: Lattice + Timestamp + Columnation,
            R: Ord + Semigroup + Columnation + 'static,
        > Builder for ValRowBuilder<K, T, R>
        {
            type Input = TimelyStack<((K, Row), T, R)>;
            type Time = T;
            type Output = OrdValBatch<ValRowLayout<((K, Row), T, R)>>;

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
                let val_codec = build_codec(
                    chain
                        .iter()
                        .flat_map(|link| link.iter().map(|((_, v), _, _)| v)),
                );

                use differential_dataflow::trace::implementations::BuilderInput;

                let (keys, vals, upds) = <Self::Input as BuilderInput<
                    TimelyStack<K>,
                    DatumContainer,
                >>::key_val_upd_counts(&chain[..]);
                let mut builder = Self::with_capacity(keys, vals, upds);
                // See `RowRowBuilder::seal`: drop the now-redundant stats gatherer.
                builder.inner.result.vals.vals.codec = val_codec;
                builder.inner.result.vals.vals.stats = None;

                for mut chunk in chain.drain(..) {
                    builder.push(&mut chunk);
                }

                builder.done(description)
            }
        }
    }

    pub struct DatumContainer {
        /// Encoder/decoder used to translate between row bytes and the stored bytes.
        /// `None` until enough pushes have been observed (or if compression is disabled).
        codec: Option<ColumnsCodec>,
        /// The stored, possibly-encoded, row bytes.
        inner: super::bytes_container::BytesContainer,
        /// Staging buffer for ingested `Row` types.
        staging: Vec<u8>,
        /// Statistics gatherer, used to build a safe codec after enough pushes.
        /// `None` once the codec has been installed or if compression is disabled.
        stats: Option<ColumnsCodec>,
    }

    impl BatchContainer for DatumContainer {
        type Owned = Row;
        type ReadItem<'a> = DatumSeq<'a>;

        fn with_capacity(size: usize) -> Self {
            let stats = if crate::DICTIONARY_COMPRESSION.load(std::sync::atomic::Ordering::Relaxed)
            {
                Some(Default::default())
            } else {
                None
            };

            Self {
                codec: None,
                inner: BatchContainer::with_capacity(size),
                staging: Vec::new(),
                stats,
            }
        }
        fn merge_capacity(cont1: &Self, cont2: &Self) -> Self {
            // We only build a merged codec when *both* inputs carry one. A codec is
            // sound only for the data whose tag usage it observed, so we cannot reuse
            // one side's codec to decode the other side's rows. When exactly one side
            // is compressed we conservatively produce an uncompressed container rather
            // than risk a tag collision; the merged container re-gathers stats and may
            // install a fresh codec later via the `STATS_THRESHOLD` path.
            let codec = match (&cont1.codec, &cont2.codec) {
                (Some(c1), Some(c2)) => Some(ColumnsCodec::new_from([c1, c2])),
                _ => None,
            };

            Self {
                codec,
                inner: BatchContainer::merge_capacity(&cont1.inner, &cont2.inner),
                staging: Vec::new(),
                stats: None,
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
            // Fast path: unencoded data is already row-formatted bytes.
            if item.iter.index.is_none() {
                // SAFETY: `iter.data` is raw row-encoded bytes when there is no codec.
                return unsafe { Row::from_bytes_unchecked(item.iter.data) };
            }
            Row::pack(item)
        }

        #[inline(always)]
        fn clone_onto<'a>(item: Self::ReadItem<'a>, other: &mut Self::Owned) {
            // Fast path: unencoded data is already row-formatted bytes.
            if item.iter.index.is_none() {
                let mut packer = other.packer();
                // SAFETY: `iter.data` is raw row-encoded bytes when there is no codec.
                unsafe { packer.extend_by_slice_unchecked(item.iter.data) };
                return;
            }
            other.packer().extend(item);
        }

        #[inline(always)]
        fn push_ref(&mut self, item: Self::ReadItem<'_>) {
            // Fast path: both sides unencoded — push raw bytes directly.
            if self.codec.is_none() && self.stats.is_none() && item.iter.index.is_none() {
                self.inner.push_ref(item.iter.data);
                return;
            }
            self.push_into(item);
        }

        #[inline(always)]
        fn push_own(&mut self, item: &Self::Owned) {
            // Fast path: container is unencoded — push raw row bytes directly.
            if self.codec.is_none() && self.stats.is_none() {
                self.inner.push_ref(item.data());
                return;
            }
            self.push_into(item);
        }

        #[inline(always)]
        fn clear(&mut self) {
            self.inner.clear();
            self.staging.clear();
            // Reset to the same state as a fresh `with_capacity`: drop any installed
            // codec and restore stats gathering (if compression is enabled). Keeping a
            // now-empty codec would leave `codec.is_some()`, which permanently routes
            // pushes down the encode path with an empty dictionary and prevents the
            // `STATS_THRESHOLD` install logic from ever re-engaging compression.
            self.codec = None;
            self.stats = if crate::DICTIONARY_COMPRESSION.load(std::sync::atomic::Ordering::Relaxed)
            {
                Some(Default::default())
            } else {
                None
            };
        }
    }

    impl DatumContainer {
        /// Visit contained allocations to determine their size and capacity.
        #[inline]
        pub fn heap_size(&self, mut callback: impl FnMut(usize, usize)) {
            self.inner.heap_size(&mut callback);
            // The staging buffer and the (possibly absent) codec and stats gatherer all
            // hold heap allocations that the bare `inner` accounting misses.
            callback(self.staging.len(), self.staging.capacity());
            if let Some(codec) = &self.codec {
                codec.heap_size(&mut callback);
            }
            if let Some(stats) = &self.stats {
                stats.heap_size(&mut callback);
            }
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

    impl PushInto<&RowRef> for DatumContainer {
        #[inline(always)]
        fn push_into(&mut self, item: &RowRef) {
            self.push_into(DatumSeq::borrow_as(item));
        }
    }

    /// Number of pushes a from-scratch container observes before it turns its
    /// gathered stats into a safe codec.
    ///
    /// A safe codec has at most `256 - SAFE_TAG_BASE` (= 134) dictionary slots per
    /// column, so we only need to identify ~134 genuinely-popular values. The
    /// `MisraGries` summary retains up to `2 * k` (= 1024) distinct candidates
    /// between tidies and reduces to `k` (= 512), comfortably more than 134, so the
    /// threshold just needs to be large enough that heavy hitters accumulate counts
    /// well above 1 before we freeze the codec. 64Ki pushes gives that headroom while
    /// keeping the pre-codec (uncompressed) window short.
    const STATS_THRESHOLD: usize = 64 * 1024;

    impl PushInto<DatumSeq<'_>> for DatumContainer {
        #[inline]
        fn push_into(&mut self, item: DatumSeq<'_>) {
            // Fast path: container and item are both unencoded.
            // This is the hot path when dictionary compression is disabled.
            if self.codec.is_none() && self.stats.is_none() && item.iter.index.is_none() {
                self.inner.push_ref(item.iter.data);
                return;
            }

            // Check if we've gathered enough stats to install a safe codec.
            if self.codec.is_none() && self.stats.is_some() && self.inner.len() >= STATS_THRESHOLD {
                let stats = self.stats.take().unwrap();
                self.codec = Some(stats.new_safe());
            }

            if let Some(codec) = &mut self.codec {
                // Encode using the installed codec.
                codec.encode(item.bytes_iter(), &mut self.staging);
            } else if let Some(stats) = &mut self.stats {
                // Stats-gathering phase: feed the statistics but store raw bytes.
                // `observe` updates the heavy-hitter/tag summaries without encoding, so
                // we copy each row exactly once (below) instead of also encoding it into
                // a buffer we would immediately discard.
                stats.observe(item.bytes_iter());
                for slice in item.bytes_iter() {
                    self.staging.extend_from_slice(slice);
                }
            } else {
                // No codec, no stats: raw copy.
                for slice in item.bytes_iter() {
                    self.staging.extend_from_slice(slice);
                }
            }
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
        fn borrow_as(other: &'a RowRef) -> Self {
            Self {
                iter: ColumnsCodec::borrow_row(other),
            }
        }

        /// Borrow a `Row` as a `DatumSeq` so that it can be used to seek into a
        /// trace whose key/value container is a [`DatumContainer`].
        #[inline]
        pub fn from_row(row: &'a Row) -> Self {
            Self::borrow_as(row)
        }

        #[inline]
        pub fn to_row(&self) -> Row {
            // Fast path: unencoded data is already row-formatted bytes.
            if self.iter.index.is_none() {
                return unsafe { Row::from_bytes_unchecked(self.iter.data) };
            }
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
            // Fast path: both sides are unencoded raw row bytes.
            if self.iter.index.is_none() && other.iter.index.is_none() {
                return self.iter.data == other.iter.data;
            }
            Iterator::eq(self.iter, other.iter)
        }
    }
    impl<'a> Eq for DatumSeq<'a> {}
    impl<'a, 'b> PartialOrd<DatumSeq<'a>> for DatumSeq<'b> {
        #[inline(always)]
        fn partial_cmp(&self, other: &DatumSeq<'a>) -> Option<Ordering> {
            // Fast path: both sides are unencoded raw row bytes.
            if self.iter.index.is_none() && other.iter.index.is_none() {
                let left = self.iter.data;
                let right = other.iter.data;
                return Some(match left.len().cmp(&right.len()) {
                    Ordering::Equal => left.cmp(right),
                    other => other,
                });
            }
            // Slow path: at least one side is dictionary-encoded.
            // Fused length + lexicographic comparison in a single pass per side.
            // Row ordering is: shorter < longer; equal lengths compared lexicographically.
            //
            // We compare byte-by-byte (via `flatten`) rather than slice-by-slice on
            // purpose: a dictionary tag expands to a multi-byte value on one side while
            // the other side may store those same bytes raw, so the per-column slice
            // boundaries do not line up between the two iterators. Decoding to a flat
            // byte stream is the only representation in which both sides are directly
            // comparable. This path is cold — it only runs when at least one operand is
            // dictionary-encoded; the common unencoded case is handled by the fast path
            // above with a single slice comparison.
            let mut left = self.iter.flatten();
            let mut right = other.iter.flatten();
            let mut first_diff = Ordering::Equal;
            loop {
                match (left.next(), right.next()) {
                    (Some(l), Some(r)) => {
                        if first_diff == Ordering::Equal {
                            first_diff = l.cmp(r);
                        }
                    }
                    // Left exhausted first: left is shorter, so Less.
                    (None, Some(_)) => return Some(Ordering::Less),
                    // Right exhausted first: right is shorter, so Greater.
                    (Some(_), None) => return Some(Ordering::Greater),
                    // Same length: use first lexicographic difference.
                    (None, None) => return Some(first_diff),
                }
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

    // Lifetimes decoupled (`'b` independent of `'a`): the arrange machinery
    // requires `for<'b> DatumSeq<'a>: PartialEq<&'b RowRef>`, i.e. a fixed
    // `DatumSeq` must compare against a `&RowRef` of any lifetime.
    impl<'a, 'b> PartialEq<&'b RowRef> for DatumSeq<'a> {
        #[inline(always)]
        fn eq(&self, other: &&'b RowRef) -> bool {
            self.eq(&DatumSeq::borrow_as(*other))
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
            // Delegate to `ColumnsIter`, which handles both the codec and no-codec
            // cases. The no-codec scan hot path is served directly by `extend_datums`
            // (which decodes without going through this iterator), so the only callers
            // left here are the codec-encoded `extend_datums`/`to_row` paths and tests;
            // none warrant a dedicated no-codec fast path.
            self.iter
                .next()
                .map(|mut bytes| unsafe { read_datum(&mut bytes) })
        }
    }

    use mz_repr::RowArena;
    use mz_repr::fixed_length::ExtendDatums;
    impl<'long> ExtendDatums for DatumSeq<'long> {
        #[inline]
        fn extend_datums<'a>(
            &'a self,
            _arena: &'a RowArena,
            target: &mut Vec<Datum<'a>>,
            max: Option<usize>,
        ) {
            // Branch on codec presence ONCE per row rather than once per datum.
            // With no codec (the common, feature-off case) push raw datums in a
            // tight loop, matching the pre-dictionary path; with a codec, fall
            // back to the per-column iterator. This keeps the codec check out of
            // the per-datum loop — the source of the feature-off scan overhead.
            if self.iter.index.is_none() {
                let mut data = self.iter.data;
                match max {
                    Some(max) => {
                        let mut n = 0;
                        while n < max && !data.is_empty() {
                            target.push(unsafe { read_datum(&mut data) });
                            n += 1;
                        }
                    }
                    None => {
                        while !data.is_empty() {
                            target.push(unsafe { read_datum(&mut data) });
                        }
                    }
                }
            } else {
                match max {
                    Some(max) => target.extend((*self).take(max)),
                    None => target.extend(*self),
                }
            }
        }
    }
}

/// Traits abstracting the processes of encoding and decoding row-encoded byte sequences.
///
/// It is unsafe to use these types to encode byte sequences that are not row-encoded,
/// as they are parsed out of contiguous `[u8]` slices using `mz_repr::read_datum`.
mod row_codec {

    pub use self::misra_gries::MisraGries;
    pub use columns::{ColumnsCodec, ColumnsIter};
    pub use dictionary::DictionaryCodec;
    #[cfg(test)]
    pub use dictionary::SAFE_TAG_BASE;

    // The codecs encode and decode `[u8]` data specific to the `[Row]` encoding. They
    // soundly decode data they themselves encoded from valid `[Row]` data, but may be
    // unsound if asked to decode data that was not row-encoded, or was encoded with a
    // different codec. `ColumnsCodec` (a per-column wrapper around `DictionaryCodec`) is
    // the only codec the spine instantiates; the methods are inherent rather than behind
    // a `Codec` trait because nothing ever dispatches over codecs generically.

    mod columns {

        use mz_repr::{RowRef, read_datum};

        use super::DictionaryCodec;

        /// Independently encodes each column.
        #[derive(Default, Debug)]
        pub struct ColumnsCodec {
            columns: Vec<DictionaryCodec>,
        }

        impl ColumnsCodec {
            /// Decode a row-encoded byte slice into per-column byte slices.
            pub(crate) fn decode<'a>(&'a self, bytes: &'a [u8]) -> ColumnsIter<'a> {
                ColumnsIter {
                    index: Some(self),
                    column: 0,
                    data: bytes,
                }
            }
            /// Encode a sequence of column byte slices, updating per-column statistics.
            pub(crate) fn encode<'a, I>(&mut self, iter: I, output: &mut Vec<u8>)
            where
                I: IntoIterator<Item = &'a [u8]>,
            {
                for (index, bytes) in iter.into_iter().enumerate() {
                    if self.columns.len() <= index {
                        self.columns.push(Default::default());
                    }
                    self.columns[index].encode(std::iter::once(bytes), output);
                }
            }

            /// Construct a codec valid for the union of the supplied codecs' data.
            pub(crate) fn new_from<'a>(stats: impl IntoIterator<Item = &'a Self>) -> Self {
                // An empty `stats` iterator yields a zero-column codec, which encodes and
                // decodes nothing; callers merging no inputs get an inert (but sound) codec.
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
                Self { columns }
            }

            /// Reveal a row's bytes for fast-path comparison, with no codec to consult.
            #[inline(always)]
            pub(crate) fn borrow_row(row: &RowRef) -> ColumnsIter<'_> {
                ColumnsIter {
                    index: None,
                    column: 0,
                    data: row.data(),
                }
            }
        }

        impl ColumnsCodec {
            /// Visit contained allocations to determine their size and capacity.
            pub(crate) fn heap_size(&self, callback: &mut impl FnMut(usize, usize)) {
                let elem = std::mem::size_of::<DictionaryCodec>();
                callback(self.columns.len() * elem, self.columns.capacity() * elem);
                for column in &self.columns {
                    column.heap_size(callback);
                }
            }
        }

        impl ColumnsCodec {
            /// Record a row's column values in the statistics without encoding.
            ///
            /// Used during the stats-gathering phase, where we want the heavy-hitter
            /// and tag-usage information but store the row raw, so encoding into a
            /// throwaway buffer would be pure waste.
            #[inline]
            pub(crate) fn observe<'a, I>(&mut self, iter: I)
            where
                I: IntoIterator<Item = &'a [u8]>,
            {
                for (index, bytes) in iter.into_iter().enumerate() {
                    if self.columns.len() <= index {
                        self.columns.push(Default::default());
                    }
                    self.columns[index].observe(bytes);
                }
            }
        }

        impl ColumnsCodec {
            /// Construct a codec using only structurally safe tags.
            ///
            /// Consumes `self`: this is only ever called on stats that have just been
            /// `take`n out of a container and are about to be discarded, so we move the
            /// per-column `MisraGries` summaries through rather than cloning them.
            pub(crate) fn new_safe(self) -> Self {
                let columns = self
                    .columns
                    .into_iter()
                    .map(DictionaryCodec::new_safe)
                    .collect();
                Self { columns }
            }
        }

        #[derive(Debug, Copy, Clone)]
        pub struct ColumnsIter<'a> {
            // `None` when iterating an owned row directly, with no codec to consult.
            pub index: Option<&'a ColumnsCodec>,
            pub column: usize,
            pub data: &'a [u8],
        }

        impl<'a> Iterator for ColumnsIter<'a> {
            type Item = &'a [u8];
            #[inline(always)]
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

    /// A dictionary encoding codec for `[Row]` data.
    ///
    /// The dictionary harvests unused tags within each column and uses them to
    /// represent popular values within that column. There are two mechanisms it
    /// uses to accomplish this:
    ///
    /// 1. Statically free tags: `SAFE_TAG_BASE` is taken as an exclusive upper bound
    ///    on the tags that will be used by `[Row]`, and tags greater or equal to this
    ///    value are always safe to use.
    /// 2. Dynamically free tags: having seen an entire collection, we can use any
    ///    tag not otherwise used by the collection, as it would not be ambiguous.
    ///
    /// It goes without saying that if either of these approaches are incorrect,
    /// there are calamitous unsoundness implications.
    mod dictionary {

        use std::collections::BTreeMap;

        pub use super::{BytesMap, MisraGries};

        /// First byte value that is structurally unused by the datum encoding.
        /// All byte values >= this are safe to use as dictionary tags without
        /// observing the data, since no datum's first byte can have this value.
        ///
        /// `mz_repr`'s `Row` `Tag` enum currently has 94 variants (discriminants
        /// 0..=93), so the truly tight bound is 94. We deliberately pick a larger,
        /// round-ish constant to leave headroom for new tags without having to also
        /// bump the safe set, and the `test_safe_tag_base` test pins the real
        /// invariant: every datum the row format produces must encode with a first
        /// byte strictly less than this value. If a future tag crosses the boundary
        /// that test fails loudly rather than silently corrupting decoding.
        pub const SAFE_TAG_BASE: u8 = 122;

        /// Per-column dictionary codec. Encodes column byte slices, replacing popular
        /// values with spare tags; decoding is performed by `ColumnsIter` reading the
        /// `decode` map directly.
        #[derive(Default, Debug)]
        pub struct DictionaryCodec {
            encode: BTreeMap<Vec<u8>, u8>,
            pub decode: BytesMap,
            stats: (MisraGries<Vec<u8>>, [u64; 4]),
        }

        impl DictionaryCodec {
            /// Encode a sequence of byte slices.
            ///
            /// Encoding also records statistics about the structure of the input.
            ///
            /// Decoding has no symmetric method here: a column's bytes are decoded by
            /// `ColumnsIter`, which consults the `decode` map directly.
            pub(super) fn encode<'a, I>(&mut self, iter: I, output: &mut Vec<u8>)
            where
                I: IntoIterator<Item = &'a [u8]>,
            {
                for bytes in iter.into_iter() {
                    debug_assert!(
                        !bytes.is_empty(),
                        "row encoding never yields empty column slices",
                    );
                    // If we have an index referencing `bytes`, use the index key.
                    if let Some(b) = self.encode.get(bytes) {
                        output.push(*b);
                    } else {
                        // Raw fall-through. Soundness rests on `bytes[0]` never being a
                        // tag we hand out as a dictionary key: `new_from`/`new_safe` only
                        // assign dictionary tags from first-byte values that were never
                        // observed (or are `>= SAFE_TAG_BASE`, which no datum first-byte
                        // can equal). If a literal datum's first byte collided with a
                        // dictionary tag, `decode` would resolve it to the dictionary
                        // entry instead of reading the datum. This `debug_assert` makes
                        // the load-bearing "no later first-byte outside the observed
                        // union" invariant self-checking.
                        debug_assert!(
                            self.decode.get(bytes[0].into()).is_none(),
                            "raw datum first-byte {} collides with a dictionary tag; \
                             decode would be ambiguous",
                            bytes[0],
                        );
                        output.extend(bytes);
                    }
                    self.observe(bytes);
                }
            }

            /// Construct a new encoder from supplied statistics.
            pub(super) fn new_from<'a>(stats: impl IntoIterator<Item = &'a Self>) -> Self {
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
                        // Tag is used by a literal datum first-byte; reserve the slot.
                        decode.push(None);
                    } else if let Some((next_bytes, _count)) = mg.next() {
                        decode.push(Some(&next_bytes[..]));
                        encode.insert(next_bytes, tag);
                    } else {
                        // Unused tag, but the heavy-hitter supply is exhausted. We must
                        // still push a slot so that `decode`'s index stays aligned with
                        // the tag value: every iteration pushes exactly once, keeping the
                        // map length 256 and `decode.get(tag)` addressable by tag.
                        decode.push(None);
                    }
                }

                Self {
                    encode,
                    decode,
                    stats: (MisraGries::default(), [0u64; 4]),
                }
            }
        }

        impl DictionaryCodec {
            /// Visit contained allocations to determine their size and capacity.
            ///
            /// `BTreeMap` exposes no capacity, so its node storage is approximated as
            /// one logical entry's worth of bytes per element; the dominant terms (the
            /// owned key bytes and the `decode` map's byte arena) are accounted exactly.
            pub fn heap_size(&self, callback: &mut impl FnMut(usize, usize)) {
                let entry = std::mem::size_of::<(Vec<u8>, u8)>();
                callback(self.encode.len() * entry, self.encode.len() * entry);
                for key in self.encode.keys() {
                    callback(key.len(), key.capacity());
                }
                self.decode.heap_size(callback);
                self.stats.0.heap_size(callback);
            }

            /// Record a single column value in this codec's statistics without
            /// producing any encoded output.
            ///
            /// Statistics come in two decoupled parts, with very different costs and
            /// purposes:
            ///
            /// 1. The tag bitmap (`stats.1`) records which first-byte values have been
            ///    observed. It is cheap (four `u64` ORs) and *soundness critical*:
            ///    `new_from`'s dynamic-tag path only hands out tags that this bitmap
            ///    reports as unused, so it must stay accurate for the entire life of the
            ///    codec, including on the hot encode path.
            /// 2. The MisraGries summary (`stats.0`) tracks heavy hitters and only
            ///    affects *which* values a future codec compresses, never correctness.
            ///    It is the expensive part (a `BTreeMap` insert per column per row). We
            ///    keep feeding it after install, on the hot encode path, on purpose: a
            ///    later merge rebuilds the merged codec from these summaries via
            ///    `new_from`. If we froze the summary at install time, then as the
            ///    collection evolves — records cancel under consolidation, the popular
            ///    set drifts — the codec could never reclaim slots for newly-popular
            ///    values and would eventually be left compressing values that no longer
            ///    occur, ceasing to compress the ones that do.
            #[inline]
            pub fn observe(&mut self, bytes: &[u8]) {
                debug_assert!(
                    !bytes.is_empty(),
                    "row encoding never yields empty column slices",
                );
                let tag = bytes[0];
                let tag_idx: usize = (tag % 4).into();
                self.stats.1[tag_idx] |= 1 << (tag >> 2);
                self.stats.0.insert_ref(bytes);
            }

            /// Construct a codec using only structurally safe tags (>= SAFE_TAG_BASE).
            /// These tags never collide with datum first-bytes, so the codec can be
            /// installed without observing all data first.
            pub(super) fn new_safe(stats: Self) -> Self {
                // The container stores its pre-install rows raw, so the first-byte
                // bitmap (`stats.1`) gathered while observing them must carry over to
                // the installed codec. The bitmap is soundness-critical: a later
                // `new_from` merge consults it to decide which one-byte tags are free
                // to hand out as dictionary keys. If we dropped it here, the merge
                // could assign a dictionary tag equal to a pre-install datum's first
                // byte, after which `decode` would resolve that literal datum to the
                // dictionary entry. The MisraGries summary (`stats.0`), by contrast,
                // is consumed below to seed the dictionary and is reset, since the
                // installed codec re-accumulates it from rows it sees post-install.
                let (mg, observed_tags) = stats.stats;
                let mut mg = mg
                    .done()
                    .into_iter()
                    .filter(|(next_bytes, count)| next_bytes.len() > 1 && count > &1);
                let mut encode = BTreeMap::new();
                let mut decode = BytesMap::default();
                // Fill slots 0..SAFE_TAG_BASE with None (reserved for datum tags).
                for _ in 0..SAFE_TAG_BASE {
                    decode.push(None);
                }
                // Assign dictionary entries to safe tags.
                for tag in SAFE_TAG_BASE..=255 {
                    if let Some((next_bytes, _count)) = mg.next() {
                        decode.push(Some(&next_bytes[..]));
                        encode.insert(next_bytes, tag);
                    }
                }
                Self {
                    encode,
                    decode,
                    stats: (MisraGries::default(), observed_tags),
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
        /// Visit contained allocations to determine their size and capacity.
        fn heap_size(&self, callback: &mut impl FnMut(usize, usize)) {
            let off = std::mem::size_of::<usize>();
            callback(self.offsets.len() * off, self.offsets.capacity() * off);
            callback(self.bytes.len(), self.bytes.capacity());
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
    }

    mod misra_gries {

        use std::collections::BTreeMap;

        /// Maintains a summary of "heavy hitters" in a presented collection of items.
        ///
        /// Uses a `BTreeMap` internally so that repeated observations of the same
        /// element only allocate once (on first sighting). Tidy is performed when
        /// the number of *distinct* elements exceeds `2 * k`, reducing to at most
        /// `k` entries.
        #[derive(Clone, Debug)]
        pub struct MisraGries<T: Ord> {
            inner: BTreeMap<T, usize>,
            k: usize,
        }

        impl<T: Ord> Default for MisraGries<T> {
            #[inline(always)]
            fn default() -> Self {
                Self {
                    inner: BTreeMap::new(),
                    k: 512,
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
                *self.inner.entry(element).or_insert(0) += count;
                if self.inner.len() > 2 * self.k {
                    self.tidy();
                }
            }

            /// Completes the summary, and extracts the items and their counts.
            pub fn done(self) -> Vec<(T, usize)> {
                let mut result: Vec<_> = self.inner.into_iter().collect();
                result.sort_by(|x, y| y.1.cmp(&x.1));
                result
            }

            /// Reduces the summary down to at most `k` distinct items by
            /// subtracting the (k+1)-th largest count from all entries and
            /// discarding those that drop to zero or below.
            fn tidy(&mut self) {
                let mut counts: Vec<usize> = self.inner.values().copied().collect();
                counts.sort_unstable_by(|a, b| b.cmp(a));
                // The (k+1)-th largest count, or 0 if fewer than k+1 entries.
                let sub_weight = counts.get(self.k).copied().unwrap_or(0);
                if sub_weight > 0 {
                    self.inner.retain(|_, count| {
                        *count = count.saturating_sub(sub_weight);
                        *count > 0
                    });
                }
            }
        }

        impl MisraGries<Vec<u8>> {
            /// Visit contained allocations to determine their size and capacity.
            ///
            /// `BTreeMap` exposes no capacity, so node storage is approximated as one
            /// logical entry per element; the owned key bytes are accounted exactly.
            pub fn heap_size(&self, callback: &mut impl FnMut(usize, usize)) {
                let entry = std::mem::size_of::<(Vec<u8>, usize)>();
                callback(self.inner.len() * entry, self.inner.len() * entry);
                for key in self.inner.keys() {
                    callback(key.len(), key.capacity());
                }
            }

            /// Insert a borrowed byte slice, only allocating if the key is new.
            #[inline]
            pub fn insert_ref(&mut self, element: &[u8]) {
                if let Some(count) = self.inner.get_mut(element) {
                    *count += 1;
                } else {
                    self.insert(element.to_owned());
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
