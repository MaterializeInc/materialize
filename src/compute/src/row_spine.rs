pub use self::codec::{BytesIter, Codec, ColumnsCodec, ColumnsIter, DictionaryCodec};
pub use self::container::DatumContainer;
pub use self::datum_seq::DatumSeq;
pub use self::spines::{RowRowSpine, RowSpine, RowValSpine};

// pub type CodecDuJour = DictionaryCodec;
// pub type IterDuJour<'a> = BytesIter<'a>;

pub type CodecDuJour = ColumnsCodec;
pub type IterDuJour<'a> = ColumnsIter<'a>;

/// Spines specialized to contain `Row` types in keys and values.
mod spines {

    use differential_dataflow::trace::implementations::merge_batcher_col::ColumnatedMergeBatcher;
    use differential_dataflow::trace::implementations::ord_neu::{OrdKeyBatch, OrdKeyBuilder};
    use differential_dataflow::trace::implementations::ord_neu::{OrdValBatch, OrdValBuilder};
    use differential_dataflow::trace::implementations::spine_fueled::Spine;
    use differential_dataflow::trace::implementations::Layout;
    use differential_dataflow::trace::implementations::Update;
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
    }
}

/// A `Row`-specialized container using dictionary compression.
mod container {

    use differential_dataflow::trace::implementations::BatchContainer;
    use timely::container::columnation::TimelyStack;

    use mz_repr::Row;

    use super::{Codec, CodecDuJour, DatumSeq};

    #[derive(Default)]
    pub struct DatumContainer {
        /// DictionaryCodec with encoder, decoder, and stastistics.
        codec: CodecDuJour,
        /// A list of rows
        inner: TimelyStack<Vec<u8>>,
        /// Staging buffer for ingested `Row` types.
        staging: Vec<u8>,
    }

    impl BatchContainer for DatumContainer {
        type PushItem = Row;
        type ReadItem<'a> = DatumSeq<'a>;

        fn push(&mut self, item: Row) {
            self.copy_push(&item);
        }
        fn copy_push(&mut self, item: &Row) {
            use differential_dataflow::trace::cursor::MyTrait;
            self.copy(MyTrait::borrow_as(item));
        }
        fn copy<'a>(&mut self, item: DatumSeq<'a>) {
            self.staging.clear();
            self.codec.encode(item.bytes_iter(), &mut self.staging);
            self.inner.copy(&self.staging);
        }
        fn copy_slice(&mut self, slice: &[Row]) {
            for item in slice.iter() {
                self.copy_push(item);
            }
        }
        fn copy_range(&mut self, other: &Self, start: usize, end: usize) {
            for index in start..end {
                self.copy(other.index(index));
            }
        }
        fn with_capacity(size: usize) -> Self {
            Self {
                codec: Default::default(),
                inner: BatchContainer::with_capacity(size),
                staging: Vec::new(),
            }
        }
        fn reserve(&mut self, additional: usize) {
            self.inner.reserve(additional);
        }
        fn merge_capacity(cont1: &Self, cont2: &Self) -> Self {
            cont1.codec.report();
            cont2.codec.report();

            Self {
                codec: CodecDuJour::new_from([&cont1.codec, &cont2.codec]),
                inner: BatchContainer::merge_capacity(&cont1.inner, &cont2.inner),
                staging: Vec::new(),
            }
        }
        fn index(&self, index: usize) -> Self::ReadItem<'_> {
            DatumSeq {
                iter: self.codec.decode(self.inner.index(index)),
            }
        }
        fn len(&self) -> usize {
            self.inner.len()
        }
    }
}

/// A wrapper presenting as a sequence of `Datum` but backed by encoded state.
mod datum_seq {

    use super::{Codec, CodecDuJour, IterDuJour};
    use differential_dataflow::trace::cursor::MyTrait;
    use mz_repr::{read_datum, Datum, Row};

    /// A reference that can be resolved to a sequence of `Datum`s.
    ///
    /// This type must "compare" as if decoded to a `Row`, which means it needs to track
    /// various nuances of `Row::cmp`, which at the moment is first by length, and then by
    /// the raw binary slice backing the row. Neither of those are explicit in this struct.
    /// We will need to produce them in order to perform comparisons.
    #[derive(Debug)]
    pub struct DatumSeq<'a> {
        pub iter: IterDuJour<'a>,
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
            Iterator::eq(self.iter, other.iter)
        }
    }
    impl<'a> Eq for DatumSeq<'a> {}
    impl<'a, 'b> PartialOrd<DatumSeq<'a>> for DatumSeq<'b> {
        fn partial_cmp(&self, other: &DatumSeq<'a>) -> Option<Ordering> {
            let len1: usize = self.iter.map(|b| b.len()).sum();
            let len2: usize = other.iter.map(|b| b.len()).sum();
            if len1 == len2 {
                for (b1, b2) in self.iter.zip(other.iter) {
                    let cmp = b1.cmp(b2);
                    if cmp != Ordering::Equal {
                        return Some(cmp);
                    }
                }
                Some(Ordering::Equal)
            } else {
                Some(len1.cmp(&len2))
            }
        }
    }
    impl<'a> Ord for DatumSeq<'a> {
        fn cmp(&self, other: &Self) -> Ordering {
            self.partial_cmp(other).unwrap()
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
        fn compare(&self, other: &Self::Owned) -> Ordering {
            let mut other_bytes = other.data();
            let len1: usize = self.iter.map(|b| b.len()).sum();
            let len2: usize = other_bytes.len();
            if len1 == len2 {
                for bytes in self.iter {
                    if other_bytes.len() >= bytes.len() && bytes.eq(&other_bytes[..bytes.len()]) {
                        other_bytes = &other_bytes[bytes.len()..];
                    } else {
                        return bytes.cmp(other_bytes);
                    }
                }
                if other_bytes.is_empty() {
                    Ordering::Equal
                } else {
                    Ordering::Less // Shouldn't happen with equal lengths
                }
            } else {
                len1.cmp(&len2)
            }
        }
        fn borrow_as(other: &'a Self::Owned) -> Self {
            Self {
                iter: CodecDuJour::borrow_row(other),
            }
        }
    }

    impl<'a> DatumSeq<'a> {
        pub fn bytes_iter(self) -> IterDuJour<'a> {
            self.iter
        }
    }

    impl<'a> Iterator for DatumSeq<'a> {
        type Item = Datum<'a>;
        fn next(&mut self) -> Option<Self::Item> {
            self.iter
                .next()
                .map(|bytes| unsafe { read_datum(bytes, &mut 0) })
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

/// Traits abstracting the processes of encoding and decoding byte sequences.
mod codec {

    use mz_repr::{read_datum, Row};

    pub use self::misra_gries::MisraGries;
    pub use dictionary::{BytesIter, DictionaryCodec};

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
        fn new_from(stats: [&Self; 2]) -> Self;
        /// Diagnostic information about the state of the codec.
        fn report(&self) {}

        fn borrow_row<'a>(row: &'a Row) -> Self::DecodeIter<'a>;

        fn shorten<'a, 'b>(iter: Self::DecodeIter<'a>) -> Self::DecodeIter<'b>
        where
            'a: 'b;
    }

    /// Independently encodes each column.
    #[derive(Default, Debug)]
    pub struct ColumnsCodec {
        columns: Vec<DictionaryCodec>,
        bytes: usize,
        total: usize,
    }

    impl Codec for ColumnsCodec {
        type DecodeIter<'a> = ColumnsIter<'a>;
        fn decode<'a>(&'a self, bytes: &'a [u8]) -> Self::DecodeIter<'a> {
            ColumnsIter {
                index: Some(self),
                column: 0,
                data: bytes,
                offset: 0,
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

        fn new_from(stats: [&Self; 2]) -> Self {
            // Is it possible that one of the inputs has no stats?
            let cols = std::cmp::max(stats[0].columns.len(), stats[1].columns.len());
            let mut columns = Vec::with_capacity(cols);
            let default: DictionaryCodec = Default::default();
            for index in 0..cols {
                columns.push(DictionaryCodec::new_from([
                    stats[0].columns.get(index).unwrap_or(&default),
                    stats[1].columns.get(index).unwrap_or(&default),
                ]));
            }
            Self {
                columns,
                total: 0,
                bytes: 0,
            }
        }
        // fn report(&self) {
        //     if self.total > 100000 && self.columns.iter().all(|c| c.decode.len() > 0) {
        //         println!("REPORT: {:?} -> {:?} (x{:?})", self.total, self.bytes, self.total/self.bytes);
        //         for column in self.columns.iter() { column.report() }
        //     }
        // }

        fn borrow_row(row: &Row) -> Self::DecodeIter<'_> {
            ColumnsIter {
                index: None,
                column: 0,
                data: row.data(),
                offset: 0,
            }
        }
        fn shorten<'a, 'b>(iter: Self::DecodeIter<'a>) -> Self::DecodeIter<'b>
        where
            'a: 'b,
        {
            ColumnsIter {
                index: iter.index,
                column: iter.column,
                data: iter.data,
                offset: iter.offset,
            }
        }
    }

    #[derive(Debug, Copy, Clone)]
    pub struct ColumnsIter<'a> {
        // Optional only to support borrowing owned as this
        pub index: Option<&'a ColumnsCodec>,
        pub column: usize,
        pub data: &'a [u8],
        pub offset: usize,
    }

    impl<'a> Iterator for ColumnsIter<'a> {
        type Item = &'a [u8];
        fn next(&mut self) -> Option<Self::Item> {
            if self.offset >= self.data.len() {
                None
            } else if let Some(bytes) = self
                .index
                .as_ref()
                .and_then(|i| i.columns.get(self.column))
                .and_then(|i| i.decode.get(self.data[self.offset].into()))
            {
                self.offset += 1;
                self.column += 1;
                Some(bytes)
            } else {
                let offset = self.offset;
                unsafe {
                    read_datum(self.data, &mut self.offset);
                }
                self.column += 1;
                Some(&self.data[offset..self.offset])
            }
        }
    }

    mod dictionary {

        use mz_repr::{read_datum, Row};
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
            type DecodeIter<'a> = BytesIter<'a>;

            /// Decode a sequence of byte slices.
            fn decode<'a>(&'a self, bytes: &'a [u8]) -> Self::DecodeIter<'a> {
                BytesIter {
                    index: Some(&self.decode),
                    data: bytes,
                    offset: 0,
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
            fn new_from(stats: [&Self; 2]) -> Self {
                // Collect most popular bytes from combined containers.
                let mut mg = MisraGries::default();
                for (thing, count) in stats[0].stats.0.clone().done() {
                    mg.update(thing, count);
                }
                for (thing, count) in stats[1].stats.0.clone().done() {
                    mg.update(thing, count);
                }
                let mut mg = mg.done().into_iter();
                // Establish encoding and decoding rules.
                let mut encode = BTreeMap::new();
                let mut decode = BytesMap::default();
                for tag in 0..=255 {
                    let tag_idx: usize = (tag % 4).into();
                    let shift = tag >> 2;
                    if ((stats[0].stats.1[tag_idx] | stats[1].stats.1[tag_idx]) >> shift) & 0x01
                        != 0
                    {
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

            fn borrow_row(row: &Row) -> Self::DecodeIter<'_> {
                BytesIter {
                    index: None,
                    data: row.data(),
                    offset: 0,
                }
            }

            fn shorten<'a, 'b>(iter: Self::DecodeIter<'a>) -> Self::DecodeIter<'b>
            where
                'a: 'b,
            {
                BytesIter {
                    index: iter.index,
                    data: iter.data,
                    offset: iter.offset,
                }
            }
        }

        #[derive(Debug, Copy, Clone)]
        pub struct BytesIter<'a> {
            // Optional only to support borrowing owned as this
            pub index: Option<&'a BytesMap>,
            pub data: &'a [u8],
            pub offset: usize,
        }

        impl<'a> Iterator for BytesIter<'a> {
            type Item = &'a [u8];
            fn next(&mut self) -> Option<Self::Item> {
                if self.offset >= self.data.len() {
                    None
                } else if let Some(bytes) = self
                    .index
                    .as_ref()
                    .and_then(|i| i.get(self.data[self.offset].into()))
                {
                    self.offset += 1;
                    Some(bytes)
                } else {
                    let offset = self.offset;
                    unsafe {
                        read_datum(self.data, &mut self.offset);
                    }
                    Some(&self.data[offset..self.offset])
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
        fn default() -> Self {
            Self {
                offsets: vec![0],
                bytes: Vec::new(),
            }
        }
    }
    impl BytesMap {
        fn push(&mut self, input: Option<&[u8]>) {
            if let Some(bytes) = input {
                self.bytes.extend(bytes);
            }
            self.offsets.push(self.bytes.len());
        }
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
            fn default() -> Self {
                Self {
                    inner: Vec::with_capacity(1024),
                }
            }
        }

        impl<T: Ord> MisraGries<T> {
            /// Inserts an additional element to the summary.
            pub fn insert(&mut self, element: T) {
                self.update(element, 1);
            }
            /// Inserts multiple copies of an element to the summary.
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
