pub use self::container::DatumContainer;
pub use self::spines::{RowRowSpine, RowSpine, RowValSpine};

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

    use differential_dataflow::trace::cursor::MyTrait;
    use differential_dataflow::trace::implementations::BatchContainer;
    use differential_dataflow::trace::implementations::OffsetList;

    use mz_repr::{read_datum, Datum, Row};

    /// A slice container with four bytes overhead per slice.
    pub struct DatumContainer {
        batches: Vec<DatumBatch>,
    }

    impl Default for DatumContainer {
        fn default() -> Self {
            Self {
                batches: Vec::new(),
            }
        }
    }

    impl BatchContainer for DatumContainer {
        type PushItem = Row;
        type ReadItem<'a> = DatumSeq<'a>;

        fn push(&mut self, item: Self::PushItem) {
            self.copy_push(&item);
        }
        fn copy_push(&mut self, item: &Self::PushItem) {
            self.copy(MyTrait::borrow_as(item));
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
        fn reserve(&mut self, _additional: usize) {}

        fn copy(&mut self, item: Self::ReadItem<'_>) {
            if let Some(batch) = self.batches.last_mut() {
                let success = batch.try_push(item.bytes);
                if !success {
                    let mut new_batch = DatumBatch::with_capacity(std::cmp::max(
                        2 * batch.storage.capacity(),
                        item.bytes.len(),
                    ));
                    new_batch.try_push(item.bytes);
                    self.batches.push(new_batch);
                }
            }
        }

        fn with_capacity(size: usize) -> Self {
            Self {
                batches: vec![DatumBatch::with_capacity(size)],
            }
        }

        fn merge_capacity(cont1: &Self, cont2: &Self) -> Self {
            Self {
                batches: vec![DatumBatch::with_capacity(cont1.len() + cont2.len())],
            }
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

    /// A batch of slice storage.
    ///
    /// The backing storage for this batch will not be resized.
    pub struct DatumBatch {
        offsets: OffsetList,
        storage: Vec<u8>,
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

        fn with_capacity(cap: usize) -> Self {
            let mut offsets = OffsetList::with_capacity(cap + 1);
            offsets.push(0);
            Self {
                offsets,
                storage: Vec::with_capacity(cap),
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
