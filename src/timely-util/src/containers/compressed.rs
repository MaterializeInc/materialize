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

/// A compressed column that stores data in a compressed format using LZ4.
///
/// The column's type needs to be rememnbered outside of this struct.
#[derive(Debug, Clone, Default)]
pub struct CompressedColumn {
    uncompressed_size: usize,
    elements: usize,
    data: Vec<u8>,
    valid: usize,
}

impl CompressedColumn {
    pub fn uncompressed_size(&self) -> usize {
        self.uncompressed_size
    }

    pub fn capacity(&self) -> usize {
        self.data.len()
    }

    pub fn elements(&self) -> usize {
        self.elements
    }

    // fn get_region(len: usize, empty: &mut Self) -> Vec<u8> {
    //     let mut region = Vec::from(std::mem::take(&mut empty.data));
    //     region.clear();
    //     region.resize(len, 0);
    //     // if region.capacity() < len {
    //     //     region.reserve(len - region.len());
    //     // }
    //     region
    // }
    //
    pub fn compress_aligned(
        column_len: usize,
        align: &[u64],
        buffer: &mut Vec<u8>,
        empty: &mut Self,
    ) -> Self {
        let max_size = lz4_flex::block::get_maximum_output_size(align.len() * 8);
        buffer.resize(max_size, 0);
        let len = lz4_flex::block::compress_into(bytemuck::cast_slice(&*align), buffer).unwrap();
        let mut region = Vec::from(std::mem::take(&mut empty.data));
        region.clear();
        region.extend_from_slice(&buffer[..len]);
        // assert_eq!(
        //     bytemuck::cast_slice::<_, u8>(&*align),
        //     &*lz4_flex::block::decompress(&region[..len], align.len() * 8).unwrap()
        // );
        // println!(
        //     "CompressedBlock: {} bytes into {len} bytes",
        //     align.len() * 8
        // );
        Self {
            uncompressed_size: align.len() * 8,
            elements: column_len,
            data: region,
            valid: len,
        }
    }

    pub fn decompress(&self, aligned: &mut [u64]) {
        // println!(
        //     "Decompressing CompressedBlock: {:.2} {} bytes into {} bytes",
        //     1. - self.data.len() as f64 / self.uncompressed_size as f64,
        //     self.data.len(),
        //     self.uncompressed_size
        // );
        // let length_in_words = (self.uncompressed_size + 7) / 8;
        // aligned.resize(length_in_words, 0);
        assert_eq!(self.uncompressed_size, aligned.len() * 8);
        lz4_flex::block::decompress_into(
            &self.data[..self.valid],
            &mut bytemuck::cast_slice_mut(aligned),
        )
        .expect("Failed to decompress block");
    }
    // pub fn decompress(&self, aligned: &mut Vec<u64>) {
    //     println!(
    //         "Decompressing CompressedBlock: {} bytes into {} bytes",
    //         self.data.len(),
    //         self.uncompressed_size
    //     );
    //     let length_in_words = (self.uncompressed_size + 7) / 8;
    //     aligned.resize(length_in_words, 0);
    //     lz4_flex::block::decompress_into(
    //         &self.data,
    //         &mut bytemuck::cast_slice_mut(&mut aligned[..length_in_words]),
    //     )
    //     .expect("Failed to decompress block");
    // }
}
/*
impl<C: Columnar> Default for CompressedColumn<C> {
    fn default() -> Self {
        CompressedColumn::Uncompressed(Column::default())
    }
}


/// A queue for extracting items from a `Column` container.
pub struct ColumnQueue<'a, T: Columnar> {
    list: <T::Container as columnar::Container<T>>::Borrowed<'a>,
    head: usize,
}

impl<'a, D, T, R> ContainerQueue<'a, CompressedColumn<(D, T, R)>> for ColumnQueue<'a, (D, T, R)>
where
    D: for<'b> Columnar<Ref<'b>: Ord>,
    T: for<'b> Columnar<Ref<'b>: Ord>,
    R: Columnar,
{
    type Item = <(D, T, R) as Columnar>::Ref<'a>;
    type SelfGAT<'b> = ColumnQueue<'b, (D, T, R)>;
    fn pop(&mut self) -> Self::Item {
        self.head += 1;
        self.list.get(self.head - 1)
    }
    fn is_empty(&mut self) -> bool {
        use columnar::Len;
        self.head == self.list.len()
    }
    fn cmp_heads<'b>(&mut self, other: &mut ColumnQueue<'b, (D, T, R)>) -> std::cmp::Ordering {
        let (data1, time1, _) = self.peek();
        let (data2, time2, _) = other.peek();

        let data1 = D::reborrow(data1);
        let time1 = T::reborrow(time1);
        let data2 = D::reborrow(data2);
        let time2 = T::reborrow(time2);

        (data1, time1).cmp(&(data2, time2))
    }
    fn new(list: &'a mut CompressedColumn<(D, T, R)>) -> ColumnQueue<'a, (D, T, R)> {
        ColumnQueue {
            list: list.borrow(),
            head: 0,
        }
    }
}

impl<'a, T: Columnar> ColumnQueue<'a, T> {
    fn peek(&self) -> T::Ref<'a> {
        self.list.get(self.head)
    }
}

impl<D, T, R> MergerChunk for CompressedColumn<(D, T, R)>
where
    D: for<'a> Columnar<Ref<'a>: Ord>,
    T: for<'a> Columnar<Ref<'a>: Ord> + Timestamp,
    for<'a> <T as Columnar>::Ref<'a>: Copy,
    R: Default + Semigroup + Columnar,
{
    type ContainerQueue<'a> = ColumnQueue<'a, (D, T, R)>;
    type TimeOwned = T;

    fn time_kept(
        (_, time, _): &Self::Item<'_>,
        upper: &AntichainRef<Self::TimeOwned>,
        frontier: &mut Antichain<Self::TimeOwned>,
        stash: &mut T,
    ) -> bool {
        stash.copy_from(*time);
        if upper.less_equal(stash) {
            frontier.insert_ref(stash);
            true
        } else {
            false
        }
    }
    fn account(&self) -> (usize, usize, usize, usize) {
        let (mut size, mut cap, mut count) = (0, 0, 0);
        match self {
            Column::Typed(_typed) => {
                // use columnar::HeapSize;
                // let mut cb = |s, c| {
                //     size += s;
                //     cap += c;
                //     count += 1;
                // };
                // data.heap_size(&mut cb);
                // time.heap_size(&mut cb);
                // diff.heap_size(&mut cb);
            }
            Column::Bytes(bytes) => {
                size += bytes.len();
                cap += bytes.len();
                count += 1;
            }
            Column::Align(align) => {
                size += align.len() * 8; // 8 bytes per u64
                cap += align.len() * 8; // 8 bytes per u64
                count += 1;
            }
        }
        (self.len(), size, cap, count)
    }
}

/// A container builder for `Column<C>`.
pub struct CompressedColumnBuilder<C: Columnar> {
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

impl<C: Columnar<Container: Push<T>>, T> PushInto<T> for CompressedColumnBuilder<C> {
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

impl<C: Columnar> Default for CompressedColumnBuilder<C> {
    #[inline(always)]
    fn default() -> Self {
        CompressedColumnBuilder {
            current: Default::default(),
            finished: None,
            pending: Default::default(),
        }
    }
}

impl<C: Columnar> ContainerBuilder for CompressedColumnBuilder<C> {
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

impl<D, T, R> PushAndAdd for CompressedColumnBuilder<(D, T, R)>
where
    D: Columnar,
    T: timely::PartialOrder + Columnar,
    R: Default + Semigroup + Columnar,
{
    type Item<'a> = <(D, T, R) as Columnar>::Ref<'a>;
    type DiffOwned = R;

    fn push_and_add(
        &mut self,
        (data, time, diff1): Self::Item<'_>,
        (_, _, diff2): Self::Item<'_>,
        stash: &mut Self::DiffOwned,
    ) {
        stash.copy_from(diff1);
        let stash2: R = R::into_owned(diff2);
        stash.plus_equals(&stash2);
        if !stash.is_zero() {
            use timely::container::PushInto;
            self.push_into((data, time, &*stash));
        }
    }
}
*/
