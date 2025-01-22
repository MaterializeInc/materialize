// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Reusable containers.

use std::hash::Hash;

use columnar::Columnar;
use differential_dataflow::trace::implementations::merge_batcher::{ColMerger, MergeBatcher};
use differential_dataflow::Hashable;
use timely::container::columnation::TimelyStack;

pub mod array;
pub mod stack;

pub use container::Column;

mod container {
    use columnar::bytes::serialization::decode;
    use columnar::common::IterOwn;
    use columnar::Columnar;
    use columnar::Container as _;
    use columnar::{AsBytes, Clear, FromBytes, Index, Len};
    use mz_ore::cast::CastFrom;
    use timely::bytes::arc::Bytes;
    use timely::container::PushInto;
    use timely::dataflow::channels::ContainerBytes;
    use timely::Container;

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
        Align(Box<[u64]>),
    }

    impl<C: Columnar> Column<C> {
        /// Borrows the container as a reference.
        fn borrow(&self) -> <C::Container as columnar::Container<C>>::Borrowed<'_> {
            match self {
                Column::Typed(t) => t.borrow(),
                Column::Bytes(b) => {
                    <<C::Container as columnar::Container<C>>::Borrowed<'_>>::from_bytes(
                        &mut decode(bytemuck::cast_slice(b)),
                    )
                }
                Column::Align(a) => {
                    <<C::Container as columnar::Container<C>>::Borrowed<'_>>::from_bytes(
                        &mut decode(a),
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
                    let mut alloc: Vec<u64> = vec![0; b.len() / 8];
                    bytemuck::cast_slice_mut(&mut alloc[..]).copy_from_slice(&b[..]);
                    Self::Align(alloc.into())
                }
                Column::Align(a) => Column::Align(a.clone()),
            }
        }
    }

    impl<C: Columnar> Container for Column<C> {
        type ItemRef<'a> = C::Ref<'a>;
        type Item<'a> = C::Ref<'a>;

        fn len(&self) -> usize {
            self.borrow().len()
        }

        // This sets the `Bytes` variant to be an empty `Typed` variant, appropriate for pushing into.
        fn clear(&mut self) {
            match self {
                Column::Typed(t) => t.clear(),
                Column::Bytes(_) => *self = Column::Typed(Default::default()),
                Column::Align(_) => *self = Column::Typed(Default::default()),
            }
        }

        type Iter<'a> = IterOwn<<C::Container as columnar::Container<C>>::Borrowed<'a>>;

        fn iter(&self) -> Self::Iter<'_> {
            self.borrow().into_iter()
        }

        type DrainIter<'a> = IterOwn<<C::Container as columnar::Container<C>>::Borrowed<'a>>;

        fn drain(&mut self) -> Self::DrainIter<'_> {
            self.borrow().into_iter()
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
                let mut alloc: Vec<u64> = vec![0; bytes.len() / 8];
                bytemuck::cast_slice_mut(&mut alloc[..]).copy_from_slice(&bytes[..]);
                Self::Align(alloc.into())
            }
        }

        fn length_in_bytes(&self) -> usize {
            match self {
                Column::Typed(t) => 8 * t.borrow().length_in_words(),
                Column::Bytes(b) => b.len(),
                Column::Align(a) => 8 * a.len(),
            }
        }

        fn into_bytes<W: ::std::io::Write>(&self, writer: &mut W) {
            match self {
                Column::Typed(t) => {
                    use columnar::Container;
                    // Columnar data is serialized as a sequence of `u64` values, with each `[u8]` slice
                    // serialize as first its length in bytes, and then as many `u64` values as needed.
                    // Padding should be added, but only for alignment; no specific values are required.
                    for (align, bytes) in t.borrow().as_bytes() {
                        assert!(align <= 8);
                        let length = u64::cast_from(bytes.len());
                        writer
                            .write_all(bytemuck::cast_slice(std::slice::from_ref(&length)))
                            .unwrap();
                        writer.write_all(bytes).unwrap();
                        let padding = usize::cast_from((8 - (length % 8)) % 8);
                        writer.write_all(&[0; 8][..padding]).unwrap();
                    }
                }
                Column::Bytes(b) => writer.write_all(b).unwrap(),
                Column::Align(a) => writer.write_all(bytemuck::cast_slice(a)).unwrap(),
            }
        }
    }
}

pub use builder::ColumnBuilder;
mod builder {
    use std::collections::VecDeque;

    use columnar::{AsBytes, Clear, Columnar, Len, Push};
    use timely::container::PushInto;
    use timely::container::{ContainerBuilder, LengthPreservingContainerBuilder};

    use super::Column;

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
            let words = self.current.borrow().length_in_words();
            let round = (words + ((1 << 18) - 1)) & !((1 << 18) - 1);
            if round - words < round / 10 {
                let mut alloc = Vec::with_capacity(round);
                columnar::bytes::serialization::encode(
                    &mut alloc,
                    self.current.borrow().as_bytes(),
                );
                self.pending
                    .push_back(Column::Align(alloc.into_boxed_slice()));
                self.current.clear();
            }
        }
    }

    impl<C: Columnar> Default for ColumnBuilder<C> {
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
pub fn columnar_exchange<K, V, T, D>(((k, _), _, _): &<((K, V), T, D) as Columnar>::Ref<'_>) -> u64
where
    K: Columnar,
    for<'a> K::Ref<'a>: Hash,
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
    use timely::container::{ContainerBuilder, PushInto};
    use timely::Container;

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
        for<'b> D::Ref<'b>: Ord + Copy,
        T: Columnar,
        for<'b> T::Ref<'b>: Ord + Copy,
        R: Columnar + Semigroup + for<'b> Semigroup<R::Ref<'b>>,
        for<'b> R::Ref<'b>: Ord,
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

pub use provided_builder::ProvidedBuilder;

mod provided_builder {
    use timely::container::ContainerBuilder;
    use timely::Container;

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
