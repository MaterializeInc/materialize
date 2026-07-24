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

//! Container for columnar data.

#![deny(missing_docs)]

pub mod batcher;
pub mod builder;
pub mod builder_input;
pub mod consolidate;
pub mod merge_batcher;

use std::hash::Hash;

use columnar::Borrow;
use columnar::bytes::indexed;
use columnar::common::IterOwn;
use columnar::{Clear, FromBytes, Index, Len};
use columnar::{Columnar, Ref};
use differential_dataflow::Hashable;
use differential_dataflow::trace::implementations::merge_batcher::MergeBatcher;
use timely::Accountable;
use timely::bytes::arc::Bytes;
use timely::container::{DrainContainer, PushInto, SizableContainer};
use timely::dataflow::channels::ContainerBytes;

use crate::columnation::ColInternalMerger;

/// A batcher for columnar storage.
///
/// The chunker is supplied to the arrange operator separately. Callers pass
/// it explicitly: [`ColumnationChunker`](crate::columnation::ColumnationChunker)
/// for `Vec<_>` input, or [`batcher::Chunker`] (over a `ColumnationStack<_>`) for
/// [`Column`] input.
pub type Col2ValBatcher<K, V, T, R> = MergeBatcher<ColInternalMerger<(K, V), T, R>>;
/// A batcher for columnar storage with unit values.
pub type Col2KeyBatcher<K, T, R> = Col2ValBatcher<K, (), T, R>;

/// Pageable counterpart to [`Col2ValBatcher`]. Routes every chunk produced
/// by chunking, merging, or extract through a [`crate::column_pager::ColumnPager`],
/// so memory pressure can spill chains to a backing store without touching
/// the merge / extract bodies.
///
/// Drop-in shape at the type level: both aliases take `(K, V, T, R)` and
/// produce a `Batcher<Input = Column<((K, V), T, R)>, Output = Column<((K,
/// V), T, R)>>`. Call sites can swap with `cargo fix`–style renaming once
/// downstream `Trace`/`Builder` impls have been wired up. The pager itself
/// defaults to [`crate::column_pager::ColumnPager::disabled`]; inject a
/// real one via [`merge_batcher::ColumnMergeBatcher::set_pager`].
pub type Col2ValPagedBatcher<K, V, T, R> = merge_batcher::ColumnMergeBatcher<(K, V), T, R>;

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
    ///
    /// `Vec<u64>` guarantees `u64` alignment for the contained bytes.
    Align(Vec<u64>),
}

impl<C: Columnar> Column<C> {
    /// Empties the column, retaining the `Typed` variant's allocation so the
    /// caller can refill it.
    ///
    /// [`columnar::Clear`] clears the typed container in place without
    /// releasing its capacity. The serialized variants (`Bytes`/`Align`) own
    /// no reusable typed buffer, so they are reset to an empty `Typed`.
    #[inline]
    pub fn clear(&mut self) {
        match self {
            Column::Typed(t) => t.clear(),
            Column::Bytes(_) | Column::Align(_) => *self = Default::default(),
        }
    }

    /// Borrows the container as a reference.
    #[inline]
    pub fn borrow(&self) -> <C::Container as Borrow>::Borrowed<'_> {
        match self {
            Column::Typed(t) => t.borrow(),
            Column::Bytes(b) => <<C::Container as Borrow>::Borrowed<'_>>::from_bytes(
                &mut indexed::decode(bytemuck::cast_slice(b)),
            ),
            Column::Align(a) => {
                <<C::Container as Borrow>::Borrowed<'_>>::from_bytes(&mut indexed::decode(a))
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
                Self::Align(bytemuck::allocation::pod_collect_to_vec(b))
            }
            Column::Align(a) => Column::Align(a.clone()),
        }
    }
}

impl<C: Columnar> Accountable for Column<C> {
    #[inline]
    fn record_count(&self) -> i64 {
        self.borrow().len().try_into().expect("Must fit")
    }
}
impl<C: Columnar> DrainContainer for Column<C> {
    type Item<'a> = Ref<'a, C>;
    type DrainIter<'a> = IterOwn<<C::Container as Borrow>::Borrowed<'a>>;
    #[inline]
    fn drain(&mut self) -> Self::DrainIter<'_> {
        self.borrow().into_index_iter()
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

/// Words per 32 KiB. `length_in_words` returns serialized size in `u64` units,
/// so this is the page count we round up to. Picked to match
/// [`builder::ColumnBuilder`]'s output granularity so chunks shipped from the
/// merger and chunks shipped from the builder are sized comparably.
const SHIP_WORDS: usize = 1 << 12;

/// Returns true once the serialized size of `borrow` is within 10% of the next
/// `SHIP_WORDS` boundary.
///
/// Same heuristic as `ColumnBuilder::push_into`; lifted out so the merger and
/// the `SizableContainer` impl agree on the ship signal.
#[inline]
pub(crate) fn at_serialized_capacity<'a, A>(borrow: &A) -> bool
where
    A: columnar::AsBytes<'a>,
{
    let words = indexed::length_in_words(borrow);
    let round = (words + (SHIP_WORDS - 1)) & !(SHIP_WORDS - 1);
    round - words < round / 10
}

impl<C: Columnar> SizableContainer for Column<C> {
    fn at_capacity(&self) -> bool {
        // Match `ColumnBuilder`'s ship heuristic: serialized size within 10%
        // of the next 32 KiB. Aligns chunk-size choices across the two paths
        // and keeps recipients dealing with a single granularity.
        //
        // Serialized chunks (`Bytes` / `Align`) have no typed builder to push
        // into, so they're trivially "at capacity" — there's no further work
        // they can absorb.
        match self {
            Column::Typed(c) => at_serialized_capacity(&c.borrow()),
            Column::Bytes(_) | Column::Align(_) => true,
        }
    }

    fn ensure_capacity(&mut self, _stash: &mut Option<Self>) {
        // No pre-reservation: chunks are recycled by the merge framework, so
        // leaf capacities settle to steady-state after the first round and
        // there is nothing useful to reserve up front. The `SizableContainer`
        // impl exists so `at_capacity` is callable on result chunks during
        // `Merger::merge` orchestration; `ensure_capacity` is a required
        // method on the trait but has no work to do here.
    }
}

impl<C: Columnar> ContainerBytes for Column<C> {
    #[inline]
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
            // We failed to cast the slice, so we'll reallocate. `Vec<u64>`
            // is u64-aligned by construction.
            Self::Align(bytemuck::allocation::pod_collect_to_vec(&bytes[..]))
        }
    }

    #[inline]
    fn length_in_bytes(&self) -> usize {
        match self {
            Column::Typed(t) => indexed::length_in_bytes(&t.borrow()),
            Column::Bytes(b) => b.len(),
            Column::Align(a) => 8 * a.len(),
        }
    }

    #[inline]
    fn into_bytes<W: ::std::io::Write>(&self, writer: &mut W) {
        match self {
            Column::Typed(t) => indexed::write(writer, &t.borrow()).unwrap(),
            Column::Bytes(b) => writer.write_all(b).unwrap(),
            Column::Align(a) => writer.write_all(bytemuck::cast_slice(a)).unwrap(),
        }
    }
}

/// An exchange function for columnar tuples of the form `((K, V), T, D)`. Rust has a hard
/// time to figure out the lifetimes of the elements when specified as a closure, so we rather
/// specify it as a function.
#[inline(always)]
pub fn columnar_exchange<K, V, T, D>(((k, _), _, _): &Ref<'_, ((K, V), T, D)>) -> u64
where
    K: Columnar,
    for<'a> Ref<'a, K>: Hash,
    V: Columnar,
    D: Columnar,
    T: Columnar,
{
    k.hashed()
}

#[cfg(test)]
mod tests {
    use timely::bytes::arc::BytesMut;
    use timely::container::PushInto;
    use timely::dataflow::channels::ContainerBytes;

    use super::*;

    /// Produce some bytes that are in columnar format.
    fn raw_columnar_bytes() -> Vec<u8> {
        let mut raw = Vec::new();
        raw.extend(16_u64.to_le_bytes()); // offsets
        raw.extend(28_u64.to_le_bytes()); // length
        raw.extend(1_i32.to_le_bytes());
        raw.extend(2_i32.to_le_bytes());
        raw.extend(3_i32.to_le_bytes());
        raw.extend([0, 0, 0, 0]); // padding
        raw
    }

    #[mz_ore::test]
    fn test_column_clone() {
        let columns = Columnar::as_columns([1, 2, 3].iter());
        let column_typed: Column<i32> = Column::Typed(columns);
        let column_typed2 = column_typed.clone();

        assert_eq!(
            column_typed2.borrow().into_index_iter().collect::<Vec<_>>(),
            vec![&1, &2, &3]
        );

        let bytes = BytesMut::from(raw_columnar_bytes()).freeze();
        let column_bytes: Column<i32> = Column::Bytes(bytes);
        let column_bytes2 = column_bytes.clone();

        assert_eq!(
            column_bytes2.borrow().into_index_iter().collect::<Vec<_>>(),
            vec![&1, &2, &3]
        );

        let raw = raw_columnar_bytes();
        let mut region: Vec<u64> = vec![0; raw.len() / 8];
        let region_bytes = bytemuck::cast_slice_mut(&mut region[..]);
        region_bytes[..raw.len()].copy_from_slice(&raw);
        let column_align: Column<i32> = Column::Align(region);
        let column_align2 = column_align.clone();

        assert_eq!(
            column_align2.borrow().into_index_iter().collect::<Vec<_>>(),
            vec![&1, &2, &3]
        );
    }

    /// Assert the desired contents of raw_columnar_bytes so that diagnosing test failures is
    /// easier.
    #[mz_ore::test]
    fn test_column_known_bytes() {
        let mut column: Column<i32> = Default::default();
        column.push_into(1);
        column.push_into(2);
        column.push_into(3);
        let mut data = Vec::new();
        column.into_bytes(&mut std::io::Cursor::new(&mut data));
        assert_eq!(data, raw_columnar_bytes());
    }

    #[mz_ore::test]
    fn test_column_from_bytes() {
        let raw = raw_columnar_bytes();

        let buf = vec![0; raw.len() + 8];
        let align = buf.as_ptr().align_offset(std::mem::size_of::<u64>());
        let mut bytes_mut = BytesMut::from(buf);
        let _ = bytes_mut.extract_to(align);
        bytes_mut[..raw.len()].copy_from_slice(&raw);
        let aligned_bytes = bytes_mut.extract_to(raw.len());

        let column: Column<i32> = Column::from_bytes(aligned_bytes);
        assert!(matches!(column, Column::Bytes(_)));
        assert_eq!(
            column.borrow().into_index_iter().collect::<Vec<_>>(),
            vec![&1, &2, &3]
        );

        let buf = vec![0; raw.len() + 8];
        let align = buf.as_ptr().align_offset(std::mem::size_of::<u64>());
        let mut bytes_mut = BytesMut::from(buf);
        let _ = bytes_mut.extract_to(align + 1);
        bytes_mut[..raw.len()].copy_from_slice(&raw);
        let unaligned_bytes = bytes_mut.extract_to(raw.len());

        let column: Column<i32> = Column::from_bytes(unaligned_bytes);
        assert!(matches!(column, Column::Align(_)));
        assert_eq!(
            column.borrow().into_index_iter().collect::<Vec<_>>(),
            vec![&1, &2, &3]
        );
    }
}
