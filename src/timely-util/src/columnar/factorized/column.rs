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

//! Timely-compatible container for factorized columnar data.
//!
//! [`FactColumn`] wraps a [`KVUpdates`] trie and implements the timely container
//! traits (`Accountable`, `PushInto`, `DrainContainer`, `ContainerBytes`), enabling
//! factorized data to flow through timely dataflow edges with efficient columnar
//! serialization.

use columnar::bytes::indexed;
use columnar::{Borrow, Columnar, FromBytes, Index, Len, Push};
use mz_ore::region::Region;
use timely::Accountable;
use timely::bytes::arc::Bytes;
use timely::container::{DrainContainer, PushInto, SizableContainer};
use timely::dataflow::channels::ContainerBytes;

use super::{KVUpdates, Level, Lists, child_range};

/// A timely-compatible container for factorized columnar `(K, V, T, R)` data.
///
/// Like [`Column<C>`](crate::columnar::Column), this enum has three variants:
/// `Typed` for building, `Bytes`/`Align` for zero-copy deserialization.
/// Serialization uses [`Level`]'s `AsBytes`/`FromBytes` implementations.
pub enum FactColumn<K: Columnar, V: Columnar, T: Columnar, R: Columnar> {
    /// Owned typed data, used for accumulating tuples via [`PushInto`].
    Typed(KVUpdates<K, V, T, R>),
    /// Binary data received from timely communication.
    Bytes(Bytes),
    /// Aligned binary data, used when `Bytes` has wrong alignment.
    Align(Region<u64>),
}

impl<K: Columnar, V: Columnar, T: Columnar, R: Columnar> FactColumn<K, V, T, R> {
    /// Borrow the stored data as a [`Level`] with borrowed inner types.
    ///
    /// Works for all three variants: typed data is borrowed directly,
    /// binary data is decoded via `FromBytes`.
    #[inline]
    pub fn borrow(
        &self,
    ) -> Level<
        <Lists<columnar::ContainerOf<K>> as Borrow>::Borrowed<'_>,
        Level<
            <Lists<columnar::ContainerOf<V>> as Borrow>::Borrowed<'_>,
            <Lists<(columnar::ContainerOf<T>, columnar::ContainerOf<R>)> as Borrow>::Borrowed<'_>,
        >,
    > {
        match self {
            FactColumn::Typed(t) => t.borrowed(),
            FactColumn::Bytes(b) => {
                Level::from_bytes(&mut indexed::decode(bytemuck::cast_slice(b)))
            }
            FactColumn::Align(a) => Level::from_bytes(&mut indexed::decode(a)),
        }
    }

    /// Clear the container, resetting it to an empty [`FactColumn::Typed`].
    ///
    /// For `Typed`, this reuses existing storage via [`KVUpdates::clear`]. For
    /// `Bytes`/`Align`, the backing allocation is dropped and replaced with a
    /// fresh empty `Typed` variant — we can't mutate `Bytes`/`Align` in place.
    #[inline]
    pub fn clear(&mut self) {
        match self {
            FactColumn::Typed(t) => t.clear(),
            FactColumn::Bytes(_) | FactColumn::Align(_) => {
                *self = Self::default();
            }
        }
    }
}

impl<K: Columnar, V: Columnar, T: Columnar, R: Columnar> Default for FactColumn<K, V, T, R> {
    fn default() -> Self {
        Self::Typed(Default::default())
    }
}

impl<K: Columnar, V: Columnar, T: Columnar, R: Columnar> Clone for FactColumn<K, V, T, R>
where
    KVUpdates<K, V, T, R>: Clone,
{
    fn clone(&self) -> Self {
        match self {
            FactColumn::Typed(t) => FactColumn::Typed(t.clone()),
            FactColumn::Bytes(b) => {
                assert_eq!(b.len() % 8, 0);
                let mut alloc: Region<u64> = crate::containers::alloc_aligned_zeroed(b.len() / 8);
                let alloc_bytes = bytemuck::cast_slice_mut(&mut alloc);
                alloc_bytes[..b.len()].copy_from_slice(b);
                Self::Align(alloc)
            }
            FactColumn::Align(a) => {
                let mut alloc = crate::containers::alloc_aligned_zeroed(a.len());
                alloc[..a.len()].copy_from_slice(a);
                FactColumn::Align(alloc)
            }
        }
    }
}

impl<K: Columnar, V: Columnar, T: Columnar, R: Columnar> Accountable for FactColumn<K, V, T, R> {
    #[inline]
    fn record_count(&self) -> i64 {
        let borrowed = self.borrow();
        // Leaf count = number of (time, diff) pairs.
        Len::len(&borrowed.rest.rest.values.0)
            .try_into()
            .expect("record count fits i64")
    }
}

impl<K: Columnar, V: Columnar, T: Columnar, R: Columnar> SizableContainer
    for FactColumn<K, V, T, R>
{
    fn at_capacity(&self) -> bool {
        // Typed containers are never "at capacity" in the Vec sense.
        // The builder controls flushing based on byte size.
        false
    }
    fn ensure_capacity(&mut self, stash: &mut Option<Self>) {
        if matches!(self, FactColumn::Bytes(_) | FactColumn::Align(_)) {
            *self = stash.take().unwrap_or_default();
        }
    }
}

impl<K, V, T, R, KV> PushInto<(KV, T, R)> for FactColumn<K, V, T, R>
where
    K: Columnar,
    V: Columnar,
    T: Columnar,
    R: Columnar,
    KV: std::borrow::Borrow<(K, V)>,
    columnar::ContainerOf<K>: for<'a> Push<&'a K>,
    columnar::ContainerOf<V>: for<'a> Push<&'a V>,
    columnar::ContainerOf<T>: for<'a> Push<&'a T>,
    columnar::ContainerOf<R>: for<'a> Push<&'a R>,
{
    #[inline]
    fn push_into(&mut self, (kv, t, r): (KV, T, R)) {
        let (k, v) = kv.borrow();
        match self {
            FactColumn::Typed(storage) => {
                storage.push_flat(k, v, (&t, &r));
            }
            FactColumn::Bytes(_) | FactColumn::Align(_) => {
                unimplemented!("Pushing into FactColumn::Bytes without first clearing");
            }
        }
    }
}

/// Drains a [`FactColumn`] by iterating `(K, V, T, R)` tuples from the trie.
impl<K, V, T, R> DrainContainer for FactColumn<K, V, T, R>
where
    K: Columnar + Clone,
    V: Columnar + Clone,
    T: Columnar + Clone,
    R: Columnar + Clone,
{
    type Item<'a> = (
        columnar::Ref<'a, K>,
        columnar::Ref<'a, V>,
        columnar::Ref<'a, T>,
        columnar::Ref<'a, R>,
    );
    type DrainIter<'a> = FactColumnDrain<'a, K, V, T, R>;

    #[inline]
    fn drain(&mut self) -> Self::DrainIter<'_> {
        FactColumnDrain::new(self)
    }
}

/// Iterator that drains `(K, V, T, R)` tuples from a [`FactColumn`].
///
/// Traverses the trie level by level using `child_range` at each level,
/// yielding one tuple per `(time, diff)` leaf entry.
pub struct FactColumnDrain<'a, K: Columnar, V: Columnar, T: Columnar, R: Columnar> {
    /// Borrowed key-level lists.
    k_lists: <Lists<columnar::ContainerOf<K>> as Borrow>::Borrowed<'a>,
    /// Borrowed val-level lists.
    v_lists: <Lists<columnar::ContainerOf<V>> as Borrow>::Borrowed<'a>,
    /// Borrowed leaf-level lists.
    leaf_lists:
        <Lists<(columnar::ContainerOf<T>, columnar::ContainerOf<R>)> as Borrow>::Borrowed<'a>,
    /// Current position tracking: which outer group, key, val, and leaf index.
    /// We flatten the trie into a linear sequence via these cursors.
    outer: usize,
    outer_end: usize,
    key_idx: usize,
    key_end: usize,
    val_idx: usize,
    val_end: usize,
    leaf_idx: usize,
    leaf_end: usize,
}

impl<'a, K: Columnar, V: Columnar, T: Columnar, R: Columnar> FactColumnDrain<'a, K, V, T, R> {
    fn new(column: &'a FactColumn<K, V, T, R>) -> Self {
        let borrowed = column.borrow();
        let k_lists = borrowed.lists;
        let v_lists = borrowed.rest.lists;
        let leaf_lists = borrowed.rest.rest;

        let outer_end = Len::len(&k_lists);
        let mut drain = FactColumnDrain {
            k_lists,
            v_lists,
            leaf_lists,
            outer: 0,
            outer_end,
            key_idx: 0,
            key_end: 0,
            val_idx: 0,
            val_end: 0,
            leaf_idx: 0,
            leaf_end: 0,
        };
        drain.advance_to_first();
        drain
    }

    /// Advance cursors to the first leaf entry, or set all to end if empty.
    fn advance_to_first(&mut self) {
        while self.outer < self.outer_end {
            let key_range = child_range(self.k_lists.bounds, self.outer);
            self.key_idx = key_range.start;
            self.key_end = key_range.end;
            while self.key_idx < self.key_end {
                let val_range = child_range(self.v_lists.bounds, self.key_idx);
                self.val_idx = val_range.start;
                self.val_end = val_range.end;
                while self.val_idx < self.val_end {
                    let leaf_range = child_range(self.leaf_lists.bounds, self.val_idx);
                    self.leaf_idx = leaf_range.start;
                    self.leaf_end = leaf_range.end;
                    if self.leaf_idx < self.leaf_end {
                        return;
                    }
                    self.val_idx += 1;
                }
                self.key_idx += 1;
            }
            self.outer += 1;
        }
    }

    /// Advance to the next leaf entry after the current one.
    fn advance_next(&mut self) {
        self.leaf_idx += 1;
        if self.leaf_idx < self.leaf_end {
            return;
        }
        self.val_idx += 1;
        while self.val_idx < self.val_end {
            let leaf_range = child_range(self.leaf_lists.bounds, self.val_idx);
            self.leaf_idx = leaf_range.start;
            self.leaf_end = leaf_range.end;
            if self.leaf_idx < self.leaf_end {
                return;
            }
            self.val_idx += 1;
        }
        self.key_idx += 1;
        while self.key_idx < self.key_end {
            let val_range = child_range(self.v_lists.bounds, self.key_idx);
            self.val_idx = val_range.start;
            self.val_end = val_range.end;
            while self.val_idx < self.val_end {
                let leaf_range = child_range(self.leaf_lists.bounds, self.val_idx);
                self.leaf_idx = leaf_range.start;
                self.leaf_end = leaf_range.end;
                if self.leaf_idx < self.leaf_end {
                    return;
                }
                self.val_idx += 1;
            }
            self.key_idx += 1;
        }
        self.outer += 1;
        while self.outer < self.outer_end {
            let key_range = child_range(self.k_lists.bounds, self.outer);
            self.key_idx = key_range.start;
            self.key_end = key_range.end;
            while self.key_idx < self.key_end {
                let val_range = child_range(self.v_lists.bounds, self.key_idx);
                self.val_idx = val_range.start;
                self.val_end = val_range.end;
                while self.val_idx < self.val_end {
                    let leaf_range = child_range(self.leaf_lists.bounds, self.val_idx);
                    self.leaf_idx = leaf_range.start;
                    self.leaf_end = leaf_range.end;
                    if self.leaf_idx < self.leaf_end {
                        return;
                    }
                    self.val_idx += 1;
                }
                self.key_idx += 1;
            }
            self.outer += 1;
        }
    }
}

impl<'a, K: Columnar, V: Columnar, T: Columnar, R: Columnar> Iterator
    for FactColumnDrain<'a, K, V, T, R>
{
    type Item = (
        columnar::Ref<'a, K>,
        columnar::Ref<'a, V>,
        columnar::Ref<'a, T>,
        columnar::Ref<'a, R>,
    );

    fn next(&mut self) -> Option<Self::Item> {
        if self.leaf_idx >= self.leaf_end && self.outer >= self.outer_end {
            return None;
        }
        let k = self.k_lists.values.get(self.key_idx);
        let v = self.v_lists.values.get(self.val_idx);
        let t = self.leaf_lists.values.0.get(self.leaf_idx);
        let r = self.leaf_lists.values.1.get(self.leaf_idx);
        self.advance_next();
        Some((k, v, t, r))
    }
}

impl<K: Columnar, V: Columnar, T: Columnar, R: Columnar> ContainerBytes for FactColumn<K, V, T, R> {
    #[inline]
    fn from_bytes(bytes: Bytes) -> Self {
        assert_eq!(bytes.len() % 8, 0);
        if bytemuck::try_cast_slice::<_, u64>(&bytes).is_ok() {
            Self::Bytes(bytes)
        } else {
            let mut alloc: Region<u64> = crate::containers::alloc_aligned_zeroed(bytes.len() / 8);
            let alloc_bytes = bytemuck::cast_slice_mut(&mut alloc);
            alloc_bytes[..bytes.len()].copy_from_slice(&bytes);
            Self::Align(alloc)
        }
    }

    #[inline]
    fn length_in_bytes(&self) -> usize {
        match self {
            FactColumn::Typed(t) => indexed::length_in_bytes(&t.borrowed()),
            FactColumn::Bytes(b) => b.len(),
            FactColumn::Align(a) => 8 * a.len(),
        }
    }

    #[inline]
    fn into_bytes<W: std::io::Write>(&self, writer: &mut W) {
        match self {
            FactColumn::Typed(t) => {
                indexed::write(writer, &t.borrowed()).unwrap();
            }
            FactColumn::Bytes(b) => writer.write_all(b).unwrap(),
            FactColumn::Align(a) => writer.write_all(bytemuck::cast_slice(a)).unwrap(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use timely::Accountable;
    use timely::bytes::arc::BytesMut;

    /// Helper: create `Bytes` from a `Vec<u8>`.
    fn bytes_from_vec(v: Vec<u8>) -> Bytes {
        BytesMut::from(v).freeze()
    }

    #[mz_ore::test]
    fn test_fact_column_push_drain_roundtrip() {
        let mut col = FactColumn::<u64, u64, u64, i64>::default();
        col.push_into(((1u64, 10u64), 100u64, 1i64));
        col.push_into(((1u64, 20u64), 200u64, -1i64));
        col.push_into(((2u64, 30u64), 300u64, 2i64));

        assert_eq!(col.record_count(), 3);

        let items: Vec<_> = col.drain().map(|(k, v, t, r)| (*k, *v, *t, *r)).collect();
        assert_eq!(
            items,
            vec![(1, 10, 100, 1), (1, 20, 200, -1), (2, 30, 300, 2)]
        );
    }

    #[mz_ore::test]
    fn test_fact_column_container_bytes_roundtrip() {
        let mut col = FactColumn::<u64, u64, u64, i64>::default();
        col.push_into(((1u64, 10u64), 100u64, 1i64));
        col.push_into(((2u64, 20u64), 200u64, -1i64));

        // Serialize.
        let len = col.length_in_bytes();
        let mut buf = Vec::with_capacity(len);
        col.into_bytes(&mut buf);
        assert_eq!(buf.len(), len);

        // Deserialize.
        let bytes = bytes_from_vec(buf);
        let mut col2 = FactColumn::<u64, u64, u64, i64>::from_bytes(bytes);

        assert_eq!(col2.record_count(), 2);
        let items: Vec<_> = col2.drain().map(|(k, v, t, r)| (*k, *v, *t, *r)).collect();
        assert_eq!(items, vec![(1, 10, 100, 1), (2, 20, 200, -1)]);
    }

    #[mz_ore::test]
    fn test_fact_column_empty() {
        let mut col = FactColumn::<u64, u64, u64, i64>::default();
        assert_eq!(col.record_count(), 0);
        assert!(col.drain().next().is_none());

        // Empty serialization roundtrip.
        let len = col.length_in_bytes();
        let mut buf = Vec::with_capacity(len);
        col.into_bytes(&mut buf);
        let bytes = bytes_from_vec(buf);
        let mut col2 = FactColumn::<u64, u64, u64, i64>::from_bytes(bytes);
        assert_eq!(col2.record_count(), 0);
        assert!(col2.drain().next().is_none());
    }

    #[mz_ore::test]
    fn test_fact_column_clone() {
        let mut col = FactColumn::<u64, u64, u64, i64>::default();
        col.push_into(((1u64, 10u64), 100u64, 1i64));

        // Clone typed.
        let mut cloned = col.clone();
        let items: Vec<_> = cloned
            .drain()
            .map(|(k, v, t, r)| (*k, *v, *t, *r))
            .collect();
        assert_eq!(items, vec![(1, 10, 100, 1)]);

        // Serialize, then clone the bytes variant.
        let len = col.length_in_bytes();
        let mut buf = Vec::with_capacity(len);
        col.into_bytes(&mut buf);
        let bytes_col = FactColumn::<u64, u64, u64, i64>::from_bytes(bytes_from_vec(buf));
        let mut cloned2 = bytes_col.clone();
        let items2: Vec<_> = cloned2
            .drain()
            .map(|(k, v, t, r)| (*k, *v, *t, *r))
            .collect();
        assert_eq!(items2, vec![(1, 10, 100, 1)]);
    }
}
