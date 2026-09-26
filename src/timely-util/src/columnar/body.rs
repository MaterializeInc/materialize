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

//! [`ColumnBody`]: a columnar chunk body at rest.
//!
//! [`Column`] is the container on dataflow edges: it is built by pushing, crosses an exchange as
//! channel bytes, and is read on arrival. The chunk machinery behind an edge holds bodies instead:
//! sorted, consolidated runs that a merge batcher chains, that a spill-backed chunk keeps on the
//! heap or in the pool, and that a batch builder seals. A body is typed while it is written and
//! serialized when a store handed it back, and nothing else. It never holds channel bytes, and
//! where it lives is the business of whatever holds it.

use std::io::Write;

use columnar::bytes::indexed;
use columnar::common::IterOwn;
use columnar::{Borrow, BorrowedOf, Clear, Columnar, Container as _, FromBytes, Index, Len, Ref};
use timely::Accountable;
use timely::container::{DrainContainer, PushInto};

use crate::columnar::{Column, at_serialized_capacity};

/// A sorted, consolidated run of columnar records, typed while written and serialized once read
/// back from a store.
pub enum ColumnBody<C: Columnar> {
    /// The typed containers, which is what a body is while it is written to.
    Typed(C::Container),
    /// The serialized form, as a store hands a body back: `u64`-aligned words holding the
    /// [`columnar::bytes::indexed`] encoding.
    Words(Vec<u64>),
}

impl<C: Columnar> Default for ColumnBody<C> {
    fn default() -> Self {
        Self::Typed(Default::default())
    }
}

impl<C: Columnar> Clone for ColumnBody<C>
where
    C::Container: Clone,
{
    fn clone(&self) -> Self {
        match self {
            ColumnBody::Typed(typed) => ColumnBody::Typed(typed.clone()),
            ColumnBody::Words(words) => ColumnBody::Words(words.clone()),
        }
    }
}

impl<C: Columnar> ColumnBody<C> {
    /// Borrows the body as a columnar view.
    ///
    /// The serialized form rebuilds its view from the encoded header on every call, so a caller
    /// that reads more than one record hoists the view out of its loop.
    #[inline(always)]
    pub fn borrow(&self) -> BorrowedOf<'_, C> {
        match self {
            ColumnBody::Typed(typed) => typed.borrow(),
            ColumnBody::Words(words) => borrow_words::<C>(words),
        }
    }

    /// The number of records.
    #[inline]
    pub fn len(&self) -> usize {
        match self {
            ColumnBody::Typed(typed) => typed.len(),
            ColumnBody::Words(words) => borrow_words::<C>(words).len(),
        }
    }

    /// True when the body holds no records.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The size of the serialized form in bytes, whether or not the body is serialized.
    pub fn length_in_bytes(&self) -> usize {
        match self {
            ColumnBody::Typed(typed) => indexed::length_in_bytes(&typed.borrow()),
            ColumnBody::Words(words) => words.len() * std::mem::size_of::<u64>(),
        }
    }

    /// Writes the serialized form, exactly [`ColumnBody::length_in_bytes`] bytes of it.
    ///
    /// The serialized form is written as it is, so a body that went through a store and back
    /// round-trips byte-identically.
    pub fn write_into<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        match self {
            ColumnBody::Typed(typed) => indexed::write(writer, &typed.borrow()),
            ColumnBody::Words(words) => writer.write_all(bytemuck::cast_slice(words)),
        }
    }

    /// The typed containers, for writing to.
    ///
    /// A serialized body is copied into typed containers first, since the serialized form cannot
    /// take pushes. The copy is a bulk per-leaf extension.
    pub fn typed_mut(&mut self) -> &mut C::Container {
        if let ColumnBody::Words(words) = &*self {
            let typed = copy_typed::<C>(borrow_words::<C>(words));
            *self = ColumnBody::Typed(typed);
        }
        let ColumnBody::Typed(typed) = self else {
            unreachable!("a serialized body was materialized above");
        };
        typed
    }

    /// The typed containers, taking a typed body as it is and copying a serialized one.
    pub fn into_typed(self) -> C::Container {
        match self {
            ColumnBody::Typed(typed) => typed,
            ColumnBody::Words(words) => copy_typed::<C>(borrow_words::<C>(&words)),
        }
    }

    /// A fresh typed copy of the body, by bulk per-leaf extension.
    pub fn copy_typed(&self) -> C::Container {
        copy_typed::<C>(self.borrow())
    }

    /// Empties the body, keeping a typed body's allocations for refilling.
    ///
    /// A serialized body owns no typed allocation, so it becomes an empty typed body.
    #[inline]
    pub fn clear(&mut self) {
        match self {
            ColumnBody::Typed(typed) => typed.clear(),
            ColumnBody::Words(_) => *self = Default::default(),
        }
    }

    /// True once the body is at the ship size (see [`at_serialized_capacity`]).
    ///
    /// A serialized body is complete, so it is always at capacity.
    #[inline]
    pub fn at_capacity(&self) -> bool {
        match self {
            ColumnBody::Typed(typed) => at_serialized_capacity(&typed.borrow()),
            ColumnBody::Words(_) => true,
        }
    }
}

/// Reconstructs the borrowed columnar view from serialized words, the same zero-copy decode
/// [`Column::borrow`] performs on its `Align` variant.
pub fn borrow_words<C: Columnar>(words: &[u64]) -> BorrowedOf<'_, C> {
    <BorrowedOf<'_, C>>::from_bytes(&mut indexed::decode(words))
}

/// Copies a view into fresh typed containers by bulk per-leaf extension.
fn copy_typed<C: Columnar>(view: BorrowedOf<'_, C>) -> C::Container {
    let mut fresh = C::Container::default();
    fresh.extend_from_self(view, 0..view.len());
    fresh
}

impl<C: Columnar> Accountable for ColumnBody<C> {
    #[inline]
    fn record_count(&self) -> i64 {
        i64::try_from(self.len()).expect("record count fits i64")
    }
}

impl<C: Columnar> DrainContainer for ColumnBody<C> {
    type Item<'a> = Ref<'a, C>;
    type DrainIter<'a> = IterOwn<BorrowedOf<'a, C>>;
    #[inline]
    fn drain(&mut self) -> Self::DrainIter<'_> {
        self.borrow().into_index_iter()
    }
}

impl<C: Columnar, T> PushInto<T> for ColumnBody<C>
where
    C::Container: columnar::Push<T>,
{
    #[inline]
    fn push_into(&mut self, item: T) {
        use columnar::Push;
        self.typed_mut().push(item);
    }
}

/// A body leaves the edge container behind: typed data moves, and channel bytes are relocated
/// into owned words, since a body never holds a channel's allocation.
impl<C: Columnar> From<Column<C>> for ColumnBody<C> {
    fn from(column: Column<C>) -> Self {
        match column {
            Column::Typed(typed) => ColumnBody::Typed(typed),
            Column::Bytes(bytes) => {
                assert_eq!(bytes.len() % 8, 0);
                ColumnBody::Words(bytemuck::allocation::pod_collect_to_vec(&bytes))
            }
            Column::Align(words) => ColumnBody::Words(words),
        }
    }
}

/// A body goes back onto an edge as it is: both of its forms are forms of the edge container.
impl<C: Columnar> From<ColumnBody<C>> for Column<C> {
    fn from(body: ColumnBody<C>) -> Self {
        match body {
            ColumnBody::Typed(typed) => Column::Typed(typed),
            ColumnBody::Words(words) => Column::Align(words),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn body(values: &[i32]) -> ColumnBody<i32> {
        let mut body: ColumnBody<i32> = Default::default();
        for value in values {
            body.push_into(*value);
        }
        body
    }

    fn collect(body: &ColumnBody<i32>) -> Vec<i32> {
        body.borrow().into_index_iter().copied().collect()
    }

    fn serialized(body: &ColumnBody<i32>) -> ColumnBody<i32> {
        let mut bytes = Vec::new();
        body.write_into(&mut bytes).expect("vec writes");
        assert_eq!(bytes.len(), body.length_in_bytes());
        ColumnBody::Words(bytemuck::allocation::pod_collect_to_vec(&bytes))
    }

    #[mz_ore::test]
    fn serialized_body_reads_and_measures_like_typed() {
        let typed = body(&[1, 2, 3]);
        let words = serialized(&typed);
        assert_eq!(collect(&words), vec![1, 2, 3]);
        assert_eq!(words.len(), 3);
        assert_eq!(words.length_in_bytes(), typed.length_in_bytes());
        assert!(words.at_capacity(), "a serialized body is complete");
    }

    #[mz_ore::test]
    fn serialized_body_round_trips_byte_identically() {
        let words = serialized(&body(&[4, 5, 6]));
        let again = serialized(&words);
        let (ColumnBody::Words(a), ColumnBody::Words(b)) = (&words, &again) else {
            panic!("serialized bodies are words");
        };
        assert_eq!(a, b);
    }

    #[mz_ore::test]
    fn writing_a_serialized_body_materializes_it() {
        let mut words = serialized(&body(&[7, 8]));
        words.push_into(9);
        assert!(matches!(words, ColumnBody::Typed(_)));
        assert_eq!(collect(&words), vec![7, 8, 9]);
    }

    #[mz_ore::test]
    fn clearing_keeps_a_typed_body_typed() {
        let mut typed = body(&[1]);
        typed.clear();
        assert!(matches!(typed, ColumnBody::Typed(_)));
        assert!(typed.is_empty());
        let mut words = serialized(&body(&[1]));
        words.clear();
        assert!(matches!(words, ColumnBody::Typed(_)));
        assert!(words.is_empty());
    }

    #[mz_ore::test]
    fn edge_conversions_move_typed_and_relocate_bytes() {
        let column: Column<i32> = Column::from(body(&[1, 2]));
        assert!(matches!(column, Column::Typed(_)));
        let column: Column<i32> = Column::from(serialized(&body(&[1, 2])));
        assert!(matches!(column, Column::Align(_)));
        let back = ColumnBody::from(column);
        assert!(matches!(back, ColumnBody::Words(_)));
        assert_eq!(collect(&back), vec![1, 2]);
    }
}
