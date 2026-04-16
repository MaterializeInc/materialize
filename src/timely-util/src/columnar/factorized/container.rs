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

//! Columnar-backed [`BatchContainer`] implementation.
//!
//! [`Coltainer`] wraps a columnar [`Container`](columnar::Container) and provides
//! the [`BatchContainer`] interface required by differential-dataflow's trace
//! infrastructure. Indexed access uses columnar's [`Borrow`](columnar::Borrow) +
//! [`Index`](columnar::Index) traits.

use columnar::{Borrow, Columnar, Container, Index, Len};
use differential_dataflow::trace::implementations::BatchContainer;

/// A columnar-backed [`BatchContainer`].
///
/// Wraps `C::Container` and provides [`BatchContainer`] access via columnar's
/// `Borrow` + `Index` traits.
pub struct Coltainer<C: Columnar> {
    /// The underlying columnar container.
    pub container: C::Container,
}

impl<C: Columnar> Default for Coltainer<C> {
    fn default() -> Self {
        Self {
            container: Default::default(),
        }
    }
}

impl<C> BatchContainer for Coltainer<C>
where
    C: Columnar + Ord + Clone,
    for<'a> columnar::Ref<'a, C>: Ord + Copy,
{
    type ReadItem<'a> = columnar::Ref<'a, C>;
    type Owned = C;

    fn reborrow<'b, 'a: 'b>(item: Self::ReadItem<'a>) -> Self::ReadItem<'b> {
        C::reborrow(item)
    }

    fn into_owned<'a>(item: Self::ReadItem<'a>) -> Self::Owned {
        C::into_owned(item)
    }

    fn clone_onto<'a>(item: Self::ReadItem<'a>, other: &mut Self::Owned) {
        other.copy_from(item);
    }

    fn push_ref(&mut self, item: Self::ReadItem<'_>) {
        columnar::Push::push(&mut self.container, item);
    }

    fn push_own(&mut self, item: &Self::Owned) {
        columnar::Push::push(&mut self.container, item);
    }

    fn clear(&mut self) {
        columnar::Clear::clear(&mut self.container);
    }

    fn with_capacity(_size: usize) -> Self {
        // Columnar containers don't support capacity hints in the general case.
        Self::default()
    }

    fn merge_capacity(cont1: &Self, cont2: &Self) -> Self {
        Self {
            container: <C::Container>::with_capacity_for(
                [cont1.container.borrow(), cont2.container.borrow()].into_iter(),
            ),
        }
    }

    fn index(&self, index: usize) -> Self::ReadItem<'_> {
        self.container.borrow().get(index)
    }

    fn len(&self) -> usize {
        Len::len(&self.container.borrow())
    }

    fn advance<F: for<'a> Fn(Self::ReadItem<'a>) -> bool>(
        &self,
        start: usize,
        end: usize,
        function: F,
    ) -> usize {
        // Galloping search: find the first position in [start..end) where function returns false.
        let small_limit = 8;
        let borrowed = self.container.borrow();

        // Linear scan for small ranges.
        if end - start < small_limit {
            let mut index = start;
            while index < end && function(borrowed.get(index)) {
                index += 1;
            }
            return index - start;
        }

        // Galloping: double step size until function returns false.
        let mut step = 1;
        let mut index = start;
        while index + step < end && function(borrowed.get(index + step)) {
            index += step;
            step <<= 1;
        }
        // Binary search within [index, min(index + step, end)).
        step = std::cmp::min(step, end - index);
        while step > 1 {
            let half = step / 2;
            if index + half < end && function(borrowed.get(index + half)) {
                index += half;
            }
            step -= half;
        }
        // Move past the last element where function was true.
        if index < end && function(borrowed.get(index)) {
            index += 1;
        }
        index - start
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn test_coltainer_push_index_roundtrip() {
        let mut c = Coltainer::<u64>::default();
        c.push_own(&10u64);
        c.push_own(&20u64);
        c.push_own(&30u64);

        assert_eq!(c.len(), 3);
        assert_eq!(*c.index(0), 10);
        assert_eq!(*c.index(1), 20);
        assert_eq!(*c.index(2), 30);
    }

    #[mz_ore::test]
    fn test_coltainer_into_owned() {
        let mut c = Coltainer::<u64>::default();
        c.push_own(&42u64);

        let item = c.index(0);
        let owned = Coltainer::<u64>::into_owned(item);
        assert_eq!(owned, 42u64);
    }

    #[mz_ore::test]
    fn test_coltainer_advance_galloping() {
        let mut c = Coltainer::<u64>::default();
        for i in 0..100u64 {
            c.push_own(&i);
        }

        // Advance past elements < 50.
        let count = c.advance(0, 100, |x| *x < 50);
        assert_eq!(count, 50);

        // Advance from middle.
        let count = c.advance(30, 100, |x| *x < 50);
        assert_eq!(count, 20);

        // Advance past all.
        let count = c.advance(0, 100, |_| true);
        assert_eq!(count, 100);

        // Advance past none.
        let count = c.advance(0, 100, |_| false);
        assert_eq!(count, 0);
    }

    #[mz_ore::test]
    fn test_coltainer_merge_capacity() {
        let mut c1 = Coltainer::<u64>::default();
        let mut c2 = Coltainer::<u64>::default();
        c1.push_own(&1u64);
        c2.push_own(&2u64);

        let merged = Coltainer::<u64>::merge_capacity(&c1, &c2);
        assert_eq!(merged.len(), 0); // Capacity only, no elements.
    }
}
