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

//! Iterator utilities.

use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
use std::collections::binary_heap::PeekMut;
use std::fmt::Debug;
use std::iter::{self, Chain, Once, Peekable};
use std::rc::Rc;

/// Extension methods for iterators.
pub trait IteratorExt
where
    Self: Iterator + Sized,
{
    /// Chains a single `item` onto the end of this iterator.
    ///
    /// Equivalent to `self.chain(iter::once(item))`.
    fn chain_one(self, item: Self::Item) -> Chain<Self, Once<Self::Item>> {
        self.chain(iter::once(item))
    }

    /// Reports whether all the elements of the iterator are the same.
    ///
    /// This condition is trivially true for iterators with zero or one elements.
    fn all_equal(mut self) -> bool
    where
        Self::Item: PartialEq,
    {
        match self.next() {
            None => true,
            Some(v1) => self.all(|v2| v1 == v2),
        }
    }

    /// Converts the the iterator into an `ExactSizeIterator` reporting the given size.
    ///
    /// The caller is responsible for providing the correct size of the iterator! Providing an
    /// incorrect size value will lead to panics and/or incorrect responses to size queries.
    ///
    /// # Panics
    ///
    /// Panics if the given length is not consistent with this iterator's `size_hint`.
    fn exact_size(self, len: usize) -> ExactSize<Self> {
        let (lower, upper) = self.size_hint();
        assert!(
            lower <= len && upper.map_or(true, |upper| upper >= len),
            "provided length {len} inconsistent with `size_hint`: {:?}",
            (lower, upper)
        );

        ExactSize { inner: self, len }
    }

    /// Wrap this iterator with one that yields a tuple of the iterator element and the extra
    /// value on each iteration. The extra value is cloned for each but the last `Some` element
    /// returned.
    ///
    /// This is useful to provide an owned extra value to each iteration, but only clone it
    /// when necessary.
    ///
    /// NOTE: Once the iterator starts producing `None` values, the extra value will be consumed
    /// and no longer be available. This should not be used for iterators that may produce
    /// `Some` values after producing `None`.
    fn repeat_clone<A: Clone>(self, extra_val: A) -> RepeatClone<Self, A> {
        RepeatClone {
            iter: self.peekable(),
            extra_val: Some(extra_val),
        }
    }
}

impl<I> IteratorExt for I where I: Iterator {}

/// Proactively consolidate adjacent elements in an iterator.
///
/// If the input iterator is sorted by `D`, the outputs will be both sorted and fully consolidated.
/// If the input is not sorted, the output will probably not be either... but still equivalent from
/// a differential perspective and slightly smaller.
#[cfg(feature = "differential-dataflow")]
pub fn consolidate_iter<D: PartialEq, R: differential_dataflow::difference::Semigroup>(
    iter: impl Iterator<Item = (D, R)>,
) -> impl Iterator<Item = (D, R)> {
    let mut peekable = iter.peekable();
    iter::from_fn(move || {
        loop {
            let (t, mut d) = peekable.next()?;
            while let Some((t_next, d_next)) = peekable.peek()
                && t == *t_next
            {
                d.plus_equals(d_next);
                let _ = peekable.next();
            }
            if d.is_zero() {
                continue;
            }
            return Some((t, d));
        }
    })
}

/// Proactively consolidate adjacent elements in an iterator. (Triple / update edition.)
///
/// If the input iterator is sorted by `D` and `T`, the outputs will be both sorted and fully consolidated.
/// If the input is not sorted, the output will probably not be either... but still equivalent from
/// a differential perspective and slightly smaller.
#[cfg(feature = "differential-dataflow")]
pub fn consolidate_update_iter<
    D: PartialEq,
    T: PartialEq,
    R: differential_dataflow::difference::Semigroup,
>(
    iter: impl Iterator<Item = (D, T, R)>,
) -> impl Iterator<Item = (D, T, R)> {
    consolidate_iter(iter.map(|(d, t, r)| ((d, t), r))).map(|((d, t), r)| (d, t, r))
}

/// Combine a stream of iterators into a new iterator, according to the provided merge function.
///
/// If the input iterators are sorted by the provided function, the resulting iterators will be
/// sorted by that function also.
pub fn merge_iters_by<I: Iterator, F: Fn(&I::Item, &I::Item) -> Ordering>(
    iters: impl IntoIterator<Item = I>,
    merge_by: F,
) -> impl Iterator<Item = I::Item> {
    /// Iterator-like struct to help with extracting rows in sorted order from `RowCollection`.
    struct RunIter<I: Iterator, F> {
        iter: I,
        peek: <I as Iterator>::Item,
        merge_by: Rc<F>,
    }

    impl<I: Iterator, F: Fn(&I::Item, &I::Item) -> Ordering> Ord for RunIter<I, F> {
        fn cmp(&self, other: &Self) -> Ordering {
            (self.merge_by)(&self.peek, &other.peek)
        }
    }

    impl<I: Iterator, F: Fn(&I::Item, &I::Item) -> Ordering> PartialOrd for RunIter<I, F> {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            Some(self.cmp(other))
        }
    }
    impl<I: Iterator, F: Fn(&I::Item, &I::Item) -> Ordering> PartialEq for RunIter<I, F> {
        fn eq(&self, other: &Self) -> bool {
            self.cmp(other).is_eq()
        }
    }

    impl<I: Iterator, F: Fn(&I::Item, &I::Item) -> Ordering> Eq for RunIter<I, F> {}

    let iters = iters.into_iter();
    let mut heap = BinaryHeap::with_capacity(iters.size_hint().0);
    let merge_by = Rc::new(merge_by);
    for mut i in iters {
        let Some(peek) = i.next() else {
            continue;
        };
        heap.push(Reverse(RunIter {
            peek,
            iter: i,
            merge_by: Rc::clone(&merge_by),
        }));
    }

    iter::from_fn(move || {
        let mut peek_mut = heap.peek_mut()?;
        let next = match peek_mut.0.iter.next() {
            None => PeekMut::pop(peek_mut).0.peek,
            Some(next) => std::mem::replace(&mut peek_mut.0.peek, next),
        };
        Some(next)
    })
}

/// Iterator type returned by [`IteratorExt::exact_size`].
#[derive(Debug)]
pub struct ExactSize<I> {
    inner: I,
    len: usize,
}

impl<I: Iterator> Iterator for ExactSize<I> {
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        self.len = self.len.saturating_sub(1);
        self.inner.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.len, Some(self.len))
    }
}

impl<I: Iterator> ExactSizeIterator for ExactSize<I> {}

/// Iterator type returned by [`IteratorExt::repeat_clone`].
pub struct RepeatClone<I: Iterator, A> {
    iter: Peekable<I>,
    extra_val: Option<A>,
}

impl<I: Iterator, A: Clone> Iterator for RepeatClone<I, A> {
    type Item = (I::Item, A);

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.iter.next()?;

        // Clone the extra_val only if there is an item to return on the next call to `next`.
        let val = match self.iter.peek() {
            Some(_) => self.extra_val.clone(),
            None => self.extra_val.take(),
        };

        // We should always return a value if there is a current element.
        Some((next, val.expect("RepeatClone invariant violated")))
    }
}

impl<I: Iterator<Item: Debug> + Debug, A: Debug> Debug for RepeatClone<I, A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RepeatClone")
            .field("iter", &self.iter)
            .field("extra_val", &self.extra_val)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use proptest::collection::vec;
    use proptest::prelude::*;

    use super::*;

    #[crate::test]
    fn test_all_equal() {
        let empty: [i64; 0] = [];
        assert!(empty.iter().all_equal());
        assert!([1].iter().all_equal());
        assert!([1, 1].iter().all_equal());
        assert!(![1, 2].iter().all_equal());
    }

    #[crate::test]
    #[cfg(feature = "differential-dataflow")]
    fn test_consolidate_sorted() {
        proptest!(|(mut data in any::<Vec<(u64, i64)>>())| {
             data.sort();
             let streamed: Vec<_> = consolidate_iter(data.iter().copied()).collect();
             differential_dataflow::consolidation::consolidate(&mut data);
             assert_eq!(data, streamed);
        });
    }

    #[crate::test]
    #[cfg(feature = "differential-dataflow")]
    fn test_consolidate() {
        proptest!(|(mut data in any::<Vec<(u64, i64)>>())| {
             let streamed: Vec<_> = consolidate_iter(data.iter().copied()).collect();
             data.dedup_by_key(|t| t.0);
             assert_eq!(data.len(), streamed.len());
        });
    }

    #[crate::test]
    fn test_merge() {
        proptest!(|(mut data in vec(vec(0usize..100usize, 0..10), 0..10))| {
            let mut expected: Vec<_> = data.iter().flatten().copied().collect();
            expected.sort();

            for series in &mut data {
                series.sort()
            }
            let merged: Vec<_> = merge_iters_by(
                data.into_iter().map(|i| i.into_iter()),
                |a, b| a.cmp(b)
            ).collect();
             assert_eq!(expected, merged);
        });
    }
}
