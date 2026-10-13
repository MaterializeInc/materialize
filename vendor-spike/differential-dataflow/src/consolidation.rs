//! Common logic for the consolidation of vectors of Semigroups.
//!
//! Often we find ourselves with collections of records with associated weights (often
//! integers) where we want to reduce the collection to the point that each record occurs
//! at most once, with the accumulated weights. These methods supply that functionality.
//!
//! Importantly, these methods are used internally by differential dataflow, but are made
//! public for the convenience of others. Their precise behavior is driven by the needs of
//! differential dataflow (chiefly: canonicalizing sequences of non-zero updates); should
//! you need specific behavior, it may be best to defensively copy, paste, and maintain the
//! specific behavior you require.

use std::collections::VecDeque;
use timely::container::{ContainerBuilder, PushInto};
use crate::Data;
use crate::difference::Semigroup;

/// Sorts and consolidates `vec`.
///
/// This method will sort `vec` and then consolidate runs of more than one entry with
/// identical first elements by accumulating the second elements of the pairs. Should the final
/// accumulation be zero, the element is discarded.
#[inline]
pub fn consolidate<T: Ord, R: Semigroup>(vec: &mut Vec<(T, R)>) {
    consolidate_from(vec, 0);
}

/// Sorts and consolidate `vec[offset..]`.
///
/// This method will sort `vec[offset..]` and then consolidate runs of more than one entry with
/// identical first elements by accumulating the second elements of the pairs. Should the final
/// accumulation be zero, the element is discarded.
#[inline]
pub fn consolidate_from<T: Ord, R: Semigroup>(vec: &mut Vec<(T, R)>, offset: usize) {
    let length = consolidate_slice(&mut vec[offset..]);
    vec.truncate(offset + length);
}

/// Sorts and consolidates a slice, returning the valid prefix length.
#[inline]
pub fn consolidate_slice<T: Ord, R: Semigroup>(slice: &mut [(T, R)]) -> usize {
    if slice.len() > 1 {
        consolidate_slice_slow(slice)
    }
    else {
        slice.iter().filter(|x| !x.1.is_zero()).count()
    }
}

/// Part of `consolidate_slice` that handles slices of length greater than 1.
fn consolidate_slice_slow<T: Ord, R: Semigroup>(slice: &mut [(T, R)]) -> usize {
    // We could do an insertion-sort like initial scan which builds up sorted, consolidated runs.
    // In a world where there are not many results, we may never even need to call in to merge sort.
    slice.sort_by(|x,y| x.0.cmp(&y.0));

    // Counts the number of distinct known-non-zero accumulations. Indexes the write location.
    let mut offset = 0;
    let mut accum = slice[offset].1.clone();

    for index in 1 .. slice.len() {
        if slice[index].0 == slice[index-1].0 {
            accum.plus_equals(&slice[index].1);
        }
        else {
            if !accum.is_zero() {
                slice.swap(offset, index-1);
                slice[offset].1.clone_from(&accum);
                offset += 1;
            }
            accum.clone_from(&slice[index].1);
        }
    }
    if !accum.is_zero() {
        slice.swap(offset, slice.len()-1);
        slice[offset].1 = accum;
        offset += 1;
    }

    offset
}

/// Sorts and consolidates `vec`.
///
/// This method will sort `vec` and then consolidate runs of more than one entry with
/// identical first two elements by accumulating the third elements of the triples. Should the final
/// accumulation be zero, the element is discarded.
#[inline]
pub fn consolidate_updates<D: Ord, T: Ord, R: Semigroup>(vec: &mut Vec<(D, T, R)>) {
    consolidate_updates_from(vec, 0);
}

/// Sorts and consolidate `vec[offset..]`.
///
/// This method will sort `vec[offset..]` and then consolidate runs of more than one entry with
/// identical first two elements by accumulating the third elements of the triples. Should the final
/// accumulation be zero, the element is discarded.
#[inline]
pub fn consolidate_updates_from<D: Ord, T: Ord, R: Semigroup>(vec: &mut Vec<(D, T, R)>, offset: usize) {
    let length = consolidate_updates_slice(&mut vec[offset..]);
    vec.truncate(offset + length);
}

/// Sorts and consolidates a slice, returning the valid prefix length.
#[inline]
pub fn consolidate_updates_slice<D: Ord, T: Ord, R: Semigroup>(slice: &mut [(D, T, R)]) -> usize {

    if slice.len() > 1 {
        consolidate_updates_slice_slow(slice)
    }
    else {
        slice.iter().filter(|x| !x.2.is_zero()).count()
    }
}

/// Part of `consolidate_updates_slice` that handles slices of length greater than 1.
fn consolidate_updates_slice_slow<D: Ord, T: Ord, R: Semigroup>(slice: &mut [(D, T, R)]) -> usize {
    // We could do an insertion-sort like initial scan which builds up sorted, consolidated runs.
    // In a world where there are not many results, we may never even need to call in to merge sort.
    slice.sort_unstable_by(|x,y| (&x.0, &x.1).cmp(&(&y.0, &y.1)));

    // Counts the number of distinct known-non-zero accumulations. Indexes the write location.
    let mut offset = 0;
    let mut accum = slice[offset].2.clone();

    for index in 1 .. slice.len() {
        if (slice[index].0 == slice[index-1].0) && (slice[index].1 == slice[index-1].1) {
            accum.plus_equals(&slice[index].2);
        }
        else {
            if !accum.is_zero() {
                slice.swap(offset, index-1);
                slice[offset].2.clone_from(&accum);
                offset += 1;
            }
            accum.clone_from(&slice[index].2);
        }
    }
    if !accum.is_zero() {
        slice.swap(offset, slice.len()-1);
        slice[offset].2 = accum;
        offset += 1;
    }

    offset
}


/// A container builder that consolidates data in-places into fixed-sized containers. Does not
/// maintain FIFO ordering.
#[derive(Default)]
pub struct ConsolidatingContainerBuilder<C>{
    current: C,
    empty: Vec<C>,
    outbound: VecDeque<C>,
}

impl<D,T,R> ConsolidatingContainerBuilder<Vec<(D, T, R)>>
where
    D: Data,
    T: Data,
    R: Semigroup+'static,
{
    /// Flush `self.current` up to the biggest `multiple` of elements. Pass 1 to flush all elements.
    // TODO: Can we replace `multiple` by a bool?
    #[cold]
    fn consolidate_and_flush_through(&mut self, multiple: usize) {
        let preferred_capacity = timely::container::buffer::default_capacity::<(D, T, R)>();
        consolidate_updates(&mut self.current);
        let mut drain = self.current.drain(..(self.current.len()/multiple)*multiple).peekable();
        while drain.peek().is_some() {
            let mut container = self.empty.pop().unwrap_or_else(|| Vec::with_capacity(preferred_capacity));
            container.clear();
            container.extend((&mut drain).take(preferred_capacity));
            self.outbound.push_back(container);
        }
    }
}

impl<D, T, R, P> PushInto<P> for ConsolidatingContainerBuilder<Vec<(D, T, R)>>
where
    D: Data,
    T: Data,
    R: Semigroup+'static,
    Vec<(D, T, R)>: PushInto<P>,
{
    /// Push an element.
    ///
    /// Precondition: `current` is not allocated or has space for at least one element.
    #[inline]
    fn push_into(&mut self, item: P) {
        let preferred_capacity = timely::container::buffer::default_capacity::<(D, T, R)>();
        if self.current.capacity() < preferred_capacity * 2 {
            self.current.reserve(preferred_capacity * 2 - self.current.capacity());
        }
        self.current.push_into(item);
        if self.current.len() == self.current.capacity() {
            // Flush complete containers.
            self.consolidate_and_flush_through(preferred_capacity);
        }
    }
}

impl<D,T,R> ContainerBuilder for ConsolidatingContainerBuilder<Vec<(D, T, R)>>
where
    D: Data,
    T: Data,
    R: Semigroup+'static,
{
    type Container = Vec<(D,T,R)>;

    #[inline]
    fn extract(&mut self) -> Option<&mut Vec<(D,T,R)>> {
        if let Some(container) = self.outbound.pop_front() {
            self.empty.push(container);
            self.empty.last_mut()
        } else {
            None
        }
    }

    #[inline]
    fn finish(&mut self) -> Option<&mut Vec<(D,T,R)>> {
        if !self.current.is_empty() {
            // Flush all
            self.consolidate_and_flush_through(1);
            // Remove all but two elements from the stash of empty to avoid memory leaks. We retain
            // two to match `current` capacity.
            self.empty.truncate(2);
        }
        self.extract()
    }
}

/// A container that can sort and consolidate its contents internally.
///
/// The container knows its own layout — how to sort its elements, how to
/// compare adjacent entries, and how to merge diffs. The caller provides
/// a `target` container to receive the consolidated output, allowing
/// reuse of allocations across calls.
///
/// After the call, `target` contains the sorted, consolidated data and
/// `self` may be empty or in an unspecified state (implementations should
/// document this).
pub trait Consolidate {
    /// The number of elements in the container.
    fn len(&self) -> usize;
    /// Clear the container.
    fn clear(&mut self);
    /// Sort and consolidate `self` into `target`.
    fn consolidate_into(&mut self, target: &mut Self);
}

impl<D: Ord, T: Ord, R: Semigroup> Consolidate for Vec<(D, T, R)> {
    fn len(&self) -> usize { Vec::len(self) }
    fn clear(&mut self) { Vec::clear(self) }
    fn consolidate_into(&mut self, target: &mut Self) {
        consolidate_updates(self);
        std::mem::swap(self, target);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_consolidate() {
        let test_cases = vec![
            (
                vec![("a", -1), ("b", -2), ("a", 1)],
                vec![("b", -2)],
            ),
            (
                vec![("a", -1), ("b", 0), ("a", 1)],
                vec![],
            ),
            (
                vec![("a", 0)],
                vec![],
            ),
            (
                vec![("a", 0), ("b", 0)],
                vec![],
            ),
            (
                vec![("a", 1), ("b", 1)],
                vec![("a", 1), ("b", 1)],
            ),
        ];

        for (mut input, output) in test_cases {
            consolidate(&mut input);
            assert_eq!(input, output);
        }
    }


    #[test]
    fn test_consolidate_updates() {
        let test_cases = vec![
            (
                vec![("a", 1, -1), ("b", 1, -2), ("a", 1, 1)],
                vec![("b", 1, -2)],
            ),
            (
                vec![("a", 1, -1), ("b", 1, 0), ("a", 1, 1)],
                vec![],
            ),
            (
                vec![("a", 1, 0)],
                vec![],
            ),
            (
                vec![("a", 1, 0), ("b", 1, 0)],
                vec![],
            ),
            (
                vec![("a", 1, 1), ("b", 2, 1)],
                vec![("a", 1, 1), ("b", 2, 1)],
            ),
        ];

        for (mut input, output) in test_cases {
            consolidate_updates(&mut input);
            assert_eq!(input, output);
        }
    }

    #[test]
    fn test_consolidating_container_builder() {
        let mut ccb = <ConsolidatingContainerBuilder<Vec<(usize, usize, usize)>>>::default();
        for _ in 0..1024 {
            ccb.push_into((0, 0, 0));
        }
        assert_eq!(ccb.extract(), None);
        assert_eq!(ccb.finish(), None);

        for i in 0..1024 {
            ccb.push_into((i, 0, 1));
        }

        let mut collected = Vec::default();
        while let Some(container) = ccb.finish() {
            collected.append(container);
        }
        // The output happens to be sorted, but it's not guaranteed.
        collected.sort();
        for i in 0..1024 {
            assert_eq!((i, 0, 1), collected[i]);
        }
    }

    #[test]
    fn test_consolidate_into() {
        let mut data = vec![(1, 1, 1), (2, 1, 1), (1, 1, -1)];
        let mut target = Vec::default();
        data.sort();
        data.consolidate_into(&mut target);
        assert_eq!(target, [(2, 1, 1)]);
    }

    #[cfg(not(debug_assertions))]
    const LEN: usize = 256 << 10;
    #[cfg(not(debug_assertions))]
    const REPS: usize = 10 << 10;

    #[cfg(debug_assertions)]
    const LEN: usize = 256 << 1;
    #[cfg(debug_assertions)]
    const REPS: usize = 10 << 1;

    #[test]
    fn test_consolidator_duration() {
        let mut data = Vec::with_capacity(LEN);
        let mut data2 = Vec::with_capacity(LEN);
        let mut target = Vec::new();
        let mut duration = std::time::Duration::default();
        for _ in 0..REPS {
            data.clear();
            data2.clear();
            target.clear();
            data.extend((0..LEN).map(|i| (i/4, 1, -2isize + ((i % 4) as isize))));
            data2.extend((0..LEN).map(|i| (i/4, 1, -2isize + ((i % 4) as isize))));
            data.sort_by(|x,y| x.0.cmp(&y.0));
            let start = std::time::Instant::now();
            data.consolidate_into(&mut target);
            duration += start.elapsed();

            consolidate_updates(&mut data2);
            assert_eq!(target, data2);
        }
        println!("elapsed consolidator {duration:?}");
    }

    #[test]
    fn test_consolidator_duration_vec() {
        let mut data = Vec::with_capacity(LEN);
        let mut duration = std::time::Duration::default();
        for _ in 0..REPS {
            data.clear();
            data.extend((0..LEN).map(|i| (i/4, 1, -2isize + ((i % 4) as isize))));
            data.sort_by(|x,y| x.0.cmp(&y.0));
            let start = std::time::Instant::now();
            consolidate_updates(&mut data);
            duration += start.elapsed();
        }
        println!("elapsed vec {duration:?}");
    }
}
