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

//! Utilities to efficiently store future updates.

use std::collections::BTreeMap;

use mz_ore::cast::CastFrom;
use timely::progress::Timestamp;
use timely::progress::frontier::AntichainRef;

/// Timestamp extension for timestamps that can advance by `2^exponent`.
///
/// Most likely, this is only relevant for totally ordered timestamps.
pub trait BucketTimestamp: Timestamp {
    /// The number of bits in the timestamp.
    const DOMAIN: usize = size_of::<Self>() * 8;
    /// Advance this timestamp by `2^exponent`. Returns `None` if the
    /// timestamp would overflow.
    fn advance_by_power_of_two(&self, exponent: u32) -> Option<Self>;
}

/// A type that can be split into two parts based on a timestamp.
pub trait Bucket: Sized {
    /// The timestamp type associated with this storage.
    type Timestamp: BucketTimestamp;
    /// Split self in two, based on the timestamp. The result is a pair of self, where the first
    /// element contains all data with a timestamp strictly less than `timestamp`, and the second
    /// all other data.
    fn split(self, timestamp: &Self::Timestamp, fuel: &mut i64) -> (Self, Self);
}

/// A sorted list of buckets, representing data bucketed by timestamp.
///
/// Bucket chains support three main APIs: finding buckets for a given timestamp, peeling
/// off buckets up to a frontier, and restoring the chain property. All operations aim to be
/// amortized logarithmic in the number of outstanding timestamps.
///
/// We achieve this by storing buckets of increasing size for timestamps that are further out
/// in the future. At the same time, in a well-formed chain, adjacent buckets span a time range at
/// most factor 4 different. Factor 4 means that we can skip at most one bucket size for adjacent
/// buckets, limiting the amount of work we need to do when peeling off buckets or restoring the
/// chain property.
///
/// A bucket chain is well-formed if all buckets are within two bits of each other, with an imaginary
/// bucket of -2 bits at the start. A chain does not need to be well-formed at all times, and supports
/// peeling and finding even if not well-formed. However, `peel` might need to split more buckets to
/// extract the desired data.
///
/// The `restore` method can be used to restore the chain property. It needs to be called repeatedly
/// with a positive amount of fuel while the remaining fuel after the call is non-positive. This
/// allows the caller to control the amount of work done in a single call and interleave it with
/// other work.
#[derive(Debug)]
pub struct BucketChain<S: Bucket> {
    content: BTreeMap<S::Timestamp, (u32, S)>,
}

impl<S: Bucket> BucketChain<S> {
    /// Construct a new bucket chain. Spans the whole time domain.
    #[inline]
    pub fn new(storage: S) -> Self {
        let bits = S::Timestamp::DOMAIN.try_into().expect("Must fit");
        Self {
            // The initial bucket starts at the minimum timestamp and spans the whole domain.
            content: BTreeMap::from([(Timestamp::minimum(), (bits, storage))]),
        }
    }

    /// Find the time range for the bucket that contains data for time `timestamp`.
    /// Returns the lower (inclusive) and upper (exclusive) time bound of the bucket,
    /// or `None` if there is no bucket for the requested time. Only times that haven't
    /// been peeled can still be found.
    ///
    /// The bounds are only valid until the next call to `peel` or `restore`.
    #[inline]
    pub fn range_of(&self, timestamp: &S::Timestamp) -> Option<(S::Timestamp, S::Timestamp)> {
        self.content
            .range(..=timestamp)
            .next_back()
            .map(|(time, (bits, _))| {
                (
                    time.clone(),
                    time.advance_by_power_of_two(bits.saturating_sub(1))
                        .expect("must exist"),
                )
            })
    }

    /// Find the bucket that contains data for time `timestamp`. Returns a reference to the bucket,
    /// or `None` if there is no bucket for the requested time.
    ///
    /// Only times that haven't been peeled can still be found.
    #[inline]
    pub fn find(&self, timestamp: &S::Timestamp) -> Option<&S> {
        self.content
            .range(..=timestamp)
            .next_back()
            .map(|(_, (_, storage))| storage)
    }

    /// Find the bucket that contains data for time `timestamp`. Returns a mutable reference to
    /// the bucket, or `None` if there is no bucket for the requested time.
    ///
    /// Only times that haven't been peeled can still be found.
    #[inline]
    pub fn find_mut(&mut self, timestamp: &S::Timestamp) -> Option<&mut S> {
        self.content
            .range_mut(..=timestamp)
            .next_back()
            .map(|(_, (_, storage))| storage)
    }

    /// Peel off all data up to `frontier`, where the returned buckets contain all
    /// data strictly less than the frontier.
    #[inline]
    pub fn peel(&mut self, frontier: AntichainRef<S::Timestamp>) -> Vec<S> {
        let mut peeled = vec![];
        // While there are buckets, and the frontier is not less than the lowest offset, peel off
        while let Some(min_entry) = self.content.first_entry()
            && !frontier.less_equal(min_entry.key())
        {
            let (offset, (bits, storage)) = self.content.pop_first().expect("must exist");
            let upper = offset.advance_by_power_of_two(bits);

            // Split the bucket if it spans the frontier.
            if upper.is_none() && !frontier.is_empty()
                || upper.is_some() && frontier.less_than(&upper.unwrap())
            {
                // We need to split the bucket, no matter how much fuel we have.
                self.split_and_insert(&mut 0, bits, offset, storage);
            } else {
                // Bucket is ready to go.
                peeled.push(storage);
            }
        }
        peeled
    }

    /// Restore the chain property by splitting buckets as necessary.
    ///
    /// The chain is well-formed if all buckets are within two bits of each other, with an imaginary
    /// bucket of -2 bits just before the smallest bucket.
    #[inline]
    pub fn restore(&mut self, fuel: &mut i64) {
        // We could write this in terms of a cursor API, but it's not stable yet. Instead, we
        // allocate a new map and move elements over.
        let mut new = BTreeMap::default();
        let mut last_bits = -2;
        while *fuel > 0
            && let Some((time, (bits, storage))) = self.content.pop_first()
        {
            // Insert bucket if the size is correct.
            if isize::cast_from(bits) <= last_bits + 2 {
                new.insert(time, (bits, storage));
                last_bits = isize::cast_from(bits);
            } else {
                // Otherwise, we need to split it.
                self.split_and_insert(fuel, bits, time, storage);
            }
        }
        // Move remaining elements if we ran out of fuel.
        new.append(&mut self.content);
        self.content = new;
    }

    /// Returns `true` if the chain is empty. This means there are no outstanding times left.
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.content.is_empty()
    }

    /// The number of buckets in the chain.
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.content.len()
    }

    /// Split the bucket specified by `(bits, offset, storage)` and insert the new buckets.
    /// Updates `fuel`.
    ///
    /// Panics if the bucket cannot be split, i.e, it covers 0 bits.
    #[inline(always)]
    fn split_and_insert(&mut self, fuel: &mut i64, bits: u32, offset: S::Timestamp, storage: S) {
        let bits = bits - 1;
        let midpoint = offset.advance_by_power_of_two(bits).expect("must exist");
        let (bot, top) = storage.split(&midpoint, fuel);
        self.content.insert(offset, (bits, bot));
        self.content.insert(midpoint, (bits, top));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    impl BucketTimestamp for u8 {
        fn advance_by_power_of_two(&self, bits: u32) -> Option<Self> {
            self.checked_add(1_u8.checked_shl(bits)?)
        }
    }

    impl BucketTimestamp for u64 {
        fn advance_by_power_of_two(&self, bits: u32) -> Option<Self> {
            self.checked_add(1_u64.checked_shl(bits)?)
        }
    }

    struct TestStorage<T> {
        inner: Vec<T>,
    }

    impl<T: std::fmt::Debug> std::fmt::Debug for TestStorage<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            self.inner.fmt(f)
        }
    }

    impl<T: BucketTimestamp> Bucket for TestStorage<T> {
        type Timestamp = T;
        fn split(self, timestamp: &T, fuel: &mut i64) -> (Self, Self) {
            *fuel = fuel.saturating_sub(self.inner.len().try_into().expect("must fit"));
            let (left, right) = self.inner.into_iter().partition(|d| *d < *timestamp);
            (Self { inner: left }, Self { inner: right })
        }
    }

    fn collect_and_sort<T: BucketTimestamp>(peeled: Vec<TestStorage<T>>) -> Vec<T> {
        let mut collected: Vec<_> = peeled
            .iter()
            .flat_map(|b| b.inner.iter().cloned())
            .collect();
        collected.sort();
        collected
    }

    #[mz_ore::test]
    fn test_bucket_chain_empty_peel_all() {
        let mut chain = BucketChain::new(TestStorage::<u8> { inner: vec![] });
        let mut fuel = 1000;
        chain.restore(&mut fuel);
        assert!(fuel > 0);
        let peeled = chain.peel(AntichainRef::new(&[]));
        assert!(collect_and_sort(peeled).is_empty());
        assert!(chain.is_empty());
    }

    #[mz_ore::test]
    fn test_bucket_chain_u8() {
        let mut chain = BucketChain::new(TestStorage::<u8> {
            inner: (0..=255).collect(),
        });
        let mut fuel = -1;
        while fuel <= 0 {
            fuel = 100;
            chain.restore(&mut fuel);
        }
        let peeled = chain.peel(AntichainRef::new(&[1]));
        assert_eq!(peeled.len(), 1);
        assert_eq!(peeled[0].inner[0], 0);
        assert!(collect_and_sort(peeled).into_iter().eq(0..1));
        let mut fuel = 1000;
        chain.restore(&mut fuel);
        assert!(fuel > 0);
        let peeled = chain.peel(AntichainRef::new(&[63]));
        let mut fuel = 1000;
        chain.restore(&mut fuel);
        assert!(fuel > 0);
        assert!(collect_and_sort(peeled).into_iter().eq(1..63));
        let peeled = chain.peel(AntichainRef::new(&[65]));
        let mut fuel = 1000;
        chain.restore(&mut fuel);
        assert!(fuel > 0);
        assert!(collect_and_sort(peeled).into_iter().eq(63..65));
        let peeled = chain.peel(AntichainRef::new(&[]));
        let mut fuel = 1000;
        chain.restore(&mut fuel);
        assert!(fuel > 0);
        assert!(collect_and_sort(peeled).into_iter().eq(65..=255));
    }

    /// Test a chain with 10M disjoint elements.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // slow
    fn test_bucket_10m() {
        let limit = 10_000_000;

        let mut chain = BucketChain::new(TestStorage::<u64> { inner: Vec::new() });
        let mut fuel = 1000;
        chain.restore(&mut fuel);
        assert!(fuel > 0);

        let now = 1739276664_u64;

        let peeled = chain.peel(AntichainRef::new(&[now]));
        let mut fuel = 1000;
        chain.restore(&mut fuel);
        assert!(fuel > 0);
        let peeled = collect_and_sort(peeled);
        assert!(peeled.is_empty());

        for i in now..now + limit {
            chain.find_mut(&i).expect("must exist").inner.push(i);
        }

        let mut offset = now;
        let step = 1000;
        while offset < now + limit {
            let peeled = chain.peel(AntichainRef::new(&[offset + step]));
            assert!(
                collect_and_sort(peeled)
                    .into_iter()
                    .eq(offset..offset + step)
            );
            offset += step;
            let mut fuel = 1000;
            chain.restore(&mut fuel);
        }
    }
}
