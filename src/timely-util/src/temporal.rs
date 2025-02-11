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

use timely::order::TotalOrder;
use timely::progress::{Antichain, Timestamp};
use timely::PartialOrder;

/// Timestamp extension for totally ordered timestamps that can advance by `2^exponent`.
pub trait BucketTimestamp: TotalOrder + Timestamp {
    /// The number of bits in the timestamp.
    const DOMAIN: usize = size_of::<Self>() * 8;
    /// Advance this timestamp by `2^exponent`.
    fn advance_by_exponent(&self, exponent: usize) -> Option<Self>;
}

pub trait Storage: Sized {
    /// The timestamp type associated with this storage.
    type Timestamp: BucketTimestamp;
    /// Split self in two, based on the timestamp. The result a pair of self, where the first
    /// element contains all data with a timestamp strictly less than `timestamp`, and the second
    /// all other data.
    fn split(self, timestamp: &Self::Timestamp, fuel: &mut isize) -> (Self, Self);
}

/// A sorted list of buckets, representing data bucketed by timestamp.
///
/// Bucket chains support three main APIs: finding buckets for a given timestamp, peeling
/// off buckets up to a frontier, and restoring the chain property. All operations aim to be
/// amortized logarithmic in the number of outstanding timestamps.
///
/// We achieve this by storing buckets of increasing size for timestamps that are further out
/// in the future. At the same time, in a well-formed chain, adjacent buckets span a time range at
/// most factor 4 different.
///
/// A bucket chain is well-formed if all buckets are within two bits of each other, with an imaginary
/// bucket of -1 bits at the start. A chain does not need to be well-formed at all times, and supports
/// peeling and finding even if not well-formed. However, `peel` might need to split more buckets to
/// extract the desired data.
///
/// The `restore` method can be used to restore the chain property. It needs to be called repeatedly
/// with a positive amount of fuel while the remaining fuel after the call is non-positive. This
/// allows the caller to control the amount of work done in a single call and interleave it with
/// other work.
///
/// ## Implementation detail
///
/// We store buckets as (bits, offset, storage) in separate vectors, sorted in reverse order by
/// offset.
#[derive(Debug)]
pub struct BucketChain<S: Storage> {
    /// Bits in reverse order.
    bits: Vec<usize>,
    /// Offsets in reverse order.
    offsets: Vec<S::Timestamp>,
    /// Storage in reverse order.
    storage: Vec<S>,
}

impl<S: Storage> BucketChain<S> {
    /// Construct a new bucket chain. Spans the whole time domain.
    #[inline]
    pub fn new(storage: S) -> Self {
        let bits = vec![S::Timestamp::DOMAIN];
        let offsets = vec![Timestamp::minimum()];
        let storage = vec![storage];
        Self {
            bits,
            offsets,
            storage,
        }
    }

    /// Find the bucket that contains data for time `timestamp`. Returns a reference to the bucket,
    /// or `None` if there is no bucket for the requested time.
    ///
    /// Only times that haven't been peeled can still be found.
    #[inline]
    pub fn find(&self, timestamp: &S::Timestamp) -> Option<&S> {
        let index = self
            .offsets
            .iter()
            .position(|offset| offset.less_equal(timestamp))?;
        Some(&self.storage[index])
    }

    /// Find the bucket that contains data for time `timestamp`. Returns a mutable reference to
    /// the bucket, or `None` if there is no bucket for the requested time.
    ///
    /// Only times that haven't been peeled can still be found.
    #[inline]
    pub fn find_mut(&mut self, timestamp: &S::Timestamp) -> Option<&mut S> {
        let index = self
            .offsets
            .iter()
            .position(|offset| offset.less_equal(timestamp))?;
        Some(&mut self.storage[index])
    }

    /// Peel off all data up to `frontier`, where the returned buckets contain all
    /// data strictly less than the frontier.
    #[inline]
    pub fn peel(&mut self, frontier: Antichain<S::Timestamp>) -> Vec<S> {
        let mut peeled = vec![];
        // While there are buckets, and the frontier is not less than the lowest offset, peel off
        while !self.is_empty() && !frontier.less_equal(self.offsets.last().expect("must exist")) {
            let (bits, offset, storage) = self.remove(self.len() - 1);
            let upper = offset.advance_by_exponent(bits);

            // Split the bucket if it spans the frontier.
            if upper.is_none() && !frontier.is_empty()
                || upper.is_some() && frontier.less_than(&upper.unwrap())
            {
                // We need to splice the bucket, no matter how much fuel we have.
                self.splice(self.len(), &mut 0, bits, offset, storage);
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
    /// bucket of -1 bits at the end.
    #[inline]
    pub fn restore(&mut self, fuel: &mut isize) {
        // Start at the biggest bucket, work towards the smallest.
        let mut index = 0;
        // While the index points at a valid bucket, and we have fuel, try to restore the chain.
        while index < self.len() && *fuel > 0 {
            // The maximum number of bits the current bucket can have is the number of bits of the
            // next bucket plus two, or 1 if there is no next bucket. This ensures the end of the
            // chain is terminated with a small bucket.
            let max_bits = if index < self.len() - 1 {
                self.bits[index + 1] + 2
            } else {
                1
            };
            if self.bits[index] > max_bits {
                let (bits, offset, storage) = self.remove(index);
                self.splice(index, fuel, bits, offset, storage);
                // Move pointer back as after splitting the bucket at `index`, we might now
                // violate the chain property at `index - 1`.
                index = index.saturating_sub(1);
            } else {
                // Bucket
                index += 1;
            }
        }
    }

    /// Returns `true` if the chain is empty. This means there are no outstanding times left.
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.bits.is_empty()
    }

    /// The number of buckets in the chain.
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.bits.len()
    }

    /// Removes the bucket at `index` and returns its components as `(bits, offset, storage)`.
    #[inline(always)]
    fn remove(&mut self, index: usize) -> (usize, S::Timestamp, S) {
        (
            self.bits.remove(index),
            self.offsets.remove(index),
            self.storage.remove(index),
        )
    }

    /// Split the bucket specified by `(bits, offset, storage)` and splice the result
    /// at `index`. Updates `fuel`.
    ///
    /// Panics if the bucket cannot be split, i.e, it covers 0 bits.
    #[inline(always)]
    fn splice(
        &mut self,
        index: usize,
        fuel: &mut isize,
        bits: usize,
        offset: S::Timestamp,
        storage: S,
    ) {
        let bits = bits - 1;
        let midpoint = offset.advance_by_exponent(bits).expect("must exist");
        let (bot, top) = storage.split(&midpoint, fuel);
        self.bits.splice(index..index, [bits, bits]);
        self.offsets.splice(index..index, [midpoint, offset]);
        self.storage.splice(index..index, [top, bot]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    impl BucketTimestamp for u8 {
        fn advance_by_exponent(&self, bits: usize) -> Option<Self> {
            self.checked_add(1_u8.checked_shl(bits as u32)?)
        }
    }

    impl BucketTimestamp for u64 {
        fn advance_by_exponent(&self, bits: usize) -> Option<Self> {
            self.checked_add(1_u64.checked_shl(bits as u32)?)
        }
    }

    #[derive(Debug)]
    struct TestStorage<T> {
        inner: Vec<T>,
    }

    impl<T: BucketTimestamp> Storage for TestStorage<T> {
        type Timestamp = T;
        fn split(self, timestamp: &T, fuel: &mut isize) -> (Self, Self) {
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

    #[test]
    fn test_bucket_chain_empty_peel_all() {
        let mut chain = BucketChain::new(TestStorage::<u8> { inner: vec![] });
        let mut fuel = 1000;
        chain.restore(&mut fuel);
        assert!(fuel > 0);
        let peeled = chain.peel(Antichain::new());
        assert!(collect_and_sort(peeled).is_empty());
        assert!(chain.is_empty());
    }

    #[test]
    fn test_bucket_chain() {
        let mut chain = BucketChain::new(TestStorage::<u8> {
            inner: (0..=255).collect(),
        });
        let peeled = chain.peel(Antichain::from_elem(1));
        assert_eq!(peeled.len(), 1);
        assert_eq!(peeled[0].inner[0], 0);
        assert!(collect_and_sort(peeled).into_iter().eq(0..1));
        let mut fuel = 1000;
        chain.restore(&mut fuel);
        assert!(fuel > 0);
        let peeled = chain.peel(Antichain::from_elem(63));
        let mut fuel = 1000;
        chain.restore(&mut fuel);
        assert!(fuel > 0);
        assert!(collect_and_sort(peeled).into_iter().eq(1..63));
        let peeled = chain.peel(Antichain::from_elem(65));
        let mut fuel = 1000;
        chain.restore(&mut fuel);
        assert!(fuel > 0);
        assert!(collect_and_sort(peeled).into_iter().eq(63..65));
        let peeled = chain.peel(Antichain::new());
        let mut fuel = 1000;
        chain.restore(&mut fuel);
        assert!(fuel > 0);
        assert!(collect_and_sort(peeled).into_iter().eq(65..=255));
    }

    /// Test a chain with 10M disjoint elements. The same with a vector would take too long for a
    /// test.
    #[test]
    fn test_bucket_10m() {
        let limit = 10_000_000;

        let mut chain = BucketChain::new(TestStorage::<u64> { inner: Vec::new() });
        let mut fuel = 1000;
        chain.restore(&mut fuel);
        assert!(fuel > 0);

        let now = 1739276664_u64;

        let peeled = chain.peel(Antichain::from_elem(now));
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
            let peeled = chain.peel(Antichain::from_elem(offset + step));
            assert!(collect_and_sort(peeled)
                .into_iter()
                .eq(offset..offset + step));
            offset += step;
            let mut fuel = 1000;
            chain.restore(&mut fuel);
        }
    }
}
