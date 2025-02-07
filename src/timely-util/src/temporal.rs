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

#![allow(missing_docs)]

use timely::order::TotalOrder;
use timely::progress::{Antichain, Timestamp};
use timely::PartialOrder;

/// Timestamp extension for totally ordered timestamps that can advance by `2^exponent`.
pub trait BucketTimestamp: TotalOrder + Timestamp {
    /// Advance this timestamp by `2^exponent`.
    fn advance_by_exponent(&self, exponent: usize) -> Option<Self>;
}

pub trait Storage: Sized {
    /// The timestamp type associated with this storage.
    type Timestamp: BucketTimestamp;
    /// Split self in two, based on the timestamp. The result a pair of self, where the first
    /// element contains all data with a timestamp strictly less than `timestamp`, and the second
    /// all other data.
    fn split(self, timestamp: &Self::Timestamp) -> (Self, Self);
}

/// A bucket of data, with a lower and an upper bound in time.
///
/// The lower and the upper bounds are represented as an offset and a log-size of the bucket.
#[derive(Debug, Clone, Copy)]
pub struct Bucket<S: Storage> {
    /// The size class of the bucket.
    bits: usize,
    /// Start of the bucket.
    offset: S::Timestamp,
    /// User-specific storage.
    storage: S,
}

impl<S: Storage> Bucket<S> {
    fn new(bits: usize, offset: S::Timestamp, storage: S) -> Self {
        Self {
            bits,
            offset,
            storage,
        }
    }

    /// Split a bucket in two in the middle. Returns two buckets, the first of which is the upper
    /// half.
    ///
    /// Panics if the bucket cannot be split.
    fn split(self) -> [Bucket<S>; 2] {
        let bits = self.bits - 1;
        let midpoint = self.offset.advance_by_exponent(bits).expect("must exist");
        let (s_lower, s_upper) = self.storage.split(&midpoint);
        [
            Bucket::new(bits, midpoint, s_upper),
            Bucket::new(bits, self.offset, s_lower),
        ]
    }

    /// The lower frontier of times in the bucket.
    pub fn lower(&self) -> Antichain<S::Timestamp> {
        Antichain::from_elem(self.offset.clone())
    }

    /// The upper frontier of times in the bucket.
    pub fn upper(&self) -> Antichain<S::Timestamp> {
        self.offset
            .advance_by_exponent(self.bits)
            .map_or_else(Default::default, Antichain::from_elem)
    }
}

impl<S: Storage> std::ops::Deref for Bucket<S> {
    type Target = S;
    fn deref(&self) -> &Self::Target {
        &self.storage
    }
}

impl<S: Storage> std::ops::DerefMut for Bucket<S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.storage
    }
}

/// A sorted list of buckets, representing data bucketed by timestamp.
///
/// Bucket chains support two main APIS: finding buckets for a given timestamp, and peeling
/// off buckets up to a frontier. All operations aim to be amortized logarithmic in the number
/// of outstanding timestamps.
///
/// We achieve this by storing buckets of increasing size for timestamps that are further out
/// in the future. At the same time, adjacent buckets span a time range at most factor 4 different.
pub struct BucketChain<S: Storage> {
    /// Buckets in reverse order.
    buckets: Vec<Bucket<S>>,
}

impl<S: Storage> BucketChain<S> {
    /// Construct a new bucket chain. Spans the whole time domain.
    pub fn new(storage: S) -> Self {
        let bucket = Bucket::new(size_of::<S::Timestamp>() * 8, Timestamp::minimum(), storage);
        let buckets = vec![bucket];
        Self { buckets }
    }

    /// Find the bucket that contains data for time `timestamp`. Returns a reference to the bucket,
    /// or `None` if there is no bucket for the requested time.
    ///
    /// Only times that haven't been peeled can still be found.
    pub fn find(&self, timestamp: &S::Timestamp) -> Option<&Bucket<S>> {
        self.buckets
            .iter()
            .rev()
            .find(|bucket| bucket.lower().less_equal(timestamp))
    }

    /// Find the bucket that contains data for time `timestamp`. Returns a mutable reference to
    /// the bucket, or `None` if there is no bucket for the requested time.
    ///
    /// Only times that haven't been peeled can still be found.
    pub fn find_mut(&mut self, timestamp: &S::Timestamp) -> Option<&mut Bucket<S>> {
        self.buckets
            .iter_mut()
            .rev()
            .find(|bucket| bucket.lower().less_equal(timestamp))
    }

    /// Peel off all data up to `frontier`, where the returned buckets contain all
    /// data strictly less than the frontier.
    pub fn peel(&mut self, frontier: Antichain<S::Timestamp>) -> Vec<Bucket<S>> {
        let mut peeled = vec![];
        while !self.buckets.is_empty()
            && PartialOrder::less_than(&self.buckets[self.buckets.len() - 1].lower(), &frontier)
        {
            let bucket = self.buckets.pop().expect("must exist");
            if PartialOrder::less_equal(&bucket.upper(), &frontier) {
                peeled.push(bucket);
            } else {
                self.buckets.extend(bucket.split());
            }
        }
        self.restore();
        peeled
    }

    /// Restore the chain property by splitting buckets as necessary.
    ///
    /// The chain is well-formed if all buckets are within two bits of each other.
    fn restore(&mut self) {
        let mut index = 0;
        while index + 1 < self.buckets.len() {
            if self.buckets[index].bits > self.buckets[index + 1].bits + 2 {
                let split = self.buckets.remove(index).split();
                self.buckets.splice(index..index, split);
            }
            index += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_ore::vec::VecExt;

    impl BucketTimestamp for u8 {
        fn advance_by_exponent(&self, bits: usize) -> Option<Self> {
            self.checked_add(1_u8.checked_shl(bits as u32)?)
        }
    }

    struct TestStorage {
        inner: Vec<u8>,
    }

    impl Storage for TestStorage {
        type Timestamp = u8;
        fn split(mut self, timestamp: &u8) -> (Self, Self) {
            let inner = self
                .inner
                .drain_filter_swapping(|d| *d < *timestamp)
                .collect();
            (Self { inner }, self)
        }
    }

    #[test]
    fn test_bucket_chain() {
        let collect_and_sort = |peeled: Vec<Bucket<TestStorage>>| {
            let mut collected: Vec<_> = peeled
                .iter()
                .flat_map(|b| b.inner.iter().copied())
                .collect();
            collected.sort();
            collected
        };

        let mut chain = BucketChain::new(TestStorage { inner: vec![] });
        let buckets = chain.peel(Antichain::new());
        assert_eq!(buckets.len(), 1);
        assert_eq!(buckets[0].lower(), Antichain::from_elem(0));
        assert_eq!(buckets[0].upper(), Antichain::new());

        let mut chain = BucketChain::new(TestStorage {
            inner: (0..=255).collect(),
        });
        let peeled = chain.peel(Antichain::from_elem(1));
        assert_eq!(peeled.len(), 1);
        assert_eq!(peeled[0].inner[0], 0);
        let peeled = chain.peel(Antichain::from_elem(63));
        assert!(collect_and_sort(peeled).into_iter().eq(1..63));
        let peeled = chain.peel(Antichain::from_elem(65));
        assert!(collect_and_sort(peeled).into_iter().eq(63..65));
        let peeled = chain.peel(Antichain::new());
        assert!(collect_and_sort(peeled).into_iter().eq(65..=255));
    }
}
