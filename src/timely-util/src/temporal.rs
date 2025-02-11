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
    fn split(self, timestamp: &Self::Timestamp, fuel: &mut isize) -> (Self, Self);
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
    fn split(self, fuel: &mut isize) -> [Bucket<S>; 2] {
        let bits = self.bits - 1;
        let midpoint = self.offset.advance_by_exponent(bits).expect("must exist");
        let (s_lower, s_upper) = self.storage.split(&midpoint, fuel);
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
///
/// A bucket chain is well-formed if all buckets are within two bits of each other, with an imaginary
/// bucket of -1 bits at the end. A chain does not need to be well-formed at all times, and supports
/// peeling and finding even if not well-formed. However, `peel` might need to split buckets to
/// extract the desired data.
///
/// The `restore` method can be used to restore the chain property. It needs to be called repeatedly
/// with a positive amount of fuel while the remaining fuel after the call is non-positive.
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
                self.buckets.extend(bucket.split(&mut 0));
            }
        }
        peeled
    }

    /// Restore the chain property by splitting buckets as necessary.
    ///
    /// The chain is well-formed if all buckets are within two bits of each other, with an imaginary
    /// bucket of -1 bits at the end.
    pub fn restore(&mut self, fuel: &mut isize) {
        // Start at the biggest bucket
        let mut index = 0;
        // While the index points at a valid bucket, and we have fuel, try to restore the chain.
        while index < self.buckets.len() && *fuel > 0 {
            // The maximum number of bits the current bucket can have is the number of bits of the
            // next bucket plus two, or 1 if there is no next bucket. This ensures the end of the
            // chain is terminated with a small bucket.
            let max_bits = if index < self.buckets.len() - 1 {
                self.buckets[index + 1].bits + 2
            } else {
                1
            };
            if self.buckets[index].bits > max_bits {
                let split = self.buckets.remove(index).split(fuel);
                self.buckets.splice(index..index, split);
                // Move pointer back as after splitting the bucket at `index`, we might now
                // violate the chain property at `index - 1`.
                index = index.saturating_sub(1);
            } else {
                // Bucket
                index += 1;
            }
        }
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

    struct TestStorage {
        inner: Vec<u8>,
    }

    impl Storage for TestStorage {
        type Timestamp = u8;
        fn split(self, timestamp: &u8, fuel: &mut isize) -> (Self, Self) {
            *fuel = fuel.saturating_sub(self.inner.len().try_into().expect("must fit"));
            let (left, right) = self.inner.into_iter().partition(|d| *d < *timestamp);
            (Self { inner: left }, Self { inner: right })
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
        let mut fuel = 1000;
        chain.restore(&mut fuel);
        assert!(fuel > 0);
        let buckets = chain.peel(Antichain::new());
        let mut fuel = 1000;
        chain.restore(&mut fuel);
        assert!(fuel > 0);
        assert_eq!(buckets[0].lower(), Antichain::from_elem(0));
        assert_eq!(buckets.last().unwrap().upper(), Antichain::new());

        let mut chain = BucketChain::new(TestStorage {
            inner: (0..=255).collect(),
        });
        let peeled = chain.peel(Antichain::from_elem(1));
        let mut fuel = 1000;
        chain.restore(&mut fuel);
        assert!(fuel > 0);
        assert_eq!(peeled.len(), 1);
        assert_eq!(peeled[0].inner[0], 0);
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
}
