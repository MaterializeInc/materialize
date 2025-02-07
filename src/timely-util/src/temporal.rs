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
use timely::progress::Antichain;
use timely::PartialOrder;

pub trait BucketTimestamp: Copy + PartialOrd + TotalOrder {
    const MIN: Self;
    fn advance_by(self, bits: usize) -> Option<Self>;
}

#[derive(Debug, Clone, Copy)]
pub struct Bucket<T> {
    bits: usize,
    offset: T,
}

impl<T: BucketTimestamp> Bucket<T> {
    fn new(bits: usize, offset: T) -> Self {
        Self { bits, offset }
    }

    /// Split a bucket in two in the middle.
    fn split(self) -> [Bucket<T>; 2] {
        let bits = self.bits - 1;
        [
            Bucket::new(bits, self.offset),
            Bucket::new(bits, self.offset.advance_by(bits).expect("must exist")),
        ]
    }

    pub fn lower(&self) -> Antichain<T> {
        Antichain::from_elem(self.offset)
    }

    pub fn upper(&self) -> Antichain<T> {
        self.offset
            .advance_by(self.bits)
            .map_or_else(Default::default, Antichain::from_elem)
    }
}

pub struct BucketChain<T> {
    /// Buckets in reverse order.
    buckets: Vec<Bucket<T>>,
}

impl<T: BucketTimestamp + std::fmt::Debug> BucketChain<T> {
    pub fn new() -> Self {
        Self {
            buckets: vec![Bucket::new(size_of::<T>() * 8, T::MIN)],
        }
    }

    pub fn peel(&mut self, frontier: Antichain<T>) -> Vec<Bucket<T>> {
        let mut peeled = vec![];
        while !self.buckets.is_empty()
            && PartialOrder::less_than(&self.buckets[self.buckets.len() - 1].lower(), &frontier)
        {
            let bucket = self.buckets.pop().expect("must exist");
            if PartialOrder::less_equal(&bucket.upper(), &frontier) {
                peeled.push(bucket);
            } else {
                let split = bucket.split();
                self.buckets.extend(split.into_iter().rev());
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
                self.buckets.splice(index..index, split.into_iter().rev());
            }
            index += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    impl BucketTimestamp for u8 {
        const MIN: Self = 0;
        fn advance_by(self, bits: usize) -> Option<Self> {
            self.checked_add(1_u8.checked_shl(bits as u32)?)
        }
    }

    #[test]
    fn test_bucket_chain() {
        println!("{:?}", Some(1) < None);
        let mut chain = BucketChain::<u8>::new();
        let buckets = chain.peel(Antichain::new());
        assert_eq!(buckets.len(), 1);
        assert_eq!(buckets[0].lower(), Antichain::from_elem(0));
        assert_eq!(buckets[0].upper(), Antichain::new());

        let mut chain = BucketChain::<u8>::new();
        let peeled = chain.peel(Antichain::from_elem(1));
        assert_eq!(peeled.len(), 1);
        println!("{:?}", peeled);
        println!("{:?}", chain.buckets);
        let peeled = chain.peel(Antichain::from_elem(63));
        println!("{:?}", peeled);
        println!("{:?}", chain.buckets);
        let peeled = chain.peel(Antichain::from_elem(65));
        println!("{:?}", peeled);
        println!("{:?}", chain.buckets);
    }
}
