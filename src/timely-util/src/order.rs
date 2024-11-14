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

//! Traits and types for partially ordered sets.

use std::cmp::Ordering;
use std::fmt::{self, Debug};
use std::hash::Hash;

use serde::{Deserialize, Serialize};
use timely::container::columnation::CopyRegion;
use timely::order::Product;
use timely::progress::timestamp::{PathSummary, Refines, Timestamp};
use timely::progress::Antichain;
use timely::{ExchangeData, PartialOrder};
use uuid::Uuid;

use mz_ore::cast::CastFrom;

/// A partially ordered timestamp that is partitioned by an arbitrary number of partitions
/// identified by `P`. The construction allows for efficient representation of frontiers with
/// Antichains.
///
/// A `Partitioned<P, T>` timestamp is internally the product order of an `Interval<P>` and a bare
/// timestamp `T`. An `Interval<P>` represents an inclusive range of values from the type `P` and
/// its partial order corresponds to the subset order.
///
/// Effectively, the minimum `Partitioned` timestamp will start out with the maximum possible
/// `Interval<P>` on one side and the minimum timestamp `T` on the other side. Users of this
/// timestamp can selectively downgrade the timestamp by advancing `T`, shrinking the interval, or
/// both.
///
/// Antichains of this type are efficient in storage. In the worst case, where all chosen
/// partitions have gaps between them, the produced antichain has twice as many elements as
/// partitions. This is because the "dead space" between the selected partitions must have a
/// representative timestamp in order for that space to be useable in the future.
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Partitioned<P, T>(Product<Interval<P>, T>);

impl<P: Clone + PartialOrd, T> Partitioned<P, T> {
    /// Constructs a new timestamp for a specific partition.
    pub fn new_singleton(partition: P, timestamp: T) -> Self {
        let interval = Interval {
            lower: partition.clone(),
            upper: partition,
        };
        Self(Product::new(interval, timestamp))
    }

    /// Constructs a new timestamp for a partition range.
    pub fn new_range(lower: P, upper: P, timestamp: T) -> Self {
        assert!(lower <= upper, "invalid range bounds");
        Self(Product::new(Interval { lower, upper }, timestamp))
    }

    /// Returns the interval component of this partitioned timestamp.
    pub fn interval(&self) -> &Interval<P> {
        &self.0.outer
    }

    /// Returns the timestamp component of this partitioned timestamp.
    pub fn timestamp(&self) -> &T {
        &self.0.inner
    }

    pub fn timestamp_mut(&mut self) -> &mut T {
        &mut self.0.inner
    }
}

impl<P: Clone + PartialOrd + Step, T: Clone> Partitioned<P, T> {
    /// Returns up to two partitions that contain the interval before and/or
    /// after given partition point, neither of which contain the point.
    pub fn split(&self, point: &P) -> (Option<Self>, Option<Self>) {
        let (before, after) = self.interval().split(point);
        let mapper = |interval| Self(Product::new(interval, self.timestamp().clone()));
        (before.map(mapper), after.map(mapper))
    }
}

impl<P: fmt::Display, T: fmt::Display> fmt::Display for Partitioned<P, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        f.write_str("(")?;
        self.0.outer.fmt(f)?;
        f.write_str(", ")?;
        self.0.inner.fmt(f)?;
        f.write_str(")")?;
        Ok(())
    }
}

impl<P, T: Timestamp> Timestamp for Partitioned<P, T>
where
    P: Extrema + Clone + Debug + ExchangeData + Hash + Ord,
{
    type Summary = ();
    fn minimum() -> Self {
        Self(Timestamp::minimum())
    }
}
impl<P, T: Timestamp> Refines<()> for Partitioned<P, T>
where
    P: Extrema + Clone + Debug + ExchangeData + Hash + Ord,
{
    fn to_inner(_other: ()) -> Self {
        Self::minimum()
    }

    fn to_outer(self) {}

    fn summarize(_path: Self::Summary) {}
}

impl<P: Ord + Eq, T: PartialOrder> PartialOrder for Partitioned<P, T> {
    #[inline]
    fn less_equal(&self, other: &Self) -> bool {
        self.0.less_equal(&other.0)
    }
}
impl<P: Clone, T: Timestamp> PathSummary<Partitioned<P, T>> for () {
    #[inline]
    fn results_in(&self, src: &Partitioned<P, T>) -> Option<Partitioned<P, T>> {
        Some(src.clone())
    }

    #[inline]
    fn followed_by(&self, _other: &Self) -> Option<Self> {
        Some(())
    }
}

impl<P: Copy, T: Copy> columnation::Columnation for Partitioned<P, T> {
    type InnerRegion = CopyRegion<Partitioned<P, T>>;
}

/// A trait defining the minimum and maximum values of a type.
pub trait Extrema {
    /// The minimum value of this type.
    fn minimum() -> Self;
    /// The maximum value of this type.
    fn maximum() -> Self;
}

impl Extrema for u64 {
    fn minimum() -> Self {
        Self::MIN
    }
    fn maximum() -> Self {
        Self::MAX
    }
}

impl Extrema for i32 {
    fn minimum() -> Self {
        Self::MIN
    }
    fn maximum() -> Self {
        Self::MAX
    }
}

impl Extrema for Uuid {
    fn minimum() -> Self {
        Self::nil()
    }
    fn maximum() -> Self {
        Self::max()
    }
}

// TODO: Switch to the std one when it's no longer unstable: https://doc.rust-lang.org/std/iter/trait.Step.html
pub trait Step
where
    Self: Sized,
{
    // Returns the element value sequenced before the type.
    fn backward_checked(&self, count: usize) -> Option<Self>;
    // Returns the element value sequenced after the type.
    fn forward_checked(&self, count: usize) -> Option<Self>;
}

impl Step for Uuid {
    fn backward_checked(&self, count: usize) -> Option<Self> {
        self.as_u128()
            .checked_sub(u128::cast_from(count))
            .map(Self::from_u128)
    }
    fn forward_checked(&self, count: usize) -> Option<Self> {
        self.as_u128()
            .checked_add(u128::cast_from(count))
            .map(Self::from_u128)
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
/// A type representing an inclusive interval of type `P`, ordered under the subset relation.
pub struct Interval<P> {
    pub lower: P,
    pub upper: P,
}

impl<P: Eq> Interval<P> {
    /// Returns the contained element if it's a singleton set.
    pub fn singleton(&self) -> Option<&P> {
        if self.lower == self.upper {
            Some(&self.lower)
        } else {
            None
        }
    }
}

impl<P: PartialOrd> Interval<P> {
    pub fn contains(&self, other: &P) -> bool {
        self.lower <= *other && *other <= self.upper
    }
}

impl<P: Step + PartialOrd + Clone> Interval<P> {
    /// Returns up to two intervals that contain the range before and/or after given point,
    /// neither of which contain the point.
    pub fn split(&self, point: &P) -> (Option<Self>, Option<Self>) {
        let before = match point.backward_checked(1) {
            Some(bef) if self.lower <= bef => Some(Interval {
                lower: self.lower.clone(),
                upper: bef,
            }),
            _ => None,
        };
        let after = match point.forward_checked(1) {
            Some(aft) if self.upper >= aft => Some(Interval {
                lower: aft,
                upper: self.upper.clone(),
            }),
            _ => None,
        };
        (before, after)
    }
}

impl<P: Ord + Eq> PartialOrder for Interval<P> {
    #[inline]
    fn less_equal(&self, other: &Self) -> bool {
        self.lower <= other.lower && other.upper <= self.upper
    }
}

impl<P: Clone> PathSummary<Interval<P>> for () {
    #[inline]
    fn results_in(&self, src: &Interval<P>) -> Option<Interval<P>> {
        Some(src.clone())
    }

    #[inline]
    fn followed_by(&self, _other: &Self) -> Option<Self> {
        Some(())
    }
}

impl<P> Timestamp for Interval<P>
where
    P: Extrema + Clone + Debug + ExchangeData + Hash + Ord,
{
    type Summary = ();

    #[inline]
    fn minimum() -> Self {
        Self {
            lower: P::minimum(),
            upper: P::maximum(),
        }
    }
}

impl<P: fmt::Display> fmt::Display for Interval<P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[")?;
        self.lower.fmt(f)?;
        f.write_str(", ")?;
        self.upper.fmt(f)?;
        f.write_str("]")?;
        Ok(())
    }
}

/// A helper struct for reverse partial ordering.
///
/// This struct is a helper that can be used with `Antichain` when the maximum inclusive frontier
/// needs to be maintained as opposed to the mininimum inclusive.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Reverse<T>(pub T);

impl<T: PartialOrder> PartialOrder for Reverse<T> {
    #[inline]
    fn less_equal(&self, other: &Self) -> bool {
        PartialOrder::less_equal(&other.0, &self.0)
    }
}
impl<T: PartialOrd> PartialOrd for Reverse<T> {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        other.0.partial_cmp(&self.0)
    }
}

impl<T: Ord> Ord for Reverse<T> {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        other.0.cmp(&self.0)
    }
}

#[cfg(test)]
mod test {
    use timely::progress::Antichain;

    use super::*;

    #[mz_ore::test]
    fn basic_properties() {
        let minimum: Partitioned<u64, u64> = Partitioned::minimum();
        assert_eq!(minimum, Partitioned::new_range(0, u64::MAX, 0));
        assert!(PartialOrder::less_equal(&minimum, &minimum));
        assert!(!PartialOrder::less_than(&minimum, &minimum));

        // All of these should be uncomparable in pairs
        let lower = Partitioned::new_range(0, 9, 0);
        let partition10 = Partitioned::new_singleton(10, 0);
        let upper = Partitioned::new_range(11, u64::MAX, 0);
        assert!(!PartialOrder::less_equal(&lower, &partition10));
        assert!(!PartialOrder::less_equal(&partition10, &lower));
        assert!(!PartialOrder::less_equal(&lower, &upper));
        assert!(!PartialOrder::less_equal(&upper, &lower));
        assert!(!PartialOrder::less_equal(&partition10, &upper));
        assert!(!PartialOrder::less_equal(&upper, &partition10));

        let partition5 = Partitioned::new_singleton(5, 0);
        // Point 5 is greater than the lower range
        assert!(PartialOrder::less_than(&lower, &partition5));
        // But uncomparable with the upper range
        assert!(!PartialOrder::less_equal(&upper, &partition5));
        assert!(!PartialOrder::less_equal(&partition5, &upper));

        let sub_range = Partitioned::new_range(2, 4, 0);
        // This is a subrange of lower
        assert!(PartialOrder::less_than(&lower, &sub_range));
        // But uncomparable with the upper range
        assert!(!PartialOrder::less_equal(&upper, &sub_range));
        assert!(!PartialOrder::less_equal(&sub_range, &upper));

        // Check less than or equals holds when equals holds
        assert!(PartialOrder::less_equal(&lower, &lower));
        assert!(PartialOrder::less_equal(&partition5, &partition5));
        assert!(PartialOrder::less_equal(&upper, &upper));
    }

    #[mz_ore::test]
    fn antichain_properties() {
        let mut frontier = Antichain::new();

        // Insert a few uncomparable elements at timestamp 5
        frontier.extend([
            Partitioned::new_range(0, 9, 5),
            Partitioned::new_singleton(10, 5),
            Partitioned::new_range(11, u64::MAX, 5),
        ]);
        assert_eq!(frontier.len(), 3);

        // Insert the biggest range at timestamp 4 that should shadow all other elements
        frontier.insert(Partitioned::new_range(0, u64::MAX, 4));
        assert_eq!(
            frontier,
            Antichain::from_elem(Partitioned::new_range(0, u64::MAX, 4))
        );

        // Create a frontier with singleton partition downgraded to timestamp 10 and all the rest at timestamp 5
        let frontier = Antichain::from_iter([
            Partitioned::new_range(0, 9, 5),
            Partitioned::new_singleton(10, 10),
            Partitioned::new_range(11, u64::MAX, 5),
        ]);

        // The frontier is less than future timestamps of singleton partition 10
        assert!(frontier.less_than(&Partitioned::new_singleton(10, 11)));
        // And also less than any other singleton partition at timestamp 6
        assert!(frontier.less_than(&Partitioned::new_singleton(0, 6)));
        // But it's not less than any partition at time 4
        assert!(!frontier.less_than(&Partitioned::new_singleton(0, 4)));
        // It's also less than the partition range [3, 5] at time 6
        assert!(frontier.less_than(&Partitioned::new_range(3, 5, 6)));
        // But it's not less than the partition range [3, 5] at time 4
        assert!(!frontier.less_than(&Partitioned::new_range(3, 5, 4)));
    }
}

/// Refine an `Antichain<T>` into a `Antichain<Inner>`, using a `Refines`
/// implementation (in the case of tuple-style timestamps, this usually
/// means appending a minimum time).
pub fn refine_antichain<T: Timestamp, Inner: Timestamp + Refines<T>>(
    frontier: &Antichain<T>,
) -> Antichain<Inner> {
    Antichain::from_iter(frontier.iter().map(|t| Refines::to_inner(t.clone())))
}
