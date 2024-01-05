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
use timely::communication::Data;
use timely::order::Product;
use timely::progress::timestamp::{PathSummary, Refines, Timestamp};
use timely::progress::Antichain;
use timely::PartialOrder;

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
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
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
    P: Extrema + Clone + Debug + Data + Hash + Ord,
{
    type Summary = ();
    fn minimum() -> Self {
        Self(Timestamp::minimum())
    }
}
impl<P, T: Timestamp> Refines<()> for Partitioned<P, T>
where
    P: Extrema + Clone + Debug + Data + Hash + Ord,
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
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
    P: Extrema + Clone + Debug + Data + Hash + Ord,
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
        assert_eq!(minimum, Partitioned::with_range(None, None, 0));
        assert!(PartialOrder::less_equal(&minimum, &minimum));
        assert!(!PartialOrder::less_than(&minimum, &minimum));

        // All of these should be uncomparable in pairs
        let lower = Partitioned::with_range(None, Some(10), 0);
        let partition10 = Partitioned::with_partition(10, 0);
        let upper = Partitioned::with_range(Some(10), None, 0);
        assert!(!PartialOrder::less_equal(&lower, &partition10));
        assert!(!PartialOrder::less_equal(&partition10, &lower));
        assert!(!PartialOrder::less_equal(&lower, &upper));
        assert!(!PartialOrder::less_equal(&upper, &lower));
        assert!(!PartialOrder::less_equal(&partition10, &upper));
        assert!(!PartialOrder::less_equal(&upper, &partition10));

        let partition5 = Partitioned::with_partition(5, 0);
        // Point 5 is greater than the lower range
        assert!(PartialOrder::less_than(&lower, &partition5));
        // But uncomparable with the upper range
        assert!(!PartialOrder::less_equal(&upper, &partition5));
        assert!(!PartialOrder::less_equal(&partition5, &upper));

        let sub_range = Partitioned::with_range(Some(1), Some(5), 0);
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
            Partitioned::with_range(None, Some(10), 5),
            Partitioned::with_partition(10, 5),
            Partitioned::with_range(Some(10), None, 5),
        ]);
        assert_eq!(frontier.len(), 3);

        // Insert the biggest range at timestamp 4 that should shadow all other elements
        frontier.insert(Partitioned::with_range(None, None, 4));
        assert_eq!(
            frontier,
            Antichain::from_elem(Partitioned::with_range(None, None, 4))
        );

        // Create a frontier with one Point downgraded to timestamp 10 and all the rest at timestamp 5
        let frontier = Antichain::from_iter([
            Partitioned::with_range(None, Some(10), 5),
            Partitioned::with_partition(10, 10),
            Partitioned::with_range(Some(10), None, 5),
        ]);

        // The frontier is less than future timestamps of Point 10
        assert!(frontier.less_than(&Partitioned::with_partition(10, 11)));
        // And also less than any other Point at timestamp 6
        assert!(frontier.less_than(&Partitioned::with_partition(0, 6)));
        // But it's not less than any Point at time 4
        assert!(!frontier.less_than(&Partitioned::with_partition(0, 4)));
        // It's also less than the partition range (2, 6) at time 6
        assert!(frontier.less_than(&Partitioned::with_range(Some(2), Some(6), 6)));
        // But it's not less than the partition range (2, 6) at time 4
        assert!(!frontier.less_than(&Partitioned::with_range(Some(2), Some(6), 4)));
    }

    #[mz_ore::test]
    fn summary_properties() {
        let summary1 = PartitionedSummary::with_range(Some(10), Some(100), 5);
        let summary2 = PartitionedSummary::with_range(Some(20), Some(30), 5);
        let summary3 = PartitionedSummary::with_range(Some(30), Some(40), 5);
        let part_summary1 = PartitionedSummary::with_partition(15, 5);
        let part_summary2 = PartitionedSummary::with_partition(16, 5);

        // Ranges are constrained and summaries combined
        assert_eq!(
            PathSummary::<Partitioned<_, u64>>::followed_by(&summary1, &summary2),
            Some(PartitionedSummary::with_range(Some(20), Some(30), 10))
        );
        assert_eq!(
            PathSummary::<Partitioned<_, u64>>::followed_by(&summary2, &summary1),
            Some(PartitionedSummary::with_range(Some(20), Some(30), 10))
        );

        // Non overlapping ranges result into nothing
        assert_eq!(
            PathSummary::<Partitioned<_, u64>>::followed_by(&summary2, &summary3),
            None
        );
        assert_eq!(
            PathSummary::<Partitioned<_, u64>>::followed_by(&summary3, &summary2),
            None
        );

        // Point with ranges result into just the Point if it's within range
        assert_eq!(
            PathSummary::<Partitioned<_, u64>>::followed_by(&part_summary1, &summary1),
            Some(PartitionedSummary::with_partition(15, 10))
        );
        assert_eq!(
            PathSummary::<Partitioned<_, u64>>::followed_by(&summary1, &part_summary1),
            Some(PartitionedSummary::with_partition(15, 10))
        );

        // Partitions with ranges result into nothing if it's not within range
        assert_eq!(
            PathSummary::<Partitioned<_, u64>>::followed_by(&part_summary1, &summary2),
            None
        );
        assert_eq!(
            PathSummary::<Partitioned<_, u64>>::followed_by(&summary2, &part_summary1),
            None
        );

        // Same Point summaries result into the summary combined
        assert_eq!(
            PathSummary::<Partitioned<_, u64>>::followed_by(&part_summary1, &part_summary1),
            Some(PartitionedSummary::with_partition(15, 10))
        );

        // Different Point summaries result into nothing
        assert_eq!(
            PathSummary::<Partitioned<_, u64>>::followed_by(&part_summary1, &part_summary2),
            None
        );
        assert_eq!(
            PathSummary::<Partitioned<_, u64>>::followed_by(&part_summary2, &part_summary1),
            None
        );

        let ts1 = Partitioned::with_range(Some(10), Some(20), 100u64);
        let ts2 = Partitioned::with_range(Some(20), Some(30), 100u64);
        let ts3 = Partitioned::with_partition(15, 100u64);
        let ts4 = Partitioned::with_partition(16, 100u64);

        // Ranges are constrained and summaries applied
        assert_eq!(
            summary1.results_in(&ts1),
            Some(Partitioned::with_range(Some(10), Some(20), 105))
        );

        // Non overlapping ranges result into nothing
        assert_eq!(summary2.results_in(&ts1), None);

        // Partitions with ranges result into just the Point if it's within range
        assert_eq!(
            part_summary1.results_in(&ts1),
            Some(Partitioned::with_partition(15, 105))
        );

        // Point with ranges result into nothing if it's not within range
        assert_eq!(part_summary1.results_in(&ts2), None);

        // Same Point summaries result into the summary applied
        assert_eq!(
            part_summary1.results_in(&ts3),
            Some(Partitioned::with_partition(15, 105))
        );

        // Different Point summaries result into nothing
        assert_eq!(part_summary1.results_in(&ts4), None);
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
