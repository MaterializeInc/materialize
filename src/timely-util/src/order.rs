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
use std::fmt;

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use timely::order::Product;
use timely::progress::timestamp::{PathSummary, Refines, Timestamp};
use timely::PartialOrder;

/// A partially ordered timestamp that is partitioned by an arbitrary number of partitions
/// identified by `P`. The construction allows for efficient representation frontiers with
/// Antichains.
///
/// ## The problem of partitioned timestamps
///
/// To understand this timestamp it helps to first think about how one would represent a
/// partitioned, partially ordered timestamp type where each partition follows its own independent
/// timeline in a natural way. Timely requires that all timestamps have a minimum element that is
/// less than all other elements so you could express a partitioned timestamp like so:
///
/// ```rust
/// enum Partitioned<P, T> {
///     Bottom,
///     Partition(P, T),
/// }
/// ```
/// Laying out the order in a graph we'd see something like this:
///
/// ```text
///           -----(P1, T0)---(P1, T1)--- ... ---(P1, Tn)
///          / ----(P2, T0)---(P2, T1)--- ... ---(P2, Tn)
///         / / ,--(P3, T0)---(P3, T1)--- ... ---(P3, Tn)
/// Bottom-+-+-+     .
///         \        .
///          \       .
///           '----(Pn, T0)---(Pn, T1)--- ... ---(Pn, Tn)
/// ```
///
/// This timestamp has the problem that if you want to downgrade your operator's capability to
/// indicate progress in one of the partitions, which implies dropping your `Bottom` capability,
/// you are forced to also mint one capability per partition that you intend to produce data for in
/// the future. If those partitions are unknown or, even worse, infinite, this type brings you to a
/// dead end.
///
/// ## How this type works
///
/// The idea of this `Partitioned` timestamp is to extend the idea of the `Bottom` element above
/// into a `Range` element that is parameterized by a lower and upper bound and represents a
/// *range* of partitions at some timestamp `T`. The represented range has exclusive bounds.
///
/// The minimum timestamp of this type is `Product(Range(Bottom, Top), T::minimum())` which is less
/// than any other `Range` element and all `Point` elements. Now, suppose an operator needs to
/// start working on some partition `P1` and present progress. All it has to do is downgrade its
/// `Antichain { Product(Range(Bottom, Top), 0) }` frontier in this frontier: `Antichain {
/// Product(Range(Bottom, Elem(P1)), 0), Product(Point(P1), T::minimum()), Product(Range(Elem(P1),
/// Top), 0) }`.
///
/// Essentially a `Range` element can be split at some partition P iff that partition is within
/// its range and produce two more `Range` elements representing the range to the left and to the
/// right of the partition respectively, plus a timestamp for the desired partition that can now be
/// downgraded individually to present progress.
///
/// Antichains of this type are efficient in storage. In the worst case, where all chosen
/// partitions have gaps between them, the produced antichain has twice as many elements as
/// partitions.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Partitioned<P, T>(Product<Interval<P>, T>);

impl<P: fmt::Display, T: fmt::Display> fmt::Display for Partitioned<P, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        f.write_str("(")?;
        match self.interval() {
            Interval::Range(lower, upper) => {
                match lower {
                    RangeBound::Elem(p) => p.fmt(f)?,
                    RangeBound::Bottom => f.write_str("-inf")?,
                    RangeBound::Top => unreachable!(),
                }
                f.write_str("..")?;
                match upper {
                    RangeBound::Elem(p) => p.fmt(f)?,
                    RangeBound::Top => f.write_str("+inf")?,
                    RangeBound::Bottom => unreachable!(),
                }
            }
            Interval::Point(p) => p.fmt(f)?,
        }
        f.write_str(", ")?;
        self.timestamp().fmt(f)?;
        f.write_str(")")?;
        Ok(())
    }
}

impl<P, T> Partitioned<P, T> {
    /// Construct a new timestamp for a specific partition
    pub fn with_partition(partition: P, timestamp: T) -> Self {
        Self(Product::new(Interval::Point(partition), timestamp))
    }

    /// Construct a new timestamp for an exclusive partition range
    pub fn with_range(lower: Option<P>, upper: Option<P>, timestamp: T) -> Self {
        let lower = lower.map(RangeBound::Elem).unwrap_or(RangeBound::Bottom);
        let upper = upper.map(RangeBound::Elem).unwrap_or(RangeBound::Top);
        Self(Product::new(Interval::Range(lower, upper), timestamp))
    }

    /// Access the interval of this partitioned timestamp
    pub fn interval(&self) -> &Interval<P> {
        &self.0.outer
    }

    /// Returns the partition of this timestamp if it's not a range timestamp
    pub fn partition(&self) -> Option<&P> {
        match self.0.outer {
            Interval::Point(ref partition) => Some(partition),
            Interval::Range(_, _) => None,
        }
    }

    /// Returns the timestamp of this partition interval
    pub fn timestamp(&self) -> &T {
        &self.0.inner
    }
}

impl<P: Partition, T: Timestamp> Timestamp for Partitioned<P, T> {
    type Summary = PartitionedSummary<P, T>;
    fn minimum() -> Self {
        Self(Timestamp::minimum())
    }
}

impl<P: Partition, T: Timestamp> Refines<()> for Partitioned<P, T> {
    fn to_inner(_other: ()) -> Self {
        Self::minimum()
    }

    fn to_outer(self) {}

    fn summarize(_path: Self::Summary) {}
}

impl<P: Eq, T: PartialOrder> PartialOrder for Partitioned<P, T>
where
    Interval<P>: PartialOrder,
{
    fn less_equal(&self, other: &Self) -> bool {
        self.0.less_equal(&other.0)
    }
}

/// Helper type alias to access to access the Summary type of a Timestamp implementing type
type Summary<T> = <T as Timestamp>::Summary;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PartitionedSummary<P: Partition, T: Timestamp>(
    Product<Summary<Interval<P>>, Summary<T>>,
);

impl<P: Partition, T: Timestamp> Default for PartitionedSummary<P, T> {
    fn default() -> Self {
        PartitionedSummary(Default::default())
    }
}

impl<P: Partition, T: Timestamp> PartialOrder for PartitionedSummary<P, T> {
    fn less_equal(&self, other: &Self) -> bool {
        self.0.less_equal(&other.0)
    }
}

impl<P: Partition, T: Timestamp> PathSummary<Partitioned<P, T>> for PartitionedSummary<P, T> {
    fn results_in(&self, src: &Partitioned<P, T>) -> Option<Partitioned<P, T>> {
        self.0.results_in(&src.0).map(Partitioned)
    }

    fn followed_by(&self, other: &Self) -> Option<Self> {
        PathSummary::<Product<Interval<P>, T>>::followed_by(&self.0, &other.0)
            .map(PartitionedSummary)
    }
}

impl<P: Partition, T: Timestamp> PartitionedSummary<P, T> {
    /// Construct a new summary for a specific partition
    pub fn with_partition(partition: P, timestamp: Summary<T>) -> Self {
        Self(Product::new(Interval::Point(partition), timestamp))
    }

    /// Construct a new summary for an exclusive partition range
    pub fn with_range(lower: Option<P>, upper: Option<P>, timestamp: Summary<T>) -> Self {
        let lower = lower.map(RangeBound::Elem).unwrap_or(RangeBound::Bottom);
        let upper = upper.map(RangeBound::Elem).unwrap_or(RangeBound::Top);
        Self(Product::new(Interval::Range(lower, upper), timestamp))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
/// A type that represents either an exclusive range or a particular point. When representing a
/// range either of the two bounds can be minus or positive infinity.
pub enum Interval<P> {
    /// A range of points
    Range(RangeBound<P>, RangeBound<P>),
    /// A single point
    Point(P),
}

impl<P: Ord + Eq> PartialOrder for Interval<P> {
    fn less_equal(&self, other: &Self) -> bool {
        use Interval::*;
        match (self, other) {
            (Range(self_lower, self_upper), Range(other_lower, other_upper)) => {
                self_lower <= other_lower && other_upper <= self_upper
            }
            (Range(lower, upper), Point(p)) => lower < p && upper > p,
            (Point(self_p), Point(other_p)) => self_p == other_p,
            // No Point element is less than a Range element
            (Point(_), Range(_, _)) => false,
        }
    }
}

impl<P: Partition> PathSummary<Interval<P>> for Interval<P> {
    fn results_in(&self, src: &Interval<P>) -> Option<Interval<P>> {
        use std::cmp::{max, min};
        use Interval::*;
        match (self, src) {
            // A range followed by another range contraints the range
            (Range(self_lower, self_upper), Range(other_lower, other_upper)) => {
                let new_lower = max(self_lower, other_lower);
                let new_upper = min(self_upper, other_upper);
                if new_lower < new_upper {
                    Some(Interval::Range(new_lower.clone(), new_upper.clone()))
                } else {
                    None
                }
            }
            // A range followed by an in-range partition or a partition followed by a range that
            // includes it keeps the partition
            (Range(lower, upper), Point(p)) | (Point(p), Range(lower, upper)) => {
                if lower < p && upper > p {
                    Some(Interval::Point(p.clone()))
                } else {
                    None
                }
            }
            // A partition followed by the same partition results in that partition
            (Point(self_p), Point(other_p)) => {
                if self_p == other_p {
                    Some(Point(self_p.clone()))
                } else {
                    None
                }
            }
        }
    }

    fn followed_by(&self, other: &Self) -> Option<Self> {
        self.results_in(other)
    }
}

impl<P: Partition> Timestamp for Interval<P> {
    type Summary = Interval<P>;

    fn minimum() -> Self {
        Self::default()
    }
}

impl<P> Default for Interval<P> {
    fn default() -> Self {
        Self::Range(RangeBound::Bottom, RangeBound::Top)
    }
}

/// Type to represent the lower or upper bound of an exclusive range
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum RangeBound<P> {
    /// An element that is less than any other element P
    Bottom,
    /// An element P
    Elem(P),
    /// An element that is greather than any other element P
    Top,
}

impl<P: PartialEq> PartialEq<P> for RangeBound<P> {
    fn eq(&self, other: &P) -> bool {
        match self {
            RangeBound::Bottom => false,
            RangeBound::Elem(p) => p.eq(other),
            RangeBound::Top => false,
        }
    }
}

impl<P: PartialOrd> PartialOrd<P> for RangeBound<P> {
    fn partial_cmp(&self, other: &P) -> Option<Ordering> {
        match self {
            RangeBound::Bottom => Some(Ordering::Less),
            RangeBound::Elem(p) => p.partial_cmp(other),
            RangeBound::Top => Some(Ordering::Greater),
        }
    }
}

/// A supertrait of all the required trait a partition type must have
pub trait Partition:
    Clone
    + std::fmt::Debug
    + Send
    + Sync
    + Serialize
    + DeserializeOwned
    + std::hash::Hash
    + Ord
    + 'static
{
}

impl<P> Partition for P where
    P: Clone
        + std::fmt::Debug
        + Send
        + Sync
        + Serialize
        + DeserializeOwned
        + std::hash::Hash
        + Ord
        + 'static
{
}

#[cfg(test)]
mod test {
    use timely::progress::Antichain;

    use super::*;

    #[test]
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

    #[test]
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

    #[test]
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

pub mod hybrid {
    use differential_dataflow::lattice::{Lattice, Maximum};
    use serde::{Deserialize, Serialize};
    use std::borrow::Cow;
    use std::fmt::Debug;
    use std::hash::Hash;
    use timely::progress::timestamp::Refines;
    use timely::progress::{PathSummary, Timestamp};
    use timely::{order, PartialOrder};

    /// Extend a timestamp type `A` with an inner timestamp `B`.
    /// This is useful when you want to track fine-grained progress (represented by `B`)
    /// "within" each instance of some coarser-grained timestamp `A`.
    /// The name is inspired by [hybrid logical clocks] (in which case the outer timestamp represents
    /// wall-clock time and the inner represents some logical clock) but the implementation is
    /// meant to work for arbitrary timestamp types.
    ///
    /// Unlike [`timely::order::Product`], where the two timestamps proceed more or less independently,
    /// our inner timestamp will "reset" every time the outer timestamp increases. This means that
    /// `Hybrid` is totally ordered if its component timestamps are, via the usual lexicographic
    /// ordering.
    ///
    /// This also differs from timely's [`Timestamp`] implementation for tuples, even though tuples
    /// are also lexicographically ordered:
    /// - Tuples use the same summary semantics as `Product`. `Hybrid` summaries will reset the inner
    ///   timestamp when the outer changes.
    /// - `Hybrid` implements `Refines` on the outer timestamp, making this usable as a way to use
    ///   a finer-grained timestamp in a nested scope.
    ///
    /// [hybrid logical clocks]: https://cse.buffalo.edu/tech-reports/2014-04.pdf
    #[derive(
        Eq, PartialEq, Serialize, Deserialize, Hash, Debug, Copy, Clone, Ord, PartialOrd, Default,
    )]
    pub struct Hybrid<A, B>(pub A, pub B);

    impl<A: PartialOrder, B: PartialOrder> PartialOrder for Hybrid<A, B> {
        #[inline]
        fn less_equal(&self, other: &Self) -> bool {
            PartialOrder::less_equal(&self.0, &other.0)
                && (&self.0 != &other.0 || PartialOrder::less_equal(&self.1, &other.1))
        }
    }

    impl<A: order::TotalOrder, B: order::TotalOrder> order::TotalOrder for Hybrid<A, B> {}

    impl<A: Lattice + Clone, B: Lattice + Timestamp + Maximum> Lattice for Hybrid<A, B> {
        fn join(&self, other: &Self) -> Self {
            if self.0 == other.0 {
                Hybrid(self.0.clone(), self.1.join(&other.1))
            } else if self.0.less_than(&other.0) {
                other.clone()
            } else if other.0.less_than(&self.0) {
                self.clone()
            } else {
                Hybrid(self.0.join(&other.0), B::minimum())
            }
        }

        fn meet(&self, other: &Self) -> Self {
            if self.0 == other.0 {
                Hybrid(self.0.clone(), self.1.meet(&other.1))
            } else if self.0.less_than(&other.0) {
                self.clone()
            } else if other.0.less_than(&self.0) {
                other.clone()
            } else {
                Hybrid(self.0.meet(&other.0), B::maximum())
            }
        }
    }

    impl<A: Timestamp, B: Timestamp> Timestamp for Hybrid<A, B> {
        type Summary = Hybrid<A::Summary, B::Summary>;
        fn minimum() -> Self {
            Hybrid(A::minimum(), B::minimum())
        }
    }

    impl<A: Timestamp, B: Timestamp> PathSummary<Hybrid<A, B>> for Hybrid<A::Summary, B::Summary> {
        fn results_in(&self, src: &Hybrid<A, B>) -> Option<Hybrid<A, B>> {
            self.0.results_in(&src.0).and_then(|outer| {
                // "Reset" the inner timestamp iff the outer has changed.
                let inner_start = if outer == src.0 {
                    Cow::Borrowed(&src.1)
                } else {
                    Cow::Owned(B::minimum())
                };
                self.1
                    .results_in(&inner_start)
                    .map(|inner| Hybrid(outer, inner))
            })
        }

        fn followed_by(&self, other: &Self) -> Option<Self> {
            self.0.followed_by(&other.0).and_then(|outer| {
                if outer == self.0 {
                    self.1
                        .followed_by(&other.1)
                        .map(|inner| Hybrid(outer, inner))
                } else {
                    Some(Hybrid(outer, other.1.clone()))
                }
            })
        }
    }

    impl<A: Timestamp, B: Timestamp> Refines<A> for Hybrid<A, B> {
        fn to_inner(other: A) -> Self {
            Hybrid(other, B::minimum())
        }

        fn to_outer(self) -> A {
            self.0
        }

        fn summarize(path: Hybrid<A::Summary, B::Summary>) -> A::Summary {
            path.0
        }
    }

    #[cfg(test)]
    mod test {
        use super::*;
        use proptest::prelude::*;

        #[test]
        fn basic_properties() {
            // Hybrid timestamps are lexicographically ordered.
            proptest!(|(left_outer: u64, left_inner: u32, right_outer: u64, right_inner: u32)| {
                let left = Hybrid(left_outer, left_inner);
                let right = Hybrid(right_outer, right_inner);
                assert_eq!(
                    left.less_equal(&right),
                    (left_outer, left_inner) <= (right_outer, right_inner)
                );
            });

            // Lattice operations are consistent with ordering
            proptest!(|(left_outer: u64, left_inner: u32, right_outer: u64, right_inner: u32)| {
                let left = Hybrid(left_outer, left_inner);
                let right = Hybrid(right_outer, right_inner);

                let join = left.join(&right);
                assert!(join >= left);
                assert!(join >= right);

                let meet = left.meet(&right);
                assert!(meet <= left);
                assert!(meet <= right);
            });

            // Adding summaries gives you the same result as applying them in order.
            proptest!(|(outer: u64, inner: u32, a_outer: u64, a_inner: u32, b_outer: u64, b_inner: u32)| {
                let ts = Hybrid(outer, inner);
                let path_a = Hybrid(a_outer, a_inner);
                let path_b = Hybrid(b_outer, b_inner);

                assert_eq!(
                    path_a.results_in(&ts).and_then(|ts| path_b.results_in(&ts)),
                    <Hybrid<u64, u32> as PathSummary<Hybrid<u64, u32>>>::followed_by(&path_a, &path_b)
                      .and_then(|path| path.results_in(&ts)),
                );
            });

            // From the timely docs:
            // It is crucial for correctness that the result of this summarization's results_in
            // method is equivalent to |time| path.results_in(time.to_inner()).to_outer(), or at
            // least produces times less or equal to that result.
            proptest!(|(time_outer: u64, outer_summary: u64, inner_summary: u32)| {
                let path_hybrid = Hybrid(outer_summary, inner_summary);
                let path_outer = <Hybrid<u64, u32> as Refines<u64>>::summarize(path_hybrid);
                let results_outer = path_outer.results_in(&time_outer);
                let time_hybrid = <Hybrid<u64, u32> as Refines<u64>>::to_inner(time_outer);
                let results_inner = path_hybrid.results_in(&time_hybrid).map(Refines::to_outer);
                assert_eq!(results_outer, results_inner);
            });
        }
    }
}

pub use hybrid::Hybrid;
