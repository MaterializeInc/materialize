// Copyright Materialize, Inc. and contributors. All rights reserved.
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Timestamp and frontier conversion between MZ's Timely and the local DD dependency.
//!
//! Chunk bodies retain MZ timestamps. Only maintenance frontiers use [`Time`],
//! which delegates ordering, lattice operations, and path summaries to the original
//! type. No Timely scopes, capabilities, or operators cross this boundary.

use serde::{Deserialize, Serialize};
use timely::progress::Timestamp;

/// An MZ timestamp or path summary implementing the local DD dependency's traits.
#[derive(
    Clone,
    Debug,
    Default,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Serialize,
    Deserialize
)]
#[serde(transparent)]
pub struct Time<T>(pub T);

impl<T: timely::PartialOrder> timely_next::PartialOrder for Time<T> {
    fn less_equal(&self, other: &Self) -> bool {
        self.0.less_equal(&other.0)
    }
}

impl<T: Timestamp> timely_next::progress::Timestamp for Time<T> {
    type Summary = Time<T::Summary>;

    fn minimum() -> Self {
        Time(T::minimum())
    }
}

impl<T: Timestamp, S: timely::progress::timestamp::PathSummary<T>>
    timely_next::progress::timestamp::PathSummary<Time<T>> for Time<S>
{
    fn results_in(&self, src: &Time<T>) -> Option<Time<T>> {
        self.0.results_in(&src.0).map(Time)
    }

    fn followed_by(&self, other: &Self) -> Option<Self> {
        self.0.followed_by(&other.0).map(Time)
    }
}

impl<T: differential_dataflow::lattice::Lattice> differential_dataflow_next::lattice::Lattice
    for Time<T>
{
    fn join(&self, other: &Self) -> Self {
        Time(self.0.join(&other.0))
    }

    fn meet(&self, other: &Self) -> Self {
        Time(self.0.meet(&other.0))
    }
}

/// Convert a maintenance frontier to the local DD timestamp type.
pub(super) fn to_native<T: Timestamp>(
    frontier: timely::progress::frontier::AntichainRef<'_, T>,
) -> timely_next::progress::Antichain<Time<T>> {
    frontier.iter().cloned().map(Time).collect()
}

/// Convert a maintenance frontier back to MZ's timestamp type.
pub(super) fn to_mz<T: Timestamp>(
    frontier: timely_next::progress::frontier::AntichainRef<'_, Time<T>>,
) -> timely::progress::Antichain<T> {
    frontier.iter().map(|time| time.0.clone()).collect()
}

#[cfg(test)]
mod tests {
    use super::{Time, to_mz, to_native};
    use differential_dataflow_next::lattice::Lattice;
    use timely::order::Product;
    use timely::progress::Antichain;
    use timely_next::PartialOrder;
    use timely_next::progress::timestamp::PathSummary;

    #[mz_ore::test]
    fn partial_order_and_frontiers_survive_conversion() {
        let a = Product::new(1u64, 3u64);
        let b = Product::new(3u64, 1u64);
        assert!(!Time(a).less_equal(&Time(b)));
        assert!(!Time(b).less_equal(&Time(a)));
        assert_eq!(Time(a).join(&Time(b)), Time(Product::new(3, 3)));
        assert_eq!(Time(a).meet(&Time(b)), Time(Product::new(1, 1)));

        let frontier = Antichain::from_iter([a, b]);
        let native = to_native(frontier.borrow());
        assert_eq!(native.len(), 2);
        assert!(native.less_equal(&Time(Product::new(2, 4))));
        assert!(!native.less_equal(&Time(Product::new(2, 2))));
        assert_eq!(to_mz(native.borrow()), frontier);

        let empty = Antichain::<Product<u64, u64>>::new();
        assert_eq!(to_mz(to_native(empty.borrow()).borrow()), empty);
    }

    #[mz_ore::test]
    fn summaries_preserve_composition_and_overflow() {
        let summary = Time(Product::new(1u64, 2u64));
        let next = Time(Product::new(3u64, 4u64));
        let time = Time(Product::new(5u64, 6u64));
        assert_eq!(summary.results_in(&time), Some(Time(Product::new(6, 8))));
        let composed =
            PathSummary::<Time<Product<u64, u64>>>::followed_by(&summary, &next).unwrap();
        assert_eq!(composed, Time(Product::new(4, 6)));
        assert_eq!(
            composed.results_in(&time),
            summary.results_in(&time).and_then(|t| next.results_in(&t))
        );
        assert_eq!(
            summary.results_in(&Time(Product::new(u64::MAX, 0u64))),
            None
        );
        assert_eq!(
            PathSummary::<Time<Product<u64, u64>>>::followed_by(
                &summary,
                &Time(Product::new(u64::MAX, 0u64))
            ),
            None
        );
    }
}
