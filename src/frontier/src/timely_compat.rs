// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License").

//! Conversions between `mz_frontier` and `timely::progress` types.
//!
//! Gated behind the `timely-compat` feature so control-plane crates that do
//! not otherwise need timely can drop it from their dependencies.

use timely::progress::Antichain as TimelyAntichain;

use crate::antichain::Antichain;

impl<T: timely::order::PartialOrder> From<Antichain<T>> for TimelyAntichain<T> {
    fn from(a: Antichain<T>) -> Self {
        match a.into_option() {
            None => TimelyAntichain::new(),
            Some(t) => TimelyAntichain::from_elem(t),
        }
    }
}

impl<T: Ord> From<TimelyAntichain<T>> for Antichain<T> {
    fn from(a: TimelyAntichain<T>) -> Self {
        // Timely antichains in the control plane contain at most one element.
        // Preserve that invariant; panic if violated.
        let mut elements = a.into_iter();
        match (elements.next(), elements.next()) {
            (None, _) => Antichain::new(),
            (Some(t), None) => Antichain::from_elem(t),
            (Some(_), Some(_)) => {
                panic!("timely antichain with more than one element cannot be represented as mz_frontier::Antichain")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    proptest! {
        #[test]
        fn roundtrip_from_mz_to_timely_and_back(v in proptest::option::of(any::<u64>())) {
            let ours = match v {
                None => Antichain::new(),
                Some(t) => Antichain::from_elem(t),
            };
            let theirs: TimelyAntichain<u64> = ours.clone().into();
            let back: Antichain<u64> = theirs.into();
            prop_assert_eq!(ours, back);
        }

        #[test]
        fn roundtrip_from_timely_to_mz_and_back(v in proptest::option::of(any::<u64>())) {
            let theirs = match v {
                None => TimelyAntichain::new(),
                Some(t) => TimelyAntichain::from_elem(t),
            };
            let ours: Antichain<u64> = theirs.clone().into();
            let back: TimelyAntichain<u64> = ours.into();
            prop_assert_eq!(theirs, back);
        }

        #[test]
        fn less_equal_matches_timely(a in proptest::option::of(any::<u64>()), b in proptest::option::of(any::<u64>())) {
            let ours_a = match a { None => Antichain::new(), Some(t) => Antichain::from_elem(t) };
            let ours_b = match b { None => Antichain::new(), Some(t) => Antichain::from_elem(t) };
            let theirs_a: TimelyAntichain<u64> = ours_a.clone().into();
            let theirs_b: TimelyAntichain<u64> = ours_b.clone().into();
            let ours_le = crate::PartialOrder::less_equal(&ours_a, &ours_b);
            let theirs_le = <TimelyAntichain<u64> as timely::PartialOrder>::less_equal(&theirs_a, &theirs_b);
            prop_assert_eq!(ours_le, theirs_le);
        }

        #[test]
        fn join_matches_timely(a in proptest::option::of(any::<u64>()), b in proptest::option::of(any::<u64>())) {
            use differential_dataflow::lattice::Lattice;
            let ours_a = match a { None => Antichain::new(), Some(t) => Antichain::from_elem(t) };
            let ours_b = match b { None => Antichain::new(), Some(t) => Antichain::from_elem(t) };
            let theirs_a: TimelyAntichain<u64> = ours_a.clone().into();
            let theirs_b: TimelyAntichain<u64> = ours_b.clone().into();
            let ours_join: TimelyAntichain<u64> = ours_a.join(&ours_b).into();
            let theirs_join = theirs_a.join(&theirs_b);
            prop_assert_eq!(ours_join, theirs_join);
        }

        #[test]
        fn meet_matches_timely(a in proptest::option::of(any::<u64>()), b in proptest::option::of(any::<u64>())) {
            use differential_dataflow::lattice::Lattice;
            let ours_a = match a { None => Antichain::new(), Some(t) => Antichain::from_elem(t) };
            let ours_b = match b { None => Antichain::new(), Some(t) => Antichain::from_elem(t) };
            let theirs_a: TimelyAntichain<u64> = ours_a.clone().into();
            let theirs_b: TimelyAntichain<u64> = ours_b.clone().into();
            let ours_meet: TimelyAntichain<u64> = ours_a.meet(&ours_b).into();
            let theirs_meet = theirs_a.meet(&theirs_b);
            prop_assert_eq!(ours_meet, theirs_meet);
        }
    }
}
