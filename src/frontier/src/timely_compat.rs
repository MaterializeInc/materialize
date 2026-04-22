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
