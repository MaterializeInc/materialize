// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Tests of how read policies map write frontiers to read frontiers.

use timely::PartialOrder;

use super::*;

/// Under `PartialOrder::less_equal` the empty antichain is the top of the frontier lattice and
/// `Timestamp::MIN` is the bottom, so the empty-upper branch of the lag policies maps top to
/// bottom. A policy that inverts the lattice this way pins the since of a sealed collection at
/// the bottom, where it can never advance again. Changing these two lines is safe only as a
/// deliberate act, taken together with the controller code that decides when a collection is
/// sealed.
#[mz_ore::test]
fn lag_policies_map_empty_upper_to_the_lattice_bottom() {
    let empty = Antichain::<Timestamp>::new();
    let bottom = Antichain::from_elem(Timestamp::MIN);
    // Beyond the lag, so that subtracting the lag does not land back on the lattice bottom.
    let non_empty = Antichain::from_elem(Timestamp::from(10_000u64));

    assert!(
        PartialOrder::less_equal(&non_empty, &empty),
        "the empty antichain is the top of the frontier lattice"
    );

    for (name, policy) in [
        ("step_back", ReadPolicy::step_back()),
        (
            "lag_writes_by",
            ReadPolicy::lag_writes_by(Timestamp::from(1000u64), Timestamp::from(1000u64)),
        ),
    ] {
        assert_eq!(
            policy.frontier(empty.borrow()),
            bottom,
            "{name} no longer maps the empty upper to the lattice bottom"
        );

        // The same fact in lattice terms: the policy is not monotone, because it maps the
        // greatest element of its domain to the least element of its range.
        assert!(
            !PartialOrder::less_equal(
                &policy.frontier(non_empty.borrow()),
                &policy.frontier(empty.borrow()),
            ),
            "{name} unexpectedly became monotone at the empty upper"
        );
    }
}

/// The constant policies are monotone at the empty upper, so a sealed collection keeps whatever
/// since they pin it to. Only the lag policies invert the lattice.
#[mz_ore::test]
fn constant_policies_ignore_the_empty_upper() {
    let empty = Antichain::<Timestamp>::new();
    let since = Antichain::from_elem(Timestamp::from(7u64));

    let valid_from = ReadPolicy::ValidFrom(since.clone());
    assert_eq!(valid_from.frontier(empty.borrow()), since);

    let no_policy = ReadPolicy::NoPolicy {
        initial_since: since.clone(),
    };
    assert_eq!(no_policy.frontier(empty.borrow()), since);
}
