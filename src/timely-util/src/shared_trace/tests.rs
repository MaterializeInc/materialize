// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use timely::progress::Antichain;
use timely::progress::frontier::MutableAntichain;

use super::adjust;

fn at(t: u64) -> Antichain<u64> {
    Antichain::from_elem(t)
}

#[mz_ore::test]
fn a_hold_at_or_above_the_meet_does_not_move_it() {
    let mut holds = MutableAntichain::new();
    assert!(
        adjust(&mut holds, &Antichain::new(), &at(5)),
        "the first hold sets the meet"
    );
    assert!(
        !adjust(&mut holds, &Antichain::new(), &at(7)),
        "a second hold above the meet leaves it at 5"
    );
    assert!(
        !adjust(&mut holds, &at(7), &at(9)),
        "advancing the hold above the meet leaves it at 5"
    );
    assert!(
        !adjust(&mut holds, &at(9), &Antichain::new()),
        "releasing the hold above the meet leaves it at 5"
    );
}

#[mz_ore::test]
fn moving_the_lowest_hold_moves_the_meet() {
    let mut holds = MutableAntichain::new();
    adjust(&mut holds, &Antichain::new(), &at(5));
    adjust(&mut holds, &Antichain::new(), &at(7));
    assert!(adjust(&mut holds, &at(5), &at(6)), "the meet advances to 6");
    assert!(
        adjust(&mut holds, &at(6), &Antichain::new()),
        "the meet advances to 7"
    );
    assert!(
        adjust(&mut holds, &at(7), &Antichain::new()),
        "the meet becomes empty"
    );
}
