// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Tests for the MV sink's shared helpers.

use std::any::Any;
use std::rc::{Rc, Weak};

use mz_repr::Timestamp;
use timely::progress::Antichain;

use super::SnapshotGate;

/// Build a gate armed on a fresh token, returning a weak handle that reports whether the gate
/// still holds the token.
fn armed(as_of: &Antichain<Timestamp>) -> (SnapshotGate, Weak<dyn Any>) {
    let token: Rc<dyn Any> = Rc::new(());
    let held = Rc::downgrade(&token);
    (SnapshotGate::new(as_of, Some(token)), held)
}

fn is_open(held: &Weak<dyn Any>) -> bool {
    held.strong_count() == 0
}

fn frontier(ts: u64) -> Antichain<Timestamp> {
    Antichain::from_elem(Timestamp::new(ts))
}

#[mz_ore::test]
fn gate_stays_shut_until_desired_passes_as_of() {
    let (mut gate, held) = armed(&frontier(10));
    assert!(!is_open(&held));

    // A frontier at the `as_of` means the snapshot updates at that time may still be arriving.
    gate.observe(&frontier(10));
    assert!(!is_open(&held));

    // A frontier before the `as_of` must not open the gate either. The `write` operator clamps
    // its tracked frontier upwards, but the gate compares against whatever it is handed.
    gate.observe(&frontier(5));
    assert!(!is_open(&held));

    gate.observe(&frontier(11));
    assert!(is_open(&held));
}

#[mz_ore::test]
fn empty_desired_frontier_opens_the_gate() {
    let (mut gate, held) = armed(&frontier(10));
    gate.observe(&Antichain::new());
    assert!(is_open(&held));
}

#[mz_ore::test]
fn empty_as_of_opens_the_gate_immediately() {
    // The empty antichain is the maximum, so no `desired` frontier can ever move beyond it.
    // Withholding the read-back forever would wedge the sink, so the gate must start open.
    let (_gate, held) = armed(&Antichain::new());
    assert!(is_open(&held));
}

#[mz_ore::test]
fn unarmed_gate_is_inert() {
    let as_of = frontier(10);
    let mut gate = SnapshotGate::new(&as_of, None);
    // Observing frontiers on a gate that was never armed must not panic.
    gate.observe(&frontier(5));
    gate.observe(&frontier(11));
}
